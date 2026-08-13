"""Unit tests for columns SQLAlchemy drops when it cannot see their type.

The MSSQL dialect reflects columns by inner-joining `sys.columns` to `sys.types`,
and SQL Server hides a user-defined type's row from a login with no permission on
that type — so every column using one disappears from the reflection with no
warning. Trimble-hosted Vista is built on UDTs: `dbo.bGLDT` reflected 5 of its
columns and RAW got a 5-column table.

Uses mocks only — no database connection.
"""

from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest
import sqlalchemy as sa

from tap_mssql.client import MSSQLConnector

BASE_CONFIG = {"azure_access_tokens": "false", "filter_schemas": ["dbo"]}

# dbo.bGLDT as the pipeline's login sees it: the plain-typed columns reflect,
# the ones over Vista's UDTs (GLCo -> bCompany, Mth -> bMonth, ...) do not.
VISIBLE_COLUMN = {"name": "SrcTable", "type": sa.VARCHAR(128), "nullable": True}


def _sys_row(
    name: str,
    base_type: str = "int",
    *,
    max_length: int = 4,
    precision: int = 10,
    scale: int = 0,
    is_nullable: bool = False,
) -> SimpleNamespace:
    return SimpleNamespace(
        name=name,
        base_type=base_type,
        max_length=max_length,
        precision=precision,
        scale=scale,
        is_nullable=is_nullable,
    )


def _connector(sys_columns: list[SimpleNamespace]) -> MSSQLConnector:
    with patch.object(MSSQLConnector, "create_engine", return_value=MagicMock()):
        connector = MSSQLConnector(
            config={**BASE_CONFIG, "filter_tables": ["dbo.bGLDT"]},
            sqlalchemy_url="placeholder",
        )
    engine = MagicMock()
    engine.connect.return_value.__enter__.return_value.execute.return_value = (
        sys_columns
    )
    connector._cached_engine = engine  # noqa: SLF001
    return connector


def _inspector(
    reflected_columns: list[dict],
    pk_columns: list[str],
) -> MagicMock:
    inspected = MagicMock()
    inspected.get_view_names.return_value = []
    inspected.get_columns.return_value = reflected_columns
    inspected.get_pk_constraint.return_value = {"constrained_columns": pk_columns}
    inspected.get_indexes.return_value = []
    return inspected


def test_columns_dropped_by_invisible_types_are_recovered() -> None:
    """A column whose UDT the login cannot see is still discovered, typed by the
    base type `sys.columns` reports — otherwise it never reaches RAW at all."""
    connector = _connector(
        [
            _sys_row("GLCo", "tinyint"),
            _sys_row("Mth", "datetime", is_nullable=True),
            _sys_row("GLTrans", "int"),
            _sys_row("SrcTable", "varchar", max_length=128, is_nullable=True),
        ],
    )
    inspected = _inspector([VISIBLE_COLUMN], ["GLCo", "Mth", "GLTrans"])

    with patch("sqlalchemy.inspect", return_value=inspected):
        entries = connector.discover_catalog_entries()

    properties = entries[0]["schema"]["properties"]
    assert list(properties) == ["GLCo", "Mth", "GLTrans", "SrcTable"]
    assert entries[0]["key_properties"] == ["GLCo", "Mth", "GLTrans"]


def test_recovered_columns_keep_their_source_types() -> None:
    """Recovered columns are typed from the base type, not dumped into strings:
    a decimal stays a number and a date stays a date once it reaches RAW."""
    connector = _connector(
        [
            _sys_row("Amount", "decimal", precision=12, scale=2),
            _sys_row("ActualDate", "datetime", is_nullable=True),
            _sys_row("Description", "nvarchar", max_length=60, is_nullable=True),
        ],
    )
    inspected = _inspector([], [])

    with patch("sqlalchemy.inspect", return_value=inspected):
        entries = connector.discover_catalog_entries()

    properties = entries[0]["schema"]["properties"]
    assert properties["Amount"]["type"] == ["number", "null"]
    assert properties["ActualDate"]["format"] == "date-time"
    assert properties["Description"]["type"] == ["string", "null"]
    assert properties["Description"]["maxLength"] == 30  # 60 bytes of nvarchar


def test_complete_reflection_is_left_alone() -> None:
    """On an instance where every type is visible, the dialect's own reflection
    is used unchanged — this repair must not become the normal path."""
    connector = _connector([_sys_row("SrcTable", "varchar", max_length=128)])
    inspected = _inspector([VISIBLE_COLUMN], ["SrcTable"])

    with patch("sqlalchemy.inspect", return_value=inspected):
        entries = connector.discover_catalog_entries()

    assert list(entries[0]["schema"]["properties"]) == ["SrcTable"]


def test_unreadable_sys_columns_does_not_stop_discovery() -> None:
    """If sys.columns cannot be read we cannot repair anything, but that alone is
    not a reason to fail — the key-property guard still catches a broken entry."""
    connector = _connector([])
    connector._cached_engine.connect.side_effect = sa.exc.OperationalError(  # noqa: SLF001
        "SELECT", {}, Exception("no permission")
    )
    inspected = _inspector([VISIBLE_COLUMN], ["SrcTable"])

    with patch("sqlalchemy.inspect", return_value=inspected):
        entries = connector.discover_catalog_entries()

    assert list(entries[0]["schema"]["properties"]) == ["SrcTable"]


def test_key_property_missing_from_columns_is_fatal() -> None:
    """A key naming a column the entry does not have must fail the run.

    Key properties and columns are reflected separately, so a table can come back
    with an intact primary key over columns that are gone. Loading it either dies
    in the target with an unexplained `invalid identifier`, or — for a stream the
    target loads by COPY — quietly lands a table missing most of its columns.
    """
    connector = _connector([])  # sys.columns unavailable, so no repair happens
    inspected = _inspector([VISIBLE_COLUMN], ["GLCo", "Mth", "GLTrans"])

    with (
        patch("sqlalchemy.inspect", return_value=inspected),
        pytest.raises(RuntimeError, match="GLCo"),
    ):
        connector.discover_catalog_entries()
