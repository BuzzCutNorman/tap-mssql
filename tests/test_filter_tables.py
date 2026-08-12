"""Unit tests for the `filter_tables` discovery setting.

Without it, discovery bulk-reflects every table and view in each schema and the
pipeline then discards all but the selected streams. On a large remote database
that is dominated by round-trip latency — ~4,200 objects in `dbo` on a Trimble
Vista instance took ~27 minutes wall clock for ~13 seconds of CPU.
Uses mocks only — no database connection.
"""

from unittest.mock import MagicMock, patch

import sqlalchemy as sa

from tap_mssql.client import MSSQLConnector

BASE_CONFIG = {"azure_access_tokens": "false", "filter_schemas": ["dbo"]}


def _connector(config: dict) -> MSSQLConnector:
    with patch.object(MSSQLConnector, "create_engine", return_value=MagicMock()):
        connector = MSSQLConnector(config=config, sqlalchemy_url="placeholder")
    # discover_catalog_entries reads self._engine, which outlives the patch above.
    connector._cached_engine = MagicMock()  # noqa: SLF001
    return connector


def _inspector(view_names: list[str] | None = None) -> MagicMock:
    inspected = MagicMock()
    inspected.get_view_names.return_value = view_names or []
    inspected.get_columns.return_value = [
        {"name": "Id", "type": sa.Integer(), "nullable": False},
    ]
    inspected.get_pk_constraint.return_value = {"constrained_columns": ["Id"]}
    inspected.get_indexes.return_value = []
    return inspected


def test_filter_tables_reflects_only_configured_objects() -> None:
    """Only the configured tables are reflected — the expensive bulk calls that
    walk every object in the schema are never made."""
    inspected = _inspector()
    connector = _connector({**BASE_CONFIG, "filter_tables": ["dbo.bJCJM", "dbo.bAPVM"]})

    with patch("sqlalchemy.inspect", return_value=inspected):
        entries = connector.discover_catalog_entries()

    assert len(entries) == 2
    assert [c.kwargs["schema"] for c in inspected.get_columns.call_args_list] == [
        "dbo",
        "dbo",
    ]
    inspected.get_multi_columns.assert_not_called()
    inspected.get_multi_indexes.assert_not_called()
    inspected.get_multi_pk_constraint.assert_not_called()


def test_stream_id_form_is_accepted() -> None:
    """`dbo-bJCJM.*` (Singer stream-id form) resolves to schema dbo, table bJCJM,
    so one YAML-anchored list can feed both filter_tables and select."""
    inspected = _inspector()
    connector = _connector({**BASE_CONFIG, "filter_tables": ["dbo-bJCJM.*"]})

    with patch("sqlalchemy.inspect", return_value=inspected):
        connector.discover_catalog_entries()

    inspected.get_columns.assert_called_once_with("bJCJM", schema="dbo")


def test_parse_filter_table_forms() -> None:
    """Both entry forms, and the hyphenated-schema case the `.*` suffix protects."""
    parse = MSSQLConnector._parse_filter_table  # noqa: SLF001

    assert parse("dbo.bJCJM") == ("dbo", "bJCJM")
    assert parse("dbo-bJCJM.*") == ("dbo", "bJCJM")
    assert parse("  dbo-bJCJM.*  ") == ("dbo", "bJCJM")
    assert parse("bJCJM") == (None, "bJCJM")
    # No hyphen before `.*` -> whole thing is the table, schema falls back later.
    assert parse("bJCJM.*") == (None, "bJCJM")
    # A hyphen in the schema is only safe in the explicit form, which is why the
    # `.*` suffix is what selects stream-id parsing.
    assert parse("my-schema.tbl") == ("my-schema", "tbl")


def test_unqualified_names_use_first_filter_schema() -> None:
    """A name with no schema falls back to the first filter_schemas entry."""
    inspected = _inspector()
    connector = _connector({**BASE_CONFIG, "filter_tables": ["bJCJM"]})

    with patch("sqlalchemy.inspect", return_value=inspected):
        connector.discover_catalog_entries()

    inspected.get_columns.assert_called_once_with("bJCJM", schema="dbo")


def test_missing_table_is_skipped_not_fatal() -> None:
    """A tenant may legitimately lack a table. It must be skipped with a warning,
    not abort the whole discovery."""
    inspected = _inspector()
    inspected.get_columns.side_effect = [
        sa.exc.NoSuchTableError("bMissing"),
        [{"name": "Id", "type": sa.Integer(), "nullable": False}],
    ]
    connector = _connector(
        {**BASE_CONFIG, "filter_tables": ["dbo.bMissing", "dbo.bAPVM"]}
    )

    with patch("sqlalchemy.inspect", return_value=inspected):
        entries = connector.discover_catalog_entries()

    assert len(entries) == 1  # the surviving table still gets discovered


def test_zero_column_reflection_is_skipped() -> None:
    """An object that reflects no columns would yield an unusable stream."""
    inspected = _inspector()
    inspected.get_columns.return_value = []
    connector = _connector({**BASE_CONFIG, "filter_tables": ["dbo.bWeird"]})

    with patch("sqlalchemy.inspect", return_value=inspected):
        assert connector.discover_catalog_entries() == []


def test_views_are_flagged_as_views() -> None:
    """is_view must still be set correctly, and view names fetched once per schema."""
    inspected = _inspector(view_names=["JCJPDescGet"])
    connector = _connector(
        {**BASE_CONFIG, "filter_tables": ["dbo.JCJPDescGet", "dbo.bAPVM"]}
    )

    with patch("sqlalchemy.inspect", return_value=inspected):
        entries = connector.discover_catalog_entries()

    by_name = {e["table_name"]: e for e in entries}
    assert by_name["JCJPDescGet"]["is_view"] is True
    assert by_name["bAPVM"]["is_view"] is False
    inspected.get_view_names.assert_called_once_with(schema="dbo")


def test_reflect_indices_false_skips_index_reflection() -> None:
    """The reflect_indices flag is still honored."""
    inspected = _inspector()
    connector = _connector({**BASE_CONFIG, "filter_tables": ["dbo.bAPVM"]})

    with patch("sqlalchemy.inspect", return_value=inspected):
        connector.discover_catalog_entries(reflect_indices=False)

    inspected.get_indexes.assert_not_called()


def test_no_filter_tables_falls_back_to_default_discovery() -> None:
    """Omitting filter_tables preserves the previous behavior."""
    connector = _connector(BASE_CONFIG)

    with patch.object(
        MSSQLConnector.__mro__[1], "discover_catalog_entries", return_value=["sentinel"]
    ) as super_discover:
        assert connector.discover_catalog_entries() == ["sentinel"]

    super_discover.assert_called_once()
