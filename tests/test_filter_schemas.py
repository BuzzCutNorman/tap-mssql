"""Unit tests for the `filter_schemas` discovery setting.

Without it, discovery reflects every schema in the database. That is slow on large
databases, and fatal on instances where SQL Server has auto-created schemas named
after Windows logins: such a name contains a dot, SQLAlchemy's MSSQL dialect reads
a dotted schema as `database.schema`, and reflection fails with
"Database ... does not exist" (error 911).
Uses mocks only — no database connection.
"""

from unittest.mock import MagicMock, patch

from tap_mssql.client import MSSQLConnector


def _connector(config: dict) -> MSSQLConnector:
    with patch.object(MSSQLConnector, "create_engine", return_value=MagicMock()):
        return MSSQLConnector(config=config, sqlalchemy_url="placeholder")


def test_filter_schemas_limits_discovery_to_configured_schemas() -> None:
    """When filter_schemas is set, only those schemas are discovered — the
    inspector is never asked for the full list."""
    mock_inspector = MagicMock()
    connector = _connector(
        {"azure_access_tokens": "false", "filter_schemas": ["dbo"]}
    )

    result = connector.get_schema_names(MagicMock(), mock_inspector)

    assert result == ["dbo"]
    mock_inspector.get_schema_names.assert_not_called()


def test_filter_schemas_excludes_dotted_windows_login_schemas() -> None:
    """The dotted schema that breaks SQLAlchemy's MSSQL reflection must not be
    returned when filter_schemas is set."""
    mock_inspector = MagicMock()
    mock_inspector.get_schema_names.return_value = [
        "dbo",
        "MYDOMAIN\\First.Last",  # dot in the name -> read as database.schema
    ]
    connector = _connector(
        {"azure_access_tokens": "false", "filter_schemas": ["dbo"]}
    )

    assert connector.get_schema_names(MagicMock(), mock_inspector) == ["dbo"]


def test_no_filter_schemas_falls_back_to_full_discovery() -> None:
    """Omitting filter_schemas preserves the previous behavior."""
    mock_inspector = MagicMock()
    mock_inspector.get_schema_names.return_value = ["dbo", "sales"]
    connector = _connector({"azure_access_tokens": "false"})

    assert connector.get_schema_names(MagicMock(), mock_inspector) == ["dbo", "sales"]


def test_empty_filter_schemas_falls_back_to_full_discovery() -> None:
    """An empty list is treated as "not set" rather than "discover nothing", so a
    misconfiguration cannot silently yield zero streams."""
    mock_inspector = MagicMock()
    mock_inspector.get_schema_names.return_value = ["dbo"]
    connector = _connector({"azure_access_tokens": "false", "filter_schemas": []})

    assert connector.get_schema_names(MagicMock(), mock_inspector) == ["dbo"]
