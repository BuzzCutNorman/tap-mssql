"""Unit tests for inspector column exclusion bug.

When inspector.get_columns() returns fewer columns than the catalog (e.g., for
MSSQL views over table-valued functions in SQLAlchemy 2.x), those columns are
silently dropped during sync. This test asserts the desired behavior: catalog
columns should be included even when the inspector omits them.
Uses mocks only — no database connection.
"""

from unittest.mock import MagicMock, patch

import sqlalchemy as sa

from tap_mssql.client import MSSQLConnector


def test_get_table_columns_includes_catalog_columns_when_inspector_omits_them() -> None:
    """When inspector.get_columns() omits a column (e.g. MSSQL views over TVFs in
    SQLAlchemy 2.x), get_table_columns should still include it if it's in
    column_names — trust the catalog over live reflection."""
    mock_engine = MagicMock()
    mock_inspector = MagicMock()
    mock_inspector.get_columns.return_value = [
        {"name": "Id", "type": sa.Integer(), "nullable": False},
        # Phase intentionally omitted - simulates SQLAlchemy 2.x MSSQL view reflection
    ]

    config = {"azure_access_tokens": "false"}
    with (
        patch.object(MSSQLConnector, "create_engine", return_value=mock_engine),
        patch("sqlalchemy.inspect", return_value=mock_inspector),
    ):
        connector = MSSQLConnector(config=config, sqlalchemy_url="placeholder")
        result = connector.get_table_columns(
            "dbo.my_view", column_names=["Id", "Phase"]
        )
        assert "Id" in result
        assert "Phase" in result  # Catalog says Phase exists; it must be included
