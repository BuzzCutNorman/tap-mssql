"""mssql tap class."""

from __future__ import annotations

from singer_sdk import SQLTap
from singer_sdk import typing as th  # JSON schema typing helpers
from singer_sdk.contrib.msgspec import MsgSpecWriter

from .client import MSSQLStream


class Tapmssql(SQLTap):
    """mssql tap class."""

    name = "tap-mssql"
    default_stream_class = MSSQLStream
    message_writer_class = MsgSpecWriter

    config_jsonschema = th.PropertiesList(
        th.Property(
            "dialect",
            th.StringType,
            description="The Dialect of SQLAlchamey",
            required=True,
            allowed_values=["mssql"],
            default="mssql"
        ),
        th.Property(
            "driver_type",
            th.StringType,
            description="The Python Driver you will be using to connect to the SQL server",  # noqa: E501
            required=True,
            allowed_values=["pyodbc", "pymssql"],
            default="pymssql"
        ),
        th.Property(
            "host",
            th.StringType,
            description="The FQDN of the Host serving out the SQL Instance",
            required=True
        ),
        th.Property(
            "port",
            th.IntegerType,
            description="The port on which SQL awaiting connection"
        ),
        th.Property(
            "user",
            th.StringType,
            description="The User Account who has been granted access to the SQL Server",  # noqa: E501
        ),
        th.Property(
            "password",
            th.StringType,
            description="The Password for the User account",
            secret=True
        ),
        th.Property(
            "database",
            th.StringType,
            description="The Default database for this connection",
            required=True
        ),
        th.Property(
            "azure_access_tokens",
            th.StringType,
            description="Obtain Azure Access Tokens when connecting: \'True\', \'False\'",
            default="False"
        ),
        th.Property(
            "filter_tables",
            th.ArrayType(th.StringType),
            description=(
                "Limit discovery to these tables/views. Accepts 'dbo.bJCJM' or the "
                "Singer stream-id form 'dbo-bJCJM.*', so one list can also serve a "
                "pipeline's selection rules (e.g. via a YAML anchor). Names without "
                "a schema use the first entry of filter_schemas. When omitted, every "
                "table and view in each discovered schema is reflected, which on a "
                "large remote database is dominated by round-trip latency (~4,200 "
                "objects took ~27 minutes on a Trimble Vista instance). Stream "
                "selection happens downstream and is not visible to the tap. Objects "
                "that do not exist are logged and skipped, not fatal."
            )
        ),
        th.Property(
            "filter_schemas",
            th.ArrayType(th.StringType),
            description=(
                "Limit discovery to these schemas, e.g. ['dbo']. When omitted, every "
                "schema in the database is reflected, which is slow on large databases "
                "and fails outright when a schema name contains a dot (SQL Server "
                "auto-creates schemas named after Windows logins, and SQLAlchemy reads "
                "a dotted schema as 'database.schema')."
            )
        ),
        th.Property(
            "sqlalchemy_eng_params",
            th.ObjectType(
                th.Property(
                    "fast_executemany",
                    th.StringType,
                    description="Fast Executemany Mode: True, False"
                ),
                th.Property(
                    "future",
                    th.StringType,
                    description="Run the engine in 2.0 mode: True, False"
                )
            ),
            description="SQLAlchemy Engine Paramaters: fast_executemany, future"
        ),
        th.Property(
            "sqlalchemy_url_query",
            th.ObjectType(
                th.Property(
                    "driver",
                    th.StringType,
                    description="The Driver to use when connection should match the Driver Type"  # noqa: E501
                ),
                th.Property(
                    "MultiSubnetFailover",
                    th.StringType,
                    description="This is a Yes No option"
                ),
                th.Property(
                    "TrustServerCertificate",
                    th.StringType,
                    description="This is a Yes No option"
                )
            ),
            description="SQLAlchemy URL Query options: driver, MultiSubnetFailover, TrustServerCertificate"  # noqa: E501
        ),
        th.Property(
            "batch_config",
            th.ObjectType(
                th.Property(
                    "encoding",
                    th.ObjectType(
                        th.Property(
                            "format",
                            th.StringType,
                            description="Currently the only format is jsonl",
                        ),
                        th.Property(
                            "compression",
                            th.StringType,
                            description="Currently the only compression options is gzip",  # noqa: E501
                        )
                    )
                ),
                th.Property(
                    "storage",
                    th.ObjectType(
                        th.Property(
                            "root",
                            th.StringType,
                            description=("the directory you want batch messages to be placed in\n"  # noqa: E501
                                        "example: file://test/batches"
                            )
                        ),
                        th.Property(
                            "prefix",
                            th.StringType,
                            description=("What prefix you want your messages to have\n"
                                        "example: test-batch-"
                            )
                        )
                    )
                )
            ),
            description="Optional Batch Message configuration",
        ),
        th.Property(
            "start_date",
            th.DateTimeType,
            description="The earliest record date to sync"
        ),
        th.Property(
            "hd_jsonschema_types",
            th.BooleanType,
            default=False,
            description="Turn on Higher Defined(HD) JSON Schema types to assist Targets"
        ),
    ).to_dict()


if __name__ == "__main__":
    Tapmssql.cli()
