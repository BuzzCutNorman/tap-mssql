"""mssql tap class."""

from __future__ import annotations

import sys
from typing import TYPE_CHECKING

from singer_sdk import SQLTap
from singer_sdk import typing as th  # JSON schema typing helpers

from .client import MSSQLStream
from .json import serialize_jsonl

if TYPE_CHECKING:
    from singer_sdk._singerlib.encoding._simple import Message


class Tapmssql(SQLTap):
    """mssql tap class."""

    name = "tap-mssql"
    default_stream_class = MSSQLStream
    default_output = sys.stdout.buffer

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
            required=True
        ),
        th.Property(
            "password",
            th.StringType,
            description="The Password for the User account",
            required=True,
            secret=True
        ),
        th.Property(
            "database",
            th.StringType,
            description="The Default database for this connection",
            required=True
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


    def serialize_message(self, message: Message) -> str | bytes:
        """Serialize a dictionary into a line of json.

        Args:
            message: A Singer message object.

        Returns:
            A string of serialized json.
        """
        return serialize_jsonl(message.to_dict())

    def write_message(self, message: Message) -> None:
        """Write a message to stdout.

        Args:
            message: The message to write.
        """
        self.default_output.write(self.format_message(message))
        self.default_output.flush()

if __name__ == "__main__":
    Tapmssql.cli()
