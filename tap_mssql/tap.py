"""mssql tap class."""

from typing import List

from singer_sdk import SQLTap
from singer_sdk import typing as th  # JSON schema typing helpers
from tap_mssql.client import mssqlStream


class Tapmssql(SQLTap):
    """mssql tap class."""
    name = "tap-mssql"
    default_stream_class = mssqlStream

    config_jsonschema = th.PropertiesList(
        th.Property(
            "dialect",
            th.StringType,
            description="The Dialect of SQLAlchamey"
        ),
        th.Property(
            "driver_type",
            th.StringType,
            description="The Python Driver you will be using to connect to the SQL server"
        ),
        th.Property(
            "host",
            th.StringType,
            description="The FQDN of the Host serving out the SQL Instance"
        ),
        th.Property(
            "port",
            th.IntegerType,
            description="The port on which SQL awaiting connection"
        ),
        th.Property(
            "user",
            th.StringType,
            description="The User Account who has been granted access to the SQL Server"
        ),
        th.Property(
            "password",
            th.StringType,
            description="The Password for the User account"
        ),
        th.Property(
            "database",
            th.StringType,
            description="The Default database for this connection"
        ),
        th.Property(
            "sqlalchemy_url_query",
            th.ObjectType(
                th.Property(
                "driver",
                th.StringType,
                description="The Driver to use when connection should match the Driver Type"
                ),
                th.Property(
                "TrustServerCertificate",
                th.StringType,
                description="This is a Yes No option"
                )
            ),
            description="SQLAlchemy URL Query options: driver, TrustServerCertificate"
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
                            description="Currently the only compression options is gzip",
                        )
                    )
                ),
                    th.Property(
                    "storage",
                    th.ObjectType(
                        th.Property(
                            "root",
                            th.StringType,
                            description="the directory you want batch messages to be placed in\n"\
                                        "example: file://test/batches",
                        ),
                        th.Property(
                            "prefix",
                            th.StringType,
                            description="What prefix you want your messages to have\n"\
                                        "example: test-batch-",
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
    ).to_dict()


if __name__ == "__main__":
    Tapmssql.cli()
