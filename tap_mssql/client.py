"""SQL client handling.

This includes mssqlStream and mssqlConnector.
"""
from __future__ import annotations

import gzip
import json
import datetime

from decimal import Decimal
from uuid import uuid4
from typing import Any, Dict, Iterable, Optional

import pendulum
import pyodbc
import sqlalchemy

from sqlalchemy.engine import Engine
from sqlalchemy.engine.url import URL

from singer_sdk import SQLConnector, SQLStream
from singer_sdk.helpers._batch import (
    BaseBatchFileEncoding,
    BatchConfig,
)
from singer_sdk.streams.core import lazy_chunked_generator


class mssqlConnector(SQLConnector):
    """Connects to the mssql SQL source."""

    def __init__(
            self,
            config: dict | None = None,
            sqlalchemy_url: str | None = None
         ) -> None:
        """Class Default Init"""
        # If pyodbc given set pyodbc.pooling to False
        # This allows SQLA to manage to connection pool
        if config['driver_type'] == 'pyodbc':
            pyodbc.pooling = False

        super().__init__(config, sqlalchemy_url)

    def get_sqlalchemy_url(cls, config: dict) -> str:
        """Return the SQLAlchemy URL string.

        Args:
            config: A dictionary of settings from the tap or target config.

        Returns:
            The URL as a string.
        """
        if config['dialect'] == "mssql":
            url_drivername: str = config['dialect']
        else:
            cls.logger.error("Invalid dialect given")
            exit(1)

        if config['driver_type'] in ["pyodbc", "pymssql"]:
            url_drivername += f"+{config['driver_type']}"
        else:
            cls.logger.error("Invalid driver_type given")
            exit(1)

        config_url = URL.create(
            url_drivername,
            config['user'],
            config['password'],
            host=config['host'],
            database=config['database']
        )

        if 'port' in config:
            config_url = config_url.set(port=config['port'])

        if 'sqlalchemy_url_query' in config:
            config_url = config_url.update_query_dict(
                config['sqlalchemy_url_query']
                )

        return (config_url)

    def create_engine(self) -> Engine:
        """Return a new SQLAlchemy engine using the provided config.

        Developers can generally override just one of the following:
        `sqlalchemy_engine`, sqlalchemy_url`.

        Returns:
            A newly created SQLAlchemy engine object.
        """
        eng_prefix = "ep."
        eng_config = {
            f"{eng_prefix}url": self.sqlalchemy_url,
            f"{eng_prefix}echo": "False"
        }

        if self.config.get('sqlalchemy_eng_params'):
            for key, value in self.config['sqlalchemy_eng_params'].items():
                eng_config.update({f"{eng_prefix}{key}": value})

        return sqlalchemy.engine_from_config(eng_config, prefix=eng_prefix)

    def to_jsonschema_type(
            self,
            from_type: str
            | sqlalchemy.types.TypeEngine
            | type[sqlalchemy.types.TypeEngine],) -> None:
        """Returns a JSON Schema equivalent for the given SQL type.

        Developers may optionally add custom logic before calling the default
        implementation inherited from the base class.

        Args:
            from_type: The SQL type as a string or as a TypeEngine.
                If a TypeEngine is provided, it may be provided as a class or
                a specific object instance.

        Returns:
            A compatible JSON Schema type definition.
        """
        if self.config.get('hd_jsonschema_types', False):
            return self.hd_to_jsonschema_type(from_type)
        else:
            return self.org_to_jsonschema_type(from_type)

    @staticmethod
    def org_to_jsonschema_type(
        from_type: str
        | sqlalchemy.types.TypeEngine
        | type[sqlalchemy.types.TypeEngine],
    ) -> dict:
        """Returns a JSON Schema equivalent for the given SQL type.

        Developers may optionally add custom logic before calling the default
        implementation inherited from the base class.

        Args:
            from_type: The SQL type as a string or as a TypeEngine.
                If a TypeEngine is provided, it may be provided as a class or
                a specific object instance.

        Returns:
            A compatible JSON Schema type definition.
        """

        """
            Checks for the MSSQL type of NUMERIC
                if scale = 0 it is typed as a INTEGER
                if scale != 0 it is typed as NUMBER
        """
        if str(from_type).startswith("NUMERIC"):
            if str(from_type).endswith(", 0)"):
                from_type = "int"
            else:
                from_type = "number"

        if str(from_type) in ["MONEY", "SMALLMONEY"]:
            from_type = "number"

        return SQLConnector.to_jsonschema_type(from_type)

    @staticmethod
    def hd_to_jsonschema_type(
        from_type: str
        | sqlalchemy.types.TypeEngine
        | type[sqlalchemy.types.TypeEngine],
    ) -> dict:
        """Returns a JSON Schema equivalent for the given SQL type.

        Developers may optionally add custom logic before calling the default
        implementation inherited from the base class.

        Args:
            from_type: The SQL type as a string or as a TypeEngine.
                If a TypeEngine is provided, it may be provided as a class or
                a specific object instance.

        Raises:
            ValueError: If the `from_type` value is not of type `str` or `TypeEngine`.

        Returns:
            A compatible JSON Schema type definition.
        """
        # This is taken from to_jsonschema_type() in typing.py
        if isinstance(from_type, str):
            sql_type_name = from_type
        elif isinstance(from_type, sqlalchemy.types.TypeEngine):
            sql_type_name = type(from_type).__name__
        elif isinstance(from_type, type) and issubclass(
            from_type, sqlalchemy.types.TypeEngine
        ):
            sql_type_name = from_type.__name__
        else:
            raise ValueError(
                "Expected `str` or a SQLAlchemy `TypeEngine` object or type."
             )

        # Add in the length of the
        if sql_type_name in ['CHAR', 'NCHAR', 'VARCHAR', 'NVARCHAR']:
            maxLength: int = getattr(from_type, 'length')

            if getattr(from_type, 'length'):
                return {
                    "type": ["string"],
                    "maxLength": maxLength
                }

        if sql_type_name == 'TIME':
            return {
                "type": ["string"],
                "format": "time"
            }

        if sql_type_name == 'UNIQUEIDENTIFIER':
            return {
                "type": ["string"],
                "format": "uuid"
            }
        
        if sql_type_name == 'XML':
            return {
                "type": ["string"],
                "contentMediaType": "application/xml",
            }

        if sql_type_name in ['BINARY', 'IMAGE', 'VARBINARY']:
            maxLength: int = getattr(sql_type, 'length')
            if getattr(sql_type, 'length'):
                return {
                    "type": ["string"],
                    "contentEncoding": "base64",
                    "maxLength": maxLength
                }
            else:
                return {
                    "type": ["string"],
                    "contentEncoding": "base64",
                }

        # This is a MSSQL only DataType
        # SQLA does the converion from 0,1
        # to Python True, False
        if sql_type_name == 'BIT':
            return {"type": ["boolean"]}

        # This is a MSSQL only DataType
        if sql_type_name == 'TINYINT':
            return {
                "type": ["integer"],
                "minimum": 0,
                "maximum": 255
            }

        if sql_type_name == 'SMALLINT':
            return {
                "type": ["integer"],
                "minimum": -32768,
                "maximum": 32767
            }

        if sql_type_name == 'INTEGER':
            return {
                "type": ["integer"],
                "minimum": -2147483648,
                "maximum": 2147483647
            }

        if sql_type_name == 'BIGINT':
            return {
                "type": ["integer"],
                "minimum": -9223372036854775808,
                "maximum": 9223372036854775807
            }

        # Checks for the MSSQL type of NUMERIC and DECIMAL
        #     if scale = 0 it is typed as a INTEGER
        #     if scale != 0 it is typed as NUMBER
        if sql_type_name in ("NUMERIC", "DECIMAL"):
            precision: int = getattr(from_type, 'precision')
            scale: int = getattr(from_type, 'scale')
            if scale == 0:
                return {
                    "type": ["integer"],
                    "minimum": (-pow(10, precision))+1,
                    "maximum": (pow(10, precision))-1
                }
            else:
                maximum_as_number = str()
                minimum_as_number: str = '-'
                for i in range(precision):
                    if i == (precision-scale):
                        maximum_as_number += '.'
                    maximum_as_number += '9'
                minimum_as_number += maximum_as_number

                maximum_scientific_format: str = '9.'
                minimum_scientific_format: str = '-'
                for i in range(scale):
                    maximum_scientific_format += '9'
                maximum_scientific_format += f"e+{precision}"
                minimum_scientific_format += maximum_scientific_format

                if "e+" not in str(float(maximum_as_number)):
                    return {
                        "type": ["number"],
                        "minimum": float(minimum_as_number),
                        "maximum": float(maximum_as_number)
                    }
                else:
                    return {
                        "type": ["number"],
                        "minimum": float(minimum_scientific_format),
                        "maximum": float(maximum_scientific_format)
                    }

        # This is a MSSQL only DataType
        if sql_type_name == "SMALLMONEY":
            return {
                "type": ["number"],
                "minimum": -214748.3648,
                "maximum": 214748.3647
            }

        # This is a MSSQL only DataType
        # The min and max are getting truncated catalog
        if sql_type_name == "MONEY":
            return {
                "type": ["number"],
                "minimum": -922337203685477.5808,
                "maximum": 922337203685477.5807
            }

        if sql_type_name == "FLOAT":
            return {
                "type": ["number"],
                "minimum": -1.79e308,
                "maximum": 1.79e308
            }

        if sql_type_name == "REAL":
            return {
                "type": ["number"],
                "minimum": -3.40e38,
                "maximum": 3.40e38
            }

        return SQLConnector.to_jsonschema_type(from_type)

    @staticmethod
    def to_sql_type(jsonschema_type: dict) -> sqlalchemy.types.TypeEngine:
        """Return a JSON Schema representation of the provided type.

        By default will call `typing.to_sql_type()`.

        Developers may override this method to accept additional input
        argument types, to support non-standard types, or to provide custom
        typing logic. If overriding this method, developers should call the
        default implementation from the base class for all unhandled cases.

        Args:
            jsonschema_type: The JSON Schema representation of the source type.

        Returns:
            The SQLAlchemy type representation of the data type.
        """

        return SQLConnector.to_sql_type(jsonschema_type)


class CustomJSONEncoder(json.JSONEncoder):
    """Custom class extends json.JSONEncoder"""

    # Override default() method
    def default(self, obj):

        # Datetime in ISO format
        if isinstance(obj, datetime.datetime):
            return pendulum.instance(obj).isoformat()

        # Date in ISO format
        if isinstance(obj, datetime.date):
            return obj.isoformat()

        # Time in ISO format truncated to the second to pass
        # json-schema validation
        if isinstance(obj, datetime.time):
            return obj.isoformat(timespec='seconds')

        # JSON Encoder doesn't know Decimals but it
        # does know float so we convert Decimal to float
        if isinstance(obj, Decimal):
            return float(obj)

        # Default behavior for all other types
        return super().default(obj)


class mssqlStream(SQLStream):
    """Stream class for mssql streams."""

    connector_class = mssqlConnector
    encoder_class = CustomJSONEncoder

    def get_records(
            self,
            partition: Optional[dict]
         ) -> Iterable[Dict[str, Any]]:
        """Return a generator of record-type dictionary objects.

        Developers may optionally add custom logic before calling the default
        implementation inherited from the base class.

        Args:
            partition: If provided, will read only from this data slice.

        Yields:
            One dict per record.
        """

        yield from super().get_records(partition)

    def get_batches(
        self,
        batch_config: BatchConfig,
        context: dict | None = None,
    ) -> Iterable[tuple[BaseBatchFileEncoding, list[str]]]:
        """Batch generator function.

        Developers are encouraged to override this method to customize batching
        behavior for databases, bulk APIs, etc.

        Args:
            batch_config: Batch config for this stream.
            context: Stream partition or context dictionary.

        Yields:
            A tuple of (encoding, manifest) for each batch.
        """
        sync_id = f"{self.tap_name}--{self.name}-{uuid4()}"
        prefix = batch_config.storage.prefix or ""

        for i, chunk in enumerate(
            lazy_chunked_generator(
                self._sync_records(context, write_messages=False),
                self.batch_size,
            ),
            start=1,
        ):
            filename = f"{prefix}{sync_id}-{i}.json.gz"
            with batch_config.storage.fs() as fs:
                with fs.open(filename, "wb") as f:
                    # TODO: Determine compression from config.
                    with gzip.GzipFile(fileobj=f, mode="wb") as gz:
                        gz.writelines(
                            (json.dumps(record, cls=self.encoder_class) + "\n").encode() for record in chunk
                        )
                file_url = fs.geturl(filename)

            yield batch_config.encoding, [file_url]
