"""SQL client handling.

This includes mssqlStream and mssqlConnector.
"""
from __future__ import annotations

import gzip
import json
from datetime import datetime
from uuid import uuid4
from typing import Any, Dict, Iterable, Optional

import pendulum
import pyodbc

import sqlalchemy
from sqlalchemy.engine import URL

from singer_sdk import SQLConnector, SQLStream
from singer_sdk.helpers._batch import (
    BaseBatchFileEncoding,
    BatchConfig,
)
from singer_sdk.streams.core import lazy_chunked_generator


class mssqlConnector(SQLConnector):
    """Connects to the mssql SQL source."""
    
    def __init__(self, config: dict | None = None, sqlalchemy_url: str | None = None) -> None:
        """Class Default Init"""
        # If pyodbc given set pyodbc.pooling to False
        # This allows SQLA to manage to connection pool
        if config['driver_type'] == 'pyodbc':
            pyodbc.pooling = False

        super().__init__(config, sqlalchemy_url)

    def get_sqlalchemy_url(cls, config: dict) -> str:
        """Concatenate a SQLAlchemy URL for use in connecting to the source."""
        if config['dialect'] == "mssql":
            url_drivername:str = config['dialect']
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
            host = config['host'],
            database = config['database']
        )

        if 'port' in config:
            config_url = config_url.set(port=config['port'])
        
        if 'sqlalchemy_url_query' in config:
            config_url = config_url.update_query_dict(config['sqlalchemy_url_query'])
        
        return (config_url)

    def create_sqlalchemy_engine(self) -> sqlalchemy.engine.Engine:
        """Return a new SQLAlchemy engine using the provided config.

        Developers can generally override just one of the following:
        `sqlalchemy_engine`, sqlalchemy_url`.

        Returns:
            A newly created SQLAlchemy engine object.
        """
        eng_prefix = "ep."
        eng_config = {f"{eng_prefix}url":self.sqlalchemy_url,f"{eng_prefix}echo":"False"}

        if self.config.get('sqlalchemy_eng_params'):
            for key, value in self.config['sqlalchemy_eng_params'].items():
                eng_config.update({f"{eng_prefix}{key}": value})

        return sqlalchemy.engine_from_config(eng_config, prefix=eng_prefix)

    def to_jsonschema_type(self, sql_type: sqlalchemy.types.TypeEngine) -> None:
        """Returns a JSON Schema equivalent for the given SQL type.

        Developers may optionally add custom logic before calling the default
        implementation inherited from the base class.
        """
        if self.config.get('hd_jsonschema_types',False):
            return self.hd_to_jsonschema_type(sql_type)
        else: 
            return self.org_to_jsonschema_type(sql_type)

    @staticmethod
    def org_to_jsonschema_type(sql_type: sqlalchemy.types.TypeEngine) -> dict:
        """Returns a JSON Schema equivalent for the given SQL type.

        Developers may optionally add custom logic before calling the default
        implementation inherited from the base class.
        """

        """
            Checks for the MSSQL type of NUMERIC 
                if scale = 0 it is typed as a INTEGER
                if scale != 0 it is typed as NUMBER
        """
        if str(sql_type).startswith("NUMERIC"):
            if str(sql_type).endswith(", 0)"):
                sql_type = "int"
            else: 
                sql_type = "number"
        
        if str(sql_type) in ["MONEY", "SMALLMONEY"]:
            sql_type = "number"
            
        return SQLConnector.to_jsonschema_type(sql_type)

    @staticmethod
    def hd_to_jsonschema_type(sql_type: sqlalchemy.types.TypeEngine) -> dict:
        # This is taken from to_jsonschema_type() in typing.py 
        if isinstance(sql_type, str):
            sql_type_name = sql_type
        elif isinstance(sql_type, sqlalchemy.types.TypeEngine):
            sql_type_name = type(sql_type).__name__
        elif isinstance(sql_type, type) and issubclass(
            sql_type, sqlalchemy.types.TypeEngine
        ):
            sql_type_name = sql_type.__name__
        else:
            raise ValueError("Expected `str` or a SQLAlchemy `TypeEngine` object or type.")

        # Add in the length of the 
        if sql_type_name in ['CHAR','NCHAR', 'VARCHAR','NVARCHAR']:
           maxLength:int = getattr(sql_type, 'length')

           if getattr(sql_type, 'length'):
            return {
                "type": ["string"],
                "maxLength": maxLength
            } 

        # This is a MSSQL only DataType
        # SQLA does the converion from 0,1
        # to Python True, False
        if sql_type_name == 'BIT':
            return{"type": ["boolean"]}

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
        if sql_type_name in ("NUMERIC", "DECMIMAL"):
            precision = getattr(sql_type, 'precision')
            scale = getattr(sql_type, 'scale')
            if scale == 0:
                    return {
                        "type": ["integer"],
                        "minimum": (-pow(10,precision))+1,
                        "maximum": (pow(10,precision))-1
                    }   
            else:
                scale_in_decimal = 1
                for i in range(scale):
                    scale_in_decimal = scale_in_decimal/10
                return {
                    "type": ["number"],
                    "minimum": (-pow(10,precision)+1)*scale_in_decimal,
                    "maximum": (pow(10,precision)-1)*scale_in_decimal
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

    
        return SQLConnector.to_jsonschema_type(sql_type)

    @staticmethod
    def to_sql_type(jsonschema_type: dict) -> sqlalchemy.types.TypeEngine:
        """Returns a JSON Schema equivalent for the given SQL type.

        Developers may optionally add custom logic before calling the default
        implementation inherited from the base class.
        """
        # Optionally, add custom logic before calling the parent SQLConnector method.
        # You may delete this method if overrides are not needed.
        return SQLConnector.to_sql_type(jsonschema_type)


# Custom class extends json.JSONEncoder
class CustomJSONEncoder(json.JSONEncoder):

    # Override default() method
    def default(self, obj):

        # Datetime to string
        if isinstance(obj, datetime):
            # Format datetime - `Fri, 21 Aug 2020 17:59:59 GMT`
            #obj = obj.strftime('%a, %d %b %Y %H:%M:%S GMT')
            obj = pendulum.instance(obj).isoformat()
            return obj

        # Default behavior for all other types
        return super().default(obj)


class mssqlStream(SQLStream):
    """Stream class for mssql streams."""

    connector_class = mssqlConnector

    def get_records(self, partition: Optional[dict]) -> Iterable[Dict[str, Any]]:
        """Return a generator of record-type dictionary objects.

        Developers may optionally add custom logic before calling the default
        implementation inherited from the base class.

        Args:
            partition: If provided, will read specifically from this data slice.

        Yields:
            One dict per record.
        """
        # Optionally, add custom logic instead of calling the super().
        # This is helpful if the source database provides batch-optimized record
        # retrieval.
        # If no overrides or optimizations are needed, you may delete this method.
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
                            (json.dumps(record, cls=CustomJSONEncoder) + "\n").encode() for record in chunk
                        )
                file_url = fs.geturl(filename)

            yield batch_config.encoding, [file_url]
