"""SQL client handling.

This includes mssqlStream and mssqlConnector.
"""
from __future__ import annotations

import gzip
import json
import datetime

from decimal import Decimal
from uuid import uuid4
from typing import Any, Dict, Iterable, Optional, cast, Type, Union
from typing_extensions import TypeAlias
from dataclasses import dataclass, fields

import pendulum
import pyodbc
import sqlalchemy
from sqlalchemy.engine import Engine, URL
from sqlalchemy.engine.reflection import Inspector

from singer_sdk import SQLConnector, SQLStream, typing as th
from singer_sdk._singerlib import CatalogEntry, MetadataMapping, Schema, StreamMetadata, Metadata
from singer_sdk.helpers._batch import (
    BaseBatchFileEncoding,
    BatchConfig,
)
from singer_sdk.streams.core import lazy_chunked_generator


@dataclass
class mssqlMetadata(Metadata):
    """MSSQL metadata."""

    sql_datatype: sqlalchemy.types.TypeEngine | None = None


# AnyMetadata: TypeAlias = Union[Metadata, StreamMetadata, mssqlMetadata]

class mssqlMetadataMapping(MetadataMapping):
    """Meta data from database to Catalog"""
    
    @classmethod
    def get_standard_metadata(
        cls: Type[MetadataMapping],
        schema: dict[str, Any] | None = None,
        schema_name: str | None = None,
        key_properties: list[str] | None = None,
        valid_replication_keys: list[str] | None = None,
        replication_method: str | None = None,
        sql_datatype: dict | None = None,
    ) -> MetadataMapping:
        """Get default metadata for a stream.

        Args:
            schema: Stream schema.
            schema_name: Stream schema name.
            key_properties: Stream key properties.
            valid_replication_keys: Stream valid replication keys.
            replication_method: Stream replication method.

        Returns:
            Metadata mapping.
        """
        mapping = cls()
        root = StreamMetadata(
            table_key_properties=key_properties,
            forced_replication_method=replication_method,
            valid_replication_keys=valid_replication_keys,
        )

        if schema:
            root.inclusion = Metadata.InclusionType.AVAILABLE

            if schema_name:
                root.schema_name = schema_name
        
            for field_name in schema.get("properties", {}).keys():
                if key_properties and field_name in key_properties:
                    entry = mssqlMetadata(inclusion=Metadata.InclusionType.AUTOMATIC)
                elif valid_replication_keys and field_name in valid_replication_keys:
                    entry = mssqlMetadata(inclusion=Metadata.InclusionType.AUTOMATIC)
                else:
                    entry = mssqlMetadata(inclusion=Metadata.InclusionType.AVAILABLE)

                field_datatype = cast(sqlalchemy.types.TypeEngine,sql_datatype.get(field_name))
                datatype_attributes = ("length","scale","precision")

                sql_datatype_string = f"{type(field_datatype).__name__}("
                for attribute in datatype_attributes:
                    if hasattr(field_datatype, attribute):
                        if getattr(field_datatype, attribute):
                            sql_datatype_string += f"{attribute}={(getattr(field_datatype, attribute))}, "
                
                if sql_datatype_string.endswith(", "):
                     sql_datatype_string = sql_datatype_string[:-2]
                     
                sql_datatype_string += ")"
                entry.sql_datatype = sql_datatype_string
                # mapping[("properties", field_name)] = mssqlMetadata(sql_datatype=sql_datatyp_string)
                
                mapping[("properties", field_name)] = entry


        mapping[()] = root

        return mapping


class mssqlConnector(SQLConnector):
    """Connects to the mssql SQL source."""
    metadatmapping_class = mssqlMetadataMapping

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

    def create_sqlalchemy_engine(self) -> Engine:
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
        if sql_type_name in ("NUMERIC", "DECIMAL"):
            precision: int = getattr(sql_type, 'precision')
            scale: int = getattr(sql_type, 'scale')
            if scale == 0:
                    return {
                        "type": ["integer"],
                        "minimum": (-pow(10,precision))+1,
                        "maximum": (pow(10,precision))-1
                    }   
            else:
                maximum_as_number = str()
                minimum_as_number: str = '-'
                for i in range(precision):
                    if i == (precision-scale):
                        maximum_as_number += '.'
                    maximum_as_number += '9'
                minimum_as_number += maximum_as_number

                maximum_scientific_format:str = '9.'
                minimum_scientific_format:str = '-'
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

        return SQLConnector.to_jsonschema_type(sql_type)

    @staticmethod
    def to_sql_type(jsonschema_type: dict) -> sqlalchemy.types.TypeEngine:
        """Returns a JSON Schema equivalent for the given SQL type.

        Developers may optionally add custom logic before calling the default
        implementation inherited from the base class.
        """

        return SQLConnector.to_sql_type(jsonschema_type)

    def discover_catalog_entry(
        self,
        engine: Engine,
        inspected: Inspector,
        schema_name: str,
        table_name: str,
        is_view: bool,
    ) -> CatalogEntry:
        """Create `CatalogEntry` object for the given table or a view.

        Args:
            engine: SQLAlchemy engine
            inspected: SQLAlchemy inspector instance for engine
            schema_name: Schema name to inspect
            table_name: Name of the table or a view
            is_view: Flag whether this object is a view, returned by `get_object_names`

        Returns:
            `CatalogEntry` object for the given table or a view
        """
        # Initialize unique stream name
        unique_stream_id = self.get_fully_qualified_name(
            db_name=None,
            schema_name=schema_name,
            table_name=table_name,
            delimiter="-",
        )

        # Detect key properties
        possible_primary_keys: list[list[str]] = []
        pk_def = inspected.get_pk_constraint(table_name, schema=schema_name)
        if pk_def and "constrained_columns" in pk_def:
            possible_primary_keys.append(pk_def["constrained_columns"])

        possible_primary_keys.extend(
            index_def["column_names"]
            for index_def in inspected.get_indexes(table_name, schema=schema_name)
            if index_def.get("unique", False)
        )

        key_properties = next(iter(possible_primary_keys), None)

        # Initialize columns list
        table_schema = th.PropertiesList()
        for column_def in inspected.get_columns(table_name, schema=schema_name):
            column_name = column_def["name"]
            is_nullable = column_def.get("nullable", False)
            jsonschema_type: dict = self.to_jsonschema_type(
                cast(sqlalchemy.types.TypeEngine, column_def["type"])
            )
            table_schema.append(
                th.Property(
                    name=column_name,
                    wrapped=th.CustomType(jsonschema_type),
                    required=not is_nullable,
                )
            )
        schema = table_schema.to_dict()
        
        table_sql_datatype = dict()
        for column_def in inspected.get_columns(table_name, schema=schema_name):
            table_sql_datatype[str(column_def["name"])] = column_def["type"]

        # self.logger.info(table_sql_datatype)
        # Initialize available replication methods
        addl_replication_methods: list[str] = [""]  # By default an empty list.
        # Notes regarding replication methods:
        # - 'INCREMENTAL' replication must be enabled by the user by specifying
        #   a replication_key value.
        # - 'LOG_BASED' replication must be enabled by the developer, according
        #   to source-specific implementation capabilities.
        replication_method = next(reversed(["FULL_TABLE"] + addl_replication_methods))

        # Create the catalog entry object
        return CatalogEntry(
            tap_stream_id=unique_stream_id,
            stream=unique_stream_id,
            table=table_name,
            key_properties=key_properties,
            schema=Schema.from_dict(schema),
            is_view=is_view,
            replication_method=replication_method,
            metadata=self.metadatmapping_class.get_standard_metadata(
                schema_name=schema_name,
                schema=schema,
                replication_method=replication_method,
                key_properties=key_properties,
                valid_replication_keys=None,  # Must be defined by user
                sql_datatype=table_sql_datatype
            ),
            database=None,  # Expects single-database context
            row_count=None,
            stream_alias=None,
            replication_key=None,  # Must be defined by user
        )

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

    def get_records(self, partition: Optional[dict]) -> Iterable[Dict[str, Any]]:
        """Return a generator of record-type dictionary objects.

        Developers may optionally add custom logic before calling the default
        implementation inherited from the base class.

        Args:
            partition: If provided, will read specifically from this data slice.

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
