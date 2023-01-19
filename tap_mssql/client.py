"""SQL client handling.

This includes mssqlStream and mssqlConnector.
"""
from __future__ import annotations

import gzip
import json
from datetime import datetime
from uuid import uuid4
from typing import Any, Dict, Iterable, Optional, cast, Type

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
                    entry = Metadata(inclusion=Metadata.InclusionType.AUTOMATIC)
                elif valid_replication_keys and field_name in valid_replication_keys:
                    entry = Metadata(inclusion=Metadata.InclusionType.AUTOMATIC)
                else:
                    entry = Metadata(inclusion=Metadata.InclusionType.AVAILABLE)

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

    @staticmethod
    def to_jsonschema_type(sql_type: sqlalchemy.types.TypeEngine) -> dict:
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
    def to_sql_type(jsonschema_type: dict) -> sqlalchemy.types.TypeEngine:
        """Returns a JSON Schema equivalent for the given SQL type.

        Developers may optionally add custom logic before calling the default
        implementation inherited from the base class.
        """
        # Optionally, add custom logic before calling the parent SQLConnector method.
        # You may delete this method if overrides are not needed.
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
            ),
            database=None,  # Expects single-database context
            row_count=None,
            stream_alias=None,
            replication_key=None,  # Must be defined by user
        )

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
