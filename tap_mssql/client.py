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
            config_url.set(port=config['port'])
        
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
        return sqlalchemy.create_engine(self.sqlalchemy_url, echo=False)

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
               sql_type = "INTEGER"
            else: 
               sql_type = "NUMBER"
        
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