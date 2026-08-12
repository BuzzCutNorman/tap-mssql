"""SQL client handling.

This includes mssqlStream and mssqlConnector.
"""
from __future__ import annotations

import datetime
import gzip
import struct
import typing as t
from base64 import b64encode
from uuid import uuid4

import pyodbc
import sqlalchemy as sa
from azure import identity
from singer_sdk import SQLConnector, SQLStream
from singer_sdk.batch import BaseBatcher, lazy_chunked_generator
from singer_sdk.contrib.msgspec import serialize_jsonl
from sqlalchemy import event

if t.TYPE_CHECKING:
    from singer_sdk.helpers.types import Context
    from sqlalchemy.engine import Engine
    from sqlalchemy.engine.reflection import Inspector

# Connection option for access tokens, as defined in msodbcsql.h
SQL_COPT_SS_ACCESS_TOKEN = 1256
TOKEN_ENCODE_CODEC = "UTF-16-LE"
TOKEN_URL = "https://database.windows.net/"  # The token URL for any Azure SQL database

# from https://docs.sqlalchemy.org/en/20/core/engines.html#generating-dynamic-authentication-tokens
def make_provide_token(
    azure_credentials: identity.DefaultAzureCredential,
) -> t.Callable[[t.Any, t.Any, t.Any, t.Any], None]:
    """A function to provide an Azure EntraID access token for SQL Server connections.

    Args:
        azure_credentials: An instance of DefaultAzureCredential used to obtain the access token.

    Returns:
        A function that obtains access tokens for connection.
    """
    def provide_token(dialect, connection_record, cargs, cparams) -> None:
        """Creates a set of correct connection paramaters with EntraID token."""
        # remove the "Trusted_Connection" parameter that SQLAlchemy adds
        cargs[0] = cargs[0].replace(";Trusted_Connection=Yes", "")
        # create token credential
        token_bytes = azure_credentials.get_token(TOKEN_URL).token.encode(TOKEN_ENCODE_CODEC)
        token_struct = struct.pack(f"<I{len(token_bytes)}s", len(token_bytes), token_bytes)
        # apply it to keyword arguments
        cparams["attrs_before"] = {SQL_COPT_SS_ACCESS_TOKEN: token_struct}
    return provide_token

class MSSQLConnector(SQLConnector):
    """Connects to the mssql SQL source."""

    def __init__(
            self,
            config: dict | None = None,
            sqlalchemy_url: str | None = None
         ) -> None:
        """Class Default Init."""
        # If pyodbc given set pyodbc.pooling to False
        # This allows SQLA to manage to connection pool
        if config.get("driver_type") == "pyodbc":
            pyodbc.pooling = False

        if config.get("azure_access_tokens").lower() == "true":
            azure_credentials: identity.DefaultAzureCredential = identity.DefaultAzureCredential()
            event.listen(sa.Engine, "do_connect", make_provide_token(azure_credentials))

        super().__init__(config, sqlalchemy_url)

    def get_schema_names(
        self,
        engine: Engine,
        inspected: Inspector,
    ) -> list[str]:
        r"""Return the schema names to discover, honoring `filter_schemas`.

        Discovery otherwise reflects every schema in the database, which is both
        slow on large databases and outright fatal on some SQL Server instances:
        a schema named after a Windows login (e.g. `MYDOMAIN\First.Last`)
        contains a dot, and SQLAlchemy's MSSQL dialect reads a dotted schema as
        `database.schema` and issues a `USE [MYDOMAIN\First]`, failing with
        "Database ... does not exist" (error 911).

        Args:
            engine: SQLAlchemy engine
            inspected: SQLAlchemy inspector instance for engine

        Returns:
            List of schema names
        """
        filter_schemas = self.config.get("filter_schemas")
        if filter_schemas:
            return list(filter_schemas)

        return super().get_schema_names(engine, inspected)

    @staticmethod
    def _parse_filter_table(entry: str) -> tuple[str | None, str]:
        """Split a `filter_tables` entry into (schema, table).

        Two forms are accepted, so that one list can serve both this setting and a
        pipeline's stream-selection rules (e.g. via a YAML anchor):

        - `schema.table`     -> ("schema", "table")
        - `schema-table.*`   -> ("schema", "table")   Singer stream-id form, where
          the stream id joins schema and table with a hyphen and `.*` selects all
          of its properties.

        The trailing `.*` is what distinguishes the second form, so a schema name
        containing a hyphen is still safe in the first.

        Args:
            entry: A single filter_tables entry.

        Returns:
            Tuple of (schema name or None, table name).
        """
        entry = entry.strip()

        if entry.endswith(".*"):
            stream_id = entry[:-2]
            schema_name, separator, table_name = stream_id.partition("-")
            if separator:
                return schema_name, table_name
            return None, stream_id

        schema_name, _, table_name = entry.rpartition(".")
        return schema_name or None, table_name

    def discover_catalog_entries(
        self,
        *,
        exclude_schemas: t.Sequence[str] = (),
        reflect_indices: bool = True,
    ) -> list[dict]:
        """Discover catalog entries, honoring `filter_tables`.

        The default implementation bulk-reflects every table and view in each
        schema, then the pipeline discards all but the selected streams. On a
        large remote database that is dominated by round-trip latency: a Trimble
        Vista instance has ~4,200 objects in `dbo` and takes ~27 minutes to
        discover, of which ~13 seconds is CPU.

        When `filter_tables` is set, reflect only those objects instead, turning
        discovery cost into a function of what is actually wanted. Selection rules
        are applied downstream by the pipeline and are not visible to the tap, so
        this list is configured separately -- see `_parse_filter_table` for the
        entry forms that let a single list serve both.

        Args:
            exclude_schemas: A list of schema names to exclude from discovery.
            reflect_indices: Whether to reflect indices to detect potential
                primary keys.

        Returns:
            The discovered catalog entries as a list.
        """
        filter_tables = self.config.get("filter_tables")
        if not filter_tables:
            return super().discover_catalog_entries(
                exclude_schemas=exclude_schemas,
                reflect_indices=reflect_indices,
            )

        engine = self._engine
        inspected = sa.inspect(engine)
        default_schema = self.config.get("filter_schemas") or [None]
        view_names_by_schema: dict[str | None, set[str]] = {}
        result: list[dict] = []

        for qualified_name in filter_tables:
            schema_name, table_name = self._parse_filter_table(qualified_name)
            schema = schema_name or default_schema[0]

            if schema in exclude_schemas:
                continue

            if schema not in view_names_by_schema:
                view_names_by_schema[schema] = set(
                    inspected.get_view_names(schema=schema)
                )

            try:
                reflected_columns = inspected.get_columns(table_name, schema=schema)
                reflected_pk = inspected.get_pk_constraint(table_name, schema=schema)
                reflected_indices = (
                    inspected.get_indexes(table_name, schema=schema)
                    if reflect_indices
                    else []
                )
            except sa.exc.NoSuchTableError:
                # Not fatal: tenants of the same product legitimately differ (a
                # module they do not license, a table added in a later version).
                # Log loudly and keep going rather than failing the whole run.
                self.logger.warning(
                    "filter_tables: skipping '%s' -- not found in the database",
                    qualified_name,
                )
                continue

            if not reflected_columns:
                self.logger.warning(
                    "filter_tables: '%s' reflected zero columns; skipping",
                    qualified_name,
                )
                continue

            result.append(
                self.discover_catalog_entry(
                    engine,
                    inspected,
                    schema,
                    table_name,
                    table_name in view_names_by_schema[schema],
                    reflected_columns=reflected_columns,
                    reflected_pk=reflected_pk,
                    reflected_indices=reflected_indices,
                ).to_dict()
            )

        self.logger.info(
            "filter_tables: discovered %d of %d configured objects",
            len(result),
            len(filter_tables),
        )
        return result

    def get_sqlalchemy_url(self, config: dict[str, t.Any]) -> str:
        """Return the SQLAlchemy URL string.

        Args:
            config: A dictionary of settings from the tap or target config.

        Returns:
            The URL as a string.
        """
        url_drivername = f"{config.get('dialect')}+{config.get('driver_type')}"

        config_url = sa.URL.create(
            drivername=url_drivername,
            username=config.get("user"),
            password=config.get("password"),
            host=config.get("host"),
            database=config.get("database")
        )

        if "port" in config:
            config_url = config_url.set(port=config.get("port"))

        if "sqlalchemy_url_query" in config:
            config_url = config_url.update_query_dict(
                config.get("sqlalchemy_url_query")
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
            f"{eng_prefix}echo": "False",
            f"{eng_prefix}json_serializer": self.serialize_json,
            f"{eng_prefix}json_deserializer": self.deserialize_json,
        }

        if self.config.get("sqlalchemy_eng_params"):
            for key, value in self.config["sqlalchemy_eng_params"].items():
                eng_config.update({f"{eng_prefix}{key}": value})

        return sa.engine_from_config(eng_config, prefix=eng_prefix)

    def to_jsonschema_type(
            self,
            sql_type: (
                str  # noqa: ANN401
                | sa.types.TypeEngine
                | type[sa.types.TypeEngine]
                | t.Any
            ),
     ) -> None:
        """Returns a JSON Schema equivalent for the given SQL type.

        Developers may optionally add custom logic before calling the default
        implementation inherited from the base class.

        Args:
            sql_type: The SQL type as a string or as a TypeEngine.
                If a TypeEngine is provided, it may be provided as a class or
                a specific object instance.

        Returns:
            A compatible JSON Schema type definition.
        """
        if self.config.get("hd_jsonschema_types", False):
            return self.hd_to_jsonschema_type(sql_type)

        return self.org_to_jsonschema_type(sql_type)

    def org_to_jsonschema_type(
            self,
            sql_type: (
                str  # noqa: ANN401
                | sa.types.TypeEngine
                | type[sa.types.TypeEngine]
                | t.Any
            ),
     ) -> dict:
        """Returns a JSON Schema equivalent for the given SQL type.

        Developers may optionally add custom logic before calling the default
        implementation inherited from the base class.

        Args:
            sql_type: The SQL type as a string or as a TypeEngine.
                If a TypeEngine is provided, it may be provided as a class or
                a specific object instance.

        Returns:
            A compatible JSON Schema type definition.

        Checks for the MSSQL type of NUMERIC
            if scale = 0 it is typed as a INTEGER
            if scale != 0 it is typed as NUMBER
        """
        if str(sql_type).startswith("NUMERIC"):
            sql_type = "int" if str(sql_type).endswith(", 0)") else "number"

        if str(sql_type) in ["MONEY", "SMALLMONEY"]:
            sql_type = "number"

        # This is a MSSQL only DataType
        # SQLA does the converion from 0,1
        # to Python True, False
        if str(sql_type) in ["BIT"]:
            sql_type = "bool"

        if str(sql_type) in ["ROWVERSION", "TIMESTAMP"]:
            sql_type = "string"

        return SQLConnector.to_jsonschema_type(self=self,sql_type=sql_type)


    def hd_to_jsonschema_type(
            self,
            sql_type: (
                str  # noqa: ANN401
                | sa.types.TypeEngine
                | type[sa.types.TypeEngine]
                | t.Any
            ),
     ) -> dict:
        """Returns a JSON Schema equivalent for the given SQL type.

        Developers may optionally add custom logic before calling the default
        implementation inherited from the base class.

        Args:
            sql_type: The SQL type as a string or as a TypeEngine.
                If a TypeEngine is provided, it may be provided as a class or
                a specific object instance.

        Raises:
            ValueError: If the `sql_type` value is not of type `str` or `TypeEngine`.

        Returns:
            A compatible JSON Schema type definition.
        """
        # This is taken from to_jsonschema_type() in typing.py
        if isinstance(sql_type, str):
            sql_type_name = sql_type
        elif isinstance(sql_type, sa.types.TypeEngine):
            sql_type_name = type(sql_type).__name__
        elif isinstance(sql_type, type) and issubclass(
            sql_type, sa.types.TypeEngine
        ):
            sql_type_name = sql_type.__name__
        else:  # pragma: no cover
            msg = "Expected `str` or a SQLAlchemy `TypeEngine` object or type."
            # TODO: this should be a TypeError, but it's a breaking change.
            raise ValueError(msg)  # noqa: TRY004

        # Add in the length of the
        if sql_type_name in ["CHAR", "NCHAR", "VARCHAR", "NVARCHAR"]:
            maxLength: int = getattr(sql_type, "length")

            if getattr(sql_type, "length"):
                return {
                    "type": ["string"],
                    "maxLength": maxLength
                }

        if sql_type_name == "TIME":
            return {
                "type": ["string"],
                "format": "time"
            }

        if sql_type_name == "UNIQUEIDENTIFIER":
            return {
                "type": ["string"],
                "format": "uuid"
            }

        if sql_type_name == "XML":
            return {
                "type": ["string"],
                "contentMediaType": "application/xml",
            }

        if sql_type_name in ["BINARY", "IMAGE", "VARBINARY"]:
            maxLength: int = getattr(sql_type, "length")
            if getattr(sql_type, "length"):
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

        if sql_type_name in ["ROWVERSION", "TIMESTAMP"]:
            return {
                "type": ["string"],
                "contentEncoding": "base64",
                "maxLength": 12
            }

        # This is a MSSQL only DataType
        # SQLA does the converion from 0,1
        # to Python True, False
        if sql_type_name == "BIT":
            return {"type": ["boolean"]}

        # This is a MSSQL only DataType
        if sql_type_name == "TINYINT":
            return {
                "type": ["integer"],
                "minimum": 0,
                "maximum": 255
            }

        if sql_type_name == "SMALLINT":
            return {
                "type": ["integer"],
                "minimum": -32768,
                "maximum": 32767
            }

        if sql_type_name == "INTEGER":
            return {
                "type": ["integer"],
                "minimum": -2147483648,
                "maximum": 2147483647
            }

        if sql_type_name == "BIGINT":
            return {
                "type": ["integer"],
                "minimum": -9223372036854775808,
                "maximum": 9223372036854775807
            }

        # Checks for the MSSQL type of NUMERIC and DECIMAL
        #     if scale = 0 it is typed as a INTEGER
        #     if scale != 0 it is typed as NUMBER
        if sql_type_name in ("NUMERIC", "DECIMAL"):
            precision: int = getattr(sql_type, "precision")
            scale: int = getattr(sql_type, "scale")
            if scale == 0:
                return {
                    "type": ["integer"],
                    "minimum": (-pow(10, precision))+1,
                    "maximum": (pow(10, precision))-1
                }

            maximum_as_number = str()
            minimum_as_number: str = "-"
            for i in range(precision):
                if i == (precision-scale):
                    maximum_as_number += "."
                maximum_as_number += "9"
            minimum_as_number += maximum_as_number

            maximum_scientific_format: str = "9."
            minimum_scientific_format: str = "-"
            for _ in range(scale):
                maximum_scientific_format += "9"
            maximum_scientific_format += f"e+{precision}"
            minimum_scientific_format += maximum_scientific_format

            if "e+" not in str(float(maximum_as_number))\
                and "1" not in str(float(maximum_as_number)):
                return {
                    "type": ["number"],
                    "minimum": float(minimum_as_number),
                    "maximum": float(maximum_as_number)
                }

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

        return SQLConnector.to_jsonschema_type(self=self,sql_type=sql_type)

    @staticmethod
    def to_sql_type(jsonschema_type: dict) -> sa.types.TypeEngine:
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

    def get_table_columns(
        self,
        full_table_name: str | FullyQualifiedName,
        column_names: list[str] | None = None,
    ) -> dict[str, sa.Column]:
        """Return table columns, trusting catalog for columns inspector omits.

        When inspector.get_columns() omits columns (e.g. MSSQL views over
        table-valued functions in SQLAlchemy 2.x), we still include them if
        they are in column_names — trust the catalog over live reflection.
        """
        result = super().get_table_columns(full_table_name, column_names)

        if not column_names:
            return result

        inspector_by_key = {
            col_name.casefold(): (col_name, col)
            for col_name, col in result.items()
        }

        columns_dict: dict[str, sa.Column] = {}
        for col_name in column_names:
            key = col_name.casefold()
            if key in inspector_by_key:
                orig_name, col = inspector_by_key[key]
                columns_dict[orig_name] = col
            else:
                columns_dict[col_name] = sa.Column(
                    col_name, sa.Text(), nullable=True
                )

        return columns_dict


class JSONLinesBatcher(BaseBatcher):
    """JSON Lines Record Batcher."""

    def get_batches(
        self,
        records: t.Iterator[dict],
    ) -> t.Iterator[list[str]]:
        """Yield manifest of batches.

        Args:
            records: The records to batch.

        Yields:
            A list of file paths (called a manifest).
        """
        sync_id = f"{self.tap_name}--{self.stream_name}-{uuid4()}"
        prefix = self.batch_config.storage.prefix or ""

        for i, chunk in enumerate(
            lazy_chunked_generator(
                records,
                self.batch_config.batch_size,
            ),
            start=1,
        ):
            filename = f"{prefix}{sync_id}-{i}.json.gz"
            with self.batch_config.storage.fs(create=True) as fs:
                # TODO: Determine compression from config.
                with fs.open(filename, "wb") as f, gzip.GzipFile(
                    fileobj=f,
                    mode="wb",
                ) as gz:
                    gz.writelines(
                        serialize_jsonl(record) for record in chunk
                    )
                file_url = fs.geturl(filename)
            yield [file_url]


class MSSQLStream(SQLStream):
    """Stream class for mssql streams."""

    connector_class = MSSQLConnector

    supports_nulls_first: bool = False
    """Whether the database supports the NULLS FIRST/LAST syntax."""

    def post_process(
        self,
        row: dict,
        context: dict | None = None,  # noqa: ARG002
    ) -> dict | None:
        """As needed, append or transform raw data to match expected structure.

        Optional. This method gives developers an opportunity to "clean up" the results
        prior to returning records to the downstream tap - for instance: cleaning,
        renaming, or appending properties to the raw record result returned from the
        API.

        Developers may also return `None` from this method to filter out
        invalid or not-applicable records from the stream.

        Args:
            row: Individual record in the stream.
            context: Stream partition or context dictionary.

        Returns:
            The resulting record dict, or `None` if the record should be excluded.
        """
        # We change the name to record so when the change breaking
        # change from row to record is done in SDK 1.0 the edits
        # to accomidate the swithc will be two
        record: dict = row

        # Get the Stream Properties Dictornary from the Schema
        properties: dict = self.schema.get("properties")

        for key, value in record.items():
            if value is not None:
                # Get the Item/Column property
                property_schema: dict = properties.get(key)
                # Date in ISO format
                if isinstance(value, datetime.date):
                    record.update({key: value.isoformat()})
                # Encode base64 binary fields in the record
                if property_schema.get("contentEncoding") == "base64":
                    record.update({key: b64encode(value).decode()})

        return record

    def get_records(self, context: Context  | None) -> t.Iterable[dict[str, t.Any]]:
        """Return a generator of record-type dictionary objects.

        If the stream has a replication_key value defined, records will be
        sorted by the incremental key. If the stream also has an available
        starting bookmark, the records will be filtered for values greater
        than or equal to the bookmark value.

        Args:
            context: If partition context is provided, will read specifically
                from this data slice.

        Yields:
            One dict per record.

        Raises:
            NotImplementedError: If partition is passed in context and the
                stream does not support partitioning.
        """
        if context:
            msg = f"Stream '{self.name}' does not support partitioning."
            raise NotImplementedError(msg)

        selected_column_names = self.get_selected_schema()["properties"].keys()
        table = self.connector.get_table(
            full_table_name=self.fully_qualified_name,
            column_names=selected_column_names,
        )
        query = table.select()

        if self.replication_key:
            replication_key_col = table.columns[self.replication_key]
            order_by = (
                sa.nulls_first(replication_key_col.asc())
                if self.supports_nulls_first
                else replication_key_col.asc()
            )
            query = query.order_by(order_by)
            # # remove all below in final #
            # self.logger.info('\n')
            # self.logger.info(f"The replication_key_col SQLA type: {replication_key_col.type}")
            # self.logger.info(' ')
            # self.logger.info(f"The replication_key_col python type: {replication_key_col.type.python_type} this is type {type(replication_key_col.type.python_type)}")
            # self.logger.info(' ')
            # self.logger.info(f"Is the a replication_key_col python type datetime or date: {(replication_key_col.type.python_type in (datetime.datetime, datetime.date))}")
            # self.logger.info('\n')
            # # remove all to here in final #
            if replication_key_col.type.python_type in (
                datetime.datetime,
                datetime.date
            ):
                start_val = self.get_starting_timestamp(context)
            else:
                start_val = self.get_starting_replication_key_value(context)

            if start_val:
                query = query.where(replication_key_col >= start_val)

        if self.ABORT_AT_RECORD_COUNT is not None:
            # Limit record count to one greater than the abort threshold.
            # This ensures
            # `MaxRecordsLimitException` exception is properly raised by caller
            # `Stream._sync_records()` if more records are available than can
            #  be processed.
            query = query.limit(self.ABORT_AT_RECORD_COUNT + 1)

        # # remove all below in final #
        # self.logger.info('\n')
        # self.logger.info(f"Passed context is: {context}")
        # self.logger.info(' ')
        # self.logger.info(f"tap_state is: {self.tap_state}")
        # self.logger.info(' ')
        # self.logger.info(f"stream_state is: {self.stream_state}")
        # self.logger.info(' ')
        # self.logger.info(f"get_context_state is: {self.get_context_state(context)}")
        # self.logger.info(' ')
        # self.logger.info(f"replication_key is type : {type(self.replication_key)} has value: {self.replication_key}")
        # self.logger.info(' ')
        # if self.replication_key:
        #     self.logger.info(f"replication_key_col type: {type(replication_key_col)}, replication_key_col type: {type(replication_key_col)}")
        #     self.logger.info(' ')
        #     self.logger.info(f"get_starting_replication_key_value is: {self.get_starting_replication_key_value(context)}")
        #     self.logger.info(' ')
        #     self.logger.info(f"start_val type: {type(start_val)}, start_val type: {type(start_val)}")
        #     self.logger.info(' ')
        # self.logger.info(query)
        # self.logger.info('\n')
        # # remove all to here in final #

        with self.connector._connect() as conn:  # noqa: SLF001
            for record in conn.execute(query).mappings():
                transformed_record = self.post_process(dict(record))
                if transformed_record is None:
                    # Record filtered out during post_process()
                    continue
                yield transformed_record

