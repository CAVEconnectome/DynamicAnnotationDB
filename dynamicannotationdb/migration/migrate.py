import logging

from geoalchemy2.types import Geometry
from psycopg2.errors import DuplicateSchema
from sqlalchemy import MetaData, create_engine, ForeignKeyConstraint
from sqlalchemy.engine.url import make_url
from sqlalchemy.pool import NullPool
from sqlalchemy import MetaData, Table
from sqlalchemy.sql.ddl import AddConstraint
from sqlalchemy.schema import DropConstraint
from sqlalchemy.exc import ProgrammingError

from dynamicannotationdb.database import DynamicAnnotationDB
from dynamicannotationdb.models import AnnoMetadata
from dynamicannotationdb.schema import DynamicSchemaClient
from emannotationschemas.errors import UnknownAnnotationTypeException
from emannotationschemas.migrations.run import run_migration

logger = logging.getLogger()
logger.setLevel(logging.DEBUG)


# SQL commands
def alter_column_name(table_name: str, current_col_name: str, new_col_name: str) -> str:
    return f"ALTER TABLE {table_name} RENAME {current_col_name} TO {new_col_name}"


def add_column(table_name: str, column_spec: str) -> str:
    return f"ALTER TABLE {table_name} ADD IF NOT EXISTS {column_spec}"


def add_primary_key(table_name: str, column_name: str):
    return f"ALTER TABLE {table_name} add primary key({column_name})"


def add_index(table_name: str, column_name: str, is_spatial=False):
    if is_spatial:
        index_name = f"idx_{table_name}_{column_name}"
        column_index_type = (
            f"{table_name} USING GIST ({column_name} gist_geometry_ops_nd)"
        )
    else:
        index_name = f"ix_{table_name}_{column_name}"
        column_index_type = f"{table_name} ({column_name})"

    return f"CREATE INDEX IF NOT EXISTS {index_name} ON {column_index_type}"


def add_foreign_key(
    table_name: str,
    foreign_key_name: str,
    foreign_key_column: str,
    foreign_key_table: str,
    target_column: str,
):
    return f"""ALTER TABLE "{table_name}"
               ADD CONSTRAINT {foreign_key_name}
               FOREIGN KEY ("{foreign_key_column}") 
               REFERENCES "{foreign_key_table}" ("{target_column}");"""


class DynamicMigration:
    """Migrate schemas with new columns and handle index creation."""

    def __init__(
        self, sql_uri: str, target_db: str, schema_db: str = "schemas"
    ) -> None:
        self._base_uri = sql_uri.rpartition("/")[0]
        self.target_db_sql_uri = make_url(f"{self._base_uri}/{target_db}")
        self.schema_sql_uri = make_url(f"{self._base_uri}/{schema_db}")
        self.target_database, self.target_inspector = self.setup_inspector(
            self.target_db_sql_uri
        )
        self.schema_client = DynamicSchemaClient()

        temp_engine = create_engine(
            self._base_uri,
            poolclass=NullPool,
            isolation_level="AUTOCOMMIT",
            pool_pre_ping=True,
        )

        with temp_engine.connect() as connection:
            connection.execute("commit")
            database_exists = connection.execute(
                f"SELECT 1 FROM pg_catalog.pg_database WHERE datname = '{schema_db}'"
            )
            if not database_exists.fetchone():
                logging.warning(f"Cannot connect to {schema_db}, attempting to create")
                connection.execute(f"CREATE DATABASE {schema_db}")
        logging.info(f"{schema_db} created")
        temp_engine.dispose()

        try:
            logging.info("Running migrations")
            run_migration(str(self.schema_sql_uri))
        except DuplicateSchema as e:
            logging.warning(f"Error migrating schema database: {e}")

        self.schema_database, self.schema_inspector = self.setup_inspector(
            self.schema_sql_uri
        )

    def setup_inspector(self, sql_uri: str):
        database_client = DynamicAnnotationDB(sql_uri)
        database_inspector = database_client.inspector
        return database_client, database_inspector

    def get_table_info(self):
        target_tables = sorted(set(self.target_inspector.get_table_names()))
        schema_tables = sorted(set(self.schema_inspector.get_table_names()))
        return target_tables, schema_tables

    def _get_target_schema_types(self, schema_type: str):
        try:
            schema = self.schema_client.get_schema(schema_type)
        except UnknownAnnotationTypeException as e:
            logging.info(f"Table {schema_type} is not an em annotation schemas: {e}")
        return (
            self.target_database.cached_session.query(
                AnnoMetadata.table_name, AnnoMetadata.schema_type
            )
            .filter(AnnoMetadata.schema_type == schema_type)
            .all()
        )

    def _get_table_schema_type(self, table_name: str):
        schema_type = (
            self.target_database.cached_session.query(AnnoMetadata.schema_type)
            .filter(AnnoMetadata.table_name == table_name)
            .one()
        )
        return schema_type[0]

    def get_target_schema(self, table_name: str):
        return self.target_database.get_table_sql_metadata(table_name)

    def get_schema_from_migration(self, schema_table_name: str):
        return self.schema_database.get_table_sql_metadata(schema_table_name)

    def upgrade_table_from_schema(self, table_name: str, dry_run: bool = True):
        """Migrate a schema if the schema model is present in the database.
        If there are missing columns in the database it will add new
        columns.

        Parameters
        ----------
        table_name : str
            table to migrate.
        dry_run : bool
            return a map of columns to add, does not affect the database.
        """
        if table_name not in self.target_database._get_existing_table_names():
            raise f"{table_name} not found."

        db_table, model_table, columns_to_create = self.get_table_diff(table_name)
        ddl_client = self.target_database.engine.dialect.ddl_compiler(
            self.target_database.engine.dialect, None
        )

        migrations = {}
        for column in columns_to_create:
            model_column = model_table.c.get(column)

            col_spec = ddl_client.get_column_specification(model_column)

            sql = add_column(db_table.name, col_spec)
            sql = self.set_default_non_nullable(db_table, column, model_column, sql)
            col_to_migrate = f"{db_table.name}.{model_column.name}"
            logging.info(f"Adding column {col_to_migrate}")
            migrations[col_to_migrate] = sql

        # get missing table indexes
        index_sql_commands = self.get_missing_indexes(table_name)

        migration_map = {}

        if migrations:
            migration_map = {"Table": table_name, "Columns": migrations}

        if index_sql_commands:
            migration_map["Indexes"] = index_sql_commands

        if dry_run:
            logging.info(
                "Dry run mode. Set dry run to False to apply changes to the db."
            )
            return migration_map
        try:
            engine = self.target_database.engine
            with engine.connect() as conn:
                if migrations:
                    for command in migrations.values():
                        logging.info(f"Running command: {command}")
                        conn.execute(command)
                if index_sql_commands:
                    for index_name, sql_command in index_sql_commands.items():
                        logging.info(f"Creating index: {index_name}")
                        conn.execute(sql_command)

            self.target_database.base.metadata.reflect()
            return migration_map
        except Exception as e:
            self.target_database.cached_session.rollback()
            raise e

    def apply_cascade_option_to_tables(self, dry_run: bool = True):
        metadata = MetaData(bind=self.target_database.engine)
        metadata.reflect(bind=self.target_database.engine)
        fkey_mappings = []
        for table in metadata.tables:
            table_metadata = self.target_database.get_table_metadata(table)
            if table_metadata:
                table = metadata.tables[table]
                try:
                    fkey_mapping = self.add_cascade_delete_to_fkey(table, dry_run)
                    if fkey_mapping:
                        fkey_mappings.append(fkey_mapping)
                except Exception as error:
                    raise error
        if not fkey_mappings:
            logging.info("No tables to migrate fkey constraints")
            return None
        return fkey_mappings

    def add_cascade_delete_to_fkey(self, table: Table, dry_run: bool = True):
        table_name = table.name
        fkeys_to_drop = {}
        fkey_to_add = {}
        for fk in self.target_inspector.get_foreign_keys(table_name):
            # check if the foreign key has no 'ondelete' option
            if not fk["options"].get("ondelete"):
                # drop the foreign key constraint
                fkey = ForeignKeyConstraint(
                    [table.c[c] for c in fk["constrained_columns"]],
                    [fk["referred_table"] + "." + c for c in fk["referred_columns"]],
                    name=fk["name"],
                )
                drop_constraint = DropConstraint(fkey)
                fkeys_to_drop[fkey.name] = str(drop_constraint)

                # create a new foreign key constraint with the specified 'ondelete' option
                new_fkey = ForeignKeyConstraint(
                    [table.c[c] for c in fk["constrained_columns"]],
                    [fk["referred_table"] + "." + c for c in fk["referred_columns"]],
                    name=fk["name"],
                    ondelete="CASCADE",
                )
                add_constraint = AddConstraint(new_fkey)

                fkey_to_add[new_fkey.name] = str(add_constraint)

                if not dry_run:
                    with self.target_database.engine.begin() as conn:
                        conn.execute(drop_constraint)
                        conn.execute(add_constraint)
                        logging.info(f"Table {table_name} altered with CASCADE DELETE")
        return (
            {
                f"Table Name: {table_name}": {
                    "Fkeys to drop": fkeys_to_drop,
                    "Fkeys to add": fkey_to_add,
                }
            }
            if fkeys_to_drop or fkey_to_add
            else None
        )

    def upgrade_annotation_models(self, dry_run: bool = True):
        """Upgrades annotation models present in the database
        if underlying schemas have changed.

        Raises
        ------
        e
            SQL Error
        """
        tables = self.target_database._get_existing_table_names(filter_valid=True)
        migrations = []
        for table in tables:
            migration_map = self.upgrade_table_from_schema(table, dry_run)
            if migration_map:
                migrations.append(migration_map)
        return migrations

    def get_table_diff(self, table_name):
        target_model_schema = (
            self.target_database.cached_session.query(AnnoMetadata.schema_type)
            .filter(AnnoMetadata.table_name == table_name)
            .one()
        )
        schema = target_model_schema[0]

        db_cols = self.target_inspector.get_columns(table_name)
        schema_cols = self.schema_inspector.get_columns(schema)

        formatted_schema_columns = self._column_names(schema_cols)
        formatted_db_columns = self._column_names(db_cols)
        db_model = self.target_database.get_table_sql_metadata(table_name)
        schema_model = self.schema_database.get_table_sql_metadata(schema)

        columns_to_create = set(formatted_schema_columns) - set(formatted_db_columns)
        return db_model, schema_model, columns_to_create

    def set_default_non_nullable(self, db_table, column, model_column, sql):
        if not model_column.nullable:
            if column == "created":
                table_name = db_table.name
                creation_time = (
                    self.target_database.cached_session.query(AnnoMetadata.created)
                    .filter(AnnoMetadata.table_name == table_name)
                    .one()
                )
                sql += f" DEFAULT '{creation_time[0].strftime('%Y-%m-%d %H:%M:%S')}'"
            else:
                model_column.nullable = True
        return sql

    def extract_target_id(self, indexes: dict) -> dict:
        return {
            "reference_table": index.get("foreign_key_table")
            for index in indexes.values()
            if index.get("foreign_key_column") == "target_id"
        }

    def get_table_indexes(self, table_name: str, db: str = "target"):
        """Reflect current indexes, primary key(s) and foreign keys
         on given target table using SQLAlchemy inspector method.

        Args:
            table_name (str): target table to reflect

        Returns:
            dict: Map of reflected indices on given table.
        """
        inspector = getattr(self, f"{db}_inspector")
        try:
            pk_columns = inspector.get_pk_constraint(table_name)
            indexed_columns = inspector.get_indexes(table_name)
            foreign_keys = inspector.get_foreign_keys(table_name)
        except Exception as e:
            logging.error(f"No table named '{table_name}', error: {e}")
            return None

        index_map = {}
        if pk_columns:
            pkey_name = pk_columns.get("name").lower()
            pk_name = {"primary_key_name": pkey_name}
            if pk_name["primary_key_name"]:
                pk = {
                    "column_name": pk_columns["constrained_columns"][0],
                    "index_name": pkey_name,
                    "type": "primary_key",
                }
                index_map[pkey_name] = pk

        if indexed_columns:
            for index in indexed_columns:
                dialect_options = index.get("dialect_options", None)
                index_name = index["name"].lower()
                indx_map = {
                    "column_name": index["column_names"][0],
                    "index_name": index_name,
                }
                if dialect_options:
                    if "gist" in dialect_options.values():
                        indx_map.update(
                            {
                                "type": "spatial_index",
                                "dialect_options": index.get("dialect_options"),
                            }
                        )
                else:
                    indx_map.update({"type": "index", "dialect_options": None})

                index_map[index_name] = indx_map
        if foreign_keys:
            for foreign_key in foreign_keys:
                foreign_key_name = foreign_key["name"].lower()
                fk_data = {
                    "column_name": foreign_key["referred_columns"][0],
                    "type": "foreign_key",
                    "foreign_key_name": foreign_key_name,
                    "foreign_key_table": foreign_key["referred_table"],
                    "foreign_key_column": foreign_key["constrained_columns"][0],
                    "target_column": foreign_key["referred_columns"][0],
                }
                index_map[foreign_key_name] = fk_data
        return index_map

    def get_index_from_model(self, table_name: str, model):
        """Generate index mapping, primary key and foreign keys(s)
        from supplied SQLAlchemy model. Returns an index map.

        Args:
            model (SqlAlchemy Model): database model to reflect indices

        Returns:
            dict: Index map
        """
        model = model.__table__
        index_map = {}
        for column in model.columns:
            if column.primary_key:
                pk_index_name = f"{table_name}_pkey".lower()
                pk = {
                    "column_name": column.name,
                    "index_name": pk_index_name,
                    "type": "primary_key",
                }
                index_map[pk_index_name] = pk
            if column.index:
                index_name = f"ix_{table_name}_{column.name}"
                indx_map = {
                    "column_name": column.name,
                    "index_name": index_name,
                    "type": "index",
                    "dialect_options": None,
                }
                index_map[index_name] = indx_map
            if isinstance(column.type, Geometry):
                sptial_index_name = f"idx_{table_name}_{column.name}".lower()
                spatial_index_map = {
                    "column_name": column.name,
                    "index_name": sptial_index_name,
                    "type": "spatial_index",
                    "dialect_options": {"postgresql_using": "gist"},
                }
                index_map[sptial_index_name] = spatial_index_map
            if column.foreign_keys:
                metadata_obj = MetaData()
                metadata_obj.reflect(bind=self.target_database.engine)
                target_table = metadata_obj.tables.get(table_name)
                foreign_keys = list(target_table.foreign_keys)

                for foreign_key in foreign_keys:
                    (
                        target_table_name,
                        target_column,
                    ) = foreign_key.target_fullname.split(".")
                    foreign_key_name = foreign_key.name.lower()

                    foreign_key_map = {
                        "type": "foreign_key",
                        "column_name": foreign_key.constraint.column_keys[0],
                        "foreign_key_name": foreign_key_name,
                        "foreign_key_table": target_table_name,
                        "foreign_key_column": foreign_key.constraint.column_keys[0],
                        "target_column": target_column,
                    }
                    index_map[foreign_key_name] = foreign_key_map
        return index_map

    def drop_table_indexes(self, table_name: str):
        """Generate SQL command to drop all indexes and
        constraints on target table.

        Args:
            table_name (str): target table to drop constraints and indices
            engine (SQLAlchemy Engine instance): supplied SQLAlchemy engine

        Returns:
            bool: True if all constraints and indices are dropped
        """
        indices = self.get_table_indexes(table_name)
        if not indices:
            return f"No indices on '{table_name}' found."
        command = f"ALTER TABLE {table_name}"

        constraints_list = []
        for column_info in indices.values():
            if "foreign_key" in column_info["type"]:
                constraints_list.append(
                    f"{command} DROP CONSTRAINT IF EXISTS {column_info['foreign_key_name']}"
                )
            if "primary_key" in column_info["type"]:
                constraints_list.append(
                    f"{command} DROP CONSTRAINT IF EXISTS {column_info['index_name']}"
                )

        drop_constraint = f"{'; '.join(constraints_list)} CASCADE"
        command = f"{drop_constraint};"
        index_list = [
            col["index_name"] for col in indices.values() if "index" in col["type"]
        ]
        if index_list:
            drop_index = f"DROP INDEX {', '.join(index_list)}"
            command = f"{command} {drop_index};"
        try:
            engine = self.target_database.engine
            with engine.connect() as conn:
                conn.execute(command)
        except Exception as e:
            raise (e)
        return True

    def get_missing_indexes(self, table_name: str, model=None):
        """Add missing indexes by comparing current table and
        schema table db indexes. Will add missing indices from model to table.

        Args:
            table_name (str): target table to drop constraints and indices
            engine (SQLAlchemy Engine instance): supplied SQLAlchemy engine

        Returns:
            str: list of indices added to table
        """
        current_indexes = self.get_table_indexes(table_name, "target")
        table_schema_type = self._get_table_schema_type(table_name)
        table_metadata = self.extract_target_id(current_indexes)
        if not model:
            schema_model = self.get_schema_from_migration(table_schema_type)
            model = self.schema_client.create_annotation_model(
                f"ref_{table_name}", table_schema_type, table_metadata, True
            )
        model_indexes = self.get_index_from_model(table_name, model)
        missing_indexes = set(model_indexes) - set(current_indexes)

        existing_indexes = current_indexes.values()
        index_list = [index["column_name"] for index in existing_indexes]
        missing_indexes = [
            key
            for key, value in model_indexes.items()
            if value["column_name"] not in index_list
        ]

        commands = {}

        for index in missing_indexes:
            index_type = model_indexes[index]["type"]
            column_name = model_indexes[index]["column_name"]

            if index_type == "primary_key":
                command = add_primary_key(table_name, column_name)
            if index_type == "index":
                command = add_index(table_name, column_name, is_spatial=False)
            if index_type == "spatial_index":
                command = add_index(table_name, column_name, is_spatial=True)
            if index_type == "foreign_key":
                foreign_key_name = model_indexes[column_name]["foreign_key_name"]
                foreign_key_table = model_indexes[column_name]["foreign_key_table"]
                foreign_key_column = model_indexes[column_name]["foreign_key_column"]
                target_column = model_indexes[column_name]["target_column"]
                command = add_foreign_key(
                    table_name,
                    foreign_key_name,
                    foreign_key_column,
                    foreign_key_table,
                    target_column,
                )

                missing_indexes.append(foreign_key_name)
            index_key = f"{column_name}_{index_type}"

            commands[index_key] = command

        return commands

    @staticmethod
    def _column_names(tables):
        if hasattr(tables, "__table__"):
            table_columns = tables.__table__.columns
        elif hasattr(tables, "columns"):
            table_columns = tables.columns
        elif isinstance(tables, object):
            return [table.get("name") for table in tables]
        return {i.name for i in table_columns}
