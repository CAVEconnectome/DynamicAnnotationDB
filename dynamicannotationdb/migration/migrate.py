import logging
from collections import namedtuple

from dynamicannotationdb.database import DynamicAnnotationDB
from dynamicannotationdb.models import AnnoMetadata
from dynamicannotationdb.schema import DynamicSchemaClient
from emannotationschemas.errors import UnknownAnnotationTypeException
from emannotationschemas.migrations.run import run_migration
from geoalchemy2.types import Geometry
from psycopg2.errors import DuplicateSchema
from sqlalchemy import MetaData, event, create_engine
from sqlalchemy.engine.url import make_url
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.schema import CreateSchema
from sqlalchemy.exc import OperationalError

# migration tables live in separate schema in database.

migration_metadata = MetaData(schema="schemas")
# bind migration metadata to declarative base
MigrationBase = declarative_base(metadata=migration_metadata)


InspectorResults = namedtuple(
    "InspectorResults", ["schema_only", "target_only", "common", "diff"]
)

# SQL commands
def alter_column_name(table_name: str, current_col_name: str, new_col_name: str) -> str:
    return f"ALTER TABLE {table_name} RENAME {current_col_name} TO {new_col_name}"


def add_column(table_name: str, column_spec: str) -> str:
    return f"ALTER TABLE {table_name} ADD {column_spec}"


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
        self.target_database, self.target_inspector = self.setup_inspector(target_db)

        try:
            self.schema_database, self.schema_inspector = self.setup_inspector(
                schema_db
            )
        except OperationalError as e:
            logging.warning(f"Cannot connect to {schema_db}, attempting to create")
            default_sql_uri = make_url(f"{self._base_uri}/postgres")
            engine = create_engine(default_sql_uri)

            with engine.connect() as conn:
                conn.execute("COMMIT")
                conn.execute(f"CREATE DATABASE {schema_db}")

            logging.info(f"{schema_db} created")

        self.schema_database, self.schema_inspector = self.setup_inspector(schema_db)

        try:
            if not self.schema_database.engine.dialect.has_schema(
                self.schema_database.engine, schema=schema_db
            ):

                event.listen(
                    self.schema_database.base.metadata,
                    "before_create",
                    CreateSchema(schema_db),
                )
                logging.info(f"Database schema {schema_db} created.")

                self.schema_database.base.metadata.create_all(
                    self.schema_database.engine, checkfirst=True
                )
                logging.info("Running migrations")
                run_migration(str(self.schema_database.engine.url))
        except DuplicateSchema as e:
            logging.warning(f"Schema {schema_db} already exists: {e}")

        self.schema_client = DynamicSchemaClient()

    def setup_inspector(self, database: str):
        sql_uri = make_url(f"{self._base_uri}/{database}")
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
        Raises
        ------
        NotImplementedError
            _description_
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
            col_to_migrate = f"{model_table.name}.{model_column.name}"
            logging.info(f"Adding column {col_to_migrate}")
            migrations[col_to_migrate] = sql

        if dry_run:
            return migrations
        try:
            engine = self.target_database.engine
            with engine.connection() as conn:
                conn.execute(sql)
            return migrations
        except Exception as e:
            self.target_database.cached_session.rollback()
            raise e

    def upgrade_annotation_models(self, dry_run: bool = True):
        """Upgrades annotation models present in the database
        if underlying schemas have changed.

        Raises
        ------
        e
            SQL Error
        """
        tables = self.target_database._get_existing_table_names()
        migrations = []
        for table in tables:
            migration_map = self.upgrade_table_from_schema(table, dry_run)
            migrations.append(migration_map)
        return migrations

    def get_table_diff(self, table_name):

        target_model_schema = (
            self.target_database.cached_session.query(AnnoMetadata.schema_type)
            .filter(AnnoMetadata.table_name == table_name)
            .one()
        )
        schema = target_model_schema[0]

        db_table = self.target_database.get_table_sql_metadata(table_name)
        model_table = self.schema_database.get_table_sql_metadata(schema)

        model_columns = self._column_names(model_table)
        db_columns = self._column_names(db_table)

        columns_to_create = model_columns - db_columns
        return db_table, model_table, columns_to_create

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

    def get_table_indexes(self, table_name: str):
        """Reflect current indexes, primary key(s) and foreign keys
         on given target table using SQLAlchemy inspector method.

        Args:
            table_name (str): target table to reflect

        Returns:
            dict: Map of reflected indices on given table.
        """

        try:
            pk_columns = self.target_inspector.get_pk_constraint(table_name)
            indexed_columns = self.target_inspector.get_indexes(table_name)
            foreign_keys = self.target_inspector.get_foreign_keys(table_name)
        except Exception as e:
            logging.error(f"No table named '{table_name}', error: {e}")
            return None
        index_map = {}
        if pk_columns:
            pk_name = {"primary_key_name": pk_columns.get("name")}
            if pk_name["primary_key_name"]:
                pk = {"index_name": f"{pk_columns['name']}", "type": "primary_key"}
                index_map[pk_columns["constrained_columns"][0]] = pk

        if indexed_columns:
            for index in indexed_columns:
                dialect_options = index.get("dialect_options", None)

                indx_map = {"index_name": index["name"]}
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

                index_map[index["column_names"][0]] = indx_map
        if foreign_keys:
            for foreign_key in foreign_keys:
                foreign_key_name = foreign_key["name"]
                fk_data = {
                    "type": "foreign_key",
                    "foreign_key_name": foreign_key_name,
                    "foreign_key_table": foreign_key["referred_table"],
                    "foreign_key_column": foreign_key["constrained_columns"][0],
                    "target_column": foreign_key["referred_columns"][0],
                }
                index_map[foreign_key_name] = fk_data
        return index_map

    def get_index_from_model(self, model):
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
                pk = {"index_name": f"{model.name}_pkey", "type": "primary_key"}
                index_map[column.name] = pk
            if column.index:
                indx_map = {
                    "index_name": f"ix_{model.name}_{column.name}",
                    "type": "index",
                    "dialect_options": None,
                }
                index_map[column.name] = indx_map
            if isinstance(column.type, Geometry):
                spatial_index_map = {
                    "index_name": f"idx_{model.name}_{column.name}",
                    "type": "spatial_index",
                    "dialect_options": {"postgresql_using": "gist"},
                }
                index_map[column.name] = spatial_index_map
            if column.foreign_keys:
                metadata_obj = MetaData()
                metadata_obj.reflect(bind=self.target_database.engine)

                foreign_keys = list(column.foreign_keys)
                for foreign_key in foreign_keys:
                    (
                        target_table_name,
                        target_column,
                    ) = foreign_key.target_fullname.split(".")
                    foreign_key_name = f"{target_table_name}_{target_column}_fkey"
                    foreign_key_map = {
                        "type": "foreign_key",
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

    def add_indexes_sql_commands(self, table_name: str, model):
        """Add missing indexes by comparing reflected table and
        model indices. Will add missing indices from model to table.

        Args:
            table_name (str): target table to drop constraints and indices
            engine (SQLAlchemy Engine instance): supplied SQLAlchemy engine

        Returns:
            str: list of indices added to table
        """
        current_indices = self.get_table_indexes(table_name)
        model_indices = self.get_index_from_model(model)
        missing_indices = set(model_indices) - set(current_indices)
        commands = []
        for column_name in missing_indices:
            index_type = model_indices[column_name]["type"]
            if index_type == "primary_key":
                command = add_primary_key(table_name, column_name)
            if index_type == "index":
                command = add_index(table_name, column_name, is_spatial=False)
            if index_type == "spatial_index":
                command = add_index(table_name, column_name, is_spatial=True)
            if index_type == "foreign_key":
                foreign_key_name = model_indices[column_name]["foreign_key_name"]
                foreign_key_table = model_indices[column_name]["foreign_key_table"]
                foreign_key_column = model_indices[column_name]["foreign_key_column"]
                target_column = model_indices[column_name]["target_column"]
                command = add_foreign_key(
                    table_name,
                    foreign_key_name,
                    foreign_key_column,
                    foreign_key_table,
                    target_column,
                )

                missing_indices.add(foreign_key_name)
            commands.append(command)
        return commands

    @staticmethod
    def _column_names(table):
        if hasattr(table, "__table__"):
            table_columns = table.__table__.columns
        else:
            table_columns = table.columns

        return {i.name for i in table_columns}
