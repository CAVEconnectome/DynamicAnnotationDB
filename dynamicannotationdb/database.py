import logging
from typing import List

from sqlalchemy import MetaData, create_engine, inspect, func
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.ext.declarative.api import DeclarativeMeta
from sqlalchemy.orm import Session, scoped_session, sessionmaker

from .errors import TableAlreadyExists, TableNameNotFound
from .models import AnnoMetadata, SegmentationMetadata, Base


class DynamicAnnotationDB:
    def __init__(self, sql_url: str) -> None:

        self._cached_session = None
        self._cached_tables = {}
        self._engine = create_engine(
            sql_url, pool_recycle=3600, pool_size=20, max_overflow=50
        )
        self.base = Base
        self.base.metadata.bind = self._engine

        table_objects = [
            AnnoMetadata.__tablename__,
            SegmentationMetadata.__tablename__,
        ]
        for table in table_objects:
            if not self.engine.dialect.has_table(self.engine, table):
                self.base.metadata.tables[table].create(bind=self.engine)

        self.mapped_base = automap_base()
        self.mapped_base.prepare(self.engine, reflect=True)

        self.session = scoped_session(
            sessionmaker(bind=self.engine, autocommit=False, autoflush=False)
        )
        self._inspector = inspect(self.engine)

        self._cached_session = None
        self._cached_tables = {}

    @property
    def inspector(self):
        return self._inspector

    @property
    def engine(self):
        return self._engine

    @property
    def cached_session(self) -> Session:
        if self._cached_session is None:
            self._cached_session = self.session()
        return self._cached_session

    def commit_session(self):
        try:
            self.cached_session.commit()
        except Exception as e:
            self.cached_session.rollback()
            logging.exception(f"SQL Error: {e}")
            raise e
        finally:
            self.cached_session.close()
        self._cached_session = None

    def get_table_sql_metadata(self, table_name: str):
        self.base.metadata.reflect(bind=self.engine)
        return self.base.metadata.tables[table_name]

    def get_table_metadata(self, table_name: str, filter_col: str = None):
        data = getattr(AnnoMetadata, filter_col) if filter_col else AnnoMetadata
        query = self.cached_session.query(data).filter(
            AnnoMetadata.table_name == table_name
        )
        result = query.one()
        if hasattr(result, "__dict__"):
            return self.get_automap_items(result)
        else:
            return result[0]

    def get_annotation_table_size(self, table_name: str) -> int:
        """Get the number of annotations in a table

        Parameters
        ----------
        table_name : str
            name of table contained within the aligned_volume database

        Returns
        -------
        int
            number of annotations
        """
        Model = self.cached_table(table_name)
        return self.cached_session.query(Model).count()

    def get_max_id_value(self, table_name: str) -> int:
        model = self.cached_table(table_name)
        return self.cached_session.query(func.max(model.id)).scalar()

    def get_min_id_value(self, table_name: str) -> int:
        model = self.cached_table(table_name)
        return self.cached_session.query(func.min(model.id)).scalar()

    def get_table_row_count(self, table_name: str, filter_valid: bool = False) -> int:
        model = self.cached_table(table_name)
        if filter_valid:
            row_count = (
                self.cached_session.query(func.count(model.id))
                    .filter(model.valid is True)
                    .scalar()
            )
        else:
            row_count = self.cached_session.query(func.count(model.id)).scalar()
        return row_count

    @staticmethod
    def get_automap_items(result):
        return {k: v for (k, v) in result.__dict__.items() if k != "_sa_instance_state"}

    def drop_table(self, table_name: str) -> bool:
        """Drop a table, actually removes it from the database
        along with segmentation tables associated with it

        Parameters
        ----------
        table_name : str
            name of table to drop

        Returns
        -------
        bool
            whether drop was successful
        """
        table = self.base.metadata.tables.get(table_name)
        if table:
            logging.info(f"Deleting {table_name} table")
            self.base.metadata.drop_all(self._engine, [table], checkfirst=True)
            if self._is_cached(table):
                del self._cached_tables[table]
            return True
        return False

    def _check_table_is_unique(self, table_name):
        existing_tables = self._get_existing_table_names()
        if table_name in existing_tables:
            raise TableAlreadyExists(
                f"Table creation failed: {table_name} already exists"
            )
        return existing_tables

    def _get_existing_table_names(self) -> List[str]:
        """Collects table_names keys of existing tables

        Returns
        -------
        list
            List of table_names
        """
        metadata = self.cached_session.query(AnnoMetadata).all()
        return [m.table_name for m in metadata]

    def _get_model_from_table_name(self, table_name: str) -> DeclarativeMeta:
        return self.mapped_base.classes[table_name]

    def _get_model_columns(self, table_name: str) -> List[tuple]:
        """Return list of column names and types of a given table

        Parameters
        ----------
        table_name : str
            Table name in database

        Returns
        -------
        list
            column names and types
        """
        db_columns = self.inspector.get_columns(table_name)
        if not db_columns:
            raise TableNameNotFound(
                f"Error: No table name exists with name {table_name}."
            )
        return [(column["name"], column["type"]) for column in db_columns]

    def cached_table(self, table_name: str) -> DeclarativeMeta:
        """Returns cached table 'DeclarativeMeta' callable for querying.

        Parameters
        ----------
        table_name : str
            Table name in database
        Returns
        -------
        DeclarativeMeta
            SQLAlchemy callable.
        """
        try:
            self._load_table(table_name)
            return self._cached_tables[table_name]
        except KeyError as error:
            raise TableNameNotFound(table_name) from error

    def _load_table(self, table_name: str):
        """Load existing table into cached lookup dict instance

        Parameters
        ----------
        table_name : str
            Table name to be loaded from existing database tables

        Returns
        -------
        bool
            Returns True if table exists and is loaded into cached table dict.
        """
        if self._is_cached(table_name):
            return True

        try:
            self._cached_tables[table_name] = self._get_model_from_table_name(
                table_name
            )
            return True
        except KeyError as key_error:
            if table_name in self._get_existing_table_names():
                logging.error(f"Could not load table: {key_error}")
            return False

    def _is_cached(self, table_name: str) -> bool:
        """Check if table is loaded into cached instance dict of tables

        Parameters
        ----------
        table_name : str
            Name of table to check if loaded

        Returns
        -------
        bool
            True if table is loaded else False.
        """

        return table_name in self._cached_tables

