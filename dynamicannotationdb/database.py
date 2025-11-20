import logging
from contextlib import contextmanager
from typing import List
import datetime

from sqlalchemy import create_engine, func, inspect, or_
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.ext.declarative.api import DeclarativeMeta
from sqlalchemy.orm import Session, scoped_session, sessionmaker
from sqlalchemy.orm.exc import NoResultFound
from sqlalchemy.schema import MetaData
from sqlalchemy.sql.schema import Table

from .errors import TableAlreadyExists, TableNameNotFound, TableNotInMetadata
from .models import AnnoMetadata, Base, SegmentationMetadata, AnalysisView
from .schema import DynamicSchemaClient


class DynamicAnnotationDB:
    def __init__(self, sql_url: str, pool_size=5, max_overflow=5) -> None:
        self._cached_session = None
        self._cached_tables = {}
        self._engine = create_engine(
            sql_url, pool_recycle=3600, pool_size=pool_size, max_overflow=max_overflow
        )
        self.base = Base
        self.base.metadata.bind = self._engine
        self.base.metadata.create_all(
            tables=[AnnoMetadata.__table__, SegmentationMetadata.__table__],
            checkfirst=True,
        )

        self.session = scoped_session(
            sessionmaker(bind=self.engine, autocommit=False, autoflush=False)
        )
        self.schema_client = DynamicSchemaClient()

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

    @contextmanager
    def session_scope(self):
        session = None
        try:
            # Validate and recreate session if needed
            if self._cached_session is None or not self._is_session_valid(self._cached_session):
                if self._cached_session is not None:
                    try:
                        self._cached_session.close()
                    except Exception:
                        pass
                self._cached_session = None
            
            session = self.cached_session
            yield session
        except Exception as e:
            if session is not None:
                try:
                    session.rollback()
                except Exception:
                    pass
            logging.exception(f"SQL Error: {e}")
            raise e
        finally:
            # Don't close the session - let connection pool manage it
            # The session will be reused for subsequent requests
            pass

    def _is_session_valid(self, session: Session) -> bool:
        """
        Check if a SQLAlchemy session is valid and can be used.

        Args:
            session (Session): The SQLAlchemy session to validate.

        Returns:
            bool: True if the session is valid, False otherwise.
        """
        if session is None:
            return False
        try:
            # Check if session is bound and connection is alive
            if not hasattr(session, 'bind') or session.bind is None:
                return False
            # Try a simple query to test the connection
            session.get_bind().execute("SELECT 1")
            return True
        except Exception:
            return False

    def commit_session(self):
        try:
            self.cached_session.commit()
        except Exception as e:
            self.cached_session.rollback()
            logging.exception(f"SQL Error: {e}")
            raise e
        # Don't close or reset the session - keep it for reuse

    def close_session(self):
        """Explicitly close the cached session (for cleanup/shutdown)."""
        if self._cached_session is not None:
            try:
                self._cached_session.close()
            except Exception as e:
                logging.exception(f"Error closing session: {e}")
            finally:
                self._cached_session = None

    def get_table_sql_metadata(self, table_name: str):
        self.base.metadata.reflect(bind=self.engine)
        return self.base.metadata.tables[table_name]

    def does_matview_exist(self, mv_name: str) -> bool:
        """Check if a materialized view of the given name exists in Postgres."""
        with self.session_scope() as session:
            result = session.execute(
                """
                SELECT 1
                FROM pg_catalog.pg_matviews
                WHERE matviewname = :mv_name
                """,
                {"mv_name": mv_name},
            ).scalar()
            return bool(result)

    def get_unique_string_values(self, table_name: str):
        """
        Return a dictionary of { column_name: [distinct values] }
        by querying either the matching MV or the base table.
        """
        mv_name = f"{table_name}_mv"  # or whatever name you used

        # If the specialized MV exists, query it;
        # otherwise, fall back to the main table.
        if self.does_matview_exist(mv_name):
            return self._get_unique_values_from_matview(mv_name)
        else:
            return self._get_unique_values_from_base_table(table_name)

    def _get_unique_values_from_matview(self, mv_name: str):
        """
        Example logic if your MV has the shape: (column_name, col_value).
        We'll group values by 'column_name'.
        """
        with self.session_scope() as session:
            rows = session.execute(f"SELECT column_name, col_value FROM {mv_name}")
            # rows is an iterator of (column_name, col_value)
            unique_values = {}
            for col_name, val in rows:
                unique_values.setdefault(col_name, []).append(val)
            return unique_values

    def _get_unique_values_from_base_table(self, table_name: str):
        """
        Your existing fallback approach – scanning each column's distinct values
        from the real table if no MV is found.
        """
        model = self.cached_table(table_name)

        unique_values = {}
        with self.session_scope() as session:
            for column_name in model.__table__.columns.keys():
                # Check if it's string
                try:
                    python_type = model.__table__.columns[column_name].type.python_type
                except NotImplementedError:
                    python_type = None

                if python_type == str:
                    query = session.query(getattr(model, column_name)).distinct()
                    unique_values[column_name] = [row[0] for row in query.all()]

        return unique_values

    def get_views(self, datastack_name: str):
        with self.session_scope() as session:
            query = session.query(AnalysisView).filter(
                AnalysisView.datastack_name == datastack_name
            )
            return query.all()

    def get_view_metadata(self, datastack_name: str, view_name: str):
        with self.session_scope() as session:
            query = (
                session.query(AnalysisView)
                .filter(AnalysisView.table_name == view_name)
                .filter(AnalysisView.datastack_name == datastack_name)
            )
            result = query.one()
            if hasattr(result, "__dict__"):
                return self.get_automap_items(result)
            else:
                return result[0]

    def get_table_metadata(self, table_name: str, filter_col: str = None):
        data = getattr(AnnoMetadata, filter_col) if filter_col else AnnoMetadata
        with self.session_scope() as session:
            if filter_col and data:
                query = session.query(data).filter(
                    AnnoMetadata.table_name == table_name
                )
                result = query.one()

                if hasattr(result, "__dict__"):
                    return self.get_automap_items(result)
                else:
                    return result[0]
            else:
                metadata = (
                    session.query(data, SegmentationMetadata)
                    .outerjoin(
                        SegmentationMetadata,
                        AnnoMetadata.table_name
                        == SegmentationMetadata.annotation_table,
                    )
                    .filter(
                        or_(
                            AnnoMetadata.table_name == table_name,
                            SegmentationMetadata.table_name == table_name,
                        )
                    )
                    .all()
                )
                try:
                    if metadata:
                        flatted_metadata = self.flatten_join(metadata)
                        return flatted_metadata[0]
                except NoResultFound:
                    return None

    def get_table_schema(self, table_name: str) -> str:
        table_metadata = self.get_table_metadata(table_name)
        return table_metadata.get("schema_type")

    def get_valid_table_names(self) -> List[str]:
        with self.session_scope() as session:
            metadata = session.query(AnnoMetadata).all()
            return [m.table_name for m in metadata if m.valid == True]

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
        with self.session_scope() as session:
            return session.query(Model).count()

    def get_max_id_value(self, table_name: str) -> int:
        model = self.cached_table(table_name)
        with self.session_scope() as session:
            return session.query(func.max(model.id)).scalar()

    def get_min_id_value(self, table_name: str) -> int:
        model = self.cached_table(table_name)
        with self.session_scope() as session:
            return session.query(func.min(model.id)).scalar()

    def get_table_row_count(
        self, table_name: str, filter_valid: bool = False, filter_timestamp: str = None
    ) -> int:
        """Get row counts. Optionally can filter by row validity and
        by timestamp.

        Args:
            table_name (str): Name of table
            filter_valid (bool, optional): Filter only valid rows. Defaults to False.
            filter_timestamp (None, optional): Filter rows up to timestamp . Defaults to False.

        Returns:
            int: number of rows
        """
        model = self.cached_table(table_name)
        with self.session_scope() as session:
            sql_query = session.query(func.count(model.id))
            if filter_valid:
                sql_query = sql_query.filter(model.valid == True)
            if filter_timestamp and hasattr(model, "created"):
                sql_query = sql_query.filter(model.created <= filter_timestamp)
            return sql_query.scalar()

    @staticmethod
    def get_automap_items(result):
        return {k: v for (k, v) in result.__dict__.items() if k != "_sa_instance_state"}

    def obj_to_dict(self, obj):
        if obj:
            return {
                column.key: getattr(obj, column.key)
                for column in inspect(obj).mapper.column_attrs
            }
        else:
            return {}

    def flatten_join(self, _list: List):
        return [{**self.obj_to_dict(a), **self.obj_to_dict(b)} for a, b in _list]

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

    def _get_existing_table_names(self, filter_valid: bool = False,
                                  filter_timestamp: datetime.datetime = None) -> List[str]:
        """Collects table_names keys of existing tables

        Returns
        -------
        list
            List of table_names
        """
        with self.session_scope() as session:
            stmt = session.query(AnnoMetadata)
            if filter_valid:
                stmt = stmt.filter(AnnoMetadata.valid == True)
            if filter_timestamp:
                stmt = stmt.filter(AnnoMetadata.created <= filter_timestamp)
                stmt = stmt.filter(or_(AnnoMetadata.deleted > filter_timestamp,
                                       AnnoMetadata.deleted == None))
            metadata = stmt.all()
            return [m.table_name for m in metadata]

    def _get_model_from_table_name(self, table_name: str) -> DeclarativeMeta:
        metadata = self.get_table_metadata(table_name)

        if metadata:
            if metadata["reference_table"]:
                return self.schema_client.create_reference_annotation_model(
                    table_name,
                    metadata["schema_type"],
                    metadata["reference_table"],
                )
            elif metadata.get("annotation_table") and table_name != metadata.get(
                "annotation_table"
            ):
                return self.schema_client.create_segmentation_model(
                    metadata["annotation_table"],
                    metadata["schema_type"],
                    metadata["pcg_table_name"],
                )

            else:
                return self.schema_client.create_annotation_model(
                    table_name, metadata["schema_type"]
                )

        else:
            raise TableNotInMetadata

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
            raise TableNameNotFound(table_name)
        return [(column["name"], column["type"]) for column in db_columns]

    def get_view_table(self, view_name: str) -> Table:
        """Return the sqlalchemy table object for a view"""
        if self._is_cached(view_name):
            return self._cached_tables[view_name]
        else:
            meta = MetaData(self._engine)
            meta.reflect(views=True, only=[view_name])
            table = meta.tables[view_name]
            self._cached_tables[view_name] = table
            return table

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
        except TableNotInMetadata:
            # cant find the table so lets try the slow reflection before giving up
            self.mapped_base = automap_base()
            self.mapped_base.prepare(self._engine, reflect=True)
            try:
                model = self.mapped_base.classes[table_name]
                self._cached_tables[table_name] = model
            except KeyError as table_error:
                logging.error(f"Could not load table: {table_error}")
                return False

        except Exception as table_error:
            logging.error(f"Could not load table: {table_error}")
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
