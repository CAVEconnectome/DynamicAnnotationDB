import logging
from contextlib import contextmanager
from typing import Any, Dict, List, Tuple
from functools import lru_cache


from sqlalchemy import (
    engine,
    MetaData,
    Table,
    create_engine,
    func,
    inspect,
    or_,
    select,
)
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import sessionmaker, scoped_session
from sqlalchemy.schema import MetaData
from sqlalchemy.sql.schema import Table


from dynamicannotationdb.errors import (
    TableAlreadyExists,
    TableNameNotFound,
    TableNotInMetadata,
)
from dynamicannotationdb.models import (
    AnalysisView,
    AnnoMetadata,
    Base,
    SegmentationMetadata,
    CombinedMetadata,
)
from dynamicannotationdb.schema import DynamicSchemaClient


class DynamicAnnotationDB:
    def __init__(self, sql_url: str, pool_size: int = 5, max_overflow: int = 5) -> None:
        self._engine: engine = create_engine(
            sql_url, pool_recycle=3600, pool_size=pool_size, max_overflow=max_overflow
        )
        self.base = Base

        session_factory = sessionmaker(bind=self._engine, expire_on_commit=False)
        self.Session = scoped_session(session_factory)

        self.schema_client = DynamicSchemaClient()
        self._inspector = inspect(self._engine)
        self._cached_tables: Dict[str, Any] = {}

    @property
    def inspector(self):
        return self._inspector

    @property
    def engine(self):
        return self._engine

    @contextmanager
    def session_scope(self):
        session = self.Session()
        try:
            yield session
            session.commit()
        except Exception as e:
            session.rollback()
            logging.exception(f"SQL Error: {e}")
            raise
        finally:
            self.Session.remove()

    def get_table_sql_metadata(self, table_name: str) -> Table:
        self.base.metadata.reflect(bind=self._engine)
        return self.base.metadata.tables[table_name]

    def get_unique_string_values(self, table_name: str):
        """Get unique string values for a given table

        Parameters
        ----------
        table_name : str
            name of table contained within the aligned_volume database

        Returns
        -------
        dict
            dictionary of column names and unique values
        """
        model = self.cached_table(table_name)
        unique_values = {}

        with self.session_scope() as session:
            for column_name, column in model.__table__.columns.items():
                if isinstance(column.type.python_type, str):
                    stmt = select(getattr(model, column_name)).distinct()
                    result = session.execute(stmt)
                    unique_values[column_name] = [row[0] for row in result.fetchall()]

        return unique_values

    def get_views(self, datastack_name: str) -> List[AnalysisView]:
        with self.session_scope() as session:
            stmt = select(AnalysisView).where(
                AnalysisView.datastack_name == datastack_name
            )
            result = session.execute(stmt)
            return result.scalars().all()

    def get_view_metadata(self, datastack_name: str, view_name: str) -> Dict[str, Any]:
        with self.session_scope() as session:
            stmt = select(AnalysisView).where(
                AnalysisView.table_name == view_name,
                AnalysisView.datastack_name == datastack_name,
            )
            result = session.execute(stmt)
            view = result.scalar_one()
            return self.get_automap_items(view)

    @lru_cache(maxsize=128)
    def get_table_metadata(self, table_name: str) -> CombinedMetadata:
        with self.session_scope() as session:
            stmt = (
                select(AnnoMetadata, SegmentationMetadata)
                .outerjoin(
                    SegmentationMetadata,
                    AnnoMetadata.table_name == SegmentationMetadata.annotation_table,
                )
                .where(
                    or_(
                        AnnoMetadata.table_name == table_name,
                        SegmentationMetadata.table_name == table_name,
                    )
                )
            )
            result = session.execute(stmt).first()

            if result is None:
                raise ValueError(f"No metadata found for table '{table_name}'")

            anno_metadata, seg_metadata = result

            return CombinedMetadata(
                table_name=table_name,
                anno_metadata=anno_metadata,
                seg_metadata=seg_metadata,
            )

    def get_table_schema(self, table_name: str) -> str:
        table_metadata = self.get_table_metadata(table_name)
        return table_metadata.anno_metadata.schema_type

    def get_valid_table_names(self) -> List[str]:
        with self.session_scope() as session:
            stmt = select(AnnoMetadata.table_name).where(AnnoMetadata.valid == True)
            result = session.execute(stmt)
            return result.scalars().all()

    def get_annotation_table_size(self, table_name: str) -> int:
        Model = self.cached_table(table_name)
        with self.session_scope() as session:
            stmt = select(func.count()).select_from(Model)
            result = session.execute(stmt)
            return result.scalar_one()

    def get_max_id_value(self, table_name: str) -> int:
        model = self.cached_table(table_name)
        with self.session_scope() as session:
            stmt = select(func.max(model.id))
            result = session.execute(stmt)
            return result.scalar_one()

    def get_min_id_value(self, table_name: str) -> int:
        model = self.cached_table(table_name)
        with self.session_scope() as session:
            stmt = select(func.min(model.id))
            result = session.execute(stmt)
            return result.scalar_one()

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
            stmt = select(func.count(model.id))
            if filter_valid:
                stmt = stmt.where(model.valid == True)
            if filter_timestamp and hasattr(model, "created"):
                stmt = stmt.where(model.created <= filter_timestamp)
            result = session.execute(stmt)
            return result.scalar_one()

    @staticmethod
    def get_automap_items(result: Any) -> Dict[str, Any]:
        return {k: v for (k, v) in result.__dict__.items() if k != "_sa_instance_state"}

    def obj_to_dict(self, obj: Any) -> Dict[str, Any]:
        return (
            {
                column.key: getattr(obj, column.key)
                for column in inspect(obj).mapper.column_attrs
            }
            if obj
            else {}
        )

    def flatten_join(self, _list: List[Tuple[Any, Any]]) -> List[Dict[str, Any]]:
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
        if table := self.base.metadata.tables.get(table_name):
            logging.info(f"Deleting {table_name} table")
            self.base.metadata.drop_all(self._engine, [table], checkfirst=True)
            if table_name in self._cached_tables:
                del self._cached_tables[table_name]
            return True
        return False

    def _check_table_is_unique(self, table_name: str) -> List[str]:
        existing_tables = self._get_existing_table_names()
        if table_name in existing_tables:
            raise TableAlreadyExists(
                f"Table creation failed: {table_name} already exists"
            )
        return existing_tables

    def _get_existing_table_names(self, filter_valid: bool = False) -> List[str]:
        with self.session_scope() as session:
            stmt = select(AnnoMetadata.table_name)
            if filter_valid:
                stmt = stmt.where(AnnoMetadata.valid == True)
            result = session.execute(stmt)
            return result.scalars().all()

    def _get_model_from_table_name(self, table_name: str) -> Any:
        combined_metadata = self.get_table_metadata(table_name)

        if combined_metadata is None:
            raise TableNotInMetadata(f"No metadata found for table '{table_name}'")

        anno_metadata = combined_metadata.anno_metadata
        seg_metadata = combined_metadata.seg_metadata

        if anno_metadata:
            if anno_metadata.reference_table:
                return self.schema_client.create_reference_annotation_model(
                    table_name, anno_metadata.schema_type, anno_metadata.reference_table
                )
            elif seg_metadata and table_name == seg_metadata.table_name:
                return self.schema_client.create_segmentation_model(
                    anno_metadata.table_name,
                    anno_metadata.schema_type,
                    seg_metadata.pcg_table_name,
                )
            else:
                return self.schema_client.create_annotation_model(
                    table_name, anno_metadata.schema_type
                )
        else:
            raise ValueError(f"Invalid metadata structure for table '{table_name}'")

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
        db_columns = self._inspector.get_columns(table_name)
        if not db_columns:
            raise TableNameNotFound(table_name)
        return [(column["name"], column["type"]) for column in db_columns]

    def get_view_table(self, view_name: str) -> Table:
        """Return the sqlalchemy table object for a view"""
        if view_name in self._cached_tables:
            return self._cached_tables[view_name]
        meta = MetaData()
        meta.reflect(bind=self._engine, views=True, only=[view_name])
        table = meta.tables[view_name]
        self._cached_tables[view_name] = table
        return table

    def cached_table(self, table_name: str):
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
        try:
            self._cached_tables[table_name] = self._get_model_from_table_name(
                table_name
            )
            return True
        except TableNotInMetadata:
            try:
                base = automap_base()
                base.prepare(self._engine, reflect=True)
                model = base.classes[table_name]
                self._cached_tables[table_name] = model
                return True
            except KeyError as table_error:
                logging.error(f"Could not load table: {table_error}")
                return False
        except Exception as table_error:
            logging.error(f"Could not load table: {table_error}")
            return False
