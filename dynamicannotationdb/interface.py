import logging

from sqlalchemy import create_engine
from sqlalchemy.engine.url import make_url
from sqlalchemy.pool import NullPool

from .annotation import DynamicAnnotationClient
from .database import DynamicAnnotationDB
from .schema import DynamicSchemaClient
from .segmentation import DynamicSegmentationClient


class DynamicAnnotationInterface:
    def __init__(self, url: str = None, aligned_volume: str = None) -> None:
        self._url = self.create_or_select_database(url, aligned_volume)
        self._aligned_volume = aligned_volume
        self._annotation = DynamicAnnotationClient(self._url, self._aligned_volume)
        self._database = DynamicAnnotationDB(self._url, self._aligned_volume)
        self._segmentation = DynamicSegmentationClient(self._url, self._aligned_volume)
        self._schema = DynamicSchemaClient()

    def create_or_select_database(self, url: str, aligned_volume: str):
        """Create a new database with the name of the aligned volume. Checks if
        database exists before creating.

        Parameters
        ----------
        url : str
            base path to the sql server
        aligned_volume : str
            name of aligned volume which the database name will inherent

        Returns
        -------
        sql_url instance
        """
        sql_base_uri = url.rpartition("/")[0]

        sql_uri = make_url(f"{sql_base_uri}/{aligned_volume}")

        temp_engine = create_engine(
            sql_base_uri,
            poolclass=NullPool,
            isolation_level="AUTOCOMMIT",
            pool_pre_ping=True,
        )

        with temp_engine.connect() as connection:
            connection.execute("commit")
            database_exists = connection.execute(
                f"SELECT 1 FROM pg_catalog.pg_database WHERE datname = '{sql_uri.database}'"
            )
            if not database_exists.fetchone():
                logging.info(f"Creating new database: {sql_uri.database}")

                connection.execute(
                    f"SELECT pg_terminate_backend(pid) FROM pg_stat_activity \
                           WHERE pid <> pg_backend_pid() AND datname = '{sql_uri.database}';"
                )

                # check if template exists, create if missing
                template_exist = connection.execute(
                    "SELECT 1 FROM pg_catalog.pg_database WHERE datname = 'template_postgis'"
                )

                if not template_exist.fetchone():

                    # create postgis template db
                    connection.execute("CREATE DATABASE template_postgis")

                    # create postgis extension
                    template_uri = make_url(f"{sql_base_uri}/template_postgis")
                    template_engine = create_engine(
                        template_uri,
                        poolclass=NullPool,
                        isolation_level="AUTOCOMMIT",
                        pool_pre_ping=True,
                    )
                    with template_engine.connect() as template_connection:
                        template_connection.execute(
                            "CREATE EXTENSION IF NOT EXISTS postgis"
                        )
                    template_engine.dispose()

                # finally create new annotation database
                connection.execute(
                    f"CREATE DATABASE {sql_uri.database} TEMPLATE template_postgis"
                )

        temp_engine.dispose()
        return sql_uri

    def _reset_interfaces(self):
        self._annotation = None
        self._database = None
        self._segmentation = None
        self._schema = None

    @property
    def annotation(self) -> DynamicAnnotationClient:
        if not self._annotation:
            self._annotation = DynamicAnnotationClient(
                self._url, self._aligned_volume)
        return self._annotation

    @property
    def database(self) -> DynamicAnnotationDB:
        if not self._database:
            self._database = DynamicAnnotationDB(
                self._url, self._aligned_volume)
        return self._database

    @property
    def segmentation(self) -> DynamicSegmentationClient:
        if not self._segmentation:
            self._segmentation = DynamicSegmentationClient(
                self._url, self._aligned_volume
            )
        return self._segmentation

    @property
    def schema(self) -> DynamicSchemaClient:
        if not self._schema:
            self._schema = DynamicSchemaClient()
        return self._schema
