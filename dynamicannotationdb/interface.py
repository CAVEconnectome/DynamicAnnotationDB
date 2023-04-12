import logging

from sqlalchemy import create_engine
from sqlalchemy.engine.url import make_url
from sqlalchemy.pool import NullPool

from .annotation import DynamicAnnotationClient
from .database import DynamicAnnotationDB
from .models import Base
from .schema import DynamicSchemaClient
from .segmentation import DynamicSegmentationClient


class DynamicAnnotationInterface:
    """An adapter layer to access all the dynamic annotation interfaces.

    Parameters
    ----------
    url : str
        URI of the database to connect to.
    aligned_volume : str
        name of aligned_volume database.

    Interface layers available
    --------------------------
    annotation :
        CRUD operations on annotation data as well as creating annotation tables.
    database :
        Database helper methods and metadata information.
    segmentation :
        CRUD operations on segmentation data as well as creating segmentation tables
        linked to annotation tables.
    schema :
        Wrapper for EMAnnotationSchemas to generate dynamic sqlalchemy models.

    """

    def __init__(
        self, url: str, aligned_volume: str, pool_size=5, max_overflow=5
    ) -> None:
        self._annotation = None
        self._database = None
        self._segmentation = None
        self._schema = None
        self.pool_size = pool_size
        self.max_overflow = max_overflow
        self._base_url = url.rpartition("/")[0]
        self._aligned_volume = aligned_volume
        self._sql_url = self.create_or_select_database(url, aligned_volume)

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
                logging.info(f"Database {aligned_volume} does not exist.")
                self._create_aligned_volume_database(sql_uri, connection)

        temp_engine.dispose()

        self._reset_interfaces()
        self._sql_url = sql_uri
        self._aligned_volume = sql_uri.database
        logging.info(f"Connected to {sql_uri.database}")
        return sql_uri

    def _create_aligned_volume_database(self, sql_uri, connection):
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
            template_uri = make_url(
                f"{str(sql_uri).rpartition('/')[0]}/template_postgis"
            )
            template_engine = create_engine(
                template_uri,
                poolclass=NullPool,
                isolation_level="AUTOCOMMIT",
                pool_pre_ping=True,
            )
            with template_engine.connect() as template_connection:
                template_connection.execute("CREATE EXTENSION IF NOT EXISTS postgis")
            template_engine.dispose()

        # finally create new annotation database
        connection.execute(
            f"CREATE DATABASE {sql_uri.database} TEMPLATE template_postgis"
        )
        aligned_volume_engine = create_engine(
            sql_uri,
            poolclass=NullPool,
            isolation_level="AUTOCOMMIT",
            pool_pre_ping=True,
        )
        try:
            Base.metadata.create_all(aligned_volume_engine)
            logging.info(f"{sql_uri.database} created.")
        except Exception as e:
            raise e
        finally:
            aligned_volume_engine.dispose()

    def _reset_interfaces(self):
        self._annotation = None
        self._database = None
        self._segmentation = None
        self._schema = None

    @property
    def url(self) -> str:
        return self._sql_url

    @property
    def aligned_volume(self) -> str:
        return self._aligned_volume

    @property
    def annotation(self) -> DynamicAnnotationClient:
        if not self._annotation:
            self._annotation = DynamicAnnotationClient(self._sql_url)
        return self._annotation

    @property
    def database(self) -> DynamicAnnotationDB:
        if not self._database:
            self._database = DynamicAnnotationDB(
                self._sql_url, self.pool_size, self.max_overflow
            )
        return self._database

    @property
    def segmentation(self) -> DynamicSegmentationClient:
        if not self._segmentation:
            self._segmentation = DynamicSegmentationClient(self._sql_url)
        return self._segmentation

    @property
    def schema(self) -> DynamicSchemaClient:
        if not self._schema:
            self._schema = DynamicSchemaClient()
        return self._schema
