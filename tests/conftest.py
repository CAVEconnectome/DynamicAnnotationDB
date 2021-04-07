import logging
import time
import uuid
import warnings

import docker
import psycopg2
import pytest
from dynamicannotationdb.annotation_client import DynamicAnnotationClient
from dynamicannotationdb.interface import DynamicAnnotationInterface
from dynamicannotationdb.materialization_client import DynamicMaterializationClient

logging.basicConfig(level=logging.DEBUG)
test_logger = logging.getLogger()


def pytest_addoption(parser):
    parser.addoption(
        "--docker",
        action="store",
        default=False,
        help="Use docker for postgres testing",
    )


@pytest.fixture(scope="session")
def docker_mode(request):
    return request.config.getoption("--docker")


def pytest_configure(config):
    config.addinivalue_line("markers", "docker: use postgres in docker")


@pytest.fixture(scope="session")
def database_metadata() -> dict:
    yield {
        "postgis_docker_image": "postgis/postgis:9.6-2.5",
        "db_host": "localhost",
        "sql_uri": f"postgresql://postgres:postgres@localhost:5432/test_volume",
    }


@pytest.fixture(scope="session")
def annotation_metadata():
    yield {
        "aligned_volume": "test_volume",
        "table_name": "anno_test",
        "schema_type": "synapse",
        "pcg_table_name": "test_pcg",
    }


@pytest.fixture(scope="session", autouse=True)
def postgis_server(docker_mode, database_metadata: dict) -> None:

    postgis_docker_image = database_metadata["postgis_docker_image"]
    sql_uri = database_metadata["sql_uri"]

    if docker_mode:
        test_logger.info(f"PULLING {postgis_docker_image} IMAGE")
        try:
            docker_client = docker.from_env()
            docker_client.images.pull(repository=postgis_docker_image)
        except Exception:
            test_logger.exception("Failed to pull postgres image")

        container_name = f"test_postgis_server_{uuid.uuid4()}"

        test_container = docker_client.containers.run(
            image=postgis_docker_image,
            detach=True,
            hostname="test_postgres",
            auto_remove=True,
            name=container_name,
            environment=[
                "POSTGRES_USER=postgres",
                "POSTGRES_PASSWORD=postgres",
                "POSTGRES_DB=test_volume",
            ],
            ports={"5432/tcp": 5432},
        )

        test_logger.info("STARTING IMAGE")
        try:
            time.sleep(10)
            check_database(sql_uri)
        except Exception as e:
            raise (e)
    yield test_container
    if docker_mode:
        warnings.filterwarnings(
            action="ignore", message="unclosed", category=ResourceWarning
        )
        container = docker_client.containers.get(container_name)
        container.stop()


@pytest.fixture(scope="session")
def dynamic_annotation_interface(postgis_server, database_metadata):
    sql_uri = database_metadata["sql_uri"]
    interface = DynamicAnnotationInterface(sql_uri)
    yield interface
    interface.cached_session.close()


@pytest.fixture(scope="session")
def annotation_client(postgis_server, annotation_metadata, database_metadata):
    aligned_volume = annotation_metadata["aligned_volume"]
    sql_uri = database_metadata["sql_uri"]
    anno_client = DynamicAnnotationClient(aligned_volume, sql_uri)
    yield anno_client
    anno_client.cached_session.close()


@pytest.fixture(scope="session")
def materialization_client(postgis_server, annotation_metadata, database_metadata):
    aligned_volume = annotation_metadata["aligned_volume"]
    sql_uri = database_metadata["sql_uri"]
    mat_client = DynamicMaterializationClient(aligned_volume, sql_uri)
    yield mat_client
    mat_client.cached_session.close()


def check_database(sql_uri: str) -> None:  # pragma: no cover
    try:
        test_logger.info("ATTEMPT TO CONNECT")
        conn = psycopg2.connect(sql_uri)
        cur = conn.cursor()
        cur.execute("SELECT 1")
        test_logger.info("CONNECTED")

        cur.close()
        conn.close()
    except Exception as e:
        test_logger.info(e)
