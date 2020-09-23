from os import environ, getenv
import uuid
import warnings
import pytest
import docker
import psycopg2
import logging
import time
from typing import Any, Callable, Type
from functools import wraps
from dynamicannotationdb.interface import DynamicAnnotationInterface
from dynamicannotationdb.materialization_client import DynamicMaterializationClient
from dynamicannotationdb.annotation_client import DynamicAnnotationClient

logging.basicConfig(level=logging.DEBUG)
test_logger = logging.getLogger()

POSTGIS_DOCKER_IMAGE = "mdillon/postgis:latest"
ALIGNED_VOLUME = 'test_volume'
DB_HOST = '127.0.0.1'
TABLE_NAME = 'anno_test'
SCHEMA_TYPE = 'synapse'

SQL_URI = f"postgres://postgres:postgres@{DB_HOST}:5432/{ALIGNED_VOLUME}"



db_enviroment = [
    f"POSTGRES_USER=postgres",
    f"POSTGRES_PASSWORD=postgres",
    f"POSTGRES_DB={ALIGNED_VOLUME}"
]

db_ports = {"5432/tcp": 5432}

USE_LOCAL_DB = getenv("USE_LOCAL_TEST_DB", False)


@pytest.fixture(scope="session")
def docker_client() -> docker.DockerClient:
    yield docker.from_env()


@pytest.fixture(scope="session")
def annotation_client():
    annotation_client = DynamicAnnotationClient(ALIGNED_VOLUME, SQL_URI)
    return annotation_client

@pytest.fixture(scope="session")
def dynamic_annotation_interface():
    dynamic_annotation_interface = DynamicAnnotationInterface(SQL_URI)
    return dynamic_annotation_interface


@pytest.fixture(scope="session")
def materialization_client():
    materialization_client = DynamicMaterializationClient(ALIGNED_VOLUME, SQL_URI)
    return materialization_client

@pytest.fixture(scope="session", autouse=True)
def postgis_server(docker_client: docker.DockerClient) -> None:
    warnings.filterwarnings("ignore", category=DeprecationWarning)

    if not USE_LOCAL_DB:
        test_logger.info(f"PULLING {POSTGIS_DOCKER_IMAGE} IMAGE")
        try:
            docker_client.images.pull(repository=POSTGIS_DOCKER_IMAGE)
        except Exception:
            test_logger.exception("Failed to pull postgres image")

        container_name = f"test_postgis_server_{uuid.uuid4()}"

        test_container = docker_client.containers.run(
            image=POSTGIS_DOCKER_IMAGE,
            detach=True,
            hostname='test_postgres',
            auto_remove=True,
            name=container_name,
            environment=db_enviroment,
            ports=db_ports,
        )



        test_logger.info('STARTING IMAGE')
        try:
            time.sleep(10)
            check_database(SQL_URI)
            yield test_container
        finally:
            container = docker_client.containers.get(container_name)
            container.stop()
    else:
        yield
        return


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
