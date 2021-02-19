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
environ["ALIGNED_VOLUME"] = 'test_volume'
environ['DB_HOST'] = '127.0.0.1'

db_enviroment = [
    f"POSTGRES_USER=postgres",
    f"POSTGRES_PASSWORD=postgres",
    f"POSTGRES_DB={environ['ALIGNED_VOLUME']}"
]

db_ports = {"5432/tcp": 5432}

USE_LOCAL_DB = getenv("USE_LOCAL_TEST_DB", False)


@pytest.fixture(scope="session")
def docker_client() -> docker.DockerClient:
    yield docker.from_env()


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

        aligned_volume = environ["ALIGNED_VOLUME"]
        db_host = environ["DB_HOST"]
        sql_uri = f"postgres://postgres:postgres@{db_host}:5432/{aligned_volume}"

        test_logger.info('STARTING IMAGE')
        try:
            environ["SQL_URI"] = sql_uri
            time.sleep(10)
            check_database(sql_uri)
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
