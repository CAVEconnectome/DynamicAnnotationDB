from unittest.mock import patch
from flask.testing import FlaskClient
from datetime import datetime
import pytz
from tests.fixtures import client, app
from dynamicannotationdb.service import AnnotationService
from dynamicannotationdb.schema import AnnotationSchema
from dynamicannotationdb.model import Annotation
from dynamicannotationdb.interface import AnnotationInterface


def make_annotation(id: int = 123, 
                    schema: str = "SynapseSchema",
                    user_id: str = "123",
                    description: str = "Fake Annotation Table") -> Annotation:
    created_time = datetime.now(pytz.utc)

    return Annotation(id=id, schema=schema, user_id=user_id, description=description, created_on=created_time)


class TestAnnotationResource:
    @patch.object(
        AnnotationService,
        "get_all",
        lambda: [
            make_annotation(id=123, schema="Test Annotation 1", user_id='123', description='Test AnnoTable'),
            make_annotation(id=456, schema="SynapseSchema", user_id='234', description='Test2 Table'),
        ],
    )
    def test_get(self, client: FlaskClient):
        with client:
            results = client.get(f"/api/annotation/", follow_redirects=True).get_json()
            expected = (
                AnnotationSchema(many=True)
                .dump(
                    [
                        make_annotation(id=123, schema="Test Annotation 1", user_id='123', description='Test AnnoTable'),
                        make_annotation(id=456, schema="SynapseSchema", user_id='234', description='Test2 Table'),
                    ]
                )
                
            )
            for r in results:
                assert r in expected

    @patch.object(
        AnnotationService, "create", lambda create_request: Annotation(**create_request)
    )
    def test_post(self, client: FlaskClient):
        with client:
            created_time = datetime.now(pytz.utc)
            anno = dict(id=123, 
                        schema="Test Annotation 1",
                        user_id='123',
                        description='Test AnnoTable',
                        created_on=created_time,
                        )
            result = client.post(f"/api/annotation/", json=anno).get_json()
            expected = (
                AnnotationSchema()
                .dump(Annotation(id=anno['id'],
                                 schema=anno["schema"],
                                 user_id=anno["user_id"],
                                 description=anno["description"],
                                 created_on=created_time,
                                 )),
            )
            assert result == expected


def fake_update(annotation: Annotation, changes: AnnotationInterface) -> Annotation:
    return Annotation(
        id=changes["id"],
        schema=changes["schema"], 
        user_id=changes["user_id"], 
        description=changes["description"], 
        created_on=changes["created_on"]
    )


class TestAnnotationIdResource:
    @patch.object(AnnotationService, "get_by_id", lambda id: make_annotation(id=id))
    def test_get(self, client: FlaskClient): 
        with client:
            result = client.get(f"/api/annotation/123").get_json()
            expected = make_annotation(id=123)
            print(f"result = ", result)
            assert result["id"] == expected.id

    @patch.object(AnnotationService, "delete_by_id", lambda id: id)
    def test_delete(self, client: FlaskClient):
        with client:
            result = client.delete(f"/api/annotation/123").get_json()
            expected = dict(status="Success", id=123)
            assert result == expected

    @patch.object(AnnotationService, "get_by_id", lambda id: make_annotation(id=id))
    @patch.object(AnnotationService, "update", fake_update)
    def test_put(self, client: FlaskClient):
        with client:
            created_time = datetime.now(pytz.utc)

            result = client.put(
                f"/api/annotation/123",
                json={"id": 123, 
                      "schema": "SynapseSchema", 
                      "user_id": "123", 
                      "description": "Test AnnoTable",
                      "created_on": created_time},
            ).get_json()
            expected = (
                AnnotationSchema()
                .dump(Annotation(id=123,
                                 schema = "SynapseSchema", 
                                 user_id = "123", 
                                 description = "Test AnnoTable",
                                 created_on = created_time))
                
            )
            assert result == expected
