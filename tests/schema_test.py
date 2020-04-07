from pytest import fixture
from datetime import datetime
from dynamicannotationdb.model import Annotation
from dynamicannotationdb.schema import AnnotationSchema
from dynamicannotationdb.interface import AnnotationInterface
import pytz

@fixture
def schema() -> AnnotationSchema:
    return AnnotationSchema()


def test_AnnotationSchema_create(schema: AnnotationSchema):
    assert schema


def test_AnnotationSchema_works(schema: AnnotationSchema):
    created_time = datetime.now(pytz.utc)
    params: AnnotationInterface = schema.load(
        {"id": "123", 
        "schema": "SynapseSchema", 
        "user_id": "test_user",
        "description":"Fake Annotation Table",
        "created_on": created_time,
        }
    )
    annotation = Annotation(**params)

    assert annotation.id == 123
    assert annotation.schema == "Test annotation"
    assert annotation.user_id == "Test purpose"
    assert annotation.description == "Fake Annotation Table"
    assert annotation.created_on == created_time

 