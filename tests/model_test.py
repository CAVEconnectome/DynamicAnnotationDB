from pytest import fixture
from flask_sqlalchemy import SQLAlchemy
from tests.fixtures import app, db 
from dynamicannotationdb.model import Annotation
from datetime import datetime
import pytz


@fixture
def annotation() -> Annotation:
    created_time = datetime.now(pytz.utc)
    return Annotation(id=1, 
                      schema="SynapseSchema",
                      user_id="123",
                      description="Fake Annotation Table",
                      created_on=created_time)
    


def test_annotation_create(annotation: Annotation):
    assert annotation


def test_annotation_retrieve(annotation: Annotation, db: SQLAlchemy):
    db.session.add(annotation)
    db.session.commit()
    s = Annotation.query.first()
    assert s.__dict__ == annotation.__dict__
