from flask_sqlalchemy import SQLAlchemy
from typing import List
from tests.fixtures import app, db
from dynamicannotationdb.model import Annotation
from dynamicannotationdb.service import AnnotationService
from dynamicannotationdb.interface import AnnotationInterface
from datetime import datetime
import pytz

def test_get_all(db: SQLAlchemy):
    created_time = datetime.now(pytz.utc)

    test_table: Annotation = Annotation(id=1, 
                                        schema="SynapseSchema",
                                        user_id="123",
                                        description="Fake Annotation Table",
                                        created_on=created_time)
    db.session.add(test_table)
    db.session.commit()

    results: List[Widget] = AnnotationService.get_all()

    assert len(results) == 1
    assert test_table in results

def test_update(db: SQLAlchemy):
    created_time = datetime.now(pytz.utc)

    test_table: Annotation = Annotation(id=1, 
                                        schema="SynapseSchema",
                                        user_id="123",
                                        description="Fake Annotation Table",
                                        created_on=created_time)

    db.session.add(test_table)
    db.session.commit()
    updates: AnnotationInterface = dict(schema="NewSchema")

    AnnotationService.update(test_table, updates)

    result: Annotation = Annotation.query.get(test_table.id)
    assert result.schema == "NewSchema"


def test_delete_by_id(db: SQLAlchemy):
    created_time = datetime.now(pytz.utc)
    test_table: Annotation = Annotation(id=1, 
                                        schema="SynapseSchema",
                                        user_id="123",
                                        description="Fake Annotation Table",
                                        created_on=str(created_time))
    db.session.add(test_table)
    db.session.commit()

    AnnotationService.delete_by_id(1)
    db.session.commit()

    results: List[Widget] = Annotation.query.all()

    assert len(results) == 0
    assert test_table not in results

def test_create(db: SQLAlchemy):
    created_time = datetime.now(pytz.utc)
    test_table: AnnotationInterface = dict(id=123,
                                           schema="SynapseSchema",
                                           user_id="123",
                                           description="Fake Annotation Table",
                                           created_on=str(created_time))
    AnnotationService.create(test_table)
    results: List[Widget] = Annotation.query.all()

    assert len(results) == 1

    for k in test_table.keys():
        assert getattr(results[0], k) == test_table[k]
