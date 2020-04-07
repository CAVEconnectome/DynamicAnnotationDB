from pytest import fixture
from dynamicannotationdb.model import Annotation
from dynamicannotationdb.interface import AnnotationInterface
from datetime import datetime
import pytz


@fixture
def interface() -> AnnotationInterface:
    created_time = datetime.now(pytz.utc)
    return AnnotationInterface(id=1, 
                               schema="SynapseSchema",
                               user_id="123",
                               description="Fake Annotation Table",
                               created_on=created_time)
    


def test_AnnotationInterface_create(interface: AnnotationInterface):
    assert interface


def test_AnnotationInterface_works(interface: AnnotationInterface):
    annotation = Annotation(**interface)
    assert annotation
