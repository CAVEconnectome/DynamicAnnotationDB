from mypy_extensions import TypedDict
from dataclasses import dataclass
from pytz import UTC
import datetime

class AnnotationInterface(TypedDict, total=False):
    id: int
    schema: str
    user_id: str
    description: str
    created_on: str
    deleted: None

# @dataclass
# class Annotation:
#     id: int
#     schema: str
#     user_id: str
#     description: str
#     created_on: str
#     deleted: None