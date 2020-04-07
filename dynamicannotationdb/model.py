from sqlalchemy import Integer, Column, String, DateTime, BigInteger
from dynamicannotationdb.flask_app import db
from dynamicannotationdb.interface import AnnotationInterface


class Annotation(db.Model):
    """Annotation Table"""

    __tablename__ = "annotation"

    id = Column(BigInteger(), primary_key=True)
    schema = Column(String(255), nullable=False)
    user_id = Column(String(255), nullable=False)
    description = Column(String(255), nullable=False)
    created_on = Column(DateTime, nullable=False)
    deleted = Column(DateTime, nullable=True)
    # reference_table = 

    def update(self, changes: AnnotationInterface):
        for key, val in changes.items():
            setattr(self, key, val)
        return self
