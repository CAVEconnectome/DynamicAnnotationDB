from sqlalchemy import Column, Boolean, String, UniqueConstraint, Integer, DateTime, Text
from emannotationschemas.models import Base

class Metadata(Base):
    __tablename__ = 'annotation_table_metadata'
    id = Column(Integer, primary_key=True)
    schema_type = Column(String(100), nullable=False)
    table_name = Column(String(100), nullable=False, unique=True)
    valid = Column(Boolean)
    created = Column(DateTime, nullable=False)
    deleted = Column(DateTime, nullable=True)
    user_id = Column(String(255), nullable=False)
    description = Column(Text, nullable=False)
    reference_table = Column(String(100), nullable=True)

