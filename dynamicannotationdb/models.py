from sqlalchemy import Column, Boolean, String, UniqueConstraint, Integer, DateTime, Text, ForeignKey
from emannotationschemas.models import Base

class AnnoMetadata(Base):
    __tablename__ = 'annotation_table_metadata'
    id = Column(Integer, primary_key=True)
    schema_type = Column(String(100), nullable=False)
    table_id = Column(String(100), nullable=False, unique=True)
    valid = Column(Boolean)
    created = Column(DateTime, nullable=False)
    deleted = Column(DateTime, nullable=True)
    user_id = Column(String(255), nullable=False)
    description = Column(Text, nullable=False)
    reference_table = Column(String(100), nullable=True)
    flat_segmentation_source = Column(String(300), nullable=True)


class SegmentationMetadata(Base):
    __tablename__ = 'segmentation_table_metadata'
    id = Column(Integer, primary_key=True)
    schema_type = Column(String(100), nullable=False)
    table_id = Column(String(100), nullable=False, unique=True)
    valid = Column(Boolean)
    created = Column(DateTime, nullable=False)
    deleted = Column(DateTime, nullable=True)
    segmentation_source = Column(String(255), nullable=True)
    pcg_table_name = Column(String(255), nullable=False)
    last_updated = Column(DateTime, nullable=True)
    annotation_table = Column(String(100), ForeignKey('annotation_table_metadata.table_id'))
