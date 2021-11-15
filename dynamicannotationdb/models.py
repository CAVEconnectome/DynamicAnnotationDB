from sqlalchemy import (
    Column,
    Boolean,
    String,
    UniqueConstraint,
    Integer,
    DateTime,
    Text,
    ForeignKey,
    Float,
    CheckConstraint,
)
from emannotationschemas.models import Base


class AnnoMetadata(Base):
    __tablename__ = "annotation_table_metadata"
    id = Column(Integer, primary_key=True)
    schema_type = Column(String(100), nullable=False)
    table_name = Column(String(100), nullable=False, unique=True)
    valid = Column(Boolean)
    created = Column(DateTime, nullable=False)
    deleted = Column(DateTime, nullable=True)
    user_id = Column(String(255), nullable=False)
    description = Column(Text, nullable=False)
    reference_table = Column(String(100), nullable=True)
    flat_segmentation_source = Column(String(300), nullable=True)
    voxel_resolution_x = Column(Float, nullable=False)
    voxel_resolution_y = Column(Float, nullable=False)
    voxel_resolution_z = Column(Float, nullable=False)


class SegmentationMetadata(Base):
    __tablename__ = "segmentation_table_metadata"
    id = Column(Integer, primary_key=True)
    schema_type = Column(String(100), nullable=False)
    table_name = Column(String(100), nullable=False, unique=True)
    valid = Column(Boolean)
    created = Column(DateTime, nullable=False)
    deleted = Column(DateTime, nullable=True)
    segmentation_source = Column(String(255), nullable=True)
    pcg_table_name = Column(String(255), nullable=False)
    last_updated = Column(DateTime, nullable=True)
    annotation_table = Column(
        String(100), ForeignKey("annotation_table_metadata.table_name")
    )


class CombinedTableMetadata(Base):
    __tablename__ = "combined_table_metadata"
    __table_args__ = (CheckConstraint(
        "reference_table <> annotation_table", name="not_self_referenced"
    ),)

    id = Column(Integer, primary_key=True)
    reference_table = Column(
        String(100), ForeignKey("annotation_table_metadata.table_name")
    )
    annotation_table = Column(
        String(100), ForeignKey("annotation_table_metadata.table_name")
    )
    valid = Column(Boolean)
    created = Column(DateTime, nullable=False)
    deleted = Column(DateTime, nullable=True)
    description = Column(Text, nullable=False)
