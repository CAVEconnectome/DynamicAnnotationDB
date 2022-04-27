from emannotationschemas.models import Base
from sqlalchemy import (
    Boolean,
    CheckConstraint,
    Column,
    DateTime,
    Float,
    ForeignKey,
    Integer,
    String,
    Text,
    Enum,
)
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship

MatBase = declarative_base()


class AnalysisVersion(Base):
    __tablename__ = "analysisversion"
    id = Column(Integer, primary_key=True)
    datastack = Column(String(100), nullable=False)
    version = Column(Integer, nullable=False)
    time_stamp = Column(DateTime, nullable=False)
    valid = Column(Boolean)
    expires_on = Column(DateTime, nullable=True)

    def __repr__(self):
        return f"{self.datastack}__mat{self.version}"


class AnalysisTable(Base):
    __tablename__ = "analysistables"
    id = Column(Integer, primary_key=True)
    aligned_volume = Column(String(100), nullable=False)
    schema = Column(String(100), nullable=False)
    table_name = Column(String(100), nullable=False)
    valid = Column(Boolean)
    created = Column(DateTime, nullable=False)
    analysisversion_id = Column(Integer, ForeignKey("analysisversion.id"))
    analysisversion = relationship("AnalysisVersion")


class MaterializedMetadata(MatBase):
    __tablename__ = "materializedmetadata"
    id = Column(Integer, primary_key=True)
    schema = Column(String(100), nullable=False)
    table_name = Column(String(100), nullable=False)
    row_count = Column(Integer, nullable=False)
    materialized_timestamp = Column(DateTime, nullable=False)


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
    __table_args__ = (
        CheckConstraint(
            "reference_table <> annotation_table", name="not_self_referenced"
        ),
    )

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
