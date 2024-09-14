import enum
from typing import Optional
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
    JSON,
)
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship
from sqlalchemy.dialects import postgresql

from marshmallow import Schema, fields, post_load
from marshmallow_sqlalchemy import SQLAlchemyAutoSchema

# Models that will be created in the 'materialized' database.
MatBase = declarative_base()
# Models that will be created in the 'annotation' database.
AnnotationBase = declarative_base()


class StatusEnum(enum.Enum):
    AVAILABLE = "AVAILABLE"
    RUNNING = "RUNNING"
    FAILED = "FAILED"
    EXPIRED = "EXPIRED"

    def fetch_values():
        return [c.value for c in StatusEnum]


class AnalysisDataBase(AnnotationBase):
    __tablename__ = "analysisdatabase"
    id = Column(Integer, primary_key=True)
    database = Column(String(100), nullable=False)
    materialize = Column(Boolean, nullable=False, default=True)


class AnalysisVersion(Base):
    __tablename__ = "analysisversion"
    id = Column(Integer, primary_key=True)
    datastack = Column(String(100), nullable=False)
    version = Column(Integer, nullable=False)
    time_stamp = Column(DateTime, nullable=False)
    valid = Column(Boolean)
    expires_on = Column(DateTime, nullable=True)
    parent_version = Column(
        Integer,
        ForeignKey("analysisversion.id"),
        nullable=True,
    )
    status = Column(
        postgresql.ENUM(
            "AVAILABLE", "RUNNING", "FAILED", "EXPIRED", name="version_status"
        ),
        nullable=False,
    )
    is_merged = Column(Boolean, default=True)

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


class VersionErrorTable(Base):
    __tablename__ = "version_error"
    id = Column(Integer, primary_key=True)
    exception = Column(String, nullable=True)
    error = Column(JSON, nullable=True)
    analysisversion_id = Column(Integer, ForeignKey("analysisversion.id"))
    analysisversion = relationship("AnalysisVersion")


class MaterializedMetadata(MatBase):
    __tablename__ = "materializedmetadata"
    id = Column(Integer, primary_key=True)
    schema = Column(String(100), nullable=False)
    table_name = Column(String(100), nullable=False)
    row_count = Column(Integer, nullable=False)
    materialized_timestamp = Column(DateTime, nullable=False)
    segmentation_source = Column(String(255), nullable=True)
    is_merged = Column(Boolean, nullable=True)


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
    notice_text = Column(Text, nullable=True)
    reference_table = Column(String(100), nullable=True)
    flat_segmentation_source = Column(String(300), nullable=True)
    voxel_resolution_x = Column(Float, nullable=False)
    voxel_resolution_y = Column(Float, nullable=False)
    voxel_resolution_z = Column(Float, nullable=False)
    write_permission = Column(
        postgresql.ENUM("PRIVATE", "GROUP", "PUBLIC", name="read_permission"),
        nullable=False,
    )
    read_permission = Column(
        postgresql.ENUM("PRIVATE", "GROUP", "PUBLIC", name="read_permission"),
        nullable=False,
    )
    last_modified = Column(DateTime, nullable=False)


class AnnotationMetadataSchema(SQLAlchemyAutoSchema):
    class Meta:
        model = AnnoMetadata
        include_relationships = True
        load_instance = True


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


class SegmentationMetadataSchema(SQLAlchemyAutoSchema):
    class Meta:
        model = SegmentationMetadata
        include_relationships = True
        load_instance = True


class CombinedMetadata:
    def __init__(
        self,
        table_name: str,
        anno_metadata: Optional[AnnoMetadata] = None,
        seg_metadata: Optional[SegmentationMetadata] = None,
    ):
        self.table_name = table_name
        self.anno_metadata = anno_metadata
        self.seg_metadata = seg_metadata


class CombinedMetadataSchema(Schema):
    table_name = fields.Str(required=True)
    anno_metadata = fields.Nested(AnnotationMetadataSchema, allow_none=True)
    seg_metadata = fields.Nested(SegmentationMetadataSchema, allow_none=True)

    @post_load
    def make_combined_metadata(self, data, **kwargs):
        return CombinedMetadata(**data)


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


# a model of a table that contains table_views, their descriptions and datastacks
class AnalysisView(Base):
    __tablename__ = "analysisviews"
    id = Column(Integer, primary_key=True)
    table_name = Column(String(100), nullable=False)
    description = Column(Text, nullable=False)
    datastack_name = Column(String(100), nullable=False)
    voxel_resolution_x = Column(Float, nullable=False)
    voxel_resolution_y = Column(Float, nullable=False)
    voxel_resolution_z = Column(Float, nullable=False)
    notice_text = Column(Text, nullable=True)
    live_compatible = Column(Boolean, nullable=False)
