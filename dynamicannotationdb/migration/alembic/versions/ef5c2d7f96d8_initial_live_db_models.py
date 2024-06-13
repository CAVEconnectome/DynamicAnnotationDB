"""Initial Live DB models

Revision ID: ef5c2d7f96d8
Revises:
Create Date: 2022-08-08 09:59:29.189065

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.engine import reflection

# revision identifiers, used by Alembic.
revision = "ef5c2d7f96d8"
down_revision = None
branch_labels = None
depends_on = None


def get_tables(connection):
    inspector = reflection.Inspector.from_engine(connection)
    return inspector.get_table_names()


def _table_has_column(connection, table, column):
    insp = reflection.Inspector.from_engine(connection)
    return any(column in col["name"] for col in insp.get_columns(table))


def upgrade():
    connection = op.get_bind()
    tables = get_tables(connection)
    if "analysisversion" not in tables:
        op.create_table(
            "analysisversion",
            sa.Column("id", sa.Integer(), nullable=False),
            sa.Column("datastack", sa.String(length=100), nullable=False),
            sa.Column("version", sa.Integer(), nullable=False),
            sa.Column("time_stamp", sa.DateTime(), nullable=False),
            sa.Column("valid", sa.Boolean(), nullable=True),
            sa.Column("expires_on", sa.DateTime(), nullable=True),
            sa.PrimaryKeyConstraint("id"),
        )
    if "analysistables" not in tables:
        op.create_table(
            "analysistables",
            sa.Column("id", sa.Integer(), nullable=False),
            sa.Column("aligned_volume", sa.String(length=100), nullable=False),
            sa.Column("schema", sa.String(length=100), nullable=False),
            sa.Column("table_name", sa.String(length=100), nullable=False),
            sa.Column("valid", sa.Boolean(), nullable=True),
            sa.Column("created", sa.DateTime(), nullable=False),
            sa.Column("analysisversion_id", sa.Integer(), nullable=True),
            sa.ForeignKeyConstraint(
                ["analysisversion_id"],
                ["analysisversion.id"],
            ),
            sa.PrimaryKeyConstraint("id"),
        )
    if "annotation_table_metadata" not in tables:
        op.create_table(
            "annotation_table_metadata",
            sa.Column("id", sa.Integer(), nullable=False),
            sa.Column("schema_type", sa.String(length=100), nullable=False),
            sa.Column("table_name", sa.String(length=100), nullable=False),
            sa.Column("valid", sa.Boolean(), nullable=True),
            sa.Column("created", sa.DateTime(), nullable=False),
            sa.Column("deleted", sa.DateTime(), nullable=True),
            sa.Column("user_id", sa.String(length=255), nullable=False),
            sa.Column("description", sa.Text(), nullable=False),
            sa.Column("reference_table", sa.String(length=100), nullable=True),
            sa.Column("flat_segmentation_source", sa.String(length=300), nullable=True),
            sa.Column("voxel_resolution_x", sa.Float(), nullable=False),
            sa.Column("voxel_resolution_y", sa.Float(), nullable=False),
            sa.Column("voxel_resolution_z", sa.Float(), nullable=False),
            sa.PrimaryKeyConstraint("id"),
            sa.UniqueConstraint("table_name"),
        )

    if "segmentation_table_metadata" not in tables:
        op.create_table(
            "segmentation_table_metadata",
            sa.Column("id", sa.Integer(), nullable=False),
            sa.Column("schema_type", sa.String(length=100), nullable=False),
            sa.Column("table_name", sa.String(length=100), nullable=False),
            sa.Column("valid", sa.Boolean(), nullable=True),
            sa.Column("created", sa.DateTime(), nullable=False),
            sa.Column("deleted", sa.DateTime(), nullable=True),
            sa.Column("segmentation_source", sa.String(length=255), nullable=True),
            sa.Column("pcg_table_name", sa.String(length=255), nullable=False),
            sa.Column("last_updated", sa.DateTime(), nullable=True),
            sa.Column("annotation_table", sa.String(length=100), nullable=True),
            sa.ForeignKeyConstraint(
                ["annotation_table"],
                ["annotation_table_metadata.table_name"],
            ),
            sa.PrimaryKeyConstraint("id"),
            sa.UniqueConstraint("table_name"),
        )


def downgrade():
    op.drop_table("analysistables")
    op.drop_table("analysisversion")
    op.drop_table("segmentation_table_metadata")
    op.drop_table("annotation_table_metadata")
