"""Add view model

Revision ID: fac66b439033
Revises: 309cf493a1e2
Create Date: 2023-03-07 12:42:08.667620

"""
from alembic import op
import sqlalchemy as sa

from sqlalchemy.dialects import postgresql
from sqlalchemy.engine import reflection
from sqlalchemy import engine_from_config

# revision identifiers, used by Alembic.
revision = "fac66b439033"
down_revision = "309cf493a1e2"
branch_labels = None
depends_on = None

def get_tables():
    config = op.get_context().config
    engine = engine_from_config(
        config.get_section(config.config_ini_section), prefix="sqlalchemy."
    )
    inspector = reflection.Inspector.from_engine(engine)
    return inspector.get_table_names()


def upgrade():
    tables = get_tables()
    if "analysisviews" not in tables:
        op.create_table(
            "analysisviews",
            sa.Column("id", sa.Integer(), nullable=False),
            sa.Column("table_name", sa.String(length=100), nullable=False),
            sa.Column("description", sa.Text(), nullable=False),
            sa.Column("datastack_name", sa.String(length=100), nullable=False),
            sa.Column("voxel_resolution_x", sa.Float(), nullable=False),
            sa.Column("voxel_resolution_y", sa.Float(), nullable=False),
            sa.Column("voxel_resolution_z", sa.Float(), nullable=False),
            sa.Column("notice_text", sa.Text(), nullable=True),
            sa.Column("live_compatible", sa.Boolean(), nullable=False),
            sa.PrimaryKeyConstraint("id"),
        )


def downgrade():
    op.drop_table("analysisviews")
