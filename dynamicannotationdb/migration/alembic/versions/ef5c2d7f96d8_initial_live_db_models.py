"""Initial Live DB models

Revision ID: ef5c2d7f96d8
Revises: 
Create Date: 2022-08-08 09:59:29.189065

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.engine import reflection
from sqlalchemy import engine_from_config

# revision identifiers, used by Alembic.
revision = "ef5c2d7f96d8"
down_revision = None
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


def downgrade():
    pass
