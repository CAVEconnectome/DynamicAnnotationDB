"""Add is merged

Revision ID: 975a79461cab
Revises: 5a1d7c0ad006
Create Date: 2022-09-15 11:51:21.484964

"""
from alembic import op
import sqlalchemy as sa

from sqlalchemy.dialects import postgresql
from sqlalchemy import engine_from_config
from sqlalchemy.engine import reflection

# revision identifiers, used by Alembic.
revision = "975a79461cab"
down_revision = "5a1d7c0ad006"
branch_labels = None
depends_on = None

def _table_has_column(table, column):
    config = op.get_context().config
    engine = engine_from_config(
        config.get_section(config.config_ini_section), prefix="sqlalchemy."
    )

    insp = reflection.Inspector.from_engine(engine)
    return any(column in col["name"] for col in insp.get_columns(table))


def upgrade():
    if not _table_has_column("analysisversion", "is_merged"):
        op.add_column(
            "analysisversion",
            sa.Column("is_merged", sa.Boolean(), nullable=True, default=True),
        )

        op.execute("UPDATE analysisversion SET is_merged = True")
        op.alter_column('analysisversion', 'is_merged', nullable=False)

def downgrade():
    op.drop_column("analysisversion", "is_merged")
