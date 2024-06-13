"""Add error msg

Revision ID: 6e7f580ff680
Revises: 814d72d74e3b
Create Date: 2022-09-22 14:37:41.506933

"""
from alembic import op
import sqlalchemy as sa

from sqlalchemy.dialects import postgresql
from sqlalchemy import engine_from_config
from sqlalchemy.engine import reflection

# revision identifiers, used by Alembic.
revision = '6e7f580ff680'
down_revision = '814d72d74e3b'
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
    if not _table_has_column("version_error", "exception"):
        op.add_column('version_error', sa.Column('exception', sa.String(), nullable=True))


def downgrade():
    op.drop_column('version_error', 'exception')
   