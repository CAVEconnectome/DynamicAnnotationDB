"""Add error msg

Revision ID: 6e7f580ff680
Revises: 814d72d74e3b
Create Date: 2022-09-22 14:37:41.506933

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.engine import reflection

# revision identifiers, used by Alembic.
revision = '6e7f580ff680'
down_revision = '814d72d74e3b'
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

    if not _table_has_column(connection, "version_error", "exception"):
        op.add_column('version_error', sa.Column('exception', sa.String(), nullable=True))


def downgrade():
    op.drop_column('version_error', 'exception')
   