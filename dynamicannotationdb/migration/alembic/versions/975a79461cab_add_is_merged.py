"""Add is merged

Revision ID: 975a79461cab
Revises: 5a1d7c0ad006
Create Date: 2022-09-15 11:51:21.484964

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.engine import reflection

# revision identifiers, used by Alembic.
revision = "975a79461cab"
down_revision = "5a1d7c0ad006"
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

    if not _table_has_column(connection,"analysisversion", "is_merged"):
        op.add_column(
            "analysisversion",
            sa.Column("is_merged", sa.Boolean(), nullable=True, default=True),
        )

        op.execute("UPDATE analysisversion SET is_merged = True")
        op.alter_column('analysisversion', 'is_merged', nullable=False)

def downgrade():
    op.drop_column("analysisversion", "is_merged")
