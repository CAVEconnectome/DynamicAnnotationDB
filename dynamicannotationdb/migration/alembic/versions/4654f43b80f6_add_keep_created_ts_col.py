"""Add keep created ts col

Revision ID: 4654f43b80f6
Revises: 309cf493a1e2
Create Date: 2023-01-17 17:16:11.658225

"""
from alembic import op
import sqlalchemy as sa

from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = "4654f43b80f6"
down_revision = "309cf493a1e2"
branch_labels = None
depends_on = None


def upgrade():
    op.add_column(
        "annotation_table_metadata",
        sa.Column("keep_created_ts_col", sa.Boolean(), nullable=True),
    )

    op.execute("UPDATE annotation_table_metadata SET keep_created_ts_col = False")
    op.alter_column("annotation_table_metadata", "keep_created_ts_col", nullable=False)


def downgrade():
    op.drop_column("annotation_table_metadata", "keep_created_ts_col")
