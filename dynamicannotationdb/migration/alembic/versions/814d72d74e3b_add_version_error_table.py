"""Add version error table

Revision ID: 814d72d74e3b
Revises: 975a79461cab
Create Date: 2022-09-15 12:23:50.769937

"""
from alembic import op
import sqlalchemy as sa

from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = "814d72d74e3b"
down_revision = "975a79461cab"
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        "version_error",
        sa.Column("id", sa.INTEGER(), autoincrement=True, nullable=False),
        sa.Column(
            "error",
            postgresql.JSON(astext_type=sa.Text()),
            autoincrement=False,
            nullable=True,
        ),
        sa.Column(
            "analysisversion_id", sa.INTEGER(), autoincrement=False, nullable=True
        ),
        sa.ForeignKeyConstraint(
            ["analysisversion_id"],
            ["analysisversion.id"],
            name="version_error_analysisversion_id_fkey",
        ),
        sa.PrimaryKeyConstraint("id", name="version_error_pkey"),
    )


def downgrade():
    op.drop_table("versionerror")
