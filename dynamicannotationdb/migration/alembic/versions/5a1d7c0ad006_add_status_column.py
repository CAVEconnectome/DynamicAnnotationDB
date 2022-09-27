"""add status column

Revision ID: 5a1d7c0ad006
Revises: 7c79eff751b4
Create Date: 2022-08-16 13:47:38.842604

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = "5a1d7c0ad006"
down_revision = "7c79eff751b4"
branch_labels = None
depends_on = None


def upgrade():
    status_enum = postgresql.ENUM(
        "AVAILABLE", "RUNNING", "FAILED", "EXPIRED", name="version_status"
    )
    status_enum.create(op.get_bind())
    op.add_column(
        "analysisversion",
        sa.Column(
            "status",
            postgresql.ENUM(
                "AVAILABLE", "RUNNING", "FAILED", "EXPIRED", name="version_status"
            ),
            nullable=True,

        ),
    )
    op.execute("UPDATE analysisversion SET status = 'EXPIRED'")
    op.alter_column('analysisversion', 'status', nullable=False)


def downgrade():
    op.drop_column("analysisversion", "status")
