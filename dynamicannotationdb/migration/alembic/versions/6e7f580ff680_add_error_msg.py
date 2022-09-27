"""Add error msg

Revision ID: 6e7f580ff680
Revises: 814d72d74e3b
Create Date: 2022-09-22 14:37:41.506933

"""
from alembic import op
import sqlalchemy as sa

from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = '6e7f580ff680'
down_revision = '814d72d74e3b'
branch_labels = None
depends_on = None


def upgrade():
    op.add_column('version_error', sa.Column('exception', sa.String(), nullable=True))


def downgrade():
    op.drop_column('version_error', 'exception')
   