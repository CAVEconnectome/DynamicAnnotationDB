"""Add parent_version column

Revision ID: 7c79eff751b4
Revises: ef5c2d7f96d8
Create Date: 2022-08-08 10:02:40.077429

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = '7c79eff751b4'
down_revision = 'ef5c2d7f96d8'
branch_labels = None
depends_on = None


def upgrade():
    op.add_column('analysisversion', sa.Column('parent_version', sa.Integer(), nullable=True))
    op.create_foreign_key(None, 'analysisversion', 'analysisversion', ['parent_version'], ['id'])


def downgrade():
    op.drop_constraint(None, 'analysisversion', type_='foreignkey')
    op.drop_column('analysisversion', 'parent_version')
