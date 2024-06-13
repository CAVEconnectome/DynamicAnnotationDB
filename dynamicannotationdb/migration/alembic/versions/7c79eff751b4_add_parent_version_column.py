"""Add parent_version column

Revision ID: 7c79eff751b4
Revises: ef5c2d7f96d8
Create Date: 2022-08-08 10:02:40.077429

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.engine import reflection

# revision identifiers, used by Alembic.
revision = "7c79eff751b4"
down_revision = "ef5c2d7f96d8"
branch_labels = None
depends_on = "ef5c2d7f96d8"


def get_tables(connection):
    inspector = reflection.Inspector.from_engine(connection)
    return inspector.get_table_names()


def _table_has_column(connection, table, column):
    insp = reflection.Inspector.from_engine(connection)
    return any(column in col["name"] for col in insp.get_columns(table))

def upgrade():
    connection = op.get_bind()
    if not _table_has_column(connection, "analysisversion", "parent_version"):  
    
        with op.batch_alter_table("analysisversion", schema=None) as batch_op:
            op.add_column(
                "analysisversion",
                sa.Column("parent_version", sa.Integer(), nullable=True),
            )
            op.create_foreign_key(
                None, "analysisversion", "analysisversion", ["parent_version"], ["id"]
            )


def downgrade():
    op.drop_constraint(None, "analysisversion", type_="foreignkey")
    op.drop_column("analysisversion", "parent_version")
