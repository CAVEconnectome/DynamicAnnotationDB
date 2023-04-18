"""Add parent_version column

Revision ID: 7c79eff751b4
Revises: ef5c2d7f96d8
Create Date: 2022-08-08 10:02:40.077429

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql
from sqlalchemy import engine_from_config
from sqlalchemy.engine import reflection

# revision identifiers, used by Alembic.
revision = "7c79eff751b4"
down_revision = "ef5c2d7f96d8"
branch_labels = None
depends_on = "ef5c2d7f96d8"


def _table_has_column(table, column):
    config = op.get_context().config
    engine = engine_from_config(
        config.get_section(config.config_ini_section), prefix="sqlalchemy."
    )

    insp = reflection.Inspector.from_engine(engine)
    return any(column in col["name"] for col in insp.get_columns(table))


def upgrade():
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
