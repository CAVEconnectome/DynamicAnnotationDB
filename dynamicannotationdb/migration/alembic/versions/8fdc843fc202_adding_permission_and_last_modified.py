"""adding permission and last modified

Revision ID: 8fdc843fc202
Revises: 6e7f580ff680
Create Date: 2022-10-17 14:11:33.017738

"""
from alembic import op
import sqlalchemy as sa

from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = "8fdc843fc202"
down_revision = "6e7f580ff680"
branch_labels = None
depends_on = None


def upgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.add_column(
        "annotation_table_metadata",
        sa.Column(
            "write_permission",
            postgresql.ENUM("PRIVATE", "GROUP", "PUBLIC", name="read_permission"),
            nullable=True,
        ),
    )
    op.add_column(
        "annotation_table_metadata",
        sa.Column(
            "read_permission",
            postgresql.ENUM("PRIVATE", "GROUP", "PUBLIC", name="read_permission"),
            nullable=True,
        ),
    )
    op.add_column(
        "annotation_table_metadata",
        sa.Column("last_modified", sa.DateTime(), nullable=True),
    )
    # ### end Alembic commands ###
    op.execute("UPDATE annotation_table_metadata SET read_permission = 'PUBLIC'")
    op.execute("UPDATE annotation_table_metadata SET write_permission = 'PRIVATE'")
    op.execute("UPDATE annotation_table_metadata SET last_modified = current_timestamp")

    op.alter_column("annotation_table_metadata", "write_permission", nullable=False)
    op.alter_column("annotation_table_metadata", "read_permission", nullable=False)
    op.alter_column("annotation_table_metadata", "last_modified", nullable=False)


def downgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_column("annotation_table_metadata", "last_modified")
    op.drop_column("annotation_table_metadata", "read_permission")
    op.drop_column("annotation_table_metadata", "write_permission")
    # ### end Alembic commands ###
