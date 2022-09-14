import logging
import os

from sqlalchemy import create_engine

from alembic import script
from alembic.command import upgrade
from alembic.config import Config
from alembic.runtime import migration


def run_alembic_migration(sql_url: str):
    migrations_dir = os.path.dirname(os.path.realpath(__file__))

    config_file = os.path.join(migrations_dir, "alembic.ini")

    config = Config(file_=config_file)
    config.set_main_option("script_location", migrations_dir)
    config.set_main_option("sqlalchemy.url", sql_url)

    engine = create_engine(sql_url)
    script_ = script.ScriptDirectory.from_config(config)
    with engine.begin() as conn:
        return _migrate(conn, script_, config)


def _migrate(conn, script_, config):
    context = migration.MigrationContext.configure(conn)
    current_version = context.get_current_revision()
    target_version = script_.get_current_head()

    if current_version == target_version:
        return f"Database up to date with version: {target_version}"

    upgrade(config, "head")
    return f"Migration complete upgraded to {target_version}"
