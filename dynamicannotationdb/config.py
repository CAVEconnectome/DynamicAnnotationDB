import os
from typing import List, Type
import logging
from emannotationschemas.models import Base
from flask_sqlalchemy import SQLAlchemy

BASE_DIR = os.path.abspath(os.path.dirname(__file__))

class BaseConfig:
    HOME = os.path.expanduser("~")
    SQLALCHEMY_DATABASE_URI = "postgres://postgres:annodb@db:5432/annodb"
    USE_MOCK_EQUIVALENCY = False
    DEBUG = True
    SQLALCHEMY_TRACK_MODIFICATIONS = False


class DevConfig(BaseConfig):
    DEBUG = True
    SQLALCHEMY_TRACK_MODIFICATIONS = False
    TESTING = False


class TestConfig(BaseConfig):
    DEBUG = True
    SQLALCHEMY_TRACK_MODIFICATIONS = False
    TESTING = True
    db_path = os.path.join(BASE_DIR, 'db.sqlite3')
    SQLALCHEMY_DATABASE_URI = "sqlite:///{0}/app-test.db".format(db_path)


config = {
    "default": "dynamicannotationdb.config.BaseConfig",
    "development": "dynamicannotationdb.config.DevConfig",
    "test": "dynamicannotationdb.config.TestConfig",
}


def configure_app(app, config_mode='default'):
    config_name = os.getenv('FLASK_CONFIGURATION', config_mode)
    print(config[config_name])
    # object-based default configuration
    app.config.from_object(config[config_name])
    if 'ANNOTATION_ENGINE_SETTINGS' in os.environ.keys():
        app.config.from_envvar('ANNOTATION_ENGINE_SETTINGS')
    # instance-folders configuration
    app.config.from_pyfile('config.cfg', silent=True)
    app.logger.debug(app.config)
    db = SQLAlchemy(model_class=Base)
    db.init_app(app)
    app.app_context().push()
    return app
