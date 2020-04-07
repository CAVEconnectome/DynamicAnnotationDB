from flask import Flask, jsonify, url_for
from flask_sqlalchemy import SQLAlchemy
from flask_restx import Api

__version__ = "0.1.0"

db = SQLAlchemy()

def has_no_empty_params(rule):
    defaults = rule.defaults if rule.defaults is not None else ()
    arguments = rule.arguments if rule.arguments is not None else ()
    return len(defaults) >= len(arguments)


def create_app(config=None):
    from dynamicannotationdb.api import api as anno_api
    from dynamicannotationdb.config import configure_app
    import logging

    app = Flask(__name__)
    logging.basicConfig(level=logging.INFO)
    if config:
        app = configure_app(app, config)
    else:
        app = configure_app(app)
    api = Api(app, title="Annotation API", version="0.1.0")
    api.add_namespace(anno_api, path="/api/annotation")

    db.init_app(app)
    
    with app.app_context():
        db.create_all()
    
    
    @app.route("/health")
    def health():
        return jsonify("healthy")
   
    @app.route("/site-map")
    def site_map():
        links = []
        for rule in app.url_map.iter_rules():
            # Filter out rules we can't navigate to in a browser
            # and rules that require parameters
            if "GET" in rule.methods and has_no_empty_params(rule):
                url = url_for(rule.endpoint, **(rule.defaults or {}))
                links.append((url, rule.endpoint))
        # links is now a list of url, endpoint tuples
        return jsonify(links)
    return app

