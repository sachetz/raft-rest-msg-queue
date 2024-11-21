from flask import Flask

def create_app(node):
    app = Flask(__name__)
    app.config['NODE'] = node

    with app.app_context():
        from . import routes
        routes.init_routes(app)

    return app