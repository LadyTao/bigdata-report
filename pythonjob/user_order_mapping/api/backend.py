#from flask.ext.cors import CORS
from flask_cors import CORS
from pprintpp import pprint as pp
from flask import Flask, request, Response, jsonify
from flask_session import Session
from flask_restful import reqparse, abort, Api, Resource
import optparse
import os
import sys
sys.path.append('..')
sys.path.append('.')
import settings
from api import vp_api

def create_app():

    app = Flask(__name__)
    app.register_blueprint(vp_api)
    app.register_blueprint(funnel_api)

    cors = CORS(app)
    sess = Session()
    sess.init_app(app)
    return app


if __name__ == "__main__":

    parser = optparse.OptionParser()
    options, _ = parser.parse_args()

    pp(options)
    app = create_app()
    api = Api(app)

    pp(app.config)
    app.run(host='0.0.0.0', port=settings.port,
                            threaded=True,
                            use_reloader=True)

