# vim: set encoding=utf-8

#
#  Copyright (c) 2015 Intel Corporation 
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
#

"""
Python-scoring-pipeline-executor
"""
from flask import Flask, request, jsonify, make_response

from flask.ext.httpauth import HTTPBasicAuth
from flask.ext.api import status, exceptions
import sys, os
import tarfile
import json
from werkzeug import secure_filename

from threading import Thread

ALLOWED_EXTENSIONS = set(['tar'])

ScoringPipeline = Flask(__name__, static_url_path="")
authorize = HTTPBasicAuth()

@ScoringPipeline.route('/v1/')
def welcome_page():
    return "Welcome to Scoring Pipeline Executor"

@authorize.get_password
def get_password(username):
    if username == 'atkuser':
        return True
    return None

@authorize.error_handler
def authorization_reject():
    return make_response(jsonify({'error': 'Bad credentials. Access Denied'}), 403)

@ScoringPipeline.errorhandler(400)
def bad_request(error):
    return make_response(jsonify({'error': 'Malformed request'}), 400)

@ScoringPipeline.errorhandler(404)
def resource_not_found(error):
    return make_response(jsonify({'error': 'Resource not found'}), 404)

@ScoringPipeline.errorhandler(500)
def internal_server_error(error):
    # StreamingDag.logger.error('Server Error %s', (error))
    return make_response(jsonify({'error': 'Server Error'}), 500)

def allowed_file(filename):
    return '.' in filename and \
           filename.rsplit('.', 1)[1] in ALLOWED_EXTENSIONS

@ScoringPipeline.route("/", methods=['POST', 'PUT'])
def upload_file():
    import tasks
    if tasks.uploaded == False:
        if request.method == 'POST':
            file = request.files['file']
            if file and allowed_file(file.filename):
                filename = secure_filename(file.filename)
                file.save(os.path.join(os.getcwd(), filename))
                _extract_and_install(filename)
                tasks.uploaded = True
                return make_response("Uploaded file", 200)
    else:
        return make_response("Cannot upload another file as the Scoring Pipeline has already been initialized", 500)


@ScoringPipeline.route("/")
def hello():
    return make_response("hello", 200)

@ScoringPipeline.route('/v1/score', methods=['POST'])
def score():
    import tasks
    if len(tasks.dag) != 0:
        if request.headers['Content-type'] == 'application/json':
            try:
                if isinstance(tasks.dag[0], tasks.sourcetask):
                    return "Scoring Pipeline is being executed via Kafka. Simultaneous execution of Scoring Pipeline via REST is not allowed"
                else:
                    return str(tasks.executedag(request.json["message"], 0, len(tasks.dag)))
            except Exception as e:
                return make_response(str(e), 500)
        else:
            return "415 Unsupported media type"

    else:
        return "Pipeline has not been initialized. Please initialize Scoring Pipeline using the upload API with the tar containing the UDFs"

def _makesimpledag():
    import tasks
    tasks.dag = [None] * len(tasks.nodes)
    if len(tasks.nodes) > 1:
        for node in tasks.nodes:
            if isinstance(node, tasks.sourcetask):
                tasks.dag[0] = node
            elif isinstance(node, tasks.generaltask):
                tasks.dag[1] = node
            elif isinstance(node, tasks.sinktask):
                tasks.dag[2] = node
            else:
                sys.stderr.write("Found an unexpected task {0} while executing single UDF scoring.\n".format(node))
    else:
        tasks.dag[0] = tasks.nodes[0]

    if isinstance(tasks.dag[0], tasks.sourcetask):
        thread = Thread(target = tasks.dag[0].execute)
        thread.start()
        return 'OK', status.HTTP_200_OK


def _extract_and_install(tar_file):
    try:
        tar = tarfile.open(tar_file)
    except:
        print("exception:")
    tar.extractall()
    print("Extracting")
    import tasks
    import atktypes
    jsonmembers = []
    source = False
    sink = False
    general_task = False
    tasks.nodes[:] = []
    for member in tar.getmembers():
        if os.path.splitext(member.name)[1] == ".json":
            jsonmembers.append(tar.extractfile(member))
    for file in jsonmembers:
        data = json.load(file)
        services = json.loads(os.getenv("VCAP_SERVICES"))
        kafka_URI = services["kafka"][0]["credentials"]["uri"]
        if(data["src_topic"]) != "":
            tasks.nodes.append(tasks.sourcetask(kafka_URI,  data["src_topic"]))
            source = True
        if(data["file_name"]) != "":
            input_schema = [(entry["name"], atktypes.valid_data_types.get_from_string(entry["type"])) for entry in data["input_schema"]["columns"]]
            #sys.stderr.write("input_schema=%s\n" % input_schema)
            tasks.nodes.append(tasks.generaltask(data["file_name"], data["func_name"], input_schema))
            general_task = True
        if(data["sink_topic"]) != "":
            tasks.nodes.append(tasks.sinktask(kafka_URI, data["sink_topic"]))
            sink = True

        if not sink and not source:
            sys.stderr.write("No sink and source nodes were provided. Assuming scoring will happen from REST endpoint.")

    if source and not sink:
        sys.stderr.write("No sink node was provided. Please provide a valid sink for output.")
        sys.exit(1)

    if not source and sink:
        sys.stderr.write(" No source node was provided. Please provide a valid source for input")
        sys.exit(1)

    if general_task:
        _makesimpledag()
    tar.close()

if __name__ == '__main__':
    if len(sys.argv) == 2:
        _extract_and_install(sys.argv[1])
        ScoringPipeline.run()
    elif len(sys.argv) == 3:
        _extract_and_install(sys.argv[1])
        port = int(sys.argv[2])
        ScoringPipeline.run(port=port)
    else:
        port = int(os.getenv("PORT"))
        ScoringPipeline.run(host="0.0.0.0", port=port)


























