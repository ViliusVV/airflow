__author__ = 'Vilius Valinskis'
__version__ = "0.6.123"

import base64
import logging

from airflow.hooks.base_hook import BaseHook
from airflow.models import DagBag, DagModel, DagRun, DAG
from airflow.plugins_manager import AirflowPlugin
from airflow import configuration
from airflow.www.app import csrf
from airflow import settings
from airflow.utils.state import State
from airflow.utils import timezone
from airflow.exceptions import TaskNotFound
from airflow.configuration import conf
from airflow.models.serialized_dag import SerializedDagModel
from airflow.exceptions import DagNotFound
from airflow.api.common.experimental import delete_dag

from typing import Any, Callable, Optional, Tuple, TypeVar, Union, cast
from functools import wraps

from flask import Blueprint, request, jsonify, Response
from flask_login.utils import _get_user

import airflow
import logging
import os
import jwt
import socket
import json
from flask_appbuilder import expose as app_builder_expose, BaseView as AppBuilderBaseView
from flask_jwt_extended.view_decorators import jwt_required, verify_jwt_in_request, _decode_jwt_from_request


import types
import importlib.machinery
import imp

import requests
from urllib3.util.retry import Retry
import time
from requests.adapters import HTTPAdapter


rest_api_endpoint = "/rest_api/api"


hostname = socket.gethostname()
airflow_version = airflow.__version__
rest_api_plugin_version = __version__


# Getting configurations from airflow.cfg file
airflow_webserver_base_url = conf.get('webserver', 'BASE_URL')
airflow_dags_folder = conf.get('core', 'DAGS_FOLDER')
read_dags_from_db = False
REST_EASAS_PERMISSSION = "AIRFLOW_REST"
REQUEST_TIMEOUT = 5


config_data = { "Base URL":        conf.get('WEBSERVER',        'base_url'),
                "Proxy fix:":      conf.getboolean("WEBSERVER", 'enable_proxy_fix'),
                "Min ser. update": conf.get('CORE',             'min_serialized_dag_update_interval'),
                "Do not pickle":   conf.getboolean('CORE',      'donot_pickle'),
                "Reload plugins":  conf.getboolean('WEBSERVER', 'reload_on_plugin_change'),
                "Auth backend":    conf.get('API',              'auth_backend'),
                "Executor":        conf.get('CORE',             'executor')
              }



config_data.items()

apis_metadata = [
    {
        "name": "deploy_dag",
        "description": "Deploy a new DAG File to the DAGs directory asdfasdfasdfasdfasdfadsfasdfasdfasdfadsfasdfasdfasdfasf",
        "http_method": "POST",
        "form_enctype": "multipart/form-data",
        "arguments": [],
        "post_arguments": [
            {"name": "dag_file", "description": "Python file to upload and deploy", "form_input_type": "file",
             "required": True},
            {"name": "force", "description": "Whether to forcefully upload the file if the file already exists or not",
             "form_input_type": "checkbox", "required": False},
            {"name": "pause",
             "description": "The DAG will be forced to be paused when created.",
             "form_input_type": "checkbox", "required": False}
        ]
    },
    {
        "name": "refresh_all_dags",
        "description": "Refresh all DAGs in the Web Server",
        "http_method": "GET",
        "arguments": []
    },
    {
        "name": "delete_dag",
        "description": "Delete a DAG in the Web Server from Airflow databas and filesystem",
        "http_method": "DELETE",
        "arguments": [
            {"name": "dag_id", "description": "The id of the dag", "form_input_type": "text", "required": True}
        ]
    },
    # {
    #     "name": "upload_file",
    #     "description": "upload a new File to the specified folder",
    #     "http_method": "POST",
    #     "form_enctype": "multipart/form-data",
    #     "arguments": [],
    #     "post_arguments": [
    #         {"name": "file", "description": "uploaded file", "form_input_type": "file", "required": True},
    #         {"name": "force", "description": "Whether to forcefully upload the file if the file already exists or not",
    #          "form_input_type": "checkbox", "required": False},
    #         {"name": "path", "description": "the path of file", "form_input_type": "text", "required": False}
    #     ]
    # },
    # {
    #     "name": "dag_state",
    #     "description": "Get the status of a dag run",
    #     "http_method": "GET",
    #     "arguments": [
    #         {"name": "dag_id", "description": "The id of the dag", "form_input_type": "text", "required": True},
    #         {"name": "run_id", "description": "The id of the dagRun", "form_input_type": "text", "required": True}
    #     ]
    # }
    # {
    #     "name": "task_instance_detail",
    #     "description": "Get the detail info of a task instance",
    #     "http_method": "GET",
    #     "arguments": [
    #         {"name": "dag_id", "description": "The id of the dag", "form_input_type": "text", "required": True},
    #         {"name": "run_id", "description": "The id of the dagRun", "form_input_type": "text", "required": True},
    #         {"name": "task_id", "description": "The id of the task", "form_input_type": "text", "required": True}
    #     ]
    # }
    # {
    #     "name": "restart_failed_task",
    #     "description": "restart failed tasks with downstream",
    #     "http_method": "GET",
    #     "arguments": [
    #         {"name": "dag_id", "description": "The id of the dag", "form_input_type": "text", "required": True},
    #         {"name": "run_id", "description": "The id of the dagRun", "form_input_type": "text", "required": True}
    #     ]
    # }
    # {
    #     "name": "kill_running_tasks",
    #     "description": "kill running tasks that status in ['none', 'running']",
    #     "http_method": "GET",
    #     "arguments": [
    #         {"name": "dag_id", "description": "The id of the dag", "form_input_type": "text", "required": True},
    #         {"name": "run_id", "description": "The id of the dagRun", "form_input_type": "text", "required": True},
    #         {"name": "task_id", "description": "If task_id is none, kill all tasks, else kill one task",
    #          "form_input_type": "text", "required": False}
    #     ]
    # },
    {
        "name": "run_task_instance",
        "description": "create dagRun, and run some tasks, other task skip",
        "http_method": "POST",
        "arguments": [
            {"name": "dag_id", "description": "The id of the dag", "form_input_type": "text", "required": True},
            {"name": "run_id", "description": "The id of the dagRun", "form_input_type": "text", "required": True},
            {"name": "tasks", "description": "The id of the tasks, Multiple tasks are split by (,)",
             "form_input_type": "text", "required": True},
            {"name": "conf", "description": "Conf of creating dagRun", "form_input_type": "text", "required": False}
        ]
    }
    # {
    #     "name": "skip_task_instance",
    #     "description": "skip one task instance",
    #     "http_method": "GET",
    #     "arguments": [
    #         {"name": "dag_id", "description": "The id of the dag", "form_input_type": "text", "required": True},
    #         {"name": "run_id", "description": "The id of the dagRun", "form_input_type": "text", "required": True},
    #         {"name": "task_id", "description": "The id of the task", "form_input_type": "text", "required": True}
    #     ]
    # }
]

class EAAuthService:
    """Keeps state of aunthentication service"""

    PUBLIC_KEY_ENDPOINT = "/rest/jwt/publicKey/"

    def __init__(self):
        self.easas_base_url = "http://localhost:8080"
        self.easas_context_root = "/EASAS"

        try:
            self.init_connection()
        except Exception as e:
            logging.error("Failed to load energyadvice connection!!!")
            logging.error(e)

    def init_connection(self):
        conn = BaseHook.get_connection("energyadvice")

        self.easas_base_url = conn.host
        self.easas_context_root = conn.schema

    def _get_public_key_link(self, kid: int):
        return f"{self.easas_base_url}{self.easas_context_root}{EAAuthService.PUBLIC_KEY_ENDPOINT}{str(kid)}"

    def fetch_public_key(self, kid: int):
        s = self.requests_retry_session()

        pub_link = self._get_public_key_link(kid)

        t0 = time.time()

        resp = None
        try:
            resp = s.get(pub_link, timeout=REQUEST_TIMEOUT)
        except Exception as x:
            print('It failed :(', x.__class__.__name__)
        else:
            print('It eventually worked', resp.status_code)
        finally:
            t1 = time.time()
            print('Took', t1 - t0, 'seconds')

            if resp is None or resp.status_code != 200:
                return None

            pub_key = resp.text

            return pub_key

    def get_public_cert(self, kid: int):
        pub_key_b64url = self.fetch_public_key(kid)

        pub_key_bytes = pub_key_b64url.encode("ASCII")
        pub_key_bytes = base64.urlsafe_b64decode(pub_key_bytes)

        pub_key_b64 = base64.b64encode(pub_key_bytes).decode('ASCII')

        pub_key_cert = f"""-----BEGIN PUBLIC KEY-----\n{pub_key_b64}\n-----END PUBLIC KEY-----\n"""

        print(f"B64Url Pub Key: {pub_key_b64}")
        print(f"B64 Pub Key: {pub_key_b64}")

        return pub_key_cert

    def veirify_jwt(self, jwt_str: str, pem_cert: str):
        return  jwt.decode(jwt_str, pem_cert, algorithms=["ES256"])


    # noinspection HttpUrlsUsage
    @staticmethod
    def requests_retry_session(
        retries=3,
        backoff_factor=1,
        status_forcelist=(500, 502, 504),
        session=None,
    ):
        session = session or requests.Session()
        retry = Retry(
            total=retries,
            read=retries,
            connect=retries,
            backoff_factor=backoff_factor,
            status_forcelist=status_forcelist,
        )
        adapter = HTTPAdapter(max_retries=retry)
        session.mount('http://', adapter)
        session.mount('https://', adapter)
        return session


_EA_AUTH_SERVICE = EAAuthService()


# Function used to validate the JWT Token

T = TypeVar("T", bound=Callable)  # pylint: disable=invalid-name

def jwt_verify(function: T):
    """Decorator for functions that require authentication"""

    @wraps(function)
    def decorated(*args, **kwargs):

        header = request.headers.get("Authorization")
        logging.info("Rest_API_Plugin.jwt_token_secure() called")

        if (header is None) or ("Bearer" not in header):
            message = "No or bad auth header"
            logging.warning(message)
            return ApiResponse.unauthorized(message=message)

        jwt_str = header.split()[1]

        header_b64 = jwt_str.split('.')[0]
        header_decoded = base64.b64decode(header_b64.encode("ASCII") + b'==')
        header_json = json.loads(header_decoded)
        kid = header_json['kid']

        pub_cert = _EA_AUTH_SERVICE.get_public_cert(kid)

        try:
            jwt_decoded = _EA_AUTH_SERVICE.veirify_jwt(jwt_str, pub_cert)
        except Exception as e:
            logging.warning("Failed JWT verification!!!")
            logging.warning(e)
            return ApiResponse.unauthorized(message=e)

        allowed = REST_EASAS_PERMISSSION in jwt_decoded['permissions']

        if(not allowed):
            ApiResponse.unauthorized(message=f"Token does not have {REST_EASAS_PERMISSSION} claim")

        return function(*args, **kwargs)

    return cast(T, decorated)


def commit_dag(dag):
    session = settings.Session()
    dag.sync_to_db(session=session)

    ser = SerializedDagModel.write_dag(dag=dag, session=session)
    print(f"Session before: {session}")
    session.commit()
    print(f"Session after commit: {session}")

    print(f"Serialized {ser}")

    dag_model = session.query(DagModel).filter(DagModel.dag_id == dag.dag_id).first()
    logging.info("dag_model:" + str(dag_model))

    return dag_model


def load_dag_as_module(save_file_path):
    """Load python file as module"""
    try:
        loader = importlib.machinery.SourceFileLoader('module.name', save_file_path)
        logging.info(f"Loader {loader}")

        dag_file = types.ModuleType(loader.name)
        logging.info(f"Loader {dag_file}")

        loader.exec_module(dag_file)
        logging.info(f"Loader {dag_file}")

        return dag_file
    except Exception as e:
        logging.warning("Failed to get dag_file")
        return None


class ApiResponse:
    def __init__(self):
        pass

    OK_200 = 200
    BAD_REQUEST_400 = 400
    UNAUTHORIZED_401 = 401
    FORBIDDEN_403 = 403
    NOT_FOUND_404 = 404
    SERVER_ERROR_500 = 500

    @staticmethod
    def standard_response(status, response_obj):
        json_data = json.dumps(response_obj)
        resp = Response(json_data, status=status, mimetype='application/json')
        return resp

    @staticmethod
    def success(response_obj={}):
        response_obj['status'] = 'success'
        return ApiResponse.standard_response(ApiResponse.OK_200, response_obj)

    @staticmethod
    def error(status, error):
        return ApiResponse.standard_response(status, {
            'error': error
        })

    @staticmethod
    def bad_request(error="Bad request", message = ""):
        return ApiResponse.error(ApiResponse.BAD_REQUEST_400, f"{error}. {message}")

    @staticmethod
    def not_found(error='Not found', message = ""):
        return ApiResponse.error(ApiResponse.NOT_FOUND_404,  f"{error}. {message}")

    @staticmethod
    def forbidden(error='Not authorized', message = ""):
        return ApiResponse.error(ApiResponse.UNAUTHORIZED_401, f"{error}. {message}")

    @staticmethod
    def unauthorized(error='Not authorized', message = ""):
        return ApiResponse.error(ApiResponse.FORBIDDEN_403, f"{error}. {message}")

    @staticmethod
    def server_error(error='Server error', message = ""):
        return ApiResponse.error(ApiResponse.SERVER_ERROR_500, f"{error}. {message}")


class ResponseFormat:
    def __init__(self):
        pass

    @staticmethod
    def format_dag_run_state(dag_run):
        return {
            'state': dag_run.get_state(),
            'startDate': (None if not dag_run.start_date else dag_run.start_date.strftime("%Y-%m-%dT%H:%M:%S.%f%z")),
            'endDate': (None if not dag_run.end_date else dag_run.end_date.strftime("%Y-%m-%dT%H:%M:%S.%f%z"))
        }

    @staticmethod
    def format_task_run_state(self):
        pass

    @staticmethod
    def format_dag_task(task_instance):
        return {
            'taskId': task_instance.task_id,
            'dagId': task_instance.dag_id,
            'state': task_instance.state,
            'tryNumber': (None if not task_instance._try_number else str(task_instance._try_number)),
            'maxTries': (None if not task_instance.max_tries else str(task_instance.max_tries)),
            'startDate': (
                None if not task_instance.start_date else task_instance.start_date.strftime("%Y-%m-%dT%H:%M:%S.%f%z")),
            'endDate': (
                None if not task_instance.end_date else task_instance.end_date.strftime("%Y-%m-%dT%H:%M:%S.%f%z")),
            'duration': (None if not task_instance.duration else str(task_instance.duration))
        }


class REST_API(AppBuilderBaseView):
    """API View which extends either flask AppBuilderBaseView or flask AdminBaseView"""

    @staticmethod
    def is_arg_not_provided(arg):
        return arg is None or arg == ""

    # Get the DagBag which has a list of all the current Dags
    @staticmethod
    def get_dagbag():
        return DagBag(dag_folder=settings.DAGS_FOLDER, read_dags_from_db=read_dags_from_db)

    @staticmethod
    def get_db_dags():
        from airflow import settings
        session = settings.Session

        print(session)

        dags = session.query(DagModel).all()

        print(dags)



    @staticmethod
    def get_argument(request, arg):
        return request.args.get(arg) or request.form.get(arg)

    # '/' Endpoint where the Admin page is which allows you to view the APIs available and trigger them
    @app_builder_expose('/')
    def list(self):
        logging.info("REST_API.list() called")

        # get the information that we want to display on the page regarding the dags that are available
        dagbag = self.get_dagbag()
        dags = []


        """Creating the REST_API_Plugin which extends the AirflowPlugin so its imported into Airflow"""

        for dag_id in dagbag.dags:
            orm_dag = DagModel.get_current(dag_id)
            dags.append({
                "dag_id": dag_id,
                "is_active": (not orm_dag.is_paused) if orm_dag is not None else False
            })

        return self.render_template("/rest_api_plugin/index.html",
                                    dags=dags,
                                    airflow_webserver_base_url=airflow_webserver_base_url,
                                    rest_api_endpoint=rest_api_endpoint,
                                    config_data=config_data,
                                    apis_metadata=apis_metadata,
                                    airflow_version=airflow_version,
                                    rest_api_plugin_version=rest_api_plugin_version,
                                    )


    # '/api' REST Endpoint where API requests should all come in
    @csrf.exempt  # Exempt the CSRF token
    @app_builder_expose('/api', methods=["GET", "POST", "DELETE"])  # for Flask AppBuilder
    @jwt_verify
    def api(self):
        # Get the api that you want to execute
        api = self.get_argument(request, 'api')

        # Validate that the API is provided
        if self.is_arg_not_provided(api):
            logging.warning("api argument not provided")
            return ApiResponse.bad_request("API should be provided")

        api = api.strip().lower()
        logging.info("REST_API.api() called (api: " + str(api) + ")")

        # Get the api_metadata from the api object list that correcsponds to the api we want to run to get the metadata.
        api_metadata = None
        for test_api_metadata in apis_metadata:
            if test_api_metadata["name"] == api:
                api_metadata = test_api_metadata
        if api_metadata is None:
            logging.info("api '" + str(api) + "' was not found in the apis list in the REST API Plugin")
            return ApiResponse.bad_request("API '" + str(api) + "' was not found")

        # check if all the required arguments are provided
        missing_required_arguments = []
        dag_id = None
        for argument in api_metadata["arguments"]:
            argument_name = argument["name"]
            argument_value = self.get_argument(request, argument_name)
            if argument["required"]:
                if self.is_arg_not_provided(argument_value):
                    missing_required_arguments.append(argument_name)
            if argument_name == "dag_id" and argument_value is not None:
                dag_id = argument_value.strip()
        if len(missing_required_arguments) > 0:
            logging.warning("Missing required arguments: " + str(missing_required_arguments))
            return ApiResponse.bad_request("The argument(s) " + str(missing_required_arguments) + " are required")


        # Check to make sure that the DAG you're referring to, already exists.
        dag_bag = self.get_dagbag()
        if api != "delete_dag":
            if dag_id is not None and dag_id not in dag_bag.dags:
                logging.info("DAG_ID '" + str(dag_id) + "' was not found in the DagBag list '" + str(dag_bag.dags) + "'")
                return ApiResponse.bad_request("The DAG ID '" + str(dag_id) + "' does not exist")

        # Deciding which function to use based off the API object that was requested.
        # Some functions are custom and need to be manually routed to.
        final_response = ApiResponse.not_found(message="API not found")

        if api == "deploy_dag":
            final_response = self.deploy_dag()
        elif api == "delete_dag":
            final_response = self.delete_dag()
        elif api == "dag_state":
            final_response = self.dag_state()
        elif api == "run_task_instance":
            final_response = self.run_task_instance()

        return final_response

    def deploy_dag(self):
        """Custom Function for the deploy_dag API
        Upload dag file，and refresh dag to session

        args:
            dag_file: the python file that defines the dag
            force: whether to force replace the original dag file
            pause: disabled dag
            unpause: enabled dag
        """
        logging.info("Executing custom 'deploy_dag' function")

        # Check if post has any file
        if 'dag_file' not in request.files or request.files['dag_file'].filename == '':
            logging.warning("The dag_file argument wasn't provided")
            return ApiResponse.bad_request("dag_file should be provided")

        dag_file = request.files['dag_file']
        logging.info("DAG file: " + str(dag_file))

        force = True if self.get_argument(request, 'force') is not None else False
        logging.info("DAG force upload: " + str(force))

        pause = True if self.get_argument(request, 'pause') is not None else False
        logging.info("DAG deploy as paused: " + str(pause))


        # Ensure that file is python script
        if dag_file and dag_file.filename.endswith(".py"):
            save_file_path = os.path.join(airflow_dags_folder, dag_file.filename)

            # Check if the file already exists.
            if os.path.isfile(save_file_path) and not force:
                msg = "The file '" + save_file_path + "' already exists on host '" + hostname
                logging.warning(msg)
                return ApiResponse.bad_request(msg)

            # Save file
            logging.info("Saving file to '" + save_file_path + "'")
            dag_file.save(save_file_path)
        else:
            msg = "File is not a python file. It must have .py extension"
            logging.warning(msg)
            return ApiResponse.bad_request(msg)


        dag_file = load_dag_as_module(save_file_path)

        try:
            if dag_file is None or dag_file.dag is None:
                warning = "Failed to get dag"
                logging.warning(warning)
                return ApiResponse.server_error("DAG File [{}] has been uploaded".format(dag_file))
        except Exception:
            warning = "Failed to get dag from dag_file"
            logging.warning(warning)
            return ApiResponse.server_error("Failed to get dag from DAG File [{}]".format(dag_file))

        dag_id = dag_file.dag.dag_id
        logging.info(f"Dagid {dag_id}")

        # Get all dags
        dagbag = self.get_dagbag()

        # Get current dag
        dag = dagbag.get_dag(dag_id)
        logging.info(f"Dag {dag}")

        # Write/Sync dag to DB and get DAG model
        dag_model = commit_dag(dag)

        # dag_model.set_is_paused(is_paused=paused)
        return ApiResponse.success({"message": "DAG [{}] has been successfully uploaded".format(dag_file)})

    @staticmethod
    def refresh_all_dags():
        """Custom Function for the refresh_all_dags API.
        Refresh all dags.
        """
        logging.info("Executing custom 'refresh_all_dags' function")

        try:
            session = settings.Session()
            orm_dag_list = session.query(DagModel).all()
            for orm_dag in orm_dag_list:
                if orm_dag:
                    orm_dag.last_expired = timezone.utcnow()
                    session.merge(orm_dag)
            session.commit()
        except Exception as e:
            error_message = "An error occurred while trying to Refresh all the DAGs: " + str(e)
            logging.error(error_message)
            return ApiResponse.server_error(error_message)

        return ApiResponse.success({
            "message": "All DAGs are now up to date"
        })

    def delete_dag(self):
        """Delete DAG form Airflow DB and its file"""
        logging.info("Executing 'dag_delete' function")

        dag_id = self.get_argument(request, 'dag_id')
        logging.info(f"DAG to delete: {str(dag_id)}")

        dag_full_path = airflow_dags_folder + os.sep + dag_id + ".py"

        if os.path.exists(dag_full_path):
            os.remove(dag_full_path)

        try:
            deleted_dags = delete_dag.delete_dag(dag_id, keep_records_in_log=False)
        except DagNotFound as e:
            logging.info("DAG not found to delete")
            return ApiResponse.success({"message": "No such DAG [{}] to delete".format(dag_id)})

        if deleted_dags > 0:
            logging.info(f"Deleted dag {dag_id}")
            return ApiResponse.success({"message": "DAG [{}] deleted".format(dag_id)})
        else:
            logging.info("No dags deleted")
            return ApiResponse.success({"message": "No such DAG [{}] to delete".format(dag_id)})


    def dag_state(self):
        """Get dag_run from session according to dag_id and run_id,
        return the state, startDate, endDate fields in dag_run

        args:
            dag_id: dag id
            run_id: the run id of dag run
        """
        logging.info("Executing custom 'dag_state' function")

        dag_id = self.get_argument(request, 'dag_id')
        run_id = self.get_argument(request, 'run_id')

        session = settings.Session()
        query = session.query(DagRun)
        dag_run = query.filter(
            DagRun.dag_id == dag_id,
            DagRun.run_id == run_id
        ).first()

        if dag_run is None:
            return ApiResponse.not_found("dag run is not found")

        res_dag_run = ResponseFormat.format_dag_run_state(dag_run)
        session.close()

        return ApiResponse.success(res_dag_run)

    def task_instance_detail(self):
        """Obtain task_instance from session according to dag_id, run_id and task_id,
        and return taskId, dagId, state, tryNumber, maxTries, startDate, endDate, duration fields in task_instance

        args:
            dag_id: dag id
            run_id: the run id of dag run
            task_id: the task id of task instance of dag
        """
        logging.info("Executing custom 'task_instance_detail' function")

        dag_id = self.get_argument(request, 'dag_id')
        run_id = self.get_argument(request, 'run_id')
        task_id = self.get_argument(request, 'task_id')

        session = settings.Session()
        query = session.query(DagRun)
        dag_run = query.filter(
            DagRun.dag_id == dag_id,
            DagRun.run_id == run_id
        ).first()

        if dag_run is None:
            return ApiResponse.not_found("dag run is not found")

        logging.info('dag_run：' + str(dag_run))

        task_instance = DagRun.get_task_instance(dag_run, task_id)

        if task_instance is None:
            return ApiResponse.not_found("dag task is not found")

        logging.info('task_instance：' + str(task_instance))

        res_task_instance = ResponseFormat.format_dag_task(task_instance)
        session.close()

        return ApiResponse.success(res_task_instance)

    def restart_failed_task(self):
        """Restart the failed task in the specified dag run.
        According to dag_id, run_id get dag_run from session,
        query task_instances that status is FAILED in dag_run,
        restart them and clear status of all task_instance's downstream of them.

        args:
            dag_id: dag id
            run_id: the run id of dag run
        """
        logging.info("Executing custom 'restart_failed_task' function")

        dagbag = self.get_dagbag()

        dag_id = self.get_argument(request, 'dag_id')
        run_id = self.get_argument(request, 'run_id')

        session = settings.Session()
        query = session.query(DagRun)
        dag_run = query.filter(
            DagRun.dag_id == dag_id,
            DagRun.run_id == run_id
        ).first()

        if dag_run is None:
            return ApiResponse.not_found("dag run is not found")

        if dag_id not in dagbag.dags:
            return ApiResponse.bad_request("Dag id {} not found".format(dag_id))

        dag = dagbag.get_dag(dag_id)

        if dag is None:
            return ApiResponse.not_found("dag is not found")

        tis = DagRun.get_task_instances(dag_run, State.FAILED)
        logging.info('task_instances: ' + str(tis))

        failed_task_count = len(tis)
        if failed_task_count > 0:
            for ti in tis:
                dag = DAG.sub_dag(
                    self=dag,
                    task_regex=r"^{0}$".format(ti.task_id),
                    include_downstream=True,
                    include_upstream=False)

                count = DAG.clear(
                    self=dag,
                    start_date=dag_run.execution_date,
                    end_date=dag_run.execution_date,
                )
                logging.info('count：' + str(count))
        else:
            return ApiResponse.not_found("dagRun don't have failed tasks")

        session.close()

        return ApiResponse.success({
            'failed_task_count': failed_task_count,
            'clear_task_count': count
        })

    def kill_running_tasks(self):
        """Stop running the specified task instance and downstream tasks.
        Obtain task_instance from session according to dag_id, run_id and task_id,
        If task_id is not empty, get task_instance with RUNNIN or NONE status from dag_run according to task_id,
          and set task_instance status to FAILED.
        If task_id is empty, get all task_instances whose status is RUNNIN or NONE from dag_run,
          and set the status of these task_instances to FAILED.

        args:
            dag_id: dag id
            run_id: the run id of dag run
            task_id: the task id of task instance of dag
        """
        logging.info("Executing custom 'kill_running_tasks' function")

        dagbag = self.get_dagbag()

        dag_id = self.get_argument(request, 'dag_id')
        run_id = self.get_argument(request, 'run_id')
        task_id = self.get_argument(request, 'task_id')

        session = settings.Session()
        query = session.query(DagRun)
        dag_run = query.filter(
            DagRun.dag_id == dag_id,
            DagRun.run_id == run_id
        ).first()

        if dag_run is None:
            return ApiResponse.not_found("dag run is not found")

        if dag_id not in dagbag.dags:
            return ApiResponse.bad_request("Dag id {} not found".format(dag_id))

        dag = dagbag.get_dag(dag_id)
        logging.info('dag: ' + str(dag))
        logging.info('dag_subdag: ' + str(dag.subdags))

        tis = []
        if task_id:
            task_instance = DagRun.get_task_instance(dag_run, task_id)
            if task_instance is None or task_instance.state not in [State.RUNNING, State.NONE]:
                return ApiResponse.not_found("task is not found or state is neither RUNNING nor NONE")
            else:
                tis.append(task_instance)
        else:
            tis = DagRun.get_task_instances(dag_run, [State.RUNNING, State.NONE])

        logging.info('tis: ' + str(tis))
        running_task_count = len(tis)

        if running_task_count > 0:
            for ti in tis:
                ti.state = State.FAILED
                ti.end_date = timezone.utcnow()
                session.merge(ti)
                session.commit()
        else:
            return ApiResponse.not_found("dagRun don't have running tasks")

        session.close()

        return ApiResponse.success()

    def run_task_instance(self):
        """Run some tasks, other tasks do not run
        According to dag_id, run_id get dag_run from session,
        Obtain the task instances that need to be run according to tasks，
        Define the status of these task instances as None,
        and define the status of other task instances that do not need to run as SUCCESS

        args:
            dag_id: dag id
            run_id: the run id of dag run
            tasks: the task id of task instance of dag, Multiple task ids are split by ','
            conf: define dynamic configuration in dag
        """
        logging.info("Executing custom 'run_task_instance' function")

        dagbag = self.get_dagbag()

        dag_id = self.get_argument(request, 'dag_id')
        run_id = self.get_argument(request, 'run_id')
        tasks = self.get_argument(request, 'tasks')
        conf = self.get_argument(request, 'conf')

        run_conf = None
        if conf:
            try:
                run_conf = json.loads(conf)
            except ValueError:
                return ApiResponse.error('Failed', 'Invalid JSON configuration')

        # Check if DAG run allready exists
        dr = DagRun.find(dag_id=dag_id, run_id=run_id)
        if dr:
            return ApiResponse.not_found('run_id {} already exists'.format(run_id))

        logging.info('tasks: ' + str(tasks))
        task_list = tasks.split(',')

        session = settings.Session()

        if dag_id not in dagbag.dags:
            return ApiResponse.not_found("Dag id {} not found".format(dag_id))

        dag = dagbag.get_dag(dag_id)
        logging.info('dag: ' + str(dag))

        for task_id in task_list:
            try:
                task = dag.get_task(task_id)
            except TaskNotFound:
                return ApiResponse.not_found("dag task of {} is not found".format(str(task_id)))
            logging.info('task：' + str(task))

        execution_date = timezone.utcnow()

        dag_run = dag.create_dagrun(
            run_id=run_id,
            execution_date=execution_date,
            state=State.RUNNING,
            conf=run_conf,
            external_trigger=True
        )

        tis = dag_run.get_task_instances()

        # We want to run only single task of DAG,
        # so other DAG tasks are marked as SUCCESS in advance and not run
        for ti in tis:
            if ti.task_id in task_list:
                ti.state = None
            else:
                ti.state = State.SUCCESS
            session.merge(ti)

        session.commit()
        session.close()

        return ApiResponse.success({
            "execution_date": (execution_date.strftime("%Y-%m-%dT%H:%M:%S.%f%z"))
        })

    def skip_task_instance(self):
        """Skip the specified task instance and downstream tasks.
        Obtain task instance from session according to dag_id, run_id and task_id,
        define the state of this task instance as SKIPPED.

        args:
            dag_id: dag id
            run_id: the run id of dag run
            task_id: the task id of task instance of dag
        """
        logging.info("Executing custom 'skip_task_instance' function")

        dag_id = self.get_argument(request, 'dag_id')
        run_id = self.get_argument(request, 'run_id')
        task_id = self.get_argument(request, 'task_id')

        session = settings.Session()
        query = session.query(DagRun)
        dag_run = query.filter(
            DagRun.dag_id == dag_id,
            DagRun.run_id == run_id
        ).first()

        if dag_run is None:
            return ApiResponse.not_found("dag run is not found")

        logging.info('dag_run：' + str(dag_run))

        task_instance = DagRun.get_task_instance(dag_run, task_id)

        if task_instance is None:
            return ApiResponse.not_found("dag task is not found")

        logging.info('task_instance：' + str(task_instance))

        task_instance.state = State.SKIPPED
        session.merge(task_instance)
        session.commit()
        session.close()

        return ApiResponse.success()


# Creating View to be used by Plugin
rest_api_view = {"category": "Admin", "name": "REST API Plugin", "view": REST_API()}

# Creating Blueprint
rest_api_bp = Blueprint(
    "rest_api_bp",
    __name__,
    template_folder='templates',
    static_folder='static',
    static_url_path='/static/'
)

# Creating Blueprint
rest_api_blueprint = Blueprint(
    'rest_api_blueprint',
    __name__,
    url_prefix='/rest/api'
)


# Airflow plugin init
class REST_API_Plugin(AirflowPlugin):
    name = "rest_api"
    operators = []
    appbuilder_views = [rest_api_view]
    flask_blueprints = [rest_api_bp, rest_api_blueprint]
    hooks = []
    executors = []
    admin_views = [rest_api_view]
    menu_links = []
