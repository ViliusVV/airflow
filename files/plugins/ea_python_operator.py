import json
import os
import signal
import time
from subprocess import PIPE, STDOUT, Popen
from tempfile import TemporaryDirectory

from urllib3 import HTTPConnectionPool
import urllib3

import requests
from urllib3.util.retry import Retry

import logging
from airflow.exceptions import AirflowException
from airflow.models.connection import Connection
from airflow.hooks.base_hook import BaseHook
from airflow.models.baseoperator import BaseOperator
from airflow.models.dag import DagContext
from airflow.utils.operator_helpers import context_to_airflow_vars
from requests.adapters import HTTPAdapter

# 172.23.145.110

class EAPythonTaskOperator(BaseOperator):
    RETRIES = 10
    FETCH_TIMEOUT = 30
    POST_TIMEOUT = 60

    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)

        self.base_url = ""
        self.context_root = ""
        self.secret = ""

        self.output_encoding: str = "utf-8"

        self.current_period: int = 0
        self.total_periods: int = 1

        self.is_started = False

        self.sub_process = None

    def init_connection(self):
        conn = BaseHook.get_connection("energyadvice")

        self.base_url = conn.host
        self.context_root = conn.schema
        self.secret = conn.password

    def get_base_url(self):
        return f"{self.base_url}{self.context_root}"

    def get_rest_base_url(self):
        return f"{self.get_base_url()}/rest/airflow/dag"


    def get_config_link(self):
        return f"{self.get_rest_base_url()}/{self.dag_id}/{self.task_id}/jobConfig"

    def get_task_data_link(self, task_period):
        """Get tasks data link"""

        calc_id = str(task_period)

        url = f"{self.get_rest_base_url()}/{self.dag_id}/{self.task_id}/getJobData?calcPeriod={calc_id}&calcTimestamp={self.now_time}"

        return url

    def get_post_task_data_link(self, task_period):
        """Get tasks data link"""

        calc_id = str(task_period)

        url = f"{self.get_rest_base_url()}/{self.dag_id}/{self.task_id}/postJobData?calcPeriod={calc_id}&calcTimestamp={self.now_time}"

        return url

    def get_bearer_header(self, additional_headers = {}):
        auth = {"Authorization": f"Bearer {self.secret}"}
        final_header = {**auth, **additional_headers}
        return final_header

    def get_task_config(self, session: requests.sessions.Session):
        """Get task's config from EA endpoint"""

        endpoint = self.get_config_link();

        logging.info(f"Getting config for airflow task {self.dag_id}:{self.task_id} at {endpoint}...")

        headers = self.get_bearer_header()
        r = session.get(endpoint,
                        headers=headers,
                        timeout=EAPythonTaskOperator.FETCH_TIMEOUT)

        if r.status_code != 200:
            logging.error(f"Failed to get airflow task config. Error {r.status_code}")
            exit(500)

        json_data = r.json()

        logging.info(f"Task config fetch finished! JSON:\n{json_data}")

        return json_data

    def get_task_data(self, calc_period, jobDataMap, session: requests.sessions.Session):
        """Get task's data form EA endpoint"""

        endpoint = self.get_task_data_link(task_period=calc_period)

        logging.info(f"Fetching task data for airflow task {self.dag_id}:{self.task_id} at {endpoint}...")

        jdmStr = str(jobDataMap)

        headers = self.get_bearer_header({"Content-Type": "application/json"})
        r = session.post(endpoint,
                        headers=headers,
                        data=jdmStr,
                        timeout=EAPythonTaskOperator.FETCH_TIMEOUT)

        if r.status_code != 200:
            logging.error(f"Data fetch failed! Error {r.status_code}")
            exit(500)

        json_data = r.json()

        logging.info(f"Data fetch finished! JSON:\n{json_data}")

        return json_data

    def post_task_data(self, calc_period, jobDataMap, jsonData, session: requests.sessions.Session):
        """Post task data back to server"""

        endpoint = self.get_post_task_data_link(task_period=calc_period)

        logging.info("Posting data...")

        dataStr = ""

        # Json data empty
        if(jsonData != None):
            jsonData['jdm'] = jobDataMap
            dataStr = str(jsonData)

        headers = self.get_bearer_header({"Content-Type": "application/json"})
        r = session.post(endpoint,
                         headers=headers,
                         data=dataStr.encode("utf-8"),
                         timeout=EAPythonTaskOperator.POST_TIMEOUT)

        if r.status_code != 200:
            logging.error(f"Error occurred posting data to server! Error: {r.status_code}")
            exit(500)

    def pre_exec(self):
        # Restore default signal disposition and invoke setsid
        for sig in ('SIGPIPE', 'SIGXFZ', 'SIGXFSZ'):
            if hasattr(signal, sig):
                signal.signal(getattr(signal, sig), signal.SIG_DFL)
        os.setsid()

    def execute_ea_python_task(self, code, json_data, env):
        """Execute EA python task"""

        folder_prefix = f"airflow__{self.dag_id}__{self.task_id}__"

        with TemporaryDirectory(prefix=folder_prefix) as tmp_dir:
            logging.info(f"Using tempdir {tmp_dir}")

            python_file_path = f"{tmp_dir}/code.py"
            input_file_path = f"{tmp_dir}/input.json"
            output_file_path = f"{tmp_dir}/output.json"

            self.create_file(python_file_path, code)
            self.create_file(input_file_path, json.dumps(json_data))
            self.create_file(output_file_path, "")

            self.sub_process = Popen(
                ['python', python_file_path,
                 "--input_file_path", input_file_path,
                 "--output_file_path", output_file_path
                 ],
                stdout=PIPE,
                stderr=STDOUT,
                cwd=tmp_dir,
                env=env,
                preexec_fn=self.pre_exec,
            )

            logging.info('Python process output start:')
            for raw_line in iter(self.sub_process.stdout.readline, b''):
                line = raw_line.decode(self.output_encoding).rstrip()
                logging.info(line)
            logging.info('Python process output end.')

            self.sub_process.wait()

            logging.info(f'Python process exited with code {self.sub_process.returncode}')

            if self.sub_process.returncode != 0:
                logging.error('EA Python task failed. The task returned a non-zero exit code.')
                exit(500)

            output_json = self.read_file(output_file_path)
            try:
                # Handle empty outputfile
                if len(output_json) != 0:
                    output_json = json.loads(output_json)
                else:
                    output_json = None
            except:
                loggin.error(f"Error parsing output JSON!")
                exit(500)

            return output_json

    def execute(self, context: dict):
        """Main opeerator entry point. Task execution starts here"""
        logging.info("EAPythonTaskOperator started...")

        # Setup environment variables if needed
        logging.info("Setting up env...")
        env = os.environ.copy()
        airflow_context_vars = context_to_airflow_vars(context, in_env_var_format=True)
        env.update(airflow_context_vars)

        # Get DAG start date.
        self.now_time = context['execution_date'].int_timestamp


        # Get EASAS connection
        logging.info("Initializing connection...")
        self.init_connection()
        session = self.requests_retry_session()

        # Get task config
        jobDataMap: dict = self.get_task_config(session)

        if(len(jobDataMap) == 0):
            logging.warning("Task config is empty!")
            return


        while True:
            logging.info(f"Begin processing period {self.current_period + 1}...")

            # Fetch data for task
            json_data: dict = self.get_task_data(calc_period=self.current_period,
                                                 jobDataMap=jobDataMap,
                                                 session=session)

            # Only get total periods on first task
            if(not self.is_started):
                logging.info("First task period. Caching total periods...")
                self.total_periods = json_data["metaData"]["totalPeriods"]
                self.is_started = True

            # self.current_period = json_data["metaData"]["currentPeriod"]

            logging.info(f"Running period {self.current_period + 1} out of {self.total_periods}.")
            logging.info(f"Calculation time for period: {json_data['metaData']['calculationTime']}.")

            # Get code and remove form json
            code = json_data.pop("pythonCode")

            # Execute python file and get data/json
            output_data = self.execute_ea_python_task(code, json_data, env)
            logging.info(f"JSON output:\n{output_data}")

            # Send calculated data back to server
            self.post_task_data(calc_period=self.current_period,
                                jobDataMap=jobDataMap,
                                jsonData=output_data,
                                session=session)

            self.current_period += 1

            if self.current_period >= self.total_periods:
                break

        print("EAPythonTaskOperator ended...")

    @staticmethod
    def create_file(file_path, string):
        """Create file used for output data"""

        with open(file_path, "w") as file:
            file.write(string)

    @staticmethod
    def read_file(file_path):
        """Read file used for input data"""

        with open(file_path, "r") as file:
            out_json = file.read()

        return out_json

    @staticmethod
    def requests_retry_session(
        retries=RETRIES,
        backoff_factor=1,
        status_forcelist=(500, 502, 504),
        session=None,
    ):
        session = session or requests.Session()
        retry = Retry(
            total=retries,
            read=retries,
            connect=retries,
            redirect=retries,
            status=retries,
            backoff_factor=backoff_factor,
            status_forcelist=status_forcelist,
        )
        adapter = HTTPAdapter(max_retries=retry)
        session.mount('http://', adapter)
        session.mount('https://', adapter)
        return session
