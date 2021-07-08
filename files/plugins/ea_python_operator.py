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



class EAPythonTaskOperator(BaseOperator):
    REQUEST_TIMEOUT = 5
    CONNECT_TIMEOUT = 10
    CONNECT_TRIES = 10
    FETCH_TIMEOUT = 30

    BASE_URL = ""
    CONTEXT_ROOT = ""
    SECRET = ""

    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)
        self.output_encoding: str = "utf-8"
        self.current_period: int = 0
        self.total_periods: int = 1
        self.sub_process = None

    def get_task_link(self, task_period):
        """Get tasks data link"""

        calc_id = str(task_period)

        url = f"{self.CONTEXT_ROOT}/rest/airflow/dag/{self.dag_id}/{self.task_id}/jobData?calcPeriod={calc_id}&calcTimestamp={self.now_time}"

        return url

    def init_connection(self):
        conn = BaseHook.get_connection("energyadvice")

        EAPythonTaskOperator.BASE_URL = conn.host
        EAPythonTaskOperator.CONTEXT_ROOT = conn.schema
        EAPythonTaskOperator.SECRET = conn.password

    @staticmethod
    def requests_retry_session(
        retries=10,
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

    def get_task_data(self, calc_period, session):
        """Get task's data form EA endpoint"""

        task_link = self.get_task_link(task_period=calc_period)

        print(f"Beginning to fetch task data for airflow task {self.dag_id}:{self.task_id} at {task_link}")
        print("Fetching!")

        headers = self.get_bearer_header()
        r = session.get(EAPythonTaskOperator.BASE_URL + task_link, headers=headers, timeout=EAPythonTaskOperator.FETCH_TIMEOUT)

        if r.status_code != 200:
            logging.error(f"Data fetch failed! Error {r.status_code}")
            exit(500)

        json_data = r.json()

        print("Data fetch finished! JSON: ")

        return json_data

    def post_task_data(self, calc_period, jsonData, session):
        """Post task data back to server"""

        task_link = self.get_task_link(task_period=calc_period)

        logging.info("Posting data...")
        headers = self.get_bearer_header({"Content-Type": "application/json"})

        r = session.post(EAPythonTaskOperator.BASE_URL + task_link,
                         headers=headers,
                         data=jsonData.encode("utf-8"),
                         timeout=EAPythonTaskOperator.FETCH_TIMEOUT)

        if r.status_code != 200:
            logging.error(f"Error occurred posting data to server! Error: {r.status_code}")
            exit(500)

    def get_bearer_header(self, additional_headers = {}):
        auth = {"Authorization": f"Bearer {EAPythonTaskOperator.SECRET}"}
        final_header = {**auth, **additional_headers}
        return final_header

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

            return output_json

    def execute(self, context: dict):
        """Main opeerator entry point. Task execution starts here"""
        logging.info("EAPythonTaskOperator started...")

        self.init_connection()

        # Get DAG start date.
        self.now_time = context['execution_date'].int_timestamp

        # Setup environment variables if needed
        env = os.environ.copy()
        airflow_context_vars = context_to_airflow_vars(context, in_env_var_format=True)
        env.update(airflow_context_vars)

        session = self.requests_retry_session()


        while True:
            logging.info("Begin processing period...")

            # Fetch data
            json_data: dict = self.get_task_data(calc_period=self.current_period, session=session)

            self.total_periods = json_data["metaData"]["totalPeriods"]
            self.current_period = json_data["metaData"]["currentPeriod"]

            logging.info(f"Running period {self.current_period + 1} out of {self.total_periods}.")
            logging.info(f"Calculation time for period: {json_data['metaData']['calculationTime']}.")

            code = json_data["pythonCode"]
            json_data.pop("pythonCode")

            data = json_data

            output_data = self.execute_ea_python_task(code, data, env)
            logging.info("JSON output:")
            logging.info(output_data)

            self.post_task_data(self.current_period, output_data, session=session)

            self.current_period += 1

            if self.current_period >= self.total_periods:
                break

        print("EAPythonTaskOperator ended...")
