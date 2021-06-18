import json
import os
import signal
import time
from subprocess import PIPE, STDOUT, Popen
from tempfile import TemporaryDirectory

from urllib3 import HTTPConnectionPool
import urllib3

from airflow.exceptions import AirflowException
from airflow.models.connection import Connection
from airflow.hooks.base_hook import BaseHook
from airflow.models.baseoperator import BaseOperator
from airflow.utils.operator_helpers import context_to_airflow_vars


class EAPythonTaskOperator(BaseOperator):
    CONNECT_TIMEOUT = 10
    CONNECT_TRIES = 10
    FETCH_TIMEOUT = 30

    BASE_URL = ""
    CONTEXT_ROOT = ""

    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)
        self.output_encoding: str = "utf-8"
        self.current_period: int = 0
        self.total_periods: int = 1
        self.now_time = int(time.time())
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


    def get_task_data(self, calc_period):
        """Get task's data form EA endpoint"""

        task_link = self.get_task_link(task_period=calc_period)

        print(f"Beginning to fetch task data for airflow task {self.dag_id}:{self.task_id} at {task_link}")

        http_pool = urllib3.PoolManager()

        print("Fetching!")
        r = http_pool.urlopen('GET', EAPythonTaskOperator.BASE_URL + task_link, timeout=EAPythonTaskOperator.FETCH_TIMEOUT, redirect=True)

        resp = r.data.decode("UTF-8")

        if r.status != 200:
            print("Data fetch failed!")
            exit(500)

        json_data = json.loads(resp)

        print("Data fetch finished! JSON: ")
        # print(json_data)

        return json_data

    def post_task_data(self, calc_period, jsonData):
        """Post task data back to server"""

        task_link = self.get_task_link(task_period=calc_period)

        print("Getting ready to post data...")

        http_pool = urllib3.PoolManager()

        print("Posting data...")
        headers = {"Content-Type": "application/json"}
        r = http_pool.urlopen('POST', EAPythonTaskOperator.BASE_URL + task_link, headers=headers, body=jsonData.encode("utf-8"),
                              timeout=EAPythonTaskOperator.FETCH_TIMEOUT)
        status = r.status


        if status != 200:
            print("Error occurred posting data to server!")
            exit(500)

    def get_connection_pool(self):
        print("Creating pool!")
        http_pool = HTTPConnectionPool(self.BASE_URL, timeout=EAPythonTaskOperator.CONNECT_TIMEOUT,
                                       retries=EAPythonTaskOperator.CONNECT_TRIES)

        return http_pool

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
            print(f"Using tempdir {tmp_dir}")

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

            print('Python process output start:')
            for raw_line in iter(self.sub_process.stdout.readline, b''):
                line = raw_line.decode(self.output_encoding).rstrip()
                print(line)
            print('Python process output end.')

            self.sub_process.wait()

            print(f'Python process exited with code {self.sub_process.returncode}')

            if self.sub_process.returncode != 0:
                raise AirflowException('EA Python task failed. The task returned a non-zero exit code.')

            output_json = self.read_file(output_file_path)

            return output_json

    def execute(self, context):
        print("EAPythonTaskOperator started...")

        self.init_connection()

        # Setup environment variables if needed
        env = os.environ.copy()
        airflow_context_vars = context_to_airflow_vars(context, in_env_var_format=True)
        env.update(airflow_context_vars)


        while True:
            print("Begin processing period...")

            # Fetch data
            json_data: dict = self.get_task_data(calc_period=self.current_period)

            self.total_periods = json_data["metaData"]["totalPeriods"]
            self.current_period = json_data["metaData"]["currentPeriod"]

            print(f"Running period {self.current_period + 1} out of {self.total_periods}.")
            print(f"Calculation time for period: {json_data['metaData']['calculationTime']}.")

            code = json_data["pythonCode"]
            json_data.pop("pythonCode")

            data = json_data

            output_data = self.execute_ea_python_task(code, data, env)

            self.post_task_data(self.current_period, output_data)

            self.current_period += 1

            if self.current_period >= self.total_periods:
                break

        print("EAPythonTaskOperator ended...")
