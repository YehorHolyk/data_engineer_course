import os
import shutil

from airflow import DAG
from airflow.models import BaseOperator
from airflow.models.param import Param
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.exceptions import AirflowException
from airflow.utils.context import Context
from datetime import datetime
import json
import fastavro
from typing import List, Tuple, Any

AVRO_SCHEMA = {
    'name': 'Sales',
    'type': 'record',
    'fields': [
        {'name': 'client', 'type': 'string'},
        {'name': 'purchase_date', 'type': 'string'},
        {'name': 'product', 'type': 'string'},
        {'name': 'price', 'type': 'int'}
    ]
}
def format_execution_date(date: datetime):
    formatted_date = datetime.strftime(date, '%Y-%m-%d')
    return formatted_date


def cleanup_folder(path: str) -> None:
    if os.path.exists(path):
        for filename in os.listdir(path):
            file_path = os.path.join(path, filename)
            if os.path.isfile(file_path):
                os.remove(file_path)
                print(f"Deleted file: {path}")
            elif os.path.isdir(file_path):
                # Delete the directory and its contents recursively
                shutil.rmtree(file_path)
                print(f"Deleted directory: {path}")
        print(f"Cleanup complete for folder: {path}")
    else:
        print(f"Folder not found: {path}")


class LoadSalesOperator(SimpleHttpOperator):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def unscramble_lists(self, nested_list: List[Any]) -> List[Any]:
        result = []
        for item in nested_list:
            if isinstance(item, list):
                result.extend(self.unscramble_lists(item))
            else:
                result.append(item)
        return result

    def execute(self, context: Context):
        ti = context['ti']
        print(f"HEADERS: {self.headers}")
        execution_date_str = format_execution_date(context['execution_date'])
        self.data['date'] = execution_date_str
        responses = []
        try:
            page = 1
            while True:
                self.data['page'] = page
                print(f"DATA: {self.data}")
                response = super().execute(context)
                responses.append(json.loads(response))
                page += 1
        except AirflowException:
            print("Pages amount limit reached")
        if len(responses) == 0:
            raise AirflowException('No Data')

        responses = self.unscramble_lists(responses)
        print(responses)

        raw_dir = context['params']['raw_dir']
        folder_path = os.path.join(raw_dir, execution_date_str)
        file_name = f"sales_{execution_date_str}.json"
        file_path = os.path.join(folder_path, file_name)
        if not os.path.exists(folder_path):
            os.makedirs(folder_path)
        else:
            cleanup_folder(folder_path)

        with open(file_path, 'w') as file:
            json_content = json.dumps(responses)
            file.write(json_content)
        print(f"Data saved to: {file_path}")

        ti.xcom_push('execution_date', execution_date_str)
        ti.xcom_push('raw_file_path', file_path)

class ConvertToAvroOperator(BaseOperator):

    def execute(self, context: Context) -> Any:
        ti = context['ti']
        execution_date_str = str(ti.xcom_pull(key='execution_date'))
        raw_file_path = str(ti.xcom_pull(key='raw_file_path'))
        stg_dir = context['params']['stg_dir']
        stg_folder = os.path.join(stg_dir, execution_date_str)
        stg_file_name = f"sales_{execution_date_str}.avro"
        stg_file_path = os.path.join(stg_folder, stg_file_name)

        if not os.path.exists(stg_folder):
            os.makedirs(stg_folder)
        else:
            cleanup_folder(stg_folder)

        with open(raw_file_path, 'r') as json_file:
            data = json.load(json_file)
            print(f"Data read from: {raw_file_path}")

        with open(stg_file_path, 'wb') as avro_file:
            fastavro.writer(avro_file, AVRO_SCHEMA, data)
            print(f"Data write to: {stg_file_path}")

DEFAULT_ARGS = {
    'depends_on_past': False,
    'email': ['admin@example.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': 30,
}

with DAG(
        dag_id='process_sales',
        start_date=datetime(2022, 8, 9),
        end_date=datetime(2022, 8, 12),
        schedule_interval="0 1 * * *",
        max_active_runs=1,
        catchup=True,
        default_args=DEFAULT_ARGS,
        params={
            "raw_dir": Param("raw/sales", type="string"),
            "stg_dir": Param("stg/sales", type="string")
        }
) as dag:

    task1 = LoadSalesOperator(
        task_id='extract_data_from_api',
        method='GET',
        http_conn_id='fake_api',
        endpoint='/sales',
        headers={'Authorization': os.environ.get('API_AUTH_TOKEN')},
        dag=dag
    )

    task2 = ConvertToAvroOperator(
        task_id="convert_to_avro"
    )

    task1 >> task2
