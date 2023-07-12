import os

from airflow import DAG
from airflow.models.param import Param
from datetime import datetime
from load_sales import LoadSalesOperator
from convert_to_avro import ConvertToAvroOperator

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
        raw_dir='{{ params.raw_dir }}',
        execution_date_str='{{ ds }}',
    )

    task2 = ConvertToAvroOperator(
        task_id="convert_to_avro",
        stg_dir="{{ params.stg_dir }}",
        execution_date_str="{{ ti.xcom_pull(key='execution_date') }}",
        raw_file_path="{{ ti.xcom_pull(key='raw_file_path') }}"
    )

    task1 >> task2
