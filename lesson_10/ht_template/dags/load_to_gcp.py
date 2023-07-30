from airflow import DAG
from datetime import datetime
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator

DEFAULT_ARGS = {
    'depends_on_past': False,
    'email': ['admin@example.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': 30,
}

with DAG(
        dag_id='load_sales_to_gcp',
        start_date=datetime(2022, 8, 9),
        end_date=datetime(2022, 8, 11),
        schedule_interval="0 1 * * *",
        max_active_runs=1,
        catchup=True,
        default_args=DEFAULT_ARGS
) as dag:
    load_sales_to_gcs = LocalFilesystemToGCSOperator(
        task_id="load_sales_from_local_to_gcs",
        src="raw/sales/{{ ds }}/sales_{{ ds }}.json",
        dst="src1/sales/v1/year={{ execution_date.year }}/month={{ execution_date.month }}/"
            "day={{ execution_date.day }}/sales_{{ ds }}.json",
        bucket="de07-yehor-holyk-bucket",
        gcp_conn_id="google_cloud_connection"
    )
