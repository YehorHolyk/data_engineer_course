from airflow import DAG
from datetime import datetime
from load_sales_to_gcp_operator import LoadSalesFromLocalFilesystemToGCSOperator

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
    load_sales_to_gcs = LoadSalesFromLocalFilesystemToGCSOperator(
        task_id="load_sales_from_local_to_gcs",
        execution_date="{{ ds }}",
        execution_date_year="{{ macros.ds_format(ds, '%Y-%m-%d', '%Y') }}",
        execution_date_month="{{ macros.ds_format(ds, '%Y-%m-%d', '%m') }}",
        execution_date_day="{{ macros.ds_format(ds, '%Y-%m-%d', '%d') }}",
        src="raw/sales/",
        dst="src1/sales/v1/",
        bucket="de07-yehor-holyk-bucket",
        gcp_conn_id="google_cloud_connection"
    )
