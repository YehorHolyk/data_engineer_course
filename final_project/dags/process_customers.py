import datetime as dt

from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator

from table_defs.customers_csv import customers_csv


DEFAULT_ARGS = {
    'depends_on_past': True,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': 5,
}

dag = DAG(
    dag_id="process_customers_pipeline",
    description="Ingest and process customers data",
    schedule_interval='0 7 * * *',
    start_date=dt.datetime(2022, 8, 1),
    end_date=dt.datetime(2022, 8, 6),
    catchup=True,
    tags=['customers'],
    default_args=DEFAULT_ARGS,
)

dag.doc_md = __doc__


transfer_customers_from_data_lake_to_bronze = BigQueryInsertJobOperator(
    task_id='transfer_customers_from_data_lake_to_bronze',
    dag=dag,
    gcp_conn_id='gcp_conn',
    location='us-east1',
    project_id='de-07-yehor-holyk',
    configuration={
        "query": {
            "query": "{% include 'sql/insert_customers_to_bronze.sql' %}",
            "useLegacySql": False,
            "tableDefinitions": {
                "customers_csv": customers_csv,
            },
        }
    },
    params={
        'data_lake_raw_bucket': "de-07-yehor-holyk-final-project-bucket",
        'project_id': "de-07-yehor-holyk"
    }
)

transfer_customers_from_bronze_to_silver = BigQueryInsertJobOperator(
    task_id='transfer_customers_from_bronze_to_silver',
    dag=dag,
    gcp_conn_id='gcp_conn',
    location='us-east1',
    project_id='de-07-yehor-holyk',
    configuration={
        "query": {
            "query": "{% include 'sql/transfer_customers_to_silver.sql' %}",
            "useLegacySql": False
        }
    },
    params={
        'project_id': "de-07-yehor-holyk"
    }
)

transfer_customers_from_data_lake_to_bronze >> transfer_customers_from_bronze_to_silver