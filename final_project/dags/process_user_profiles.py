import datetime as dt

from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator

from table_defs.user_profiles_jsonl import user_profiles_jsonl

DEFAULT_ARGS = {
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': 5,
}

dag = DAG(
    dag_id="process_user_profiles_pipeline",
    description="Ingest and process user profiles data",
    start_date=dt.datetime(2023, 8, 16),
    schedule_interval=None,
    tags=['user_profiles'],
    default_args=DEFAULT_ARGS,
)

dag.doc_md = __doc__

transfer_user_profiles_from_data_lake_to_silver = BigQueryInsertJobOperator(
    task_id='transfer_user_profiles_from_data_lake_to_silver',
    dag=dag,
    gcp_conn_id='gcp_conn',
    location='us-east1',
    project_id='de-07-yehor-holyk',
    configuration={
        "query": {
            "query": "{% include 'sql/insert_user_profiles_to_silver.sql' %}",
            "useLegacySql": False,
            "tableDefinitions": {
                "user_profiles_jsonl": user_profiles_jsonl,
            },
        }
    },
    params={
        'data_lake_raw_bucket': "de-07-yehor-holyk-final-project-bucket",
        'project_id': "de-07-yehor-holyk"
    }
)

transfer_user_profiles_from_data_lake_to_silver
