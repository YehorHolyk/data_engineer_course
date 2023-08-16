import datetime as dt

from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator


DEFAULT_ARGS = {
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': 5,
}

dag = DAG(
    dag_id="enrich_user_profiles_pipeline",
    description="Enrich user profiles data with customers data",
    start_date=dt.datetime.now(),
    schedule_interval=None,
    catchup=False,
    tags=['user_profiles_enriched'],
    default_args=DEFAULT_ARGS,
)

dag.doc_md = __doc__


enrich_user_profiles_with_customers_data = BigQueryInsertJobOperator(
    task_id='enrich_user_profiles_with_customers_data',
    dag=dag,
    gcp_conn_id='gcp_conn',
    location='us-east1',
    project_id='de-07-yehor-holyk',
    configuration={
        "query": {
            "query": "{% include 'sql/enrich_user_profiles.sql' %}",
            "useLegacySql": False,
        }
    },
    params={
        'project_id': "de-07-yehor-holyk"
    }
)

enrich_user_profiles_with_customers_data