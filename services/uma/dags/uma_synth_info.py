import datetime
import requests
import airflow
from airflow.operators import bash_operator, python_operator
from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator
from airflow.utils.dates import days_ago



default_args = {
    'depends_on_past': False,
    'email': [''],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': datetime.timedelta(minutes=5),
    'start_date': datetime.datetime(2021, 1, 5, 8, 0, 0),
}

dag = airflow.DAG(
        'synth_dag',
        'catchup=False',
        default_args=default_args,
        schedule_interval=datetime.timedelta(minutes=15))

def call_synth_info_api():
    url = "https://synthinfo-ue.a.run.app/upload"
    r = requests.get(url)
    print(r)

synthInfoCall = python_operator.PythonOperator(
    task_id="call_the_api",
    python_callable=call_synth_info_api,
    dag=dag
)

synthGCStoBQSync = GoogleCloudStorageToBigQueryOperator(
    task_id='gcs_to_bq',
    bucket='synth-info',
    source_objects=['synthinfo.csv'],
    schema_fields= [{"mode": "NULLABLE", "name": "address", "type": "STRING"},
    {"mode": "NULLABLE", "name": "collateralToken", "type": "STRING"},
    {"mode": "NULLABLE", "name": "syntheticToken", "type": "STRING"},
    {"mode": "NULLABLE", "name": "totalPositionCollateral", "type": "FLOAT"},
    {"mode": "NULLABLE", "name": "totalTokensOutstanding", "type": "FLOAT"},
    {"mode": "NULLABLE", "name": "timestamp", "type": "TIMESTAMP"},
    {"mode": "NULLABLE", "name": "collateralTokenName", "type": "STRING"},
    {"mode": "NULLABLE", "name": "syntheticTokenName", "type": "STRING"}],
    destination_project_dataset_table='uma.SynthInfo',
    skip_leading_rows=1,
    write_disposition="WRITE_APPEND",
    dag=dag
)

synthInfoCall >> synthGCStoBQSync
