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
    'start_date': datetime.datetime(2021, 1, 7, 10, 0, 0),
}

with airflow.DAG(
        'amm_dag',
        'catchup=False',
        default_args=default_args,
        schedule_interval=datetime.timedelta(minutes=15)) as dag:

    def call_amm_api():
        url = "https://ammpositions-ue.a.run.app/upload"
        r = requests.get(url)
        print(r)

    ammPosCall = python_operator.PythonOperator(
        task_id="call_the_api",
        python_callable=call_amm_api

    )

    ammGCStoBQSync = GoogleCloudStorageToBigQueryOperator(
        task_id='gcs_to_bq',
        bucket='amm-positions',
        source_objects=['ammpos.csv'],
        schema_fields= [{"mode": "NULLABLE", "name": "address", "type": "STRING"},
        {"mode": "NULLABLE", "name": "label", "type": "STRING"},
        {"mode": "NULLABLE", "name": "tokenA", "type": "STRING"},
        {"mode": "NULLABLE", "name": "tokenB", "type": "STRING"},
        {"mode": "NULLABLE", "name": "poolTokenAbalance", "type": "FLOAT"},
        {"mode": "NULLABLE", "name": "poolTokenBbalance", "type": "FLOAT"},
        {"mode": "NULLABLE", "name": "LPtokenBalance", "type": "FLOAT"},
        {"mode": "NULLABLE", "name": "tokenAbalance", "type": "FLOAT"},
        {"mode": "NULLABLE", "name": "tokenBbalance", "type": "FLOAT"},
        {"mode": "NULLABLE", "name": "tokenAweight", "type": "FLOAT"},
        {"mode": "NULLABLE", "name": "tokenBweight", "type": "FLOAT"},
        {"mode": "NULLABLE", "name": "timestamp", "type": "TIMESTAMP"}],
        destination_project_dataset_table='uma.AMMPos',
        skip_leading_rows=1,
        write_disposition="WRITE_APPEND"
    )

    ammPosCall >> ammGCStoBQSync
