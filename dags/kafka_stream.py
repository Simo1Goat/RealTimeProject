import json
import time
import logging
from airflow import DAG
from config import KAFKA_HOST
from datetime import datetime
from kafka import KafkaProducer
from utils.utils import get_data, format_data
from airflow.operators.python import PythonOperator


def stream_data():
    response = get_data(method="GET")
    formatted_data = format_data(response)
    return json.dumps(formatted_data, indent=3)


default_args = {
    "owner": "mhabibi",
    "start_date": datetime(2024, 1, 1)
}

with DAG(dag_id="user_automation",
         default_args=default_args,
         schedule_interval="@daily",
         catchup=False) as dag:

    streaming_task = PythonOperator(
        task_id="stream_data_from_api",
        python_callable=stream_data
    )
