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
    producer = KafkaProducer(bootstrap_servers=KAFKA_HOST, max_block_ms=5000)
    curr_time = time.time()
    while True:
        try:
            # push the data each minute to the broker
            if time.time() > curr_time + 60:
                response = get_data()
                formatted_data = format_data(response)
                producer.send(topic="users", value=json.dumps(formatted_data).encode("utf-8"))
                logging.info(msg="The message is sent successfully")
                curr_time = time.time()
            else:
                logging.info(msg="waiting ...")
                time.sleep(30)
        except Exception as err:
            logging.error(msg="An Error occurred during fetching data, details {}".format(err))
            continue


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
