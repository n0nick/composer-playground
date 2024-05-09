"""
A dummy data generator for the Airflow playground. Emits a random cat fact to Pub/Sub.
Based on https://docs.astronomer.io/learn/airflow-passing-data-between-tasks
"""

import datetime
import json
import requests
from airflow import models
from airflow.providers.google.cloud.operators.pubsub import PubSubPublishMessageOperator
from airflow.operators.python import PythonOperator


def get_a_cat_fact(ti):
    """
    Fetches a cat fact and pushes to XCom
    """
    url = "http://catfact.ninja/fact"
    res = requests.get(url, timeout=10)
    ti.xcom_push(key="cat_fact", value=json.loads(res.text)["fact"])


# Define a DAG (directed acyclic graph) of tasks.
# Any task you create within the context manager is automatically added to the
# DAG object.
with models.DAG(
    "airflow_playground_publisher",
    schedule_interval=datetime.timedelta(seconds=30),
    default_args={"start_date": datetime.datetime(2024, 5, 9, 18, 50, 0)},
) as dag:
    get_cat_data = PythonOperator(
        task_id="get_a_cat_fact", python_callable=get_a_cat_fact
    )

    publish_cat_fact = PubSubPublishMessageOperator(
        task_id="publish_cat_fact",
        topic="{{ var.value.pubsub_topic_id }}",
        messages=[
            {
                "data": b"hello world",
                "attributes": {
                    "fact": "{{ ti.xcom_pull(key='cat_fact', task_ids='get_a_cat_fact') }}"
                },
            }
        ],
    )

    # DAG shifting; pylint: disable=pointless-statement
    get_cat_data >> publish_cat_fact
