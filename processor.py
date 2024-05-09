"""
An example processing pipeline, consuming Pub/Sub and writing batches to GCS.
Based on https://cloud.google.com/composer/docs/composer-2/triggering-gcf-pubsub
"""

import datetime
import os
import tempfile
from airflow import models
from airflow.providers.google.cloud.operators.pubsub import (
    PubSubCreateSubscriptionOperator,
    PubSubPullOperator,
)
from airflow.providers.google.cloud.transfers.local_to_gcs import (
    LocalFilesystemToGCSOperator,
)
from airflow.operators.python import PythonOperator


# pylint: disable=unused-argument
def handle_messages(pulled_messages, context):
    """
    Processes a pool of pulled Pub/Sub messages, collects their data into a Python array.
    """
    dag_ids = list()
    for idx, m in enumerate(pulled_messages):
        fact = m.message.attributes["fact"]
        print(f"message {idx} fact is {fact}")
        dag_ids.append(fact)
    return dag_ids


def write_collection_to_file(**kwargs):
    """
    Write the pulled collection into a file.
    """
    ti = kwargs["ti"]
    msgs = ti.xcom_pull(task_ids="pull_messages")

    path = os.path.join(tempfile.mkdtemp(), "facts")
    ti.xcom_push(key="filename", value=path)

    with open(path, "w", encoding="utf-8") as fp:
        fp.write("\n".join(msgs))


with models.DAG(
    "airflow_playground_processor",
    schedule_interval="*/5 * * * *",
    default_args={"start_date": datetime.datetime(2024, 5, 9, 18, 50, 0)},
) as dag:
    # If subscription exists, we will use it. If not - create new one
    subscribe_task = PubSubCreateSubscriptionOperator(
        task_id="subscribe",
        topic="{{ var.value.pubsub_topic_id }}",
        subscription="{{ var.value.pubsub_subscription_id }}",
    )

    subscription = subscribe_task.output

    pull_messages_operator = PubSubPullOperator(
        task_id="pull_messages",
        project_id="{{ var.value.gcp_project_id }}",
        ack_messages=True,
        messages_callback=handle_messages,
        subscription=subscription,
        max_messages=50,
    )

    write_collection_to_file = PythonOperator(
        task_id="write_collection",
        python_callable=write_collection_to_file,
        provide_context=True,
    )

    upload_to_gcs = LocalFilesystemToGCSOperator(
        task_id="upload_to_gcs",
        src="{{ ti.xcom_pull(key='filename', task_ids='write_collection') }}",
        dst="{{ data_interval_start }}-{{ data_interval_end }}.txt",
        bucket="{{ var.value.gcs_bucket_id }}",
        mime_type="text/plain",
    )

    # DAG shifting; pylint: disable=pointless-statement
    (subscribe_task >> pull_messages_operator >> write_collection_to_file >> upload_to_gcs)
