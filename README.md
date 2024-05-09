# Composer playground

Just a simple example Composer / Airflow environment with a couple of DAGs
demoing Airflow behavior.

## DAGs

1. `airflow_playground_publisher` (publisher.py) creates dummy data and
   publishes to a Pub/Sub topic every 30 seconds. This is kind of noisy, so set
   appropriate retention policies in Pub/Sub and only turn on when demonstrating
   the playground.

2. `airflow_playground_processor` (processor.py) consumes the Pub/Sub topic,
   reading a batch of messages and writing each batch in a timed file in a GCS
   bucket.

## Pre-requisites

This assumes you run on [Google Cloud
Composer](https://cloud.google.com/composer).

### Airflow variables

- `gcp_project_id`: GCP project ID.
- `gcs_bucket_id`: GCS bucket to write to.
- `pubsub_topic_id`: Pub/Sub topic to use for dummy messages.
- `pubsub_subscription_id`: Pub/Sub subscription name to consume the topic.

### IAM

The service account running the Composer environment needs a 'Storage Object
Creator' permission on the relevant GCS bucket.

## Deploying

```shell
$ gcloud composer environments storage dags import \
    --environment $COMPOSER_ENVIRONMENT \
    --location $GCP_LOCATION --project $GCP_PROJECT_ID \
    --source="composer-playground"
```

## License

This project is licensed under the terms of the MIT license.