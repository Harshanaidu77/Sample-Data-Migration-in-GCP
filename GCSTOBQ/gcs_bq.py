from datetime import datetime

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import (
    GCSToBigQueryOperator,
)

BUCKET_NAME = "your bucket name"
FILE_NAME = "file name"

PROJECT_ID = "project ID"
DATASET_ID = "datasetId"
TABLE_ID = "table name"

with DAG(
    dag_id="gcs_to_bq_two_tasks",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
) as dag:

    start = EmptyOperator(
        task_id="start"
    )

    load_to_bq = GCSToBigQueryOperator(
        task_id="load_to_bigquery",
        bucket=BUCKET_NAME,
        source_objects=[FILE_NAME],
        destination_project_dataset_table=(
            f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"
        ),
        source_format="CSV",
        skip_leading_rows=1,
        autodetect=True,
        write_disposition="WRITE_TRUNCATE",
        create_disposition="CREATE_IF_NEEDED",
        gcp_conn_id="google_cloud_default",
    )

    start >> load_to_bq