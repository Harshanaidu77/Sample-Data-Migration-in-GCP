from airflow import DAG
from airflow.decorators import task
from airflow.operators.empty import EmptyOperator
from airflow.utils.dates import days_ago

from google.cloud import storage
from google.cloud import bigquery

import pandas as pd
from io import BytesIO
import re

# -------------------------------------------------------------------
# Configuration
# -------------------------------------------------------------------

PROJECT_ID = "project ID"
DATASET = "dataset ID"

BUCKET_NAME = "name"
FILE_NAME = "filename.csv"

# -------------------------------------------------------------------
# DAG Definition
# -------------------------------------------------------------------

with DAG(
    dag_id="state_wise_students_load",
    start_date=days_ago(1),
    schedule="@daily",
    catchup=False,
    tags=["BigQuery", "GCS", "Students"],
) as dag:

    # ----------------------------------------------------------------
    # Dummy Start Task
    # ----------------------------------------------------------------

    start = EmptyOperator(
        task_id="start"
    )

    # ----------------------------------------------------------------
    # Main Task
    # ----------------------------------------------------------------

    @task
    def create_state_tables():

        storage_client = storage.Client(project=PROJECT_ID)
        bq_client = bigquery.Client(project=PROJECT_ID)

        # Read CSV from GCS
        bucket = storage_client.bucket(BUCKET_NAME)
        blob = bucket.blob(FILE_NAME)

        data = blob.download_as_bytes()

        df = pd.read_csv(BytesIO(data))

        # Convert admission_date to BigQuery DATE format
        df["admission_date"] = (
            pd.to_datetime(
                df["admission_date"],
                format="%d-%m-%Y"
            ).dt.date
        )

        # Create state-wise tables
        for state in df["state"].dropna().unique():

            table_name = "students_" + re.sub(
                r"[^a-zA-Z0-9]+",
                "_",
                state.strip().lower()
            )

            table_id = f"{PROJECT_ID}.{DATASET}.{table_name}"

            state_df = df[df["state"] == state]

            job_config = bigquery.LoadJobConfig(

                schema=[
                    bigquery.SchemaField("student_id", "INTEGER"),
                    bigquery.SchemaField("student_name", "STRING"),
                    bigquery.SchemaField("gender", "STRING"),
                    bigquery.SchemaField("department", "STRING"),
                    bigquery.SchemaField("year", "INTEGER"),
                    bigquery.SchemaField("city", "STRING"),
                    bigquery.SchemaField("state", "STRING"),
                    bigquery.SchemaField("admission_date", "DATE"),
                    bigquery.SchemaField("fees", "INTEGER"),
                ],

                create_disposition=bigquery.CreateDisposition.CREATE_IF_NEEDED,

                write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
            )

            load_job = bq_client.load_table_from_dataframe(
                dataframe=state_df,
                destination=table_id,
                job_config=job_config,
            )

            load_job.result()

            print(
                f"Successfully loaded {len(state_df)} rows into {table_name}"
            )

    state_tables = create_state_tables()

    # ----------------------------------------------------------------
    # Dummy End Task
    # ----------------------------------------------------------------

    end = EmptyOperator(
        task_id="end"
    )

    # ----------------------------------------------------------------
    # Task Dependencies
    # ----------------------------------------------------------------

    start >> state_tables >> end