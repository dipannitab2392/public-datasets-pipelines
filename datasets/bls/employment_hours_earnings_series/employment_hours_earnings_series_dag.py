# Copyright 2021 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


from airflow import DAG
from airflow.contrib.operators import gcs_to_bq, kubernetes_pod_operator

default_args = {
    "owner": "Google",
    "depends_on_past": False,
    "start_date": "2021-03-01",
}


with DAG(
    dag_id="bls.employment_hours_earnings_series",
    default_args=default_args,
    max_active_runs=1,
    schedule_interval="@daily",
    catchup=False,
    default_view="graph",
) as dag:

    # Run CSV transform within kubernetes pod
    transform_csv = kubernetes_pod_operator.KubernetesPodOperator(
        task_id="transform_csv",
        startup_timeout_seconds=600,
        name="employment_hours_earnings_series",
        namespace="composer",
        service_account_name="datasets",
        image_pull_policy="Always",
        image="{{ var.json.bls.container_registry.run_csv_transform_kub }}",
        env_vars={
            "SOURCE_URLS": '["gs://pdp-feeds-staging/Bureau/ce.series.tsv"]',
            "SOURCE_FILES": '["files/data1.tsv"]',
            "TARGET_FILE": "files/data_output.csv",
            "TARGET_GCS_BUCKET": "{{ var.value.composer_bucket }}",
            "TARGET_GCS_PATH": "data/bls/employment_hours_earnings_series/data_output.csv",
            "PIPELINE_NAME": "employment_hours_earnings_series",
            "JOINING_KEY": "",
            "TRIM_SPACE": '["series_id","footnote_codes"]',
            "CSV_HEADERS": '["series_id","supersector_code","industry_code","data_type_code","seasonal","series_title","footnote_codes","begin_year","begin_period","end_year","end_period"]',
        },
        resources={"request_memory": "4G", "request_cpu": "1"},
    )

    # Task to load CSV data to a BigQuery table
    load_to_bq = gcs_to_bq.GoogleCloudStorageToBigQueryOperator(
        task_id="load_to_bq",
        bucket="{{ var.value.composer_bucket }}",
        source_objects=["data/bls/employment_hours_earnings_series/data_output.csv"],
        source_format="CSV",
        destination_project_dataset_table="bls.employment_hours_earnings_series",
        skip_leading_rows=1,
        allow_quoted_newlines=True,
        write_disposition="WRITE_TRUNCATE",
        schema_fields=[
            {"name": "series_id", "type": "STRING", "mode": "required"},
            {"name": "supersector_code", "type": "INTEGER", "mode": "NULLABLE"},
            {"name": "industry_code", "type": "INTEGER", "mode": "NULLABLE"},
            {"name": "data_type_code", "type": "INTEGER", "mode": "NULLABLE"},
            {"name": "seasonal", "type": "STRING", "mode": "NULLABLE"},
            {"name": "series_title", "type": "STRING", "mode": "NULLABLE"},
            {"name": "footnote_codes", "type": "STRING", "mode": "NULLABLE"},
            {"name": "begin_year", "type": "INTEGER", "mode": "NULLABLE"},
            {"name": "begin_period", "type": "STRING", "mode": "NULLABLE"},
            {"name": "end_year", "type": "INTEGER", "mode": "NULLABLE"},
            {"name": "end_period", "type": "STRING", "mode": "NULLABLE"},
        ],
    )

    transform_csv >> load_to_bq
