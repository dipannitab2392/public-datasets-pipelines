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
from airflow.providers.google.cloud.operators import kubernetes_engine
from airflow.providers.google.cloud.transfers import gcs_to_bigquery

default_args = {
    "owner": "Google",
    "depends_on_past": False,
    "start_date": "2021-03-01",
}


with DAG(
    dag_id="mlcommons.cc_by_clean_new",
    default_args=default_args,
    max_active_runs=1,
    schedule_interval="@daily",
    catchup=False,
    default_view="graph",
) as dag:

    # Run CSV transform within GKES pod
    transform_csv = kubernetes_engine.GKEStartPodOperator(
        task_id="transform_csv",
        project_id="{{ var.json.mlcommons.gcp_project_id }}",
        location="{{ var.json.mlcommons.gcp_location }}",
        cluster_name="GKE_CLUSTER_NAME",
        use_internal_ip=True,
        service_account_name="{{ var.json.mlcommons.service_account }}",
        namespace="default",
        name="cc_by_clean_new",
        image_pull_policy="Always",
        image="{{ var.json.mlcommons.container_registry.run_csv_transform_kub }}",
        env_vars={
            "SOURCE_URLS": '["gs://the-peoples-speech-west-europe/forced-aligner/cuda-forced-aligner/peoples-speech/cc_by_clean_new/dataset_manifest_single/part-00000-38582da5-5171-420f-a074-ca0fb80cbef1-c000.json"]',
            "SOURCE_FILES": '["files/data.json"]',
            "TARGET_FILE": "files/data_output.csv",
            "TARGET_GCS_BUCKET": "{{ var.value.composer_bucket }}",
            "TARGET_GCS_PATH": "data/mlcommons/cc_by_clean_new/data_output.csv",
            "PIPELINE_NAME": "cc_by_clean_new",
        },
        do_xcom_push=True,
    )

    # Task to load CSV data to a BigQuery table
    load_to_bq = gcs_to_bigquery.GCSToBigQueryOperator(
        task_id="load_to_bq",
        bucket="{{ var.value.composer_bucket }}",
        source_objects=["data/mlcommons/cc_by_clean_new/data_output.csv"],
        source_format="CSV",
        destination_project_dataset_table="mlcommons.cc_by_clean_new",
        skip_leading_rows=1,
        allow_quoted_newlines=True,
        write_disposition="WRITE_TRUNCATE",
        schema_fields=[
            {"name": "audio_document_id", "type": "STRING", "mode": "NULLABLE"},
            {"name": "identifier", "type": "STRING", "mode": "NULLABLE"},
            {"name": "text_document_id", "type": "STRING", "mode": "NULLABLE"},
            {"name": "duration_ms", "type": "STRING", "mode": "NULLABLE"},
            {"name": "label", "type": "STRING", "mode": "NULLABLE"},
            {"name": "name", "type": "STRING", "mode": "NULLABLE"},
        ],
    )

    transform_csv >> load_to_bq
