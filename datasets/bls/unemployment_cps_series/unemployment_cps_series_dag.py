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
    dag_id="bls.unemployment_cps_series",
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
        name="unemployment_cps_series",
        namespace="composer",
        service_account_name="datasets",
        image_pull_policy="Always",
        image="{{ var.json.bls.container_registry.run_csv_transform_kub }}",
        env_vars={
            "SOURCE_URLS": '["gs://pdp-feeds-staging/Bureau/ln.series.tsv"]',
            "SOURCE_FILES": '["files/data1.tsv"]',
            "TARGET_FILE": "files/data_output.tsv",
            "TARGET_GCS_BUCKET": "{{ var.value.composer_bucket }}",
            "TARGET_GCS_PATH": "data/bls/unemployment_cps_series/data_output.csv",
            "PIPELINE_NAME": "unemployment_cps_series",
            "JOINING_KEY": "",
            "TRIM_SPACE": '["series_id","footnote_codes","series_title"]',
            "CSV_HEADERS": '["series_id","lfst_code","periodicity_code","series_title","absn_code","activity_code","ages_code","class_code","duration_code","education_code","entr_code","expr_code","hheader_code","hour_code","indy_code","jdes_code","look_code","mari_code","mjhs_code","occupation_code","orig_code","pcts_code","race_code","rjnw_code","rnlf_code","rwns_code","seek_code","sexs_code","tdat_code","vets_code","wkst_code","born_code","chld_code","disa_code","seasonal","footnote_codes","begin_year","begin_period","end_year","end_period","cert_code"]',
        },
        resources={"request_memory": "4G", "request_cpu": "1"},
    )

    # Task to load CSV data to a BigQuery table
    load_to_bq = gcs_to_bq.GoogleCloudStorageToBigQueryOperator(
        task_id="load_to_bq",
        bucket="{{ var.value.composer_bucket }}",
        source_objects=["data/bls/unemployment_cps_series/data_output.csv"],
        source_format="CSV",
        destination_project_dataset_table="bls.unemployment_cps_series",
        skip_leading_rows=1,
        allow_quoted_newlines=True,
        write_disposition="WRITE_TRUNCATE",
        schema_fields=[
            {"name": "series_id", "type": "STRING", "mode": "required"},
            {"name": "lfst_code", "type": "INTEGER", "mode": "NULLABLE"},
            {"name": "periodicity_code", "type": "STRING", "mode": "NULLABLE"},
            {"name": "series_title", "type": "STRING", "mode": "NULLABLE"},
            {"name": "absn_code", "type": "INTEGER", "mode": "NULLABLE"},
            {"name": "activity_code", "type": "INTEGER", "mode": "NULLABLE"},
            {"name": "ages_code", "type": "INTEGER", "mode": "NULLABLE"},
            {"name": "class_code", "type": "INTEGER", "mode": "NULLABLE"},
            {"name": "duration_code", "type": "INTEGER", "mode": "NULLABLE"},
            {"name": "education_code", "type": "INTEGER", "mode": "NULLABLE"},
            {"name": "entr_code", "type": "INTEGER", "mode": "NULLABLE"},
            {"name": "expr_code", "type": "INTEGER", "mode": "NULLABLE"},
            {"name": "hheader_code", "type": "INTEGER", "mode": "NULLABLE"},
            {"name": "hour_code", "type": "INTEGER", "mode": "NULLABLE"},
            {"name": "indy_code", "type": "INTEGER", "mode": "NULLABLE"},
            {"name": "jdes_code", "type": "INTEGER", "mode": "NULLABLE"},
            {"name": "look_code", "type": "INTEGER", "mode": "NULLABLE"},
            {"name": "mari_code", "type": "INTEGER", "mode": "NULLABLE"},
            {"name": "mjhs_code", "type": "INTEGER", "mode": "NULLABLE"},
            {"name": "occupation_code", "type": "INTEGER", "mode": "NULLABLE"},
            {"name": "orig_code", "type": "INTEGER", "mode": "NULLABLE"},
            {"name": "pcts_code", "type": "INTEGER", "mode": "NULLABLE"},
            {"name": "race_code", "type": "INTEGER", "mode": "NULLABLE"},
            {"name": "rjnw_code", "type": "INTEGER", "mode": "NULLABLE"},
            {"name": "rnlf_code", "type": "INTEGER", "mode": "NULLABLE"},
            {"name": "rwns_code", "type": "INTEGER", "mode": "NULLABLE"},
            {"name": "seek_code", "type": "INTEGER", "mode": "NULLABLE"},
            {"name": "sexs_code", "type": "INTEGER", "mode": "NULLABLE"},
            {"name": "tdat_code", "type": "INTEGER", "mode": "NULLABLE"},
            {"name": "vets_code", "type": "INTEGER", "mode": "NULLABLE"},
            {"name": "wkst_code", "type": "INTEGER", "mode": "NULLABLE"},
            {"name": "born_code", "type": "INTEGER", "mode": "NULLABLE"},
            {"name": "chld_code", "type": "INTEGER", "mode": "NULLABLE"},
            {"name": "disa_code", "type": "INTEGER", "mode": "NULLABLE"},
            {"name": "seasonal", "type": "STRING", "mode": "NULLABLE"},
            {"name": "footnote_codes", "type": "STRING", "mode": "NULLABLE"},
            {"name": "begin_year", "type": "INTEGER", "mode": "NULLABLE"},
            {"name": "begin_period", "type": "STRING", "mode": "NULLABLE"},
            {"name": "end_year", "type": "INTEGER", "mode": "NULLABLE"},
            {"name": "end_period", "type": "STRING", "mode": "NULLABLE"},
            {"name": "cert_code", "type": "INTEGER", "mode": "NULLABLE"},
        ],
    )

    transform_csv >> load_to_bq
