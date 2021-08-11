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
    dag_id="chicago_taxi_trips.taxi_trips",
    default_args=default_args,
    max_active_runs=1,
    schedule_interval="@weekly",
    catchup=False,
    default_view="graph",
) as dag:

    # Run CSV transform within kubernetes pod
    chicago_taxi_trips_transform_csv = kubernetes_pod_operator.KubernetesPodOperator(
        task_id="chicago_taxi_trips_transform_csv",
        startup_timeout_seconds=600,
        name="taxi_trips",
        namespace="default",
        image_pull_policy="Always",
        image="{{ var.json.chicago_taxi_trips.container_registry.run_csv_transform_kub }}",
        env_vars={
            "SOURCE_URL": "https://data.cityofchicago.org/api/views/wrvz-psew/rows.csv",
            "SOURCE_FILE": "files/data.csv",
            "TARGET_FILE": "files/data_output.csv",
            "TARGET_GCS_BUCKET": "{{ var.json.shared.composer_bucket }}",
            "TARGET_GCS_PATH": "data/chicago_taxi_trips/taxi_trips/data_output.csv",
        },
        resources={"request_memory": "4G", "request_cpu": "1"},
    )

    # Task to load CSV data to a BigQuery table
    load_chicago_taxi_trips_to_bq = gcs_to_bq.GoogleCloudStorageToBigQueryOperator(
        task_id="load_chicago_taxi_trips_to_bq",
        bucket="{{ var.json.shared.composer_bucket }}",
        source_objects=["data/chicago_taxi_trips/taxi_trips/data_output.csv"],
        source_format="CSV",
        destination_project_dataset_table="chicago_taxi_trips.taxi_trips",
        skip_leading_rows=1,
        write_disposition="WRITE_TRUNCATE",
        schema_fields=[
            {"name": "unique_key", "type": "string", "mode": "required"},
            {"name": "taxi_id", "type": "string", "mode": "required"},
            {"name": "trip_start_timestamp", "type": "timestamp", "mode": "nullable"},
            {"name": "trip_end_timestamp", "type": "timestamp", "mode": "nullable"},
            {"name": "trip_seconds", "type": "integer", "mode": "nullable"},
            {"name": "trip_miles", "type": "float", "mode": "nullable"},
            {"name": "pickup_census_tract", "type": "integer", "mode": "nullable"},
            {"name": "dropoff_census_tract", "type": "integer", "mode": "nullable"},
            {"name": "pickup_community_area", "type": "integer", "mode": "nullable"},
            {"name": "dropoff_community_area", "mode": "nullable"},
            {"name": "fare", "type": "float", "mode": "nullable"},
            {"name": "tips", "type": "float", "mode": "nullable"},
            {"name": "tolls", "type": "float", "mode": "nullable"},
            {"name": "extras", "type": "float", "mode": "nullable"},
            {"name": "trip_total", "type": "float", "mode": "nullable"},
            {"name": "payment_  type", "type": "string", "mode": "nullable"},
            {"name": "company", "type": "string", "mode": "nullable"},
            {"name": "pickup_latitude", "type": "float", "mode": "nullable"},
            {"name": "pickup_longitude", "type": "float", "mode": "nullable"},
            {"name": "pickup_location", "type": "string", "mode": "nullable"},
            {"name": "dropoff_latitude", "type": "float", "mode": "nullable"},
            {"name": "dropoff_longitude", "type": "float", "mode": "nullable"},
            {"name": "dropoff_location", "type": "string", "mode": "nullable"},
        ],
    )

    chicago_taxi_trips_transform_csv >> load_chicago_taxi_trips_to_bq
