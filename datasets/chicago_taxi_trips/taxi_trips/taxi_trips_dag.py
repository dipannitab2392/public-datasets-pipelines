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
        name="taxi_trips",
        namespace="default",
        affinity={
            "nodeAffinity": {
                "requiredDuringSchedulingIgnoredDuringExecution": {
                    "nodeSelectorTerms": [
                        {
                            "matchExpressions": [
                                {
                                    "key": "cloud.google.com/gke-nodepool",
                                    "operator": "In",
                                    "values": ["pool-e2-standard-4"],
                                }
                            ]
                        }
                    ]
                }
            }
        },
        image_pull_policy="Always",
        image="{{ var.json.chicago_taxi_trips.container_registry.run_csv_transform_kub }}",
        env_vars={
            "SOURCE_URL": "https://data.cityofchicago.org/api/views/wrvz-psew/rows.csv",
            "SOURCE_FILE": "files/data1.csv",
            "TARGET_FILE": "files/data_output.csv",
            "TARGET_GCS_BUCKET": "{{ var.value.composer_bucket }}",
            "TARGET_GCS_PATH": "data/chicago_taxi_trips/taxi_trips/data_output.csv",
            "RENAME_MAPPINGS": '{"Trip ID" : "unique_key" ,"Taxi ID" : "taxi_id" ,"Trip Start Timestamp" : "trip_start_timestamp" ,"Trip End Timestamp" : "trip_end_timestamp" ,"Trip Seconds" : "trip_seconds" ,"Trip Miles" : "trip_miles" ,"Pickup Census Tract" : "pickup_census_tract" ,"Dropoff Census Tract" : "dropoff_census_tract" ,"Pickup Community Area" : "pickup_community_area" ,"Dropoff Community Area" : "dropoff_community_area" ,"Fare" : "fare" ,"Tips" : "tips" ,"Tolls" : "tolls" ,"Extras" : "extras" ,"Trip Total" : "trip_total" ,"Payment Type" : "payment_type" ,"Company" : "company" ,"Pickup Centroid Latitude" : "pickup_latitude" ,"Pickup Centroid Longitude" : "pickup_longitude" ,"Pickup Centroid Location" : "pickup_location" ,"Dropoff Centroid Latitude" : "dropoff_latitude" ,"Dropoff Centroid Longitude" : "dropoff_longitude" ,"Dropoff Centroid  Location" : "dropoff_location"}',
        },
        resources={"request_memory": "8G", "request_cpu": "3"},
    )

    # Task to load CSV data to a BigQuery table
    load_chicago_taxi_trips_to_bq = gcs_to_bq.GoogleCloudStorageToBigQueryOperator(
        task_id="load_chicago_taxi_trips_to_bq",
        bucket="{{ var.value.composer_bucket }}",
        source_objects=["data/chicago_taxi_trips/taxi_trips/data_output.csv"],
        source_format="CSV",
        destination_project_dataset_table="chicago_taxi_trips.taxi_trips",
        skip_leading_rows=1,
        allow_quoted_newlines=True,
        write_disposition="WRITE_TRUNCATE",
        schema_fields=[
            {
                "name": "unique_key",
                "type": "string",
                "description": "Unique identifier for the trip.",
                "mode": "required",
            },
            {
                "name": "taxi_id",
                "type": "string",
                "description": "A unique identifier for the taxi.",
                "mode": "required",
            },
            {
                "name": "trip_start_timestamp",
                "type": "timestamp",
                "description": "When the trip started, rounded to the nearest 15 minutes.",
                "mode": "nullable",
            },
            {
                "name": "trip_end_timestamp",
                "type": "timestamp",
                "description": "When the trip ended, rounded to the nearest 15 minutes.",
                "mode": "nullable",
            },
            {
                "name": "trip_seconds",
                "type": "integer",
                "description": "Time of the trip in seconds.",
                "mode": "nullable",
            },
            {
                "name": "trip_miles",
                "type": "float",
                "description": "Distance of the trip in miles.",
                "mode": "nullable",
            },
            {
                "name": "pickup_census_tract",
                "type": "integer",
                "description": "The Census Tract where the trip began. For privacy, this Census Tract is not shown for some trips.",
                "mode": "nullable",
            },
            {
                "name": "dropoff_census_tract",
                "type": "integer",
                "description": "The Census Tract where the trip ended. For privacy, this Census Tract is not shown for some trips.",
                "mode": "nullable",
            },
            {
                "name": "pickup_community_area",
                "type": "integer",
                "description": "The Community Area where the trip began.",
                "mode": "nullable",
            },
            {
                "name": "dropoff_community_area",
                "type": "integer",
                "description": "The Community Area where the trip ended.",
                "mode": "nullable",
            },
            {
                "name": "fare",
                "type": "float",
                "description": "The fare for the trip.",
                "mode": "nullable",
            },
            {
                "name": "tips",
                "type": "float",
                "description": "The tip for the trip. Cash tips generally will not be recorded.",
                "mode": "nullable",
            },
            {
                "name": "tolls",
                "type": "float",
                "description": "The tolls for the trip.",
                "mode": "nullable",
            },
            {
                "name": "extras",
                "type": "float",
                "description": "Extra charges for the trip.",
                "mode": "nullable",
            },
            {
                "name": "trip_total",
                "type": "float",
                "description": "Total cost of the trip, the total of the fare, tips, tolls, and extras.",
                "mode": "nullable",
            },
            {
                "name": "payment_type",
                "type": "string",
                "description": "Type of payment for the trip.",
                "mode": "nullable",
            },
            {
                "name": "company",
                "type": "string",
                "description": "The taxi company.",
                "mode": "nullable",
            },
            {
                "name": "pickup_latitude",
                "type": "float",
                "description": "The latitude of the center of the pickup census tract or the community area if the census tract has been hidden for privacy.",
                "mode": "nullable",
            },
            {
                "name": "pickup_longitude",
                "type": "float",
                "description": "The longitude of the center of the pickup census tract or the community area if the census tract has been hidden for privacy.",
                "mode": "nullable",
            },
            {
                "name": "pickup_location",
                "type": "string",
                "description": "The location of the center of the pickup census tract or the community area if the census tract has been hidden for privacy.",
                "mode": "nullable",
            },
            {
                "name": "dropoff_latitude",
                "type": "float",
                "description": "The latitude of the center of the dropoff census tract or the community area if the census tract has been hidden for privacy.",
                "mode": "nullable",
            },
            {
                "name": "dropoff_longitude",
                "type": "float",
                "description": "The longitude of the center of the dropoff census tract or the community area if the census tract has been hidden for privacy.",
                "mode": "nullable",
            },
            {
                "name": "dropoff_location",
                "type": "string",
                "description": "The location of the center of the dropoff census tract or the community area if the census tract has been hidden for privacy.",
                "mode": "nullable",
            },
        ],
    )

    chicago_taxi_trips_transform_csv >> load_chicago_taxi_trips_to_bq
