/**
 * Copyright 2021 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


resource "google_bigquery_table" "series_time" {
  project    = var.project_id
  dataset_id = "world_bank_wdi"
  table_id   = "series_time"

  description = "Series Times table"




  depends_on = [
    google_bigquery_dataset.world_bank_wdi
  ]
}

output "bigquery_table-series_time-table_id" {
  value = google_bigquery_table.series_time.table_id
}

output "bigquery_table-series_time-id" {
  value = google_bigquery_table.series_time.id
}
