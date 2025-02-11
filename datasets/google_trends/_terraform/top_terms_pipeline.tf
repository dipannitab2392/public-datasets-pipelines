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


resource "google_bigquery_table" "google_trends_top_terms" {
  project    = var.project_id
  dataset_id = "google_trends"
  table_id   = "top_terms"

  description = "Daily top 25 terms in the United States with score, ranking, time, and designated market area"
  time_partitioning {
    type = "DAY"

    field = "refresh_date"

    require_partition_filter = false
  }


  schema = <<EOF
    [
  {
    "name": "refresh_date",
    "type": "DATE"
  },
  {
    "name": "dma_name",
    "type": "STRING"
  },
  {
    "name": "dma_id",
    "type": "INTEGER"
  },
  {
    "name": "term",
    "type": "STRING"
  },
  {
    "name": "week",
    "type": "DATE"
  },
  {
    "name": "score",
    "type": "INTEGER"
  },
  {
    "name": "rank",
    "type": "INTEGER"
  }
]
    EOF
  depends_on = [
    google_bigquery_dataset.google_trends
  ]
}

output "bigquery_table-google_trends_top_terms-table_id" {
  value = google_bigquery_table.google_trends_top_terms.table_id
}

output "bigquery_table-google_trends_top_terms-id" {
  value = google_bigquery_table.google_trends_top_terms.id
}

resource "google_bigquery_table" "google_trends_top_rising_terms" {
  project    = var.project_id
  dataset_id = "google_trends"
  table_id   = "top_rising_terms"

  description = "Daily top rising terms in the United States with score, ranking, time, and designated market area"
  time_partitioning {
    type = "DAY"

    field = "refresh_date"

    require_partition_filter = false
  }


  schema = <<EOF
    [
  {
    "name": "dma_name",
    "type": "STRING"
  },
  {
    "name": "dma_id",
    "type": "INTEGER"
  },
  {
    "name": "term",
    "type": "STRING"
  },
  {
    "name": "week",
    "type": "DATE"
  },
  {
    "name": "score",
    "type": "INTEGER"
  },
  {
    "name": "rank",
    "type": "INTEGER"
  },
  {
    "name": "percent_gain",
    "type": "INTEGER"
  },
  {
    "name": "refresh_date",
    "type": "DATE"
  }
]
    EOF
  depends_on = [
    google_bigquery_dataset.google_trends
  ]
}

output "bigquery_table-google_trends_top_rising_terms-table_id" {
  value = google_bigquery_table.google_trends_top_rising_terms.table_id
}

output "bigquery_table-google_trends_top_rising_terms-id" {
  value = google_bigquery_table.google_trends_top_rising_terms.id
}
