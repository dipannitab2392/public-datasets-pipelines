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


from airflow.contrib.operators import gcs_to_bq
from airflow.contrib.operators import kubernetes_pod_operator
from airflow import DAG


default_args = {
    "owner": "Google",
    "depends_on_past": False,
    "start_date": "2021-03-01",
}


with DAG(
    dag_id="irs_990.irs_990_ez_2014",
    default_args=default_args,
    max_active_runs=1,
    schedule_interval="@daily",
    catchup=False,
    default_view="graph",
) as dag:

    # Run CSV transform within kubernetes pod
    irs_990_ez_2014_transform_csv = kubernetes_pod_operator.KubernetesPodOperator(
        task_id="irs_990_ez_2014_transform_csv",
        startup_timeout_seconds=600,
        name="irs_990_ez_2014",
        namespace="default",
        image_pull_policy="Always",
        image="{{ var.json.irs_990_irs_990_ez_2014.container_registry.run_csv_transform_kub_ez }}",
        env_vars={
            "SOURCE_URL": "https://www.irs.gov/pub/irs-soi/14eofinextract990ez.zip",
            "SOURCE_FILE": "files/data.dat",
            "TARGET_FILE": "files/data_output.csv",
            "TARGET_GCS_BUCKET": "{{ var.json.shared.composer_bucket }}",
            "TARGET_GCS_PATH": "data/irs_990/irs_990_ez_2014/data_output.csv",
        },
        resources={"request_memory": "4G", "request_cpu": "1"},
    )

    # Task to load CSV data to a BigQuery table
    load_irs_990_ez_2014_to_bq = gcs_to_bq.GoogleCloudStorageToBigQueryOperator(
        task_id="load_irs_990_ez_2014_to_bq",
        bucket="{{ var.json.shared.composer_bucket }}",
        source_objects=["data/irs_990/irs_990_ez_2014/data_output.csv"],
        source_format="CSV",
        destination_project_dataset_table="irs_990.irs_990_ez_2014",
        skip_leading_rows=1,
        write_disposition="WRITE_TRUNCATE",
        schema_fields=[
            {
                "name": "ein",
                "type": "string",
                "description": "Employer Identification Number",
                "mode": "required",
            },
            {
                "name": "tax_pd",
                "type": "integer",
                "description": "Tax period",
                "mode": "nullable",
            },
            {
                "name": "subseccd",
                "type": "integer",
                "description": "Subsection code",
                "mode": "nullable",
            },
            {
                "name": "totcntrbs",
                "type": "integer",
                "description": "Contributions gifts grants etc received",
                "mode": "nullable",
            },
            {
                "name": "prgmservrev",
                "type": "integer",
                "description": "Program service revenue",
                "mode": "nullable",
            },
            {
                "name": "duesassesmnts",
                "type": "integer",
                "description": "Membership dues and assessments",
                "mode": "nullable",
            },
            {
                "name": "othrinvstinc",
                "type": "integer",
                "description": "Investment income",
                "mode": "nullable",
            },
            {
                "name": "grsamtsalesastothr",
                "type": "integer",
                "description": "Gross amount from sale of assets",
                "mode": "nullable",
            },
            {
                "name": "basisalesexpnsothr",
                "type": "integer",
                "description": "Cost or other basis and sales expenses",
                "mode": "nullable",
            },
            {
                "name": "gnsaleofastothr",
                "type": "integer",
                "description": "Gain or (loss) from sale of assets",
                "mode": "nullable",
            },
            {
                "name": "grsincgaming",
                "type": "integer",
                "description": "Gross income from gaming",
                "mode": "nullable",
            },
            {
                "name": "grsrevnuefndrsng",
                "type": "integer",
                "description": "Special events gross revenue",
                "mode": "nullable",
            },
            {
                "name": "direxpns",
                "type": "integer",
                "description": "Special events direct expenses",
                "mode": "nullable",
            },
            {
                "name": "netincfndrsng",
                "type": "integer",
                "description": "Special events net income (or loss)",
                "mode": "nullable",
            },
            {
                "name": "grsalesminusret",
                "type": "integer",
                "description": "Gross sales of inventory",
                "mode": "nullable",
            },
            {
                "name": "costgoodsold",
                "type": "integer",
                "description": "Less: cost of goods sold",
                "mode": "nullable",
            },
            {
                "name": "grsprft",
                "type": "integer",
                "description": "Gross profit (or loss) from sales of inventory",
                "mode": "nullable",
            },
            {
                "name": "othrevnue",
                "type": "integer",
                "description": "Other revenue - total",
                "mode": "nullable",
            },
            {
                "name": "totrevnue",
                "type": "integer",
                "description": "Total revenue",
                "mode": "nullable",
            },
            {
                "name": "totexpns",
                "type": "integer",
                "description": "Total expenses",
                "mode": "nullable",
            },
            {
                "name": "totexcessyr",
                "type": "integer",
                "description": "Excess or deficit",
                "mode": "nullable",
            },
            {
                "name": "othrchgsnetassetfnd",
                "type": "integer",
                "description": "Other changes in net assets",
                "mode": "nullable",
            },
            {
                "name": "networthend",
                "type": "integer",
                "description": "Net assets EOY",
                "mode": "nullable",
            },
            {
                "name": "totassetsend",
                "type": "integer",
                "description": "Total assets e-o-y",
                "mode": "nullable",
            },
            {
                "name": "totliabend",
                "type": "integer",
                "description": "Total liabilities e-o-y",
                "mode": "nullable",
            },
            {
                "name": "totnetassetsend",
                "type": "integer",
                "description": "Total net worth e-o-y",
                "mode": "nullable",
            },
            {
                "name": "actvtynotprevrptcd",
                "type": "string",
                "description": "Activity not previously reported?",
                "mode": "nullable",
            },
            {
                "name": "chngsinorgcd",
                "type": "string",
                "description": "Significant changes to governing docs?",
                "mode": "nullable",
            },
            {
                "name": "unrelbusincd",
                "type": "string",
                "description": "UBI over $1000?",
                "mode": "nullable",
            },
            {
                "name": "filedf990tcd",
                "type": "string",
                "description": "Organization Filed 990T",
                "mode": "nullable",
            },
            {
                "name": "contractioncd",
                "type": "string",
                "description": "Liquidation dissolution termination or contraction",
                "mode": "nullable",
            },
            {
                "name": "politicalexpend",
                "type": "integer",
                "description": "Direct or indirect political expenditures",
                "mode": "nullable",
            },
            {
                "name": "filedf1120polcd",
                "type": "string",
                "description": "File Form 1120-POL?",
                "mode": "nullable",
            },
            {
                "name": "loanstoofficerscd",
                "type": "string",
                "description": "Loans to/from officers directors or trustees?",
                "mode": "nullable",
            },
            {
                "name": "loanstoofficers",
                "type": "integer",
                "description": "Amount of loans to/from officers",
                "mode": "nullable",
            },
            {
                "name": "initiationfee",
                "type": "integer",
                "description": "Initiation fees and capital contributions",
                "mode": "nullable",
            },
            {
                "name": "grspublicrcpts",
                "type": "integer",
                "description": "Gross receipts for public use of club facilities",
                "mode": "nullable",
            },
            {
                "name": "s4958excessbenefcd",
                "type": "string",
                "description": "Section 4958 excess benefit transactions?",
                "mode": "nullable",
            },
            {
                "name": "prohibtdtxshltrcd",
                "type": "string",
                "description": "Party to a prohibited tax shelter transaction?",
                "mode": "nullable",
            },
            {
                "name": "nonpfrea",
                "type": "integer",
                "description": "Reason for non-PF status",
                "mode": "nullable",
            },
            {
                "name": "totnooforgscnt",
                "type": "integer",
                "description": "Number of organizations supported",
                "mode": "nullable",
            },
            {
                "name": "totsupport",
                "type": "integer",
                "description": "Sum of amounts of support",
                "mode": "nullable",
            },
            {
                "name": "gftgrntsrcvd170",
                "type": "integer",
                "description": "Gifts grants membership fees received (170)",
                "mode": "nullable",
            },
            {
                "name": "txrevnuelevied170",
                "type": "integer",
                "description": "Tax revenues levied (170)",
                "mode": "nullable",
            },
            {
                "name": "srvcsval170",
                "type": "integer",
                "description": "Services or facilities furnished by gov (170)",
                "mode": "nullable",
            },
            {
                "name": "pubsuppsubtot170",
                "type": "integer",
                "description": "Public support subtotal (170)",
                "mode": "nullable",
            },
            {
                "name": "exceeds2pct170",
                "type": "integer",
                "description": "Amount support exceeds total (170)",
                "mode": "nullable",
            },
            {
                "name": "pubsupplesspct170",
                "type": "integer",
                "description": "Public support (170)",
                "mode": "nullable",
            },
            {
                "name": "samepubsuppsubtot170",
                "type": "integer",
                "description": "Public support from line 4 (170)",
                "mode": "nullable",
            },
            {
                "name": "grsinc170",
                "type": "integer",
                "description": "Gross income from interest etc (170)",
                "mode": "nullable",
            },
            {
                "name": "netincunreltd170",
                "type": "integer",
                "description": "Net UBI (170)",
                "mode": "nullable",
            },
            {
                "name": "othrinc170",
                "type": "integer",
                "description": "Other income (170)",
                "mode": "nullable",
            },
            {
                "name": "totsupp170",
                "type": "integer",
                "description": "Total support (170)",
                "mode": "nullable",
            },
            {
                "name": "grsrcptsrelated170",
                "type": "integer",
                "description": "Gross receipts from related activities (170)",
                "mode": "nullable",
            },
            {
                "name": "totgftgrntrcvd509",
                "type": "integer",
                "description": "Gifts grants membership fees received (509)",
                "mode": "nullable",
            },
            {
                "name": "grsrcptsadmissn509",
                "type": "integer",
                "description": "Receipts from admissions merchandise etc (509)",
                "mode": "nullable",
            },
            {
                "name": "grsrcptsactivities509",
                "type": "integer",
                "description": "Gross receipts from related activities (509)",
                "mode": "nullable",
            },
            {
                "name": "txrevnuelevied509",
                "type": "integer",
                "description": "Tax revenues levied (509)",
                "mode": "nullable",
            },
            {
                "name": "srvcsval509",
                "type": "integer",
                "description": "Services or facilities furnished by gov (509)",
                "mode": "nullable",
            },
            {
                "name": "pubsuppsubtot509",
                "type": "integer",
                "description": "Public support subtotal (509)",
                "mode": "nullable",
            },
            {
                "name": "rcvdfrmdisqualsub509",
                "type": "integer",
                "description": "Amounts from disqualified persons (509)",
                "mode": "nullable",
            },
            {
                "name": "exceeds1pct509",
                "type": "integer",
                "description": "Amount support exceeds total (509)",
                "mode": "nullable",
            },
            {
                "name": "subtotpub509",
                "type": "integer",
                "description": "Public support subtotal (509)",
                "mode": "nullable",
            },
            {
                "name": "pubsupplesub509",
                "type": "integer",
                "description": "Public support (509)",
                "mode": "nullable",
            },
            {
                "name": "samepubsuppsubtot509",
                "type": "integer",
                "description": "Public support from line 6 (509)",
                "mode": "nullable",
            },
            {
                "name": "grsinc509",
                "type": "integer",
                "description": "Gross income from interest etc (509)",
                "mode": "nullable",
            },
            {
                "name": "unreltxincls511tx509",
                "type": "integer",
                "description": "Net UBI (509)",
                "mode": "nullable",
            },
            {
                "name": "subtotsuppinc509",
                "type": "integer",
                "description": "Subtotal total support (509)",
                "mode": "nullable",
            },
            {
                "name": "netincunrelatd509",
                "type": "integer",
                "description": "Net income from UBI not in 10b (509)",
                "mode": "nullable",
            },
            {
                "name": "othrinc509",
                "type": "integer",
                "description": "Other income (509)",
                "mode": "nullable",
            },
            {
                "name": "totsupp509",
                "type": "integer",
                "description": "Total support (509)",
                "mode": "nullable",
            },
        ],
    )

    irs_990_ez_2014_transform_csv >> load_irs_990_ez_2014_to_bq
