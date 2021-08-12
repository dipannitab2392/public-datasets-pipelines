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


import datetime
import fnmatch
import logging
import os
import pathlib
from zipfile import ZipFile

import pandas as pd

# import numpy as np
import requests
from google.cloud import storage


def main(
    source_url: str,
    source_file: pathlib.Path,
    source_csv_name: str,
    target_file: pathlib.Path,
    target_gcs_bucket: str,
    target_gcs_path: str,
):

    logging.info(
        "google political ads process started at "
        + str(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    )

    logging.info("creating 'files' folder")
    pathlib.Path("./files").mkdir(parents=True, exist_ok=True)

    logging.info(f"Downloading file {source_url}")
    download_file(source_url, source_file)

    logging.info(f"Opening file {source_file}")
    df = read_csv_file(source_file, source_csv_name)

    logging.info(f"Transforming.. {source_file}")

    logging.info(f"Transform: Rename columns.. {source_file}")
    rename_headers(df)

    logging.info("Transform: Reordering headers.. ")
    df = df[
        [
            "ad_id",
            "ad_url",
            "ad_type",
            "regions",
            "advertiser_id",
            "advertiser_name",
            "ad_campaigns_list",
            "date_range_start",
            "date_range_end",
            "num_of_days",
            "impressions",
            "spend_usd",
            "first_served_timestamp",
            "last_served_timestamp",
            "age_targeting",
            "gender_targeting",
            "geo_targeting_included",
            "geo_targeting_excluded",
            "spend_range_min_usd",
            "spend_range_max_usd",
            "spend_range_min_eur",
            "spend_range_max_eur",
            "spend_range_min_inr",
            "spend_range_max_inr",
            "spend_range_min_bgn",
            "spend_range_max_bgn",
            "spend_range_min_hrk",
            "spend_range_max_hrk",
            "spend_range_min_czk",
            "spend_range_max_czk",
            "spend_range_min_dkk",
            "spend_range_max_dkk",
            "spend_range_min_huf",
            "spend_range_max_huf",
            "spend_range_min_pln",
            "spend_range_max_pln",
            "spend_range_min_ron",
            "spend_range_max_ron",
            "spend_range_min_sek",
            "spend_range_max_sek",
            "spend_range_min_gbp",
            "spend_range_max_gbp",
            "spend_range_min_nzd",
            "spend_range_max_nzd",
        ]
    ]

    logging.info(f"Saving to output file.. {target_file}")
    try:
        save_to_new_file(df, file_path=str(target_file))
    except Exception as e:
        logging.error(f"Error saving output file: {e}.")

    # upload to GCS
    logging.info(
        f"Uploading output file to.. gs://{target_gcs_bucket}/{target_gcs_path}"
    )
    upload_file_to_gcs(target_file, target_gcs_bucket, target_gcs_path)

    logging.info(
        "Google Political Ads process completed at "
        + str(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    )


def save_to_new_file(df, file_path):
    df.to_csv(file_path, index=False)


def upload_file_to_gcs(file_path: pathlib.Path, gcs_bucket: str, gcs_path: str) -> None:
    storage_client = storage.Client()
    bucket = storage_client.bucket(gcs_bucket)
    blob = bucket.blob(gcs_path)
    blob.upload_from_filename(file_path)


def download_file(source_url: str, source_file: pathlib.Path):
    logging.info(f"Downloading {source_url} into {source_file}")
    r = requests.get(source_url, stream=True)
    if r.status_code == 200:
        with open(source_file, "wb") as f:
            for chunk in r:
                f.write(chunk)
    else:
        logging.error(f"Couldn't download {source_url}: {r.text}")


def read_csv_file(source_file, source_csv_name):
    with ZipFile(source_file) as zipfiles:
        file_list = zipfiles.namelist()
        csv_files = fnmatch.filter(file_list, source_csv_name)
        data = [pd.read_csv(zipfiles.open(file_name)) for file_name in csv_files]

    df = pd.concat(data)
    return df


def rename_headers(df):
    header_names = {
        "Ad_ID": "ad_id",
        "Ad_URL": "ad_url",
        "Ad_Type": "ad_type",
        "Regions": "regions",
        "Advertiser_ID": "advertiser_id",
        "Advertiser_Name": "advertiser_name",
        "Ad_Campaigns_List": "ad_campaigns_list",
        "Date_Range_Start": "date_range_start",
        "Date_Range_End": "date_range_end",
        "Num_of_Days": "num_of_days",
        "Impressions": "impressions",
        "Spend_USD": "spend_usd",
        "Spend_Range_Min_USD": "spend_range_min_usd",
        "Spend_Range_Max_USD": "spend_range_max_usd",
        "Spend_Range_Min_EUR": "spend_range_min_eur",
        "Spend_Range_Max_EUR": "spend_range_max_eur",
        "Spend_Range_Min_INR": "spend_range_min_inr",
        "Spend_Range_Max_INR": "spend_range_max_inr",
        "Spend_Range_Min_BGN": "spend_range_min_bgn",
        "Spend_Range_Max_BGN": "spend_range_max_bgn",
        "Spend_Range_Min_HRK": "spend_range_min_hrk",
        "Spend_Range_Max_HRK": "spend_range_max_hrk",
        "Spend_Range_Min_CZK": "spend_range_min_czk",
        "Spend_Range_Max_CZK": "spend_range_max_czk",
        "Spend_Range_Min_DKK": "spend_range_min_dkk",
        "Spend_Range_Max_DKK": "spend_range_max_dkk",
        "Spend_Range_Min_HUF": "spend_range_min_huf",
        "Spend_Range_Max_HUF": "spend_range_max_huf",
        "Spend_Range_Min_PLN": "spend_range_min_pln",
        "Spend_Range_Max_PLN": "spend_range_max_pln",
        "Spend_Range_Min_RON": "spend_range_min_ron",
        "Spend_Range_Max_RON": "spend_range_max_ron",
        "Spend_Range_Min_SEK": "spend_range_min_sek",
        "Spend_Range_Max_SEK": "spend_range_max_sek",
        "Spend_Range_Min_GBP": "spend_range_min_gbp",
        "Spend_Range_Max_GBP": "spend_range_max_gbp",
        "Spend_Range_Min_NZD": "spend_range_min_nzd",
        "Spend_Range_Max_NZD": "spend_range_max_nzd",
        "Age_Targeting": "age_targeting",
        "Gender_Targeting": "gender_targeting",
        "Geo_Targeting_Included": "geo_targeting_included",
        "Geo_Targeting_Excluded": "geo_targeting_excluded",
        "First_Served_Timestamp": "first_served_timestamp",
        "Last_Served_Timestamp": "last_served_timestamp",
    }
    df.rename(columns=header_names, inplace=True)


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)

    main(
        source_url=os.environ["SOURCE_URL"],
        source_file=pathlib.Path(os.environ["SOURCE_FILE"]).expanduser(),
        source_csv_name=os.environ["FILE_NAME"],
        target_file=pathlib.Path(os.environ["TARGET_FILE"]).expanduser(),
        target_gcs_bucket=os.environ["TARGET_GCS_BUCKET"],
        target_gcs_path=os.environ["TARGET_GCS_PATH"],
    )
