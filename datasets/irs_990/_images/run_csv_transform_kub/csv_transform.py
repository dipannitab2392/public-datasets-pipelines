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
import json
import logging
import math
import os
import pathlib
import typing
from urllib.parse import urlparse

import pandas as pd

# import numpy as np
import requests
from google.cloud import storage


def main(
    source_url: str,
    source_file: pathlib.Path,
    target_file: pathlib.Path,
    target_gcs_bucket: str,
    target_gcs_path: str,
    headers: typing.List[str],
    rename_mappings: dict,
    pipeline_name: str
):

    logging.info(
        f"irs 990 {pipeline_name} process started at "
        + str(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    )

    logging.info("creating 'files' folder")
    pathlib.Path("./files").mkdir(parents=True, exist_ok=True)

    logging.info(f"Downloading file from {source_url}... ")
    download_file(source_url, source_file)

    # open the input file
    logging.info(f"Opening file {source_file}... ")

    if os.path.basename(urlparse(source_url).path) == "14eofinextract990.zip" or os.path.basename(urlparse(source_url).path) == "14eofinextract990ez.zip":
        df = pd.read_csv(
            str(source_file), compression="zip", encoding="utf-8", sep=r"\s+"
        )
    else:
        df = pd.read_csv(str(source_file), encoding="utf-8", sep=r"\s+")

    # steps in the pipeline
    logging.info(f"Transforming.. {source_file}")

    logging.info(f"Transform: Rename columns.. {source_file}")

    rename_headers(df,rename_mappings)

    logging.info(f"Transform: filtering null values.. {source_file}")

    filter_null_rows(df)

    logging.info("Transform: Converting to integr.. ")

    df["totsupp509"] = df["totsupp509"].apply(convert_to_int)

    logging.info(
        f"Transform: Reordering headers for.. {os.path.basename(urlparse(source_url).path)}"
    )

    df = df[headers]

    # save to output file
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
        f"irs 990 {pipeline_name} process completed at "
        + str(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    )


def rename_headers(df,rename_mappings):
    df = df.rename(columns=rename_mappings, inplace=True)


def filter_null_rows(df):
    df = df[df.ein != ""]


def save_to_new_file(df, file_path):
    # df.export_csv(file_path)
    df.to_csv(file_path, index=False)


def download_file(source_url: str, source_file: pathlib.Path):
    logging.info(f"Downloading {source_url} into {source_file}")
    r = requests.get(source_url, stream=True)
    if r.status_code == 200:
        with open(source_file, "wb") as f:
            for chunk in r:
                f.write(chunk)
    else:
        logging.error(f"Couldn't download {source_url}: {r.text}")


def convert_to_int(input: str) -> str:
    str_val = ""
    if input == "" or (math.isnan(input)):
        str_val = ""
    else:
        str_val = str(int(round(input, 0)))
    return str_val


def upload_file_to_gcs(file_path: pathlib.Path, gcs_bucket: str, gcs_path: str) -> None:
    storage_client = storage.Client()
    bucket = storage_client.bucket(gcs_bucket)
    blob = bucket.blob(gcs_path)
    blob.upload_from_filename(file_path)


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)

    main(
        source_url=os.environ["SOURCE_URL"],
        source_file=pathlib.Path(os.environ["SOURCE_FILE"]).expanduser(),
        target_file=pathlib.Path(os.environ["TARGET_FILE"]).expanduser(),
        target_gcs_bucket=os.environ["TARGET_GCS_BUCKET"],
        target_gcs_path=os.environ["TARGET_GCS_PATH"],
        headers=json.loads(os.environ["CSV_HEADERS"]),
        rename_mappings=json.loads(os.environ["RENAME_MAPPINGS"]),
        pipeline_name=os.environ["PIPELINE_NAME"]
    )
