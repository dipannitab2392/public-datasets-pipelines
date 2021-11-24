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
import os
import pathlib
import re
import subprocess
import typing

import pandas as pd
from google.cloud import storage


def main(
    source_urls: typing.List[str],
    source_files: typing.List[pathlib.Path],
    target_file: pathlib.Path,
    target_gcs_bucket: str,
    target_gcs_path: str,
    # headers: typing.List[str],
    pipeline_name: str,
    # joining_key: str,
    # columns: typing.List[str],
) -> None:

    logging.info(
        f"ML Commons {pipeline_name} process started at "
        + str(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    )

    logging.info("Creating 'files' folder")
    pathlib.Path("./files").mkdir(parents=True, exist_ok=True)

    logging.info("Download file...")
    download_file(source_urls, source_files)

    logging.info("Read file to a datafrmae...")
    df=read_file_to_dataframe(source_files)

    logging.info(f"Saving to output file.. {target_file}")
    try:
        save_to_new_file(df, file_path=str(target_file))
    except Exception as e:
        logging.error(f"Error saving output file: {e}.")

    logging.info(
        f"Uploading output file to.. gs://{target_gcs_bucket}/{target_gcs_path}"
    )
    upload_file_to_gcs(target_file, target_gcs_bucket, target_gcs_path)

    logging.info(
        f"ML Commons {pipeline_name} process completed at "
        + str(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    )


def download_file(
    source_urls: typing.List[str], source_files: typing.List[pathlib.Path]
) -> None:
    for url, file in zip(source_urls, source_files):
        logging.info(f"Downloading file from {url} ...")
        subprocess.check_call(["gsutil", "cp", f"{url}", f"{file}"])
        # subprocess.check_call(["gsutil", "-i", "public-data-mlcommons-speech@bigquery-public-data-dev.iam.gserviceaccount.com","cp", f"{url}", f"{file}"])


def read_file_to_dataframe(source_files) :
    for file in source_files :
        with open(file) as f:
            data = [json.loads(line) for line in f]
    df = pd.json_normalize(data)
    df= df.explode(['training_data.duration_ms', 'training_data.label','training_data.name'])
    return df

def save_to_new_file(df: pd.DataFrame, file_path: str) -> None:
    df.to_csv(file_path, index=False)


def upload_file_to_gcs(file_path: pathlib.Path, gcs_bucket: str, gcs_path: str) -> None:
    storage_client = storage.Client()
    bucket = storage_client.bucket(gcs_bucket)
    blob = bucket.blob(gcs_path)
    blob.upload_from_filename(file_path)


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)

    main(
        source_urls=json.loads(os.environ["SOURCE_URLS"]),
        source_files=json.loads(os.environ["SOURCE_FILES"]),
        target_file=pathlib.Path(os.environ["TARGET_FILE"]).expanduser(),
        target_gcs_bucket=os.environ["TARGET_GCS_BUCKET"],
        target_gcs_path=os.environ["TARGET_GCS_PATH"],
        # headers=json.loads(os.environ["CSV_HEADERS"]),
        pipeline_name=os.environ["PIPELINE_NAME"],
        # joining_key=os.environ["JOINING_KEY"],
        # columns=json.loads(os.environ["TRIM_SPACE"]),
    )
