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
import re
import subprocess
import typing

import pandas as pd
import requests
from google.cloud import storage


def main(
    source_url: str,
    source_file: pathlib.Path,
    target_file: pathlib.Path,
    target_gcs_bucket: str,
    target_gcs_path: str,
    # headers: typing.List[str],
    rename_mappings: dict,
    # pipeline_name: str,
) -> None:

    logging.info(
        "Chicago Taxi Trips process started at "
        + str(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    )

    logging.info("Creating 'files' folder")
    pathlib.Path("./files").mkdir(parents=True, exist_ok=True)

    logging.info(f"Downloading file from {source_url}...")
    download_file(source_url, source_file)

    logging.info(f"Opening file {source_file}...")
    with pd.read_csv(
        source_file,
        # engine="python",
        # encoding="utf-8",
        # quotechar='"',
        # compression="gzip",
        chunksize=1000000,
    ) as reader:
        for chunk_number, chunk in enumerate(reader):
            logging.info(f"Processing batch {chunk_number}")
            target_file_batch = str(target_file).replace(
                ".csv", "-" + str(chunk_number) + ".csv"
            )
            df = pd.DataFrame()
            df = pd.concat([df, chunk])

            logging.info(f"Transforming {source_file}... ")

            logging.info(f"Transform: Rename columns.. {source_file}")
            rename_headers(df, rename_mappings)

            logging.info(f"Transform: Converting date values for {source_file}... ")
            convert_values(df)

            logging.info("Transform: Search and replacing values...")
            search_and_replace(df)

            logging.info(f"Transform: filtering rows for {source_file}... ")
            filter_null_rows(df)

            logging.info(f"Transform: converting to integer {source_file}... ")
            convert_to_integer_string(df)

            processChunk(df, target_file_batch)

            logging.info(f"Appending batch {chunk_number} to {target_file}")
            if chunk_number == 0:
                subprocess.run(["cp", target_file_batch, target_file])
            else:
                subprocess.check_call(f"sed -i '1d' {target_file_batch}", shell=True)
                subprocess.check_call(
                    f"cat {target_file_batch} >> {target_file}", shell=True
                )
            subprocess.run(["rm", target_file_batch])

            # logging.info(f"Saving to output file.. {target_file}")
            # try:
            #     save_to_new_file(df, file_path=str(target_file))
            # except Exception as e:
            #     logging.error(f"Error saving output file: {e}.")

        logging.info(
            f"Uploading output file to.. gs://{target_gcs_bucket}/{target_gcs_path}"
        )
        upload_file_to_gcs(target_file, target_gcs_bucket, target_gcs_path)

    logging.info(
        f"Chicago Taxi Trips process completed at "
        + str(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    )


def processChunk(df: pd.DataFrame, target_file_batch: str) -> None:

    logging.info(f"Saving to output file.. {target_file_batch}")
    try:
        save_to_new_file(df, file_path=str(target_file_batch))
    except Exception as e:
        logging.error(f"Error saving output file: {e}.")
    logging.info("..Done!")


def integer_string(input: typing.Union[str, float]) -> str:
    str_val = ""
    if not input or (math.isnan(input)):
        str_val = ""
    else:
        str_val = str(int(round(input, 0)))
    return str_val


def convert_to_integer_string(df):
    col = [
        "trip_seconds",
        "pickup_census_tract",
        "dropoff_census_tract",
        "pickup_community_area",
        "dropoff_community_area",
    ]

    for columns in col:
        df[columns] = df[columns].apply(integer_string)


def rename_headers(df: pd.DataFrame, rename_mappings: dict) -> None:
    df.rename(columns=rename_mappings, inplace=True)


# def convert_dt_format(dt_str: str) -> str:
#     # Old format: MM/dd/yyyy hh:mm:ss aa
#     # New format: yyyy-MM-dd HH:mm:ss
#     if not dt_str:
#         return dt_str
#     else:
#         return datetime.datetime.strptime(dt_str, "%m/%d/%Y %H:%M:%S %p").strftime(
#             "%Y-%m-%d %H:%M:%S"
#         )


def convert_dt_format(dt_str: str) -> str:
    # Old format: MM/dd/yyyy hh:mm:ss aa
    # New format: yyyy-MM-dd HH:mm:ss
    a = ""
    if not dt_str or str(dt_str) == "nan":
        return str(a)
    else:
        return datetime.datetime.strptime(str(dt_str), "%m/%d/%Y %H:%M:%S %p").strftime(
            "%Y-%m-%d %H:%M:%S"
        )


def convert_values(df):
    dt_cols = ["trip_end_timestamp", "trip_start_timestamp"]

    for dt_col in dt_cols:
        df[dt_col] = df[dt_col].apply(convert_dt_format)


def filter_null_rows(df):
    df = df.query('unique_key != "" | taxi_id !="" ')
    df = df.dropna(subset=["unique_key", "taxi_id"],inplace=True)


def reg_exp_tranformation(str_value, search_pattern, replace_val):
    str_value = re.sub(search_pattern, replace_val, str_value)
    return str_value


def search_and_replace(df):
    col = ["fare", "tips", "tolls", "extras", "trip_total"]

    for col in col:
        df[col] = (
            df[col].astype("str").apply(reg_exp_tranformation, args=(r"[^0-9.]", ""))
        )


def save_to_new_file(df: pd.DataFrame, file_path: str) -> None:
    df.to_csv(file_path, index=False)


def download_file(source_url: str, source_file: pathlib.Path) -> None:
    logging.info(f"Downloading {source_url} into {source_file}")
    r = requests.get(source_url, stream=True)
    if r.status_code == 200:
        with open(source_file, "wb") as f:
            for chunk in r:
                f.write(chunk)
    else:
        logging.error(f"Couldn't download {source_url}: {r.text}")


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
        # headers=json.loads(os.environ["CSV_HEADERS"]),
        rename_mappings=json.loads(os.environ["RENAME_MAPPINGS"]),
        # pipeline_name=os.environ["PIPELINE_NAME"],
    )
