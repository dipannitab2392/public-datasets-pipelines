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

# CSV transform for: irs_990.irs_990_2015
#
#       Column Name                         Type            Length / Format                 Description
#
#       ID                                  integer         255                             ""
#       Case Number                         string          255                             ""
#       Date                                date          255                             ""
#       Block                               string          512                             ""
#       IUCR                                string          255                             ""
#       Primary Type                        integer         255                             ""
#       Description                         string          11/23/2020 01:41:21 PM          ""
#       Location Description                integer         11/23/2020 01:41:21 PM          ""
#       Arrest                              string          11/23/2020 01:41:21 PM          ""
#       Domestic                            integer         11/23/2020 01:41:21 PM          ""
#       Beat                                float           255                             ""
#       District                            string          255                             ""
#       Ward                                integer         255                             ""
#       Community Area                      datetime        255                             ""
#       FBI Code                            datetime        255                             ""
#       X Coordinate                        datetime        255                             ""
#       Y Coordinate                        datetime        255                             ""
#       Year                                datetime        255                             ""
#       Updated On                          datetime        255                             ""
#       Latitude                            datetime        255                             ""
#       Longitude                           datetime        255                             ""
#       Location                            datetime        255                             ""


import datetime
import logging
import math
import os
import pathlib

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
):

    logging.info(
        "chicago taxi trips process started at "
        + str(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    )

    logging.info("creating 'files' folder")
    pathlib.Path("./files").mkdir(parents=True, exist_ok=True)

    logging.info(f"Downloading file {source_url}")
    download_file(source_url, source_file)

    # open the input file
    logging.info(f"Opening file {source_file}")

    df = pd.read_csv(str(source_file))

    # steps in the pipeline
    logging.info(f"Transforming.. {source_file}")

    logging.info(f"Transform: Rename columns.. {source_file}")

    rename_headers(df)

    logging.info(f"Transform: filtering null values.. {source_file}")

    filter_null_rows(df)

    logging.info("Transform: Converting to datetime format.. ")

    convert_values(df)

    logging.info("Transform: Converting to integr.. ")

    df["trip_seconds"]           =  df["trip_seconds"].apply(convert_to_int)
    df["pickup_census_tract"]    =  df["pickup_census_tract"].apply(convert_to_int)
    df["dropoff_census_tract"]   =  df["dropoff_census_tract"].apply(convert_to_int)
    df["pickup_community_area"]  =  df["pickup_community_area"].apply(convert_to_int)
    df["dropoff_community_area"] =  df["dropoff_community_area"].apply(convert_to_int)


    # logging.info(
    #     f"Transform: Reordering headers.. "
    # )

    # df = df[
    #     [
            
    #     ]
    # ]
    
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
        "chicago  process completed at "
        + str(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    )


def rename_headers(df) : 
    header_names = {
        'Trip ID' : 'unique_key' ,
        'Taxi ID' : 'taxi_id' ,
        'Trip Start Timestamp' : 'trip_start_timestamp' ,
        'Trip End Timestamp' : 'trip_end_timestamp' ,
        'Trip Seconds' : 'trip_seconds' ,
        'Trip Miles' : 'trip_miles' ,
        'Pickup Census Tract' : 'pickup_census_tract' ,
        'Dropoff Census Tract' : 'dropoff_census_tract' ,
        'Pickup Community Area' : 'pickup_community_area' ,
        'Dropoff Community Area' : 'dropoff_community_area' ,
        'Fare' : 'fare' ,
        'Tips' : 'tips' ,
        'Tolls' : 'tolls' ,
        'Extras' : 'extras' ,
        'Trip Total' : 'trip_total' ,
        'Payment Type' : 'payment_type' ,
        'Company' : 'company' ,
        'Pickup Centroid Latitude' : 'pickup_latitude' ,
        'Pickup Centroid Longitude' : 'pickup_longitude' ,
        'Pickup Centroid Location' : 'pickup_location' ,
        'Dropoff Centroid Latitude' : 'dropoff_latitude' ,
        'Dropoff Centroid Longitude' : 'dropoff_longitude' ,
        'Dropoff Centroid  Location' : 'dropoff_location' 
    }

    df.rename(columns=header_names,inplace=True)


def filter_null_rows(df):
    df = df.query('unique_key != "" | taxi_id !="" ')

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


def convert_dt_format(dt_str):
    # Old format: MM/dd/yyyy hh:mm:ss aa
    # New format: yyyy-MM-dd HH:mm:ss
    if dt_str is None or len(dt_str) == 0:
        return dt_str
    else:
        return datetime.datetime.strptime(dt_str, "%m/%d/%Y %H:%M:%S %p").strftime(
            "%Y-%m-%d %H:%M:%S"
        )

def convert_values(df):
    dt_cols = [
        "trip_end_timestamp" , 
        "trip_start_timestamp"
    ]

    for dt_col in dt_cols:
        df[dt_col] = df[dt_col].apply(convert_dt_format)

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
    )
