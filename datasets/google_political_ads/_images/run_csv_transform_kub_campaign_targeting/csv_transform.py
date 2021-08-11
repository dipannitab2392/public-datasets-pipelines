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
import os
import pathlib
from zipfile import ZipFile, Path
import fnmatch

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
    df = read_csv_file(source_file,source_csv_name)

    logging.info(f"Transforming.. {source_file}")

    logging.info(f"Transform: Rename columns.. {source_file}")
    rename_headers(df)

    logging.info("Transform: Reordering headers.. ")
    df = df[
        [
           'campaign_id', 
           'age_targeting', 
           'gender_targeting', 
           'geo_targeting_included', 
           'geo_targeting_excluded', 
           'start_date', 
           'end_date', 
           'ads_list', 
           'advertiser_id', 
           'advertiser_name'      
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

def read_csv_file (source_file,source_csv_name) : 
    with ZipFile(source_file) as zipfiles:
        file_list = zipfiles.namelist()
        csv_files = fnmatch.filter(file_list, source_csv_name)
        data = [pd.read_csv(zipfiles.open(file_name)) for file_name in csv_files]
    
    df = pd.concat(data)
    return df

def rename_headers(df) : 
    header_names = {
        'Campaign_ID' : 'campaign_id' ,
        'Age_Targeting' : 'age_targeting' ,
        'Gender_Targeting' : 'gender_targeting' ,
        'Geo_Targeting_Included' : 'geo_targeting_included' ,
        'Geo_Targeting_Excluded' : 'geo_targeting_excluded' ,
        'Start_Date' : 'start_date' ,
        'End_Date' : 'end_date' ,
        'Ads_List' : 'ads_list' ,
        'Advertiser_ID' : 'advertiser_id' ,
        'Advertiser_Name' : 'advertiser_name' 
        
    }
    df.rename(columns=header_names,inplace=True)


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)

    main(
        source_url=os.environ["SOURCE_URL"],
        source_file=pathlib.Path(os.environ["SOURCE_FILE"]).expanduser(),
        source_csv_name = os.environ["FILE_NAME"],
        target_file=pathlib.Path(os.environ["TARGET_FILE"]).expanduser(),
        target_gcs_bucket=os.environ["TARGET_GCS_BUCKET"],
        target_gcs_path=os.environ["TARGET_GCS_PATH"],
    )