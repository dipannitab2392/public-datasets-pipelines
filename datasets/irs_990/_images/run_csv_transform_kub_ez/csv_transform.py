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
):

    logging.info(
        "irs 990 ez_2015 process started at "
        + str(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    )

    logging.info("creating 'files' folder")
    pathlib.Path("./files").mkdir(parents=True, exist_ok=True)

    logging.info(f"Downloading file {source_url}")
    download_file(source_url, source_file)

    # open the input file
    logging.info(f"Opening file {source_file}")

    if os.path.basename(urlparse(source_url).path) == "14eofinextract990ez.zip":
        df = pd.read_csv(
            str(source_file), compression="zip", encoding="utf-8", sep="\s+"
        )
    else:
        df = pd.read_csv(str(source_file), encoding="utf-8", sep="\s+")

    # steps in the pipeline
    logging.info(f"Transforming.. {source_file}")

    logging.info(f"Transform: Rename columns.. {source_file}")

    rename_headers(df)

    logging.info(f"Transform: filtering null values.. {source_file}")

    filter_null_rows(df)

    logging.info("Transform: Reordering headers..")

    if os.path.basename(urlparse(source_url).path) == "14eofinextract990ez.zip":
        df = df[
            [
                "ein",
                "tax_pd",
                "subseccd",
                "totcntrbs",
                "prgmservrev",
                "duesassesmnts",
                "othrinvstinc",
                "grsamtsalesastothr",
                "basisalesexpnsothr",
                "gnsaleofastothr",
                "grsincgaming",
                "grsrevnuefndrsng",
                "direxpns",
                "netincfndrsng",
                "grsalesminusret",
                "costgoodsold",
                "grsprft",
                "othrevnue",
                "totrevnue",
                "totexpns",
                "totexcessyr",
                "othrchgsnetassetfnd",
                "networthend",
                "totassetsend",
                "totliabend",
                "totnetassetsend",
                "actvtynotprevrptcd",
                "chngsinorgcd",
                "unrelbusincd",
                "filedf990tcd",
                "contractioncd",
                "politicalexpend",
                "filedf1120polcd",
                "loanstoofficerscd",
                "loanstoofficers",
                "initiationfee",
                "grspublicrcpts",
                "s4958excessbenefcd",
                "prohibtdtxshltrcd",
                "nonpfrea",
                "totnooforgscnt",
                "totsupport",
                "gftgrntsrcvd170",
                "txrevnuelevied170",
                "srvcsval170",
                "pubsuppsubtot170",
                "exceeds2pct170",
                "pubsupplesspct170",
                "samepubsuppsubtot170",
                "grsinc170",
                "netincunreltd170",
                "othrinc170",
                "totsupp170",
                "grsrcptsrelated170",
                "totgftgrntrcvd509",
                "grsrcptsadmissn509",
                "grsrcptsactivities509",
                "txrevnuelevied509",
                "srvcsval509",
                "pubsuppsubtot509",
                "rcvdfrmdisqualsub509",
                "exceeds1pct509",
                "subtotpub509",
                "pubsupplesub509",
                "samepubsuppsubtot509",
                "grsinc509",
                "unreltxincls511tx509",
                "subtotsuppinc509",
                "netincunrelatd509",
                "othrinc509",
                "totsupp509",
            ]
        ]
    else:
        df = df[
            [
                "ein",
                "elf",
                "tax_pd",
                "subseccd",
                "totcntrbs",
                "prgmservrev",
                "duesassesmnts",
                "othrinvstinc",
                "grsamtsalesastothr",
                "basisalesexpnsothr",
                "gnsaleofastothr",
                "grsincgaming",
                "grsrevnuefndrsng",
                "direxpns",
                "netincfndrsng",
                "grsalesminusret",
                "costgoodsold",
                "grsprft",
                "othrevnue",
                "totrevnue",
                "totexpns",
                "totexcessyr",
                "othrchgsnetassetfnd",
                "networthend",
                "totassetsend",
                "totliabend",
                "totnetassetsend",
                "actvtynotprevrptcd",
                "chngsinorgcd",
                "unrelbusincd",
                "filedf990tcd",
                "contractioncd",
                "politicalexpend",
                "filedf1120polcd",
                "loanstoofficerscd",
                "loanstoofficers",
                "initiationfee",
                "grspublicrcpts",
                "s4958excessbenefcd",
                "prohibtdtxshltrcd",
                "nonpfrea",
                "totnooforgscnt",
                "totsupport",
                "gftgrntsrcvd170",
                "txrevnuelevied170",
                "srvcsval170",
                "pubsuppsubtot170",
                "exceeds2pct170",
                "pubsupplesspct170",
                "samepubsuppsubtot170",
                "grsinc170",
                "netincunreltd170",
                "othrinc170",
                "totsupp170",
                "grsrcptsrelated170",
                "totgftgrntrcvd509",
                "grsrcptsadmissn509",
                "grsrcptsactivities509",
                "txrevnuelevied509",
                "srvcsval509",
                "pubsuppsubtot509",
                "rcvdfrmdisqualsub509",
                "exceeds1pct509",
                "subtotpub509",
                "pubsupplesub509",
                "samepubsuppsubtot509",
                "grsinc509",
                "unreltxincls511tx509",
                "subtotsuppinc509",
                "netincunrelatd509",
                "othrinc509",
                "totsupp509",
            ]
        ]

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
        "irs 990 ez_2015 process completed at "
        + str(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    )


def rename_headers(df):

    header_names = {
        "EIN": "ein",
        "a_tax_prd": "tax_pd",
        "taxpd": "tax_pd",
        "taxprd": "tax_pd",
        "subseccd": "subseccd",
        "prgmservrev": "prgmservrev",
        "duesassesmnts": "duesassesmnts",
        "othrinvstinc": "othrinvstinc",
        "grsamtsalesastothr": "grsamtsalesastothr",
        "basisalesexpnsothr": "basisalesexpnsothr",
        "gnsaleofastothr": "gnsaleofastothr",
        "grsincgaming": "grsincgaming",
        "grsrevnuefndrsng": "grsrevnuefndrsng",
        "direxpns": "direxpns",
        "netincfndrsng": "netincfndrsng",
        "grsalesminusret": "grsalesminusret",
        "costgoodsold": "costgoodsold",
        "grsprft": "grsprft",
        "othrevnue": "othrevnue",
        "totrevnue": "totrevnue",
        "totexpns": "totexpns",
        "totexcessyr": "totexcessyr",
        "othrchgsnetassetfnd": "othrchgsnetassetfnd",
        "networthend": "networthend",
        "totassetsend": "totassetsend",
        "totliabend": "totliabend",
        "totnetassetsend": "totnetassetsend",
        "actvtynotprevrptcd": "actvtynotprevrptcd",
        "chngsinorgcd": "chngsinorgcd",
        "unrelbusincd": "unrelbusincd",
        "filedf990tcd": "filedf990tcd",
        "contractioncd": "contractioncd",
        "politicalexpend": "politicalexpend",
        "filedfYYN0polcd": "filedf1120polcd",
        "loanstoofficerscd": "loanstoofficerscd",
        "loanstoofficers": "loanstoofficers",
        "initiationfee": "initiationfee",
        "grspublicrcpts": "grspublicrcpts",
        "s4958excessbenefcd": "s4958excessbenefcd",
        "prohibtdtxshltrcd": "prohibtdtxshltrcd",
        "nonpfrea": "nonpfrea",
        "totnoforgscnt": "totnooforgscnt",
        "totsupport": "totsupport",
        "gftgrntrcvd170": "gftgrntsrcvd170",
        "txrevnuelevied170": "txrevnuelevied170",
        "srvcsval170": "srvcsval170",
        "pubsuppsubtot170": "pubsuppsubtot170",
        "excds2pct170": "exceeds2pct170",
        "pubsupplesspct170": "pubsupplesspct170",
        "samepubsuppsubtot170": "samepubsuppsubtot170",
        "grsinc170": "grsinc170",
        "netincunrelatd170": "netincunreltd170",
        "othrinc170": "othrinc170",
        "totsupport170": "totsupp170",
        "grsrcptsrelatd170": "grsrcptsrelated170",
        "totgftgrntrcvd509": "totgftgrntrcvd509",
        "grsrcptsadmiss509": "grsrcptsadmissn509",
        "grsrcptsactvts509": "grsrcptsactivities509",
        "txrevnuelevied509": "txrevnuelevied509",
        "srvcsval509": "srvcsval509",
        "pubsuppsubtot509": "pubsuppsubtot509",
        "rcvdfrmdisqualsub509": "rcvdfrmdisqualsub509",
        "excds1pct509": "exceeds1pct509",
        "subtotpub509": "subtotpub509",
        "pubsupplesssub509": "pubsupplesub509",
        "samepubsuppsubtot509": "samepubsuppsubtot509",
        "grsinc509": "grsinc509",
        "unreltxincls511tx509": "unreltxincls511tx509",
        "subtotsuppinc509": "subtotsuppinc509",
        "netincunreltd509": "netincunrelatd509",
        "othrinc509": "othrinc509",
        "totsupp509": "totsupp509",
        "elf": "elf",
        "totcntrbs": "totcntrbs",
    }

    df = df.rename(columns=header_names, inplace=True)


def filter_null_rows(df):
    df = df[df.ein != ""]


def save_to_new_file(df, file_path):
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
