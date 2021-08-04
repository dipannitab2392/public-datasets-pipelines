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
        "irs 990 pf process started at "
        + str(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    )

    logging.info("creating 'files' folder")
    pathlib.Path("./files").mkdir(parents=True, exist_ok=True)

    logging.info(f"Downloading file {source_url}")
    download_file(source_url, source_file)

    # open the input file
    logging.info(f"Opening file {source_file}")
    
    df = pd.read_csv(str(source_file),encoding='utf-8', sep='\s+')

    # steps in the pipeline
    logging.info(f"Transforming.. {source_file}")

    logging.info(f"Transform: Rename columns.. {source_file}")
    
    rename_headers(df)

    logging.info(f"Transform: filtering null values.. {source_file}")

    filter_null_rows(df)

    logging.info("Transform: Reordering headers..")
    df = df [
            [
                'ein',
                'elf',
                'tax_prd',
                'eostatus',
                'tax_yr',
                'operatingcd',
                'subcd',
                'fairmrktvalamt',
                'grscontrgifts',
                'schedbind',
                'intrstrvnue',
                'dividndsamt',
                'grsrents',
                'grsslspramt',
                'costsold',
                'grsprofitbus',
                'otherincamt',
                'totrcptperbks',
                'compofficers',
                'pensplemplbenf',
                'legalfeesamt',
                'accountingfees',
                'interestamt',
                'depreciationamt',
                'occupancyamt',
                'travlconfmtngs',
                'printingpubl',
                'topradmnexpnsa',
                'contrpdpbks',
                'totexpnspbks',
                'excessrcpts',
                'totrcptnetinc',
                'topradmnexpnsb',
                'totexpnsnetinc',
                'netinvstinc',
                'trcptadjnetinc',
                'totexpnsadjnet',
                'adjnetinc',
                'topradmnexpnsd',
                'totexpnsexempt',
                'othrcashamt',
                'invstgovtoblig',
                'invstcorpstk',
                'invstcorpbnd',
                'totinvstsec',
                'mrtgloans',
                'othrinvstend',
                'othrassetseoy',
                'totassetsend',
                'mrtgnotespay',
                'othrliabltseoy',
                'totliabend',
                'tfundnworth',
                'fairmrktvaleoy',
                'totexcapgnls',
                'totexcapgn',
                'totexcapls',
                'invstexcisetx',
                'sec4940notxcd',
                'sec4940redtxcd',
                'sect511tx',
                'subtitleatx',
                'totaxpyr',
                'esttaxcr',
                'txwithldsrc',
                'txpaidf2758',
                'erronbkupwthld',
                'estpnlty',
                'taxdue',
                'overpay',
                'crelamt',
                'infleg',
                'actnotpr',
                'chgnprvrptcd',
                'filedf990tcd',
                'contractncd',
                'furnishcpycd',
                'claimstatcd',
                'cntrbtrstxyrcd',
                'distribdafcd',
                'orgcmplypubcd',
                'filedlf1041ind',
                'propexchcd',
                'brwlndmnycd',
                'furngoodscd',
                'paidcmpncd',
                'transfercd',
                'agremkpaycd',
                'exceptactsind',
                'prioractvcd',
                'undistrinccd',
                'applyprovind',
                'dirindirintcd',
                'excesshldcd',
                'invstjexmptcd',
                'prevjexmptcd',
                'propgndacd',
                'ipubelectcd',
                'grntindivcd',
                'nchrtygrntcd',
                'nreligiouscd',
                'excptransind',
                'rfprsnlbnftind',
                'pyprsnlbnftind',
                'tfairmrktunuse',
                'valncharitassets',
                'cmpmininvstret',
                'distribamt',
                'undistribincyr',
                'adjnetinccola',
                'adjnetinccolb',
                'adjnetinccolc',
                'adjnetinccold',
                'adjnetinctot',
                'qlfydistriba',
                'qlfydistribb',
                'qlfydistribc',
                'qlfydistribd',
                'qlfydistribtot',
                'valassetscola',
                'valassetscolb',
                'valassetscolc',
                'valassetscold',
                'valassetstot',
                'qlfyasseta',
                'qlfyassetb',
                'qlfyassetc',
                'qlfyassetd',
                'qlfyassettot',
                'endwmntscola',
                'endwmntscolb',
                'endwmntscolc',
                'endwmntscold',
                'endwmntstot',
                'totsuprtcola',
                'totsuprtcolb',
                'totsuprtcolc',
                'totsuprtcold',
                'totsuprttot',
                'pubsuprtcola',
                'pubsuprtcolb',
                'pubsuprtcolc',
                'pubsuprtcold',
                'pubsuprttot',
                'grsinvstinca',
                'grsinvstincb',
                'grsinvstincc',
                'grsinvstincd',
                'grsinvstinctot',
                'grntapprvfut',
                'progsrvcacold',
                'progsrvcacole',
                'progsrvcbcold',
                'progsrvcbcole',
                'progsrvcccold',
                'progsrvcccole',
                'progsrvcdcold',
                'progsrvcdcole',
                'progsrvcecold',
                'progsrvcecole',
                'progsrvcfcold',
                'progsrvcfcole',
                'progsrvcgcold',
                'progsrvcgcole',
                'membershpduesd',
                'membershpduese',
                'intonsvngsd',
                'intonsvngse',
                'dvdndsintd',
                'dvdndsinte',
                'trnsfrcashcd',
                'trnsothasstscd',
                'salesasstscd',
                'prchsasstscd',
                'rentlsfacltscd',
                'reimbrsmntscd',
                'loansguarcd',
                'perfservicescd',
                'sharngasstscd',
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
        "irs 990 pf process completed at "
        + str(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    )

def rename_headers(df) : 

    header_names = {
        'ELF' : 'elf' ,
        'ELFCD' : 'elf' ,
        'EIN' : 'ein' ,
        'TAX_PRD' : 'tax_prd' ,
        'EOSTATUS' : 'eostatus' ,
        'TAX_YR' : 'tax_yr' ,
        'OPERATINGCD' : 'operatingcd' ,
        'SUBCD' : 'subcd' ,
        'FAIRMRKTVALAMT' : 'fairmrktvalamt' ,
        'GRSCONTRGIFTS' : 'grscontrgifts' ,
        'SCHEDBIND' : 'schedbind' ,
        'INTRSTRVNUE' : 'intrstrvnue' ,
        'DIVIDNDSAMT' : 'dividndsamt' ,
        'GRSRENTS' : 'grsrents' ,
        'GRSSLSPRAMT' : 'grsslspramt' ,
        'COSTSOLD' : 'costsold' ,
        'GRSPROFITBUS' : 'grsprofitbus' ,
        'OTHERINCAMT' : 'otherincamt' ,
        'TOTRCPTPERBKS' : 'totrcptperbks' ,
        'COMPOFFICERS' : 'compofficers' ,
        'PENSPLEMPLBENF' : 'pensplemplbenf' ,
        'LEGALFEESAMT' : 'legalfeesamt' ,
        'ACCOUNTINGFEES' : 'accountingfees' ,
        'INTERESTAMT' : 'interestamt' ,
        'DEPRECIATIONAMT' : 'depreciationamt' ,
        'OCCUPANCYAMT' : 'occupancyamt' ,
        'TRAVLCONFMTNGS' : 'travlconfmtngs' ,
        'PRINTINGPUBL' : 'printingpubl' ,
        'TOPRADMNEXPNSA' : 'topradmnexpnsa' ,
        'CONTRPDPBKS' : 'contrpdpbks' ,
        'TOTEXPNSPBKS' : 'totexpnspbks' ,
        'EXCESSRCPTS' : 'excessrcpts' ,
        'TOTRCPTNETINC' : 'totrcptnetinc' ,
        'TOPRADMNEXPNSB' : 'topradmnexpnsb' ,
        'TOTEXPNSNETINC' : 'totexpnsnetinc' ,
        'NETINVSTINC' : 'netinvstinc' ,
        'TRCPTADJNETINC' : 'trcptadjnetinc' ,
        'TOTEXPNSADJNET' : 'totexpnsadjnet' ,
        'ADJNETINC' : 'adjnetinc' ,
        'TOPRADMNEXPNSD' : 'topradmnexpnsd' ,
        'TOTEXPNSEXEMPT' : 'totexpnsexempt' ,
        'OTHRCASHAMT' : 'othrcashamt' ,
        'INVSTGOVTOBLIG' : 'invstgovtoblig' ,
        'INVSTCORPSTK' : 'invstcorpstk' ,
        'INVSTCORPBND' : 'invstcorpbnd' ,
        'TOTINVSTSEC' : 'totinvstsec' ,
        'MRTGLOANS' : 'mrtgloans' ,
        'OTHRINVSTEND' : 'othrinvstend' ,
        'OTHRASSETSEOY' : 'othrassetseoy' ,
        'TOTASSETSEND' : 'totassetsend' ,
        'MRTGNOTESPAY' : 'mrtgnotespay' ,
        'OTHRLIABLTSEOY' : 'othrliabltseoy' ,
        'TOTLIABEND' : 'totliabend' ,
        'TFUNDNWORTH' : 'tfundnworth' ,
        'FAIRMRKTVALEOY' : 'fairmrktvaleoy' ,
        'TOTEXCAPGNLS' : 'totexcapgnls' ,
        'TOTEXCAPGN' : 'totexcapgn' ,
        'TOTEXCAPLS' : 'totexcapls' ,
        'INVSTEXCISETX' : 'invstexcisetx' ,
        'SEC4940NOTXCD' : 'sec4940notxcd' ,
        'SEC4940REDTXCD' : 'sec4940redtxcd' ,
        'SECT511TX' : 'sect511tx' ,
        'SUBTITLEATX' : 'subtitleatx' ,
        'TOTAXPYR' : 'totaxpyr' ,
        'ESTTAXCR' : 'esttaxcr' ,
        'TXWITHLDSRC' : 'txwithldsrc' ,
        'TXPAIDF2758' : 'txpaidf2758' ,
        'ERRONBKUPWTHLD' : 'erronbkupwthld' ,
        'ESTPNLTY' : 'estpnlty' ,
        'TAXDUE' : 'taxdue' ,
        'OVERPAY' : 'overpay' ,
        'CRELAMT' : 'crelamt' ,
        'INFLEG' : 'infleg' ,
        'ACTNOTPR' : 'actnotpr' ,
        'CHGNPRVRPTCD' : 'chgnprvrptcd' ,
        'FILEDF990TCD' : 'filedf990tcd' ,
        'CONTRACTNCD' : 'contractncd' ,
        'FURNISHCPYCD' : 'furnishcpycd' ,
        'CLAIMSTATCD' : 'claimstatcd' ,
        'CNTRBTRSTXYRCD' : 'cntrbtrstxyrcd' ,
        'DISTRIBDAFCD' : 'distribdafcd' ,
        'ACQDRINDRINTCD' : 'distribdafcd' ,
        'ORGCMPLYPUBCD' : 'orgcmplypubcd' ,
        'FILEDLF1041IND' : 'filedlf1041ind' ,
        'PROPEXCHCD' : 'propexchcd' ,
        'BRWLNDMNYCD' : 'brwlndmnycd' ,
        'FURNGOODSCD' : 'furngoodscd' ,
        'PAIDCMPNCD' : 'paidcmpncd' ,
        'TRANSFERCD' : 'transfercd' ,
        'AGREMKPAYCD' : 'agremkpaycd' ,
        'EXCEPTACTSIND' : 'exceptactsind' ,
        'PRIORACTVCD' : 'prioractvcd' ,
        'UNDISTRINCCD' : 'undistrinccd' ,
        'APPLYPROVIND' : 'applyprovind' ,
        'DIRINDIRINTCD' : 'dirindirintcd' ,
        'EXCESSHLDCD' : 'excesshldcd' ,
        'INVSTJEXMPTCD' : 'invstjexmptcd' ,
        'PREVJEXMPTCD' : 'prevjexmptcd' ,
        'PROPGNDACD' : 'propgndacd' ,
        'IPUBELECTCD' : 'ipubelectcd' ,
        'GRNTINDIVCD' : 'grntindivcd' ,
        'NCHRTYGRNTCD' : 'nchrtygrntcd' ,
        'NRELIGIOUSCD' : 'nreligiouscd' ,
        'EXCPTRANSIND' : 'excptransind' ,
        'RFPRSNLBNFTIND' : 'rfprsnlbnftind' ,
        'PYPRSNLBNFTIND' : 'pyprsnlbnftind' ,
        'TFAIRMRKTUNUSE' : 'tfairmrktunuse' ,
        'VALNCHARITASSETS' : 'valncharitassets' ,
        'CMPMININVSTRET' : 'cmpmininvstret' ,
        'DISTRIBAMT' : 'distribamt' ,
        'UNDISTRIBINCYR' : 'undistribincyr' ,
        'ADJNETINCCOLA' : 'adjnetinccola' ,
        'ADJNETINCCOLB' : 'adjnetinccolb' ,
        'ADJNETINCCOLC' : 'adjnetinccolc' ,
        'ADJNETINCCOLD' : 'adjnetinccold' ,
        'ADJNETINCTOT' : 'adjnetinctot' ,
        'QLFYDISTRIBA' : 'qlfydistriba' ,
        'QLFYDISTRIBB' : 'qlfydistribb' ,
        'QLFYDISTRIBC' : 'qlfydistribc' ,
        'QLFYDISTRIBD' : 'qlfydistribd' ,
        'QLFYDISTRIBTOT' : 'qlfydistribtot' ,
        'VALASSETSCOLA' : 'valassetscola' ,
        'VALASSETSCOLB' : 'valassetscolb' ,
        'VALASSETSCOLC' : 'valassetscolc' ,
        'VALASSETSCOLD' : 'valassetscold' ,
        'VALASSETSTOT' : 'valassetstot' ,
        'QLFYASSETA' : 'qlfyasseta' ,
        'QLFYASSETB' : 'qlfyassetb' ,
        'QLFYASSETC' : 'qlfyassetc' ,
        'QLFYASSETD' : 'qlfyassetd' ,
        'QLFYASSETTOT' : 'qlfyassettot' ,
        'ENDWMNTSCOLA' : 'endwmntscola' ,
        'ENDWMNTSCOLB' : 'endwmntscolb' ,
        'ENDWMNTSCOLC' : 'endwmntscolc' ,
        'ENDWMNTSCOLD' : 'endwmntscold' ,
        'ENDWMNTSTOT' : 'endwmntstot' ,
        'TOTSUPRTCOLA' : 'totsuprtcola' ,
        'TOTSUPRTCOLB' : 'totsuprtcolb' ,
        'TOTSUPRTCOLC' : 'totsuprtcolc' ,
        'TOTSUPRTCOLD' : 'totsuprtcold' ,
        'TOTSUPRTTOT' : 'totsuprttot' ,
        'PUBSUPRTCOLA' : 'pubsuprtcola' ,
        'PUBSUPRTCOLB' : 'pubsuprtcolb' ,
        'PUBSUPRTCOLC' : 'pubsuprtcolc' ,
        'PUBSUPRTCOLD' : 'pubsuprtcold' ,
        'PUBSUPRTTOT' : 'pubsuprttot' ,
        'GRSINVSTINCA' : 'grsinvstinca' ,
        'GRSINVSTINCB' : 'grsinvstincb' ,
        'GRSINVSTINCC' : 'grsinvstincc' ,
        'GRSINVSTINCD' : 'grsinvstincd' ,
        'GRSINVSTINCTOT' : 'grsinvstinctot' ,
        'GRNTAPPRVFUT' : 'grntapprvfut' ,
        'PROGSRVCACOLD' : 'progsrvcacold' ,
        'PROGSRVCACOLE' : 'progsrvcacole' ,
        'PROGSRVCBCOLD' : 'progsrvcbcold' ,
        'PROGSRVCBCOLE' : 'progsrvcbcole' ,
        'PROGSRVCCCOLD' : 'progsrvcccold' ,
        'PROGSRVCCCOLE' : 'progsrvcccole' ,
        'PROGSRVCDCOLD' : 'progsrvcdcold' ,
        'PROGSRVCDCOLE' : 'progsrvcdcole' ,
        'PROGSRVCECOLD' : 'progsrvcecold' ,
        'PROGSRVCECOLE' : 'progsrvcecole' ,
        'PROGSRVCFCOLD' : 'progsrvcfcold' ,
        'PROGSRVCFCOLE' : 'progsrvcfcole' ,
        'PROGSRVCGCOLD' : 'progsrvcgcold' ,
        'PROGSRVCGCOLE' : 'progsrvcgcole' ,
        'MEMBERSHPDUESD' : 'membershpduesd' ,
        'MEMBERSHPDUESE' : 'membershpduese' ,
        'INTONSVNGSD' : 'intonsvngsd' ,
        'INTONSVNGSE' : 'intonsvngse' ,
        'DVDNDSINTD' : 'dvdndsintd' ,
        'DVDNDSINTE' : 'dvdndsinte' ,
        'TRNSFRCASHCD' : 'trnsfrcashcd' ,
        'TRNSOTHASSTSCD' : 'trnsothasstscd' ,
        'SALESASSTSCD' : 'salesasstscd' ,
        'PRCHSASSTSCD' : 'prchsasstscd' ,
        'RENTLSFACLTSCD' : 'rentlsfacltscd' ,
        'REIMBRSMNTSCD' : 'reimbrsmntscd' ,
        'LOANSGUARCD' : 'loansguarcd' ,
        'PERFSERVICESCD' : 'perfservicescd' ,
        'SHARNGASSTSCD' : 'sharngasstscd' 
    }
    

    df = df.rename(columns=header_names,inplace=True)

def filter_null_rows(df):
    df = df[df.ein != ""]


def save_to_new_file(df, file_path):
    df.to_csv(file_path,index=False)


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
