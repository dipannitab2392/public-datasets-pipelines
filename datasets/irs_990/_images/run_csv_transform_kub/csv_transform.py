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
        "irs 990 process started at "
        + str(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    )

    logging.info("creating 'files' folder")
    pathlib.Path("./files").mkdir(parents=True, exist_ok=True)

    logging.info(f"Downloading file {source_url}")
    download_file(source_url, source_file)

    # open the input file
    logging.info(f"Opening file {source_file}")

    if os.path.basename(urlparse(source_url).path) == "14eofinextract990.zip":
        df = pd.read_csv(
            str(source_file), compression="zip", encoding="utf-8", sep="\s+"
        )
    else:
        df = pd.read_csv(str(source_file), encoding="utf-8", sep="\s+")

    # steps in the pipeline
    logging.info(f"Transforming.. {source_file}")

    logging.info(f"Transform: Rename columns.. {source_file}")
    header_names = {
        "elf": "elf",
        "EIN": "ein",
        "tax_prd": "tax_pd",
        "subseccd": "subseccd",
        "s50Yc3or4947aYcd": "s501c3or4947a1cd",
        "schdbind": "schdbind",
        "politicalactvtscd": "politicalactvtscd",
        "lbbyingactvtscd": "lbbyingactvtscd",
        "subjto6033cd": "subjto6033cd",
        "dnradvisedfundscd": "dnradvisedfundscd",
        "prptyintrcvdcd": "prptyintrcvdcd",
        "maintwrkofartcd": "maintwrkofartcd",
        "crcounselingqstncd": "crcounselingqstncd",
        "hldassetsintermpermcd": "hldassetsintermpermcd",
        "rptlndbldgeqptcd": "rptlndbldgeqptcd",
        "rptinvstothsecd": "rptinvstothsecd",
        "rptinvstprgrelcd": "rptinvstprgrelcd",
        "rptothasstcd": "rptothasstcd",
        "rptothliabcd": "rptothliabcd",
        "sepcnsldtfinstmtcd": "sepcnsldtfinstmtcd",
        "sepindaudfinstmtcd": "sepindaudfinstmtcd",
        "inclinfinstmtcd": "inclinfinstmtcd",
        "operateschoolsY70cd": "operateschools170cd",
        "frgnofficecd": "frgnofficecd",
        "frgnrevexpnscd": "frgnrevexpnscd",
        "frgngrntscd": "frgngrntscd",
        "frgnaggragrntscd": "frgnaggragrntscd",
        "rptprofndrsngfeescd": "rptprofndrsngfeescd",
        "rptincfnndrsngcd": "rptincfnndrsngcd",
        "rptincgamingcd": "rptincgamingcd",
        "operatehosptlcd": "operatehosptlcd",
        "hospaudfinstmtcd": "hospaudfinstmtcd",
        "rptgrntstogovtcd": "rptgrntstogovtcd",
        "rptgrntstoindvcd": "rptgrntstoindvcd",
        "rptyestocompnstncd": "rptyestocompnstncd",
        "txexmptbndcd": "txexmptbndcd",
        "invstproceedscd": "invstproceedscd",
        "maintescrwaccntcd": "maintescrwaccntcd",
        "actonbehalfcd": "actonbehalfcd",
        "engageexcessbnftcd": "engageexcessbnftcd",
        "awarexcessbnftcd": "awarexcessbnftcd",
        "loantofficercd": "loantofficercd",
        "grantoofficercd": "grantoofficercd",
        "dirbusnreltdcd": "dirbusnreltdcd",
        "fmlybusnreltdcd": "fmlybusnreltdcd",
        "servasofficercd": "servasofficercd",
        "recvnoncashcd": "recvnoncashcd",
        "recvartcd": "recvartcd",
        "ceaseoperationscd": "ceaseoperationscd",
        "sellorexchcd": "sellorexchcd",
        "ownsepentcd": "ownsepentcd",
        "reltdorgcd": "reltdorgcd",
        "intincntrlcd": "intincntrlcd",
        "orgtrnsfrcd": "orgtrnsfrcd",
        "conduct5percentcd": "conduct5percentcd",
        "compltschocd": "compltschocd",
        "f1096cnt": "f1096cnt",
        "fw2gcnt": "fw2gcnt",
        "wthldngrulescd": "wthldngrulescd",
        "noemplyeesw3cnt": "noemplyeesw3cnt",
        "filerqrdrtnscd": "filerqrdrtnscd",
        "unrelbusinccd": "unrelbusinccd",
        "filedf990tcd": "filedf990tcd",
        "frgnacctcd": "frgnacctcd",
        "prohibtdtxshltrcd": "prohibtdtxshltrcd",
        "prtynotifyorgcd": "prtynotifyorgcd",
        "filedf8886tcd": "filedf8886tcd",
        "solicitcntrbcd": "solicitcntrbcd",
        "exprstmntcd": "exprstmntcd",
        "providegoodscd": "providegoodscd",
        "notfydnrvalcd": "notfydnrvalcd",
        "filedf8N8Ncd": "filedf8282cd",
        "f8282cnt": "f8282cnt",
        "fndsrcvdcd": "fndsrcvdcd",
        "premiumspaidcd": "premiumspaidcd",
        "filedf8899cd": "filedf8899cd",
        "filedfY098ccd": "filedf1098ccd",
        "excbushldngscd": "excbushldngscd",
        "s4966distribcd": "s4966distribcd",
        "distribtodonorcd": "distribtodonorcd",
        "initiationfees": "initiationfees",
        "grsrcptspublicuse": "grsrcptspublicuse",
        "grsincmembers": "grsincmembers",
        "grsincother": "grsincother",
        "filedlieufY04Ycd": "filedlieuf1041cd",
        "txexmptint": "txexmptint",
        "qualhlthplncd": "qualhlthplncd",
        "qualhlthreqmntn": "qualhlthreqmntn",
        "qualhlthonhnd": "qualhlthonhnd",
        "rcvdpdtngcd": "rcvdpdtngcd",
        "filedf7N0cd": "filedf720cd",
        "totreprtabled": "totreprtabled",
        "totcomprelatede": "totcomprelatede",
        "totestcompf": "totestcompf",
        "noindiv100kcnt": "noindiv100kcnt",
        "nocontractor100kcnt": "nocontractor100kcnt",
        "totcntrbgfts": "totcntrbgfts",
        "prgmservcode2acd": "prgmservcode2acd",
        "totrev2acola": "totrev2acola",
        "prgmservcode2bcd": "prgmservcode2bcd",
        "totrev2bcola": "totrev2bcola",
        "prgmservcode2ccd": "prgmservcode2ccd",
        "totrev2ccola": "totrev2ccola",
        "prgmservcode2dcd": "prgmservcode2dcd",
        "totrev2dcola": "totrev2dcola",
        "prgmservcode2ecd": "prgmservcode2ecd",
        "totrev2ecola": "totrev2ecola",
        "totrev2fcola": "totrev2fcola",
        "totprgmrevnue": "totprgmrevnue",
        "invstmntinc": "invstmntinc",
        "txexmptbndsproceeds": "txexmptbndsproceeds",
        "royaltsinc": "royaltsinc",
        "grsrntsreal": "grsrntsreal",
        "grsrntsprsnl": "grsrntsprsnl",
        "rntlexpnsreal": "rntlexpnsreal",
        "rntlexpnsprsnl": "rntlexpnsprsnl",
        "rntlincreal": "rntlincreal",
        "rntlincprsnl": "rntlincprsnl",
        "netrntlinc": "netrntlinc",
        "grsalesecur": "grsalesecur",
        "grsalesothr": "grsalesothr",
        "cstbasisecur": "cstbasisecur",
        "cstbasisothr": "cstbasisothr",
        "gnlsecur": "gnlsecur",
        "gnlsothr": "gnlsothr",
        "netgnls": "netgnls",
        "grsincfndrsng": "grsincfndrsng",
        "lessdirfndrsng": "lessdirfndrsng",
        "netincfndrsng": "netincfndrsng",
        "grsincgaming": "grsincgaming",
        "lessdirgaming": "lessdirgaming",
        "netincgaming": "netincgaming",
        "grsalesinvent": "grsalesinvent",
        "lesscstofgoods": "lesscstofgoods",
        "netincsales": "netincsales",
        "miscrev11acd": "miscrev11acd",
        "miscrevtota": "miscrevtota",
        "miscrev11bcd": "miscrev11bcd",
        "miscrevtot11b": "miscrevtot11b",
        "miscrev11ccd": "miscrev11ccd",
        "miscrevtot11c": "miscrevtot11c",
        "miscrevtot11d": "miscrevtot11d",
        "miscrevtot11e": "miscrevtot11e",
        "totrevenue": "totrevenue",
        "grntstogovt": "grntstogovt",
        "grnsttoindiv": "grnsttoindiv",
        "grntstofrgngovt": "grntstofrgngovt",
        "benifitsmembrs": "benifitsmembrs",
        "compnsatncurrofcr": "compnsatncurrofcr",
        "compnsatnandothr": "compnsatnandothr",
        "othrsalwages": "othrsalwages",
        "pensionplancontrb": "pensionplancontrb",
        "othremplyeebenef": "othremplyeebenef",
        "payrolltx": "payrolltx",
        "feesforsrvcmgmt": "feesforsrvcmgmt",
        "legalfees": "legalfees",
        "accntingfees": "accntingfees",
        "feesforsrvclobby": "feesforsrvclobby",
        "profndraising": "profndraising",
        "feesforsrvcinvstmgmt": "feesforsrvcinvstmgmt",
        "feesforsrvcothr": "feesforsrvcothr",
        "advrtpromo": "advrtpromo",
        "officexpns": "officexpns",
        "infotech": "infotech",
        "royaltsexpns": "royaltsexpns",
        "occupancy": "occupancy",
        "travel": "travel",
        "travelofpublicoffcl": "travelofpublicoffcl",
        "converconventmtng": "converconventmtng",
        "interestamt": "interestamt",
        "pymtoaffiliates": "pymtoaffiliates",
        "deprcatndepletn": "deprcatndepletn",
        "insurance": "insurance",
        "othrexpnsa": "othrexpnsa",
        "othrexpnsb": "othrexpnsb",
        "othrexpnsc": "othrexpnsc",
        "othrexpnsd": "othrexpnsd",
        "othrexpnse": "othrexpnse",
        "othrexpnsf": "othrexpnsf",
        "totfuncexpns": "totfuncexpns",
        "nonintcashend": "nonintcashend",
        "svngstempinvend": "svngstempinvend",
        "pldgegrntrcvblend": "pldgegrntrcvblend",
        "accntsrcvblend": "accntsrcvblend",
        "currfrmrcvblend": "currfrmrcvblend",
        "rcvbldisqualend": "rcvbldisqualend",
        "notesloansrcvblend": "notesloansrcvblend",
        "invntriesalesend": "invntriesalesend",
        "prepaidexpnsend": "prepaidexpnsend",
        "lndbldgsequipend": "lndbldgsequipend",
        "invstmntsend": "invstmntsend",
        "invstmntsothrend": "invstmntsothrend",
        "invstmntsprgmend": "invstmntsprgmend",
        "intangibleassetsend": "intangibleassetsend",
        "othrassetsend": "othrassetsend",
        "totassetsend": "totassetsend",
        "accntspayableend": "accntspayableend",
        "grntspayableend": "grntspayableend",
        "deferedrevnuend": "deferedrevnuend",
        "txexmptbndsend": "txexmptbndsend",
        "escrwaccntliabend": "escrwaccntliabend",
        "paybletoffcrsend": "paybletoffcrsend",
        "secrdmrtgsend": "secrdmrtgsend",
        "unsecurednotesend": "unsecurednotesend",
        "othrliabend": "othrliabend",
        "totliabend": "totliabend",
        "unrstrctnetasstsend": "unrstrctnetasstsend",
        "temprstrctnetasstsend": "temprstrctnetasstsend",
        "permrstrctnetasstsend": "permrstrctnetasstsend",
        "capitalstktrstend": "capitalstktrstend",
        "paidinsurplusend": "paidinsurplusend",
        "retainedearnend": "retainedearnend",
        "totnetassetend": "totnetassetend",
        "totnetliabastend": "totnetliabastend",
        "nonpfrea": "nonpfrea",
        "totnooforgscnt": "totnooforgscnt",
        "totsupport": "totsupport",
        "gftgrntsrcvd170": "gftgrntsrcvd170",
        "txrevnuelevied170": "txrevnuelevied170",
        "srvcsval170": "srvcsval170",
        "pubsuppsubtot170": "pubsuppsubtot170",
        "exceeds2pct170": "exceeds2pct170",
        "pubsupplesspct170": "pubsupplesspct170",
        "samepubsuppsubtot170": "samepubsuppsubtot170",
        "grsinc170": "grsinc170",
        "netincunreltd170": "netincunreltd170",
        "othrinc170": "othrinc170",
        "totsupp170": "totsupp170",
        "grsrcptsrelated170": "grsrcptsrelated170",
        "totgftgrntrcvd509": "totgftgrntrcvd509",
        "grsrcptsadmissn509": "grsrcptsadmissn509",
        "grsrcptsactivities509": "grsrcptsactivities509",
        "txrevnuelevied509": "txrevnuelevied509",
        "srvcsval509": "srvcsval509",
        "pubsuppsubtot509": "pubsuppsubtot509",
        "rcvdfrmdisqualsub509": "rcvdfrmdisqualsub509",
        "exceeds1pct509": "exceeds1pct509",
        "subtotpub509": "subtotpub509",
        "pubsupplesub509": "pubsupplesub509",
        "samepubsuppsubtot509": "samepubsuppsubtot509",
        "grsinc509": "grsinc509",
        "unreltxincls511tx509": "unreltxincls511tx509",
        "subtotsuppinc509": "subtotsuppinc509",
        "netincunrelatd509": "netincunrelatd509",
        "othrinc509": "othrinc509",
        "totsupp509": "totsupp509",
    }

    df.rename(columns=header_names, inplace=True)

    logging.info(f"Transform: filtering null values.. {source_file}")

    filter_null_rows(df)

    logging.info("Transform: Reordering headers..")

    if os.path.basename(urlparse(source_url).path) == "14eofinextract990.zip":
        df = df[
            [
                "ein",
                "tax_pd",
                "subseccd",
                "s501c3or4947a1cd",
                "schdbind",
                "politicalactvtscd",
                "lbbyingactvtscd",
                "subjto6033cd",
                "dnradvisedfundscd",
                "prptyintrcvdcd",
                "maintwrkofartcd",
                "crcounselingqstncd",
                "hldassetsintermpermcd",
                "rptlndbldgeqptcd",
                "rptinvstothsecd",
                "rptinvstprgrelcd",
                "rptothasstcd",
                "rptothliabcd",
                "sepcnsldtfinstmtcd",
                "sepindaudfinstmtcd",
                "inclinfinstmtcd",
                "operateschools170cd",
                "frgnofficecd",
                "frgnrevexpnscd",
                "frgngrntscd",
                "frgnaggragrntscd",
                "rptprofndrsngfeescd",
                "rptincfnndrsngcd",
                "rptincgamingcd",
                "operatehosptlcd",
                "hospaudfinstmtcd",
                "rptgrntstogovtcd",
                "rptgrntstoindvcd",
                "rptyestocompnstncd",
                "txexmptbndcd",
                "invstproceedscd",
                "maintescrwaccntcd",
                "actonbehalfcd",
                "engageexcessbnftcd",
                "awarexcessbnftcd",
                "loantofficercd",
                "grantoofficercd",
                "dirbusnreltdcd",
                "fmlybusnreltdcd",
                "servasofficercd",
                "recvnoncashcd",
                "recvartcd",
                "ceaseoperationscd",
                "sellorexchcd",
                "ownsepentcd",
                "reltdorgcd",
                "intincntrlcd",
                "orgtrnsfrcd",
                "conduct5percentcd",
                "compltschocd",
                "f1096cnt",
                "fw2gcnt",
                "wthldngrulescd",
                "noemplyeesw3cnt",
                "filerqrdrtnscd",
                "unrelbusinccd",
                "filedf990tcd",
                "frgnacctcd",
                "prohibtdtxshltrcd",
                "prtynotifyorgcd",
                "filedf8886tcd",
                "solicitcntrbcd",
                "exprstmntcd",
                "providegoodscd",
                "notfydnrvalcd",
                "filedf8282cd",
                "f8282cnt",
                "fndsrcvdcd",
                "premiumspaidcd",
                "filedf8899cd",
                "filedf1098ccd",
                "excbushldngscd",
                "s4966distribcd",
                "distribtodonorcd",
                "initiationfees",
                "grsrcptspublicuse",
                "grsincmembers",
                "grsincother",
                "filedlieuf1041cd",
                "txexmptint",
                "qualhlthplncd",
                "qualhlthreqmntn",
                "qualhlthonhnd",
                "rcvdpdtngcd",
                "filedf720cd",
                "totreprtabled",
                "totcomprelatede",
                "totestcompf",
                "noindiv100kcnt",
                "nocontractor100kcnt",
                "totcntrbgfts",
                "prgmservcode2acd",
                "totrev2acola",
                "prgmservcode2bcd",
                "totrev2bcola",
                "prgmservcode2ccd",
                "totrev2ccola",
                "prgmservcode2dcd",
                "totrev2dcola",
                "prgmservcode2ecd",
                "totrev2ecola",
                "totrev2fcola",
                "totprgmrevnue",
                "invstmntinc",
                "txexmptbndsproceeds",
                "royaltsinc",
                "grsrntsreal",
                "grsrntsprsnl",
                "rntlexpnsreal",
                "rntlexpnsprsnl",
                "rntlincreal",
                "rntlincprsnl",
                "netrntlinc",
                "grsalesecur",
                "grsalesothr",
                "cstbasisecur",
                "cstbasisothr",
                "gnlsecur",
                "gnlsothr",
                "netgnls",
                "grsincfndrsng",
                "lessdirfndrsng",
                "netincfndrsng",
                "grsincgaming",
                "lessdirgaming",
                "netincgaming",
                "grsalesinvent",
                "lesscstofgoods",
                "netincsales",
                "miscrev11acd",
                "miscrevtota",
                "miscrev11bcd",
                "miscrevtot11b",
                "miscrev11ccd",
                "miscrevtot11c",
                "miscrevtot11d",
                "miscrevtot11e",
                "totrevenue",
                "grntstogovt",
                "grnsttoindiv",
                "grntstofrgngovt",
                "benifitsmembrs",
                "compnsatncurrofcr",
                "compnsatnandothr",
                "othrsalwages",
                "pensionplancontrb",
                "othremplyeebenef",
                "payrolltx",
                "feesforsrvcmgmt",
                "legalfees",
                "accntingfees",
                "feesforsrvclobby",
                "profndraising",
                "feesforsrvcinvstmgmt",
                "feesforsrvcothr",
                "advrtpromo",
                "officexpns",
                "infotech",
                "royaltsexpns",
                "occupancy",
                "travel",
                "travelofpublicoffcl",
                "converconventmtng",
                "interestamt",
                "pymtoaffiliates",
                "deprcatndepletn",
                "insurance",
                "othrexpnsa",
                "othrexpnsb",
                "othrexpnsc",
                "othrexpnsd",
                "othrexpnse",
                "othrexpnsf",
                "totfuncexpns",
                "nonintcashend",
                "svngstempinvend",
                "pldgegrntrcvblend",
                "accntsrcvblend",
                "currfrmrcvblend",
                "rcvbldisqualend",
                "notesloansrcvblend",
                "invntriesalesend",
                "prepaidexpnsend",
                "lndbldgsequipend",
                "invstmntsend",
                "invstmntsothrend",
                "invstmntsprgmend",
                "intangibleassetsend",
                "othrassetsend",
                "totassetsend",
                "accntspayableend",
                "grntspayableend",
                "deferedrevnuend",
                "txexmptbndsend",
                "escrwaccntliabend",
                "paybletoffcrsend",
                "secrdmrtgsend",
                "unsecurednotesend",
                "othrliabend",
                "totliabend",
                "unrstrctnetasstsend",
                "temprstrctnetasstsend",
                "permrstrctnetasstsend",
                "capitalstktrstend",
                "paidinsurplusend",
                "retainedearnend",
                "totnetassetend",
                "totnetliabastend",
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
                "s501c3or4947a1cd",
                "schdbind",
                "politicalactvtscd",
                "lbbyingactvtscd",
                "subjto6033cd",
                "dnradvisedfundscd",
                "prptyintrcvdcd",
                "maintwrkofartcd",
                "crcounselingqstncd",
                "hldassetsintermpermcd",
                "rptlndbldgeqptcd",
                "rptinvstothsecd",
                "rptinvstprgrelcd",
                "rptothasstcd",
                "rptothliabcd",
                "sepcnsldtfinstmtcd",
                "sepindaudfinstmtcd",
                "inclinfinstmtcd",
                "operateschools170cd",
                "frgnofficecd",
                "frgnrevexpnscd",
                "frgngrntscd",
                "frgnaggragrntscd",
                "rptprofndrsngfeescd",
                "rptincfnndrsngcd",
                "rptincgamingcd",
                "operatehosptlcd",
                "hospaudfinstmtcd",
                "rptgrntstogovtcd",
                "rptgrntstoindvcd",
                "rptyestocompnstncd",
                "txexmptbndcd",
                "invstproceedscd",
                "maintescrwaccntcd",
                "actonbehalfcd",
                "engageexcessbnftcd",
                "awarexcessbnftcd",
                "loantofficercd",
                "grantoofficercd",
                "dirbusnreltdcd",
                "fmlybusnreltdcd",
                "servasofficercd",
                "recvnoncashcd",
                "recvartcd",
                "ceaseoperationscd",
                "sellorexchcd",
                "ownsepentcd",
                "reltdorgcd",
                "intincntrlcd",
                "orgtrnsfrcd",
                "conduct5percentcd",
                "compltschocd",
                "f1096cnt",
                "fw2gcnt",
                "wthldngrulescd",
                "noemplyeesw3cnt",
                "filerqrdrtnscd",
                "unrelbusinccd",
                "filedf990tcd",
                "frgnacctcd",
                "prohibtdtxshltrcd",
                "prtynotifyorgcd",
                "filedf8886tcd",
                "solicitcntrbcd",
                "exprstmntcd",
                "providegoodscd",
                "notfydnrvalcd",
                "filedf8282cd",
                "f8282cnt",
                "fndsrcvdcd",
                "premiumspaidcd",
                "filedf8899cd",
                "filedf1098ccd",
                "excbushldngscd",
                "s4966distribcd",
                "distribtodonorcd",
                "initiationfees",
                "grsrcptspublicuse",
                "grsincmembers",
                "grsincother",
                "filedlieuf1041cd",
                "txexmptint",
                "qualhlthplncd",
                "qualhlthreqmntn",
                "qualhlthonhnd",
                "rcvdpdtngcd",
                "filedf720cd",
                "totreprtabled",
                "totcomprelatede",
                "totestcompf",
                "noindiv100kcnt",
                "nocontractor100kcnt",
                "totcntrbgfts",
                "prgmservcode2acd",
                "totrev2acola",
                "prgmservcode2bcd",
                "totrev2bcola",
                "prgmservcode2ccd",
                "totrev2ccola",
                "prgmservcode2dcd",
                "totrev2dcola",
                "prgmservcode2ecd",
                "totrev2ecola",
                "totrev2fcola",
                "totprgmrevnue",
                "invstmntinc",
                "txexmptbndsproceeds",
                "royaltsinc",
                "grsrntsreal",
                "grsrntsprsnl",
                "rntlexpnsreal",
                "rntlexpnsprsnl",
                "rntlincreal",
                "rntlincprsnl",
                "netrntlinc",
                "grsalesecur",
                "grsalesothr",
                "cstbasisecur",
                "cstbasisothr",
                "gnlsecur",
                "gnlsothr",
                "netgnls",
                "grsincfndrsng",
                "lessdirfndrsng",
                "netincfndrsng",
                "grsincgaming",
                "lessdirgaming",
                "netincgaming",
                "grsalesinvent",
                "lesscstofgoods",
                "netincsales",
                "miscrev11acd",
                "miscrevtota",
                "miscrev11bcd",
                "miscrevtot11b",
                "miscrev11ccd",
                "miscrevtot11c",
                "miscrevtot11d",
                "miscrevtot11e",
                "totrevenue",
                "grntstogovt",
                "grnsttoindiv",
                "grntstofrgngovt",
                "benifitsmembrs",
                "compnsatncurrofcr",
                "compnsatnandothr",
                "othrsalwages",
                "pensionplancontrb",
                "othremplyeebenef",
                "payrolltx",
                "feesforsrvcmgmt",
                "legalfees",
                "accntingfees",
                "feesforsrvclobby",
                "profndraising",
                "feesforsrvcinvstmgmt",
                "feesforsrvcothr",
                "advrtpromo",
                "officexpns",
                "infotech",
                "royaltsexpns",
                "occupancy",
                "travel",
                "travelofpublicoffcl",
                "converconventmtng",
                "interestamt",
                "pymtoaffiliates",
                "deprcatndepletn",
                "insurance",
                "othrexpnsa",
                "othrexpnsb",
                "othrexpnsc",
                "othrexpnsd",
                "othrexpnse",
                "othrexpnsf",
                "totfuncexpns",
                "nonintcashend",
                "svngstempinvend",
                "pldgegrntrcvblend",
                "accntsrcvblend",
                "currfrmrcvblend",
                "rcvbldisqualend",
                "notesloansrcvblend",
                "invntriesalesend",
                "prepaidexpnsend",
                "lndbldgsequipend",
                "invstmntsend",
                "invstmntsothrend",
                "invstmntsprgmend",
                "intangibleassetsend",
                "othrassetsend",
                "totassetsend",
                "accntspayableend",
                "grntspayableend",
                "deferedrevnuend",
                "txexmptbndsend",
                "escrwaccntliabend",
                "paybletoffcrsend",
                "secrdmrtgsend",
                "unsecurednotesend",
                "othrliabend",
                "totliabend",
                "unrstrctnetasstsend",
                "temprstrctnetasstsend",
                "permrstrctnetasstsend",
                "capitalstktrstend",
                "paidinsurplusend",
                "retainedearnend",
                "totnetassetend",
                "totnetliabastend",
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

    # df.fillna("")

    # convert data type format to integer
    # df["unique_key"] = df["unique_key"].apply(convert_to_int)
    # df["beat"] = df["beat"].apply(convert_to_int)
    # df["district"] = df["district"].apply(convert_to_int)
    # df["ward"] = df["ward"].apply(convert_to_int)
    # df["community_area"] = df["community_area"].apply(convert_to_int)
    # df["year"] = df["year"].apply(convert_to_int)

    # # convert data type format to float
    # df["x_coordinate"] = df["x_coordinate"].apply(resolve_nan)
    # df["y_coordinate"] = df["y_coordinate"].apply(resolve_nan)
    # df["latitude"] = df["latitude"].apply(resolve_nan)
    # df["longitude"] = df["longitude"].apply(resolve_nan)

    # pdb.set_trace()

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
        "irs 990 2015 process completed at "
        + str(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    )


def resolve_nan(input: str) -> str:
    str_val = ""
    if input == "" or (math.isnan(input)):
        str_val = ""
    else:
        str_val = str(input)
    return str_val.replace("None", "")


def convert_to_int(input: str) -> str:
    str_val = ""
    if input == "" or (math.isnan(input)):
        str_val = ""
    else:
        str_val = str(int(round(input, 0)))
    return str_val


# def rename_headers(df):
#     header_names = {
#         "ID": "unique_key",
#         "Case Number": "case_number",
#         "Date": "date",
#         "Block": "block",
#         "IUCR": "iucr",
#         "Primary Type": "primary_type",
#         "Description": "description",
#         "Location Description": "location_description",
#         "Arrest": "arrest",
#         "Domestic": "domestic",
#         "Beat": "beat",
#         "District": "district",
#         "Ward": "ward",
#         "Community Area": "community_area",
#         "FBI Code": "fbi_code",
#         "X Coordinate": "x_coordinate",
#         "Y Coordinate": "y_coordinate",
#         "Year": "year",
#         "Updated On": "updated_on",
#         "Latitude": "latitude",
#         "Longitude": "longitude",
#         "Location": "location"
#     }

#     for old_name, new_name in header_names.items():
#         df.rename(old_name, new_name)


# def convert_dt_format(dt_str):
#     # Old format: MM/dd/yyyy hh:mm:ss aa
#     # New format: yyyy-MM-dd HH:mm:ss
#     if dt_str is None or len(dt_str) == 0:
#         return dt_str
#     else:
#         return datetime.datetime.strptime(dt_str, "%m/%d/%Y %H:%M:%S %p").strftime(
#             "%Y-%m-%d %H:%M:%S"
#         )


# def convert_values(df):
#     dt_cols = [
#         "date" ,
#         "updated_on"
#     ]

#     for dt_col in dt_cols:
#         df[dt_col] = df[dt_col].apply(convert_dt_format)

# int_cols = ["city_asset_number"]
# for int_col in int_cols:
#     df[int_col] = df[int_col].astype('Int64')


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
