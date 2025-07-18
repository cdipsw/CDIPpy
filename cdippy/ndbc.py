"""Methods for working with NDBC"""

import os
from datetime import datetime, timezone

import cdippy.utils.urls as uu
import cdippy.utils.utils as cu

sos_base = "https://sdf.ndbc.noaa.gov/sos/server.php"
request = "request=DescribeSensor"
service = "service=SOS"
version = "version=1.0.0"
outputformat = 'outputformat=text/xml;subtype="sensorML/1.0.1"'
describe_stn = "procedure=urn:ioos:station:wmo:"

cdip_base = "https://cdip.ucsd.edu"


def get_stn_info(wmo_id):
    """Work in progress, querying ndbc sos service."""
    qry = "&".join([request, service, version, outputformat, describe_stn + wmo_id])
    url = "?".join([sos_base, qry])
    root = uu.load_et_root(url)
    results = []
    uu.rfindt(root, results, "description")


def get_wmo_id(
    stn,
    store=True,
    filepath=".",
):
    """Queries cdip wmo id table for a given station. Drops pickle file locally."""
    pkl_fl = filepath + "/WMO_IDS.pkl" if store else None
    now = datetime.now(timezone.utc)
    if not pkl_fl or now.minute == 23 or not os.path.isfile(pkl_fl):
        url = "/".join([cdip_base, "wmo_ids"])
        r = uu.read_url(url)
        ids = {}
        for line in r.splitlines():
            ids[line[0:3]] = line[5:].strip()
        if pkl_fl:
            cu.pkl_dump(ids, pkl_fl)
    else:
        ids = cu.pkl_load(pkl_fl)
    if stn in ids:
        return ids[stn]
    else:
        return None
