"""
Convenience methods for working with NDBC stations.
"""

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
    """
    *Work in progress*:  querying ndbc sos service.

    Args:
        wmo_id (str): The WMO id of the station.
    """
    qry = "&".join([request, service, version, outputformat, describe_stn + wmo_id])
    url = "?".join([sos_base, qry])
    root = uu.load_et_root(url)
    results = []
    uu.rfindt(root, results, "description")


def get_wmo_id(stn):
    """
    Queries cdip table of WMO ids for the id of a given station. Optionally stores the table as a pickle file.

    Args:
        stn (str): CDIP 3 digit id.
        store (bool): Whether to store the table locally. Defaults to `True`.
        filepath (str): Where to store the WMO id table locally. Does nothing if `store=False`. Defaults to ".".

    Returns:
        id (str): The NDBC id for the station as a string or `None` if it is not found.
    """
    pkl_fl = "./WMO_IDS.pkl"
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
    return None
