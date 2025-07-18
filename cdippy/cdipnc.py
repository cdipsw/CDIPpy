from datetime import datetime, timedelta, timezone
import os

import logging
import netCDF4

import numpy as np
import numbers
from bisect import bisect_left, bisect_right

import cdippy.ndbc as ndbc
import cdippy.utils.utils as cu
import cdippy.utils.urls as uu


logger = logging.getLogger(__name__)


class CDIPnc:
    """A base class used by the class StnData for retrieving data from
    CDIP netCDF (nc) files located either locally or remotely.

    Files accessed remotely are served by CDIP's THREDDS server.
    Files accessed locally need to be located within a specific
    directory hierarchy.

    For each CDIP nc file "type" such as historic.nc or archive.nc,
    there is a corresponding sub-class, e.g. Historic or Archive.
    Although the constructors of these classes can be used to access
    data, StnData is recommended because it seamlessly combines
    records across multiple files.
    """

    THREDDS_url = "https://thredds.cdip.ucsd.edu"
    dods = "thredds/dodsC"
    url = None

    # - Load_stn_nc_files only checks for this number of deployments
    max_deployments = 99

    # - Top level data dir for nc files. Files must be within subdirectories:
    # - i.e. <data_dir>/REALTIME, <data_dir>/ARCHIVE/201p1
    data_dir = None

    # DATA QUALITY FLAGS AND PUBLIC/NONPUB
    #
    # waveFlagPrimary (WFP): 1-good, 2-not_evaluated, 3-questionable, 4-bad, 9-missing
    # waveFlagSecondary (WFS): 0-unspecified, 1-sensor issues, 2... are specific messages e.g. Hs out of bounds
    #
    # Data for public release is distinguished by WFP=1 and found in all nc files.
    # Data not for public release is distinguished by WFP=4 and found in all files except historic.nc
    #
    # There are cases where WFP=1 and WFS!=0 - e.g. if frequency bands have been reformatted.
    # Records with WFP=4 are not necessarily bad data.
    # All xy records are flagged WFP=2 - not_evaluated.
    #
    # NC files: latest, pre-deploy, moored, offsite, recovered, historic, archive
    #
    pub_set_default = "public"
    # Dashed tags such as public-good are for backwards compatibility
    pub_set_map = {
        "public": "public",
        "nonpub": "nonpub",
        "all": "all",
        "public-good": "public",
        "nonpub-all": "nonpub",
        "both-all": "all",
    }

    # Applies the mask before data is returned
    apply_mask = True

    # Active datasets - deployments that span NOW
    active_datasets = {
        "predeploy": "p0",
        "moored": "p1",
        "offsite": "p2",
        "recovered": "p3",
    }

    # Spectral layout. For each dataset we need to determine if it is mk3 (64 bands)
    # or mk4 (100 bands) spectral layout. Prior to aggregation, if 1 dataset is mk3,
    # all spectral layouts must be converted to mk3 during aggregation.
    spectral_layout = None

    # REQUESTING DATA PROCEDURE
    #
    # HOW TO USE
    # 1. call set_request_info
    # 2. call get_request
    #
    # HOW IT WORKS
    # 1. For a given set of variables of the same type (e.g. 'wave'),
    #   a. determine the dimension var name and if it is a time dimension
    #   b. determine the ancillary variable name (e.g. 'waveFlagPrimary'), if it exists
    # 2. If the dimension is a time dimension, find the start and end indices based on the query
    #    (Use start and end indices to subset all variables henceforth)
    # 3. Create an ancillary variable mask based on the pub set (and start, end indices if applicable)
    # 4. For each variable,
    #    a. use start, end indices to create a masked array
    #    b. union the variable's mask with the ancillary mask
    #    c. set the new masked array variable's mask to the union mask
    # 5. Apply the mask if self.apply_mask set True.

    def __init__(self, data_dir: str = None, deployment: int = None):
        """PARAMETERS
        ----------
        data_dir : str [optional]
            Either a full path to a directory containing a local directory hierarchy
            of nc files. E.g. '/project/WNC' or a url to a THREDDS server.
        deployment : int [optional]
            Supply this to access specific station deployment data.
            Must be >= 1.
        """

        self.nc = None
        self.data_dir = data_dir
        self.deployment = deployment

    def set_request_info(
        self,
        start: datetime = None,
        end: datetime = None,
        vrs: list = ["waveHs"],
        pub_set: str = "public",
        apply_mask: bool = True,
    ) -> None:
        """Initializes data request information for get_request.

        PARAMETERS
        ----------
        start : str or datetime [optional] : default Jan 1, 1975
            Start time of data request (UTC). If provided as a string must
            be in the format Y-m-d H:M:S where Y is 4 chars and all others
            are 2 chars. Ex. '2020-03-30 19:32:56'.
        end : str or datetime [optional] : default now
            End time of data request (UTC). If not supplied defaults to now.
        vrs : list [optional] : default ['waveHs']
            A list of the names of variables to retrieve. They all must start
            with the same prefix, e.g. ['waveHs', 'waveTp', 'waveDp']
        pub_set: str [optional] values = public|nonpub|all
            Filters data based on data quality flags.
        apply_mask: bool [optional] default True
            Removes values from the masked array that have a mask value of True.
            Ex. If nonpub data is requested and apply_mask is False, the returned
            array will contain both public and nonpublic data (although public
            data records will have the mask value set to True). If apply_mask
            is set to True, only nonpub records will be returned.
        """
        if start is None:
            start = datetime(1975, 1, 1).replace(tzinfo=timezone.utc)
        if end is None:
            end = datetime.now(timezone.utc)
        self.set_timespan(start, end)
        self.pub_set = self.get_pub_set(pub_set)  # Standardize the set name
        if apply_mask is not None:
            self.apply_mask = apply_mask
        self.vrs = vrs

    def set_timespan(self, start, end):
        """Sets request timespan"""
        if isinstance(start, str):
            self.start_dt = datetime.strptime(start, "%Y-%m-%d %H:%M:%S").replace(
                tzinfo=timezone.utc
            )
        else:
            self.start_dt = start
        if isinstance(end, str):
            self.end_dt = datetime.strptime(end, "%Y-%m-%d %H:%M:%S").replace(
                tzinfo=timezone.utc
            )
        else:
            self.end_dt = end
        self.start_stamp = cu.datetime_to_timestamp(self.start_dt)

        self.end_stamp = cu.datetime_to_timestamp(self.end_dt)

    def get_request(self) -> dict:
        """Returns the data specified using set_request_info.

        RETURNS
        -------
        A dictionary containing keys of the requested variables each
        of which is a numpy masked array of data values. In addition,
        the time values are returned as well. For example, if waveHs
        was requested, the dictionary will look like this:
        {'waveHs': <np.masked_array>, 'waveTime': <np.masked_array>}
        """
        mask_results = {}
        save = {}
        result = {}

        # - Check if requested variable 0 exists
        first_var = self.get_var(self.vrs[0])
        if first_var is None:
            return result

        # Use first var to determine the dimension, grab it and find indices
        time_dim = None
        for dim_name in first_var.dimensions:
            nc_var = self.get_var(dim_name)
            if nc_var is None:  # To handle non-existing "count" variables
                continue
            if nc_var.units[0:7] == "seconds":
                time_dim = dim_name
                # dim_data = np.ma.asarray(self.nc.variables[dim_name][:])
                dim_data = self.__make_masked_array(nc_var, 0, nc_var.size)
                # - find time dimension start and end indices
                s_idx, e_idx = self.__get_indices(
                    dim_data[:], self.start_stamp, self.end_stamp
                )
                if s_idx == e_idx:
                    return result
                mask_results[time_dim] = dim_data[s_idx:e_idx]
            else:  # E.g. waveFrequency (Do I want to add to result?
                save[dim_name] = self.nc.variables[dim_name]

        # Grab the time subset of each variable
        for v_name in self.vrs:
            v = self.get_var(v_name)
            if v is None:
                continue
            if v_name == "metaStationName":
                # Use existing byte_arr_to_string method for station name
                result[v_name] = self.byte_arr_to_string(self.nc.variables[v_name][:])
            elif len(v.dimensions) == 1 and v.dimensions[0] == "maxStrlen64":
                arr = self.nc.variables[v_name][:]
                result[v_name] = self.byte_arr_to_string(arr).strip("\x00")
            elif time_dim:
                mask_results[v_name] = self.__make_masked_array(v, s_idx, e_idx)
            else:
                # !!! This could be a problem for 2-d arrays. Specifying end
                # index too large may reshape array?
                #
                # Also, there seems to be a bug for single values such as
                # metaWaterDepth in realtime files. Those variables have
                # no shape (shape is an empty tupble) and len(v) bombs even
                # though v[:] returns an array with one value.
                try:
                    v_len = len(v)
                except Exception:
                    v_len = 1
                result[v_name] = self.__make_masked_array(v, 0, v_len)

        # Use first var to determine the ancillary variable, e.g. waveFlagPrimary
        # If there is an ancillary variable, use pub/nonpub to create a mask
        if hasattr(first_var, "ancillary_variables"):
            anc_names = first_var.ancillary_variables.split(" ")
            anc_name = anc_names[0]
            # Create the variable mask using pub/nonpub choice
            if not time_dim:
                s_idx = None
            anc_mask = self.make_pub_mask(anc_name, s_idx, e_idx)
        else:
            anc_mask = None

        # Still a problem. 2-d vars.
        # Seems to work if the variable has no mask set. But
        # if mask set, returns 1-d var.
        for v_name in mask_results:
            if self.apply_mask and anc_mask is not None:
                v = mask_results[v_name]
                mask_results[v_name] = v[~anc_mask]
            result[v_name] = mask_results[v_name]

        return result

    def __make_masked_array(
        self, nc_var: str, s_idx: int, e_idx: int
    ) -> np.ma.masked_array:
        """Returns a numpy masked array for a given nc variable and indices.

        e_idx is appropriate for python arrays. I.e. one more than last index.
        """
        if len(nc_var.shape) <= 1:
            try:
                data = np.ma.asarray(nc_var[s_idx:e_idx])
            except Exception:
                try:
                    data = np.ma.asarray(nc_var[s_idx:e_idx])
                except Exception:
                    return None
            return data
        elif len(nc_var.shape) == 2:
            try:
                arr = np.ma.asarray(nc_var[s_idx:e_idx, :])
            except Exception:
                try:
                    arr = np.ma.asarray(nc_var[s_idx:e_idx, :])
                except Exception:
                    return None
            return arr

    def make_pub_mask(self, anc_name: str, s_idx: int, e_idx: int) -> np.ndarray:
        """Returns an np.ndarray of bools given pub_set and ancillary var"""

        # No s_idx, use whole array. Otherwise time subset the anc var.
        nc_primary = self.get_var(anc_name)
        if s_idx is None:
            s_idx = 0
            e_idx = len(nc_primary)
        primary_flag_values = nc_primary[s_idx:e_idx]

        if anc_name == "waveFrequencyFlagPrimary":
            return None  # Not sure about this one
        elif anc_name == "gpsStatusFlags":
            return np.ma.make_mask(primary_flag_values < 0, shrink=False)
        elif (
            anc_name == "waveFlagPrimary"
            or anc_name == "sstFlagPrimary"
            or anc_name == "acmFlagPrimary"
            or anc_name == "cat4FlagPrimary"
        ):
            public_mask = primary_flag_values != 1
        elif anc_name == "xyzFlagPrimary":
            public_mask = primary_flag_values != 2
        else:
            return None

        if self.pub_set == "public":
            return np.ma.make_mask(public_mask, shrink=False)
        elif self.pub_set == "nonpub":
            return np.ma.make_mask(~public_mask, shrink=False)
        elif self.pub_set == "all":
            return np.ma.make_mask(primary_flag_values < 0, shrink=False)

    def get_pub_set(self, name: str) -> str:
        """Returns either 'public', 'nonpub' or 'all'.

        Maintains backwards compatibility with prior pub_set names.
        """
        if name is None or name not in self.pub_set_map.keys():
            return self.pub_set_default
        else:
            return self.pub_set_map[name]

    def get_var_prefix(self, var_name: str) -> str:
        """Returns 'wave' part of the string 'waveHs'."""
        s = ""
        for c in var_name:
            if c.isupper():
                break
            s += c
        return s

    def get_flag_meanings(self, flag_name: str) -> list:
        """Returns flag category values and meanings given a flag_name."""
        return self.get_var(flag_name).flag_meanings.split(" ")

    def get_flag_values(self, flag_name: str) -> list:
        """Returns flag category values and meanings given a flag_name."""
        v = self.get_var(flag_name)
        if flag_name[0:3] == "gps":
            return v.flag_masks
        else:
            return v.flag_values

    def get_date_modified(self) -> datetime:
        """Returns the time the nc file was last modified."""
        return datetime.strptime(self.nc.date_modified, "%Y-%m-%dT%H:%M:%SZ")

    def get_coverage_start(self) -> datetime:
        """Returns the start time of the nc file data coverage."""
        return datetime.strptime(self.nc.time_coverage_start, "%Y-%m-%dT%H:%M:%SZ")

    def get_coverage_end(self) -> datetime:
        """Returns the end time of the nc file data coverage."""
        return datetime.strptime(self.nc.time_coverage_end, "%Y-%m-%dT%H:%M:%SZ")

    def __get_indices(self, times: list, start_stamp: int, end_stamp: int) -> tuple:
        """Returns start and end indices to include any times that are equal to start_stamp or end_stamp."""
        s_idx = bisect_left(times, start_stamp)  # Will include time if equal
        # Will give e_idx appropriate for python arrays
        e_idx = bisect_right(times, end_stamp, s_idx)
        return s_idx, e_idx

    def get_nc(self, url: str = None, retry: bool = False) -> netCDF4.Dataset:
        if not url:
            url = self.url
        try:
            return netCDF4.Dataset(url)
        except Exception as e:
            # Try again if unsuccessful (nc file not ready? THREDDS problem?)
            if retry:
                logger.warning(
                    msg=f"Retrying to open dataset at {url} due to an unexpected exception: {e}"
                )
                try:
                    return netCDF4.Dataset(url)
                except Exception:
                    pass
            logger.exception(
                msg=f"Failed to open dataset at {url} due to an unexpected exception: {e}"
            )
            return None

    def byte_arr_to_string(self, b_arr: np.ma.masked_array) -> str:
        if np.ma.is_masked(b_arr):
            b_arr = b_arr[~b_arr.mask]
        s = ""
        for c in b_arr[:].astype("U"):
            s += c
        return s

    def metaStationName(self) -> str:
        """Returns the metaStationName."""
        if self.nc is None:
            return None
        return self.byte_arr_to_string(self.nc.variables["metaStationName"][:])

    def get_var(self, var_name: str):
        """Checks if a variable exists then returns a pointer to it."""
        if self.nc is None or var_name not in self.nc.variables:
            return None
        return self.nc.variables[var_name]

    def get_dataset_urls(self) -> dict:
        """Returns a dict of two lists of urls (or paths) to all CDIP station datasets.

        The top level keys are 'realtime' and 'historic'. The urls are retrieved by
        either descending into the THREDDS catalog.xml or recursively walking through data_dir sub
        directories.

        For applications that need to use the data from multiple deployment files for
        a station, stndata:get_nc_files will load those files efficiently.
        """
        if self.data_dir is not None:
            result = {"realtime": [], "archive": []}
            # - Walk through data_dir sub dirs
            for dirpath, dirnames, filenames in os.walk(self.data_dir):
                if dirpath.find("REALTIME") >= 0:
                    for file in filenames:
                        if os.path.splitext(file)[1] == ".nc":
                            result["realtime"].append(os.path.join(dirpath, file))
                elif dirpath.find("ARCHIVE") >= 0:
                    for file in filenames:
                        if os.path.splitext(file)[1] == ".nc":
                            result["archive"].append(os.path.join(dirpath, file))
            return result

        catalog_url = "/".join([self.THREDDS_url, "thredds", "catalog.xml"])

        result = {}
        root = uu.load_et_root(catalog_url)
        catalogs = []
        uu.rfindta(root, catalogs, "catalogRef", "href")
        for catalog in catalogs:
            # - Archive data sets
            url = self.THREDDS_url + catalog
            cat = uu.load_et_root(url)
            if catalog.find("archive") >= 0:
                ar_urls = []
                uu.rfindta(cat, ar_urls, "catalogRef", "href")
                b_url = os.path.dirname(url)
                # - Station datasets
                ar_ds_urls = []
                for u in ar_urls:
                    url = b_url + "/" + u
                    ds = uu.load_et_root(url)
                    uu.rfindta(ds, ar_ds_urls, "dataset", "urlPath")
                full_urls = []
                for url in ar_ds_urls:
                    full_urls.append(
                        "/".join([self.THREDDS_url, self.dods, "cdip", url[5:]])
                    )
                result["archive"] = full_urls
            elif catalog.find("realtime") >= 0:
                rt_ds_urls = []
                uu.rfindta(cat, rt_ds_urls, "dataset", "urlPath")
                full_urls = []
                for url in rt_ds_urls:
                    full_urls.append(
                        "/".join([self.THREDDS_url, self.dods, "cdip", url[5:]])
                    )
                result["realtime"] = full_urls
        return result

    def set_dataset_info(
        self, stn: str, org: str, dataset_name: str, deployment: int = None
    ) -> None:
        """Sets self.stn, org, filename, url and loads self.nc. The key to understanding all of
        this is that we are ultimately setting _url_, which can be an actual path to the
        nc file or a url to THREDDS DoDS service.

        PARAMETERS
        ----------
        stn : str
           Can be in 3char (e.g. 028) or 5char (e.g. 028p2) format for org=cdip
        org: str
            (Organization) Values are: cdip|ww3|external
        dataset_name : str
            Values: realtime|historic|archive|realtimexy|archivexy|
                    predeploy|moored|offsite|recovered
        deployment : int [optional]
            Supply this to access specific station deployment data.
            Must be >= 1.

        Paths are:
            <top_dir>/EXTERNAL/WW3/<filename>  [filename=<stn>_<org_dir>_<dataset_name>.nc][CDIP stn like 192w3]
            <top_dir>/REALTIME/<filename> [filename=<stn><p1>_rt.nc]
            <top_dir>/REALTIME/<filename> [filename=<stn><p1>_xy.nc]
            <top_dir>/ARCHIVE/<stn>/<filename> [filename=<stn3><p1>_<deployment>.nc]
            <top_dir>/PREDEPLOY/<stn>/<filename> [filename=<stn3><pX>_<deployment>_rt.nc]**
            <top_dir>/PREDEPLOY/<stn>/<filename> [filename=<stn3><pX>_<deployment>_xy.nc]**

            **Active deployment directories are PREDEPLOY (p0), MOORED (p1), OFFSITE (p2)  and RECOVERED (p3)
              pX = p0|p1|p2|p3; deployment = dXX e.g. d01

        Urls are:
            http://thredds.cdip.ucsd/thredds/dodsC/<org1>/<org_dir>/<filename>
               [org1=external|cdip,org_dir=WW3|OWI etc]
            http://thredds.cdip.ucsd/thredds/dodsC/<org1>/<dataset_name>/<filename>

            Note:
               Since adding dataset_name, we no longer need the 5char stn id
               for org=cdip datasets. The p_val will be 'p1' for every dataset except
               active datasets in buoy states predeploy (p0), offsite (p2) and recovered (p3).
        """
        ext = ".nc"

        # Allowing data_dir to be either url or path
        __using_path = False
        if self.data_dir:
            if self.data_dir[0:4] == "http":
                self.THREDDS_url = self.data_dir
            else:
                __using_path = True

        if org is None:
            org = "cdip"
        if org == "cdip":
            org1 = "cdip"
        else:
            org1 = "external"
        # Org_dir follows 'external' and always uppercase (isn't used when org is cdip)
        org_dir = org.upper()

        # Handle the xy datasets
        if "xy" in dataset_name:
            ftype = "xy"
            dataset_name = dataset_name[0:-2]
        else:
            ftype = "rt"

        # Historic and archive both use archive as a dataset_dir
        # Lowercase for url, uppercase for url
        if dataset_name == "historic":
            dataset_dir = "archive"
        else:
            dataset_dir = dataset_name

        # Local paths use uppercase
        if __using_path:
            org1 = org1.upper()
            dataset_dir = dataset_dir.upper()
            if org == "cdip":
                url_pre = self.data_dir
            else:
                url_pre = "/".join([self.data_dir, org1])
        else:
            url_pre = "/".join([self.THREDDS_url, self.dods, org1])

        # Set p_val to 'p1' - it will get changed appropriately below
        stn = stn[0:3] + "p1"

        # Make filename and url
        if org == "cdip":
            if type(deployment) is not str:
                deployment = "d" + str(deployment).zfill(2)
            if dataset_name in self.active_datasets.keys():
                stn = stn[0:3] + self.active_datasets[dataset_name]
                dataset_name = "_".join([deployment, ftype])
            elif dataset_name == "realtime":
                dataset_name = ftype
            elif dataset_name == "historic":
                dataset_dir = "/".join([dataset_dir, stn])
            elif dataset_name == "archive" and deployment:
                dataset_name = deployment
                dataset_dir = "/".join([dataset_dir, stn])
            self.filename = "_".join([stn, dataset_name + ext])
            self.url = "/".join([url_pre, dataset_dir, self.filename])
        else:
            if stn[3:4] == "p" and org == "ww3":  # Cdip stn id
                stn_tmp = ndbc.get_wmo_id(stn[0:3])
            else:
                stn_tmp = stn
            self.filename = "_".join([stn_tmp, org_dir, dataset_name + ext])
            self.url = "/".join([url_pre, org_dir, self.filename])

        self.stn = stn
        self.org = org
        self.nc = self.get_nc()


class Latest(CDIPnc):
    """Loads the latest_3day.nc and has methods for retrieving the data."""

    # Do not apply the mask to get_request calls.
    apply_mask = False

    def __init__(self, data_dir: str = None):
        """PARAMETERS
        ----------
        data_dir : str [optional]
            Either a full path to a directory containing a local directory hierarchy
            of nc files. E.g. '/project/WNC' or a url to a THREDDS server.
        """

        CDIPnc.__init__(self, data_dir)
        self.labels = []  # - Holds stn labels, e.g. '100p1' for this instance
        # Set latest timespan (Latest_3day goes up to 30 minutes beyond now)
        now_plus_30min = datetime.now(timezone.utc) + timedelta(minutes=30)
        # Using the unix epoch to catch all data in latest_3day in case the file is very old
        epoch = datetime.fromtimestamp(0)
        self.set_timespan(epoch, now_plus_30min)

        # Set basic information and init self.nc
        self.filename = "latest_3day.nc"
        if self.data_dir:
            self.url = "/".join([self.data_dir, "REALTIME", self.filename])
        else:
            self.url = "/".join(
                [CDIPnc.THREDDS_url, CDIPnc.dods, "cdip/realtime/latest_3day.nc"]
            )
        self.nc = self.get_nc(self.url)

    def metaStationNames(self) -> list:
        """Get list of latest station names."""
        if self.nc is None:
            return None
        names = []
        for name_arr in self.nc.variables["metaStationName"]:
            names.append(self.byte_arr_to_string(name_arr))
        return names

    def metaSiteLabels(self) -> list:
        """Sets and returns self.labels, a list of station labels, e.g. ['100p1',...]."""
        if self.nc is None:
            return None
        for label_arr in self.nc.variables["metaSiteLabel"]:
            self.labels.append(self.byte_arr_to_string(label_arr))
        return self.labels

    def metaDeployLabels(self) -> list:
        """Returns a list of metaDeployLabels."""
        if self.nc is None:
            return None
        labels = []
        for label_arr in self.nc.variables["metaDeployLabel"]:
            labels.append(self.byte_arr_to_string(label_arr))
        return labels

    def metaDeployNumbers(self) -> list:
        """Returns a list of metaDeployNumbers."""
        if self.nc is None:
            return None
        numbers = []
        for number in self.nc.variables["metaDeployNumber"]:
            numbers.append(number)
        return numbers

    def metaWMOids(self) -> list:
        """Returns a list of WMO ids, e.g. ['46225',...]."""
        if self.nc is None:
            return None
        labels = []
        for label_arr in self.nc.variables["metaWMOid"]:
            labels.append(self.byte_arr_to_string(label_arr))
        return labels

    def metaLatitudes(self) -> list:
        """Returns a list of station latitudes, e.g. [23.4,...]."""
        if self.nc is None:
            return None
        lats = []
        for lat in self.nc.variables["metaLatitude"][:]:
            lats.append(lat)
        return lats

    def metaLongitudes(self) -> list:
        """Returns a list of station longitudes, e.g. [23.4,...]."""
        if self.nc is None:
            return None
        lons = []
        for lon in self.nc.variables["metaLongitude"][:]:
            lons.append(lon)
        return lons

    def metaWaterDepths(self) -> list:
        """Returns a list of station water depths."""
        if self.nc is None:
            return None
        depths = []
        for d in self.nc.variables["metaWaterDepth"][:]:
            depths.append(d)
        return depths

    def get_latest(
        self,
        pub_set: str = "public",
        meta_vars: list = None,
        params: list = None,
        array_format=True,
    ) -> list:
        """
        By default, array_format = True, it will return a dictionary of numpy masked
        arrays of the latest requested parameters as well as metadata information.

        If array_format = False, it returns a list of dicts. Each dict will contain
        latest station data and metadata.

        Parameter data values that are masked or non-existant are set to np.nan.
        Time values (e.g. 'waveTime') for the wave data if masked or non-existant
        are set to None.

        Both meta_vars and params if None (or not included in the argument list) will
        return default sets of meta_vars and parameters. If meta_vars and params are set
        just those will be returned.
        """

        # Use these if params (or meta_vars) is None

        default_params_by_type = {
            "wave": ["waveHs", "waveTp", "waveDp", "waveTa"],
            "sst": ["sstSeaSurfaceTemperature"],
            "acm": ["acmSpeed", "acmDirection"],
            "cat4": ["cat4AirTemperature"],
            "gps": ["gpsLongitude", "gpsLatitude"],
            "meta": [
                "metaLongitude",
                "metaLatitude",
                "metaWaterDepth",
                "metaStationName",
                "metaSiteLabel",
                "metaDeployLabel",
                "metaWMOid",
            ],
        }

        if params is None:
            params = []
            for t in default_params_by_type:
                if "meta" not in t:
                    params += default_params_by_type[t]

        # Initialize requested parameters by type

        requested_params = {}
        for typ in default_params_by_type:
            for p in params:
                if typ in p:
                    if typ not in requested_params:
                        requested_params[typ] = []
                    requested_params[typ].append(p)
        requested_types = list(set(requested_params.keys()))

        self.pub_set = self.get_pub_set(pub_set)

        # Load meta variables

        if meta_vars is None:
            meta_vars = default_params_by_type["meta"]

        meta = {}
        for p in meta_vars:
            meta[p] = getattr(self, p + "s")()

        # We always need these to remove duplicates

        site_labels = self.metaSiteLabels()
        deploy_labels = self.metaDeployLabels()

        # Loop through the data types (e.g. 'wave', 'sst', 'acm' ...)
        # and grab data for the parameters requested.

        req = {}
        for typ in requested_types:

            # Add the parameters requested into the request list
            self.vrs = requested_params[typ].copy()

            # Add the necessary time variables into the request list
            self.vrs += [typ + "Time", typ + "TimeOffset", typ + "TimeBounds"]

            # Make the data request for the included parameters and time variables.
            req[typ] = self.get_request()

            # We don't quality check the GPS
            if typ != "gps":
                pub_mask = self.make_pub_mask(typ + "FlagPrimary", None, None)
                mask = np.ma.mask_or(req[typ][typ + "TimeOffset"].mask, pub_mask)
                req[typ][typ + "TimeOffset"].mask = mask

        num_stations = self.get_var("waveTimeOffset").shape[1]

        result = {}  # Store station dictionaries
        for s in range(num_stations):
            stn = {}

            # To remove duplicates (p1 usually) use the site label as a key, e.g. 162p1
            # We will be keeping the pX with the greatest deploy label.

            site_label = site_labels[s]
            if site_label in result:
                if deploy_labels[s] < result[site_label]["deploy_label"]:
                    continue

            stn["deploy_label"] = deploy_labels[s]

            latest_timestamp = -1  # To help find a time
            latest_type = None  # for the group of
            waves_included = False  # parameters.
            has_data = False
            for typ in requested_types:
                offsets = req[typ][typ + "TimeOffset"][:, s]
                t_n = typ + "Time"
                tb_n = typ + "TimeBounds"
                # Find the highest data index (latest data) for the type
                # using the TimeOffset.
                idx = -1
                if self.__has_a_number(offsets):
                    idx = np.ma.flatnotmasked_edges(offsets)[1]
                    stn[t_n] = req[typ][t_n][idx] + offsets[idx]
                    stn[tb_n] = np.ma.array([None, None])
                    for i in [0, 1]:
                        stn[tb_n][i] = req[typ][tb_n][idx][i] + offsets[idx]
                    for pm in requested_params[typ]:
                        stn[pm] = req[typ][pm][idx, s]
                    if typ != "gps":
                        has_data = True
                else:
                    stn[t_n] = np.nan
                    stn[tb_n] = np.nan
                    for pm in requested_params[typ]:
                        stn[pm] = np.nan
                if stn[t_n] is not np.nan and typ != "gps":
                    if typ == "wave":
                        waves_included = True
                    if stn[t_n] > latest_timestamp:
                        latest_timestamp = stn[t_n]
                        latest_type = typ
            stn["hasParameterData"] = has_data
            if latest_type is not None:
                group_type = "wave" if waves_included else latest_type
                stn["groupTime"] = stn[group_type + "Time"]
                stn["groupTimeBounds"] = stn[group_type + "TimeBounds"]
                least_timestamp = max(stn["groupTime"] - 1800, 0)
                for typ in requested_types:
                    t_n = typ + "Time"
                    if stn[t_n] is not np.nan and stn[t_n] < least_timestamp:
                        stn[t_n] = np.nan
            else:
                stn["groupTime"] = np.nan
                stn["groupTimeBounds"] = np.nan
            for m in meta_vars:
                stn[m] = meta[m][s]
            if stn["hasParameterData"] or (len(params) == 0 and len(meta_vars) > 0):
                result[site_label] = stn

        # To satisfy the original array_format = False, remove the site Labels

        new_result = []
        for site_label in result:
            new_result.append(result[site_label])
        result = new_result

        # Return parameters as lists in a single dict rather than a list of dicts.

        array_result = {}
        if array_format:
            for r in result:
                for key in r:
                    if key not in array_result:
                        array_result[key] = []
                    array_result[key].append(r[key])
            result = array_result

        return result

    def __has_a_number(self, arr):
        """Test if there is at least one number in the array"""
        for x in arr:
            if isinstance(x, numbers.Number):
                return True
        return False


class Active(CDIPnc):
    """Loads an "active" (predeploy, moored, offsite, recovered) rt nc file
    for the given station and deployment.

    E.g. a = Active('100', 6, 'predeploy')  # The predeploy data for stn 100 dep 6.
    """

    def __init__(
        self,
        stn: str,
        deployment: int,
        active_state_key: str,
        data_dir: str = None,
        org: str = None,
    ):
        """
        PARAMETERS
        ----------
        stn : str
           Can be in 2, 3 or 5 char format e.g. 28, 028, 028p2
        active_state_key : str
            Values: predeploy|moored|offsite|recovered
        deployment : int [optional]
            Supply this to access specific station deployment data.
            Must be >= 1.
        data_dir : str [optional]
            Either a full path to a directory containing a local directory hierarchy
            of nc files. E.g. '/project/WNC' or a url to a THREDDS server.
        """
        CDIPnc.__init__(self, data_dir)
        self.set_dataset_info(stn, org, active_state_key, deployment)
        self.pub_set_default = "all"


class Realtime(CDIPnc):
    """Loads the realtime nc file for the given station."""

    def __init__(self, stn: str, data_dir: str = None, org: str = None):
        """For parameters: See CDIPnc.set_dataset_info."""
        CDIPnc.__init__(self, data_dir)
        self.set_dataset_info(stn, org, "realtime")


class Historic(CDIPnc):
    """Loads the historic nc file for a given station."""

    def __init__(self, stn, data_dir=None, org=None):
        """For parameters see CDIPnc.set_dataset_info."""

        CDIPnc.__init__(self, data_dir)
        self.set_dataset_info(stn, org, "historic")


# In the following, _deployment_ can be str or int, e.g. 'd02' or 2


class Archive(CDIPnc):
    """Loads an archive (deployment) file for a given station and deployment."""

    def __init__(self, stn, deployment=None, data_dir=None, org=None):
        """For parameters see CDIPnc.set_dataset_info."""
        CDIPnc.__init__(self, data_dir)
        if not deployment:
            deployment = 1
        self.set_dataset_info(stn, org, "archive", deployment)

    def __get_idx_from_timestamp(self, timestamp: int) -> int:
        t0 = self.get_var("xyzStartTime")[0]
        r = self.get_var("xyzSampleRate")[0]
        # Mark I will have filter delay set to fill value
        d = self.get_var("xyzFilterDelay")
        d = 0 if d[0] is np.ma.masked else d[0]
        return int(round(r * (timestamp - t0 + d), 0))

    def __make_xyzTime(self, start_idx: int, end_idx: int) -> int:
        t0 = np.ma.asarray(self.get_var("xyzStartTime")[0])
        r = np.ma.asarray(self.get_var("xyzSampleRate")[0])
        # Mark I will have filter delay set to fill value
        d = self.get_var("xyzFilterDelay")
        d = 0 if d[0] is np.ma.masked else d[0]
        d = np.ma.asarray(d)
        i = np.ma.asarray(range(start_idx, end_idx))
        return t0 - d + i / r

    def get_xyz_timestamp(self, xyzIndex: int) -> int:
        """Returns the timestamp corresponding to the given xyz array index."""
        t0 = self.get_var("xyzStartTime")[0]
        r = self.get_var("xyzSampleRate")[0]
        # Mark I will have filter delay set to fill value
        d = self.get_var("xyzFilterDelay")
        d = 0 if d[0] is np.ma.masked else d[0]
        if t0 and r and d >= 0:
            return t0 - d + xyzIndex / r
        else:
            return None

    def get_request(self):
        """Overrides the base class method to handle xyz data requests."""

        # If not an xyz request, use base class version
        if self.get_var_prefix(self.vrs[0]) != "xyz":
            return super(Archive, self).get_request()

        # xyzData is shorthand for all these vars
        if self.vrs[0] == "xyzData":
            self.vrs = ["xyzXDisplacement", "xyzYDisplacement", "xyzZDisplacement"]

        # Handle the xyz request
        start_idx = self.__get_idx_from_timestamp(self.start_stamp)
        end_idx = self.__get_idx_from_timestamp(self.end_stamp)
        z = self.get_var("xyzZDisplacement")
        # Find out if the request timespan overlaps the data
        ts1 = cu.Timespan(start_idx, end_idx)
        ts2 = cu.Timespan(0, len(z) - 1)
        if not ts1.overlap(ts2):
            return {}
        # Make sure the indices will work with the arrays
        start_idx = max(0, start_idx)
        end_idx = min(len(z) - 1, end_idx)
        # Just calculate xyz times for the good indices
        xyzTime = self.__make_xyzTime(start_idx, end_idx)
        result = {"xyzTime": xyzTime}
        for vname in self.vrs:
            result[vname] = self.get_var(vname)[start_idx:end_idx]
        return result


class ActiveXY(Archive):
    """Loads an "active" (predeploy, moored, offsite, recovered) xy nc file
    for the given station and deployment.
    """

    def __init__(self, stn, deployment, dataset, data_dir=None, org=None):
        """
        PARAMETERS
            ----------
            dataset : str
                Active dataset name.
                Values are: predeploy|moored|offsite|recovered.
            For other parameters see CDIPnc.set_dataset_info.
        """
        CDIPnc.__init__(self, data_dir)
        self.set_dataset_info(stn, org, dataset + "xy", deployment)
        self.pub_set_default = "all"


class RealtimeXY(Archive):
    """Loads the realtime xy nc file for the given station."""

    def __init__(self, stn, data_dir=None, org=None):
        """For parameters see CDIPnc.set_dataset_info."""
        CDIPnc.__init__(self, data_dir)
        self.set_dataset_info(stn, org, "realtimexy")
