from datetime import datetime, timedelta
from bisect import bisect_left

import numpy.ma as ma
import numpy as np
import operator

from cdippy.cdipnc import (
    CDIPnc,
    Archive,
    Realtime,
    RealtimeXY,
    Historic,
    Active,
    ActiveXY,
)
import cdippy.utils.utils as cdip_utils
from cdippy.spectra import Spectra


class StnData(CDIPnc):
    """Returns data and metadata for the specified station.

    This class merges data from multiple CDIP netCDF files to
    produce a single dictionary with keys of the requested variables.
    Each key corresponds to a numpy masked array.

    METHODS
    -------
    get_series(start, end, vrs)
        Returns data for a station given start date, end date and a
        list of variables.
    get_parameters(start, end)
        Calls get_series with vrs set to parameter variables.
    get_spectra(start, end)
        Calls get_series with vrs set to spectrum variables.
    get_xyz
        Calls get_series with vrs set to xyz variables.
    get_stn_meta
        Returns all station meta variables.
    get_nc_files
        Returns a dictionary of all this station's netCDF files.
    get_target_times
        Returns a 2-tuple of timestamps, an interval corresponding
        to  n records to the right or left of target_timestamp.
    """

    nc_file_types = [
        "historic",
        "archive",
        "predeploy",
        "moored",
        "offsite",
        "recovered",
    ]

    # Commonly requested sets of variables
    parameter_vars = ["waveHs", "waveTp", "waveDp", "waveTa"]
    xyz_vars = ["xyzXDisplacement", "xyzYDisplacement", "xyzZDisplacement"]
    gps_vars = ["gpsLatitude", "gpsLongitude", "gpsStatusFlags"]
    spectrum_vars = [
        "waveEnergyDensity",
        "waveMeanDirection",
        "waveA1Value",
        "waveB1Value",
        "waveA2Value",
        "waveB2Value",
        "waveCheckFactor",
    ]
    meta_vars = [
        "metaStationName",
        "metaDeployLatitude",
        "metaDeployLongitude",
        "metaWaterDepth",
        "metaDeclination",
    ]
    meta_attributes = [
        "wmo_id",
        "geospatial_lat_min",
        "geospatial_lat_max",
        "geospatial_lat_units",
        "geospatial_lat_resolution",
        "geospatial_lon_min",
        "geospatial_lon_max",
        "geospatial_lon_units",
        "geospatial_lon_resolution",
        "geospatial_vertical_min",
        "geospatial_vertical_max",
        "geospatial_vertical_units",
        "geospatial_vertical_resolution",
        "time_coverage_start",
        "time_coverage_end",
        "date_created",
        "date_modified",
    ]

    pub_set = None
    vrs = None
    meta = None

    def __init__(
        self, stn: str, data_dir: str = None, org: str = None, deploy_num: int = None
    ):
        """
        PARAMETERS
        ----------
        stn : str
           Can be in 2, 3 or 5 char format e.g. 28, 028, 028p2
        data_dir : str [optional]
            Either a full path to a directory containing a local directory hierarchy
            of nc files. E.g. '/project/WNC' or a url to a THREDDS server.
        org: str
            (Organization) Values are: cdip|ww3|external
        deploy_num : int [optional]
            Supply this to access specific station deployment data.
            Must be >= 1.
        """
        self.nc = None
        self.stn = stn
        self.data_dir = data_dir
        self.org = org

        # Accept numbers for cdip stations
        if type(stn) is not str:
            stn = str(stn).zfill(3) + "p1"

        # Initialize nc file used for meta information
        self.deploy_num = deploy_num
        if deploy_num:
            # Check all active datasets in this order p3 -> p2 -> p1 -> p0
            p_lookup = dict([[v, k] for k, v in self.active_datasets.items()])
            __found_active_meta = False
            for p in reversed(sorted(p_lookup)):
                self.meta = Active(
                    self.stn, self.deploy_num, p_lookup[p], self.data_dir, self.org
                )
                if self.meta.nc:
                    __found_active_meta = True
                    break
            if not __found_active_meta:
                self.meta = Archive(self.stn, self.deploy_num, self.data_dir, self.org)
        else:
            self.historic = Historic(self.stn, self.data_dir, self.org)
            self.realtime = Realtime(self.stn, self.data_dir, self.org)
            if self.historic and self.historic.nc:
                self.meta = self.historic
            else:
                if self.realtime and self.realtime.nc:
                    self.meta = self.realtime
        if self.meta is None:
            return None

    def get_stn_meta(self) -> dict:
        """Returns a dict of station meta data."""
        result = {}
        if self.meta is None:
            return result
        self.meta.set_request_info(vrs=self.meta_vars)
        result = self.meta.get_request()
        for attr_name in self.meta_attributes:
            if hasattr(self.meta.nc, attr_name):
                result[attr_name] = getattr(self.meta.nc, attr_name)
        return result

    def get_parameters(
        self,
        start: datetime = None,
        end: datetime = None,
        pub_set: str = "public",
        apply_mask=True,
        target_records=0,
    ) -> dict:
        """Calls get_series to return wave parameters."""
        return self.get_series(
            start, end, self.parameter_vars, pub_set, apply_mask, target_records
        )

    def get_xyz(
        self, start: datetime = None, end: datetime = None, pub_set: str = "public"
    ) -> dict:
        """Calls get_series to return displacement data."""
        return self.get_series(start, end, self.xyz_vars, pub_set)

    def get_spectra(
        self,
        start: datetime = None,
        end: datetime = None,
        pub_set: str = "public",
        apply_mask: bool = True,
        target_records: int = 0,
        force_64bands: bool = False,
    ) -> dict:
        """Calls get_series to return spectral data."""
        return self.get_series(
            start,
            end,
            self.spectrum_vars,
            pub_set,
            apply_mask,
            target_records,
            force_64bands,
        )

    def get_series(
        self,
        start: datetime = None,
        end: datetime = None,
        vrs: list = None,
        pub_set: str = None,
        apply_mask: bool = None,
        target_records: int = 0,
        force_64bands: bool = False,
    ) -> dict:
        """
        Returns a dict of data between start and end dates with specified quality.

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
        target_records: int [optional]
            If start is specified and end is None, this will specify the number
            of additional records to return closest to start.
        force_64bands: bool [optional]
            For the case in which all spectra returned are mk4 100 band format,
            force the conversion to 64bands. Mixed formats are always returned in mk3
            64 band format.
        """
        if vrs is None:
            vrs = self.parameter_vars
        prefix = self.get_var_prefix(vrs[0])

        if start is not None and end is None:  # Target time
            if isinstance(start, str):
                start = datetime.strptime(start, "%Y-%m-%d %H:%M:%S")
            ts_I = self.get_target_timespan(
                cdip_utils.datetime_to_timestamp(start), target_records, prefix + "Time"
            )
            if ts_I[0] is not None:
                start = cdip_utils.timestamp_to_datetime(ts_I[0])
                end = cdip_utils.timestamp_to_datetime(ts_I[1])
            else:
                return None
        elif start is None:  # Use default 3 days back
            start = datetime.utcnow() - timedelta(days=3)
            end = datetime.utcnow()

        if pub_set is None:
            pub_set = self.pub_set

        if apply_mask is None:
            apply_mask = self.apply_mask

        self.force_64bands = force_64bands

        self.set_request_info(start, end, vrs, pub_set, apply_mask)
        print(self.deploy_num)

        if prefix == "xyz" and self.deploy_num is None:
            return self.__merge_xyz_request()
        elif prefix == "xyz" and self.deploy_num is not None:
            return self.__merge_active_request("xyz")
        elif self.deploy_num is None:
            return self.__merge_request()
        else:
            return self.__merge_active_request("rt")

    def __aggregate_dicts(self, dict1: dict, dict2: dict) -> dict:
        """
        Returns a dict of data combined from two dictionaries. Dict1 has oldest data.
        All the other __merge methods end up using this method.

        This method also redistributes 100 band spectra to 64 band format if 1) both
        formats are present in dict1 and dict2 or 2) the force_64bands option is True.
        """
        # Union the keys to make sure we check each one
        ukeys = set(dict1.keys()) | set(dict2.keys())
        # Determine if there are any spectra vars to redistribute
        svars = set(self.spectrum_vars) & ukeys

        if len(svars) != 0:
            key = next(iter(svars))  # retrieves an element from the set
            shape1 = dict1[key].shape[1] if key in dict1.keys() else 0
            shape2 = dict2[key].shape[1] if key in dict2.keys() else 0
            shapes = [shape1, shape2]
            if 100 in shapes and (self.force_64bands or 64 in shapes):
                dicts = [dict1, dict2]
                for i, shape in enumerate(shapes):
                    if shape == 100:
                        spectra_obj = Spectra()
                        spectra_obj.set_spectrumArr_fromQuery(dicts[i])
                        spectra_obj.redist_specArr("Spectrum_64band")
                        redistributed_dict = spectra_obj.specArr_ToDict()
                        for v in self.spectrum_vars:
                            if v in dicts[i].keys():
                                dicts[i][v] = redistributed_dict[v]
        # Concatenate the variables
        result = {}
        for key in ukeys:
            if key in dict2 and key in dict1:
                result[key] = ma.concatenate([dict1[key], dict2[key]])
            elif key in dict2:
                result[key] = dict2[key]
            else:
                result[key] = dict1[key]
        return result

    def __merge_archive_helper(self, cdip_nc: CDIPnc, result):
        file_start_stamp = cdip_utils.datetime_to_timestamp(
            cdip_nc.get_coverage_start()
        )
        file_end_stamp = cdip_utils.datetime_to_timestamp(cdip_nc.get_coverage_end())
        file_timespan = cdip_utils.Timespan(file_start_stamp, file_end_stamp)
        request_timespan = cdip_utils.Timespan(self.start_stamp, self.end_stamp)
        if request_timespan.overlap(file_timespan):
            cdip_nc.start_stamp = self.start_stamp
            cdip_nc.end_stamp = self.end_stamp
            cdip_nc.pub_set = self.pub_set
            cdip_nc.apply_mask = self.apply_mask
            cdip_nc.vrs = self.vrs
            tmp_result = cdip_nc.get_request()

            result = self.__aggregate_dicts(result, tmp_result)
        return result, file_start_stamp

    def __merge_xyz_helper(
        self, cdip_nc: CDIPnc, request_timespan: cdip_utils.Timespan, result: dict
    ):
        # Try the next file if it is without xyz data
        z = cdip_nc.get_var("xyzZDisplacement")
        if z is None:
            return result, self.start_stamp
        # Try the next file if start_stamp cannot be calculated
        start_stamp = cdip_nc.get_xyz_timestamp(0)
        end_stamp = cdip_nc.get_xyz_timestamp(len(z) - 1)
        if start_stamp is None:
            return result, self.start_stamp
        file_timespan = cdip_utils.Timespan(start_stamp, end_stamp)
        # Add data if request timespan overlaps data timespan
        if request_timespan.overlap(file_timespan):
            cdip_nc.start_stamp = self.start_stamp
            cdip_nc.end_stamp = self.end_stamp
            cdip_nc.pub_set = self.pub_set
            cdip_nc.apply_mask = self.apply_mask
            cdip_nc.vrs = self.vrs
            tmp_result = cdip_nc.get_request()
            result = self.__aggregate_dicts(result, tmp_result)
        return result, start_stamp

    def remove_duplicates(self, data_dict: dict) -> dict:
        """Duplicate records may exist after merge_ routines. This removes them."""
        result = {}
        keys = list(data_dict.keys())
        if len(keys) > 0:
            key = keys[0]
            prefix = self.get_var_prefix(key)
            time_dimension_name = prefix + "Time"
            time_values, indices_of_unique_values = np.unique(
                data_dict[time_dimension_name], return_index=True
            )
            result[time_dimension_name] = time_values
            for key in keys:
                if key != time_dimension_name:
                    result[key] = data_dict[key][indices_of_unique_values]
            return result
        else:
            return data_dict

    def __merge_xyz_request(self):
        """Merge xyz data from realtime and archive nc files."""
        if self.vrs and self.vrs[0] == "xyzData":
            self.vrs = ["xyzXDisplacement", "xyzYDisplacement", "xyzZDisplacement"]
        request_timespan = cdip_utils.Timespan(self.start_stamp, self.end_stamp)
        arch_file_used = False
        rt_file_used = False
        result = {}

        # First get realtime data if it exists
        rt = RealtimeXY(self.stn)
        if rt.nc is not None:
            rt_file_used = True
            result, start_stamp = self.__merge_xyz_helper(rt, request_timespan, result)

        # If the request start time is more recent than the realtime
        # start time, no need to look in the archives
        if self.start_stamp > start_stamp:
            return result

        # Second, look in archive files for data
        for dep in range(1, self.max_deployments):
            ar = Archive(self.stn, dep, self.data_dir, self.org)
            if ar.nc is None:
                break
            arch_file_used = True
            result, start_stamp = self.__merge_xyz_helper(ar, request_timespan, result)
            # Break if file start stamp is greater than request end stamp
            if start_stamp > self.end_stamp:
                break

        if rt_file_used and arch_file_used:
            result = self.remove_duplicates(result)
        return result

    def __merge_active_request(self, nc_class_type: str = "rt"):
        """
        Returns data for a given request across active datasets.

        When deploy_num is supplied all files (active and archive)
        are checked for data.
        """
        sorted_datasets = sorted(
            self.meta.active_datasets.items(), key=operator.itemgetter(1)
        )

        result = {}
        num_files_used = 0
        for ds in sorted_datasets:
            if nc_class_type == "xyz":
                a = ActiveXY(self.stn, self.deploy_num, ds[0], self.data_dir, self.org)
            else:
                a = Active(self.stn, self.deploy_num, ds[0], self.data_dir, self.org)

            if ds[0] == "moored" and a.nc is None:
                a = Archive(
                    self.stn[0:3] + "p1", self.deploy_num, self.data_dir, self.org
                )

            if a.nc is not None:
                a.vrs = self.vrs
                a.start_stamp = self.start_stamp
                a.end_stamp = self.end_stamp
                a.pub_set = self.pub_set
                a.apply_mask = self.apply_mask
                tmp_result = a.get_request()
                result = self.__aggregate_dicts(result, tmp_result)
                num_files_used += 1

        if num_files_used > 1:
            result = self.remove_duplicates(result)
        return result

    def __merge_request(self):
        """Returns data for given request across realtime and historic files"""

        num_files_used = 0
        rt = {}
        r = self.realtime
        # Note that we are assuming that waveTime will work for every time dim.
        if r.nc is not None and r.get_var("waveTime")[0] <= self.end_stamp:
            num_files_used += 1
            r.vrs = self.vrs
            r.start_stamp = self.start_stamp
            r.end_stamp = self.end_stamp
            r.pub_set = self.pub_set
            r.apply_mask = self.apply_mask
            rt = r.get_request()

        ht = {}
        h = self.historic
        # Historic file contains public data
        if (
            h.nc is not None
            and h.get_var("waveTime")[-1] >= self.start_stamp
            and self.pub_set == "public"
        ):
            num_files_used += 1
            h.vrs = self.vrs
            h.start_stamp = self.start_stamp
            h.end_stamp = self.end_stamp
            h.pub_set = self.pub_set
            h.apply_mask = self.apply_mask
            ht = h.get_request()

        result = self.__aggregate_dicts(ht, rt)

        # Check Archive files if requesting non-pub data
        if self.pub_set != "public":
            for dep in range(1, self.max_deployments):
                ar = Archive(self.stn, dep, self.data_dir, self.org)
                if ar.nc is None:
                    break
                num_files_used += 1
                result, start_stamp = self.__merge_archive_helper(ar, result)
                # Break if file start stamp is greater than request end stamp
                if start_stamp > self.end_stamp:
                    break

        if num_files_used > 1:
            result = self.remove_duplicates(result)
        return result

    def get_nc_files(self, types: list = nc_file_types) -> dict:
        """Returns dict of netCDF4 objects of a station's netcdf files"""
        result = {}
        for ftype in types:
            if ftype == "historic":
                ht = Historic(self.stn, self.data_dir, self.org)
                if ht.nc:
                    result[ht.filename] = ht.nc
            if ftype == "archive":
                for dep in range(1, self.max_deployments):
                    ar = Archive(self.stn, dep, self.data_dir, self.org)
                    if ar.nc is None:
                        break
                    result[ar.filename] = ar
            if ftype in self.meta.active_datasets:
                for dep in range(1, self.max_deployments):
                    ac = Active(self.stn, dep, ftype, self.data_dir, self.org)
                    if ac.nc is not None:
                        result[ac.filename] = ac
                    ac = ActiveXY(self.stn, dep, ftype, self.data_dir, self.org)
                    if ac.nc is not None:
                        result[ac.filename] = ac
        return result

    def get_target_timespan(
        self, target_timestamp: int, num_target_records: int, time_var: str
    ) -> tuple:
        """Returns a timespan containing the n closest records to the target_timestamp.

        PARAMETERS
        ----------
        target_timestamp : int
            A unix timestamp which is the target time about which the closest
            n records will be returned.
        n : int
            The number of records to return that are closest to the target
            timestamp.
        time_var : str
            The name of the time dimension variable to use. E.g. waveTime.

        RETURNS
        -------
        A 2-tuple of timestamps corresponding to i and i+n (where n may
        be negative) which will be the timestamps for the n records
        closest to the target_timestamp.
        """
        r_ok = False
        if self.realtime.nc is not None:
            r_ok = True
        h_ok = False
        if self.historic.nc is not None:
            h_ok = True

        # Check realtime to find closest index

        r_closest_idx = None
        if r_ok:
            r_stamps = self.realtime.get_var(time_var)[:]
            r_last_idx = len(r_stamps) - 1
            i_b = bisect_left(r_stamps, target_timestamp)
            # i_b will be possibly one more than the last index
            i_b = min(i_b, r_last_idx)
            # Target timestamp is exactly equal to a data time
            if i_b == r_last_idx or r_stamps[i_b] == target_timestamp:
                r_closest_idx = i_b
            elif i_b > 0:
                r_closest_idx = cdip_utils.get_closest_index(
                    i_b - 1, i_b, r_stamps, target_timestamp
                )

        # If closest index not found, check historic

        h_closest_idx = None
        h_last_idx = None  # Let's us know if h_stamps has been loaded
        if h_ok and not r_closest_idx:
            h_stamps = self.historic.get_var(time_var)[:]
            h_last_idx = len(h_stamps) - 1
            i_b = bisect_left(h_stamps, target_timestamp)
            i_b = min(i_b, h_last_idx)
            # Target timestamp is exactly equal to a data time
            if (i_b <= h_last_idx and h_stamps[i_b] == target_timestamp) or i_b == 0:
                h_closest_idx = i_b
            elif i_b >= h_last_idx:  # Target is between the two files
                if r_ok:
                    if abs(h_stamps[h_last_idx] - target_timestamp) < abs(
                        r_stamps[0] - target_timestamp
                    ):
                        h_closest_idx = i_b
                    else:
                        r_closest_idx = 0
                else:  # No realtime file
                    h_closest_idx = i_b
            else:  # Within middle of historic stamps
                h_closest_idx = cdip_utils.get_closest_index(
                    i_b - 1, i_b, h_stamps, target_timestamp
                )

        # Now we have the closest index, find the intervals

        if r_closest_idx is not None:
            r_interval = cdip_utils.get_interval(
                r_stamps, r_closest_idx, num_target_records
            )
            # If bound exceeded toward H and H exists, cacluate h_interval
            if r_interval[2] < 0 and h_ok:
                if not h_last_idx:
                    h_stamps = self.historic.get_var(time_var)[:]
                    h_last_idx = len(h_stamps) - 1
                h_interval = cdip_utils.get_interval(
                    h_stamps, h_last_idx, num_target_records + r_closest_idx + 1
                )
                return cdip_utils.combine_intervals(h_interval, r_interval)
            else:
                return r_interval
        elif h_closest_idx is not None:
            h_interval = cdip_utils.get_interval(
                h_stamps, h_closest_idx, num_target_records
            )
            # If bound exceeded toward R and R exists, cacluate r_interval
            if h_interval[2] > 0 and r_ok:
                r_interval = cdip_utils.get_interval(
                    r_stamps, 0, num_target_records + h_closest_idx - h_last_idx - 1
                )
                return cdip_utils.combine_intervals(h_interval, r_interval)
            else:
                return h_interval

        # If we get to here there's a problem
        return (None, None, None)
