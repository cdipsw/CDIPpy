from datetime import datetime, timedelta, timezone
from bisect import bisect_left

from cdippy.cdipnc import CDIPnc
import cdippy.utils.utils as cu


class MopData(CDIPnc):
    """Returns data and metadata for the specified MOP point.

    This class has a consistent interface with
    the StnData class for accessing CDIP buoy data.

    METHODS
    -------
    get_series(start, end, vrs)
        Returns data for a station given start date, end date and a
        list of variables.
    get_parameters(start, end)
        Calls get_series with vrs set to parameter variables.
    get_spectra(start, end)
        Calls get_series with vrs set to spectrum variables.
    get_mop_meta
        Returns all mop meta variables.
    get_target_times
        Returns a 2-tuple of timestamps, an interval corresponding
        to  n records to the right or left of target_timestamp.
    """

    dataset_names = ["nowcast", "forecast", "hindcast", "ecmwf_fc"]
    alongshore_prefixes = [
        "D0",
        "D1",
        "OC",
        "L0",
        "L1",
        "VE",
        "B0",
        "B1",
        "SL",
        "MO",
        "SC",
        "SM",
        "SF",
        "MA",
        "SN",
        "M0",
        "M1",
        "HU",
        "DN",
    ]
    validation_prefixes = ["BP"]

    # Commonly requested sets of variables
    parameter_vars = ["waveHs", "waveTp", "waveDp", "waveTa"]
    gps_vars = ["gpsLatitude", "gpsLongitude", "gpsStatusFlags"]
    spectrum_vars = [
        "waveEnergyDensity",
        "waveMeanDirection",
        "waveA1Value",
        "waveB1Value",
        "waveA2Value",
        "waveB2Value",
        "waveCheckFactor",
        "waveHs",
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

    vrs = None

    def __init__(self, mop_id: str, dataset_name: str, data_dir: str = None):
        """
        PARAMETERS
        ----------
        mop_id : str
           5 char format - 2 char prefix followed by 3 digit id. E.g. BP092
        dataset_name : str
            One of nowcast, forecast, hindcast, ecmwf_fc
        data_dir : str [optional]
            Either a full path to a directory containing a local directory hierarchy
            of nc files. E.g. '/project/WNC' or a url to a THREDDS server.


        For MOP single point output.

        There are four primary dataset labels or types:
            nowcast - buoy-driven, long-term, updated by realtime cron
            hindcast - buoy-driven, long-term, updated very infrequently
            forecast - WW3-driven forecast, short-term, created by realtime cron
            ecmwf_fc - ECMWF-driven forecast, short-term, created by realtime cron

         The location of all four of these types depends on the first two characters
         of the mop ID.

         Alongshore points: located in MODELS/MOP_alongshore
             Prefixes:
                 'D0', 'D1', 'OC', 'L0', 'L1', 'VE', 'B0', 'B1', 'SL', 'MO',
                 'SC', 'SM', 'SF', 'MA', 'SN', 'M0', 'M1', 'HU', 'DN'

         Validation points: located in MODELS/MOP_validation
             Prefixes: 'BP'

         All other points/prefixes: MODELS/misc

         Datasets can then be found under WNC_DATA as {PATH}/{MOPID}_{LABEL}.nc, e.g.
         WNC_DATA/MODELS/MOP_validation/BP100_ecmwf_fc.nc
        """

        self.data_dir = data_dir
        self.dataset_name = dataset_name
        self.mop_id = mop_id
        self.nc = None
        self.url = None

        prefix = mop_id[0:2]
        MOP_type = (
            "MOP_validation" if prefix in self.validation_prefixes else "MOP_alongshore"
        )

        self.filename = "_".join([self.mop_id, self.dataset_name + ".nc"])

        # Allowing data_dir to be either url or path
        __using_path = False
        if self.data_dir:
            if self.data_dir[0:4] == "http":
                self.THREDDS_url = self.data_dir
            else:
                __using_path = True

        if __using_path:
            self.url = "/".join([self.data_dir, "MODELS", MOP_type, self.filename])
        else:
            self.url = "/".join(
                [self.THREDDS_url, "thredds/dodsC/cdip/model", MOP_type, self.filename]
            )

        self.nc = self.get_nc()

    def get_mop_meta(self) -> dict:
        """Returns a dict of mop meta data."""
        result = {}
        self.set_request_info(vrs=self.meta_vars)
        result = self.get_request()
        for attr_name in self.meta_attributes:
            if hasattr(self.nc, attr_name):
                result[attr_name] = getattr(self.nc, attr_name)
        return result

    def get_parameters(
        self,
        start: datetime = None,
        end: datetime = None,
        apply_mask=True,
        target_records=0,
    ) -> dict:
        """Calls get_series to return wave parameters."""
        return self.get_series(
            start, end, self.parameter_vars, apply_mask, target_records
        )

    def get_spectra(
        self,
        start: datetime = None,
        end: datetime = None,
        apply_mask: bool = True,
        target_records: int = 0,
    ) -> dict:
        """Calls get_series to return spectral data."""
        return self.get_series(
            start, end, self.spectrum_vars, apply_mask, target_records
        )

    def get_series(
        self,
        start: datetime = None,
        end: datetime = None,
        vrs: list = None,
        pub_set: str = "public",
        apply_mask: bool = None,
        target_records: int = 0,
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
        apply_mask: bool [optional] default True
            Removes values from the masked array that have a mask value of True.
            Ex. If nonpub data is requested and apply_mask is False, the returned
            array will contain both public and nonpublic data (although public
            data records will have the mask value set to True). If apply_mask
            is set to True, only nonpub records will be returned.
        target_records: int [optional]
            If start is specified and end is None, this will specify the number
            of additional records to return closest to start.
        """
        if vrs is None:
            vrs = self.parameter_vars
        prefix = self.get_var_prefix(vrs[0])

        if start is not None and end is None:  # Target time
            if isinstance(start, str):
                start = datetime.strptime(start, "%Y-%m-%d %H:%M:%S").replace(
                    tzinfo=timezone.utc
                )
            ts_I = self.get_target_timespan(
                cu.datetime_to_timestamp(start), target_records, prefix + "Time"
            )
            if ts_I[0] is not None:
                start = cu.timestamp_to_datetime(ts_I[0])
                end = cu.timestamp_to_datetime(ts_I[1])
            else:
                return None
        elif start is None:  # Use default 3 days back
            start = datetime.utcnow() - timedelta(days=3)
            end = datetime.utcnow()

        if apply_mask is None:
            apply_mask = self.apply_mask

        self.set_request_info(start, end, vrs, pub_set, apply_mask)

        return self.get_request()

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
        A 3-tuple of timestamps corresponding to i and i+n (where n may
        be negative) which will be the timestamps for the n records
        closest to the target_timestamp.
        """

        stamps = self.get_var(time_var)[:]
        last_idx = len(stamps) - 1
        i_b = bisect_left(stamps, target_timestamp)
        # i_b will be possibly one more than the last index
        i_b = min(i_b, last_idx)
        # Target timestamp is exactly equal to a data time
        closest_idx = None
        if i_b == last_idx or stamps[i_b] == target_timestamp:
            closest_idx = i_b
        elif i_b > 0:
            closest_idx = cu.get_closest_index(i_b - 1, i_b, stamps, target_timestamp)

        # Now we have the closest index, find the intervals

        if closest_idx is not None:
            interval = cu.get_interval(stamps, closest_idx, num_target_records)
            return interval

        # If we get to here there's a problem
        return (None, None, None)
