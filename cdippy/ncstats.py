import pandas as pd
from datetime import datetime, timezone
from cdippy.stndata import StnData


class NcStats(StnData):
    """For a given station, produces data availability.

    There are methods to return counts for the entire station record to be
    used diretly by a web app, and there are methods to save to disk availabililty
    counts (e.g. xyz counts) for individual nc files. In that case updates
    to totals would be calculated by re-summarizing any files that have changed
    and adding up all the files to produce new totals.
    """

    QC_flags = ["waveFlagPrimary", "sstFlagPrimary", "gpsStatusFlags"]

    def __init__(self, stn: str, data_dir: str = None):
        """
        PARAMETERS: See StnData
        """

        StnData.__init__(self, stn, data_dir)

        self.date_modifieds = {}
        self.start = datetime.strptime("1975-01-01 00:00:00", "%Y-%m-%d %H:%M:%S")
        self.end = datetime.now(timezone.utc)
        self.pub_set = "all"

    def make_stats(self) -> dict:
        """Returns various statistics off the given station."""
        result = {}
        result["flag_counts"] = self.flag_counts()
        result["deployments"] = self.deployment_summary()
        return result

    def deployment_summary(self) -> dict:
        """Returns deployment summary statistics."""
        self.load_nc_files()
        result = {}
        dep_cnt = 0
        for nc_name in self.nc_files:
            dep = nc_name[-6:-3]
            if dep[0:1] == "d":
                dep_cnt += 1
                result[dep] = {}
                result[dep]["time_coverage_start"] = self.nc_files[
                    nc_name
                ].get_coverage_start()
                result[dep]["time_coverage_end"] = self.nc_files[
                    nc_name
                ].get_coverage_end()
        result["number_of_deployments"] = dep_cnt
        return result

    def load_nc_files(self, types: list = ["realtime", "historic", "archive"]) -> dict:
        """Returns netCDF4 objects of a station's netcdf files"""
        self.nc_files = self.get_nc_files(types)

    def load_file(self, nc_filename: str):
        """Sets self.nc for a given nc_filename"""
        if nc_filename in self.nc_files:
            self.nc = self.nc_files[nc_filename]
        else:
            self.nc = self.get_nc(self.filename_to_url(nc_filename))

    def load_date_modifieds(self):
        pass

    def store_date_modified(self):
        pass

    def nc_file_summaries(self) -> dict:
        self.load_nc_files()
        result = {}
        for nc_name in self.nc_files:
            result[nc_name] = self.nc_file_summary(nc_name)
        return result

    def nc_file_summary(self, nc_filename: str) -> dict:
        """Returns statistical summaries given an nc file name."""
        if self.nc is None:
            self.load_file(nc_filename)
        result = {}
        # - Currently have just one summary
        result["flag_counts"] = self.flag_counts()
        return result

    def flag_counts(self, QC_flags: list = None) -> dict:
        """Returns pandas dataframe of counts of flag variables for the entire station record."""
        result = {"totals": {}, "by_month": {}}
        if not QC_flags:
            QC_flags = self.QC_flags
        for flag_name in QC_flags:
            dim = self.meta.get_var_prefix(flag_name)
            self.data = self.get_series(self.start, self.end, [flag_name], self.pub_set)
            cat_var = self.make_categorical_flag_var(flag_name)
            result["totals"][flag_name] = self.total_count(cat_var)
            result["by_month"][flag_name] = self.by_month_count(cat_var, dim)
        return result

    def total_count(self, cat_var) -> pd.DataFrame:
        """Returns count totals for a given flag variable."""
        return pd.DataFrame({"cnt": cat_var}).groupby(cat_var).count()

    def by_month_count(self, cat_var, dim: str) -> pd.DataFrame:
        """Returns pandas dataframe of Counts by month for a given flag variable."""
        df = pd.DataFrame(
            {"cnt": cat_var}, index=pd.to_datetime(self.data[dim + "Time"], unit="s")
        )
        mon_map = df.index.map(lambda x: str(x.year) + str("{:02d}".format(x.month)))
        return df.groupby([mon_map, cat_var]).count().fillna(0).astype(int)

    def make_categorical_flag_var(self, flag_name: str):
        cat = pd.Categorical(
            self.data[flag_name], categories=self.meta.get_flag_values(flag_name)
        )
        return cat.rename_categories(self.meta.get_flag_meanings(flag_name))
