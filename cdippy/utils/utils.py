import os
import errno
import pickle as pkl
import calendar
from datetime import datetime
import pytz

# File utils


def mkdir_p(path: str):
    try:
        os.makedirs(path)
    except OSError as exc:  # Python >= 2.5
        if exc.errno == errno.EEXIST and os.path.isdir(path):
            pass
        else:
            raise


def cdip_open(path: str, mode: str = "w", buffer_bytes: int = 4096):
    dir = os.path.dirname(path)
    if mode == "w" or mode == "a":
        mkdir_p(dir)
    try:
        f = open(path, mode, buffer_bytes)
    except Exception:
        return None
    else:
        return f


# Pickle utils


def pkl_load(fl: str):
    try:
        with open(fl, "rb") as f:
            return pkl.load(f)
    except Exception:
        return None


def pkl_dump(obj, fl: str):
    try:
        with open(fl, "wb") as f:
            pkl.dump(obj, f, -1)
    except Exception:
        pass


# Time utils


def cdip_datestring(datetime_obj: datetime) -> str:
    return datetime_obj.strftime("%Y%m%d%H%M%S")


def cdip_datetime(cdip_str: str) -> datetime:
    str_len = len(cdip_str)
    if str_len > 14 or str_len < 4 or str_len % 2 != 0:
        return None
    full_fmt = ["%Y", "%m", "%d", "%H", "%M", "%S"]
    ixs = [14, 12, 10, 8, 6, 4]
    cnt = 6
    for ix in ixs:
        if str_len >= ix:
            return datetime.strptime(cdip_str, "".join(full_fmt[0:cnt]))
        cnt -= 1
    return None


def datetime_to_timestamp(dt: datetime) -> int:
    return calendar.timegm(dt.utctimetuple())


def timestamp_to_datetime(ts: int) -> datetime:
    return datetime.fromtimestamp(ts)


def datetime_to_format(dt: datetime, format: str = None) -> str:
    if format is None:
        return cdip_datestring(dt)
    return dt.strftime(format)


def datetime_to_tz(dt: datetime, tzname: str = "US/Pacific") -> datetime:
    """Returns a non-localized utc datetime object as tzname timezone datetime object."""
    utc = pytz.utc
    utc_dt = utc.localize(dt)
    tz = pytz.timezone(tzname)
    return utc_dt.astimezone(tz)


# Utils for srrays of timestamps


def get_closest_index(i1: int, i2: int, T: list, t0: int) -> int:
    """Returns the index i such that abs(T[i]-t0) is minimum for i1,i2"""
    if abs(T[i1] - t0) <= abs(T[i2] - t0):
        return i1
    else:
        return i2


def get_interval(T: list, i: int, n: int) -> tuple:
    """
    Returns a 3-tuple (T[i], T[i+n], bounds_exceeded) where -inf < n < inf
    and bounds_exceeded is -1 or +1 if exceeded left or right respectively.
    """
    bounds_exceeded = 0
    last_T_idx = len(T) - 1

    if i + n > last_T_idx:
        bounds_exceeded = 1
    elif i + n < 0:
        bounds_exceeded = -1

    if n >= 0:
        return (T[i], T[min(i + n, last_T_idx)], bounds_exceeded)
    else:
        return (T[max(0, i + n)], T[i], bounds_exceeded)


def combine_intervals(I1: tuple, I2: tuple) -> tuple:
    """Return a 2-tuple of timestamps (I1[0], I2[1])."""
    return (I1[0], I2[1])


# Timespan
class Timespan:
    """Class to handle timespans."""

    def __init__(self, start_dt, end_dt):
        self.start_dt = start_dt
        self.end_dt = end_dt

    def overlap(self, tspan):
        """If supplied timespan overlaps this timespan, returns True."""
        if self.start_dt <= tspan.end_dt and self.end_dt >= tspan.start_dt:
            return True
        else:
            return False
