import numpy as np
import pandas as pd
from datetime import datetime
import bisect


# CDIP imports
from cdippy.utils import utils as ut
from cdippy.stndata import StnData

import matplotlib as mpl

mpl.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402


def make_plot(stn: str, x_inch: int = None, y_inch: int = None) -> tuple:
    """
    Year-long Climatology of Sea Surface Temperature across all years of available data for a station.

    Returns a tuple containing a figure object and an error message.

    PARAMETERS
    ----------
    stn: str
        A 5-char station identifier e.g. '100p1'
    x_inch: int
        Sets the width of the plot in inches
    y_inch: int
        Sets the height of the plot in inches
    """

    def Tf(Tc):
        return (9.0 / 5.0) * Tc + 32

    # Initialize the return figure and status
    fig = None
    error_msg = None

    # Defaults are needed later for scaling the plot
    default_x_inch = 9
    default_y_inch = 5
    if x_inch is None:
        x_inch = default_x_inch
    if y_inch is None:
        y_inch = default_y_inch

    stn_data = StnData(stn)
    if stn_data.realtime.nc is None and stn_data.historic.nc is None:
        return fig, "Station does not exist"

    data = stn_data.get_series(
        datetime(1975, 1, 1), datetime.utcnow(), ["sstSeaSurfaceTemperature"]
    )
    meta = stn_data.get_stn_meta()

    buoytitle = meta["metaStationName"]

    datetimearr = [datetime.utcfromtimestamp(t) for t in data["sstTime"]]
    temparray = data["sstSeaSurfaceTemperature"]

    minval = np.min(temparray)
    maxval = np.max(temparray)

    yscale_top = np.ceil(maxval) + 1  # Degrees celsius
    yscale_bottom = np.floor(minval) - 1  # Degrees celsius

    # Check max temp against yscale_top
    if maxval > yscale_top:
        yscale_top = 40

    pdtimetemp = pd.Series(temparray, datetimearr)
    pdtimetempfill = pdtimetemp.resample("30Min", base=17)
    tempfill = pdtimetempfill.values
    timefill = pdtimetempfill.index
    timefillunix = (pd.DatetimeIndex(timefill)).astype(np.int64) / (10**9)

    yearsall = [dt.year for dt in datetimearr]
    yearsort = list(set(yearsall))
    yearsort.sort()
    # Last year of data
    curryear = yearsort[-1]
    lastyear = curryear - 1

    startidx = []
    endidx = []

    for year in yearsort:

        unixstart = ut.datetime_to_timestamp(datetime(year, 1, 1))
        ncstart = timefillunix[bisect.bisect_left(timefillunix, unixstart)]
        nearidx = np.where(timefillunix == ncstart)[0][0]

        unixend = ut.datetime_to_timestamp(datetime(year, 12, 31, 23, 59, 59))
        ncend = timefillunix[(bisect.bisect_right(timefillunix, unixend)) - 1]
        futureidx = np.where(timefillunix == ncend)[0][0]

        startidx.append(nearidx)
        endidx.append(futureidx)

    dtarr = [datetime.utcfromtimestamp(t) for t in timefillunix]
    datescop = []

    for dateobj in range(len(dtarr)):
        eachdate = dtarr[dateobj]
        yearcop = eachdate.replace(year=2012)
        datescop.append(yearcop)

    fig = plt.figure(figsize=(x_inch, y_inch))
    ax = fig.add_subplot(111)
    ax2 = ax.twinx()  # For the celsius scale
    ax2.set_ylabel("Fahrenheit")

    for idx in range(len(startidx)):
        if dtarr[startidx[idx]].year == curryear:
            ax.plot(
                datescop[startidx[idx] : endidx[idx]],
                tempfill[startidx[idx] : endidx[idx]],
                "m",
                linewidth=0.80,
            )
        elif dtarr[startidx[idx]].year == lastyear:
            ax.plot(
                datescop[startidx[idx] : endidx[idx]],
                tempfill[startidx[idx] : endidx[idx]],
                "#36CCCF",
                linewidth=0.80,
            )
        else:
            ax.plot(
                datescop[startidx[idx] : endidx[idx]],
                tempfill[startidx[idx] : endidx[idx]],
                "0.75",
                linewidth=0.55,
                alpha=0.3,
            )

    linepast = mpl.lines.Line2D([], [], color="gray")
    linelast = mpl.lines.Line2D([], [], color="c")
    linecurr = mpl.lines.Line2D([], [], color="m")

    # Set blank start and end years so a blank plot is returned if no data
    if len(yearsort) == 0:
        yearsort = ["", ""]

    pastyearrange = str(yearsort[0]) + "-" + str(lastyear - 1)

    plt.xlim(734503)
    ax.set_xticklabels(
        [
            "Jan",
            "Feb",
            "Mar",
            "Apr",
            "May",
            "Jun",
            "Jul",
            "Aug",
            "Sep",
            "Oct",
            "Nov",
            "Dec",
        ]
    )

    # Adjust font sizes appropriately (if r=2 suptitle=30,title=22,ylabel=22,xlabel=18)
    font_r = x_inch / default_x_inch
    sup_f = font_r * 15
    title_f = font_r * 11
    ylab_f = font_r * 11
    plt.suptitle(buoytitle, fontsize=sup_f, y=0.99)
    plt.title(
        "Sea Surface Temperature " + str(yearsort[0]) + "-" + str(yearsort[-1]),
        fontsize=title_f,
        y=1.01,
    )
    ax.set_ylabel("Temperature (Celsius)", fontsize=ylab_f)
    ax.get_yaxis().labelpad = 15
    ax.tick_params(axis="y", which="major", labelsize=14, right="off")
    ax.tick_params(axis="x", which="major", labelsize=14, top="off")

    # Set absolute axes
    ax2.set_ylim(Tf(yscale_bottom), Tf(yscale_top))
    ax.set_ylim(yscale_bottom, yscale_top)

    # Monkey around a bit to make sure the legend does not cover data
    lns = (linepast, linelast, linecurr)
    lbs = (pastyearrange, lastyear, curryear)
    frm_a = 0.2
    if minval > 10:
        ax.legend(lns, lbs, framealpha=frm_a, loc="lower right")
    elif maxval < 25:
        ax.legend(lns, lbs, framealpha=frm_a, loc="upper right")
    else:  # Put legend at bottom around Aug otherwise
        ax.legend(
            lns, lbs, framealpha=frm_a, loc="lower left", bbox_to_anchor=(0.55, 0.01)
        )

    return fig, error_msg
