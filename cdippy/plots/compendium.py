import cdippy.utils.utils as ut
import cdippy.plots.utils as pu
from datetime import datetime
import numpy as np
import calendar
import matplotlib.dates as mdates

# CDIP imports
from cdippy.stndata import StnData

import matplotlib as mpl

mpl.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402


def make_plot(
    stns: str, start: datetime, end: datetime, params: str, x_inch: int = None
) -> tuple:
    """CDIP's classic compendium plot for multiple stations and parameters.

    Returns a tuple containing a figure object and an error message.

    PARAMETERS
    ----------
    stns: str
        A comma-delimited list of 5-char station identifiers e.g. '100p1,201p1'
    start: str | datetime
        Start time of data series. If start is a string must be in cdip format, e.g. 20200420040201
        with the day ... seconds portion optional.
    end: str | datetime
        End time of data series. Can be either cdip format or datetime. If not set
        defaults to the current date and time.
    params: str
        A comma-delimited string of parameter names, e.g. 'waveHs,waveTp'
    x_inch: int
        Width in inches of the resultant plot

    """

    # Figure settings
    default_x_inch = 15
    aspect_ratio = 1.0  # x/y

    # Font sizes
    default_title_font_size = 26
    default_subtitle_font_size = 18
    default_label_font_size = 18
    default_axtitle_font_size = 20
    default_datelabel_font_size = 14

    # Set these so we can return None if problem
    fig = None
    error_msg = None

    multiple_stns = False
    if stns is None:
        return fig, "Error: stn not set"
    else:
        stns = stns.split(",")
        num_stns = len(stns)
        if num_stns > 1:
            multiple_stns = True

    if start is None:
        return fig, "Error: start is not set"

    if type(start) is str:
        start = ut.cdip_datetime(start)

    month_plot = False
    if end is None:  # Month compendium plot
        start = datetime(start.year, start.month, 1, 0, 0, 0)
        end = datetime(
            start.year,
            start.month,
            calendar.monthrange(start.year, start.month)[1],
            23,
            59,
            59,
        )
        month_plot = True
    elif type(end) is str:
        end = ut.cdip_datetime(end)

    if params is None:
        params = "waveHs,waveTp,waveDp"
    params = params.split(",")

    if x_inch is None:
        x_inch = default_x_inch
    x_inch = int(x_inch)
    y_inch = x_inch / aspect_ratio

    # Scale the fonts
    font_scale = x_inch / default_x_inch
    title_font_size = round(font_scale * default_title_font_size, 0)
    subtitle_font_size = round(font_scale * default_subtitle_font_size, 0)
    label_font_size = round(font_scale * default_label_font_size, 0)
    axtitle_font_size = round(font_scale * default_axtitle_font_size, 0)
    datelabel_font_size = round(font_scale * default_datelabel_font_size, 0)

    # Create figure subplots
    fig, pm_axes = plt.subplots(len(params), 1, sharex=True, figsize=(x_inch, y_inch))

    # This pushes the axes down so the buoy title can be prominent
    title_vertical = 0.03
    top = 0.90 - title_vertical * (num_stns - 1)
    fig.subplots_adjust(top=top)

    # Check if there is only one parameter, to generalize the looping
    if type(pm_axes) is not np.ndarray:
        pm_axes = (pm_axes,)

    month_name = calendar.month_name[start.month]

    title_shift = 0
    hs_ylim_max = 0
    stn_idx = 0
    for stn in stns:

        stn_data = StnData(stn)

        # Get the color for the station (circular list)
        if multiple_stns:
            stn_color = pu.stn_colors[stn_idx]
        else:
            stn_color = "k"
        stn_idx += 1

        # Get stn meta data, continue to next station if not there
        try:
            meta = stn_data.get_stn_meta()
        except Exception:
            continue

        # Plot figure title
        buoytitle = meta["metaStationName"]
        plt.figtext(
            0.5,
            0.99 - title_shift,
            buoytitle,
            fontsize=title_font_size,
            color=stn_color,
            ha="center",
            va="top",
        )
        title_shift += title_vertical

        data = stn_data.get_series(start, end, params)

        # Data may be empty, continue to next station if not there
        if not data:
            continue

        # Prepare data to show gaps where there is no data
        index_name = "waveTime"
        data = pu.prepare_gaps_dict(data, index_name)

        # Plot the processed data.
        if len(data) > 0:
            wT = [ut.timestamp_to_datetime(x) for x in data["waveTime"]]
            for idx in range(len(params)):
                attr = pu.pm_data[params[idx]]
                ax = pm_axes[idx]
                if multiple_stns:
                    plot_color = stn_color
                else:
                    plot_color = attr["color"]
                ax.plot(
                    wT,
                    data[params[idx]],
                    attr["linestyle"],
                    marker=attr["marker"],
                    color=plot_color,
                    zorder=3,
                )
                if month_plot:
                    ax.set_xlim(start, end)
                ax.set_title(attr["title"], fontsize=axtitle_font_size)

        # Find Hs max
        if "waveHs" in data.keys():
            mx = np.nanmax(data["waveHs"])
            if mx > hs_ylim_max:
                hs_ylim_max = mx

    # Set x axis attributes
    days = mdates.DayLocator(interval=5)
    if month_plot:
        daysFmt = mdates.DateFormatter("%d")
        pm_axes[-1].xaxis.set_major_formatter(daysFmt)
        pm_axes[-1].xaxis.set_major_locator(days)
        plt.xlabel("Day of Month (UTC)", fontsize=label_font_size)
    elif data:
        date_fmt = mdates.DateFormatter("%Y/%m/%d\n%H:%M")
        pm_axes[-1].xaxis.set_major_formatter(date_fmt)
        pm_axes[-1].xaxis.set_minor_locator(days)

    # Adjust ticks, ylim, and labels to our liking
    plt.minorticks_on()
    for idx in range(len(params)):
        ax = pm_axes[idx]
        attr = pu.pm_data[params[idx]]
        # Grids
        ax.grid(
            axis="x", which="major", color=pu.grid_color, linestyle="-", linewidth=2
        )
        ax.grid(axis="y", which="major", color=pu.grid_color, linestyle="-")
        # Ticks
        ax.tick_params(axis="x", which="minor", length=4, top="off")
        ax.tick_params(
            axis="x", which="major", width=2, top="off", labelsize=datelabel_font_size
        )
        ax.tick_params(axis="y", which="major", labelsize=label_font_size)
        if "yticks" in attr.keys():
            ax.set_yticks(attr["yticks"])
        # Labels
        ax.set_ylabel(attr["ylabel"], fontsize=label_font_size)
        if params[idx] != "waveHs":
            ax.set_ylim(attr["ylim"][0], attr["ylim"][1])

    # Adjust the last axis
    pm_axes[-1].tick_params(axis="y", which="minor", left="off", right="off")

    # Set hs ylim if less than standard ylim, otherwise let matplotlib set it.
    # Note waveHs may not be in current stn data, hence check if in params.
    if "waveHs" in params:
        hs_ax = pm_axes[params.index("waveHs")]
        low = pu.pm_data["waveHs"]["ylim"][0]
        high = pu.pm_data["waveHs"]["ylim"][1]
        if hs_ylim_max < high:
            hs_ax.set_ylim(low, high)
        # Add second Hs axes
        attr = pu.pm_data["waveHs"]
        pHs2 = hs_ax.twinx()
        pHs2.set_ylabel(attr["ylabel_ft"], fontsize=label_font_size)
        pHs2.tick_params(axis="y", which="major", labelsize=label_font_size)
        hs_ylim = hs_ax.get_ylim()
        pHs2.set_ylim(hs_ylim[0], hs_ylim[1] * 3.28084)

    if "waveDp" in params:
        dp_ax = pm_axes[params.index("waveDp")]
        attr = pu.pm_data["waveDp"]
        pDp2 = dp_ax.twinx()
        pDp2.set_ylabel(attr["ylabel_compass"], fontsize=label_font_size)
        pDp2.tick_params(axis="y", which="major", labelsize=label_font_size)
        pDp2.set_yticks(attr["yticks"])
        pDp2.set_yticklabels(attr["compass_points"])
        pDp2.set_ylim(attr["ylim"][0], attr["ylim"][1])

    y = 0.99 - title_vertical * num_stns
    if month_plot:
        plt.figtext(
            0.5,
            y,
            month_name + " " + str(start.year),
            fontsize=subtitle_font_size,
            ha="center",
            va="top",
        )
    elif "waveTime" in data.keys():
        fmt = "%Y/%m/%d %H:%M:%S"
        plt.figtext(
            0.5,
            y,
            " - ".join([wT[0].strftime(fmt), wT[-1].strftime(fmt)]),
            None,
            ha="center",
            fontsize=subtitle_font_size,
            va="top",
        )
    else:
        fmt = "%Y/%m/%d %H:%M:%S"
        plt.figtext(
            0.5,
            y,
            " - ".join([start.strftime(fmt), end.strftime(fmt)]),
            None,
            ha="center",
            fontsize=subtitle_font_size,
            va="top",
        )

    return fig, error_msg
