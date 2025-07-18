"""Utilities to help with CDIP plotting"""

import numpy as np
import pandas as pd

# Figure settings
default_x_inch = 15
aspect_ratio = 1.0  # x/y

# Colors
stn_colors = ["blue", "red", "green", "cyan", "magenta", "brown", "coral"]
grid_color = "0.8"

# Font sizes
default_title_font_size = 26
default_subtitle_font_size = 18
default_label_font_size = 18
default_axtitle_font_size = 20
default_datelabel_font_size = 15

# Parameter information
pm_data = {
    "waveHs": {
        "title": "Significant wave height",
        "ylabel": "Hs (m)",
        "ylabel_ft": "Hs (ft)",
        "ylim": (0, 8),
        "ylim_ft": (0, 8 * 3.28084),
        "marker": "",
        "linestyle": "-",
        "color": "blue",
    },
    "waveTp": {
        "title": "Peak wave period",
        "ylabel": "Tp (s)",
        "ylim": (0, 28),
        "marker": ".",
        "linestyle": " ",
        "color": "green",
    },
    "waveDp": {
        "title": "Wave direction at peak period",
        "ylabel": "Dp (degT)",
        "ylabel_compass": "Compass points (waves from ...)",
        "ylim": (0, 360),
        "yticks": np.arange(0, 361, 45),
        "compass_points": ["N", "NE", "E", "SE", "S", "SW", "W", "NW", "N"],
        "marker": "+",
        "linestyle": " ",
        "color": "red",
    },
    "waveTa": {
        "title": "Average wave period",
        "ylabel": "Ta (s)",
        "ylim": (0, 28),
        "marker": "",
        "linestyle": "-",
        "color": "magenta",
    },
}


def prepare_gaps_df(df: pd.DataFrame, max_timegap_secs: int = 21600) -> pd.DataFrame:
    """Prepares a dataframe so that line plots are not continuous where there are time gaps in the data."""
    # Make a copy of the original dataframe
    df2 = df.copy()
    # Identify the time gaps and specify there is a gap at the start
    df2 = df2[np.insert(np.ediff1d(df2.index) > max_timegap_secs, 0, True)]
    # This sets the records to be inserted into the dataframe to be one second before the end of each gap
    df2.index -= 1
    # Set the values of the variables to be added to NaN's
    df2.loc[:, :] = np.nan
    # df_new = df.append(df2)
    return df.append(df2).sort_index()


def prepare_gaps_dict(data, index_name: str, max_timegap_secs: int = 21600) -> dict:
    """Stn data returns a dictionary. This will prepare that for gaps"""
    df = pd.DataFrame(data, index=data[index_name])
    df = prepare_gaps_df(df, max_timegap_secs)
    new_data = {}
    for key in data:
        if key == index_name:
            new_data[key] = df.index.values
        else:
            new_data[key] = df[key].values
    return new_data
