from . import plots
from matplotlib.pyplot import Figure


def make_annual_hs_boxplot(stn: str, year: int) -> Figure:
    """
    Create a boxplot of annual significant wave heights for a station.

    Args:
        stn (str): A 5-char station identifier, e.g. '100p1'.
        year (int): The year to plot.

    Returns:
        fig (Figure): A matplotlib.pyplot.Figure object for the created plot.
    """

    return plots.annual_hs_boxplot.make_plot(stn, year)


def make_compendium_plot(
    stns: str, start: str, end: str, params: str, x_inch: int
) -> Figure:
    """CDIP's classic compendium plot for multiple stations and parameters.

    Args:
        stns (str): A comma-delimited list of 5-char station identifiers, e.g. '100p1,201p1'.
        start (str): Start time of data series formatted as 'yyyymm[ddHHMMss]' where 'ddHHMMss' are optional components.
        end (str): End time of data series ('yyyymm[ddHHMMss]') If 'None' is provided, defaults to the current date and time.
        params (str): A comma-delimited string of parameter names, e.g. 'waveHs,waveTp'.

    Returns:
        fig (Figure): A matplotlib.pyplot.Figure object for the created plot.

    """

    return plots.compendium.make_plot(stns, start, end, params, x_inch)


def make_sst_climatology_plot(
    stn: str, x_inch: int = None, y_inch: int = None
) -> Figure:
    """
    Create a plot of yearly climatology of sea surface temperature at a station for all years of available data.

    Args:
        stn (str): A 5-char station identifier, e.g. '100p1'.

    Returns:
        fig (Figure): A matplotlib.pyplot.Figure object for the created plot.
    """

    return plots.sst_climatology.make_plot(stn, x_inch, y_inch)
