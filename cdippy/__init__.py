# import public top-level modules
from . import cdipnc, nchashes, ncstats, ndbc, plotting, spectra, stndata

# import plots library for backward compatibility
from . import plots as plots  # noqa: F401

# public API (i.e. "from cdippy import *")
__all__ = ["cdipnc", "nchashes", "ncstats", "ndbc", "plotting", "spectra", "stndata"]
