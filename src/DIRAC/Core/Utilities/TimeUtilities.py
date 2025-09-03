"""Backward compatibility wrapper - moved to DIRACCommon

This module has been moved to DIRACCommon.Core.Utilities.TimeUtilities to avoid
circular dependencies and allow DiracX to use these utilities without
triggering DIRAC's global state initialization.

All exports are maintained for backward compatibility.
"""
from functools import partial

# Re-export everything from DIRACCommon for backward compatibility
from DIRACCommon.Core.Utilities.TimeUtilities import *  # noqa: F401, F403

from DIRAC import gLogger


timeThis = partial(timeThis, logger_info=gLogger.info)
