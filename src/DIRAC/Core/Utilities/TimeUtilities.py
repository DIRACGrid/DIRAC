"""Backward compatibility wrapper - moved to DIRACCommon

This module has been moved to DIRACCommon.Core.Utilities.TimeUtilities to avoid
circular dependencies and allow DiracX to use these utilities without
triggering DIRAC's global state initialization.

All exports are maintained for backward compatibility.
"""
from functools import partial

# Re-export everything from DIRACCommon for backward compatibility
from DIRACCommon.Core.Utilities.TimeUtilities import *  # noqa: F401, F403

# TimeUtilities imports datetime as well
# To avoid conflicts/surprises, import copies here used a _local
from datetime import datetime as dt_local, timezone as tz_local

from DIRAC import gLogger


timeThis = partial(timeThis, logger_info=gLogger.info)


class DiracTime:
    """datetime tools for DIRAC:
    There are a number of facilities in DIRAC which can't handle timezone aware
    dateetime objects: These don't serialise, don't fit in database schemas, etc.
    As the python non-timezone aware helper functions are being deprecated we
    provider our own equivalents here.
    """

    @staticmethod
    def utcnow() -> dt_local:
        """Returns a UTC datetime object for now *without* a timezone field."""
        return dt_local.now(tz_local.utc).replace(tzinfo=None)

    @staticmethod
    def utcfromtimestamp(epoch: float) -> dt_local:
        """Returns a UTC datetime object from the given epoch offset (also in UTC)."""
        return dt_local.fromtimestamp(epoch, tz=tz_local.utc).replace(tzinfo=None)

    @staticmethod
    def timestamp_utc(dt: dt_local) -> int:
        """Converts a datetime object to a UTC epoch offset int.
        Naive timezones are assumed to be UTC.
        """
        if dt.tzinfo is None:
            return int(dt.replace(tzinfo=tz_local.utc).timestamp())
        return int(dt.timestamp())
