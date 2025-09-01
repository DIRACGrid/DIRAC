"""Backward compatibility wrapper - moved to DIRACCommon

This module has been moved to DIRACCommon.Core.Utilities.ReturnValues to avoid
circular dependencies and allow DiracX to use these utilities without
triggering DIRAC's global state initialization.

All exports are maintained for backward compatibility.
"""
# Re-export everything from DIRACCommon for backward compatibility
from DIRACCommon.Core.Utilities.ReturnValues import *  # noqa: F401, F403
