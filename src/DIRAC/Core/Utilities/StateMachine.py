"""Backward compatibility wrapper - moved to DIRACCommon

This module has been moved to DIRACCommon.Core.Utilities.StateMachine to avoid
circular dependencies and allow DiracX to use these utilities without
triggering DIRAC's global state initialization.

All exports are maintained for backward compatibility.
"""
# Re-export everything from DIRACCommon for backward compatibility
from DIRACCommon.Core.Utilities.StateMachine import *  # noqa: F401, F403

from DIRAC import gLogger


class StateMachine(StateMachine):  # noqa: F405 pylint: disable=function-redefined
    """Backward compatibility wrapper - moved to DIRACCommon"""

    def setState(self, candidateState, noWarn=False):
        return super().setState(candidateState, noWarn, logger_warn=gLogger.warn)
