from DIRAC.FrameworkSystem.private.standardLogging.LoggingContext import contextLogger, setContextLogger
from DIRAC.FrameworkSystem.private.standardLogging.LoggingRoot import LoggingRoot

gLogger = LoggingRoot()


def getLogger():
    return gLogger


__all__ = ["contextLogger", "setContextLogger", "getLogger"]
