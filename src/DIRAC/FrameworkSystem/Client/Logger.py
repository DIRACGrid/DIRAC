__RCSID__ = "$Id$"

from DIRAC.FrameworkSystem.private.standardLogging.LoggingRoot import LoggingRoot

gLogger = LoggingRoot()


def getLogger():
    return gLogger
