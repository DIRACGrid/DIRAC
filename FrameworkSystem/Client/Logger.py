# $HeadURL$
__RCSID__ = "$Id$"
from DIRAC.FrameworkSystem.private.logging.Logger import Logger
from DIRAC.FrameworkSystem.private.standardLogging.LoggingRoot import LoggingRoot

# old logging system
gLogger = Logger()

# To update the logging system, you have to uncomment this line:
#gLogger = LoggingRoot()


def getLogger():
  return gLogger
