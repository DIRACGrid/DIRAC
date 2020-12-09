from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
__RCSID__ = "$Id$"

from DIRAC.FrameworkSystem.private.standardLogging.LoggingRoot import LoggingRoot

gLogger = LoggingRoot()


def getLogger():
  return gLogger
