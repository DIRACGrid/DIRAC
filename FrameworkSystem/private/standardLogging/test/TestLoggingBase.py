"""
Test Logger Wrapper
"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

__RCSID__ = "$Id$"

import unittest
import logging
from StringIO import StringIO

from DIRAC.FrameworkSystem.private.standardLogging.LoggingRoot import LoggingRoot
from DIRAC.FrameworkSystem.private.standardLogging.Logging import Logging


gLogger = LoggingRoot()


def cleaningLog(log):
  """
  Remove date and space from the log string
  """
  log = log[20:]
  log = log.replace(" ", "")
  return log


class Test_Logging(unittest.TestCase):
  """
  Test get and set levels.
  """

  def setUp(self):
    """
    Initialize at debug level with a sublogger and a special handler
    """
    # Reinitialize the system/component name after other tests
    # because LoggingRoot is a singleton and can not be reinstancied
    Logging._componentName = 'Framework'

    gLogger.setLevel('debug')
    self.log = gLogger.getSubLogger('log')
    self.buffer = StringIO()

    gLogger.showHeaders(True)
    gLogger.showThreadIDs(False)

    # modify the output to capture the log into a buffer
    if logging.getLogger('dirac').handlers:
      logging.getLogger('dirac').handlers[0].stream = self.buffer

    # reset the levels
    logging.getLogger('dirac').getChild('log').setLevel(logging.NOTSET)
    self.log._levelModified = False
