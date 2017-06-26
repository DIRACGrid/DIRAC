"""
Test Logger Wrapper
"""

__RCSID__ = "$Id$"

#pylint: disable=invalid-name

import unittest
import logging
import sys
from StringIO import StringIO

from DIRAC.FrameworkSystem.private.logging.Logger import Logger
from DIRAC.FrameworkSystem.private.standardLogging.LoggingRoot import LoggingRoot


oldgLogger = Logger()
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
    gLogger.setLevel('debug')
    self.log = gLogger.getSubLogger('log')
    self.buffer = StringIO()

    oldgLogger.setLevel('debug')
    self.oldlog = oldgLogger.getSubLogger('log')
    self.oldbuffer = StringIO()
    sys.stdout = self.oldbuffer

    gLogger.showHeaders(True)
    gLogger.showThreadIDs(False)

    oldgLogger.showHeaders(True)
    oldgLogger.showThreadIDs(False)

    # modify the output to capture the log into a buffer
    if logging.getLogger().handlers:
      logging.getLogger().handlers[0].stream = self.buffer

    # reset the levels
    logging.getLogger('root.log').setLevel(logging.NOTSET)
    self.log._levelModified = False


if __name__ == '__main__':
  from DIRAC.FrameworkSystem.test.testLogging.tests.Test_DisplayOptions import Test_DisplayOptions
  from DIRAC.FrameworkSystem.test.testLogging.tests.Test_Levels import Test_Levels
  from DIRAC.FrameworkSystem.test.testLogging.tests.Test_LogRecordCreation import Test_LogRecordCreation
  from DIRAC.FrameworkSystem.test.testLogging.tests.Test_SubLogger import Test_SubLogger

  suite = unittest.defaultTestLoader.loadTestsFromTestCase(Test_Logging)
  suite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(Test_DisplayOptions))
  suite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(Test_Levels))
  suite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(Test_LogRecordCreation))
  suite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(Test_SubLogger))
  testResult = unittest.TextTestRunner(verbosity=2).run(suite)
