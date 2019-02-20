"""
Test Logger Wrapper
"""

__RCSID__ = "$Id$"

import unittest
import logging
import sys
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

    self.oldbuffer = StringIO()
    sys.stdout = self.oldbuffer

    gLogger.showHeaders(True)
    gLogger.showThreadIDs(False)

    # modify the output to capture the log into a buffer
    if logging.getLogger('dirac').handlers:
      logging.getLogger('dirac').handlers[0].stream = self.buffer

    # reset the levels
    logging.getLogger('dirac').getChild('log').setLevel(logging.NOTSET)
    self.log._levelModified = False


if __name__ == '__main__':
  from DIRAC.FrameworkSystem.test.testLogging.Test_DisplayOptions import Test_DisplayOptions
  from DIRAC.FrameworkSystem.test.testLogging.Test_Levels import Test_Levels
  from DIRAC.FrameworkSystem.test.testLogging.Test_LogRecordCreation import Test_LogRecordCreation
  from DIRAC.FrameworkSystem.test.testLogging.Test_SubLogger import Test_SubLogger
  from DIRAC.FrameworkSystem.test.testLogging.Test_ConfigForExternalLibs import Test_ConfigForExternalLibs

  suite = unittest.defaultTestLoader.loadTestsFromTestCase(Test_Logging)
  suite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(Test_DisplayOptions))
  suite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(Test_Levels))
  suite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(Test_LogRecordCreation))
  suite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(Test_SubLogger))
  suite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(Test_ConfigForExternalLibs))
  testResult = unittest.TextTestRunner(verbosity=2).run(suite)
