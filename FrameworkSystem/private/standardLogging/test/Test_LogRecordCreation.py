"""
Test LogRecord Creation
"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

__RCSID__ = "$Id$"

import unittest

from DIRAC.FrameworkSystem.private.standardLogging.test.TestLoggingBase import Test_Logging, gLogger, cleaningLog


class Test_LogRecordCreation(Test_Logging):
  """
  Test the creation of the different log records
  via the always, notice, ..., fatal methods.
  """

  def setUp(self):
    super(Test_LogRecordCreation, self).setUp()
    self.log.setLevel('debug')

  def test_00always(self):
    """
    Create Always log and test it
    """
    gLogger.always("message")

    logstring1 = cleaningLog(self.buffer.getvalue())

    self.assertEqual("UTCFrameworkALWAYS:message\n", logstring1)
    self.buffer.truncate(0)

    self.log.always("message")

    logstring1 = cleaningLog(self.buffer.getvalue())

    self.assertEqual("UTCFramework/logALWAYS:message\n", logstring1)
    self.buffer.truncate(0)

  def test_01notice(self):
    """
    Create Notice log and test it
    """
    gLogger.notice("message")

    logstring1 = cleaningLog(self.buffer.getvalue())

    self.assertEqual("UTCFrameworkNOTICE:message\n", logstring1)
    self.buffer.truncate(0)

    self.log.notice("message")

    logstring1 = cleaningLog(self.buffer.getvalue())

    self.assertEqual("UTCFramework/logNOTICE:message\n", logstring1)
    self.buffer.truncate(0)

  def test_02info(self):
    """
    Create Info log and test it
    """
    gLogger.info("message")

    logstring1 = cleaningLog(self.buffer.getvalue())

    self.assertEqual("UTCFrameworkINFO:message\n", logstring1)
    self.buffer.truncate(0)

    self.log.info("message")

    logstring1 = cleaningLog(self.buffer.getvalue())

    self.assertEqual("UTCFramework/logINFO:message\n", logstring1)
    self.buffer.truncate(0)

  def test_03verbose(self):
    """
    Create Verbose log and test it
    Differences between the two systems :
    - gLogger: VERBOSE
    """
    gLogger.verbose("message")

    logstring1 = cleaningLog(self.buffer.getvalue())

    self.assertEqual("UTCFrameworkVERBOSE:message\n", logstring1)
    self.buffer.truncate(0)

    self.log.verbose("message")

    logstring1 = cleaningLog(self.buffer.getvalue())

    self.assertEqual("UTCFramework/logVERBOSE:message\n", logstring1)
    self.buffer.truncate(0)

  def test_04debug(self):
    """
    Create Debug log and test it
    """
    gLogger.debug("message")

    logstring1 = cleaningLog(self.buffer.getvalue())

    self.assertEqual("UTCFrameworkDEBUG:message\n", logstring1)
    self.buffer.truncate(0)

    self.log.debug("message")

    logstring1 = cleaningLog(self.buffer.getvalue())

    self.assertEqual("UTCFramework/logDEBUG:message\n", logstring1)
    self.buffer.truncate(0)

  def test_05warn(self):
    """
    Create Warn log and test it
    """
    gLogger.warn("message")

    logstring1 = cleaningLog(self.buffer.getvalue())

    self.assertEqual("UTCFrameworkWARN:message\n", logstring1)
    self.buffer.truncate(0)

    self.log.warn("message")

    logstring1 = cleaningLog(self.buffer.getvalue())

    self.assertEqual("UTCFramework/logWARN:message\n", logstring1)
    self.buffer.truncate(0)

  def test_06error(self):
    """
    Create Error log and test it
    """
    gLogger.error("message")

    logstring1 = cleaningLog(self.buffer.getvalue())

    self.assertEqual("UTCFrameworkERROR:message\n", logstring1)
    self.buffer.truncate(0)

    self.log.error("message")

    logstring1 = cleaningLog(self.buffer.getvalue())

    self.assertEqual("UTCFramework/logERROR:message\n", logstring1)
    self.buffer.truncate(0)

  def test_07fatal(self):
    """
    Create Fatal log and test it
    """
    gLogger.fatal("message")

    logstring1 = cleaningLog(self.buffer.getvalue())

    self.assertEqual("UTCFrameworkFATAL:message\n", logstring1)
    self.buffer.truncate(0)

    self.log.fatal("message")

    logstring1 = cleaningLog(self.buffer.getvalue())

    self.assertEqual("UTCFramework/logFATAL:message\n", logstring1)
    self.buffer.truncate(0)

  def test_09WithExtrasArgs(self):
    """
    Create Always log with extra arguments and test it
    """
    self.log.always('%s.' % "message")
    logstring1 = cleaningLog(self.buffer.getvalue())

    self.assertEqual(logstring1, "UTCFramework/logALWAYS:message.\n")
    self.buffer.truncate(0)

    # display Framework/log ALWAYS: message

  def test_10onMultipleLines(self):
    """
    Create Always log on multiple lines and test it
    """
    self.log.always('this\nis\na\nmessage\non\nmultiple\nlines.')
    logstring1 = cleaningLog(self.buffer.getvalue())

    self.assertEqual(logstring1, "UTCFramework/logALWAYS:this\nis\na\nmessage\non\nmultiple\nlines.\n")
    self.buffer.truncate(0)

  def test_11WithVarMsg(self):
    """
    Create Always log with variable message and test it
    """
    self.log.always("mess", "age")
    logstring1 = cleaningLog(self.buffer.getvalue())

    self.assertEqual(logstring1, "UTCFramework/logALWAYS:message\n")
    self.buffer.truncate(0)

  def test_14showStack(self):
    """
    Get the showStack
    """
    gLogger.showStack()

    logstring1 = cleaningLog(self.buffer.getvalue())

    self.assertEqual(logstring1, "UTCFrameworkDEBUG:\n")
    self.buffer.truncate(0)


if __name__ == '__main__':
  suite = unittest.defaultTestLoader.loadTestsFromTestCase(Test_LogRecordCreation)
  testResult = unittest.TextTestRunner(verbosity=2).run(suite)
