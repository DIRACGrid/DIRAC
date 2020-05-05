"""
Test Config for External Libraries
"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

__RCSID__ = "$Id$"


import unittest
import logging
from StringIO import StringIO

from DIRAC.FrameworkSystem.private.standardLogging.test.TestLoggingBase import Test_Logging, gLogger, cleaningLog


class Test_ConfigForExternalLibs(Test_Logging):
  """
  Test enableLogsFromExternalLibs method of LoggingRoot.
  logging.getLogger() returns the root logger which is used in external libraries
  """

  def test_00rootLoggerConfiguration(self):
    """
    Test the good configuration of the root logger
    """
    gLogger.enableLogsFromExternalLibs()
    handlers = logging.getLogger().handlers

    self.assertEqual(logging.getLogger().getEffectiveLevel(), logging.DEBUG)

    self.assertEqual(1, len(handlers))
    self.assertIsInstance(handlers[0], logging.StreamHandler)

    gLogger.disableLogsFromExternalLibs()
    handlers = logging.getLogger().handlers

    self.assertEqual(logging.getLogger().getEffectiveLevel(), logging.DEBUG)

    self.assertEqual(1, len(handlers))
    self.assertIsInstance(handlers[0], logging.NullHandler)

  def test_01displayLogs(self):
    """
    Test the display of the logs according to the value of the boolean in the method.
    """
    # Enabled
    gLogger.enableLogsFromExternalLibs()

    # modify the output to capture logs of the root logger
    bufferRoot = StringIO()
    logging.getLogger().handlers[0].stream = bufferRoot

    logging.getLogger().info("message")
    logstring1 = cleaningLog(bufferRoot.getvalue())

    self.assertEqual("UTCExternalLibrary/rootINFO:message\n", logstring1)
    bufferRoot.truncate(0)

    # this is a direct child of root, as the logger in DIRAC
    logging.getLogger("sublog").info("message")
    logstring1 = cleaningLog(bufferRoot.getvalue())

    self.assertEqual("UTCExternalLibrary/sublogINFO:message\n", logstring1)
    bufferRoot.truncate(0)

    # Disabled
    gLogger.disableLogsFromExternalLibs()

    logging.getLogger().info("message")
    # this is a direct child of root, as the logger in DIRAC
    logging.getLogger("sublog").info("message")

    self.assertEqual("", bufferRoot.getvalue())

  def test_02propagation(self):
    """
    Test the no propagation of the logs from the Logging objects to the root logger of 'logging'
    """
    gLogger.enableLogsFromExternalLibs()
    # modify the output to capture logs of the root logger
    bufferRoot = StringIO()
    logging.getLogger().handlers[0].stream = bufferRoot

    gLogger.debug('message')

    self.assertNotEqual(self.buffer.getvalue(), "")
    self.assertEqual(bufferRoot.getvalue(), "")
    self.buffer.truncate(0)

  def test_03multipleCalls(self):
    """
    Test the multiple calls to the method to see if we have no duplication of the logs
    """
    gLogger.enableLogsFromExternalLibs()
    gLogger.enableLogsFromExternalLibs()
    gLogger.enableLogsFromExternalLibs()
    gLogger.enableLogsFromExternalLibs()
    handlers = logging.getLogger().handlers

    self.assertEqual(1, len(handlers))
    self.assertIsInstance(handlers[0], logging.StreamHandler)

    gLogger.disableLogsFromExternalLibs()
    gLogger.disableLogsFromExternalLibs()
    gLogger.disableLogsFromExternalLibs()
    gLogger.disableLogsFromExternalLibs()
    handlers = logging.getLogger().handlers

    self.assertEqual(1, len(handlers))
    self.assertIsInstance(handlers[0], logging.NullHandler)

    gLogger.enableLogsFromExternalLibs()
    gLogger.disableLogsFromExternalLibs()
    gLogger.enableLogsFromExternalLibs()
    gLogger.disableLogsFromExternalLibs()
    handlers = logging.getLogger().handlers

    self.assertEqual(1, len(handlers))
    self.assertIsInstance(handlers[0], logging.NullHandler)

    gLogger.disableLogsFromExternalLibs()
    gLogger.enableLogsFromExternalLibs()
    gLogger.disableLogsFromExternalLibs()
    gLogger.enableLogsFromExternalLibs()
    handlers = logging.getLogger().handlers

    self.assertEqual(1, len(handlers))
    self.assertIsInstance(handlers[0], logging.StreamHandler)


if __name__ == '__main__':
  suite = unittest.defaultTestLoader.loadTestsFromTestCase(Test_ConfigForExternalLibs)
  testResult = unittest.TextTestRunner(verbosity=2).run(suite)
