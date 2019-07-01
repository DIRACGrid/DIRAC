"""
Test Display Options
"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from __future__ import absolute_import, print_function, unicode_literals

__RCSID__ = "$Id$"

import unittest
import threading

from DIRAC.FrameworkSystem.private.standardLogging.test.TestLoggingBase import Test_Logging, gLogger, cleaningLog


class Test_DisplayOptions(Test_Logging):
  """
  Test the creation of subloggers and their properties
  """

  def setUp(self):
    super(Test_DisplayOptions, self).setUp()
    self.filename = '/tmp/logtmp.log'
    with open(self.filename, "w"):
      pass

  def test_00setShowHeaders(self):
    """
    Set the headers
    """
    gLogger.showHeaders(False)
    gLogger.notice('message', 'varmessage')

    self.assertEqual("message varmessage\n", self.buffer.getvalue())
    self.buffer.truncate(0)

    gLogger.showHeaders(True)
    gLogger.notice('message')

    logstring1 = cleaningLog(self.buffer.getvalue())

    self.assertEqual("UTCFrameworkNOTICE:message\n", logstring1)
    self.buffer.truncate(0)

  def test_01setShowThreadIDs(self):
    """
    Set the thread ID
    """
    gLogger.showThreadIDs(False)
    gLogger.notice('message')

    logstring1 = cleaningLog(self.buffer.getvalue())

    self.assertEqual("UTCFrameworkNOTICE:message\n", logstring1)
    self.buffer.truncate(0)

    gLogger.showThreadIDs(True)
    gLogger.notice('message')

    logstring1 = cleaningLog(self.buffer.getvalue())

    self.assertIn(str(threading.ident), logstring1)
    self.buffer.truncate(0)

  def test_02setShowThreadIDsHeaders(self):
    """
    Create a subsubsublogger and create a logrecord
    """
    gLogger.showHeaders(False)
    gLogger.showThreadIDs(False)
    gLogger.notice('message')

    self.assertEqual("message\n", self.buffer.getvalue())
    self.buffer.truncate(0)

    gLogger.showHeaders(False)
    gLogger.showThreadIDs(True)
    gLogger.notice('message')

    self.assertEqual("message\n", self.buffer.getvalue())
    self.buffer.truncate(0)

    gLogger.showHeaders(True)
    gLogger.showThreadIDs(False)
    gLogger.notice('message')

    logstring1 = cleaningLog(self.buffer.getvalue())

    self.assertEqual("UTCFrameworkNOTICE:message\n", logstring1)
    self.buffer.truncate(0)

    gLogger.showHeaders(True)
    gLogger.showThreadIDs(True)
    gLogger.notice('message')

    logstring1 = cleaningLog(self.buffer.getvalue())

    self.assertIn(str(threading.ident), logstring1)
    self.buffer.truncate(0)

  def test_03setSubLogShowHeaders(self):
    """
    Create a sublogger and set it its own Header option.
    """
    sublog = gLogger.getSubLogger('sublog')
    sublog.setLevel('notice')
    sublog.showHeaders(False)
    sublog.registerBackend('file', {'FileName': self.filename})
    # Empty the buffer to remove the Object Loader log message "trying to load..."
    self.buffer.truncate(0)

    sublog.notice("message")
    with open(self.filename) as file:
      message = file.read()

    self.assertEqual(message, "message\n")
    logstring1 = cleaningLog(self.buffer.getvalue())
    self.assertEqual(logstring1, "UTCFramework/sublogNOTICE:message\n")

  def test_04SubLogShowHeadersChange(self):
    """
    Create a sublogger and show that its Header option follow the change of its parent Header option.
    """
    sublog = gLogger.getSubLogger('sublog2')
    sublog.setLevel('notice')
    sublog.registerBackend('file', {'FileName': self.filename})
    # Empty the buffer to remove the Object Loader log message "trying to load..."
    self.buffer.truncate(0)
    gLogger.showHeaders(False)

    sublog.notice("message")
    with open(self.filename) as file:
      message = file.read()

    self.assertEqual(message, "message\n")
    self.assertEqual(self.buffer.getvalue(), "message\n")

  def test_05setSubLoggLoggerShowHeaders(self):
    """
    Create a sublogger, set its Header option and the Header option of the gLogger.
    Show that its Header option do not follow the change of its parent Header option.
    """
    sublog = gLogger.getSubLogger('sublog3')
    sublog.setLevel('notice')
    sublog.showHeaders(False)
    sublog.registerBackend('file', {'FileName': self.filename})
    # Empty the buffer to remove the Object Loader log message "trying to load..."
    self.buffer.truncate(0)
    gLogger.showHeaders(True)

    sublog.notice("message")
    with open(self.filename) as file:
      message = file.read()

    self.assertEqual(message, "message\n")
    logstring1 = cleaningLog(self.buffer.getvalue())
    self.assertEqual(logstring1, "UTCFramework/sublog3NOTICE:message\n")

  def test_06setSubLoggLoggerShowHeadersInverse(self):
    """
    Create a sublogger, set the Header option of the gLogger and its Header option.
    Show that the gLogger Header option do not follow the change of its child Header option.
    """
    sublog = gLogger.getSubLogger('sublog4')
    sublog.setLevel('notice')
    sublog.registerBackend('file', {'FileName': self.filename})
    # Empty the buffer to remove the Object Loader log message "trying to load..."
    self.buffer.truncate(0)
    gLogger.showHeaders(True)
    sublog.showHeaders(False)

    sublog.notice("message")
    with open(self.filename) as file:
      message = file.read()

    self.assertEqual(message, "message\n")
    logstring1 = cleaningLog(self.buffer.getvalue())
    self.assertEqual(logstring1, "UTCFramework/sublog4NOTICE:message\n")

  def test_07subLogShowHeadersChange(self):
    """
    Create a subsublogger and show that its Header option follow the change of its parent Header option.
    """
    sublog = gLogger.getSubLogger('sublog5')
    sublog.setLevel('notice')
    sublog.registerBackend('file', {'FileName': self.filename})
    # Empty the buffer to remove the Object Loader log message "trying to load..."
    self.buffer.truncate(0)
    subsublog = sublog.getSubLogger('subsublog')
    subsublog.registerBackend('file', {'FileName': self.filename})
    # Empty the buffer to remove the Object Loader log message "trying to load..."
    self.buffer.truncate(0)
    gLogger.showHeaders(False)

    subsublog.notice("message")
    with open(self.filename) as file:
      message = file.read()

    self.assertEqual(message, "message\nmessage\n")
    self.assertEqual(self.buffer.getvalue(), "message\n")

  def test_07subLogShowHeadersChangeSetSubLogger(self):
    """
    Create a subsublogger and show that its Header option follow the change of its parent Header option.
    """
    sublog = gLogger.getSubLogger('sublog6')
    sublog.setLevel('notice')
    sublog.registerBackend('file', {'FileName': self.filename})
    # Empty the buffer to remove the Object Loader log message "trying to load..."
    self.buffer.truncate(0)
    subsublog = sublog.getSubLogger('subsublog')
    subsublog.registerBackend('file', {'FileName': self.filename})
    # Empty the buffer to remove the Object Loader log message "trying to load..."
    self.buffer.truncate(0)
    sublog.showHeaders(False)

    subsublog.notice("message")
    with open(self.filename) as file:
      message = file.read()

    self.assertEqual(message, "message\nmessage\n")
    logstring1 = cleaningLog(self.buffer.getvalue())
    self.assertEqual(logstring1, "UTCFramework/sublog6/subsublogNOTICE:message\n")

  def test_09subLogShowHeadersChangeSetSubLogger(self):
    """
    Create a subsublogger and set its Header option and show that
    its Header option do not follow the change of its parent Header option.
    """
    sublog = gLogger.getSubLogger('sublog7')
    sublog.setLevel('notice')
    sublog.registerBackend('file', {'FileName': self.filename})
    # Empty the buffer to remove the Object Loader log message "trying to load..."
    self.buffer.truncate(0)
    subsublog = sublog.getSubLogger('subsublog')
    subsublog.registerBackends(['file'], {'FileName': self.filename})
    # Empty the buffer to remove the Object Loader log message "trying to load..."
    self.buffer.truncate(0)
    sublog.showHeaders(False)
    subsublog.showHeaders(True)

    subsublog.notice("message")
    with open(self.filename) as file:
      message = file.read()

    self.assertIn("UTC Framework/sublog7/subsublog NOTICE: message\nmessage\n", message)
    logstring1 = cleaningLog(self.buffer.getvalue())
    self.assertEqual(logstring1, "UTCFramework/sublog7/subsublogNOTICE:message\n")

  def test_10gLoggerShowHeadersChange2Times(self):
    """
    Create a sublogger with a file backend and change the Header option of gLogger 2 times
    in order to verify the propagation.
    """
    sublog = gLogger.getSubLogger('sublog8')
    sublog.registerBackends(['file'], {'FileName': self.filename})
    # Empty the buffer to remove the Object Loader log message "trying to load..."
    self.buffer.truncate(0)
    gLogger.showHeaders(False)
    sublog.notice("message")

    with open(self.filename) as file:
      message = file.read()

    self.assertEqual("message\n", message)

    gLogger.showHeaders(True)
    sublog.notice("message")

    with open(self.filename) as file:
      message = file.read()
    self.assertIn("UTC Framework/sublog8 NOTICE: message\n", message)


if __name__ == '__main__':
  suite = unittest.defaultTestLoader.loadTestsFromTestCase(Test_DisplayOptions)
  testResult = unittest.TextTestRunner(verbosity=2).run(suite)
