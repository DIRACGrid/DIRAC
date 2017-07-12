"""
Test SubLogger
"""

__RCSID__ = "$Id$"

#pylint: disable=invalid-name

import unittest

from DIRAC.FrameworkSystem.test.testLogging.tests.Test_Logging import Test_Logging, gLogger


class Test_SubLogger(Test_Logging):
  """
  Test the creation of subloggers and their properties
  """

  def test_00getSubLogger(self):
    """
    Create a sublogger and create a log record
    """
    log = gLogger.getSubLogger('log')
    log.always('message')

    self.assertIn(" Framework/log ", self.buffer.getvalue())
    self.buffer.truncate(0)
    self.oldbuffer.truncate(0)

  def test_01getSubSubLogger(self):
    """
    Create a subsublogger and create a logrecord
    """
    log = gLogger.getSubLogger('log')
    sublog = log.getSubLogger('sublog')
    sublog.always('message')

    self.assertIn(" Framework/log/sublog ", self.buffer.getvalue())
    self.buffer.truncate(0)
    self.oldbuffer.truncate(0)

  def test_02getSubSubSubLogger(self):
    """
    Create a subsubsublogger and create a logrecord
    """
    log = gLogger.getSubLogger('log')
    sublog = log.getSubLogger('sublog')
    subsublog = sublog.getSubLogger('subsublog')
    subsublog.always('message')

    self.assertIn(" Framework/log/sublog/subsublog ", self.buffer.getvalue())
    self.buffer.truncate(0)
    self.oldbuffer.truncate(0)


if __name__ == '__main__':
  suite = unittest.defaultTestLoader.loadTestsFromTestCase(Test_SubLogger)
  testResult = unittest.TextTestRunner(verbosity=2).run(suite)
