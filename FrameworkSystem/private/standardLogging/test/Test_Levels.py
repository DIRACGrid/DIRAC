"""
Test Levels
"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

__RCSID__ = "$Id$"

import unittest

from DIRAC.FrameworkSystem.private.standardLogging.test.TestLoggingBase import Test_Logging, gLogger


class Test_Levels(Test_Logging):
  """
  Test get and set levels.
  """

  def test_00shown(self):
    """
    Test the validity of the shown method
    """
    gLogger.setLevel('warn')

    gLogger.debug('message')

    self.assertEqual(self.buffer.getvalue(), "")
    self.assertEqual(gLogger.shown('debug'), False)
    self.buffer.truncate(0)

    gLogger.warn('message')

    self.assertIn("", self.buffer.getvalue())
    self.assertEqual(gLogger.shown('warn'), True)
    self.buffer.truncate(0)

  def test_01setLevelGetLevel(self):
    """
    Set gLogger level to error and get it
    """
    gLogger.setLevel('error')
    self.assertEqual(gLogger.getLevel(), 'ERROR')

  def test_02setLevelCreateLog(self):
    """
    Set gLogger level to error and try to create debug and error logs
    """
    gLogger.setLevel('error')

    self.assertEqual(gLogger.shown('debug'), False)
    self.assertEqual(gLogger.shown('verbose'), False)
    self.assertEqual(gLogger.shown('info'), False)
    self.assertEqual(gLogger.shown('warn'), False)
    self.assertEqual(gLogger.shown('notice'), False)

    self.assertEqual(gLogger.shown('error'), True)
    self.assertEqual(gLogger.shown('always'), True)
    self.assertEqual(gLogger.shown('fatal'), True)

  def test_03setLevelGetSubLogLevel(self):
    """
    Set gLogger level to error and get its sublogger level
    """
    gLogger.setLevel('error')
    self.assertEqual(self.log.getLevel(), 'ERROR')

  def test_04setLevelCreateLogSubLog(self):
    """
    Set gLogger level to error and try to create debug and error logs and sublogs
    """
    gLogger.setLevel('error')

    gLogger.debug("message")
    self.log.debug("message")
    self.assertEqual(gLogger.shown('debug'), False)
    self.assertEqual(self.log.shown('debug'), False)
    gLogger.verbose('message')
    self.log.verbose('message')
    self.assertEqual(gLogger.shown('verbose'), False)
    self.assertEqual(self.log.shown('verbose'), False)
    gLogger.info('message')
    self.log.info('message')
    self.assertEqual(gLogger.shown('info'), False)
    self.assertEqual(self.log.shown('info'), False)
    gLogger.warn('message')
    self.log.warn('message')
    self.assertEqual(gLogger.shown('warn'), False)
    self.assertEqual(self.log.shown('warn'), False)
    gLogger.notice('message')
    self.log.notice('message')
    self.assertEqual(gLogger.shown('notice'), False)
    self.assertEqual(self.log.shown('notice'), False)

    gLogger.error('message')
    self.log.error('message')
    self.assertEqual(gLogger.shown('error'), True)
    self.assertEqual(self.log.shown('error'), True)
    gLogger.always('message')
    self.log.always('message')
    self.assertEqual(gLogger.shown('always'), True)
    self.assertEqual(self.log.shown('always'), True)
    gLogger.fatal('message')
    self.log.fatal('message')
    self.assertEqual(gLogger.shown('fatal'), True)
    self.assertEqual(self.log.shown('fatal'), True)

  def test_05setLevelSubLevelCreateLogSubLog(self):
    """
    Set gLogger level to error and log level to debug, and try to create debug and error logs and sublogs
    """
    gLogger.setLevel('error')
    self.log.setLevel('debug')

    self.assertEqual(gLogger.debug("message"), False)
    self.assertEqual(self.log.debug("message"), True)

    self.assertEqual(gLogger.verbose('message'), False)
    self.assertEqual(self.log.verbose('message'), True)

    self.assertEqual(gLogger.info('message'), False)
    self.assertEqual(self.log.info('message'), True)

    self.assertEqual(gLogger.warn('message'), False)
    self.assertEqual(self.log.warn('message'), True)

    self.assertEqual(gLogger.notice('message'), False)
    self.assertEqual(self.log.notice('message'), True)

    self.assertEqual(gLogger.error('message'), True)
    self.assertEqual(self.log.error('message'), True)

    self.assertEqual(gLogger.always('message'), True)
    self.assertEqual(self.log.always('message'), True)

    self.assertEqual(gLogger.fatal('message'), True)
    self.assertEqual(self.log.fatal('message'), True)

  def test_06setLevelSubLevelCreateLogSubLog2(self):
    """
    Set gLogger level to debug and log level to error, and try to create debug and error logs and sublogs
    """
    gLogger.setLevel('debug')
    self.log.setLevel('error')

    self.assertEqual(gLogger.debug("message"), True)
    self.assertEqual(self.log.debug("message"), False)

    self.assertEqual(gLogger.verbose('message'), True)
    self.assertEqual(self.log.verbose('message'), False)

    self.assertEqual(gLogger.info('message'), True)
    self.assertEqual(self.log.info('message'), False)

    self.assertEqual(gLogger.warn('message'), True)
    self.assertEqual(self.log.warn('message'), False)

    self.assertEqual(gLogger.notice('message'), True)
    self.assertEqual(self.log.notice('message'), False)

    self.assertEqual(gLogger.error('message'), True)
    self.assertEqual(self.log.error('message'), True)

    self.assertEqual(gLogger.always('message'), True)
    self.assertEqual(self.log.always('message'), True)

    self.assertEqual(gLogger.fatal('message'), True)
    self.assertEqual(self.log.fatal('message'), True)

  def test_07getAllLevels(self):
    """
    Get all possible levels
    """
    self.assertEqual(gLogger.getAllPossibleLevels(), ['INFO', 'WARN',
                                                      'NOTICE', 'VERBOSE', 'ERROR', 'DEBUG', 'ALWAYS', 'FATAL'])

    self.assertEqual(self.log.getAllPossibleLevels(), ['INFO', 'WARN',
                                                       'NOTICE', 'VERBOSE', 'ERROR', 'DEBUG', 'ALWAYS', 'FATAL'])

  def test_08modifySubLevelAndGetSubSubLevel(self):
    """
    Modify the sub logger level, then the gLogger level and get the subsublogger level
    """
    gLogger.setLevel('debug')
    sublogger = self.log.getSubLogger("sublog")
    self.assertEqual(gLogger.getLevel(), "DEBUG")
    self.assertEqual(self.log.getLevel(), "DEBUG")
    self.assertEqual(sublogger.getLevel(), "DEBUG")
    gLogger.setLevel('error')
    self.assertEqual(gLogger.getLevel(), "ERROR")
    self.assertEqual(self.log.getLevel(), "ERROR")
    self.assertEqual(sublogger.getLevel(), "ERROR")
    self.log.setLevel('notice')
    self.assertEqual(gLogger.getLevel(), "ERROR")
    self.assertEqual(self.log.getLevel(), "NOTICE")
    self.assertEqual(sublogger.getLevel(), "NOTICE")
    gLogger.setLevel('verbose')
    self.assertEqual(gLogger.getLevel(), "VERBOSE")
    self.assertEqual(self.log.getLevel(), "NOTICE")
    self.assertEqual(sublogger.getLevel(), "NOTICE")


if __name__ == '__main__':
  suite = unittest.defaultTestLoader.loadTestsFromTestCase(Test_Levels)
  testResult = unittest.TextTestRunner(verbosity=2).run(suite)
