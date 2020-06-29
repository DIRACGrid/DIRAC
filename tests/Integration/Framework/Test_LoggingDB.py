""" This is a test of the SystemLoggingDB

    It supposes that the DB is present and installed in DIRAC
"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

# pylint: disable=invalid-name,wrong-import-position,protected-access

import unittest
import sys

from DIRAC.Core.Base.Script import parseCommandLine
parseCommandLine()

from DIRAC import gLogger
from DIRAC.Core.Utilities.Time import toString
from DIRAC.FrameworkSystem.DB.SystemLoggingDB import SystemLoggingDB
from DIRAC.FrameworkSystem.private.standardLogging.Message import tupleToMessage


class TestSystemLoggingDBTestCase(unittest.TestCase):

  def setUp(self):
    self.db = SystemLoggingDB()

  def tearDown(self):
    pass


class testDB(TestSystemLoggingDBTestCase):

  def test_addAndRemove(self):
    """ Some test cases
    """

    systemName = 'TestSystem'
    subSystemName = 'TestSubSystem'
    level = 10
    time = toString()
    msgTest = 'Hello'
    variableText = time
    frameInfo = ""
    message = tupleToMessage((systemName, level, time, msgTest, variableText, frameInfo, subSystemName))
    site = 'somewehere'
    longSite = 'somewehere1234567890123456789012345678901234567890123456789012345678901234567890'
    nodeFQDN = '127.0.0.1'
    userDN = 'Yo'
    userGroup = 'Us'
    remoteAddress = 'elsewhere'

    records = 10

    db = SystemLoggingDB()
    res = db._connect()
    self.assertTrue(res['OK'])

    gLogger.info('\n Inserting some records\n')
    for k in xrange(records):
      result = db.insertMessage(message, site, nodeFQDN, userDN, userGroup, remoteAddress)
      self.assertTrue(result['OK'])
      self.assertEqual(result['lastRowId'], k + 1)
      self.assertEqual(result['Value'], 1)

    result = db._queryDB(showFieldList=['SiteName'])
    self.assertTrue(result['OK'])
    self.assertEqual(result['Value'][0][0], site)

    result = db._queryDB(showFieldList=['SystemName'])
    self.assertTrue(result['OK'])
    self.assertEqual(result['Value'][0][0], systemName)

    result = db._queryDB(showFieldList=['SubSystemName'])
    self.assertTrue(result['OK'])
    self.assertEqual(result['Value'][0][0], subSystemName)

    result = db._queryDB(showFieldList=['OwnerGroup'])
    self.assertTrue(result['OK'])
    self.assertEqual(result['Value'][0][0], userGroup)

    result = db._queryDB(showFieldList=['FixedTextString'])
    self.assertTrue(result['OK'])
    self.assertEqual(result['Value'][0][0], msgTest)

    result = db._queryDB(showFieldList=['VariableText', 'SiteName'], count=True, groupColumn='VariableText')
    self.assertTrue(result['OK'])
    self.assertEqual(result['Value'][0][1], site)
    self.assertEqual(result['Value'][0][2], records)

    result = db.insertMessage(message, longSite, nodeFQDN, userDN, userGroup, remoteAddress)
    self.assertFalse(result['OK'])


if __name__ == '__main__':
  suite = unittest.defaultTestLoader.loadTestsFromTestCase(TestSystemLoggingDBTestCase)
  suite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(testDB))
  testResult = unittest.TextTestRunner(verbosity=2).run(suite)
  sys.exit(not testResult.wasSuccessful())
