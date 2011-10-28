""" UnitTest class for ResourceManagementDB
"""

import sys
import unittest

from DIRAC.Core.Base import Script
Script.parseCommandLine()

from DIRAC.ResourceStatusSystem.Utilities.mock import Mock
from DIRAC.ResourceStatusSystem import ValidRes,ValidStatus

import DIRAC.ResourceStatusSystem.test.fake_Logger

class ResourceManagementDBTestCase(unittest.TestCase):
  """ Base class for the ResourceManagementDB test cases
  """
  def setUp(self):
    # Create a mock of DB class
    self.mock_DB = Mock()

    # Setting mock return value
    self.mock_DB._query.return_value = {'OK': True, 'Value': ''}
    self.mock_DB._update.return_value = {'OK': True, 'Value': ''}

    sys.modules["DIRAC"] = DIRAC.ResourceStatusSystem.test.fake_Logger
    sys.modules["DIRAC.Core.Utilities.SiteCEMapping"] = DIRAC.ResourceStatusSystem.test.fake_Logger
    sys.modules["DIRAC.Core.Utilities.SiteSEMapping"] = DIRAC.ResourceStatusSystem.test.fake_Logger
    sys.modules["DIRAC.Core.Utilities.SitesDIRACGOCDBmapping"] = DIRAC.ResourceStatusSystem.test.fake_Logger

    from DIRAC.ResourceStatusSystem.DB.ResourceManagementDB import ResourceManagementDB

    # setting mock interface
    self.rmDB = ResourceManagementDB(DBin=self.mock_DB)

    self.mock_DB_1 = Mock()
    self.mock_DB_1._query.return_value = {'OK': True, 'Value': (('VOMS',),)}

    self.rmDB_1 = ResourceManagementDB(DBin=self.mock_DB_1)

class ResourceManagementDBSuccess(ResourceManagementDBTestCase):

  ##########################
  ###test general methods###
  ##########################

  def test_addOrModifyPolicyRes(self):
    for g in ValidRes:
      for s in ValidStatus:
        res = self.rmDB.addOrModifyPolicyRes(g, 'XXX', 'ppp', s, 'XXX')
        self.assertEqual(res, None)

  def test_getPolicyRes(self):
    res = self.rmDB.getPolicyRes('XX', 'YY')
    self.assertEqual(res, [])

  def test_addOrModifyClientCacheRes(self):
    for g in ValidRes:
      res = self.rmDB.addOrModifyClientsCacheRes(g, 'XXX', 'ppp', 'XXX', 'ID')
      self.assertEqual(res, None)

  def test_getClientsCacheStuff(self):
    res = self.rmDB.getClientsCacheStuff()
    self.assertEqual(res, [])
    res = self.rmDB.getClientsCacheStuff('XX')
    self.assertEqual(res, [])
    res = self.rmDB.getClientsCacheStuff(None, 'YY')
    self.assertEqual(res, [])
    res = self.rmDB.getClientsCacheStuff(None, None, 'ZZ')
    self.assertEqual(res, [])
    res = self.rmDB.getClientsCacheStuff('XX', None, 'ZZ')
    self.assertEqual(res, [])
    res = self.rmDB.getClientsCacheStuff('XX', 'YY', None)
    self.assertEqual(res, [])
    res = self.rmDB.getClientsCacheStuff(None, 'XX', 'YY')
    self.assertEqual(res, [])
    res = self.rmDB.getClientsCacheStuff('XX', 'YY', None, 'XX', 'YY', None)
    self.assertEqual(res, [])
    res = self.rmDB.getClientsCacheStuff(None, 'XX', 'YY', 'XX', 'YY', None, 'ZZ')
    self.assertEqual(res, [])
    res = self.rmDB.getClientsCacheStuff(None, 'XX', 'YY', ['XX', 'YY'], None, ['ZZ'])
    self.assertEqual(res, [])

  def test_addOrModifyAccountingCacheRes(self):
    res = self.rmDB.addOrModifyAccountingCacheRes('Name', 'XXX', 'ppp', 'XXX')
    self.assertEqual(res, None)

  def test_getAccountingCacheStuff(self):
    res = self.rmDB.getAccountingCacheStuff()
    self.assertEqual(res, [])
    res = self.rmDB.getAccountingCacheStuff('XX')
    self.assertEqual(res, [])
    res = self.rmDB.getAccountingCacheStuff(None, 'YY')
    self.assertEqual(res, [])
    res = self.rmDB.getAccountingCacheStuff(None, None, 'ZZ')
    self.assertEqual(res, [])
    res = self.rmDB.getAccountingCacheStuff('XX', None, 'ZZ')
    self.assertEqual(res, [])
    res = self.rmDB.getAccountingCacheStuff('XX', 'YY', None)
    self.assertEqual(res, [])
    res = self.rmDB.getAccountingCacheStuff(None, 'XX', 'YY')
    self.assertEqual(res, [])
    res = self.rmDB.getAccountingCacheStuff(['XX'], 'YY', None, 'XX')
    self.assertEqual(res, [])
    res = self.rmDB.getAccountingCacheStuff(None, 'XX', 'YY', 'XX')
    self.assertEqual(res, [])

  def test_removeStatus(self):
    for status in ValidStatus:
      res = self.rmDB.removeStatus(status)
      self.assertEqual(res, None)

  def test_addStatus(self):
    for status in ValidStatus:
      res = self.rmDB.addStatus(status, 'test desc')
      self.assertEqual(res, None)

  def test_getStatusList(self):
    res = self.rmDB.getStatusList()
    self.assertEqual(res, [])

  def test_getDownTimesWeb(self):
    g1 = 'Site'
    g2 = 'Resource'
    s1 = 'OUTAGE'
    s2 = 'AT_RISK'
    for g in (g1, g2):
      for s in (s1, s2):
        res = self.rmDB.getDownTimesWeb({'Granularity': g, 'Severity': [s]})
        self.assertEqual(res['Records'], [])
        res = self.rmDB.getDownTimesWeb({'Granularity': [g1, g2], 'Severity': [s]})
        self.assertEqual(res['Records'], [])
        res = self.rmDB.getDownTimesWeb({'Granularity': g, 'Severity': [s]}, [])
        self.assertEqual(res['Records'], [])
        res = self.rmDB.getDownTimesWeb({'Granularity': [g1, g2], 'Severity': [s]})
        self.assertEqual(res['Records'], [])
      res = self.rmDB.getDownTimesWeb({'Granularity': [g], 'Severity': [s1, s2]})
      self.assertEqual(res['Records'], [])
    res = self.rmDB.getDownTimesWeb({'Granularity': [g1, g2], 'Severity': [s1, s2]})
    self.assertEqual(res['Records'], [])

class ResourceManagementDBFailure(ResourceManagementDBTestCase):

  def test_DBFail(self):
    self.mock_DB._query.return_value = {'OK': False, 'Message': 'boh'}
    self.mock_DB._update.return_value = {'OK': False, 'Message': 'boh'}
    from DIRAC.ResourceStatusSystem.DB.ResourceManagementDB import RSSManagementDBException

    self.assertRaises(RSSManagementDBException, self.rmDB.getStatusList)
    self.assertRaises(RSSManagementDBException, self.rmDB.addStatus, '')
    self.assertRaises(RSSManagementDBException, self.rmDB.removeStatus, '')


if __name__ == '__main__':
  suite = unittest.defaultTestLoader.loadTestsFromTestCase(ResourceManagementDBTestCase)
  suite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(ResourceManagementDBSuccess))
  suite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(ResourceManagementDBFailure))

  unittest.TextTestRunner(verbosity=2).run(suite)
