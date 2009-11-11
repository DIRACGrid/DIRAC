""" UnitTest class for ResourceStatusDB
"""

# bisognerebbe testare valori di ritorno "veri" dentro a Value

import unittest
from datetime import datetime, timedelta
from DIRAC.ResourceStatusSystem.Utilities.mock import Mock
from DIRAC.ResourceStatusSystem.DB.ResourceStatusDB import *
from DIRAC.ResourceStatusSystem.Utilities.Utils import *
from DIRAC.ResourceStatusSystem.Utilities.Exceptions import *

class ResourceStatusDBTestCase(unittest.TestCase):
  """ Base class for the ResourceStatusDB test cases
  """
  def setUp(self):
    # Create a mock of DB class

    self.mock_DB = Mock()

    # Setting mock return value
    self.mock_DB._query.return_value = {'OK': True, 'Value': ''}
    self.mock_DB._update.return_value = {'OK': True, 'Value': ''}

    # setting mock interface
    self.rsDB = ResourceStatusDB(DBin=self.mock_DB)


class ResourceStatusDBSuccess(ResourceStatusDBTestCase):

  #test get methods

  def test_getSitesList(self):
    res = self.rsDB.getSitesList()
    self.assertEqual(res, [])
    res = self.rsDB.getSitesList(siteName = 'CNAF')
    self.assertEqual(res, [])
    res = self.rsDB.getSitesList(siteName = ['CNAF', 'Ferrara'])
    self.assertEqual(res, [])
    res = self.rsDB.getSitesList(paramsList = 'Status')
    self.assertEqual(res, [])
    res = self.rsDB.getSitesList(paramsList = ['SiteName', 'Status'])
    self.assertEqual(res, [])
    res = self.rsDB.getSitesList(status = ['Active'])
    self.assertEqual(res, [])
    res = self.rsDB.getSitesList(siteType = ['XXX'])
    self.assertEqual(res, [])
    res = self.rsDB.getSitesList(paramsList = ['SiteName', 'Status'], status = ['Active'], siteType = ['XXX'])
    self.assertEqual(res, [])
    

  def test_getSitesStatusWeb(self):
    res = self.rsDB.getSitesStatusWeb({}, [], 0, 500)
    self.assertEqual(res['Records'], [])
    res = self.rsDB.getSitesStatusWeb({'ExpandSiteHistory':'XX'}, [], 0, 500)
    self.assertEqual(res['Records'], [])
    res = self.rsDB.getSitesStatusWeb({'SiteName':['XX', 'zz']}, [], 0, 500)
    self.assertEqual(res['Records'], [])
    res = self.rsDB.getSitesStatusWeb({'Status':['XX', 'zz']}, [], 0, 500)
    self.assertEqual(res['Records'], [])
    res = self.rsDB.getSitesStatusWeb({'SiteType':['XX', 'zz']}, [], 0, 500)
    self.assertEqual(res['Records'], [])
    res = self.rsDB.getSitesStatusWeb({'SiteName':['XX', 'zz'], 'Status':['XX', 'zz']}, [], 0, 500)
    self.assertEqual(res['Records'], [])
    res = self.rsDB.getSitesStatusWeb({'SiteName':['XX', 'zz'], 'SiteType':['XX', 'zz'], 'Status':['XX', 'zz']}, [], 0, 500)
    self.assertEqual(res['Records'], [])

  def test_getSiteTypeList(self):
    res = self.rsDB.getSiteTypeList()
    self.assertEqual(res, [])
    res = self.rsDB.getSiteTypeList('T1')
    self.assertEqual(res, [])

  def test_getStatusList(self):
    res = self.rsDB.getStatusList()
    self.assertEqual(res, [])

  def test_getResourceTypeList(self):
    res = self.rsDB.getResourceTypeList()
    self.assertEqual(res, [])
    res = self.rsDB.getResourceTypeList('T1')
    self.assertEqual(res, [])

  def test_getResourcesList(self):
    res = self.rsDB.getResourcesList()
    self.assertEqual(res, [])
    res = self.rsDB.getResourcesList(resourceName = 'CNAF')
    self.assertEqual(res, [])
    res = self.rsDB.getResourcesList(resourceName = ['CNAF', 'Ferrara'])
    self.assertEqual(res, [])
    res = self.rsDB.getResourcesList(paramsList = 'Status')
    self.assertEqual(res, [])
    res = self.rsDB.getResourcesList(siteName = ['xx', 'ss'])
    self.assertEqual(res, [])
    res = self.rsDB.getResourcesList(paramsList = ['ResourceName', 'Status'])
    self.assertEqual(res, [])
    res = self.rsDB.getResourcesList(paramsList = ['ResourceName', 'Status'], siteName = ['xx', 'ss'], status = ['xx'], resourceType = ['xx', 'cc'])
    self.assertEqual(res, [])

  def test_getResourcesListWeb(self):
    res = self.rsDB.getResourcesStatusWeb({}, None, 0, 500)
    self.assertEqual(res['Records'], [])
    res = self.rsDB.getResourcesStatusWeb({'ExpandResourceHistory':'XX'}, [], 0, 500)
    self.assertEqual(res['Records'], [])
    res = self.rsDB.getResourcesStatusWeb({'ResourceName':['XX', 'zz']}, [], 0, 500)
    self.assertEqual(res['Records'], [])
    res = self.rsDB.getResourcesStatusWeb({'SiteName':['XX', 'zz']}, [], 0, 500)
    self.assertEqual(res['Records'], [])
    res = self.rsDB.getResourcesStatusWeb({'Status':['XX', 'zz']}, [], 0, 500)
    self.assertEqual(res['Records'], [])
    res = self.rsDB.getResourcesStatusWeb({'ResourceType':['XX', 'zz']}, [], 0, 500)
    self.assertEqual(res['Records'], [])
    res = self.rsDB.getResourcesStatusWeb({'ResourceName':['XX', 'zz'], 'Status':['XX', 'zz']}, [], 0, 500)
    self.assertEqual(res['Records'], [])
    res = self.rsDB.getResourcesStatusWeb({'ResourceName':['XX', 'zz'], 'SiteName':['XX', 'zz'], 'Status':['XX', 'zz'], 'ResourceType':['xx']}, [], 0, 500)
    self.assertEqual(res['Records'], [])

  def test_getResourcesListByStatus(self):
    res = self.rsDB.getResourcesList('Active')
    self.assertEqual(res, [])

  def test_getSitesHistory(self):
    res = self.rsDB.getSitesHistory()
    self.assertEqual(res, [])
    res = self.rsDB.getSitesHistory('LCG.Ferrara.it')
    self.assertEqual(res, [])

  def test_getResourcesHistory(self):
    res = self.rsDB.getResourcesHistory()
    self.assertEqual(res, [])
    res = self.rsDB.getResourcesHistory('grid01.fe.infn.it')
    self.assertEqual(res, [])

  def test_getSitesToCheck(self):
    res = self.rsDB.getSitesToCheck(1,2,3)
    self.assertEqual(res, [])

  def test_getResourcesToCheck(self):
    res = self.rsDB.getResourcesToCheck(1,2,3)
    self.assertEqual(res, [])

  def test_getEndings(self):
    res = self.rsDB.getEndings('Sites')
    self.assertEqual(res, [])

  def test_getTablesWithHistory(self):
    res = self.rsDB.getTablesWithHistory()
    self.assertEqual(res, [])

  def test_getPeriods(self):
    for granularity in ValidRes:
      for status in ValidStatus:
        res = self.rsDB.getPeriods(granularity, 'XX', status, 2)
        self.assertEqual(res, None)
#        res = self.rsDB.getPeriods(granularity, 'XX', status, 2)
#        ((datetime.datetime(2009, 9, 21, 14, 38, 54), datetime.datetime(2009, 9, 21, 14, 38, 54)), (datetime.datetime(2009, 9, 21, 14, 38, 54), datetime.datetime(2009, 9, 22, 7, 8, 4)), (datetime.datetime(2009, 9, 22, 7, 8, 4), datetime.datetime(2009, 9, 22, 10, 48, 26)), (datetime.datetime(2009, 9, 22, 10, 48, 26), datetime.datetime(2009, 9, 24, 12, 12, 33)), (datetime.datetime(2009, 9, 24, 12, 12, 33), datetime.datetime(2009, 9, 24, 13, 5, 41)))

  def test_getGeneralName(self):
    res = self.rsDB.getGeneralName('XX', 'Resource', 'Site')
    self.assertEqual(res, [])
    res = self.rsDB.getGeneralName('XX', 'Resource', 'Service')
    self.assertEqual(res, [])
    res = self.rsDB.getGeneralName('XX', 'Service', 'Site')
    self.assertEqual(res, [])

  def test_getServiceStats(self):
    for service in ValidService:
      res = self.rsDB.getServiceStats(service, 'XX')
#      self.assertEqual(res, [])

  #test add methods

  def test_addOrModifySite(self):
    for status in ValidStatus:
      res = self.rsDB.addOrModifySite('CNAF', 'T1', 'test desc', status, 'ho delle ragioni', datetime.utcnow(), 'testOP', datetime.utcnow() + timedelta(minutes=10))
      self.assertEqual(res, None)

  def test_addOrModifyResource(self):
    for status in ValidStatus:
      res = self.rsDB.addOrModifyResource('CE01', 'T1', 'CNAF', status, 'test reason', datetime.utcnow(), 'testOP', datetime.utcnow() + timedelta(minutes=10))
      self.assertEqual(res, None)

  def test_addResourceType(self):
    res = self.rsDB.addResourceType('CE', 'test desc')
    self.assertEqual(res, None)

  def test_addSiteType(self):
    res = self.rsDB.addSiteType('T1', 'test desc')
    self.assertEqual(res, None)

  def test_addStatus(self):
    for status in ValidStatus:
      res = self.rsDB.addStatus(status, 'test desc')
      self.assertEqual(res, None)

  #test remove methods

  def test_removeSite(self):
    res = self.rsDB.removeSite('CNAF')
    self.assertEqual(res, None)

  def test_removeSiteRow(self):
    res = self.rsDB.removeSiteRow('CNAF', datetime.utcnow())
    self.assertEqual(res, None)

  def test_removeResource(self):
    res = self.rsDB.removeResource('XX')
    self.assertEqual(res, None)

  def test_removeResouceRow(self):
    res = self.rsDB.removeResourceRow('CE01', 'CNAF', datetime.utcnow())
    self.assertEqual(res, None)

  def test_removeResouceType(self):
    res = self.rsDB.removeResourceType('CE')
    self.assertEqual(res, None)

  def test_removeSiteType(self):
    res = self.rsDB.removeSiteType('T1')
    self.assertEqual(res, None)

  def test_removeStatus(self):
    for status in ValidStatus:
      res = self.rsDB.removeStatus(status)
      self.assertEqual(res, None)

  #test transact2history

  def test_transact2History(self):
    res = self.rsDB.transact2History('Site', 'CNAF', datetime.utcnow())
    self.assertEqual(res, None)
    res = self.rsDB.transact2History('Site', 1)
    self.assertEqual(res, None)
    res = self.rsDB.transact2History('Resource', 'CE01', 'CNAF', datetime.utcnow())
    self.assertEqual(res, None)
    res = self.rsDB.transact2History('Resource', 1)
    self.assertEqual(res, None)

  #test set methods

  def test_setLastSiteCheckTime(self):
    res = self.rsDB.setLastSiteCheckTime('CNAF')
    self.assertEqual(res, None)

  def test_setLastResourceCheckTime(self):
    res = self.rsDB.setLastResourceCheckTime('CE01')
    self.assertEqual(res, None)

  def test_setDateEnd(self):
    for siteOrRes in ValidRes:
      res = self.rsDB.setDateEnd(siteOrRes, 'XX', datetime.utcnow())
      self.assertEqual(res, None)

  def test_setSiteStatus(self):
    res = self.rsDB.setSiteStatus('CNAF', 'Active', 'reasons', 'Federico')
    self.assertEqual(res, None)

  def test_setResourceStatus(self):
    res = self.rsDB.setResourceStatus('CE01', 'Active', 'reasons', 'Federico')
    self.assertEqual(res, None)

  def test_setSiteReason(self):
    res = self.rsDB.setSiteReason('Active', 'reasons', 'Federico')
    self.assertEqual(res, None)

  def test_setResourceReason(self):
    res = self.rsDB.setResourceReason('Active', 'reasons', 'Federico')
    self.assertEqual(res, None)

  def test_unique(self):
    self.mock_DB._query.return_value = {'OK': True, 'Value': ((1L,),)}
    res = self.rsDB.unique('Sites', 1)
    self.assert_(res)
    self.mock_DB._query.return_value = {'OK': True, 'Value': ((2L,),)}
    res = self.rsDB.unique('Sites', 1)
    self.assertFalse(res)

  #test private methods

  def test__addSiteRow(self):
    res = self.rsDB._addSiteRow('Ferrara', 'T2', 'test desc', 'Active', 'reasons', datetime.utcnow(), datetime.utcnow(), datetime.utcnow() + timedelta(minutes=10), 'Federico')
    self.assertEqual(res, None)
    
  def test__addResourcesRow(self):
    res = self.rsDB._addResourcesRow('CE01', 'CE', 'Ferrara', 'Active', 'reasons', datetime.utcnow(), datetime.utcnow(), datetime.utcnow() + timedelta(minutes=10), 'Federico')
    self.assertEqual(res, None)
    
  def test__addSiteHistoryRow(self):
    res = self.rsDB._addSiteHistoryRow('Ferrara', 'Active', 'reasons', datetime.utcnow(), datetime.utcnow(), datetime.utcnow() + timedelta(minutes=10), 'Federico')
    self.assertEqual(res, None)
    
  def test__addResourcesHistoryRow(self):
    res = self.rsDB._addResourcesHistoryRow('CE01', 'Ferrara', 'Active', 'reasons', datetime.utcnow(), datetime.utcnow(), datetime.utcnow() + timedelta(minutes=10), 'Federico')
    self.assertEqual(res, None)
    
  def test__getSiteByID(self):
    res = self.rsDB._getSiteByID(1)
    self.assertEqual(res, [])
  
  def test__getResourceByID(self):
    res = self.rsDB._getResourceByID(1)
    self.assertEqual(res, [])
  

class ResourceStatusDBFailure(ResourceStatusDBTestCase):

  def test_InvalidStatus(self):
    self.assertRaises(InvalidStatus, self.rsDB.addOrModifySite, 'CNAF', 'T1', 'test desc', 'BadStatus', 'test reason', datetime.utcnow(), 'testOP', datetime.utcnow() + timedelta(minutes=10))
    self.assertRaises(InvalidStatus, self.rsDB.addOrModifyResource, 'CE01', 'T1', 'CNAF', 'BadStatus', 'test reason', datetime.utcnow(), 'testOP', datetime.utcnow() + timedelta(minutes=10))
    self.assertRaises(InvalidStatus, self.rsDB._addSiteRow, 'Ferrara', 'T2', 'test desc', 'Actives', 'reasons', datetime.utcnow(), datetime.utcnow(), datetime.utcnow() + timedelta(minutes=10), 'Federico')
    self.assertRaises(InvalidStatus, self.rsDB._addResourcesRow, 'CE01', 'CE', 'Ferrara', 'Actives', 'reasons', datetime.utcnow(), datetime.utcnow(), datetime.utcnow() + timedelta(minutes=10), 'Federico')

  def test_InvalidRes(self):
    self.assertRaises(InvalidRes, self.rsDB.setDateEnd, 'Sites')
        
  def test_NotAllowedDate(self):
    self.assertRaises(NotAllowedDate, self.rsDB.addOrModifySite, 'CNAF', 'T1', 'test desc', 'Active', 'test reason', datetime.utcnow(), 'testOP', datetime.utcnow() - timedelta(minutes=10))
    self.assertRaises(NotAllowedDate, self.rsDB.addOrModifyResource, 'CE01', 'T1', 'CNAF', 'Active', 'test reason', datetime.utcnow(), 'testOP', datetime.utcnow() - timedelta(minutes=10))
    
  def test_DBFail(self):
    self.mock_DB._query.return_value = {'OK': False, 'Message': 'boh'}
    self.mock_DB._update.return_value = {'OK': False, 'Message': 'boh'}
    self.assertRaises(RSSDBException, self.rsDB.getSitesList) 
    self.assertRaises(RSSDBException, self.rsDB.getResourcesList)
    self.assertRaises(RSSDBException, self.rsDB.getSiteTypeList) 
    self.assertRaises(RSSDBException, self.rsDB.getResourceTypeList) 
    self.assertRaises(RSSDBException, self.rsDB.getStatusList)
    self.assertRaises(RSSDBException, self.rsDB.getSitesListByStatus, 'Banned') 
    self.assertRaises(RSSDBException, self.rsDB.getResourcesListByStatus, 'Banned')
    self.assertRaises(RSSDBException, self.rsDB.getSitesHistory) 
    self.assertRaises(RSSDBException, self.rsDB.getResourcesHistory)
    self.assertRaises(RSSDBException, self.rsDB.getSitesToCheck, 1,2,3) 
    self.assertRaises(RSSDBException, self.rsDB.getResourcesToCheck, 1,2,3)
    self.assertRaises(RSSDBException, self.rsDB.getEndings, 'Resources') 
    self.assertRaises(RSSDBException, self.rsDB.getTablesWithHistory)
    self.assertRaises(RSSDBException, self.rsDB.addOrModifySite, 'CNAF', 'T1', 'test desc', 'Banned', 'test reason', datetime.utcnow(), 'testOP', datetime.utcnow() + timedelta(minutes=10)) 
    self.assertRaises(RSSDBException, self.rsDB.addOrModifyResource, 'CE01', 'T1', 'CNAF', 'Banned', 'test reason', datetime.utcnow(), 'testOP', datetime.utcnow() + timedelta(minutes=10))
    self.assertRaises(RSSDBException, self.rsDB.addSiteType, '') 
    self.assertRaises(RSSDBException, self.rsDB.addResourceType, '')
    self.assertRaises(RSSDBException, self.rsDB.addStatus, '')
    self.assertRaises(RSSDBException, self.rsDB.removeSiteType, '') 
    self.assertRaises(RSSDBException, self.rsDB.removeResourceType, '')
    self.assertRaises(RSSDBException, self.rsDB.removeStatus, '')
    self.assertRaises(RSSDBException, self.rsDB.removeSiteRow, 'CNAF', datetime.utcnow()) 
    self.assertRaises(RSSDBException, self.rsDB.removeResourceRow, 'CE01', 'CNAF', datetime.utcnow())
    self.assertRaises(RSSDBException, self.rsDB.setLastSiteCheckTime, 'CNAF')
    self.assertRaises(RSSDBException, self.rsDB.setLastResourceCheckTime, 'CE01')
    self.assertRaises(RSSDBException, self.rsDB.setDateEnd, 'Site', 'CNAF', datetime.utcnow())
    self.assertRaises(RSSDBException, self.rsDB.setSiteStatus, 'CNAF', 'Active', 'reasons', 'Federico')
    self.assertRaises(RSSDBException, self.rsDB.setResourceStatus, 'CE01', 'Active', 'reasons', 'Federico')
    self.assertRaises(RSSDBException, self.rsDB._addSiteRow, 'Ferrara', 'T2', 'test desc', 'Active', 'reasons', datetime.utcnow(), datetime.utcnow(), datetime.utcnow() + timedelta(minutes=10), 'Federico')
    self.assertRaises(RSSDBException, self.rsDB._addResourcesRow, 'CE01', 'CE', 'Ferrara', 'Active', 'reasons', datetime.utcnow(), datetime.utcnow(), datetime.utcnow() + timedelta(minutes=10), 'Federico')
    self.assertRaises(RSSDBException, self.rsDB._addSiteHistoryRow, 'Ferrara', 'Active', 'reasons', datetime.utcnow(), datetime.utcnow(), datetime.utcnow() + timedelta(minutes=10), 'Federico')
    self.assertRaises(RSSDBException, self.rsDB._addResourcesHistoryRow, 'CE01', 'Ferrara', 'Active', 'reasons', datetime.utcnow(), datetime.utcnow(), datetime.utcnow() + timedelta(minutes=10), 'Federico')



if __name__ == '__main__':
  suite = unittest.defaultTestLoader.loadTestsFromTestCase(ResourceStatusDBTestCase)
  suite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(ResourceStatusDBSuccess))
  suite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(ResourceStatusDBFailure))
  testResult = unittest.TextTestRunner(verbosity=2).run(suite)
