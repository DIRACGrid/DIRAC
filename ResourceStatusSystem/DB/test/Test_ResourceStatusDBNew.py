""" UnitTest class for ResourceStatusDB
"""

# bisognerebbe testare valori di ritorno "veri" dentro a Value

import unittest
from datetime import datetime, timedelta
from DIRAC.ResourceStatusSystem.Utilities.mock import Mock
from DIRAC.ResourceStatusSystem.DB.ResourceStatusDBNew import *
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

  ###########################
  #####test site methods#####
  ###########################
  
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

  def test_getSitesToCheck(self):
    res = self.rsDB.getSitesToCheck(1,2,3)
    self.assertEqual(res, [])

  def test_getSitesHistory(self):
    res = self.rsDB.getSitesHistory()
    self.assertEqual(res, [])
    res = self.rsDB.getSitesHistory('LCG.Ferrara.it')
    self.assertEqual(res, [])

  def test_getSiteTypeList(self):
    res = self.rsDB.getSiteTypeList()
    self.assertEqual(res, [])
    res = self.rsDB.getSiteTypeList('T1')
    self.assertEqual(res, [])

  def test__addSiteRow(self):
    res = self.rsDB._addSiteRow('Ferrara', 'T2', 'Active', 'reasons', datetime.utcnow(), datetime.utcnow(), datetime.utcnow() + timedelta(minutes=10), 'Federico')
    self.assertEqual(res, None)
    
  def test__addSiteHistoryRow(self):
    res = self.rsDB._addSiteHistoryRow('Ferrara', 'Active', 'reasons', datetime.utcnow(), datetime.utcnow(), datetime.utcnow() + timedelta(minutes=10), 'Federico')
    self.assertEqual(res, None)
  
  def test_setSiteStatus(self):
    res = self.rsDB.setSiteStatus('CNAF', 'Active', 'reasons', 'Federico')
    self.assertEqual(res, None)

  def test_setSiteReason(self):
    res = self.rsDB.setSiteReason('Active', 'reasons', 'Federico')
    self.assertEqual(res, None)

  def test_addOrModifySite(self):
    for status in ValidStatus:
      res = self.rsDB.addOrModifySite('CNAF', 'T1', status, 'ho delle ragioni', datetime.utcnow(), 'testOP', datetime.utcnow() + timedelta(minutes=10))
      self.assertEqual(res, None)

  def test_addSiteType(self):
    res = self.rsDB.addSiteType('T1', 'test desc')
    self.assertEqual(res, None)

  def test_removeSite(self):
    res = self.rsDB.removeSite('CNAF')
    self.assertEqual(res, None)

  def test_removeSiteRow(self):
    res = self.rsDB.removeSiteRow('CNAF', datetime.utcnow())
    self.assertEqual(res, None)

  def test_removeSiteType(self):
    res = self.rsDB.removeSiteType('T1')
    self.assertEqual(res, None)

  ##############################
  #####test service methods#####
  ##############################
  
  def test_getServicesList(self):
    res = self.rsDB.getServicesList()
    self.assertEqual(res, [])
    res = self.rsDB.getServicesList(serviceName = 'CNAF')
    self.assertEqual(res, [])
    res = self.rsDB.getServicesList(serviceName = ['CNAF', 'Ferrara'])
    self.assertEqual(res, [])
    res = self.rsDB.getServicesList(paramsList = 'Status')
    self.assertEqual(res, [])
    res = self.rsDB.getServicesList(paramsList = ['ServiceName', 'Status'])
    self.assertEqual(res, [])
    res = self.rsDB.getServicesList(status = ['Active'])
    self.assertEqual(res, [])
    res = self.rsDB.getServicesList(serviceType = ['XXX'])
    self.assertEqual(res, [])
    res = self.rsDB.getServicesList(paramsList = ['ServiceName', 'Status'], status = ['Active'], serviceType = ['XXX'])
    self.assertEqual(res, [])
    

  def test_getServicesStatusWeb(self):
    res = self.rsDB.getServicesStatusWeb({}, [], 0, 500)
    self.assertEqual(res['Records'], [])
    res = self.rsDB.getServicesStatusWeb({'ExpandServiceHistory':'XX'}, [], 0, 500)
    self.assertEqual(res['Records'], [])
    res = self.rsDB.getServicesStatusWeb({'ServiceName':['XX', 'zz']}, [], 0, 500)
    self.assertEqual(res['Records'], [])
    res = self.rsDB.getServicesStatusWeb({'Status':['XX', 'zz']}, [], 0, 500)
    self.assertEqual(res['Records'], [])
    res = self.rsDB.getServicesStatusWeb({'ServiceType':['XX', 'zz']}, [], 0, 500)
    self.assertEqual(res['Records'], [])
    res = self.rsDB.getServicesStatusWeb({'ServiceName':['XX', 'zz'], 'Status':['XX', 'zz']}, [], 0, 500)
    self.assertEqual(res['Records'], [])
    res = self.rsDB.getServicesStatusWeb({'ServiceName':['XX', 'zz'], 'ServiceType':['XX', 'zz'], 'Status':['XX', 'zz']}, [], 0, 500)
    self.assertEqual(res['Records'], [])

  def test_getServicesToCheck(self):
    res = self.rsDB.getServicesToCheck(1,2,3)
    self.assertEqual(res, [])

  def test_getServicesHistory(self):
    res = self.rsDB.getServicesHistory()
    self.assertEqual(res, [])
    res = self.rsDB.getServicesHistory('LCG.Ferrara.it')
    self.assertEqual(res, [])

  def test_getServiceTypeList(self):
    res = self.rsDB.getServiceTypeList()
    self.assertEqual(res, [])
    res = self.rsDB.getServiceTypeList('T1')
    self.assertEqual(res, [])

  def test__addServiceRow(self):
    res = self.rsDB._addServiceRow('Computing@Ferrara', 'Computing', 'Ferrara', 'Active', 'reasons', datetime.utcnow(), datetime.utcnow(), datetime.utcnow() + timedelta(minutes=10), 'Federico')
    self.assertEqual(res, None)
    
  def test__addServiceHistoryRow(self):
    res = self.rsDB._addServiceHistoryRow('Computing@Ferrara', 'Ferrara', 'Active', 'reasons', datetime.utcnow(), datetime.utcnow(), datetime.utcnow() + timedelta(minutes=10), 'Federico')
    self.assertEqual(res, None)
  
  def test_setServiceStatus(self):
    res = self.rsDB.setServiceStatus('CNAF', 'Active', 'reasons', 'Federico')
    self.assertEqual(res, None)

  def test_setServiceReason(self):
    res = self.rsDB.setServiceReason('Active', 'reasons', 'Federico')
    self.assertEqual(res, None)

  def test_addOrModifyService(self):
    for status in ValidStatus:
      res = self.rsDB.addOrModifyService('Computing@Ferrara', 'Computing', 'Ferrara', status, 'ho delle ragioni', datetime.utcnow(), 'testOP', datetime.utcnow() + timedelta(minutes=10))
      self.assertEqual(res, None)

  def test_addServiceType(self):
    res = self.rsDB.addServiceType('T1', 'test desc')
    self.assertEqual(res, None)


  def test_removeService(self):
    res = self.rsDB.removeService('CNAF')
    self.assertEqual(res, None)

  def test_removeServiceRow(self):
    res = self.rsDB.removeServiceRow('CNAF', datetime.utcnow())
    self.assertEqual(res, None)

  def test_removeServiceType(self):
    res = self.rsDB.removeServiceType('T1')
    self.assertEqual(res, None)


  ###############################
  #####test resource methods#####
  ###############################
  
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

  def test_getResourcesHistory(self):
    res = self.rsDB.getResourcesHistory()
    self.assertEqual(res, [])
    res = self.rsDB.getResourcesHistory('grid01.fe.infn.it')
    self.assertEqual(res, [])

  def test__addResourcesRow(self):
    res = self.rsDB._addResourcesRow('CE01', 'CE', 'Computing@Ferrara', 'Ferrara', 'Active', 'reasons', datetime.utcnow(), datetime.utcnow(), datetime.utcnow() + timedelta(minutes=10), 'Federico')
    self.assertEqual(res, None)
    
  def test__addResourcesHistoryRow(self):
    res = self.rsDB._addResourcesHistoryRow('CE01', 'Computing@Ferrara', 'Ferrara', 'Active', 'reasons', datetime.utcnow(), datetime.utcnow(), datetime.utcnow() + timedelta(minutes=10), 'Federico')
    self.assertEqual(res, None)
    
  def test_getResourcesToCheck(self):
    res = self.rsDB.getResourcesToCheck(1,2,3)
    self.assertEqual(res, [])

  def test_setResourceStatus(self):
    res = self.rsDB.setResourceStatus('CE01', 'Active', 'reasons', 'Federico')
    self.assertEqual(res, None)

  def test_setResourceReason(self):
    res = self.rsDB.setResourceReason('Active', 'reasons', 'Federico')
    self.assertEqual(res, None)

  def test_addOrModifyResource(self):
    for status in ValidStatus:
      res = self.rsDB.addOrModifyResource('CE01', 'T1', 'Computing@Ferrara', 'CNAF', status, 'test reason', datetime.utcnow(), 'testOP', datetime.utcnow() + timedelta(minutes=10))
      self.assertEqual(res, None)

  def test_addResourceType(self):
    res = self.rsDB.addResourceType('CE', 'test desc')
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

  def test_getResourceStats(self):
    res = self.rsDB.getResourceStats('Service', 'XX')
    self.assertEqual(res, None)
    res = self.rsDB.getResourceStats('Site', 'XX')
    self.assertEqual(res, None)


  
  ##########################
  ###test general methods###
  ##########################

  def test_removeStatus(self):
    for status in ValidStatus:
      res = self.rsDB.removeStatus(status)
      self.assertEqual(res, None)
  
  def test_transact2History(self):
    res = self.rsDB.transact2History('Site', 'CNAF', datetime.utcnow())
    self.assertEqual(res, None)
    res = self.rsDB.transact2History('Site', 1)
    self.assertEqual(res, None)
    res = self.rsDB.transact2History('Resource', 'CE01', 'CNAF', datetime.utcnow())
    self.assertEqual(res, None)
    res = self.rsDB.transact2History('Resource', 1)
    self.assertEqual(res, None)

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

  def test_addStatus(self):
    for status in ValidStatus:
      res = self.rsDB.addStatus(status, 'test desc')
      self.assertEqual(res, None)

  def test_unique(self):
    self.mock_DB._query.return_value = {'OK': True, 'Value': ((1L,),)}
    res = self.rsDB.unique('Sites', 1)
    self.assert_(res)
    self.mock_DB._query.return_value = {'OK': True, 'Value': ((2L,),)}
    res = self.rsDB.unique('Sites', 1)
    self.assertFalse(res)

  def test_getStatusList(self):
    res = self.rsDB.getStatusList()
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

  def test_getResourceStats(self):
    res = self.rsDB.getResourceStats('Site', 'XX')
    self.assertEqual(res, None)
    res = self.rsDB.getResourceStats('Service', 'XX')
    self.assertEqual(res, None)

  def test_getServiceStats(self):
    res = self.rsDB.getServiceStats('XX')
    self.assertEqual(res, None)
    
  def test_syncWithCS(self):
    res = self.rsDB.syncWithCS()
    self.assertEqual(res, None)


class ResourceStatusDBFailure(ResourceStatusDBTestCase):

  def test_InvalidStatus(self):
    self.assertRaises(InvalidStatus, self.rsDB.addOrModifySite, 'CNAF', 'T1', 'BadStatus', 'test reason', datetime.utcnow(), 'testOP', datetime.utcnow() + timedelta(minutes=10))
    self.assertRaises(InvalidStatus, self.rsDB._addSiteRow, 'Ferrara', 'T2', 'Actives', 'reasons', datetime.utcnow(), datetime.utcnow(), datetime.utcnow() + timedelta(minutes=10), 'Federico')
    self.assertRaises(InvalidStatus, self.rsDB.addOrModifyService, 'Computing@CERN', 'Computing', 'CERN', 'BadStatus', 'test reason', datetime.utcnow(), 'testOP', datetime.utcnow() + timedelta(minutes=10))
    self.assertRaises(InvalidStatus, self.rsDB._addServiceRow, 'Computing@CERN', 'Computing', 'CERN', 'Actives', 'reasons', datetime.utcnow(), datetime.utcnow(), datetime.utcnow() + timedelta(minutes=10), 'Federico')
    self.assertRaises(InvalidStatus, self.rsDB.addOrModifyResource, 'CE01', 'CE', 'Computing@CERN', 'CNAF', 'BadStatus', 'test reason', datetime.utcnow(), 'testOP', datetime.utcnow() + timedelta(minutes=10))
    self.assertRaises(InvalidStatus, self.rsDB._addResourcesRow, 'CE01', 'CE', 'Computing@CERN', 'Ferrara', 'Actives', 'reasons', datetime.utcnow(), datetime.utcnow(), datetime.utcnow() + timedelta(minutes=10), 'Federico')

  def test_InvalidRes(self):
    self.assertRaises(InvalidRes, self.rsDB.setDateEnd, 'Sites')
        
  def test_NotAllowedDate(self):
    self.assertRaises(NotAllowedDate, self.rsDB.addOrModifySite, 'CNAF', 'T1', 'Active', 'test reason', datetime.utcnow(), 'testOP', datetime.utcnow() - timedelta(minutes=10))
    self.assertRaises(NotAllowedDate, self.rsDB.addOrModifyService, 'Computing@CERN', 'Computing', 'CERN', 'Active', 'test reason', datetime.utcnow(), 'testOP', datetime.utcnow() - timedelta(minutes=10))
    self.assertRaises(NotAllowedDate, self.rsDB.addOrModifyResource, 'CE01', 'CE', 'Computing@CERN', 'CERN', 'Active', 'test reason', datetime.utcnow(), 'testOP', datetime.utcnow() - timedelta(minutes=10))
    
  def test_DBFail(self):
    self.mock_DB._query.return_value = {'OK': False, 'Message': 'boh'}
    self.mock_DB._update.return_value = {'OK': False, 'Message': 'boh'}
    
    self.assertRaises(RSSDBException, self.rsDB.getSitesList) 
    self.assertRaises(RSSDBException, self.rsDB.getSiteTypeList) 
    self.assertRaises(RSSDBException, self.rsDB.getSitesHistory) 
    self.assertRaises(RSSDBException, self.rsDB.getSitesToCheck, 1,2,3) 
    self.assertRaises(RSSDBException, self.rsDB.addOrModifySite, 'CNAF', 'T1', 'Banned', 'test reason', datetime.utcnow(), 'testOP', datetime.utcnow() + timedelta(minutes=10)) 
    self.assertRaises(RSSDBException, self.rsDB.addSiteType, '') 
    self.assertRaises(RSSDBException, self.rsDB.removeSiteType, '') 
    self.assertRaises(RSSDBException, self.rsDB.removeSiteRow, 'CNAF', datetime.utcnow()) 
    self.assertRaises(RSSDBException, self.rsDB.setLastSiteCheckTime, 'CNAF')
    self.assertRaises(RSSDBException, self.rsDB.setSiteStatus, 'CNAF', 'Active', 'reasons', 'Federico')
    self.assertRaises(RSSDBException, self.rsDB._addSiteRow, 'Ferrara', 'T2', 'Active', 'reasons', datetime.utcnow(), datetime.utcnow(), datetime.utcnow() + timedelta(minutes=10), 'Federico')
    self.assertRaises(RSSDBException, self.rsDB._addSiteHistoryRow, 'Ferrara', 'Active', 'reasons', datetime.utcnow(), datetime.utcnow(), datetime.utcnow() + timedelta(minutes=10), 'Federico')
    
    self.assertRaises(RSSDBException, self.rsDB.getServicesList)
    self.assertRaises(RSSDBException, self.rsDB.getServiceTypeList) 
    self.assertRaises(RSSDBException, self.rsDB.getServicesHistory)
    self.assertRaises(RSSDBException, self.rsDB.getServicesToCheck, 1,2,3)
    self.assertRaises(RSSDBException, self.rsDB.addOrModifyService, 'Computing@CERN', 'Computing', 'CERN', 'Banned', 'test reason', datetime.utcnow(), 'testOP', datetime.utcnow() + timedelta(minutes=10))
    self.assertRaises(RSSDBException, self.rsDB.addServiceType, '')
    self.assertRaises(RSSDBException, self.rsDB.removeServiceType, '')
    self.assertRaises(RSSDBException, self.rsDB.removeServiceRow, 'Computing@CERN', datetime.utcnow())
    self.assertRaises(RSSDBException, self.rsDB.setLastServiceCheckTime, 'CE01')
    self.assertRaises(RSSDBException, self.rsDB.setServiceStatus, 'Computing@CERN', 'Active', 'reasons', 'Federico')
    self.assertRaises(RSSDBException, self.rsDB._addServiceRow, 'Computing@CERN', 'Computing', 'Ferrara', 'Active', 'reasons', datetime.utcnow(), datetime.utcnow(), datetime.utcnow() + timedelta(minutes=10), 'Federico')
    self.assertRaises(RSSDBException, self.rsDB._addServiceHistoryRow, 'Computing@CERN', 'Ferrara', 'Active', 'reasons', datetime.utcnow(), datetime.utcnow(), datetime.utcnow() + timedelta(minutes=10), 'Federico')

    self.assertRaises(RSSDBException, self.rsDB.getResourcesList)
    self.assertRaises(RSSDBException, self.rsDB.getResourceTypeList) 
    self.assertRaises(RSSDBException, self.rsDB.getResourcesHistory)
    self.assertRaises(RSSDBException, self.rsDB.getResourcesToCheck, 1,2,3)
    self.assertRaises(RSSDBException, self.rsDB.addOrModifyResource, 'CE01', 'T1', 'Computing@CERN', 'CNAF', 'Banned', 'test reason', datetime.utcnow(), 'testOP', datetime.utcnow() + timedelta(minutes=10))
    self.assertRaises(RSSDBException, self.rsDB.addResourceType, '')
    self.assertRaises(RSSDBException, self.rsDB.removeResourceType, '')
    self.assertRaises(RSSDBException, self.rsDB.removeResourceRow, 'CE01', 'CNAF', datetime.utcnow())
    self.assertRaises(RSSDBException, self.rsDB.setLastResourceCheckTime, 'CE01')
    self.assertRaises(RSSDBException, self.rsDB.setResourceStatus, 'CE01', 'Active', 'reasons', 'Federico')
    self.assertRaises(RSSDBException, self.rsDB._addResourcesRow, 'CE01', 'CE', 'Computing@CERN', 'Ferrara', 'Active', 'reasons', datetime.utcnow(), datetime.utcnow(), datetime.utcnow() + timedelta(minutes=10), 'Federico')
    self.assertRaises(RSSDBException, self.rsDB._addResourcesHistoryRow, 'CE01', 'Computing@CERN', 'Ferrara', 'Active', 'reasons', datetime.utcnow(), datetime.utcnow(), datetime.utcnow() + timedelta(minutes=10), 'Federico')

    self.assertRaises(RSSDBException, self.rsDB.getStatusList)
    self.assertRaises(RSSDBException, self.rsDB.getEndings, 'Resources') 
    self.assertRaises(RSSDBException, self.rsDB.getTablesWithHistory)
    self.assertRaises(RSSDBException, self.rsDB.addStatus, '')
    self.assertRaises(RSSDBException, self.rsDB.removeStatus, '')
    self.assertRaises(RSSDBException, self.rsDB.setDateEnd, 'Site', 'CNAF', datetime.utcnow())
    self.assertRaises(RSSDBException, self.rsDB.syncWithCS)
   


if __name__ == '__main__':
  suite = unittest.defaultTestLoader.loadTestsFromTestCase(ResourceStatusDBTestCase)
  suite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(ResourceStatusDBSuccess))
  suite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(ResourceStatusDBFailure))
  testResult = unittest.TextTestRunner(verbosity=2).run(suite)
