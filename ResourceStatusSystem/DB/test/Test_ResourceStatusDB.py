""" UnitTest class for ResourceStatusDB
"""

import sys
import datetime
import unittest

from DIRAC.Core.Base import Script
Script.parseCommandLine()

from DIRAC.ResourceStatusSystem.Utilities.mock import Mock
from DIRAC.ResourceStatusSystem import ValidRes,ValidStatus,ValidSiteType
from DIRAC.ResourceStatusSystem.Utilities.Exceptions import InvalidStatus
from DIRAC.ResourceStatusSystem.Utilities import CS
import DIRAC.ResourceStatusSystem.test.fake_Logger

class ResourceStatusDBTestCase(unittest.TestCase):
  """ Base class for the ResourceStatusDB test cases
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
    from DIRAC.ResourceStatusSystem.DB.ResourceStatusDB import ResourceStatusDB

    # setting mock interface
    self.rsDB = ResourceStatusDB(DBin=self.mock_DB)

    self.mock_DB_1 = Mock()
    self.mock_DB_1._query.return_value = {'OK': True, 'Value': (('VOMS',),)}

    self.rsDB_1 = ResourceStatusDB(DBin=self.mock_DB_1)

class ResourceStatusDBSuccess(ResourceStatusDBTestCase):

  ###########################
  #####test site methods#####
  ###########################

  def test__addSiteRow(self):
    res = self.rsDB._addSiteRow('Ferrara', 'T2', 'INFN-FERRARA', 'Active', 'reasons', datetime.datetime.utcnow(), datetime.datetime.utcnow(), datetime.datetime.utcnow() + datetime.timedelta(minutes=10), 'Federico')
    self.assertEqual(res, None)

  def test__addSiteHistoryRow(self):
    res = self.rsDB._addSiteHistoryRow('Ferrara', 'Active', 'reasons', datetime.datetime.utcnow(), datetime.datetime.utcnow(), datetime.datetime.utcnow() + datetime.timedelta(minutes=10), 'Federico')
    self.assertEqual(res, None)

  def test_setSiteStatus(self):
    res = self.rsDB.setSiteStatus('LCG.CNAF.it', 'Active', 'reasons', 'Federico')
    self.assertEqual(res, None)

  def test_addOrModifySite(self):
    for status in ValidStatus:
      res = self.rsDB.addOrModifySite('CNAF', 'T1', 'INFN-FERRARA', status, 'ho delle ragioni', datetime.datetime.utcnow(), 'testOP', datetime.datetime.utcnow() + datetime.timedelta(minutes=10))
      self.assertEqual(res, None)

  def test_removeSite(self):
    res = self.rsDB.removeSite('LCG.CNAF.it')
    self.assertEqual(res, None)

  ##############################
  #####test service methods#####
  ##############################


  def test__addServiceRow(self):
    res = self.rsDB._addServiceRow('Computing@Ferrara', 'Computing', 'Ferrara', 'Active', 'reasons', datetime.datetime.utcnow(), datetime.datetime.utcnow(), datetime.datetime.utcnow() + datetime.timedelta(minutes=10), 'Federico')
    self.assertEqual(res, None)

  def test__addServiceHistoryRow(self):
    res = self.rsDB._addServiceHistoryRow('Computing@Ferrara', 'Active', 'reasons', datetime.datetime.utcnow(), datetime.datetime.utcnow(), datetime.datetime.utcnow() + datetime.timedelta(minutes=10), 'Federico')
    self.assertEqual(res, None)

  def test_setServiceStatus(self):
    res = self.rsDB.setServiceStatus('CNAF', 'Active', 'reasons', 'Federico')
    self.assertEqual(res, None)

  def test_addOrModifyService(self):
    for status in ValidStatus:
      res = self.rsDB.addOrModifyService('Computing@Ferrara', 'Computing', 'Ferrara', status, 'ho delle ragioni', datetime.datetime.utcnow(), 'testOP', datetime.datetime.utcnow() + datetime.timedelta(minutes=10))
      self.assertEqual(res, None)

  def test_removeService(self):
    res = self.rsDB.removeService('Storage@CNAF')
    self.assertEqual(res, None)

  ###############################
  #####test resource methods#####
  ###############################

  def test__addResourcesRow(self):
    res = self.rsDB._addResourcesRow('CE01', 'CE', 'Computing', 'Ferrara', 'INFN-Ferrara', 'Active', 'reasons', datetime.datetime.utcnow(), datetime.datetime.utcnow(), datetime.datetime.utcnow() + datetime.timedelta(minutes=10), 'Federico')
    self.assertEqual(res, None)
    res = self.rsDB._addResourcesRow('CE01', 'CE', 'Computing', None, 'INFN-Ferrara', 'Active', 'reasons', datetime.datetime.utcnow(), datetime.datetime.utcnow(), datetime.datetime.utcnow() + datetime.timedelta(minutes=10), 'Federico')
    self.assertEqual(res, None)
    res = self.rsDB._addResourcesRow('CE01', 'CE', 'Computing', 'NULL', 'INFN-Ferrara', 'Active', 'reasons', datetime.datetime.utcnow(), datetime.datetime.utcnow(), datetime.datetime.utcnow() + datetime.timedelta(minutes=10), 'Federico')
    self.assertEqual(res, None)

  def test__addResourcesHistoryRow(self):
    res = self.rsDB._addResourcesHistoryRow('CE01', 'Active', 'reasons', datetime.datetime.utcnow(), datetime.datetime.utcnow(), datetime.datetime.utcnow() + datetime.timedelta(minutes=10), 'Federico')
    self.assertEqual(res, None)

  def test_setResourceStatus(self):
    res = self.rsDB.setResourceStatus('CE01', 'Active', 'reasons', 'Federico')
    self.assertEqual(res, None)

  def test_addOrModifyResource(self):
    for status in ValidStatus:
      res = self.rsDB.addOrModifyResource('CE01', 'T1', 'Computing', 'CNAF', 'INFN-T1', status, 'test reason', datetime.datetime.utcnow(), 'testOP', datetime.datetime.utcnow() + datetime.timedelta(minutes=10))
      self.assertEqual(res, None)

  def test_removeResource(self):
    res = self.rsDB.removeResource('XX')
    self.assertEqual(res, None)
    res = self.rsDB.removeResource(None, 'Storage@PIPPO.it')
    self.assertEqual(res, None)
    res = self.rsDB.removeResource(None, None, 'm')
    self.assertEqual(res, None)


  ###############################
  #####test storageElement methods#####
  ###############################

  def test__addStorageElementRow(self):
    res = self.rsDB._addStorageElementRow('xxx', 'xxx.Ferrara.it', 'Ferrara', 'Active', 'reasons', datetime.datetime.utcnow(), datetime.datetime.utcnow(), datetime.datetime.utcnow() + datetime.timedelta(minutes=10), 'Federico', "Read")
    self.assertEqual(res, None)
    res = self.rsDB._addStorageElementRow('xxx', 'xxx.Ferrara.it', None, 'Active', 'reasons', datetime.datetime.utcnow(), datetime.datetime.utcnow(), datetime.datetime.utcnow() + datetime.timedelta(minutes=10), 'Federico', "Read")
    self.assertEqual(res, None)
    res = self.rsDB._addStorageElementRow('xxx', 'xxx.Ferrara.it', 'NULL', 'Active', 'reasons', datetime.datetime.utcnow(), datetime.datetime.utcnow(), datetime.datetime.utcnow() + datetime.timedelta(minutes=10), 'Federico', "Read")
    self.assertEqual(res, None)

  def test__addStorageElementHistoryRow(self):
    res = self.rsDB._addStorageElementHistoryRow('xxx', 'Active', 'reasons', datetime.datetime.utcnow(), datetime.datetime.utcnow(), datetime.datetime.utcnow() + datetime.timedelta(minutes=10), 'Federico', "Read")
    self.assertEqual(res, None)

  def test_setStorageElementStatus(self):
    res = self.rsDB.setStorageElementStatus('SE', 'Active', 'reasons', 'Federico', "Read")
    self.assertEqual(res, None)

  def test_addOrModifyStorageElement(self):
    for status in ValidStatus:
      res = self.rsDB.addOrModifyStorageElement('se', 'xxx.Ferrara.it', 'CNAF', status, 'test reason', datetime.datetime.utcnow(), 'testOP', datetime.datetime.utcnow() + datetime.timedelta(minutes=10), "Read")
      self.assertEqual(res, None)

  def test_removeStorageElement(self):
    res = self.rsDB.removeStorageElement('XX', None, "Read")
    self.assertEqual(res, None)
    res = self.rsDB.removeStorageElement('XX', 'XX', "Read")
    self.assertEqual(res, None)

  ##########################
  ###test general methods###
  ##########################

  def test_addOrModifyGridSite(self):
    for t in ValidSiteType:
      res = self.rsDB.addOrModifyGridSite('XXX', t)
      self.assertEqual(res, None)

  def test_getGridSitesList(self):
    res = self.rsDB.getGridSitesList(paramsList = ['GridSiteName', 'GridTier'])
    self.assertEqual(res, [])
    res = self.rsDB.getGridSitesList(gridSiteName = ['AAAA', 'BBBB'])
    self.assertEqual(res, [])
    res = self.rsDB.getGridSitesList(paramsList = ['GridSiteName', 'GridTier'], gridSiteName = ['AAAA', 'BBBB'])
    self.assertEqual(res, [])
    res = self.rsDB.getGridSitesList(paramsList = ['GridSiteName', 'GridTier'], gridTier = ['T0', 'T1'])
    self.assertEqual(res, [])
    res = self.rsDB.getGridSitesList(gridSiteName = ['AAAA', 'BBBB'], gridTier = ['T0', 'T1'])
    self.assertEqual(res, [])
    res = self.rsDB.getGridSitesList(paramsList = ['GridSiteName', 'GridTier'],
                                     gridSiteName = ['AAAA', 'BBBB'], gridTier = ['T0', 'T1'])
    self.assertEqual(res, [])

  def test_getMonitoredsList(self):
    for g in ValidRes:
      res = self.rsDB.getMonitoredsList(g, countries = 'a')
      self.assertEqual(res, [])
      res = self.rsDB.getMonitoredsList(g, paramsList = ['SiteName', 'Status'], countries = 'a')
      self.assertEqual(res, [])
      res = self.rsDB.getMonitoredsList(g, status = ['Active'], countries = 'a')
      self.assertEqual(res, [])
      res = self.rsDB.getMonitoredsList(g, siteName = ['xx', 'ss'], countries = 'a')
      self.assertEqual(res, [])

    res = self.rsDB.getMonitoredsList('Site', siteType = ['XXX'], countries = 'a')
    self.assertEqual(res, [])
    res = self.rsDB.getMonitoredsList('Site', paramsList = ['SiteName', 'Status'], status = ['Active'], siteType = ['XXX'], countries = 'a')
    self.assertEqual(res, [])

    res = self.rsDB.getMonitoredsList('Service', serviceName = ['CNAF', 'Ferrara'], countries = 'a')
    self.assertEqual(res, [])
    res = self.rsDB.getMonitoredsList('Service', serviceType = ['XXX'], countries = 'a')
    self.assertEqual(res, [])
    res = self.rsDB.getMonitoredsList('Service', paramsList = ['ServiceName', 'Status'], status = ['Active'], serviceType = ['XXX'], countries = 'a')
    self.assertEqual(res, [])

    res = self.rsDB.getMonitoredsList('Resource', resourceName = ['CNAF', 'Ferrara'], countries = 'a')
    self.assertEqual(res, [])
    res = self.rsDB.getMonitoredsList('Service', resourceType = ['XXX'], countries = 'a')
    self.assertEqual(res, [])
    res = self.rsDB.getMonitoredsList('Resource', paramsList = ['ResourceName', 'Status'], siteName = ['xx', 'ss'], status = ['xx'], resourceType = ['xx', 'cc'], countries = 'a')
    self.assertEqual(res, [])

    res = self.rsDB.getMonitoredsList('StorageElementRead', storageElementName = ['CNAF', 'Ferrara'], countries = 'a')
    self.assertEqual(res, [])
    res = self.rsDB.getMonitoredsList('StorageElementRead', paramsList = ['ResourceName', 'Status'], storageElementName = ['xx', 'ss'], status = ['xx'], resourceType = ['xx', 'cc'], countries = 'a')
    self.assertEqual(res, [])


  def test_getMonitoredsStatusWeb(self):
    for g in ValidRes:
      res = self.rsDB.getMonitoredsStatusWeb(g, {'Countries':'a'}, [], 0, 500)
      self.assertEqual(res['Records'], [])
      res = self.rsDB.getMonitoredsStatusWeb(g, {'Status':['XX', 'zz'], 'Countries':'a'}, [], 0, 500)
      self.assertEqual(res['Records'], [])

    res = self.rsDB.getMonitoredsStatusWeb('Site', {'ExpandSiteHistory':'XX', 'Countries':'a'}, [], 0, 500)
    self.assertEqual(res['Records'], [])
    res = self.rsDB.getMonitoredsStatusWeb('Site', {'SiteName':['XX', 'zz'], 'Countries':'a'}, [], 0, 500)
    self.assertEqual(res['Records'], [])
    res = self.rsDB.getMonitoredsStatusWeb('Site', {'SiteType':['XX', 'zz'], 'Countries':'a'}, [], 0, 500)
    self.assertEqual(res['Records'], [])
    res = self.rsDB.getMonitoredsStatusWeb('Site', {'SiteName':['XX', 'zz'], 'Status':['XX', 'zz'], 'Countries':'a'}, [], 0, 500)
    self.assertEqual(res['Records'], [])
    res = self.rsDB.getMonitoredsStatusWeb('Site', {'SiteName':['XX', 'zz'], 'SiteType':['XX', 'zz'], 'Status':['XX', 'zz'], 'Countries':'a'}, [], 0, 500)
    self.assertEqual(res['Records'], [])

    res = self.rsDB.getMonitoredsStatusWeb('Service', {'ExpandServiceHistory':'XX', 'Countries':'a'}, [], 0, 500)
    self.assertEqual(res['Records'], [])
    res = self.rsDB.getMonitoredsStatusWeb('Service', {'ServiceName':['XX', 'zz'], 'Countries':'a'}, [], 0, 500)
    self.assertEqual(res['Records'], [])
    res = self.rsDB.getMonitoredsStatusWeb('Service', {'ServiceType':['XX', 'zz'], 'Countries':'a'}, [], 0, 500)
    self.assertEqual(res['Records'], [])
    res = self.rsDB.getMonitoredsStatusWeb('Service', {'ServiceName':['XX', 'zz'], 'Status':['XX', 'zz'], 'Countries':'a'}, [], 0, 500)
    self.assertEqual(res['Records'], [])
    res = self.rsDB.getMonitoredsStatusWeb('Service', {'ServiceName':['XX', 'zz'], 'ServiceType':['XX', 'zz'], 'Status':['XX', 'zz'], 'Countries':'a'}, [], 0, 500)
    self.assertEqual(res['Records'], [])

    res = self.rsDB.getMonitoredsStatusWeb('Resource', {'ExpandResourceHistory':'XX', 'Countries':'a'}, [], 0, 500)
    self.assertEqual(res['Records'], [])
    res = self.rsDB.getMonitoredsStatusWeb('Resource', {'ResourceName':['XX', 'zz'], 'Countries':'a'}, [], 0, 500)
    self.assertEqual(res['Records'], [])
    res = self.rsDB.getMonitoredsStatusWeb('Resource', {'SiteName':['XX', 'zz'], 'Countries':'a'}, [], 0, 500)
    self.assertEqual(res['Records'], [])
    res = self.rsDB.getMonitoredsStatusWeb('Resource', {'ResourceType':['XX', 'zz'], 'Countries':'a'}, [], 0, 500)
    self.assertEqual(res['Records'], [])
    res = self.rsDB.getMonitoredsStatusWeb('Resource', {'ResourceName':['XX', 'zz'], 'Status':['XX', 'zz'], 'Countries':'a'}, [], 0, 500)
    self.assertEqual(res['Records'], [])
    res = self.rsDB.getMonitoredsStatusWeb('Resource', {'ResourceName':['XX', 'zz'], 'SiteName':['XX', 'zz'], 'Status':['XX', 'zz'], 'ResourceType':['xx'], 'Countries':'a'}, [], 0, 500)
    self.assertEqual(res['Records'], [])

  def test_getMonitoredsHistory(self):
    for g in ValidRes:
      res = self.rsDB.getMonitoredsHistory(g)
      self.assertEqual(res, [])
      res = self.rsDB.getMonitoredsHistory(g, paramsList = ['aaa'])
      self.assertEqual(res, [])

  def test_getTypesList(self):
    for g in ['Site', 'Service', 'Resource']:
      res = self.rsDB.getTypesList(g)
      self.assertEqual(res, [])
      res = self.rsDB.getTypesList(g, 'xx')
      self.assertEqual(res, [])

  def test_removeType(self):
    for g in ['Site', 'Service', 'Resource']:
      res = self.rsDB.removeType(g, 'xx')
      self.assertEqual(res, None)

  def test_removeRow(self):
    for g in ValidRes:
      res = self.rsDB.removeRow(g, 'xx', datetime.datetime.utcnow())
      self.assertEqual(res, None)

  def test_removeStatus(self):
    for status in ValidStatus:
      res = self.rsDB.removeStatus(status)
      self.assertEqual(res, None)

  def test_setMonitoredReason(self):
    for g in ValidRes:
      res = self.rsDB.setMonitoredReason(g, 'xx', 'reasons', 'Federico')
      self.assertEqual(res, None)

  def test_transact2History(self):
    res = self.rsDB.transact2History('Site', 'CNAF', datetime.datetime.utcnow())
    self.assertEqual(res, None)
    res = self.rsDB.transact2History('Site', 1)
    self.assertEqual(res, None)
    res = self.rsDB.transact2History('Resource', 'CE01', datetime.datetime.utcnow())
    self.assertEqual(res, None)
    res = self.rsDB.transact2History('Resource', 1)
    self.assertEqual(res, None)

  def test_setLastMonitoredCheckTime(self):
    for g in ValidRes:
      res = self.rsDB.setLastMonitoredCheckTime(g, 'CNAF')
      self.assertEqual(res, None)

  def test_setDateEnd(self):
    for granularity in ValidRes:
      res = self.rsDB.setDateEnd(granularity, 'XX', datetime.datetime.utcnow())
      self.assertEqual(res, None)

  def test_addStatus(self):
    for status in ValidStatus:
      res = self.rsDB.addStatus(status, 'test desc')
      self.assertEqual(res, None)

  def test_addType(self):
    for g in ('Site', 'Service', 'Resource'):
      res = self.rsDB.addType(g, 'CE', 'test desc')
      self.assertEqual(res, None)

  def test_getCountries(self):
    for g in ValidRes:
      res = self.rsDB.getCountries(g)
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
        res = self.rsDB.getPeriods(granularity, 'XX', status, None, 10)
        self.assertEqual(res, None)
#        res = self.rsDB.getPeriods(granularity, 'XX', status, 2)
#        ((datetime.datetime.datetime.datetime(2009, 9, 21, 14, 38, 54), datetime.datetime.datetime.datetime(2009, 9, 21, 14, 38, 54)), (datetime.datetime.datetime.datetime(2009, 9, 21, 14, 38, 54), datetime.datetime.datetime.datetime(2009, 9, 22, 7, 8, 4)), (datetime.datetime.datetime.datetime(2009, 9, 22, 7, 8, 4), datetime.datetime.datetime.datetime(2009, 9, 22, 10, 48, 26)), (datetime.datetime.datetime.datetime(2009, 9, 22, 10, 48, 26), datetime.datetime.datetime.datetime(2009, 9, 24, 12, 12, 33)), (datetime.datetime.datetime.datetime(2009, 9, 24, 12, 12, 33), datetime.datetime.datetime.datetime(2009, 9, 24, 13, 5, 41)))

  def test_getGeneralName(self):
    for g1 in ('Service', 'Resource', 'StorageElementRead', 'StorageElementWrite'):
      for g2 in ('Site', 'Service', 'Resource'):
        if g1 == g2:
          continue
        res = self.rsDB_1.getGeneralName('XX', g1, g2)
        if g2 == 'Service':
          if g1 == 'StorageElementRead' or g1 == 'StorageElementWrite':
            self.assertEqual(res, ['Storage@VOMS'])
          else:
            self.assertEqual(res, ['VOMS@VOMS'])
        else:
          self.assertEqual(res, ['VOMS'])

  def test_getResourceStats(self):
    res = self.rsDB.getResourceStats('Site', 'XX')
    self.assertEqual(res, {'Active': 0, 'Probing': 0, 'Bad': 0, 'Banned': 0, 'Total': 0})
    res = self.rsDB.getResourceStats('Service', 'Computing@LCG.CERN.ch')
    self.assertEqual(res, {'Active': 0, 'Probing': 0, 'Bad': 0, 'Banned': 0, 'Total': 0})
    res = self.rsDB.getResourceStats('Service', 'Storage@LCG.CERN.ch')
    self.assertEqual(res, {'Active': 0, 'Probing': 0, 'Bad': 0, 'Banned': 0, 'Total': 0})

  def test_getServiceStats(self):
    res = self.rsDB.getServiceStats('XX')
    self.assertEqual(res, {'Active': 0, 'Probing': 0, 'Bad': 0, 'Banned': 0, 'Total': 0})

  def test_getStorageElementsStats(self):
    res = self.rsDB.getStorageElementsStats('Resource', 'XX', "Read")
    self.assertEqual(res, {'Active': 0, 'Probing': 0, 'Bad': 0, 'Banned': 0, 'Total': 0})
    res = self.rsDB.getStorageElementsStats('Site', 'XX', "Read")
    self.assertEqual(res, {'Active': 0, 'Probing': 0, 'Bad': 0, 'Banned': 0, 'Total': 0})


  def test_getStuffToCheck(self):
    for g in ValidRes:
      res = self.rsDB.getStuffToCheck(g,CS.getTypedDictRootedAt("CheckingFreqs/SitesFreqs"),3)
      self.assertEqual(res, [])
      res = self.rsDB.getStuffToCheck(g,CS.getTypedDictRootedAt("CheckingFreqs/SitesFreqs"))
      self.assertEqual(res, [])
      res = self.rsDB.getStuffToCheck(g,None,None,'aaa')
      self.assertEqual(res, [])

  def test_getTokens(self):
    for g in ValidRes:
      res = self.rsDB.getTokens(g, datetime.datetime(9999, 12, 31, 23, 59, 59))
      self.assertEqual(res, [])

  def test_setToken(self):
    for g in ValidRes:
      res = self.rsDB.setToken(g, 'LCG.Ferrara.it', 'Federico', datetime.datetime(9999, 12, 31, 23, 59, 59))
      self.assertEqual(res, None)

  def test_whatIs(self):
    res = self.rsDB.whatIs('LCG.Ferrara.it')
    self.assertEqual(res, 'Unknown')


class ResourceStatusDBFailure(ResourceStatusDBTestCase):

  def test_InvalidStatus(self):
    self.assertRaises(InvalidStatus, self.rsDB.addOrModifySite, 'CNAF', 'T1', 'INFN-FERRARA', 'BadStatus', 'test reason', datetime.datetime.utcnow(), 'testOP', datetime.datetime.utcnow() + datetime.timedelta(minutes=10))
    self.assertRaises(InvalidStatus, self.rsDB._addSiteRow, 'Ferrara', 'T2', 'INFN-FERRARA', 'Actives', 'reasons', datetime.datetime.utcnow(), datetime.datetime.utcnow(), datetime.datetime.utcnow() + datetime.timedelta(minutes=10), 'Federico')
    self.assertRaises(InvalidStatus, self.rsDB.addOrModifyService, 'Computing@CERN', 'Computing', 'CERN', 'BadStatus', 'test reason', datetime.datetime.utcnow(), 'testOP', datetime.datetime.utcnow() + datetime.timedelta(minutes=10))
    self.assertRaises(InvalidStatus, self.rsDB._addServiceRow, 'Computing@CERN', 'Computing', 'CERN', 'Actives', 'reasons', datetime.datetime.utcnow(), datetime.datetime.utcnow(), datetime.datetime.utcnow() + datetime.timedelta(minutes=10), 'Federico')
    self.assertRaises(InvalidStatus, self.rsDB.addOrModifyResource, 'CE01', 'CE', 'Computing', 'CNAF', 'INFN-T1', 'BadStatus', 'test reason', datetime.datetime.utcnow(), 'testOP', datetime.datetime.utcnow() + datetime.timedelta(minutes=10))
    self.assertRaises(InvalidStatus, self.rsDB._addResourcesRow, 'CE01', 'CE', 'Computing', 'Computing@CERN', 'Ferrara', 'Actives', 'reasons', datetime.datetime.utcnow(), datetime.datetime.utcnow(), datetime.datetime.utcnow() + datetime.timedelta(minutes=10), 'Federico')

  def test_NotAllowedDate(self):
    from DIRAC.ResourceStatusSystem.DB.ResourceStatusDB import NotAllowedDate
    self.assertRaises(NotAllowedDate, self.rsDB.addOrModifySite, 'CNAF', 'T1', 'INFN-FERRARA', 'Active', 'test reason', datetime.datetime.utcnow(), 'testOP', datetime.datetime.utcnow() - datetime.timedelta(minutes=10))
    self.assertRaises(NotAllowedDate, self.rsDB.addOrModifyService, 'Computing@CERN', 'Computing', 'CERN', 'Active', 'test reason', datetime.datetime.utcnow(), 'testOP', datetime.datetime.utcnow() - datetime.timedelta(minutes=10))
    self.assertRaises(NotAllowedDate, self.rsDB.addOrModifyResource, 'CE01', 'CE', 'Computing', 'CERN', 'INFN-T1', 'Active', 'test reason', datetime.datetime.utcnow(), 'testOP', datetime.datetime.utcnow() - datetime.timedelta(minutes=10))

  def test_DBFail(self):
    self.mock_DB._query.return_value = {'OK': False, 'Message': 'boh'}
    self.mock_DB._update.return_value = {'OK': False, 'Message': 'boh'}
    from DIRAC.ResourceStatusSystem.DB.ResourceStatusDB import RSSDBException

    self.assertRaises(RSSDBException, self.rsDB.addOrModifySite, 'CNAF', 'T1', 'INFN-FERRARA', 'Banned', 'test reason', datetime.datetime.utcnow(), 'testOP', datetime.datetime.utcnow() + datetime.timedelta(minutes=10))
    self.assertRaises(RSSDBException, self.rsDB.setSiteStatus, 'CNAF', 'Active', 'reasons', 'Federico')
    self.assertRaises(RSSDBException, self.rsDB._addSiteRow, 'Ferrara', 'T2', 'INFN-FERRARA', 'Active', 'reasons', datetime.datetime.utcnow(), datetime.datetime.utcnow(), datetime.datetime.utcnow() + datetime.timedelta(minutes=10), 'Federico')
    self.assertRaises(RSSDBException, self.rsDB._addSiteHistoryRow, 'Ferrara', 'Active', 'reasons', datetime.datetime.utcnow(), datetime.datetime.utcnow(), datetime.datetime.utcnow() + datetime.timedelta(minutes=10), 'Federico')

    self.assertRaises(RSSDBException, self.rsDB.addOrModifyService, 'Computing@CERN', 'Computing', 'CERN', 'Banned', 'test reason', datetime.datetime.utcnow(), 'testOP', datetime.datetime.utcnow() + datetime.timedelta(minutes=10))
    self.assertRaises(RSSDBException, self.rsDB.setServiceStatus, 'Computing@CERN', 'Active', 'reasons', 'Federico')
    self.assertRaises(RSSDBException, self.rsDB._addServiceRow, 'Computing@CERN', 'Computing', 'Ferrara', 'Active', 'reasons', datetime.datetime.utcnow(), datetime.datetime.utcnow(), datetime.datetime.utcnow() + datetime.timedelta(minutes=10), 'Federico')
    self.assertRaises(RSSDBException, self.rsDB._addServiceHistoryRow, 'Computing@CERN', 'Active', 'reasons', datetime.datetime.utcnow(), datetime.datetime.utcnow(), datetime.datetime.utcnow() + datetime.timedelta(minutes=10), 'Federico')

    self.assertRaises(RSSDBException, self.rsDB.addOrModifyResource, 'CE01', 'T1', 'Computing', 'CNAF', 'INFN-T1', 'Banned', 'test reason', datetime.datetime.utcnow(), 'testOP', datetime.datetime.utcnow() + datetime.timedelta(minutes=10))
    self.assertRaises(RSSDBException, self.rsDB.setResourceStatus, 'CE01', 'Active', 'reasons', 'Federico')
    self.assertRaises(RSSDBException, self.rsDB._addResourcesRow, 'CE01', 'CE', 'Computing', 'Ferrara', 'INFN-FE', 'Active', 'reasons', datetime.datetime.utcnow(), datetime.datetime.utcnow(), datetime.datetime.utcnow() + datetime.timedelta(minutes=10), 'Federico')
    self.assertRaises(RSSDBException, self.rsDB._addResourcesHistoryRow, 'CE01', 'Active', 'reasons', datetime.datetime.utcnow(), datetime.datetime.utcnow(), datetime.datetime.utcnow() + datetime.timedelta(minutes=10), 'Federico')

    self.assertRaises(RSSDBException, self.rsDB.getStatusList)
    self.assertRaises(RSSDBException, self.rsDB.getEndings, 'Resources')
    self.assertRaises(RSSDBException, self.rsDB.getTablesWithHistory)
    self.assertRaises(RSSDBException, self.rsDB.addStatus, '')
    self.assertRaises(RSSDBException, self.rsDB.removeStatus, '')
    self.assertRaises(RSSDBException, self.rsDB.setDateEnd, 'Site', 'CNAF', datetime.datetime.utcnow())
    self.assertRaises(RSSDBException, self.rsDB.setMonitoredToBeChecked, 'Service', 'Site', 'CNAF')
    self.assertRaises(RSSDBException, self.rsDB.rankRes, 'Site', 30)
    self.assertRaises(RSSDBException, self.rsDB.getServiceStats, 'xxx')
    self.assertRaises(RSSDBException, self.rsDB.getResourceStats, 'Site', 'xxx')
    self.assertRaises(RSSDBException, self.rsDB.getStorageElementsStats, 'Site', 'xxx', "Read")
    self.assertRaises(RSSDBException, self.rsDB.getCountries, 'Site')
    #print self.defaultTestResult()

    for g in ['Site', 'Service', 'Resource']:
      self.assertRaises(RSSDBException, self.rsDB.getTypesList, g)
      self.assertRaises(RSSDBException, self.rsDB.removeType, g, 'xx')

    for g in ValidRes:
      self.assertRaises(RSSDBException, self.rsDB.getMonitoredsList, g)
      self.assertRaises(RSSDBException, self.rsDB.getStuffToCheck, g,CS.getTypedDictRootedAt("CheckingFreqs/SitesFreqs"),3)
      self.assertRaises(RSSDBException, self.rsDB.removeRow, g, 'xx', datetime.datetime.utcnow())
      self.assertRaises(RSSDBException, self.rsDB.setLastMonitoredCheckTime, g, 'xxx')
      self.assertRaises(RSSDBException, self.rsDB.setMonitoredReason, g, 'xxx', 'x', 'x')
#    self.assertRaises(RSSDBException, self.rsDB.syncWithCS)

if __name__ == '__main__':

  suite = unittest.defaultTestLoader.loadTestsFromTestCase(ResourceStatusDBTestCase)
  suite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(ResourceStatusDBSuccess))
  suite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(ResourceStatusDBFailure))
  unittest.TextTestRunner(verbosity=2).run(suite)
