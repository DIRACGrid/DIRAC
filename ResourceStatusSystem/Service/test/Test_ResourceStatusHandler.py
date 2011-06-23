import unittest

from DIRAC.Core.Base import Script
Script.parseCommandLine()

from DIRAC.ResourceStatusSystem.Utilities.mock import Mock
import DIRAC.ResourceStatusSystem.test.fake_RequestHandler
import DIRAC.ResourceStatusSystem.test.fake_rsDB
import DIRAC.ResourceStatusSystem.test.fake_Logger
from DIRAC.ResourceStatusSystem.Utilities.Exceptions import *
from DIRAC.ResourceStatusSystem.Utilities.Utils import *

class ResourceStatusHandlerTestCase(unittest.TestCase):
  """ Base class for the ResourceStatusHandlerTestCase test cases
  """
  def setUp(self):
    import sys
    sys.modules["DIRAC.Core.DISET.RequestHandler"] = DIRAC.ResourceStatusSystem.test.fake_RequestHandler
    sys.modules["DIRAC.ResourceStatusSystem.DB.ResourceStatusDB"] = DIRAC.ResourceStatusSystem.test.fake_rsDB
    sys.modules["DIRAC"] = DIRAC.ResourceStatusSystem.test.fake_Logger
    from DIRAC.ResourceStatusSystem.Service.ResourceStatusHandler import ResourceStatusHandler, initializeResourceStatusHandler

    a = Mock()
    initializeResourceStatusHandler(a)
    self.rsh = ResourceStatusHandler('', '', '')

    self.mock_command = Mock()

class ResourceStatusHandlerSuccess(ResourceStatusHandlerTestCase):

#############################################################################
# Sites functions
#############################################################################

  def test_export_setSiteStatus(self):
    for status in ValidStatus:
      res = self.rsh.export_setSiteStatus('XX', status, 'reason', 'Op')
      self.assert_(res['OK'])

  def test_export_addOrModifySite(self):
    for status in ValidStatus:
      res = self.rsh.export_addOrModifySite('XX', 'XX', 'XX', status, 'reason', 'dateEffective', 'OP', '')
      self.assert_(res['OK'])

  def test_export_removeSite(self):
    res = self.rsh.export_removeSite('')
    self.assert_(res['OK'])

  def test_export_getSitesHistory(self):
    res = self.rsh.export_getSitesHistory('')
    self.assert_(res['OK'])

  def test_export_getSiteTypeList(self):
    res = self.rsh.export_getSiteTypeList()
    self.assert_(res['OK'])

  def test_export_getSitesList(self):
    res = self.rsh.export_getSitesList()
    self.assert_(res['OK'])

  def test_export_getSitesStatusWeb(self):
    res = self.rsh.export_getSitesStatusWeb({}, [], 0, 500)
    self.assert_(res['OK'])


#############################################################################
# Services functions
#############################################################################

  def test_export_setServiceStatus(self):
    for status in ValidStatus:
      res = self.rsh.export_setServiceStatus('XX', status, 'reason', 'Op')
      self.assert_(res['OK'])

  def test_export_addOrModifyService(self):
    for status in ValidStatus:
      res = self.rsh.export_addOrModifyService('XX', 'XX', 'description', status, 'reason', 'dateEffective', 'OP', '')
      self.assert_(res['OK'])

  def test_export_removeService(self):
    res = self.rsh.export_removeService('')
    self.assert_(res['OK'])

  def test_export_getServicesHistory(self):
    res = self.rsh.export_getServicesHistory('')
    self.assert_(res['OK'])

  def test_export_getServiceTypeList(self):
    res = self.rsh.export_getServiceTypeList()
    self.assert_(res['OK'])

  def test_export_getServicesList(self):
    res = self.rsh.export_getServicesList()
    self.assert_(res['OK'])

  def test_export_getServicesStatusWeb(self):
    res = self.rsh.export_getServicesStatusWeb({}, [], 0, 500)
    self.assert_(res['OK'])

  def test_export_getServiceStats(self):
    res = self.rsh.export_getServiceStats('ZZ')
    self.assert_(res['OK'])



#############################################################################
# Resources functions
#############################################################################

  def test_export_setResourceStatus(self):
    for status in ValidStatus:
      res = self.rsh.export_setResourceStatus('XX', status, 'reason', 'Op')
      self.assert_(res['OK'])

  def test_export_addOrModifyResource(self):
    for status in ValidStatus:
      res = self.rsh.export_addOrModifyResource('resourceName', 'resourceType', 'Computing', 'siteName', 'gridSiteName',
                                                status, 'reason', 'dateEffective', 'operatorCode', 'dateEnd')
      self.assert_(res['OK'])

  def test_export_removeResource(self):
    res = self.rsh.export_removeResource('')
    self.assert_(res['OK'])

  def test_export_getResourcesList(self):
    res = self.rsh.export_getResourcesList()
    self.assert_(res['OK'])

  def test_export_getCEsList(self):
    res = self.rsh.export_getResourcesList()
    self.assert_(res['OK'])

  def test_export_getResourcesStatusWeb(self):
    res = self.rsh.export_getResourcesStatusWeb({}, [], 0, 500)
    self.assert_(res['OK'])

  def test_export_getResourcesHistory(self):
    res = self.rsh.export_getResourcesHistory('')
    self.assert_(res['OK'])

  def test_export_getResourceTypeList(self):
    res = self.rsh.export_getResourceTypeList()
    self.assert_(res['OK'])

  def test_export_getresourceStats(self):
    res = self.rsh.export_getResourceStats('Service', 'ZZ')
    self.assert_(res['OK'])
    res = self.rsh.export_getResourceStats('Site', 'ZZ')
    self.assert_(res['OK'])


#############################################################################
# StorageElements functions
#############################################################################

  def test_export_setStorageElementStatus(self):
    for status in ValidStatus:
      res = self.rsh.export_setStorageElementStatus('XX', status, 'reason', 'Op', "Read")
      self.assert_(res['OK'])
      res = self.rsh.export_setStorageElementStatus('XX', status, 'reason', 'Op', "Write")
      self.assert_(res['OK'])

  def test_export_addOrModifyStorageElement(self):
    for status in ValidStatus:
      res = self.rsh.export_addOrModifyStorageElement('XX', 'XX', 'XX', status, 'reason', 'dateEffective', 'OP', '', "Read")
      self.assert_(res['OK'])
      res = self.rsh.export_addOrModifyStorageElement('XX', 'XX', 'XX', status, 'reason', 'dateEffective', 'OP', '', "Write")
      self.assert_(res['OK'])

  def test_export_removeStorageElement(self):
    res = self.rsh.export_removeStorageElement('', "Read")
    self.assert_(res['OK'])
    res = self.rsh.export_removeStorageElement('', "Write")
    self.assert_(res['OK'])

  def test_export_getStorageElementsHistory(self):
    res = self.rsh.export_getStorageElementsHistory('', "Read")
    self.assert_(res['OK'])
    res = self.rsh.export_getStorageElementsHistory('', "Write")
    self.assert_(res['OK'])


  def test_export_getStorageElementsList(self):
    res = self.rsh.export_getStorageElementsList("Read")
    self.assert_(res['OK'])
    res = self.rsh.export_getStorageElementsList("Write")
    self.assert_(res['OK'])

  def test_export_getStorageElementsStatusWeb(self):
    res = self.rsh.export_getStorageElementsStatusWeb({}, [], 0, 500, "Read")
    self.assert_(res['OK'])
    res = self.rsh.export_getStorageElementsStatusWeb({}, [], 0, 500, "Write")
    self.assert_(res['OK'])

  def test_export_getStorageElementsStats(self):
    res = self.rsh.export_getStorageElementsStats('Service', 'ZZ', "Read")
    self.assert_(res['OK'])
    res = self.rsh.export_getStorageElementsStats('Resource', 'ZZ', "Read")
    self.assert_(res['OK'])
    res = self.rsh.export_getStorageElementsStats('Service', 'ZZ', "Write")
    self.assert_(res['OK'])
    res = self.rsh.export_getStorageElementsStats('Resource', 'ZZ', "Write")
    self.assert_(res['OK'])



#############################################################################
# Mixed functions
#############################################################################

  def test_export_getStatusList(self):
    res = self.rsh.export_getStatusList()
    self.assert_(res['OK'])

  def test_export_getCountries(self):
    res = self.rsh.export_getCountries('Site')
    self.assert_(res['OK'])

  def test_export_getPeriods(self):
    for granularity in ValidRes:
      for status in ValidStatus:
        res = self.rsh.export_getPeriods(granularity, 'XX', status, 20)
        self.assert_(res['OK'])

#  def test_export_getPolicyRes(self):
#    res = self.rsh.export_getPolicyRes('XX', 'XX', False)
#    self.assert_(res['OK'])

#  def test_export_getDownTimesWeb(self):
#    res = self.rsh.export_getDownTimesWeb({}, [], 0, 500)
#    self.assert_(res['OK'])

#  def test_export_getCachedAccountingResult(self):
#    res = self.rsh.export_getCachedAccountingResult('XX', 'YY', 'ZZ')
#    self.assert_(res['OK'])

#  def test_export_getCachedResult(self):
#    res = self.rsh.export_getCachedResult('XX', 'YY', 'ZZ', 1)
#    self.assert_(res['OK'])

#  def test_export_getCachedIDs(self):
#    res = self.rsh.export_getCachedIDs('XX', 'YY')
#    self.assert_(res['OK'])

  def test_export_getGeneralName(self):
    for g_1 in ValidRes:
      for g_2 in ValidRes:
        res = self.rsh.export_getGeneralName(g_1, 'XX', g_2)
        self.assert_(res['OK'])

  def test_export_getGridSiteName(self):
    for g in ValidRes:
      res = self.rsh.export_getGridSiteName(g, 'XX')
      self.assert_(res['OK'])

  def test_export_reAssignToken(self):
    for g in ValidRes:
      res = self.rsh.export_reAssignToken(g, 'XX', 'Fede')
      self.assert_(res['OK'])

  def test_export_extendToken(self):
    for g in ValidRes:
      res = self.rsh.export_extendToken(g, 'XX', 8)
      self.assert_(res['OK'])

  def test_export_whatIs(self):
    res = self.rsh.export_whatIs('XX')
    self.assert_(res['OK'])


#  def test_export_enforcePolicies(self):
#    for g in ValidRes:
#      res = self.rsh.export_enforcePolicies(g, 'XX')

#  def test_export_publisher(self):
#    for g in ValidRes:
#      res = self.rsh.export_publisher(g, 'XX')
#      print res
#      self.assert_(res['OK'])

if __name__ == '__main__':
  suite = unittest.defaultTestLoader.loadTestsFromTestCase(ResourceStatusHandlerTestCase)
  suite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(ResourceStatusHandlerSuccess))
  testResult = unittest.TextTestRunner(verbosity=2).run(suite)
