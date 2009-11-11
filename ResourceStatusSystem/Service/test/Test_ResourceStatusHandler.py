import unittest
import sys
from types import *
from DIRAC.ResourceStatusSystem.Utilities.mock import Mock
import DIRAC.ResourceStatusSystem.test.fake_RequestHandler
import DIRAC.ResourceStatusSystem.test.fake_rsDB
import DIRAC.ResourceStatusSystem.test.fake_Logger
from DIRAC.ResourceStatusSystem.Utilities.Exceptions import *
from DIRAC.ResourceStatusSystem.Utilities.Utils import *

class ResourceStatusHandlerTestCase(unittest.TestCase):
  """ Base class for the RS2history test cases
  """
  def setUp(self):
    sys.modules["DIRAC.Core.DISET.RequestHandler"] = DIRAC.ResourceStatusSystem.test.fake_RequestHandler
    sys.modules["DIRAC.ResourceStatusSystem.DB.ResourceStatusDB"] = DIRAC.ResourceStatusSystem.test.fake_rsDB
    sys.modules["DIRAC.LoggingSystem.Client.Logger"] = DIRAC.ResourceStatusSystem.test.fake_Logger
    from DIRAC.ResourceStatusSystem.Service.ResourceStatusHandler import ResourceStatusHandler, initializeResourceStatusHandler
    
    a = Mock()
    initializeResourceStatusHandler(a)
    self.rsh = ResourceStatusHandler('', '', '')
            
class ResourceStatusHandlerSuccess(ResourceStatusHandlerTestCase):
  
  def test_export_setSiteStatus(self):
    for status in ValidStatus:
      res = self.rsh.export_setSiteStatus('XX', status, 'reason', 'Op')
      self.assert_(res['OK'])

  def test_export_setResourceStatus(self):
    for status in ValidStatus:
      res = self.rsh.export_setResourceStatus('XX', status, 'reason', 'Op')
      self.assert_(res['OK'])

  def test_export_addOrModifySite(self):
    for status in ValidStatus:
      res = self.rsh.export_addOrModifySite('XX', 'XX', 'description', status, 'reason', 'dateEffective', 'OP', '')
      self.assert_(res['OK'])

  def test_export_addOrModifyResource(self):
    for status in ValidStatus:
      res = self.rsh.export_addOrModifyResource('resourceName', 'resourceType', 'siteName', status, 'reason', 'dateEffective', 'operatorCode', 'dateEnd')
      self.assert_(res['OK'])

  def test_export_addSiteType(self):
    res = self.rsh.export_addSiteType('')
    self.assert_(res['OK'])

  def test_export_addResourceType(self):
    res = self.rsh.export_addResourceType('')
    self.assert_(res['OK'])

  def test_export_addStatus(self):
    res = self.rsh.export_addStatus('')
    self.assert_(res['OK'])

  def test_export_removeSite(self):
    res = self.rsh.export_removeSite('')
    self.assert_(res['OK'])

  def test_export_removeResource(self):
    res = self.rsh.export_removeResource('')
    self.assert_(res['OK'])

  def test_export_removeSiteType(self):
    res = self.rsh.export_removeSiteType('')
    self.assert_(res['OK'])

  def test_export_removeResourceType(self):
    res = self.rsh.export_removeResourceType('')
    self.assert_(res['OK'])

  def test_export_removeStatus(self):
    res = self.rsh.export_removeStatus('')
    self.assert_(res['OK'])

  def test_export_getSitesList(self):
    res = self.rsh.export_getSitesList()
    self.assert_(res['OK'])
    
  def test_export_getSitesStatusWeb(self):
    res = self.rsh.export_getSitesStatusWeb({}, [], 0, 500)
    self.assert_(res['OK'])
    
  def test_export_getSitesListByStatus(self):
    res = self.rsh.export_getSitesListByStatus('')
    self.assert_(res['OK'])
    
  def test_export_getResourcesList(self):
    res = self.rsh.export_getResourcesList()
    self.assert_(res['OK'])
    
  def test_export_getResourcesStatusWeb(self):
    res = self.rsh.export_getResourcesStatusWeb({}, [], 0, 500)
    self.assert_(res['OK'])
    
  def test_export_getSitesListByStatus(self):
    res = self.rsh.export_getResourcesListByStatus('')
    self.assert_(res['OK'])
    
  def test_export_getSitesHistory(self):
    res = self.rsh.export_getSitesHistory('')
    self.assert_(res['OK'])
    
  def test_export_getResourcesHistory(self):
    res = self.rsh.export_getResourcesHistory('')
    self.assert_(res['OK'])
    
  def test_export_getPeriods(self):
    for granularity in ValidRes:
      for status in ValidStatus:
        res = self.rsh.export_getPeriods(granularity, 'XX', status, 20)
        self.assert_(res['OK'])
    
  def test_export_getServiceStats(self):
    res = self.rsh.export_getServiceStats('')
    self.assert_(res['OK'])

  def test_export_getSiteTypeList(self):
    res = self.rsh.export_getSiteTypeList()
    self.assert_(res['OK'])
        
  def test_export_getResourceTypeList(self):
    res = self.rsh.export_getResourceTypeList()
    self.assert_(res['OK'])
        
  def test_export_getStatusList(self):
    res = self.rsh.export_getStatusList()
    self.assert_(res['OK'])
        

if __name__ == '__main__':
  suite = unittest.defaultTestLoader.loadTestsFromTestCase(ResourceStatusHandlerTestCase)
  suite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(ResourceStatusHandlerSuccess))
  testResult = unittest.TextTestRunner(verbosity=2).run(suite)
