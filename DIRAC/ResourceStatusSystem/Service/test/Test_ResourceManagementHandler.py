#import unittest
#
#from DIRAC.Core.Base import Script
#Script.parseCommandLine()
#
#from DIRAC.ResourceStatusSystem.Utilities.mock import Mock
#import DIRAC.ResourceStatusSystem.test.fake_RequestHandler
#import DIRAC.ResourceStatusSystem.test.fake_rsDB
#import DIRAC.ResourceStatusSystem.test.fake_rmDB
#import DIRAC.ResourceStatusSystem.test.fake_Logger
#from DIRAC.ResourceStatusSystem.Utilities.Exceptions import *
#from DIRAC.ResourceStatusSystem.Utilities.Utils import *
#
#class ResourceManagementHandlerTestCase(unittest.TestCase):
#  """ Base class for the ResourceManagementHandlerTestCase test cases
#  """
#  def setUp(self):
#    import sys
#    sys.modules["DIRAC.Core.DISET.RequestHandler"] = DIRAC.ResourceStatusSystem.test.fake_RequestHandler
#    sys.modules["DIRAC.ResourceStatusSystem.DB.ResourceStatusDB"] = DIRAC.ResourceStatusSystem.test.fake_rsDB
#    sys.modules["DIRAC.ResourceStatusSystem.DB.ResourceManagementDB"] = DIRAC.ResourceStatusSystem.test.fake_rmDB
#    sys.modules["DIRAC"] = DIRAC.ResourceStatusSystem.test.fake_Logger
#    from DIRAC.ResourceStatusSystem.Service.ResourceManagementHandler import ResourceManagementHandler, initializeResourceManagementHandler
#
#    a = Mock()
#    initializeResourceManagementHandler(a)
#    self.rmh = ResourceManagementHandler('', '', '')
#
#    self.mock_command = Mock()
#
#class ResourceManagementHandlerSuccess(ResourceManagementHandlerTestCase):
#
##############################################################################
## Mixed functions
##############################################################################
#
#  def test_export_getStatusList(self):
#    res = self.rmh.export_getStatusList()
#    self.assert_(res['OK'])
#
#  def test_export_getPolicyRes(self):
#    res = self.rmh.export_getPolicyRes('XX', 'XX', False)
#    self.assert_(res['OK'])
#
#  def test_export_getDownTimesWeb(self):
#    res = self.rmh.export_getDownTimesWeb({}, [], 0, 500)
#    self.assert_(res['OK'])
#
#  def test_export_getCachedAccountingResult(self):
#    res = self.rmh.export_getCachedAccountingResult('XX', 'YY', 'ZZ')
#    self.assert_(res['OK'])
#
#  def test_export_getCachedResult(self):
#    res = self.rmh.export_getCachedResult('XX', 'YY', 'ZZ', 1)
#    self.assert_(res['OK'])
#
#  def test_export_getCachedIDs(self):
#    res = self.rmh.export_getCachedIDs('XX', 'YY')
#    self.assert_(res['OK'])
#
##  def test_export_enforcePolicies(self):
##    for g in ValidRes:
##      res = self.rmh.export_enforcePolicies(g, 'XX')
##      self.assert_(res['OK'])
#
##  def test_export_publisher(self):
##    for g in ValidRes:
##      res = self.rmh.export_publisher(g, 'XX')
##      #print res
##      self.assert_(res['OK'])
#
#if __name__ == '__main__':
#  suite = unittest.defaultTestLoader.loadTestsFromTestCase(ResourceManagementHandlerTestCase)
#  suite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(ResourceManagementHandlerSuccess))
#  testResult = unittest.TextTestRunner(verbosity=2).run(suite)
#
##def run(output):
##  suite = unittest.defaultTestLoader.loadTestsFromTestCase(ResourceManagementHandlerTestCase)
##  suite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(ResourceManagementHandlerSuccess))
##
##  #print suite.getTestCaseNames(ResourceStatusDBSuccess)
##
##  output.write('Test_ResourceManagementHandler\n')
##
##  testResult = unittest.TextTestRunner(output,verbosity=2).run(suite)
##
##  output.write('#######################################################\n')
##  #print testResult
##
##  return testResult
