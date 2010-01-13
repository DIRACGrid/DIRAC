### UnitTest class for GOCDBClient class

import unittest
from DIRAC.ResourceStatusSystem.Utilities.mock import Mock
from DIRAC.ResourceStatusSystem.Client.GOCDBClient import GOCDBClient
from DIRAC.ResourceStatusSystem.Client.JobsClient import JobsClient
from DIRAC.ResourceStatusSystem.Client.PilotsClient import PilotsClient
from DIRAC.ResourceStatusSystem.Client.ResourceStatusClient import ResourceStatusClient
from DIRAC.ResourceStatusSystem.Client.SAMResultsClient import SAMResultsClient
from DIRAC.ResourceStatusSystem.Client.DataOperationsClient import DataOperationsClient
from DIRAC.ResourceStatusSystem.Utilities.Exceptions import *
from DIRAC.ResourceStatusSystem.Utilities.Utils import *

class ClientsTestCase(unittest.TestCase):
  """ Base class for the clients test cases
  """
  def setUp(self):
    
    self.mockRSS = Mock()
    
    self.GOCCli = GOCDBClient()
    self.RSCli = ResourceStatusClient(serviceIn = self.mockRSS)
    self.SAMCli = SAMResultsClient()
    self.PilotsCli = PilotsClient()
    self.JobsCli = JobsClient()
    self.DOCli = DataOperationsClient()

class GOCDBClientSuccess(ClientsTestCase):

  def test_getStatus(self):
    for granularity in ValidRes:
      res = self.GOCCli.getStatus((granularity, 'XX'))
      self.assertEqual(res, None)
      
#    res = self.GOCCli.getStatus(('Site', 'pic'))
#    self.assertEqual(res, None)
    
  
class GOCDBClient_Failure(ClientsTestCase):
    
  def test_badArgs(self):
    self.failUnlessRaises(InvalidRes, self.GOCCli.getStatus, ('sites', ''))
     


class ResourceStatusClientSuccess(ClientsTestCase):

  def test_getPeriods(self):
    self.mockRSS.getPeriods.return_value = {'OK':True, 'Value':[]}
    for granularity in ValidRes:
      for status in ValidStatus:
        res = self.RSCli.getPeriods(granularity, 'XX', status, 20)
        self.assertEqual(res['Periods'], [])
  
  def test_getServiceStats(self):
    self.mockRSS.getServiceStats.return_value = {'OK':True, 'Value':[]}
    res = self.RSCli.getServiceStats('')
    self.assertEqual(res, [])
  
  def test_getResourceStats(self):
    self.mockRSS.getResourceStats.return_value = {'OK':True, 'Value':[]}
    res = self.RSCli.getResourceStats('Site', '')
    self.assertEqual(res, [])
    res = self.RSCli.getResourceStats('Service', '')
    self.assertEqual(res, [])
  
  def test_getStorageElementsStats(self):
    self.mockRSS.getStorageElementsStats.return_value = {'OK':True, 'Value':[]}
    res = self.RSCli.getStorageElementsStats('Site', '')
    self.assertEqual(res, [])
    res = self.RSCli.getStorageElementsStats('Resource', '')
    self.assertEqual(res, [])
  
class ResourceStatusClient_Failure(ClientsTestCase):
    
  def test_badArgs(self):
    self.failUnlessRaises(InvalidRes, self.RSCli.getPeriods, 'sites', '', '', 20)


class SAMResultsClientSuccess(ClientsTestCase):

  def test_getStatus(self):
    res = self.SAMCli.getStatus(('LCG.CERN.ch', 'ce104.cern.ch'))
    self.assertEqual(res['Status'], 'ok')
      

class JobsClientSuccess(ClientsTestCase):

  def test_getJobsSimpleEff(self):
    for granularity in ValidRes:
      self.JobsCli.getJobsSimpleEff(granularity, 'XX')

class PilotsClientSuccess(ClientsTestCase):

  def test_getPilotsStats(self):
    self.mockRSS.getPeriods.return_value = {'OK':True, 'Value':[]}
    for granularity in ValidRes:
      for status in ValidStatus:
        res = self.RSCli.getPeriods(granularity, 'XX', status, 20)
        self.assertEqual(res['Periods'], [])
         
#  def test_getPilotsEff(self):

class DataOperationsClientSuccess(ClientsTestCase):

  def test_getQualityStats(self):
    res = self.DOCli.getQualityStats('XX')
    self.assertEqual(res['TransferQuality'], None)
   
     

if __name__ == '__main__':
  suite = unittest.defaultTestLoader.loadTestsFromTestCase(ClientsTestCase)
  suite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(GOCDBClientSuccess))
  suite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(GOCDBClient_Failure))
  suite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(ResourceStatusClientSuccess))
  suite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(ResourceStatusClient_Failure))
  suite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(SAMResultsClientSuccess))
  suite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(JobsClientSuccess))
  suite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(PilotsClientSuccess))
  suite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(DataOperationsClientSuccess))
  testResult = unittest.TextTestRunner(verbosity=2).run(suite)