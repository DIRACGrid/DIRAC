import unittest
from datetime import datetime
from DIRAC.ResourceStatusSystem.Utilities.mock import Mock
from DIRAC.ResourceStatusSystem.Client.GOCDBClient import GOCDBClient
from DIRAC.ResourceStatusSystem.Client.SLSClient import *
from DIRAC.ResourceStatusSystem.Client.JobsClient import JobsClient
from DIRAC.ResourceStatusSystem.Client.PilotsClient import PilotsClient
from DIRAC.ResourceStatusSystem.Client.ResourceStatusClient import ResourceStatusClient
from DIRAC.ResourceStatusSystem.Client.SAMResultsClient import *
from DIRAC.ResourceStatusSystem.Client.GGUSTicketsClient import GGUSTicketsClient
from DIRAC.ResourceStatusSystem.Utilities.Exceptions import *
from DIRAC.ResourceStatusSystem.Utilities.Utils import *

#############################################################################

class ClientsTestCase(unittest.TestCase):
  """ Base class for the clients test cases
  """
  def setUp(self):

    from DIRAC.Core.Base.Script import parseCommandLine
    parseCommandLine()
    
    self.mockRSS = Mock()
    
    self.GOCCli = GOCDBClient()
    self.SLSCli = SLSClient()
    self.RSCli = ResourceStatusClient(serviceIn = self.mockRSS)
    self.SAMCli = SAMResultsClient()
    self.PilotsCli = PilotsClient()
    self.JobsCli = JobsClient()
    self.GGUSCli = GGUSTicketsClient()

#############################################################################

class ResourceStatusClientSuccess(ClientsTestCase):

  def test_getPeriods(self):
    self.mockRSS.getPeriods.return_value = {'OK':True, 'Value':[]}
    for granularity in ValidRes:
      for status in ValidStatus:
        res = self.RSCli.getPeriods(granularity, 'XX', status, 20)
        self.assertEqual(res['Periods'], [])
  
  def test_getServiceStats(self):
    self.mockRSS.getServiceStats.return_value = {'OK':True, 'Value':[]}
    res = self.RSCli.getServiceStats('Site', '')
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
    
  def test_getMonitoredStatus(self):
    self.mockRSS.getSitesStatusWeb.return_value = {'OK':True, 'Value': {'Records': [['', '', '', '', 'Active', '']]}}
    self.mockRSS.getServicesStatusWeb.return_value = {'OK':True, 'Value':{'Records': [['', '', '', '', 'Active', '']]}}
    self.mockRSS.getResourcesStatusWeb.return_value = {'OK':True, 'Value':{'Records': [['', '', '', '', '', 'Active', '']]}}
    self.mockRSS.getStorageElementsStatusWeb.return_value = {'OK':True, 'Value':{'Records': [['', '', '', '', 'Active', '']]}}
    for g in ValidRes:
      res = self.RSCli.getMonitoredStatus(g, '')
      self.assertEqual(res, 'Active')

  def test_getServiceStats(self):
    self.mockRSS.getCachedResult.return_value = {'OK':True, 'Value':[]}
    res = self.RSCli.getCachedResult('XX', 'pippo', 'ZZ')
    self.assertEqual(res, [])
  

 
#############################################################################

class JobsClientSuccess(ClientsTestCase):

  def test_getJobsSimpleEff(self):
    res = self.JobsCli.getJobsSimpleEff('XX')
    self.assertEqual(res, None)

#############################################################################

class PilotsClientSuccess(ClientsTestCase):

#  def test_getPilotsStats(self):
#    self.mockRSS.getPeriods.return_value = {'OK':True, 'Value':[]}
#    for granularity in ValidRes:
#      for status in ValidStatus:
#        res = self.RSCli.getPeriods(granularity, 'XX', status, 20)
#        self.assertEqual(res['Periods'], [])
         
  def test_getPilotsSimpleEff(self):
    #self.mockRSS.getPilotsSimpleEff.return_value = {'OK':True, 'Value':{'Records': [['', '', 0, 3L, 0, 0, 0, 283L, 66L, 0, 0, 352L, '1.00', '81.25', 'Fair', 'Yes']]}}
    res = self.PilotsCli.getPilotsSimpleEff('Site', 'LCG.Ferrara.it')
    self.assertEqual(res, None)
    res = self.PilotsCli.getPilotsSimpleEff('Resource', 'grid0.fe.infn.it', 'LCG.Ferrara.it')
    self.assertEqual(res, None)
    res = self.PilotsCli.getPilotsSimpleEff('Resource', 'grid0.fe.infn.it')
    self.assertEqual(res, None)
    
#############################################################################

if __name__ == '__main__':
  suite = unittest.defaultTestLoader.loadTestsFromTestCase(ClientsTestCase)
  suite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(ResourceStatusClientSuccess))
  suite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(JobsClientSuccess))
  suite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(PilotsClientSuccess))
  testResult = unittest.TextTestRunner(verbosity=2).run(suite)