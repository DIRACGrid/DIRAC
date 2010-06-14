import unittest
from datetime import datetime
from DIRAC.ResourceStatusSystem.Utilities.mock import Mock
from DIRAC.Core.LCG.GOCDBClient import GOCDBClient
from DIRAC.Core.LCG.SLSClient import *
from DIRAC.Core.LCG.SAMResultsClient import *
from DIRAC.Core.LCG.GGUSTicketsClient import GGUSTicketsClient
#from DIRAC.ResourceStatusSystem.Utilities.Exceptions import *
#from DIRAC.ResourceStatusSystem.Utilities.Utils import *

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
    self.SAMCli = SAMResultsClient()
    self.GGUSCli = GGUSTicketsClient()

#############################################################################

class GOCDBClientSuccess(ClientsTestCase):

  def test_getStatus(self):
    for granularity in ('Site', 'Resource'):
      res = self.GOCCli.getStatus(granularity, 'XX')['Value']
      self.assertEqual(res, None)
      res = self.GOCCli.getStatus(granularity, 'XX', datetime.utcnow())['Value']
      self.assertEqual(res, None)
      res = self.GOCCli.getStatus(granularity, 'XX', datetime.utcnow(), 12)['Value']
      self.assertEqual(res, None)
      
    res = self.GOCCli.getStatus('Site', 'pic')['Value']
    self.assertEqual(res, None)
    
  def test_getServiceEndpointInfo(self):
    for granularity in ('hostname', 'sitename', 'roc', 
                        'country', 'service_type', 'monitored'):
      res = self.GOCCli.getServiceEndpointInfo(granularity, 'XX')['Value']
      self.assertEqual(res, [])
  
#############################################################################

class SAMResultsClientSuccess(ClientsTestCase):

  def test_getStatus(self):
    res = self.SAMCli.getStatus('Resource', 'grid0.fe.infn.it', 'INFN-FERRARA')['Value']
    self.assertEqual(res, {'SS':'ok'})
    res = self.SAMCli.getStatus('Resource', 'grid0.fe.infn.it', 'INFN-FERRARA', ['ver'])['Value']
    self.assertEqual(res, {'ver':'ok'})
    res = self.SAMCli.getStatus('Resource', 'grid0.fe.infn.it', 'INFN-FERRARA', ['LHCb CE-lhcb-os', 'PilotRole'])['Value']
    self.assertEqual(res, {'PilotRole':'ok', 'LHCb CE-lhcb-os':'ok'})
    res = self.SAMCli.getStatus('Resource', 'grid0.fe.infn.it', 'INFN-FERRARA', ['wrong'])['Value']
    self.assertEqual(res, None)
    res = self.SAMCli.getStatus('Resource', 'grid0.fe.infn.it', 'INFN-FERRARA', ['ver', 'wrong'])['Value']
    self.assertEqual(res, {'ver':'ok'})
    res = self.SAMCli.getStatus('Resource', 'grid0.fe.infn.it', 'INFN-FERRARA')['Value']
    self.assertEqual(res, {'SS':'ok'})

    res = self.SAMCli.getStatus('Site', 'INFN-FERRARA')['Value']
    self.assertEqual(res, {'SiteStatus':'ok'})
      
#############################################################################

#class SAMResultsClientFailure(ClientsTestCase):
#
#  def test_getStatus(self):
#    self.failUnlessRaises(NoSAMTests, self.SAMCli.getStatus, 'Resource', 'XX', 'INFN-FERRARA')

#############################################################################

class SLSClientSuccess(ClientsTestCase):

  def test_getAvailabilityStatus(self):
    res = self.SLSCli.getAvailabilityStatus('RAL-LHCb_FAILOVER')['Value']
    self.assertEqual(res, 100)

  def test_getServiceInfo(self):
    res = self.SLSCli.getServiceInfo('CASTORLHCB_LHCBMDST', ["Volume to be recallled GB"])['Value']
    self.assertEqual(res["Volume to be recallled GB"], 0.0)

#############################################################################

#class SLSClientFailure(ClientsTestCase):
#
#  def test_getStatus(self):
#    self.failUnlessRaises(NoServiceException, self.SLSCli.getAvailabilityStatus, 'XX')

#############################################################################

class GGUSTicketsClientSuccess(ClientsTestCase):
  
  def test_getTicketsList(self):
    res = self.GGUSCli.getTicketsList('INFN-CAGLIARI')['Value']
    self.assertEqual(res[0]['open'], 0)


#############################################################################

if __name__ == '__main__':
  suite = unittest.defaultTestLoader.loadTestsFromTestCase(ClientsTestCase)
  suite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(GOCDBClientSuccess))
#  suite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(GOCDBClient_Failure))
  suite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(SAMResultsClientSuccess))
#  suite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(SAMResultsClientFailure))
  suite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(SLSClientSuccess))
#  suite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(SLSClientFailure))
  suite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(GGUSTicketsClientSuccess))
  testResult = unittest.TextTestRunner(verbosity=2).run(suite)