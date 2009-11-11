""" UnitTest class for Client Commands classes
"""

import unittest
from DIRAC.ResourceStatusSystem.Utilities.mock import Mock
from DIRAC.ResourceStatusSystem.Client.Command.ClientsInvoker import ClientsInvoker
from DIRAC.ResourceStatusSystem.Client.Command.GOCDBStatus_Command import GOCDBStatus_Command
from DIRAC.ResourceStatusSystem.Client.Command.Res2SiteStatus_Command import Res2SiteStatus_Command
from DIRAC.ResourceStatusSystem.Client.Command.Pilots_Command import *
from DIRAC.ResourceStatusSystem.Client.Command.Jobs_Command import *
from DIRAC.ResourceStatusSystem.Client.Command.SAMResults_Command import SAMResults_Command
from DIRAC.ResourceStatusSystem.Client.Command.GGUSTickets_Command import GGUSTickets_Command
from DIRAC.ResourceStatusSystem.Client.Command.Service_Command import ServiceStats_Command
from DIRAC.ResourceStatusSystem.Client.Command.RS_Command import *
from DIRAC.ResourceStatusSystem.Utilities.Exceptions import *
from DIRAC.ResourceStatusSystem.Utilities.Utils import *

class ClientsCommandsTestCase(unittest.TestCase):
  """ Base class for the ClientsCommands test cases
  """
  def setUp(self):
    self.mock_command = Mock()
    self.ci = ClientsInvoker()
    self.GOCDBS_C = GOCDBStatus_Command()
    self.mock_client = Mock()
    self.R2SS_C = Res2SiteStatus_Command()
    self.PE_C = PilotsEff_Command()
    self.PS_C = PilotsStats_Command()
    self.JE_C = JobsEff_Command()
    self.JS_C = JobsStats_Command()
    self.SC_C = SystemCharge_Command()
    self.JES_C = JobsEffSimple_Command()
    self.PES_C = PilotsEffSimple_Command()
    self.SAMR_C = SAMResults_Command()
    self.RSP_C = RSPeriods_Command()
    self.GGUS_C = GGUSTickets_Command()
    self.SeSt_C = ServiceStats_Command()

class ClientsInvokerSuccess(ClientsCommandsTestCase):

  def test_setCommand(self):
    self.ci.setCommand(self.mock_command)
    self.assertEqual(self.ci.command, self.mock_command)

  def test_doCommand(self):
    self.mock_command.doCommand.return_value = {'DT': 'OUTAGE', 'Enddate': '2009-09-09 13:00:00'}
    self.ci.setCommand(self.mock_command)
    for granularity in ValidRes:
      res = self.ci.doCommand((granularity, 'XX'))
      self.assertEqual(res['DT'], 'OUTAGE')
    self.mock_command.doCommand.return_value = {'DT': 'AT_RISK', 'Enddate': '2009-09-09 13:00:00'}
    self.ci.setCommand(self.mock_command)
    for granularity in ValidRes:
      res = self.ci.doCommand((granularity, 'XX'))
      self.assertEqual(res['DT'], 'AT_RISK')
    self.mock_command.doCommand.return_value = None
    self.ci.setCommand(self.mock_command)
    for granularity in ValidRes:
      res = self.ci.doCommand((granularity, 'XX'))
      self.assertEqual(res, None)
        

class ClientsInvokerFailure(ClientsCommandsTestCase):
  
  def test_badArgs(self):
    self.failUnlessRaises(Exception, self.ci.doCommand, [''])
    self.failUnlessRaises(Exception, self.ci.doCommand, None)
     

class GOCDBStatus_CommandSuccess(ClientsCommandsTestCase):
  
  def test_doCommand(self):

    for granularity in ValidRes:
      args = (granularity, 'XX')
      self.mock_client.getStatus.return_value =  {'DT':'OUTAGE', 'Enddate':''}
      res = self.GOCDBS_C.doCommand(args, clientIn = self.mock_client)
      self.assertEqual(res['DT'], 'OUTAGE')
      self.mock_client.getStatus.return_value = {'DT':'AT_RISK', 'Enddate':''}
      res = self.GOCDBS_C.doCommand(args, clientIn = self.mock_client)
      self.assertEqual(res['DT'], 'AT_RISK')
      self.mock_client.getStatus.return_value =  None
      res = self.GOCDBS_C.doCommand(args, clientIn = self.mock_client)
      self.assertEqual(res, None)

    

class GOCDBStatus_CommandFailure(ClientsCommandsTestCase):

  def test_badArgs(self):
    self.failUnlessRaises(InvalidRes, self.GOCDBS_C.doCommand, ('sites', ''))
    self.failUnlessRaises(TypeError, self.GOCDBS_C.doCommand, None)
     

class Res2SiteStatus_CommandSuccess(ClientsCommandsTestCase):
  
  def test_doCommand(self):

    for granularity in ValidRes:
      args = (granularity, 'XX')
      self.mock_client.getStatus.return_value =  {}
      res = self.R2SS_C.doCommand(args, clientIn = self.mock_client)
      self.assertEqual()
      self.mock_client.getStatus.return_value = {}
      res = self.R2SS_C.doCommand(args, clientIn = self.mock_client)
      self.assertEqual()
      self.mock_client.getStatus.return_value =  None
      res = self.R2SS_C.doCommand(args, clientIn = self.mock_client)
      self.assertEqual(res, None)

    

class Res2SiteStatus_CommandFailure(ClientsCommandsTestCase):

  def test_badArgs(self):
    self.failUnlessRaises(TypeError, self.R2SS_C.doCommand, None)
     

class PilotsEff_CommandSuccess(ClientsCommandsTestCase):
  
  def test_doCommand(self):

    for granularity in ValidRes:
      args = (granularity, 'XX', ['', ''] )
      for pe in (0, 20, 40, 60, 80):
        self.mock_client.getPilotsEff.return_value =  {'PilotsEff':pe}
        res = self.PE_C.doCommand(args, clientIn = self.mock_client)
        self.assertEqual(res['PilotsEff'], pe)

    

class PilotsEff_CommandFailure(ClientsCommandsTestCase):
  
  def test_badArgs(self):
    self.failUnlessRaises(InvalidRes, self.PE_C.doCommand, ('sites', '', []))
    self.failUnlessRaises(TypeError, self.PE_C.doCommand, None)
     

class PilotsStats_CommandSuccess(ClientsCommandsTestCase):
  
  def test_doCommand(self):

    for granularity in ValidRes:
      args = (granularity, 'XX', [])
      for pe in (0, 20, 40, 60, 80):
        self.mock_client.getPilotsStats.return_value =  {'PilotsEff':pe}
        res = self.PS_C.doCommand(args, clientIn = self.mock_client)
        self.assertEqual(res['PilotsEff'], pe)

    

class PilotsStats_CommandFailure(ClientsCommandsTestCase):
  
  def test_badArgs(self):
    self.failUnlessRaises(InvalidRes, self.PS_C.doCommand, ('sites', '', []))
    self.failUnlessRaises(TypeError, self.PS_C.doCommand, None)
     

class PilotsEffSimple_CommandSuccess(ClientsCommandsTestCase):
  
  def test_doCommand(self):

    for granularity in ValidRes:
      args = (granularity, 'XX')
      for pe in ('Good', 'Fair', 'Poor', 'Bad', 'Idle'):
        self.mock_client.getPilotsSimpleEff.return_value =  {'PilotsEff':pe}
        res = self.PES_C.doCommand(args, clientIn = self.mock_client)
        self.assertEqual(res['PilotsEff'], pe)

    

class PilotsEffSimple_CommandFailure(ClientsCommandsTestCase):
  
  def test_badArgs(self):
    self.failUnlessRaises(InvalidRes, self.PES_C.doCommand, ('sites', ''))
    self.failUnlessRaises(TypeError, self.PES_C.doCommand, None)
     


class JobsEff_CommandSuccess(ClientsCommandsTestCase):
  
  def test_doCommand(self):

    for granularity in ValidRes:
      args = (granularity, 'XX', ['', ''] )
      for pe in (0, 20, 40, 60, 80):
        self.mock_client.getJobsEff.return_value =  {'JobsEff':pe}
        res = self.JE_C.doCommand(args, clientIn = self.mock_client)
        self.assertEqual(res['JobsEff'], pe)

    

class JobsEff_CommandFailure(ClientsCommandsTestCase):
  
  def test_badArgs(self):
    self.failUnlessRaises(InvalidRes, self.JE_C.doCommand, ('sites', '', []))
    self.failUnlessRaises(TypeError, self.JE_C.doCommand, None)
     

class JobsStats_CommandSuccess(ClientsCommandsTestCase):
  
  def test_doCommand(self):

    for granularity in ValidRes:
      args = (granularity, 'XX', [])
      for pe in (0, 20, 40, 60, 80):
        self.mock_client.getJobsStats.return_value =  {'MeanProcessedJobs': pe}
        res = self.JS_C.doCommand(args, clientIn = self.mock_client)
        self.assertEqual(res['MeanProcessedJobs'], pe)

    

class JobsStats_CommandFailure(ClientsCommandsTestCase):
  
  def test_badArgs(self):
    self.failUnlessRaises(InvalidRes, self.JS_C.doCommand, ('sites', '', []))
    self.failUnlessRaises(TypeError, self.JS_C.doCommand, None)
     

class SystemCharge_CommandSuccess(ClientsCommandsTestCase):
  
  def test_doCommand(self):

    self.mock_client.getSystemCharge.return_value =  {'LastHour': 50, 'anHourBefore': 30}
    res = self.SC_C.doCommand(clientIn = self.mock_client)
    self.assertEqual(res['LastHour'], 50)
    self.assertEqual(res['anHourBefore'], 30)
    

class JobsEffSimple_CommandSuccess(ClientsCommandsTestCase):
  
  def test_doCommand(self):

    for granularity in ValidRes:
      args = (granularity, 'XX')
      for pe in ('Good', 'Fair', 'Poor', 'Bad', 'Idle'):
        self.mock_client.getJobsSimpleEff.return_value =  {'JobsEff':pe}
        res = self.JES_C.doCommand(args, clientIn = self.mock_client)
        self.assertEqual(res['JobsEff'], pe)

    

class JobsEffSimple_CommandFailure(ClientsCommandsTestCase):
  
  def test_badArgs(self):
    self.failUnlessRaises(InvalidRes, self.JES_C.doCommand, ('sites', ''))
    self.failUnlessRaises(TypeError, self.JES_C.doCommand, None)
     


class SAMResults_CommandSuccess(ClientsCommandsTestCase):
  
  def test_doCommand(self):

    args = ('XX', 'XX')
    for status in ['ok', 'down', 'na', 'degraded', 'partial', 'maint']:
      self.mock_client.getStatus.return_value =  {'Status':status}
      res = self.SAMR_C.doCommand(args, clientIn = self.mock_client)
      self.assertEqual(res['Status'], status)
    

class SAMResults_CommandFailure(ClientsCommandsTestCase):

  def test_badArgs(self):
    self.failUnlessRaises(TypeError, self.SAMR_C.doCommand, None)
     


class GGUSTickets_CommandSuccess(ClientsCommandsTestCase):
  
  def test_doCommand(self):

    for n in [1,3]:
      self.mock_client.getTicketsNumber.return_value =  {'GGUSTickets':n}
      res = self.GGUS_C.doCommand(('XX', ), clientIn = self.mock_client)
      self.assertEqual(res['GGUSTickets'], n)
    

class GGUSTickets_CommandFailure(ClientsCommandsTestCase):

  def test_badArgs(self):
    self.failUnlessRaises(TypeError, self.GGUS_C.doCommand, None)
     


class RSPeriods_CommandSuccess(ClientsCommandsTestCase):
  
  def test_doCommand(self):

    for granularity in ValidRes:
      args = (granularity, 'XX', 'XX', 20)
      self.mock_client.getPeriods.return_value =  {'Periods':[]}
      res = self.RSP_C.doCommand(args, clientIn = self.mock_client)
      self.assertEqual(res['Periods'], [])
    

class RSPeriods_CommandFailure(ClientsCommandsTestCase):

  def test_badArgs(self):
    self.failUnlessRaises(InvalidRes, self.RSP_C.doCommand, ('sites', '', ''), 20)
    self.failUnlessRaises(TypeError, self.RSP_C.doCommand, None, 20)
     

class ServiceStats_CommandSuccess(ClientsCommandsTestCase):
  
  def test_doCommand(self):

    self.mock_client.getServiceStats.return_value = {}
    for service in ValidService:
      res = self.SeSt_C.doCommand((service, ''), clientIn = self.mock_client)
      self.assertEqual(res, {})
      

class ServiceStats_CommandFailure(ClientsCommandsTestCase):

  def test_badArgs(self):
    self.failUnlessRaises(TypeError, self.SeSt_C.doCommand, None)
     



if __name__ == '__main__':
  suite = unittest.defaultTestLoader.loadTestsFromTestCase(ClientsCommandsTestCase)
  suite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(ClientsInvokerSuccess))
  suite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(ClientsInvokerFailure))
  suite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(GOCDBStatus_CommandSuccess))
  suite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(GOCDBStatus_CommandFailure))
#  suite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(Res2SiteStatus_CommandSuccess))
#  suite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(Res2SiteStatus_CommandFailure))
  suite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(PilotsEff_CommandSuccess))
  suite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(PilotsEff_CommandFailure))
  suite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(PilotsStats_CommandSuccess))
  suite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(PilotsStats_CommandFailure))
  suite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(PilotsEffSimple_CommandSuccess))
  suite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(PilotsEffSimple_CommandFailure))
  suite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(JobsEff_CommandSuccess))
  suite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(JobsEff_CommandFailure))
  suite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(JobsStats_CommandSuccess))
  suite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(JobsStats_CommandFailure))
  suite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(SystemCharge_CommandSuccess))
  suite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(JobsEffSimple_CommandSuccess))
  suite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(JobsEffSimple_CommandFailure))
  suite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(SAMResults_CommandSuccess))
  suite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(SAMResults_CommandFailure))
  suite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(GGUSTickets_CommandSuccess))
  suite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(GGUSTickets_CommandFailure))
  suite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(RSPeriods_CommandSuccess))
  suite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(RSPeriods_CommandFailure))
  suite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(ServiceStats_CommandSuccess))
  suite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(ServiceStats_CommandFailure))
  testResult = unittest.TextTestRunner(verbosity=2).run(suite)