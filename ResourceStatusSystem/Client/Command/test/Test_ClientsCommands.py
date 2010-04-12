""" UnitTest class for Client Commands classes
"""

import unittest
from datetime import datetime 

from DIRAC.ResourceStatusSystem.Utilities.mock import Mock
from DIRAC.ResourceStatusSystem.Client.Command.ClientsInvoker import ClientsInvoker
from DIRAC.ResourceStatusSystem.Client.Command.GOCDBStatus_Command import *
from DIRAC.ResourceStatusSystem.Client.Command.Pilots_Command import *
from DIRAC.ResourceStatusSystem.Client.Command.Jobs_Command import *
from DIRAC.ResourceStatusSystem.Client.Command.SAMResults_Command import SAMResults_Command
from DIRAC.ResourceStatusSystem.Client.Command.GGUSTickets_Command import *
from DIRAC.ResourceStatusSystem.Client.Command.RS_Command import *
from DIRAC.ResourceStatusSystem.Client.Command.DataOperations_Command import TransferQuality_Command
from DIRAC.ResourceStatusSystem.Client.Command.DIRACAccounting_Command import DIRACAccounting_Command
from DIRAC.ResourceStatusSystem.Client.Command.SLS_Command import *
from DIRAC.ResourceStatusSystem.Utilities.Exceptions import *
from DIRAC.ResourceStatusSystem.Utilities.Utils import *


#############################################################################

class ClientsCommandsTestCase(unittest.TestCase):
  """ Base class for the ClientsCommands test cases
  """
  def setUp(self):
    
    from DIRAC.Core.Base.Script import parseCommandLine
    parseCommandLine()
    
    self.mock_command = Mock()
    self.ci = ClientsInvoker()
    self.GOCDBS_C = GOCDBStatus_Command()
    self.GOCDBI_C = GOCDBInfo_Command()
    self.mock_client = Mock()
    self.mock_client_2 = Mock()
    self.PE_C = PilotsEff_Command()
    self.PS_C = PilotsStats_Command()
    self.JE_C = JobsEff_Command()
    self.JS_C = JobsStats_Command()
    self.SC_C = SystemCharge_Command()
    self.JES_C = JobsEffSimple_Command()
    self.PES_C = PilotsEffSimple_Command()
    self.SAMR_C = SAMResults_Command()
    self.RSP_C = RSPeriods_Command()
    self.GGUS_O_C = GGUSTickets_Open()
    self.GGUS_L_C = GGUSTickets_Link()
    self.GGUS_I_C = GGUSTickets_Info()
    self.SeSt_C = ServiceStats_Command()
    self.ReSt_C = ResourceStats_Command()
    self.StElSt_C = StorageElementsStats_Command()
    self.MS_C = MonitoredStatus_Command()
    self.DQ_C = TransferQuality_Command()
    self.DA_C = DIRACAccounting_Command()
    self.SLSS_C = SLSStatus_Command()

#############################################################################

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
        

#############################################################################

class ClientsInvokerFailure(ClientsCommandsTestCase):
  
  def test_badArgs(self):
    self.failUnlessRaises(Exception, self.ci.doCommand, [''])
    self.failUnlessRaises(Exception, self.ci.doCommand, None)
     
#############################################################################

class GOCDBStatus_CommandSuccess(ClientsCommandsTestCase):
  
  def test_doCommand(self):

    for granularity in ValidRes:
      args = (granularity, 'XX')
      for retVal in ({'DT':'OUTAGE', 'Enddate':'', 'Type': 'OnGoing'}, 
                     [{'DT':'OUTAGE', 'Enddate':'', 'Type': 'Programmed', 'InHours' : 8}, 
                      {'DT':'OUTAGE', 'Enddate':'', 'Type': 'OnGoing'}]):
        self.mock_client.getStatus.return_value = retVal  
        res = self.GOCDBS_C.doCommand(args, clientIn = self.mock_client)
        self.assertEqual(res['DT'], 'OUTAGE')
      self.mock_client.getStatus.return_value = {'DT':'OUTAGE', 'Enddate':'', 
                                                 'Type': 'Programmed', 'InHours' : 8}
      res = self.GOCDBS_C.doCommand(args, clientIn = self.mock_client)
      self.assertEqual(res['DT'], 'OUTAGE in 8 hours')
      self.mock_client.getStatus.return_value = {'DT':'AT_RISK', 'Enddate':'', 'Type': 'OnGoing'}
      res = self.GOCDBS_C.doCommand(args, clientIn = self.mock_client)
      self.assertEqual(res['DT'], 'AT_RISK')
      self.mock_client.getStatus.return_value =  None
      res = self.GOCDBS_C.doCommand(args, clientIn = self.mock_client)
      self.assertEqual(res['DT'], None)

#############################################################################

class GOCDBStatus_CommandFailure(ClientsCommandsTestCase):

  def test_badArgs(self):
    self.failUnlessRaises(InvalidRes, self.GOCDBS_C.doCommand, ('sites', ''))
    self.failUnlessRaises(TypeError, self.GOCDBS_C.doCommand, None)
     
#############################################################################

class GOCDBInfo_CommandSuccess(ClientsCommandsTestCase):
  
  def test_doCommand(self):

    for granularity in ValidRes:
      args = (granularity, 'XX')
      self.mock_client.getInfo.return_value =  ['XXXX', 'YYYYYYYYY']
      res = self.GOCDBI_C.doCommand(args, clientIn = self.mock_client)
      self.assertEqual(res, ['XXXX', 'YYYYYYYYY'])

#############################################################################

class GOCDBInfo_CommandFailure(ClientsCommandsTestCase):

  def test_badArgs(self):
    self.failUnlessRaises(InvalidRes, self.GOCDBI_C.doCommand, ('sites', ''))
    self.failUnlessRaises(TypeError, self.GOCDBI_C.doCommand, None)
     
#############################################################################

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

#############################################################################
    

class Res2SiteStatus_CommandFailure(ClientsCommandsTestCase):

  def test_badArgs(self):
    self.failUnlessRaises(TypeError, self.R2SS_C.doCommand, None)
     
#############################################################################

class PilotsEff_CommandSuccess(ClientsCommandsTestCase):
  
  def test_doCommand(self):

    for granularity in ValidRes:
      args = (granularity, 'XX', ['', ''] )
      for pe in (0, 20, 40, 60, 80):
        self.mock_client.getPilotsEff.return_value =  {'PilotsEff':pe}
        res = self.PE_C.doCommand(args, clientIn = self.mock_client)
        self.assertEqual(res['PilotsEff'], pe)

#############################################################################
    

class PilotsEff_CommandFailure(ClientsCommandsTestCase):
  
  def test_badArgs(self):
    self.failUnlessRaises(InvalidRes, self.PE_C.doCommand, ('sites', '', []))
    self.failUnlessRaises(TypeError, self.PE_C.doCommand, None)
     
#############################################################################

class PilotsStats_CommandSuccess(ClientsCommandsTestCase):
  
  def test_doCommand(self):

    for granularity in ValidRes:
      args = (granularity, 'XX', [])
      for pe in (0, 20, 40, 60, 80):
        self.mock_client.getPilotsStats.return_value =  {'PilotsEff':pe}
        res = self.PS_C.doCommand(args, clientIn = self.mock_client)
        self.assertEqual(res['PilotsEff'], pe)

#############################################################################

class PilotsStats_CommandFailure(ClientsCommandsTestCase):
  
  def test_badArgs(self):
    self.failUnlessRaises(InvalidRes, self.PS_C.doCommand, ('sites', '', []))
    self.failUnlessRaises(TypeError, self.PS_C.doCommand, None)
     
#############################################################################

class PilotsEffSimple_CommandSuccess(ClientsCommandsTestCase):
  
  def test_doCommand(self):

    for granularity in ('Site', 'Service', 'Resource'):
      args = (granularity, 'XX')
      for pe in ('Good', 'Fair', 'Poor', 'Bad', 'Idle'):
        self.mock_client.getPilotsSimpleEff.return_value =  {'PilotsEff':pe}
        res = self.PES_C.doCommand(args, clientIn = self.mock_client)
        self.assertEqual(res['PilotsEff'], pe)

#############################################################################
    
class PilotsEffSimple_CommandFailure(ClientsCommandsTestCase):
  
  def test_badArgs(self):
    self.failUnlessRaises(InvalidRes, self.PES_C.doCommand, ('sites', ''))
    self.failUnlessRaises(TypeError, self.PES_C.doCommand, None)
     
#############################################################################

class JobsEff_CommandSuccess(ClientsCommandsTestCase):
  
  def test_doCommand(self):

    for granularity in ValidRes:
      args = (granularity, 'XX', ['', ''] )
      for pe in (0, 20, 40, 60, 80):
        self.mock_client.getJobsEff.return_value =  {'JobsEff':pe}
        res = self.JE_C.doCommand(args, clientIn = self.mock_client)
        self.assertEqual(res['JobsEff'], pe)

#############################################################################
    
class JobsEff_CommandFailure(ClientsCommandsTestCase):
  
  def test_badArgs(self):
    self.failUnlessRaises(InvalidRes, self.JE_C.doCommand, ('sites', '', []))
    self.failUnlessRaises(TypeError, self.JE_C.doCommand, None)
     
#############################################################################

class JobsStats_CommandSuccess(ClientsCommandsTestCase):
  
  def test_doCommand(self):

    for granularity in ValidRes:
      args = (granularity, 'XX', [])
      for pe in (0, 20, 40, 60, 80):
        self.mock_client.getJobsStats.return_value =  {'MeanProcessedJobs': pe}
        res = self.JS_C.doCommand(args, clientIn = self.mock_client)
        self.assertEqual(res['MeanProcessedJobs'], pe)

#############################################################################
    
class JobsStats_CommandFailure(ClientsCommandsTestCase):
  
  def test_badArgs(self):
    self.failUnlessRaises(InvalidRes, self.JS_C.doCommand, ('sites', '', []))
    self.failUnlessRaises(TypeError, self.JS_C.doCommand, None)
     
#############################################################################

class SystemCharge_CommandSuccess(ClientsCommandsTestCase):
  
  def test_doCommand(self):

    self.mock_client.getSystemCharge.return_value =  {'LastHour': 50, 'anHourBefore': 30}
    res = self.SC_C.doCommand(clientIn = self.mock_client)
    self.assertEqual(res['LastHour'], 50)
    self.assertEqual(res['anHourBefore'], 30)
    
#############################################################################

class JobsEffSimple_CommandSuccess(ClientsCommandsTestCase):
  
  def test_doCommand(self):

    for granularity in ('Site', 'Service'):
      args = (granularity, 'XX')
      for pe in ('Good', 'Fair', 'Poor', 'Bad', 'Idle'):
        self.mock_client.getJobsSimpleEff.return_value =  {'JobsEff':pe}
        res = self.JES_C.doCommand(args, clientIn = self.mock_client)
        self.assertEqual(res['JobsEff'], pe)

#############################################################################
   
class JobsEffSimple_CommandFailure(ClientsCommandsTestCase):
  
  def test_badArgs(self):
    self.failUnlessRaises(InvalidRes, self.JES_C.doCommand, ('sites', ''))
    self.failUnlessRaises(TypeError, self.JES_C.doCommand, None)
     
#############################################################################

class SAMResults_CommandSuccess(ClientsCommandsTestCase):
  
  def test_doCommand(self):

    args = ('Site', 'XX')
    self.mock_client.getStatus.return_value =  {'Status':None}
    res = self.SAMR_C.doCommand(args, clientIn = self.mock_client)
    self.assertEqual(res['SAM-Status'], {'Status': None})
    args = ('Site', 'XX', 'XXX')
    res = self.SAMR_C.doCommand(args, clientIn = self.mock_client)
    self.assertEqual(res['SAM-Status'], {'Status':None})

    args = ('Resource', 'XX')
    self.mock_client.getStatus.return_value =  {'Status':None}
    res = self.SAMR_C.doCommand(args, clientIn = self.mock_client)
    args = ('Resource', 'XX', 'XXX')
    res = self.SAMR_C.doCommand(args, clientIn = self.mock_client)
    self.assertEqual(res['SAM-Status'], {'Status':None})
    self.assertEqual(res['SAM-Status'], {'Status': None})

    args = ('Resource', 'grid0.fe.infn.it', None, ['aa', 'bbb'])
    for status in ['ok', 'down', 'na', 'degraded', 'partial', 'maint']:
      self.mock_client.getStatus.return_value =  {'Status':status}
      res = self.SAMR_C.doCommand(args, clientIn = self.mock_client)
      self.assertEqual(res['SAM-Status']['Status'], status)
    args = ('Resource', 'grid0.fe.infn.it', 'LCG.Ferrara.it', ['aa', 'bbb'])
    for status in ['ok', 'down', 'na', 'degraded', 'partial', 'maint']:
      self.mock_client.getStatus.return_value =  {'Status':status}
      res = self.SAMR_C.doCommand(args, clientIn = self.mock_client)
      self.assertEqual(res['SAM-Status']['Status'], status)

#############################################################################

class SAMResults_CommandFailure(ClientsCommandsTestCase):

  def test_badArgs(self):
    self.failUnlessRaises(TypeError, self.SAMR_C.doCommand, None)
     
#############################################################################

class GGUSTickets_Open_CommandSuccess(ClientsCommandsTestCase):
  
  def test_doCommand(self):

    self.mock_client.getTicketsList.return_value = ({'terminal': 211, 'open': 2}, 
                                                    'https://gus.fzk.de/ws/ticket_search.php?')
    res = self.GGUS_O_C.doCommand(('XX', ), clientIn = self.mock_client)
    self.assertEqual(res['OpenT'], 2)
    
#############################################################################

class GGUSTickets_All_CommandFailure(ClientsCommandsTestCase):

  def test_badArgs(self):
    self.failUnlessRaises(TypeError, self.GGUS_O_C.doCommand, None)
    self.failUnlessRaises(TypeError, self.GGUS_L_C.doCommand, None)
    self.failUnlessRaises(TypeError, self.GGUS_I_C.doCommand, None)
     
#############################################################################

class GGUSTickets_Link_CommandSuccess(ClientsCommandsTestCase):
  
  def test_doCommand(self):

    self.mock_client.getTicketsList.return_value = ({'terminal': 211, 'open': 2}, 
                                                    'https://gus.fzk.de/ws/ticket_search.php?', 
                                                    {56220: 'jobs failed at gridce2.pi.infn.it INFN-PISA', 
                                                     55948: 'Jobs Failed at INFN-PISA'})
    res = self.GGUS_L_C.doCommand(('XX', ), clientIn = self.mock_client)
    self.assertEqual(res['GGUS_Link'], 'https://gus.fzk.de/ws/ticket_search.php?')
    
#############################################################################

class GGUSTickets_Info_CommandSuccess(ClientsCommandsTestCase):
  
  def test_doCommand(self):

    self.mock_client.getTicketsList.return_value = ({'terminal': 211, 'open': 2}, 
                                                    'https://gus.fzk.de/ws/ticket_search.php?', 
                                                    {56220: 'jobs failed at gridce2.pi.infn.it INFN-PISA', 
                                                     55948: 'Jobs Failed at INFN-PISA'})
    res = self.GGUS_I_C.doCommand(('XX', ), clientIn = self.mock_client)
    self.assertEqual(res['GGUS_Info'], {56220: 'jobs failed at gridce2.pi.infn.it INFN-PISA', 
                                                     55948: 'Jobs Failed at INFN-PISA'})
    
#############################################################################


class RSPeriods_CommandSuccess(ClientsCommandsTestCase):
  
  def test_doCommand(self):

    for granularity in ValidRes:
      args = (granularity, 'XX', 'XX', 20)
      self.mock_client.getPeriods.return_value =  {'Periods':[]}
      res = self.RSP_C.doCommand(args, clientIn = self.mock_client)
      self.assertEqual(res['Periods'], [])
    
#############################################################################

class RSPeriods_CommandFailure(ClientsCommandsTestCase):

  def test_badArgs(self):
    self.failUnlessRaises(InvalidRes, self.RSP_C.doCommand, ('sites', '', ''), 20)
    self.failUnlessRaises(TypeError, self.RSP_C.doCommand, None, 20)
     
#############################################################################

class ServiceStats_CommandSuccess(ClientsCommandsTestCase):
  
  def test_doCommand(self):

    self.mock_client.getServiceStats.return_value = {}
    for g in ValidRes:
      res = self.SeSt_C.doCommand((g, ''), clientIn = self.mock_client)
      self.assertEqual(res, {'stats':{}})
      
#############################################################################

class ServiceStats_CommandFailure(ClientsCommandsTestCase):

  def test_badArgs(self):
    self.failUnlessRaises(TypeError, self.SeSt_C.doCommand, None)
     
#############################################################################

class ResourceStats_CommandSuccess(ClientsCommandsTestCase):
  
  def test_doCommand(self):

    self.mock_client.getResourceStats.return_value = {}
    res = self.ReSt_C.doCommand(('Site', ''), clientIn = self.mock_client)
    self.assertEqual(res, {'stats':{}})
    res = self.ReSt_C.doCommand(('Service', ''), clientIn = self.mock_client)
    self.assertEqual(res, {'stats':{}})
      
#############################################################################

class ResourceStats_CommandFailure(ClientsCommandsTestCase):

  def test_badArgs(self):
    self.failUnlessRaises(TypeError, self.ReSt_C.doCommand, None)
     
#############################################################################

class StorageElementsStats_CommandSuccess(ClientsCommandsTestCase):
  
  def test_doCommand(self):

    self.mock_client.getStorageElementsStats.return_value = {}
    res = self.StElSt_C.doCommand(('Site', ''), clientIn = self.mock_client)
    self.assertEqual(res, {'stats': {}})
    res = self.StElSt_C.doCommand(('Resource', ''), clientIn = self.mock_client)
    self.assertEqual(res, {'stats': {}})
      
#############################################################################

class StorageElementsStats_CommandFailure(ClientsCommandsTestCase):

  def test_badArgs(self):
    self.failUnlessRaises(TypeError, self.StElSt_C.doCommand, None)
     
#############################################################################

class MonitoredStatus_CommandSuccess(ClientsCommandsTestCase):
  
  def test_doCommand(self):

    self.mock_client.getMonitoredStatus.return_value = 'Active'
    for g in ValidRes:
      res = self.MS_C.doCommand((g, ''), clientIn = self.mock_client)
      self.assertEqual(res, {'MonitoredStatus':'Active'})
      
#############################################################################

class MonitoredStatus_CommandFailure(ClientsCommandsTestCase):

  def test_badArgs(self):
    self.failUnlessRaises(TypeError, self.MS_C.doCommand, None)
     
#############################################################################

class TransferOperations_CommandSuccess(ClientsCommandsTestCase):
  
  def test_doCommand(self):

    self.mock_client.getQualityStats.return_value = {}
    res = self.DQ_C.doCommand(('StorageElement', 'XXX'), 
                              clientIn = self.mock_client)
    self.assertEqual(res, {})
    res = self.DQ_C.doCommand(('StorageElement', 'XXX', datetime.utcnow()), 
                              clientIn = self.mock_client)
    self.assertEqual(res, {})
    res = self.DQ_C.doCommand(('StorageElement', 'XXX', datetime.utcnow(), 
                               datetime.utcnow()), clientIn = self.mock_client)
    self.assertEqual(res, {})
      
#############################################################################

class TransferOperations_CommandFailure(ClientsCommandsTestCase):

  def test_badArgs(self):
    self.failUnlessRaises(TypeError, self.DQ_C.doCommand, None)

#############################################################################

class DIRACAccounting_CommandSuccess(ClientsCommandsTestCase):
  
  def test_doCommand(self):
    
    self.mock_client.getReport.return_value = {'OK': True, 
                                               'Value': {'data': 
                                                         {'SAM': 
                                                          {1268053200L: 0.011889755732000001, 
                                                           1268056800L: 0.011889755731900001}}, 
                                                           'granularity': 3600}}
    res = self.DA_C.doCommand(('Site', 'LCG.CERN.ch', 'Job', 'CPUEfficiency', 
                               {'Format': 'LastHours', 'hours': 24}, 
                               'JobType'), clientIn = self.mock_client)
    self.assertEqual(res['data']['SAM'], {1268053200L: 0.011889755732000001, 1268056800L: 0.011889755731900001})
     
#############################################################################


class SLSStatus_CommandSuccess(ClientsCommandsTestCase):
  
  def test_doCommand(self):
    
    for ret in (80, 10, 1, None):
      self.mock_client.getStatus.return_value = {'SLS':ret}
      for SE in ('CNAF-RAW', 'CNAF_MC_M-DST'):
        res = self.SLSS_C.doCommand(('StorageElement', SE), clientIn = self.mock_client)
        self.assertEqual(res['SLS'], ret)
      res = self.SLSS_C.doCommand(('Service', 'XX'), clientIn = self.mock_client)
      self.assertEqual(res['SLS'], ret) 

#############################################################################

class SLSStatus_CommandFailure(ClientsCommandsTestCase):
  
  def test_badArgs(self):
    self.failUnlessRaises(TypeError, self.SLSS_C.doCommand, None)
    self.failUnlessRaises(InvalidRes, self.SLSS_C.doCommand, ('sites', ''))

#############################################################################



#class Macros_CommandSuccess(ClientsCommandsTestCase):
#  
#  def test_doCommand(self):
#    
#    self.mock_client.getJobsSimpleEff.return_value =  {'JobsEff':'Good'}
#    self.mock_client_2.getStatus.return_value =  {'DT':'OUTAGE', 'Enddate':''}
#    
#    macroC = MacroCommand()
    
#############################################################################
    
if __name__ == '__main__':
  suite = unittest.defaultTestLoader.loadTestsFromTestCase(ClientsCommandsTestCase)
  suite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(ClientsInvokerSuccess))
  suite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(ClientsInvokerFailure))
  suite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(GOCDBStatus_CommandSuccess))
  suite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(GOCDBStatus_CommandFailure))
#  suite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(GOCDBInfo_CommandSuccess))
#  suite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(GOCDBInfo_CommandFailure))
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
  suite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(GGUSTickets_Open_CommandSuccess))
  suite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(GGUSTickets_Link_CommandSuccess))
  suite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(GGUSTickets_Info_CommandSuccess))
  suite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(GGUSTickets_All_CommandFailure))
  suite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(RSPeriods_CommandSuccess))
  suite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(RSPeriods_CommandFailure))
  suite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(ServiceStats_CommandSuccess))
  suite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(ServiceStats_CommandFailure))
  suite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(ResourceStats_CommandSuccess))
  suite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(ResourceStats_CommandFailure))
  suite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(StorageElementsStats_CommandSuccess))
  suite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(StorageElementsStats_CommandFailure))
  suite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(MonitoredStatus_CommandSuccess))
  suite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(MonitoredStatus_CommandFailure))
  suite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(TransferOperations_CommandSuccess))
  suite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(TransferOperations_CommandFailure))
  suite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(DIRACAccounting_CommandSuccess))
  suite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(SLSStatus_CommandSuccess))
  suite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(SLSStatus_CommandFailure))
  testResult = unittest.TextTestRunner(verbosity=2).run(suite)