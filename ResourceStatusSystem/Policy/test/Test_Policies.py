""" UnitTest class for policy classes
"""

import unittest
from datetime import datetime

from DIRAC.ResourceStatusSystem.Utilities.mock import Mock
from DIRAC.ResourceStatusSystem.Policy.PolicyInvoker import PolicyInvoker
from DIRAC.ResourceStatusSystem.Policy.DT_Policy import DT_Policy
from DIRAC.ResourceStatusSystem.Policy.AlwaysFalse_Policy import AlwaysFalse_Policy 
#from DIRAC.ResourceStatusSystem.Policy.Res2SiteStatus_Policy import Res2SiteStatus_Policy 
from DIRAC.ResourceStatusSystem.Policy.PilotsEfficiency_Policy import PilotsEfficiency_Policy 
from DIRAC.ResourceStatusSystem.Policy.PilotsEfficiency_Simple_Policy import PilotsEfficiency_Simple_Policy
from DIRAC.ResourceStatusSystem.Policy.JobsEfficiency_Policy import JobsEfficiency_Policy
from DIRAC.ResourceStatusSystem.Policy.JobsEfficiency_Simple_Policy import JobsEfficiency_Simple_Policy
from DIRAC.ResourceStatusSystem.Policy.SAMResults_Policy import SAMResults_Policy 
from DIRAC.ResourceStatusSystem.Policy.GGUSTickets_Policy import GGUSTickets_Policy 
from DIRAC.ResourceStatusSystem.Policy.OnServicePropagation_Policy import OnServicePropagation_Policy 
from DIRAC.ResourceStatusSystem.Policy.OnSENodePropagation_Policy import OnSENodePropagation_Policy 
from DIRAC.ResourceStatusSystem.Policy.Propagation_Policy import Propagation_Policy 
from DIRAC.ResourceStatusSystem.Policy.OnStorageElementPropagation_Policy import OnStorageElementPropagation_Policy 
from DIRAC.ResourceStatusSystem.Policy.TransferQuality_Policy import TransferQuality_Policy 
from DIRAC.ResourceStatusSystem.Policy.SEOccupancy_Policy import SEOccupancy_Policy 
from DIRAC.ResourceStatusSystem.Policy.SEQueuedTransfers_Policy import SEQueuedTransfers_Policy 
from DIRAC.ResourceStatusSystem.Utilities.Exceptions import *
from DIRAC.ResourceStatusSystem.Utilities.Utils import *
from DIRAC.ResourceStatusSystem.Policy import Configurations

#############################################################################

class PoliciesTestCase(unittest.TestCase):
  """ Base class
  """
  def setUp(self):

    from DIRAC.Core.Base import Script
    Script.parseCommandLine() 
    
    self.mock_policy = Mock()
    self.mock_DB = Mock()
    self.pi = PolicyInvoker()
    self.DT_P = DT_Policy()
    self.AF_P = AlwaysFalse_Policy()
#    self.Res2SiteStatus_P = Res2SiteStatus_Policy()
    self.PE_P = PilotsEfficiency_Policy()
    self.JES_P = JobsEfficiency_Simple_Policy()
    self.SAMR_P = SAMResults_Policy()
    self.GGUS_P = GGUSTickets_Policy()
    self.OSP_P = OnServicePropagation_Policy()
    self.OSENP_P = OnSENodePropagation_Policy()
    self.P_P = Propagation_Policy()
    self.TQ_P = TransferQuality_Policy()
    self.SEO_P = SEOccupancy_Policy()
    self.SEQT_P = SEQueuedTransfers_Policy()
    self.OSEP_P = OnStorageElementPropagation_Policy()
    self.mock_command = Mock()
    self.mock_commandPeriods = Mock()
    self.mock_commandStats = Mock()
    self.mock_commandEff = Mock()
    self.mock_commandCharge = Mock()
    self.mock_propCommand = Mock()
    self.mock_siteStatusCommand = Mock()
  
#############################################################################

class PolicyInvokerSuccess(PoliciesTestCase):

  def test_setPolicy(self):
    self.pi.setPolicy(self.mock_policy)
    self.assertEqual(self.pi.policy, self.mock_policy)

  def test_evaluatePolicy(self):
    
    self.mock_policy.evaluate.return_value = {'Result':'Satisfied', 'Status':'Banned', 'Reason':"reason"}
    self.pi.setPolicy(self.mock_policy)
    for granularity in ValidRes:
      res = self.pi.evaluatePolicy()
      self.assertEqual(res['Result'], 'Satisfied')
    self.mock_policy.evaluate.return_value = {'Result':'Un-Satisfied'}
    self.pi.setPolicy(self.mock_policy)
    for granularity in ValidRes:
      res = self.pi.evaluatePolicy()
      self.assertEqual(res['Result'], 'Un-Satisfied')
    
#############################################################################

class PolicyInvokerFailure(PoliciesTestCase):
  
  def test_policyFail(self):
    self.mock_policy.evaluate.side_effect = RSSException()
    for granularity in ValidRes:
      self.failUnlessRaises(Exception, self.pi.evaluatePolicy)
        
#############################################################################

class DT_PolicySuccess(PoliciesTestCase):
  
  def test_evaluate(self):
    for granularity in ValidRes:
      for status in ValidStatus:
        args = (granularity, 'XX', status)
        for commandRes in ({'Result':{'DT':'OUTAGE', 'EndDate':''}}, 
                           {'Result':{'DT':'AT_RISK', 'EndDate':''}}, 
                           {'Result':{'DT':None}}, 
                           {'Result':'Unknown'}):
          self.mock_command.doCommand.return_value = commandRes
          self.DT_P.setArgs(args)
          self.DT_P.setCommand(self.mock_command)
          self.DT_P.setKnownInfo(commandRes)
          self.DT_P.setInfoName('Result')
          res = self.DT_P.evaluate()
          if commandRes in ({'Result':{'DT':'OUTAGE', 'EndDate':''}}, 
                            {'Result':{'DT':'AT_RISK', 'EndDate':''}}) and status == 'Active':
            self.assert_(res['SAT'])
          elif commandRes in ({'Result':{'DT':'OUTAGE', 'EndDate':''}}, 
                              {'Result':{'DT':None}}) and status in ('Probing', 'Bad'):
            self.assert_(res['SAT'])
          elif commandRes in ({'Result':{'DT':'AT_RISK', 'EndDate':''}}, 
                              {'Result':{'DT':None}}) and status == 'Banned':
            self.assert_(res['SAT'])
          elif commandRes == {'Result':'Unknown'}:
            self.assertEqual(res['SAT'], 'Unknown')
          else:
            self.assertFalse(res['SAT'])
          
          self.DT_P = DT_Policy()
          self.DT_P.setArgs(args)
          self.DT_P.setCommand(self.mock_command)
          res = self.DT_P.evaluate()
          if commandRes in ({'Result':{'DT':'OUTAGE', 'EndDate':''}}, 
                            {'Result':{'DT':'AT_RISK', 'EndDate':''}}) and status == 'Active':
            self.assert_(res['SAT'])
          elif commandRes in ({'Result':{'DT':'OUTAGE', 'EndDate':''}}, 
                              {'Result':{'DT':None}}) and status in ('Probing', 'Bad'):
            self.assert_(res['SAT'])
          elif commandRes in ({'Result':{'DT':'AT_RISK', 'EndDate':''}}, 
                              {'Result':{'DT':None}}) and status == 'Banned':
            self.assert_(res['SAT'])
          elif commandRes == {'Result':'Unknown'}:
            self.assertEqual(res['SAT'], 'Unknown')
          else:
            self.assertFalse(res['SAT'])
          
       
#############################################################################

class DT_Policy_Failure(PoliciesTestCase):
  
  def test_commandFail(self):
    self.mock_command.doCommand.side_effect = RSSException()
    for granularity in ValidRes:
      for status in ValidStatus:
        self.failUnlessRaises(Exception, self.DT_P.evaluate)

  def test_badArgs(self):
    for status in ValidStatus:
      self.failUnlessRaises(InvalidRes, self.DT_P.setArgs, ('sites', '', status))
    self.failUnlessRaises(TypeError, self.DT_P.setArgs, None )
     

#############################################################################

class Res2SiteStatus_PolicySuccess(PoliciesTestCase):
  
  def test_evaluate(self):
    for status in ValidStatus:
      args = ('XX', status)
      for clientRes in ():
        self.mock_command.doCommand.return_value = clientRes
        self.Res2SiteStatus_P.setArgs(args)
        self.Res2SiteStatus_P.setCommand(self.mock_command)
        self.Res2SiteStatus_P.setKnownInfo(commandRes)
        self.Res2SiteStatus_P.setInfoName('Result')
        res = self.DT_P.evaluate()
        if clientRes in ({'DT':'OUTAGE', 'EndDate':''}, {'DT':'AT_RISK', 'EndDate':''}) and status == 'Active':
          self.assert_(res['SAT'])
        elif clientRes in ({'DT':'OUTAGE', 'EndDate':''}, None) and status == 'Probing':
          self.assert_(res['SAT'])
        elif clientRes in ({'DT':'AT_RISK', 'EndDate':''}, None) and status == 'Banned':
          self.assert_(res['SAT'])
        else:
          self.assertFalse(res['SAT'])
        
        res = self.Res2SiteStatus_P.evaluate(args, commandIn = self.mock_command)
        if clientRes in ({'DT':'OUTAGE', 'EndDate':''}, {'DT':'AT_RISK', 'EndDate':''}) and status == 'Active':
          self.assert_(res['SAT'])
        elif clientRes in ({'DT':'OUTAGE', 'EndDate':''}, None) and status == 'Probing':
          self.assert_(res['SAT'])
        elif clientRes in ({'DT':'AT_RISK', 'EndDate':''}, None) and status == 'Banned':
          self.assert_(res['SAT'])
        else:
          self.assertFalse(res['SAT'])
        

#############################################################################

class Res2SiteStatus_Policy_Failure(PoliciesTestCase):
  
  def test_commandFail(self):
    self.mock_command.doCommand.side_effect = RSSException()
    for status in ValidStatus:
      self.failUnlessRaises(Exception, self.Res2SiteStatus_P.evaluate, ('XX', status), self.mock_command)

  def test_badArgs(self):
    self.failUnlessRaises(InvalidStatus, self.Res2SiteStatus_P.evaluate, ('XX', ''))
    self.failUnlessRaises(TypeError, self.Res2SiteStatus_P.evaluate, None )

     
#############################################################################

class PilotsEfficiency_PolicySuccess(PoliciesTestCase):
  
  def test_evaluate(self):
    for granularity in ValidRes:
      for status in ValidStatus:
        args = (granularity, 'XX', status)
        for i in [0,20,40,60,80]:
          clientRes = {'PilotsEff':'%d'%(i)}
          res = self.PE_P.evaluate(args, knownInfo=clientRes)
          if clientRes['PilotsEff'] > Configurations.GOOD_PILOTS_EFFICIENCY and status == 'Active':
            self.assertFalse(res['SAT'])
          elif clientRes['PilotsEff'] < Configurations.GOOD_PILOTS_EFFICIENCY and status == 'Active':
            self.assert_(res['SAT'])
          elif clientRes['PilotsEff'] < Configurations.GOOD_PILOTS_EFFICIENCY and status == 'Probing':
            self.assertFalse(res['SAT'])
          elif clientRes['PilotsEff'] > Configurations.GOOD_PILOTS_EFFICIENCY and status == 'Probing':
            self.assert_(res['SAT'])
          
          self.mock_commandPeriods.doCommand.return_value = []
          self.mock_commandStats.doCommand.return_value = {'MeanProcessedPilots':'%d'%(i), 'LastProcessedPilots':'%d'%(i)}
          self.mock_commandEff.doCommand.return_value = clientRes
          res = self.PE_P.evaluate(args, commandPeriods = self.mock_commandPeriods, commandStats = self.mock_commandStats, commandEff = self.mock_commandEff)
          if clientRes['PilotsEff'] > Configurations.GOOD_PILOTS_EFFICIENCY and status == 'Active':
            self.assertFalse(res['SAT'])
          elif clientRes['PilotsEff'] < Configurations.GOOD_PILOTS_EFFICIENCY and status == 'Active':
            self.assert_(res['SAT'])
          elif clientRes['PilotsEff'] < Configurations.GOOD_PILOTS_EFFICIENCY and status == 'Probing':
            self.assertFalse(res['SAT'])
          elif clientRes['PilotsEff'] > Configurations.GOOD_PILOTS_EFFICIENCY and status == 'Probing':
            self.assertFalse(res['SAT'])
          
          
  def test__getPilotsStats(self):
    for granularity in ValidRes:
      for status in ValidStatus:
        args = (granularity, 'XX')
        for i in [0,20,40,60,80]:
          clientRes = {'MeanProcessedPilots':'%d'%(i), 'LastProcessedPilots':'%d'%(i)}
          self.mock_command.doCommand.return_value = clientRes
          res = self.PE_P._getPilotsStats(args, [''], commandIn = self.mock_command)
          self.assertEqual(res['MeanProcessedPilots'], str(i))
          self.assertEqual(res['LastProcessedPilots'], str(i))
          
  def test__getPilotsEff(self):
    for granularity in ValidRes:
      for status in ValidStatus:
        args = (granularity, 'XX')
        for i in [0,20,40,60,80]:
          clientRes = {'PilotsEff':'%d'%(i)}
          self.mock_command.doCommand.return_value = clientRes
          res = self.PE_P._getPilotsEff(args, [''], commandIn = self.mock_command)
          self.assertEqual(res['PilotsEff'], str(i))
          
  def test__getPeriods(self):
    
    for granularity in ValidRes:
      for status in ValidStatus:
        args = (granularity, 'XX', status)
        for i in [0,20,40,60,80]:
          clientRes = {'Periods':[]}
          self.mock_command.doCommand.return_value = clientRes
          res = self.PE_P._getPeriods(args, meanProcessedPilots = i, commandIn = self.mock_command)
          self.assert_(res.has_key('Periods'))
    
       
#############################################################################

class PilotsEfficiency_Policy_Failure(PoliciesTestCase):
  
  def test_commandFail(self):
    self.mock_command.doCommand.side_effect = RSSException()
    for granularity in ValidRes:
      for status in ValidStatus:
        self.failUnlessRaises(Exception, self.PE_P.evaluate, (granularity, 'XX', status), self.mock_command)

  def test_badArgs(self):
    for status in ValidStatus:
      self.failUnlessRaises(InvalidRes, self.PE_P.evaluate, ('sites', '', status))
    self.failUnlessRaises(TypeError, self.PE_P.evaluate, None )
     
#############################################################################

class PilotsEfficiency_Simple_PolicySuccess(PoliciesTestCase):
  
  def test_evaluate(self):
    for granularity in ValidRes:
      for status in ValidStatus:
        args = (granularity, 'XX', status)
        for i in ['Good', 'Fair', 'Poor', 'Bad']:
          clientRes = {'Result':i}
          PES_P = PilotsEfficiency_Simple_Policy()
          PES_P.setArgs(args)
          PES_P.setKnownInfo(clientRes)
          res = PES_P.evaluate()
          self.assert_(res.has_key('SAT'))

          PES_P = PilotsEfficiency_Simple_Policy()
          self.mock_commandEff.doCommand.return_value = clientRes
          PES_P.setCommand(self.mock_commandEff)
          PES_P.setArgs(args)
          res = PES_P.evaluate()
          self.assert_(res.has_key('SAT'))
          PES_P.setCommand(self.mock_commandEff)
          res = PES_P.evaluate()
          self.assert_(res.has_key('SAT'))
        
        clientRes = {'Result':'Idle'}
        self.mock_commandEff.doCommand.return_value = clientRes
        PES_P = PilotsEfficiency_Simple_Policy()
        PES_P.setCommand(self.mock_commandEff)
        PES_P.setArgs(args)
        res = PES_P.evaluate()
        self.assertEqual(res['SAT'], None)
          
        clientRes = {'Result':'Unknown'}
        PES_P = PilotsEfficiency_Simple_Policy()
        self.mock_commandEff.doCommand.return_value = clientRes
        PES_P.setCommand(self.mock_commandEff)
        PES_P.setArgs(args)
        res = PES_P.evaluate()
        self.assertEqual(res['SAT'], 'Unknown')
          

#############################################################################

class PilotsEfficiency_Simple_Policy_Failure(PoliciesTestCase):
  
  def test_commandFail(self):
    self.mock_command.doCommand.side_effect = RSSException()
    PES_P = PilotsEfficiency_Simple_Policy()
    PES_P.setCommand(self.mock_command)
    for granularity in ValidRes:
      for status in ValidStatus:
        self.failUnlessRaises(Exception, PES_P.evaluate)

  def test_badArgs(self):
    PES_P = PilotsEfficiency_Simple_Policy()
    for status in ValidStatus:
      self.failUnlessRaises(InvalidRes, PES_P.setArgs, ('sites', '', status))
    self.failUnlessRaises(TypeError, PES_P.setArgs, None )
     

#############################################################################


class JobsEfficiency_PolicySuccess(PoliciesTestCase):
  
  def test_evaluate(self):
    for granularity in ValidRes:
      for status in ValidStatus:
        args = (granularity, 'XX', status)
        for i in [0,20,40,60,80]:
          clientRes = {'JobsEff':'%d'%(i)}
          res = self.JE_P.evaluate(args, knownInfo=clientRes)
          if clientRes['JobsEff'] > Configurations.GOOD_JOBS_EFFICIENCY and status == 'Active':
            self.assertFalse(res['SAT'])
          elif clientRes['JobsEff'] < Configurations.GOOD_JOBS_EFFICIENCY and status == 'Active':
            self.assert_(res['SAT'])
          elif clientRes['JobsEff'] < Configurations.GOOD_JOBS_EFFICIENCY and status == 'Probing':
            self.assertFalse(res['SAT'])
          elif clientRes['JobsEff'] > Configurations.GOOD_JOBS_EFFICIENCY and status == 'Probing':
            self.assert_(res['SAT'])
          
          self.mock_commandPeriods.doCommand.return_value = []
          self.mock_commandStats.doCommand.return_value = {'MeanProcessedJobs':'%d'%(i), 'LastProcessedJobs':'%d'%(i)}
          self.mock_commandEff.doCommand.return_value = clientRes
          self.mock_commandCharge.doCommand.return_value = {'LastHour': 50, 'anHourBefore': 30}      
          res = self.JE_P.evaluate(args, commandPeriods = self.mock_commandPeriods, commandStats = self.mock_commandStats, commandEff = self.mock_commandEff, commandCharge = self.mock_commandCharge)
          if clientRes['JobsEff'] > Configurations.GOOD_JOBS_EFFICIENCY and status == 'Active':
            self.assertFalse(res['SAT'])
          elif clientRes['JobsEff'] < Configurations.GOOD_JOBS_EFFICIENCY and status == 'Active':
            self.assert_(res['SAT'])
          elif clientRes['JobsEff'] < Configurations.GOOD_JOBS_EFFICIENCY and status == 'Probing':
            self.assertFalse(res['SAT'])
          elif clientRes['JobsEff'] > Configurations.GOOD_JOBS_EFFICIENCY and status == 'Probing':
            self.assertFalse(res['SAT'])
          self.mock_commandCharge.doCommand.return_value = {'LastHour': 100, 'anHourBefore': 30}      
          res = self.JE_P.evaluate(args, commandPeriods = self.mock_commandPeriods, commandStats = self.mock_commandStats, commandEff = self.mock_commandEff, commandCharge = self.mock_commandCharge)
          self.assertEqual(res['SAT'], None)
          
  def test__getJobsStats(self):
    for granularity in ValidRes:
      for status in ValidStatus:
        args = (granularity, 'XX')
        for i in [0,20,40,60,80]:
          clientRes = {'MeanProcessedJobs':'%d'%(i), 'LastProcessedJobs':'%d'%(i)}
          self.mock_command.doCommand.return_value = clientRes
          res = self.JE_P._getJobsStats(args, [''], commandIn = self.mock_command)
          self.assertEqual(res['MeanProcessedJobs'], str(i))
          self.assertEqual(res['LastProcessedJobs'], str(i))
          
  def test__getJobsEff(self):
    for granularity in ValidRes:
      for status in ValidStatus:
        args = (granularity, 'XX')
        for i in [0,20,40,60,80]:
          clientRes = {'JobsEff':'%d'%(i)}
          self.mock_command.doCommand.return_value = clientRes
          res = self.JE_P._getJobsEff(args, [''], commandIn = self.mock_command)
          self.assertEqual(res['JobsEff'], str(i))
          
  def test__getPeriods(self):
    
    for granularity in ValidRes:
      for status in ValidStatus:
        args = (granularity, 'XX', status)
        for i in [0,20,40,60,80]:
          clientRes = {'Periods':[]}
          self.mock_command.doCommand.return_value = clientRes
          res = self.JE_P._getPeriods(args, meanProcessedJobs = i, commandIn = self.mock_command)
          self.assert_(res.has_key('Periods'))
    
  def test__getSystemCharge(self):
    clientRes = {'LastHour': 50, 'anHourBefore': 30}
    self.mock_commandCharge.doCommand.return_value = clientRes
    res = self.JE_P._getSystemCharge((), commandIn = self.mock_commandCharge)
    self.assertEqual(res['LastHour'], 50)
    self.assertEqual(res['anHourBefore'], 30)

#############################################################################

class JobsEfficiency_Policy_Failure(PoliciesTestCase):
  
  def test_commandFail(self):
    self.mock_command.doCommand.side_effect = RSSException()
    for granularity in ValidRes:
      for status in ValidStatus:
        self.failUnlessRaises(Exception, self.JE_P.evaluate, (granularity, 'XX', status), self.mock_command)

  def test_badArgs(self):
    for status in ValidStatus:
      self.failUnlessRaises(InvalidRes, self.JE_P.evaluate, ('sites', '', status))
    self.failUnlessRaises(TypeError, self.JE_P.evaluate, None )
     
       
#############################################################################

class JobsEfficiency_Simple_PolicySuccess(PoliciesTestCase):
  
  def test_evaluate(self):
    mock_commandEff = Mock()
    for granularity in ValidRes:
      for status in ValidStatus:
        args = (granularity, 'XX', status)
        for i in ['Good', 'Fair', 'Poor', 'Bad']:
          clientRes = {'Result':i}
          JES_P = JobsEfficiency_Simple_Policy()
          JES_P.setKnownInfo(clientRes)
          JES_P.setArgs(args)
          res = JES_P.evaluate()
          self.assert_(res.has_key('SAT'))
          
          mock_commandEff.doCommand.return_value = clientRes
          JES_P = JobsEfficiency_Simple_Policy()
          JES_P.setArgs(args)
          JES_P.setCommand(mock_commandEff)
          res = JES_P.evaluate()
          self.assert_(res.has_key('SAT'))
        
        clientRes = {'Result':'Idle'}
        mock_commandEff.doCommand.return_value = clientRes
        JES_P = JobsEfficiency_Simple_Policy()
        JES_P.setArgs(args)
        JES_P.setCommand(mock_commandEff)
        res = JES_P.evaluate()
        self.assertEqual(res['SAT'], None)
          
        clientRes = {'Result':'Unknown'}
        mock_commandEff.doCommand.return_value = clientRes
        JES_P = JobsEfficiency_Simple_Policy()
        JES_P.setArgs(args)
        JES_P.setCommand(mock_commandEff)
        res = JES_P.evaluate()
        self.assertEqual(res['SAT'], 'Unknown')
          

#############################################################################

class JobsEfficiency_Simple_Policy_Failure(PoliciesTestCase):
  
  def test_commandFail(self):
    mock_command = Mock()
    mock_command.doCommand.side_effect = RSSException()
    JES_P = JobsEfficiency_Policy()
    JES_P.setCommand(mock_command)
    for granularity in ValidRes:
      for status in ValidStatus:
        self.failUnlessRaises(Exception, JES_P.evaluate)

  def test_badArgs(self):
    JES_P = JobsEfficiency_Policy()
    for status in ValidStatus:
      self.failUnlessRaises(InvalidRes, JES_P.setArgs, ('sites', '', status))
    self.failUnlessRaises(TypeError, JES_P.setArgs, None )
     

#############################################################################

class AlwaysFalse_PolicySuccess(PoliciesTestCase):
  
  def test_evaluate(self):
    for granularity in ValidRes:
      for status in ValidStatus:
        res = self.AF_P.evaluate((granularity, 'XX', status))
        self.assertEqual(res['SAT'], False)
        self.assertEqual(res['Status'], status)

#############################################################################

class SAMResults_PolicySuccess(PoliciesTestCase):
  
  def test_evaluate(self):
    for status in ValidStatus:
      for g in ('Site', 'Resource'):
        args = (g, 'XX', status)
        for resCl in ['ok', 'error', 'down', 'warn', 'maint']:
          self.SAMR_P.setArgs(args)
          self.SAMR_P.setKnownInfo({'Result':{'SS':resCl, 'js':'ok'}})
          res = self.SAMR_P.evaluate()
          self.assert_(res.has_key('SAT'))
          self.assert_(res.has_key('Reason'))
          self.mock_command.doCommand.return_value =  {'Result':{'SS':resCl}}
          SAMR_P = SAMResults_Policy()
          SAMR_P.setArgs(args)
          SAMR_P.setCommand(self.mock_command)
          res = SAMR_P.evaluate()
          self.assert_(res.has_key('SAT'))
          self.assert_(res.has_key('Reason'))
        SAMR_P = SAMResults_Policy()
        SAMR_P.setArgs(args)
        SAMR_P.setKnownInfo({'Result':{'SS':'na'}})
        res = self.SAMR_P.evaluate()
        self.assert_(res.has_key('SAT'))

#        self.mock_command.doCommand.return_value =  {'SAM-Status':{'SS':'na'}}
#        res = self.SAMR_P.evaluate(args, commandIn = self.mock_command)
#        self.assert_(res.has_key('SAT'))
#      
#        self.mock_command.doCommand.return_value =  {'SAM-Status':'Unknown'}
#        res = self.SAMR_P.evaluate(args, commandIn = self.mock_command)
#        self.assertEqual(res['SAT'], 'Unknown')
      
#############################################################################

class SAMResults_Policy_Failure(PoliciesTestCase):
  
  def test_commandFail(self):
    self.mock_command.doCommand.side_effect = RSSException()
    SAMR_P = SAMResults_Policy()
    SAMR_P.setArgs(('Site', 'XX', 'Active'))
    SAMR_P.setCommand(self.mock_command)
    self.failUnlessRaises(Exception, SAMR_P.evaluate)

  def test_badArgs(self):
    SAMR_P = SAMResults_Policy()
    self.failUnlessRaises(TypeError, SAMR_P.setArgs, None )
     

#############################################################################

class GGUSTickets_PolicySuccess(PoliciesTestCase):
  
  def test_evaluate(self):
    for g in ValidRes:
      for status in ValidStatus:
        args = (g, 'XX', status)
        for resCl in [0, 1, 3]:
          self.GGUS_P.setArgs(args)
          self.GGUS_P.setKnownInfo({'Result':resCl})
          res = self.GGUS_P.evaluate()
          self.assert_(res.has_key('SAT'))
          self.mock_command.doCommand.return_value =  {'Result':resCl}
          GGUS_P = GGUSTickets_Policy()
          GGUS_P.setArgs(args)
          GGUS_P.setCommand(self.mock_command)
          res = GGUS_P.evaluate()
          self.assert_(res.has_key('SAT'))

        self.mock_command.doCommand.return_value =  {'Result':'Unknown'}
        GGUS_P = GGUSTickets_Policy()
        GGUS_P.setArgs(args)
        GGUS_P.setCommand(self.mock_command)
        res = GGUS_P.evaluate()
        self.assertEqual(res['SAT'], 'Unknown')
      
#############################################################################

class GGUSTickets_Policy_Failure(PoliciesTestCase):
  
  def test_commandFail(self):
    GGUS_P = GGUSTickets_Policy()
    self.mock_command.doCommand.side_effect = RSSException()
    GGUS_P.setArgs(('Site', 'XX', 'Active'))
    GGUS_P.setCommand(self.mock_command)
    self.failUnlessRaises(Exception, GGUS_P.evaluate)

  def test_badArgs(self):
    GGUS_P = GGUSTickets_Policy()
    self.failUnlessRaises(TypeError, GGUS_P.setArgs, None )
     

#############################################################################

class OnservicePropagation_PolicySuccess(PoliciesTestCase):
  
  def test_evaluate(self):
    for status in ValidStatus:
      args = ('Service', 'XX', status)
      for resCl_1 in [{'Active':0, 'Probing':0, 'Banned':2, 'Total':2}, \
                      {'Active':2, 'Probing':2, 'Banned':0, 'Total':2}, \
                      {'Active':0, 'Probing':0, 'Banned':0, 'Total':2}, \
                      {'Active':1, 'Probing':1, 'Banned':0, 'Total':2}, \
                      {'Active':1, 'Probing':0, 'Banned':1, 'Total':2}, \
                      {'Active':0, 'Probing':1, 'Banned':1, 'Total':2} ] :
        for resCl_2 in ValidStatus:
          res = self.OSP_P.evaluate(args, knownInfo = {'ResourceStats':resCl_1, 'MonitoredStatus':resCl_2})
          self.assert_(res.has_key('SAT'))
          self.assert_(res.has_key('Status'))
          self.assert_(res.has_key('Reason'))
          
          self.mock_propCommand.doCommand.return_value = {'ResourceStats':resCl_1}
          self.mock_siteStatusCommand.doCommand.return_value = {'MonitoredStatus':resCl_2}
          commandList = [self.mock_propCommand, self.mock_siteStatusCommand]
          res = self.OSP_P.evaluate(args, commandIn = commandList)
          self.assert_(res.has_key('SAT'))
          self.assert_(res.has_key('Status'))
          self.assert_(res.has_key('Reason'))
          
        
class OnservicePropagation_Policy_Failure(PoliciesTestCase):
  
  def test_commandFail(self):
    self.mock_command.doCommand.side_effect = RSSException()
    for status in ValidStatus:
      self.failUnlessRaises(Exception, self.OSP_P.evaluate, ('Service', 'XX', status), self.mock_command)

  def test_badArgs(self):
    self.failUnlessRaises(TypeError, self.OSP_P.evaluate, None )
  
#############################################################################

class OnSENodePropagation_PolicySuccess(PoliciesTestCase):
  
  def test_evaluate(self):
    for status in ValidStatus:
      args = ('Resource', 'XX', status)
      for resCl in [{'Active':0, 'Probing':0, 'Banned':2, 'Total':2},
                    {'Active':2, 'Probing':2, 'Banned':0, 'Total':2}, 
                    {'Active':0, 'Probing':0, 'Banned':0, 'Total':2},
                    {'Active':1, 'Probing':1, 'Banned':0, 'Total':2}, 
                    {'Active':1, 'Probing':0, 'Banned':1, 'Total':2},
                    {'Active':0, 'Probing':1, 'Banned':1, 'Total':2} ] :
        res = self.OSENP_P.evaluate(args, knownInfo = {'StorageElementStats':{'StorageElementStats':resCl}})
        self.assert_(res.has_key('SAT'))
        self.assert_(res.has_key('Status'))
        self.assert_(res.has_key('Reason'))
        
        self.mock_propCommand.doCommand.return_value = {'StorageElementStats':  resCl}
        res = self.OSENP_P.evaluate(args, commandIn = self.mock_propCommand)
        self.assert_(res.has_key('SAT'))
        self.assert_(res.has_key('Status'))
        self.assert_(res.has_key('Reason'))
          
        
class OnSENodePropagation_Policy_Failure(PoliciesTestCase):
  
  def test_commandFail(self):
    self.mock_command.doCommand.sideEffect = RSSException
    for status in ValidStatus:
      self.failUnlessRaises(Exception, self.OSENP_P.evaluate, ('XX', status), self.mock_command)

  def test_badArgs(self):
    self.failUnlessRaises(TypeError, self.OSENP_P.evaluate, None )
  
#############################################################################

class Propagation_PolicySuccess(PoliciesTestCase):
  
  def test_evaluate(self):
    for status in ValidStatus:
      for g in ('Site', 'Service'):
        for g2 in ('Service', 'Resource', 'StorageElement'):
          args = (g, 'XX', status, g2)
          for resCl in [{'Active':0, 'Probing':0, 'Bad':0, 'Banned':4, 'Total':4},
                        {'Active':2, 'Probing':2, 'Bad':0, 'Banned':0, 'Total':4}, 
                        {'Active':0, 'Probing':0, 'Bad':4, 'Banned':0, 'Total':4},
                        {'Active':1, 'Probing':1, 'Bad':0, 'Banned':0, 'Total':2}, 
                        {'Active':1, 'Probing':0, 'Bad':2, 'Banned':1, 'Total':4},
                        {'Active':0, 'Probing':1, 'Bad':0, 'Banned':1, 'Total':2} ] :
            self.P_P.setArgs(args)
            self.P_P.setKnownInfo({'Result':resCl})
            res = self.P_P.evaluate()
            self.assert_(res.has_key('SAT'))
            self.assert_(res.has_key('Status'))
            self.assert_(res.has_key('Reason'))
            
            self.mock_propCommand.doCommand.return_value = {'Result': resCl}
            P_P = Propagation_Policy()
            P_P.setArgs(args)
            P_P.setCommand(self.mock_propCommand)
            res = P_P.evaluate()
            self.assert_(res.has_key('SAT'))
            self.assert_(res.has_key('Status'))
            self.assert_(res.has_key('Reason'))
          
          self.mock_propCommand.doCommand.return_value = {'Result': 'Unknown'}
          P_P = Propagation_Policy()
          P_P.setArgs(args)
          P_P.setCommand(self.mock_propCommand)
          res = P_P.evaluate()
          self.assertEqual(res['SAT'], 'Unknown')
        
          self.mock_propCommand.doCommand.return_value = {'Result': {'Active':0, 'Probing':0, 
                                                                    'Bad':0, 'Banned':0, 'Total':0}}
          P_P = Propagation_Policy()
          P_P.setArgs(args)
          P_P.setCommand(self.mock_propCommand)
          res = P_P.evaluate()
          self.assertEqual(res['SAT'], None)
        
class Propagation_Policy_Failure(PoliciesTestCase):
  
  def test_commandFail(self):
    self.mock_command.doCommand.side_effect = RSSException()
    P_P = Propagation_Policy()
    P_P.setArgs(('Site', 'XX', 'Active'))
    P_P.setCommand(self.mock_command)
    self.failUnlessRaises(Exception, self.P_P.evaluate)

  def test_badArgs(self):
    self.failUnlessRaises(TypeError, self.P_P.setArgs, None )
  
#############################################################################

class TransferQuality_PolicySuccess(PoliciesTestCase):
  
  def test_evaluate(self):
    for status in ValidStatus:
      for SE in ('CNAF-RAW', 'CNAF-FAILOVER'):
        args = ('StorageElement', SE, status)
        for resCl in [100.0, 91.0, 50.9, 0, None]:
          self.TQ_P.setArgs(args)
          self.TQ_P.setKnownInfo({'Result':resCl})
          res = self.TQ_P.evaluate()
          self.assert_(res.has_key('SAT'))
          if resCl is not None:
            self.assert_(res.has_key('Reason'))
          self.mock_command.doCommand.return_value =  {'Result':resCl}
          TQ_P = TransferQuality_Policy()
          TQ_P.setArgs(args)
          TQ_P.setCommand(self.mock_command)
          res = TQ_P.evaluate()
          self.assert_(res.has_key('SAT'))
          if resCl is not None:
            self.assert_(res.has_key('Reason'))
        
        args = ('StorageElement', 'XX', status, datetime.utcnow())
        for resCl in [100.0, 91.0, 50.9, 0, None]:
          TQ_P = TransferQuality_Policy()
          TQ_P.setArgs(args)
          TQ_P.setKnownInfo({'Result':resCl})
          res = TQ_P.evaluate()
          self.assert_(res.has_key('SAT'))
          if resCl is not None:
            self.assert_(res.has_key('Reason'))
          self.mock_command.doCommand.return_value =  {'Result':resCl}
          TQ_P = TransferQuality_Policy()
          TQ_P.setArgs(args)
          TQ_P.setCommand(self.mock_command)
          res = TQ_P.evaluate()
          self.assert_(res.has_key('SAT'))
          if resCl is not None:
            self.assert_(res.has_key('Reason'))
        res = self.TQ_P.evaluate()
        self.assert_(res.has_key('SAT'))
      
        args = ('StorageElement', 'XX', status, datetime.utcnow(), datetime.utcnow())
        for resCl in [1.0, 0.91, 0.50, 0]:
          TQ_P = TransferQuality_Policy()
          TQ_P.setArgs(args)
          TQ_P.setKnownInfo({'Result':resCl})
          res = self.TQ_P.evaluate()
          self.assert_(res.has_key('SAT'))
          if resCl is not None:
            self.assert_(res.has_key('Reason'))
          self.mock_command.doCommand.return_value =  {'Result':resCl}
          TQ_P = TransferQuality_Policy()
          TQ_P.setArgs(args)
          TQ_P.setCommand(self.mock_command)
          res = self.TQ_P.evaluate()
          self.assert_(res.has_key('SAT'))
          if resCl is not None:
            self.assert_(res.has_key('Reason'))
        res = self.TQ_P.evaluate()
        self.assert_(res.has_key('SAT'))

      self.mock_command.doCommand.return_value =  {'Result':'Unknown'}
      TQ_P = TransferQuality_Policy()
      TQ_P.setArgs(args)
      TQ_P.setCommand(self.mock_command)
      res = TQ_P.evaluate()
      self.assertEqual(res['SAT'], 'Unknown')


class TransferQuality_Policy_Failure(PoliciesTestCase):
  
  def test_commandFail(self):
    TQ_P = TransferQuality_Policy()
    TQ_P.setArgs(('Site', 'XX', 'Active'))
    self.mock_command.doCommand.side_effect = RSSException()
    TQ_P.setCommand(self.mock_command)    
    self.failUnlessRaises(Exception, self.TQ_P.evaluate)

  def test_badArgs(self):
    self.failUnlessRaises(TypeError, self.TQ_P.setArgs, None )
      
#############################################################################

class OnStorageElementPropagation_PolicySuccess(PoliciesTestCase):
  
  def test_evaluate(self):
    for status in ValidStatus:
      args = ('Resource', 'XX', status)
      for resCl in ValidRes :
        self.OSEP_P.setArgs(args)
        self.OSEP_P.setKnownInfo({'Result':resCl})
        res = self.OSEP_P.evaluate()
        self.assert_(res.has_key('SAT'))
        if resCl == 'Banned':
          self.assert_(res.has_key('Status'))
          self.assert_(res.has_key('Reason'))
        
        self.mock_command.doCommand.return_value = {'Result':  resCl}
        OSEP_P = OnStorageElementPropagation_Policy()
        OSEP_P.setArgs(args)
        OSEP_P.setCommand(self.mock_command)
        res = OSEP_P.evaluate()
        self.assert_(res.has_key('SAT'))
        if resCl == 'Banned':
          self.assert_(res.has_key('Status'))
          self.assert_(res.has_key('Reason'))
          
        
class OnStorageElementPropagation_Policy_Failure(PoliciesTestCase):
  
  def test_commandFail(self):
    self.mock_command.doCommand.side_effect = RSSException()
    OSEP_P = OnStorageElementPropagation_Policy()
    OSEP_P.setCommand(self.mock_command)
    self.failUnlessRaises(Exception, self.OSEP_P.evaluate)

  def test_badArgs(self):
    self.failUnlessRaises(TypeError, self.OSEP_P.setArgs, None )
  
#############################################################################

class SEOccupancy_PolicySuccess(PoliciesTestCase):
  
  def test_evaluate(self):
    for status in ValidStatus:
      args = ('StorageElement', 'XX', status)
      for resCl in [100, 10, 1, 0, None]:
        self.SEO_P.setArgs(args)
        self.SEO_P.setKnownInfo({'Result':resCl})
        res = self.SEO_P.evaluate()
        self.assert_(res.has_key('SAT'))
        if resCl is not None:
          self.assert_(res.has_key('Reason'))
        self.mock_command.doCommand.return_value =  {'Result':resCl}
        SEO_P = SEOccupancy_Policy()
        SEO_P.setArgs(args)
        SEO_P.setCommand(self.mock_command)
        res = SEO_P.evaluate()
        self.assert_(res.has_key('SAT'))
        if resCl is not None:
          self.assert_(res.has_key('Reason'))
      
#############################################################################

class SEOccupancy_Policy_Failure(PoliciesTestCase):
  
  def test_commandFail(self):
    self.mock_command.doCommand.side_effect = RSSException()
    for status in ValidStatus:
      self.failUnlessRaises(Exception, self.TQ_P.evaluate)

  def test_badArgs(self):
    self.failUnlessRaises(TypeError, self.SEO_P.setArgs, None )
     

#############################################################################

class SEQueuedTransfers_PolicySuccess(PoliciesTestCase):
  
  def test_evaluate(self):
    for status in ValidStatus:
      args = ('StorageElement', 'XX', status, ["Queued transfers"])
      for resCl in [{'Queued transfers':110.0}, {'Queued transfers':10.0}, 
                    {'Queued transfers':1.0}]:
        self.SEQT_P.setArgs(args)
        self.SEQT_P.setKnownInfo({'Result':resCl})
        res = self.SEQT_P.evaluate()
        self.assert_(res.has_key('SAT'))
#        if resCl is not None:
#          self.assert_(res.has_key('Reason'))
        self.mock_command.doCommand.return_value =  {'Result':resCl}
        SEQT_P = SEQueuedTransfers_Policy()
        SEQT_P.setArgs(args)
        SEQT_P.setCommand(self.mock_command)
        res = SEQT_P.evaluate()
        self.assert_(res.has_key('SAT'))
#        if resCl is not None:
#          self.assert_(res.has_key('Reason'))

    self.mock_command.doCommand.return_value =  {'Result':'Unknown'}
    SEQT_P = SEQueuedTransfers_Policy()
    SEQT_P.setArgs(args)
    SEQT_P.setCommand(self.mock_command)
    res = self.SEQT_P.evaluate()
    self.assert_(res['SAT'], 'Unknown')
      
    self.mock_command.doCommand.return_value =  {'Result':None}
    SEQT_P = SEQueuedTransfers_Policy()
    SEQT_P.setArgs(args)
    SEQT_P.setCommand(self.mock_command)
    res = self.SEQT_P.evaluate()
    self.assert_(res['SAT'], None)
      
#############################################################################

class SEQueuedTransfers_Policy_Failure(PoliciesTestCase):
  
  def test_commandFail(self):
    self.mock_command.doCommand.side_effect = RSSException()
    for status in ValidStatus:
      self.failUnlessRaises(Exception, self.TQ_P.evaluate)

  def test_badArgs(self):
    self.failUnlessRaises(TypeError, self.SEQT_P.setArgs, None )
     

#############################################################################



if __name__ == '__main__':
  suite = unittest.defaultTestLoader.loadTestsFromTestCase(PoliciesTestCase)
  suite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(PolicyInvokerSuccess))
  suite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(PolicyInvokerFailure))
  suite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(DT_PolicySuccess))
  suite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(DT_Policy_Failure))
#  suite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(Res2SiteStatus_PolicySuccess))
#  suite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(Res2SiteStatus_Policy_Failure))
  suite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(AlwaysFalse_PolicySuccess))
#  suite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(PilotsEfficiency_PolicySuccess))
#  suite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(PilotsEfficiency_Policy_Failure))
  suite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(PilotsEfficiency_Simple_PolicySuccess))
  suite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(PilotsEfficiency_Simple_Policy_Failure))
#  suite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(JobsEfficiency_PolicySuccess))
#  suite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(JobsEfficiency_Policy_Failure))
  suite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(JobsEfficiency_Simple_PolicySuccess))
  suite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(JobsEfficiency_Simple_Policy_Failure))
  suite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(SAMResults_PolicySuccess))
  suite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(SAMResults_Policy_Failure))
  suite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(GGUSTickets_PolicySuccess))
  suite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(GGUSTickets_Policy_Failure))
#  suite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(OnservicePropagation_PolicySuccess))
#  suite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(OnservicePropagation_Policy_Failure))
#  suite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(OnSENodePropagation_PolicySuccess))
#  suite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(OnSENodePropagation_Policy_Failure))
  suite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(Propagation_PolicySuccess))
  suite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(Propagation_Policy_Failure))
  suite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(TransferQuality_PolicySuccess))
  suite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(TransferQuality_Policy_Failure))
  suite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(OnStorageElementPropagation_PolicySuccess))
  suite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(OnStorageElementPropagation_Policy_Failure))
  suite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(SEOccupancy_PolicySuccess))
  suite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(SEOccupancy_Policy_Failure))
  suite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(SEQueuedTransfers_PolicySuccess))
  suite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(SEQueuedTransfers_Policy_Failure))
  testResult = unittest.TextTestRunner(verbosity=2).run(suite)