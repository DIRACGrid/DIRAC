""" UnitTest class for policy classes
"""

import unittest
from DIRAC.ResourceStatusSystem.Utilities.mock import Mock
from DIRAC.ResourceStatusSystem.Policy.PolicyInvoker import PolicyInvoker
from DIRAC.ResourceStatusSystem.Policy.DT_Policy import DT_Policy
from DIRAC.ResourceStatusSystem.Policy.AlwaysFalse_Policy import AlwaysFalse_Policy 
from DIRAC.ResourceStatusSystem.Policy.Res2SiteStatus_Policy import Res2SiteStatus_Policy 
from DIRAC.ResourceStatusSystem.Policy.PilotsEfficiency_Policy import PilotsEfficiency_Policy 
from DIRAC.ResourceStatusSystem.Policy.PilotsEfficiency_Simple_Policy import PilotsEfficiency_Simple_Policy
from DIRAC.ResourceStatusSystem.Policy.JobsEfficiency_Policy import JobsEfficiency_Policy
from DIRAC.ResourceStatusSystem.Policy.JobsEfficiency_Simple_Policy import JobsEfficiency_Simple_Policy
from DIRAC.ResourceStatusSystem.Policy.SAMResults_Policy import SAMResults_Policy 
from DIRAC.ResourceStatusSystem.Policy.GGUSTickets_Policy import GGUSTickets_Policy 
from DIRAC.ResourceStatusSystem.Policy.OnServicePropagation_Policy import OnServicePropagation_Policy 
from DIRAC.ResourceStatusSystem.Policy.OnSENodePropagation_Policy import OnSENodePropagation_Policy 
from DIRAC.ResourceStatusSystem.Policy.TransferQuality_Policy import TransferQuality_Policy 
from DIRAC.ResourceStatusSystem.Utilities.Exceptions import *
from DIRAC.ResourceStatusSystem.Utilities.Utils import *
from DIRAC.ResourceStatusSystem.Policy import Configurations

#############################################################################

class PoliciesTestCase(unittest.TestCase):
  """ Base class
  """
  def setUp(self):

    self.mock_policy = Mock()
    self.mock_DB = Mock()
    self.pi = PolicyInvoker()
    self.DT_P = DT_Policy()
    self.AF_P = AlwaysFalse_Policy()
    self.Res2SiteStatus_P = Res2SiteStatus_Policy()
    self.PE_P = PilotsEfficiency_Policy()
    self.PES_P = PilotsEfficiency_Simple_Policy()
    self.JE_P = JobsEfficiency_Policy()
    self.JES_P = JobsEfficiency_Simple_Policy()
    self.SAMR_P = SAMResults_Policy()
    self.GGUS_P = GGUSTickets_Policy()
    self.OSP_P = OnServicePropagation_Policy()
    self.OSENP_P = OnSENodePropagation_Policy()
    self.TQ_P = TransferQuality_Policy()
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
      res = self.pi.evaluatePolicy((granularity, 'XX'))
      self.assertEqual(res['Result'], 'Satisfied')
    self.mock_policy.evaluate.return_value = {'Result':'Un-Satisfied'}
    self.pi.setPolicy(self.mock_policy)
    for granularity in ValidRes:
      res = self.pi.evaluatePolicy((granularity, 'XX'))
      self.assertEqual(res['Result'], 'Un-Satisfied')
    
#############################################################################

class PolicyInvokerFailure(PoliciesTestCase):
  
  def test_policyFail(self):
    self.mock_policy.evaluate.sideEffect = RSSException
    for granularity in ValidRes:
      self.failUnlessRaises(Exception, self.pi.evaluatePolicy, (granularity, 'XX'))
    
  def test_badArgs(self):
    self.failUnlessRaises(TypeError, self.pi.evaluatePolicy, [''])
     
        
#############################################################################

class DT_PolicySuccess(PoliciesTestCase):
  
  def test_evaluate(self):
    for granularity in ValidRes:
      for status in ValidStatus:
        args = (granularity, 'XX', status)
        for clientRes in ({'DT':'OUTAGE', 'Enddate':''}, {'DT':'AT_RISK', 'Enddate':''}, None):
          self.mock_command.doCommand.return_value = clientRes
          res = self.DT_P.evaluate(args, commandIn = self.mock_command, knownInfo=clientRes)
          if clientRes in ({'DT':'OUTAGE', 'Enddate':''}, {'DT':'AT_RISK', 'Enddate':''}) and status == 'Active':
            self.assert_(res['SAT'])
          elif clientRes in ({'DT':'OUTAGE', 'Enddate':''}, None) and status == 'Probing':
            self.assert_(res['SAT'])
          elif clientRes in ({'DT':'AT_RISK', 'Enddate':''}, None) and status == 'Banned':
            self.assert_(res['SAT'])
          else:
            self.assertFalse(res['SAT'])
          
          res = self.DT_P.evaluate(args, commandIn = self.mock_command)
          if clientRes in ({'DT':'OUTAGE', 'Enddate':''}, {'DT':'AT_RISK', 'Enddate':''}) and status == 'Active':
            self.assert_(res['SAT'])
          elif clientRes in ({'DT':'OUTAGE', 'Enddate':''}, None) and status == 'Probing':
            self.assert_(res['SAT'])
          elif clientRes in ({'DT':'AT_RISK', 'Enddate':''}, None) and status == 'Banned':
            self.assert_(res['SAT'])
          else:
            self.assertFalse(res['SAT'])
          
       
#############################################################################

class DT_Policy_Failure(PoliciesTestCase):
  
  def test_commandFail(self):
    self.mock_command.doCommand.sideEffect = RSSException
    for granularity in ValidRes:
      for status in ValidStatus:
        self.failUnlessRaises(Exception, self.DT_P.evaluate, (granularity, 'XX', status), self.mock_command)

  def test_badArgs(self):
    for status in ValidStatus:
      self.failUnlessRaises(InvalidRes, self.DT_P.evaluate, ('sites', '', status))
    self.failUnlessRaises(TypeError, self.DT_P.evaluate, None )
     

#############################################################################

class Res2SiteStatus_PolicySuccess(PoliciesTestCase):
  
  def test_evaluate(self):
    for status in ValidStatus:
      args = ('XX', status)
      for clientRes in ():
        self.mock_command.doCommand.return_value = clientRes
        res = self.Res2SiteStatus_P.evaluate(args, commandIn = self.mock_command, knownInfo=clientRes)
        if clientRes in ({'DT':'OUTAGE', 'Enddate':''}, {'DT':'AT_RISK', 'Enddate':''}) and status == 'Active':
          self.assert_(res['SAT'])
        elif clientRes in ({'DT':'OUTAGE', 'Enddate':''}, None) and status == 'Probing':
          self.assert_(res['SAT'])
        elif clientRes in ({'DT':'AT_RISK', 'Enddate':''}, None) and status == 'Banned':
          self.assert_(res['SAT'])
        else:
          self.assertFalse(res['SAT'])
        
        res = self.Res2SiteStatus_P.evaluate(args, commandIn = self.mock_command)
        if clientRes in ({'DT':'OUTAGE', 'Enddate':''}, {'DT':'AT_RISK', 'Enddate':''}) and status == 'Active':
          self.assert_(res['SAT'])
        elif clientRes in ({'DT':'OUTAGE', 'Enddate':''}, None) and status == 'Probing':
          self.assert_(res['SAT'])
        elif clientRes in ({'DT':'AT_RISK', 'Enddate':''}, None) and status == 'Banned':
          self.assert_(res['SAT'])
        else:
          self.assertFalse(res['SAT'])
        

#############################################################################

class Res2SiteStatus_Policy_Failure(PoliciesTestCase):
  
#  def test_commandFail(self):
#    self.mock_command.doCommand.sideEffect = RSSException
#    for status in ValidStatus:
#      self.failUnlessRaises(Exception, self.Res2SiteStatus_P.evaluate, ('XX', status), self.mock_command)

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
    self.mock_command.doCommand.sideEffect = RSSException
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
          clientRes = {'PilotsEff':i}
          res = self.PES_P.evaluate(args, knownInfo=clientRes)
          self.assert_(res.has_key('SAT'))
          
          self.mock_commandEff.doCommand.return_value = clientRes
          res = self.PES_P.evaluate(args, commandIn = self.mock_commandEff)
          self.assert_(res.has_key('SAT'))
        
        clientRes = {'PilotsEff':'Idle'}
        self.mock_commandEff.doCommand.return_value = clientRes
        res = self.PES_P.evaluate(args, commandIn = self.mock_commandEff)
        self.assertEqual(res['SAT'], None)
          

#############################################################################

class PilotsEfficiency_Simple_Policy_Failure(PoliciesTestCase):
  
#  def test_commandFail(self):
#    self.mock_command.doCommand.sideEffect = RSSException
#    for granularity in ValidRes:
#      for status in ValidStatus:
#        self.failUnlessRaises(Exception, self.PES_P.evaluate, (granularity, 'XX', status), self.mock_command)

  def test_badArgs(self):
    for status in ValidStatus:
      self.failUnlessRaises(InvalidRes, self.PES_P.evaluate, ('sites', '', status))
    self.failUnlessRaises(TypeError, self.PES_P.evaluate, None )
     

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
    self.mock_command.doCommand.sideEffect = RSSException
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
    for granularity in ValidRes:
      for status in ValidStatus:
        args = (granularity, 'XX', status)
        for i in ['Good', 'Fair', 'Poor', 'Bad']:
          clientRes = {'JobsEff':i}
          res = self.JES_P.evaluate(args, knownInfo=clientRes)
          self.assert_(res.has_key('SAT'))
          
          self.mock_commandEff.doCommand.return_value = clientRes
          res = self.JES_P.evaluate(args, commandIn = self.mock_commandEff)
          self.assert_(res.has_key('SAT'))
        
        clientRes = {'JobsEff':'Idle'}
        self.mock_commandEff.doCommand.return_value = clientRes
        res = self.JES_P.evaluate(args, commandIn = self.mock_commandEff)
        self.assertEqual(res['SAT'], None)
          

#############################################################################

class JobsEfficiency_Simple_Policy_Failure(PoliciesTestCase):
  
#  def test_commandFail(self):
#    self.mock_command.doCommand.sideEffect = RSSException
#    for granularity in ValidRes:
#      for status in ValidStatus:
#        self.failUnlessRaises(Exception, self.JES_P.evaluate, (granularity, 'XX', status), self.mock_command)

  def test_badArgs(self):
    for status in ValidStatus:
      self.failUnlessRaises(InvalidRes, self.JES_P.evaluate, ('sites', '', status))
    self.failUnlessRaises(TypeError, self.JES_P.evaluate, None )
     

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
      args = ('XX', 'XX', status)
      for resCl in ['ok', 'down', 'degraded', 'partial', 'maint']:
        res = self.SAMR_P.evaluate(args, commandIn = self.mock_command, knownInfo={'Status':resCl})
        self.assert_(res.has_key('SAT'))
        self.assert_(res.has_key('Reason'))
        self.mock_command.doCommand.return_value =  {'Status':resCl}
        res = self.SAMR_P.evaluate(args, commandIn = self.mock_command)
        self.assert_(res.has_key('SAT'))
        self.assert_(res.has_key('Reason'))
      res = self.SAMR_P.evaluate(args, commandIn = self.mock_command, knownInfo={'Status':'na'})
      self.assert_(res.has_key('SAT'))
      self.mock_command.doCommand.return_value =  {'Status':'na'}
      res = self.SAMR_P.evaluate(args, commandIn = self.mock_command)
      self.assert_(res.has_key('SAT'))
      
#############################################################################

class SAMResults_Policy_Failure(PoliciesTestCase):
  
  def test_commandFail(self):
    self.mock_command.doCommand.sideEffect = RSSException
    for status in ValidStatus:
      self.failUnlessRaises(Exception, self.SAMR_P.evaluate, ('XX', 'XX', status), self.mock_command)

  def test_badArgs(self):
    self.failUnlessRaises(TypeError, self.SAMR_P.evaluate, None )
     

#############################################################################

class GGUSTickets_PolicySuccess(PoliciesTestCase):
  
  def test_evaluate(self):
    for status in ValidStatus:
      args = ('XX', status)
      for resCl in [1, 3]:
        res = self.GGUS_P.evaluate(args, commandIn = self.mock_command, knownInfo={'GGUSTickets':resCl})
        self.assert_(res.has_key('SAT'))
        if status != 'Banned':
          self.assert_(res.has_key('Reason'))
        self.mock_command.doCommand.return_value =  {'GGUSTickets':resCl}
        res = self.GGUS_P.evaluate(args, commandIn = self.mock_command)
        self.assert_(res.has_key('SAT'))
        if status != 'Banned':
          self.assert_(res.has_key('Reason'))
      
#############################################################################

class GGUSTickets_Policy_Failure(PoliciesTestCase):
  
  def test_commandFail(self):
    self.mock_command.doCommand.sideEffect = RSSException
    for status in ValidStatus:
      self.failUnlessRaises(Exception, self.GGUS_P.evaluate, ('XX', status), self.mock_command)

  def test_badArgs(self):
    self.failUnlessRaises(TypeError, self.GGUS_P.evaluate, None )
     

#############################################################################

class OnservicePropagation_PolicySuccess(PoliciesTestCase):
  
  def test_evaluate(self):
    for status in ValidStatus:
      args = ('XX', status)
      for resCl_1 in [{'Active':0, 'Probing':0, 'Banned':2, 'Total':2}, \
                      {'Active':2, 'Probing':2, 'Banned':0, 'Total':2}, \
                      {'Active':0, 'Probing':0, 'Banned':0, 'Total':2}, \
                      {'Active':1, 'Probing':1, 'Banned':0, 'Total':2}, \
                      {'Active':1, 'Probing':0, 'Banned':1, 'Total':2}, \
                      {'Active':0, 'Probing':1, 'Banned':1, 'Total':2} ] :
        for resCl_2 in ValidStatus:
          res = self.OSP_P.evaluate(args, knownInfo = {'ResourceStats':resCl_1, 'SiteStatus':resCl_2})
          self.assert_(res.has_key('SAT'))
          self.assert_(res.has_key('Status'))
          self.assert_(res.has_key('Reason'))
          
          self.mock_propCommand.doCommand.return_value = resCl_1
          self.mock_siteStatusCommand.doCommand.return_value = resCl_2
          res = self.OSP_P.evaluate(args, self.mock_propCommand, self.mock_siteStatusCommand)
          self.assert_(res.has_key('SAT'))
          self.assert_(res.has_key('Status'))
          self.assert_(res.has_key('Reason'))
          
        
class OnservicePropagation_Policy_Failure(PoliciesTestCase):
  
  def test_commandFail(self):
    self.mock_command.doCommand.sideEffect = RSSException
    for status in ValidStatus:
      self.failUnlessRaises(Exception, self.OSP_P.evaluate, ('XX', status), self.mock_command)

  def test_badArgs(self):
    self.failUnlessRaises(TypeError, self.OSP_P.evaluate, None )
  
#############################################################################

class OnSENodePropagation_PolicySuccess(PoliciesTestCase):
  
  def test_evaluate(self):
    for status in ValidStatus:
      args = ('XX', status)
      for resCl in [{'Active':0, 'Probing':0, 'Banned':2, 'Total':2},
                    {'Active':2, 'Probing':2, 'Banned':0, 'Total':2}, 
                    {'Active':0, 'Probing':0, 'Banned':0, 'Total':2},
                    {'Active':1, 'Probing':1, 'Banned':0, 'Total':2}, 
                    {'Active':1, 'Probing':0, 'Banned':1, 'Total':2},
                    {'Active':0, 'Probing':1, 'Banned':1, 'Total':2} ] :
        res = self.OSENP_P.evaluate(args, knownInfo = {'SEStats':resCl})
        self.assert_(res.has_key('SAT'))
        self.assert_(res.has_key('Status'))
        self.assert_(res.has_key('Reason'))
        
        self.mock_propCommand.doCommand.return_value = resCl
        res = self.OSENP_P.evaluate(args, self.mock_propCommand)
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

class TransferQuality_PolicySuccess(PoliciesTestCase):
  
  def test_evaluate(self):
    for status in ValidStatus:
      args = ('XX', status)
      for resCl in [1, 0.91, 0.50, 0]:
        res = self.TQ_P.evaluate(args, commandIn = self.mock_command, knownInfo={'TransferQuality':resCl})
        self.assert_(res.has_key('SAT'))
        self.assert_(res.has_key('Reason'))
        self.mock_command.doCommand.return_value =  {'TransferQuality':resCl}
        res = self.TQ_P.evaluate(args, commandIn = self.mock_command)
        self.assert_(res.has_key('SAT'))
        self.assert_(res.has_key('Reason'))
      res = self.TQ_P.evaluate(args, commandIn = self.mock_command)
      self.assert_(res.has_key('SAT'))
      
#############################################################################

class TransferQuality_Policy_Failure(PoliciesTestCase):
  
#  def test_commandFail(self):
#    self.mock_command.doCommand.sideEffect = RSSException
#    for status in ValidStatus:
#      self.failUnlessRaises(Exception, self.TQ_P.evaluate, ('XX', status), self.mock_command)

  def test_badArgs(self):
    self.failUnlessRaises(TypeError, self.TQ_P.evaluate, None )
     

#############################################################################




if __name__ == '__main__':
  suite = unittest.defaultTestLoader.loadTestsFromTestCase(PoliciesTestCase)
  suite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(PolicyInvokerSuccess))
  suite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(PolicyInvokerFailure))
  suite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(DT_PolicySuccess))
  suite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(DT_Policy_Failure))
#  suite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(Res2SiteStatus_PolicySuccess))
  suite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(Res2SiteStatus_Policy_Failure))
  suite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(AlwaysFalse_PolicySuccess))
  suite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(PilotsEfficiency_PolicySuccess))
  suite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(PilotsEfficiency_Policy_Failure))
  suite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(PilotsEfficiency_Simple_PolicySuccess))
  suite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(PilotsEfficiency_Simple_Policy_Failure))
  suite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(JobsEfficiency_PolicySuccess))
  suite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(JobsEfficiency_Policy_Failure))
  suite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(JobsEfficiency_Simple_PolicySuccess))
  suite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(JobsEfficiency_Simple_Policy_Failure))
  suite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(SAMResults_PolicySuccess))
  suite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(SAMResults_Policy_Failure))
  suite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(GGUSTickets_PolicySuccess))
  suite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(GGUSTickets_Policy_Failure))
  suite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(OnservicePropagation_PolicySuccess))
  suite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(OnservicePropagation_Policy_Failure))
  suite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(OnSENodePropagation_PolicySuccess))
  suite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(OnSENodePropagation_Policy_Failure))
  suite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(TransferQuality_PolicySuccess))
  suite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(TransferQuality_Policy_Failure))
  testResult = unittest.TextTestRunner(verbosity=2).run(suite)