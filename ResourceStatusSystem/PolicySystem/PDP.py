""" 
The PDP (Policy Decision Point) module is used to:

1. Decides which policies have to be applied.

2. Invokes an evaluation of the policies, and returns the result (to a PEP)
"""
#############################################################################

import time
#import threading
from DIRAC.ResourceStatusSystem.Utilities.Utils import *
from DIRAC.ResourceStatusSystem.Utilities.Exceptions import *
from DIRAC.Core.Utilities.ThreadPool import ThreadPool,ThreadedJob
from DIRAC.ResourceStatusSystem.Policy import Configurations
from DIRAC.ResourceStatusSystem.Policy.PolicyInvoker import PolicyInvoker

#############################################################################

class PDP:
  """ 
  PDP (Policy Decision Point) initialization

  :params:
    :attr:`granularity`: string - a ValidRes
  
    :attr:`name`: string - name (e.g. of a site)
    
    :attr:`status`: string - status
    
    :attr:`formerStatus`: string - former status
    
    :attr:`reason`: string - optional reason for last status change

    :attr:`siteType`: string - optional site type

    :attr:`serviceType`: string - optional service type

    :attr:`resourceType`: string - optional resource type
  """

#############################################################################
  
  def __init__(self, granularity = None, name = None, status = None, formerStatus = None, 
               reason = None, siteType = None, serviceType = None, resourceType = None):
    
    self.__granularity = granularity
    if self.__granularity is not None:
      if self.__granularity not in ValidRes:
        raise InvalidRes, where(self, self.__init__)
    
    self.__name = name
    
    self.__status = status
    if self.__status is not None:
      if self.__status not in ValidStatus:
        raise InvalidStatus, where(self, self.__init__)
    
    self.__formerStatus = formerStatus
    if self.__formerStatus is not None:
      if self.__formerStatus not in ValidStatus:
        raise InvalidStatus, where(self, self.__init__)
    
    self.__reason = reason
    
    self.__siteType = siteType
    if self.__siteType is not None:
      if self.__siteType not in ValidSiteType:
        raise InvalidSiteType, where(self, self.__init__)
    
    self.__serviceType = serviceType
    if self.__serviceType is not None:
      if self.__serviceType not in ValidServiceType:
        raise InvalidServiceType, where(self, self.__init__)
    
    self.__resourceType = resourceType
    if self.__resourceType is not None:
      if self.__resourceType not in ValidResourceType:
        raise InvalidResourceType, where(self, self.__init__)


#    self.lockObj = threading.RLock()
    

      
#############################################################################
  
  def takeDecision(self, policyIn=None, argsIn=None, knownInfo=None):
    """ PDP MAIN FUNCTION
        
        decides policies that have to be applied, based on
        
        __granularity, 
        
        __name, 
        
        __status, 
        
        __formerStatus
        
        __reason
        
        If more than one policy is evaluated, results are combined.
        
        Logic for combination: a conservative approach is followed 
        (i.e. if a site should be banned for at least one policy, that's what is returned)
        
        returns:
        
          { 'PolicyType': a policyType (in a string),
            'Action': True|False,
            'Status': 'Active'|'Probing'|'Banned',
            'Reason': a reason
            'EndDate: datetime (in a string)}
    """
  
    self.args = argsIn
    self.policy = policyIn
    self.knownInfo = knownInfo

            
    EVAL = self._getPolicyToApply(granularity = self.__granularity, 
                                  status = self.__status, 
                                  formerStatus = self.__formerStatus, 
                                  siteType = self.__siteType, 
                                  serviceType = self.__serviceType,
                                  resourceType = self.__resourceType)

    policyCombinedResultsList = []
      
    for policyGroup in EVAL:
          
      self.__policyType = policyGroup['PolicyType']
  
      if self.policy is not None:
        singlePolicyResults = self.policy.evaluate(self.policy)
      else:
        if policyGroup['Policies'] is None:
          return {'SinglePolicyResults' : [], 
                  'PolicyCombinedResult' : [{'PolicyType': self.__policyType, 
                                             'Action': False, 
                                             'Reason':'No policy results'}]}
                  
        singlePolicyResults = self._invocation(self.__granularity, self.__name,
                                               self.__status, self.policy, 
                                               self.args, policyGroup['Policies'])

      policyCombinedResults = self._evaluate(singlePolicyResults)
    
      if policyCombinedResults == None:
          return {'SinglePolicyResults' : singlePolicyResults, 
                  'PolicyCombinedResult' : [{'PolicyType': self.__policyType, 
                                             'Action': False, 
                                             'Reason':'No policy results'}]}
      
      #policy results communication
      if policyCombinedResults['SAT']:
        newstatus = policyCombinedResults['Status']
        reason = policyCombinedResults['Reason']
        decision = {'PolicyType': self.__policyType, 'Action': True, 'Status':'%s'%newstatus, 
                    'Reason':'%s'%reason}
        if policyCombinedResults.has_key('EndDate'):
          decision['EndDate'] = policyCombinedResults['EndDate']
        policyCombinedResultsList.append(decision)
      elif not policyCombinedResults['SAT']:
        reason = policyCombinedResults['Reason']
        decision = {'PolicyType': self.__policyType, 'Action': False, 'Reason':'%s'%reason}
        if policyCombinedResults.has_key('EndDate'):
          decision['EndDate'] = policyCombinedResults['EndDate']
        policyCombinedResultsList.append(decision)

    res = {'SinglePolicyResults': singlePolicyResults, 
           'PolicyCombinedResult' : policyCombinedResultsList}

    return res

#############################################################################

  def _getPolicyToApply(self, granularity, status = None, formerStatus = None, 
                       siteType = None, serviceType = None, resourceType = None ):
    
    pol_to_eval = []
    pol_types = []
    
    for p in Configurations.Policies.keys():
      if granularity in Configurations.Policies[p]['Granularity']:
        pol_to_eval.append(p)
        
        if status is not None:
          if status not in Configurations.Policies[p]['Status']:
            pol_to_eval.remove(p)
        
        if formerStatus is not None:
          if formerStatus not in Configurations.Policies[p]['FormerStatus']:
            try:
              pol_to_eval.remove(p)
            except Exception:
              continue
            
        if siteType is not None:
          if siteType not in Configurations.Policies[p]['SiteType']:
            try:
              pol_to_eval.remove(p)
            except Exception:
              continue
            
        if serviceType is not None:
          if serviceType not in Configurations.Policies[p]['ServiceType']:
            try:
              pol_to_eval.remove(p)
            except Exception:
              continue
            
        if resourceType is not None:
          if resourceType not in Configurations.Policies[p]['ResourceType']:
            try:
              pol_to_eval.remove(p)
            except Exception:
              continue
            
     
    for pt in Configurations.Policy_Types.keys():
      if granularity in Configurations.Policy_Types[pt]['Granularity']:
        pol_types.append(pt)  
    
        if status is not None:
          if status not in Configurations.Policy_Types[pt]['Status']:
            pol_to_eval.remove(pt)
        
        if formerStatus is not None:
          if formerStatus not in Configurations.Policy_Types[pt]['FormerStatus']:
            try:
              pol_to_eval.remove(pt)
            except Exception:
              continue
            
        if siteType is not None:
          if siteType not in Configurations.Policy_Types[pt]['SiteType']:
            try:
              pol_to_eval.remove(pt)
            except Exception:
              continue
            
        if serviceType is not None:
          if serviceType not in Configurations.Policy_Types[pt]['ServiceType']:
            try:
              pol_to_eval.remove(pt)
            except Exception:
              continue
            
        if resourceType is not None:
          if resourceType not in Configurations.Policy_Types[pt]['ResourceType']:
            try:
              pol_to_eval.remove(pt)
            except Exception:
              continue
              
    EVAL = [{'PolicyType':pol_types, 'Policies':pol_to_eval}]    
    
    return EVAL

#############################################################################

  def _invocation(self, granularity, name, status, policy, args, policies):
    
    policyResults = []
    
    for p in policies:
      res = self._policyInvocation(granularity = granularity, name = name,
                                   status = status, policy = policy, args = args,
                                   pol = p)
      
      if res['SAT'] != None:
        policyResults.append(res)
    
    return policyResults
      
#############################################################################


  def _policyInvocation(self, granularity = None, name = None, status = None, 
                        policy = None, args = None, pol = None):
    
    if pol == 'DT_Policy_OnGoing_Only':
      p = policy
      a = args
      if policy is None:
        from DIRAC.ResourceStatusSystem.Policy.DT_Policy import DT_Policy 
        p = DT_Policy()
      if args is None:
        a = (granularity, name, status)
      res = self.__innerEval(p, a)
      
    if pol == 'DT_Policy_Scheduled':
      p = policy
      a = args
      if policy is None:
        from DIRAC.ResourceStatusSystem.Policy.DT_Policy import DT_Policy 
        p = DT_Policy()
      if args is None:
        a = (granularity, name, status, Configurations.DTinHours)
      res = self.__innerEval(p, a)
      
    if pol == 'AlwaysFalse_Policy':
      p = policy
      a = args
      if policy is None:
        from DIRAC.ResourceStatusSystem.Policy.AlwaysFalse_Policy import AlwaysFalse_Policy 
        p = AlwaysFalse_Policy()
      if args is None:
        a = (granularity, name, status)
      res = self.__innerEval(p, a)
  
    if pol == 'SAM_Policy':
      p = policy
      a = args
      if policy is None:
        from DIRAC.ResourceStatusSystem.Policy.SAMResults_Policy import SAMResults_Policy 
        p = SAMResults_Policy()
      if args is None:
        a = (granularity, name, status)
      res = self.__innerEval(p, a)
  
    if pol == 'SAM_CE_Policy':
      p = policy
      a = args
      if policy is None:
        from DIRAC.ResourceStatusSystem.Policy.SAMResults_Policy import SAMResults_Policy 
        p = SAMResults_Policy()
      if args is None:
        a = (granularity, name, status, None, 
             ['LHCb CE-lhcb-availability', 'LHCb CE-lhcb-install', 'LHCb CE-lhcb-job-Boole', 
              'LHCb CE-lhcb-job-Brunel', 'LHCb CE-lhcb-job-DaVinci', 'LHCb CE-lhcb-job-Gauss', 'LHCb CE-lhcb-os', 
              'LHCb CE-lhcb-queues', 'bi', 'csh', 'js', 'gfal', 'swdir', 'voms'])
      res = self.__innerEval(p, a)
  
    if pol == 'SAM_CREAMCE_Policy':
      p = policy
      a = args
      if policy is None:
        from DIRAC.ResourceStatusSystem.Policy.SAMResults_Policy import SAMResults_Policy 
        p = SAMResults_Policy()
      if args is None:
        a = (granularity, name, status, None, 
             ['bi', 'csh', 'gfal', 'swdir', 'creamvoms'])
      res = self.__innerEval(p, a)
  
    if pol == 'SAM_SE_Policy':
      p = policy
      a = args
      if policy is None:
        from DIRAC.ResourceStatusSystem.Policy.SAMResults_Policy import SAMResults_Policy 
        p = SAMResults_Policy()
      if args is None:
        a = (granularity, name, status, None, 
             ['DiracTestUSER', 'FileAccessV2'])
      res = self.__innerEval(p, a)
  
    if pol == 'SAM_LFC_C_Policy':
      p = policy
      a = args
      if policy is None:
        from DIRAC.ResourceStatusSystem.Policy.SAMResults_Policy import SAMResults_Policy 
        p = SAMResults_Policy()
      if args is None:
        a = (granularity, name, status, None, 
             ['lfcwf', 'lfclr', 'lfcls', 'lfcping'])
      res = self.__innerEval(p, a)
  
    if pol == 'SAM_LFC_L_Policy':
      p = policy
      a = args
      if policy is None:
        from DIRAC.ResourceStatusSystem.Policy.SAMResults_Policy import SAMResults_Policy 
        p = SAMResults_Policy()
      if args is None:
        a = (granularity, name, status, None, 
             ['lfcstreams', 'lfclr', 'lfcls', 'lfcping'])
      res = self.__innerEval(p, a)
  
    if pol == 'GGUSTickets_Policy':
      p = policy
      a = args
      if policy is None:
        from DIRAC.ResourceStatusSystem.Policy.GGUSTickets_Policy import GGUSTickets_Policy 
        p = GGUSTickets_Policy()
      if args is None:
        a = (granularity, name, status)
      res = self.__innerEval(p, a)
  
    if pol == 'PilotsEfficiency_Policy':
      p = policy
      a = args
      if policy is None:
        from DIRAC.ResourceStatusSystem.Policy.PilotsEfficiency_Policy import PilotsEfficiency_Policy 
        p = PilotsEfficiency_Policy()
      if args is None:
        a = (granularity, name, status)
      res = self.__innerEval(p, a)
  
    if pol == 'PilotsEfficiencySimple_Policy_Service':
      p = policy
      a = args
      if policy is None:
        from DIRAC.ResourceStatusSystem.Policy.PilotsEfficiency_Simple_Policy import PilotsEfficiency_Simple_Policy 
        p = PilotsEfficiency_Simple_Policy()
      if args is None:
        a = (granularity, name, status)
      res = self.__innerEval(p, a)
  
    if pol == 'PilotsEfficiencySimple_Policy_Resource':
      p = policy
      a = args
      if policy is None:
        from DIRAC.ResourceStatusSystem.Policy.PilotsEfficiency_Simple_Policy import PilotsEfficiency_Simple_Policy 
        p = PilotsEfficiency_Simple_Policy()
      if args is None:
        a = (granularity, name, status)
      res = self.__innerEval(p, a)
  
    if pol == 'JobsEfficiency_Policy':
      p = policy
      a = args
      if policy is None:
        from DIRAC.ResourceStatusSystem.Policy.JobsEfficiency_Policy import JobsEfficiency_Policy 
        p = JobsEfficiency_Policy()
      if args is None:
        a = (granularity, name, status)
      res = self.__innerEval(p, a)
  
    if pol == 'JobsEfficiencySimple_Policy':
      p = policy
      a = args
      if policy is None:
        from DIRAC.ResourceStatusSystem.Policy.JobsEfficiency_Simple_Policy import JobsEfficiency_Simple_Policy 
        p = JobsEfficiency_Simple_Policy()
      if args is None:
        a = (granularity, name, status)
      res = self.__innerEval(p, a)
  
    if pol == 'OnSitePropagation_Policy':
      p = policy
      a = args
      if policy is None:
        from DIRAC.ResourceStatusSystem.Policy.Propagation_Policy import Propagation_Policy 
        p = Propagation_Policy()
      if args is None:
        a = (granularity, name, status, 'Service')
      res = self.__innerEval(p, a)

    if pol == 'OnComputingServicePropagation_Policy':
      p = policy
      a = args
      if policy is None:
        from DIRAC.ResourceStatusSystem.Policy.Propagation_Policy import Propagation_Policy 
        p = Propagation_Policy()
      if args is None:
        a = (granularity, name, status, 'Resource')
      res = self.__innerEval(p, a)

    if pol == 'OnStorageServicePropagation_Policy_Resources':
      p = policy
      a = args
      if policy is None:
        from DIRAC.ResourceStatusSystem.Policy.Propagation_Policy import Propagation_Policy 
        p = Propagation_Policy()
      if args is None:
        a = (granularity, name, status, 'Resource')
      res = self.__innerEval(p, a)
    
    if pol == 'OnStorageServicePropagation_Policy_StorageElements':
      p = policy
      a = args
      if policy is None:
        from DIRAC.ResourceStatusSystem.Policy.Propagation_Policy import Propagation_Policy 
        p = Propagation_Policy()
      if args is None:
        a = (granularity, name, status, 'StorageElement')
      res = self.__innerEval(p, a)
    
    if pol == 'OnServicePropagation_Policy':
      p = policy
      a = args
      if policy is None:
        from DIRAC.ResourceStatusSystem.Policy.OnServicePropagation_Policy import OnServicePropagation_Policy 
        p = OnServicePropagation_Policy()
      if args is None:
        a = (granularity, name, status)
      res = self.__innerEval(p, a)
  
    if pol == 'OnSENodePropagation_Policy':
      p = policy
      a = args
      if policy is None:
        from DIRAC.ResourceStatusSystem.Policy.OnSENodePropagation_Policy import OnSENodePropagation_Policy 
        p = OnSENodePropagation_Policy()
      if args is None:
        a = (granularity, name, status)
      res = self.__innerEval(p, a)
  
    if pol == 'TransferQuality_Policy':
      p = policy
      a = args
      if policy is None:
        from DIRAC.ResourceStatusSystem.Policy.TransferQuality_Policy import TransferQuality_Policy 
        p = TransferQuality_Policy()
      if args is None:
        a = (granularity, name, status)
      res = self.__innerEval(p, a)
  
    res['PolicyName'] = pol
  
    return res

        
#############################################################################

  def __innerEval(self, p, a, knownInfo=None):
    """ policy evaluation
    """
    policyInvoker = PolicyInvoker()
  
    policyInvoker.setPolicy(p)
    res = policyInvoker.evaluatePolicy(a, knownInfo = knownInfo)
    return res 
      
#############################################################################


    
  def _evaluate(self, policyResults):
    
    if len(policyResults) == 1:
      return self._policyCombination(policyResults[0])
    elif len(policyResults) == 2:
      return self._policyCombination(policyResults[0], policyResults[1])
    elif len(policyResults) == 3:
      return self._policyCombination(policyResults[0], policyResults[1], policyResults[2])
    elif len(policyResults) == 4:
      return self._policyCombination(policyResults[0], policyResults[1], policyResults[2], 
                                     policyResults[3])

  
#############################################################################
 
  def _policyCombination(self, *args):
    
    if len(args) == 1:
      res = {}
      for k in args[0].keys():
        res[k] = args[0][k]
      return res
        
    elif len(args) == 2:
    
      if ( ( not args[0]['SAT'] ) and ( not args[1]['SAT'] ) ):
        compReason = args[0]['Reason'] + '|' + args[1]['Reason']
        pcr = args[0]

      # only one of the two is SAT
      elif ( ( args[0]['SAT'] and ( not args[1]['SAT'] ) )
            or
            ( ( not args[0]['SAT'] ) and args[1]['SAT'] ) ):
        s0 = args[0]['Status']
        s1 = args[1]['Status']
        if ValidStatus.index(s0) > ValidStatus.index(s1):
          pcr = args[0]
        elif ValidStatus.index(s0) < ValidStatus.index(s1):
          pcr = args[1]

      # both are SAT
      elif args[0]['SAT'] and args[1]['SAT']:
        s0 = args[0]['Status']
        s1 = args[1]['Status']

        if ValidStatus.index(s0) > ValidStatus.index(s1):
          pcr = args[0]
        elif ValidStatus.index(s0) < ValidStatus.index(s1):
          pcr = args[1]
        else:
          pcr = args[0]
          compReason = args[0]['Reason'] + '|' + args[1]['Reason']

      # if there's an EndDate
      if args[0].has_key('EndDate') and args[1].has_key('EndDate'):
        endDate = max(args[0]['EndDate'], args[1]['EndDate'])
      elif args[0].has_key('EndDate') and not args[1].has_key('EndDate'):
        endDate = args[0]['EndDate']
      elif not args[0].has_key('EndDate') and args[1].has_key('EndDate'):
        endDate = args[1]['EndDate']

      res = {}

      for k in pcr.keys():
        res[k] = pcr[k]
      try:
        res['Reason'] = compReason
      except:
        pass
      try:
        res['EndDate'] = endDate
      except:
        pass
    
      return res
    
    elif len(args) == 3:
      
      resFirstCombination = self._policyCombination(args[0], args[1])
      resSecondCombination = self._policyCombination(resFirstCombination, args[2])
      return resSecondCombination

    elif len(args) == 4:
      
      resFirstCombination = self._policyCombination(args[0], args[1])
      resSecondCombination = self._policyCombination(resFirstCombination, args[2])
      resThirdCombination = self._policyCombination(resSecondCombination, args[3])
      return resThirdCombination

#############################################################################
