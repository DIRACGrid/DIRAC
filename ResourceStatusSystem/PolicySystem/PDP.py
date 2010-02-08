""" 
The PDP (Policy Decision Point) module is used to:

1. Decides which policies have to be applied.

2. Invokes an evaluation of the policies, and returns the result (to a PEP)
"""
#############################################################################

import time
import threading
from DIRAC.ResourceStatusSystem.Utilities.Utils import *
from DIRAC.ResourceStatusSystem.Utilities.Exceptions import *
from DIRAC.Core.Utilities.ThreadPool import ThreadPool,ThreadedJob
from DIRAC.ResourceStatusSystem.Policy import Configurations

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


    self.policyResults = []

    self.lockObj = threading.RLock()
    

      
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

            
    EVAL = Configurations.getPolicyToApply(granularity = self.__granularity, 
                                           status = self.__status, 
                                           formerStatus = self.__formerStatus, 
                                           siteType = self.__siteType, 
                                           serviceType = self.__serviceType,
                                           resourceType = self.__resourceType)

      
    for policyGroup in EVAL:
          
      self.__policyType = policyGroup['PolicyType']
  
      if self.policy is not None:
        res = self.policy.evaluate(self.args)
      else:
        if policyGroup['Policies'] is None:
          return [{'PolicyType': self.__policyType, 'Action': False, 'Reason':'No policy results'}]
        res = self._evaluate(policyGroup['Policies'])
                  
      policyResults = []
      
      if res == None:
        return [{'PolicyType': self.__policyType, 'Action': False, 'Reason':'No policy results'}]
      
      #policy results communication
      if res['SAT']:
        newstatus = res['Status']
        reason = res['Reason']
        decision = {'PolicyType': self.__policyType, 'Action': True, 'Status':'%s'%newstatus, 
                    'Reason':'%s'%reason}
        if res.has_key('EndDate'):
          decision['EndDate'] = res['EndDate']
        policyResults.append(decision)
      elif not res['SAT']:
        reason = res['Reason']
        decision = {'PolicyType': self.__policyType, 'Action': False, 'Reason':'%s'%reason}
        if res.has_key('EndDate'):
          decision['EndDate'] = res['EndDate']
        policyResults.append(decision)

    return policyResults

#############################################################################
    
  def _evaluate(self, policies):
    
    policyResults = self._policyInvocation(self.__granularity, self.__name, self.__status, 
                                           self.policy, self.args, policies)

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
  
  def _policyInvocation(self, granularity, name, status, policy, args, policies):
    
    for i in range(len(policies)):
      self.__policyInternalInvocation(granularity, name, status, policy, args, policies[i])
    
    return self.policyResults
      
#############################################################################
  
  def __policyInternalInvocation(self, granularity, name, status, policy, args, policyToEval):
  
    res = Configurations.policyInvocation(granularity = granularity, name = name, 
                                          status = status, policy = policy, args = args, 
                                          pol = policyToEval)
    
    if res['SAT'] != None:
      self.lockObj.acquire()
      try:
        self.policyResults.append(res)
      finally:
        self.lockObj.release()

    
#############################################################################
  
  def _policyCombination(self, *args):
    
    if len(args) == 1:
      return args[0]
        
    elif len(args) == 2:
    
      # none is SAT
      if not args[0]['SAT'] and not args[1]['SAT']:
        compReason = args[0]['Reason'] + '|' + args[1]['Reason']
        if args[0].has_key('Enddate') and args[1].has_key('Enddate'):
          new = args[0]
          new['EndDate'] = max(args[0]['Enddate'], args[1]['Enddate'])
          new['Reason'] = compReason
          return new 
        elif args[0].has_key('Enddate'):
          new = args[0]
          new['Reason'] = compReason
          return new
        else:
          new = args[1]
          new['Reason'] = compReason
          return new

      # only the first of the two is SAT
      elif (args[0]['SAT'] and not args[1]['SAT']):
        s0 = args[0]['Status']
        s1 = args[1]['Status']
        if ValidStatus.index(s0) > ValidStatus.index(s1):
          return args[0]
        elif ValidStatus.index(s0) < ValidStatus.index(s1):
          return args[1]

      # only the second of the two is SAT
      elif (not args[0]['SAT'] and args[1]['SAT']):
        s0 = args[0]['Status']
        s1 = args[1]['Status']
        if ValidStatus.index(s0) > ValidStatus.index(s1):
          return args[0]
        elif ValidStatus.index(s0) < ValidStatus.index(s1):
          return args[1]

      # both are SAT
      elif args[0]['SAT'] and args[1]['SAT']:
        s0 = args[0]['Status']
        s1 = args[1]['Status']

        if ValidStatus.index(s0) > ValidStatus.index(s1):
          return args[0]
        elif ValidStatus.index(s0) < ValidStatus.index(s1):
          return args[1]
        else:
          compReason = args[0]['Reason'] + '|' + args[1]['Reason']
        if args[0].has_key('Enddate') and args[1].has_key('Enddate'):
          new = args[0]
          new['EndDate'] = max(args[0]['Enddate'], args[1]['Enddate'])
          new['Reason'] = compReason
          return new 
        elif args[0].has_key('Enddate'):
          new = args[0]
          new['Reason'] = compReason
          return new
        else:
          new = args[1]
          new['Reason'] = compReason
          return new
        

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
