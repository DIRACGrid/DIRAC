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

#############################################################################
  
  def __init__(self, granularity, name, status, formerStatus, reason):
    """ 
    PDP (Policy Decision Point) initialization
  
    :params:
      granularity: string - a ValidRes
    
      name: string - name (e.g. of a site)
      
      status: string - status
      
      formerStatus: string - former status
      
      reason: string - optional reason for last status change
    """
    granularity = granularity.capitalize()
    if granularity not in ValidRes:
      raise InvalidRes, where(self, self.__init__)
    self.__granularity = granularity
    self.__name = name
    status = status.capitalize()
    if status not in ValidStatus:
      raise InvalidStatus, where(self, self.__init__)
    self.__status = status
    formerStatus = formerStatus.capitalize()
    if formerStatus not in ValidStatus:
      raise InvalidStatus, where(self, self.__init__)
    self.__formerStatus = formerStatus
    self.__reason = reason

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
            'Enddate: datetime (in a string)}
    """
  
    self.args = argsIn
    self.policy = policyIn
    self.knownInfo = knownInfo
    
    if self.__granularity == 'Site': 
      if self.__status == 'Active':
        if self.__formerStatus == 'Probing':
          EVAL = Configurations.SAP
        elif self.__formerStatus == 'Banned':
          EVAL = Configurations.SAB
      elif self.__status == 'Probing':
        if self.__formerStatus == 'Active':
          EVAL = Configurations.SPA
        elif self.__formerStatus == 'Banned':
          EVAL = Configurations.SPB
      elif self.__status == 'Banned':
        if self.__formerStatus == 'Active':
          EVAL = Configurations.SBA
        elif self.__formerStatus == 'Probing':
          EVAL = Configurations.SBP
          
    elif self.__granularity == 'Service':
      if self.__status == 'Active':
        if self.__formerStatus == 'Probing':
          EVAL = Configurations.SeAP
        elif self.__formerStatus == 'Banned':
          EVAL = Configurations.SeAB
      elif self.__status == 'Probing':
        if self.__formerStatus == 'Active':
          EVAL = Configurations.SePA
        elif self.__formerStatus == 'Banned':
          EVAL = Configurations.SePB
      elif self.__status == 'Banned':
        if self.__formerStatus == 'Active':
          EVAL = Configurations.SeBA
        elif self.__formerStatus == 'Probing':
          EVAL = Configurations.SeBP
            
    elif self.__granularity == 'Resource': 
      if self.__status == 'Active':
        if self.__formerStatus == 'Probing':
          EVAL = Configurations.RAP
        elif self.__formerStatus == 'Banned':
          EVAL = Configurations.RAB
      elif self.__status == 'Probing':
        if self.__formerStatus == 'Active':
          EVAL = Configurations.RPA
        elif self.__formerStatus == 'Banned':
          EVAL = Configurations.RPB
      elif self.__status == 'Banned':
        if self.__formerStatus == 'Active':
          EVAL = Configurations.RBA
        elif self.__formerStatus == 'Probing':
          EVAL = Configurations.RBP
            

    for policyGroup in EVAL:
    
      self.__policyType = policyGroup['PolicyType']
  
      if self.policy is not None:
        res = self.policy.evaluate(self.args)
      else:
        res = self._evaluate(policyGroup['Policies'])
      
      policyResults = []
      
      #policy results communication
      if res['SAT']:
        newstatus = res['Status']
        reason = res['Reason']
        decision = {'PolicyType': self.__policyType, 'Action': True, 'Status':'%s'%newstatus, 'Reason':'%s'%reason}
        if res.has_key('Enddate'):
          decision['Enddate'] = res['Enddate']
        policyResults.append(decision)
      elif not res['SAT']:
        reason = res['Reason']
        decision = {'PolicyType': self.__policyType, 'Action': False, 'Reason':'%s'%reason}
        if res.has_key('Enddate'):
          decision['Enddate'] = res['Enddate']
        policyResults.append(decision)

    return policyResults

#############################################################################
    
  def _evaluate(self, policies):
    
    policyResults = self._policyInvocation(self.__granularity, self.__name, self.__status, self.policy, self.args, policies)

    if len(policyResults) == 1:
      return self._policyCombination(policyResults[0])
    if len(policyResults) == 2:
      return self._policyCombination(policyResults[0], policyResults[1])
    if len(policyResults) == 3:
      return self._policyCombination(policyResults[0], policyResults[1], policyResults[2])
    if len(policyResults) == 4:
      return self._policyCombination(policyResults[0], policyResults[1], policyResults[2], policyResults[3])

  
#############################################################################
  
  def _policyInvocation(self, granularity, name, status, policy, args, policies):
    
    for i in range(len(policies)):
      self.__policyInternalInvocation(granularity, name, status, policy, args, policies[i])
    
#    maxNumberOfThreads = len(policies)
#    threadPool = ThreadPool(1, len(policies))
#    if not threadPool:
#      raise RSSException, where(self, self._policyInvocation)
#    
#    try:
#      for i in range(maxNumberOfThreads):
#        policyResGetter = ThreadedJob(self.__policyInternalInvocation, args = [granularity, name, status, policy, args, policies[i]])
#        threadPool.queueJob(policyResGetter)
#  
#      #threadPool.processAllResults()
#      threadPool.processResults()
#    
#
#    except Exception, x:
#      raise RSSException, where(self, self._policyInvocation) + x
#
#    
#    while(threadPool.isWorking()):
#      time.sleep(1)
    
    return self.policyResults
      
#############################################################################
  
  def __policyInternalInvocation(self, granularity, name, status, policy, args, policyToEval):
  
    res = Configurations.policyInvocation(granularity = granularity, name = name, status = status, policy = policy, args = args, pol = policyToEval)
    
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

#############################################################################
