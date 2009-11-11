""" The DT_SAM_Policy class is a policy class that invokes DT_policy and SAMResults_Policy
    and combine its results
"""

#from DIRAC.ResourceStatusSystem.Policy.PolicyBase import PolicyBase
#from DIRAC.ResourceStatusSystem import *
#
#class DT_SAM_Policy(PolicyBase):
#
#  def evaluate(self, args, policyDTIn=None, policySAMIn=None, knownInfo=None):
#    """ evaluate policy on DT, using args (tuple). 
#        - args[0] should be a ValidRes
#        - args[1] should be the name of the ValidRes
#        - args[2] should be the present status
#        
#        returns:
#            { 
#              'SAT':True|False, 
#              'Status':Active|Probing|Banned, 
#              'Reason':'DT:None'|'DT:OUTAGE|'DT:AT_RISK',
#              'Enddate':datetime (if needed)
#            }
#    """ 
#
#    if not isinstance(args, tuple):
#      raise TypeError, where(self, self.evaluate)
#    
#    if args[0].capitalize() not in ValidRes:
#      raise InvalidRes, where(self, self.evaluate)
#    
#    if args[2] not in ValidStatus:
#      raise InvalidStatus, where(self, self.evaluate)
#
#    if knownInfo is not None:
#      if 'DT_SAM' in knownInfo.keys():
#        status = knownInfo
#    else:
#      if policyDTIn is not None:
#        policyDT = policyDTIn
#      else:
#        # use standard Command
#        from DIRAC.ResourceStatusSystem.Policy.DT_Policy import DT_Policy
#        policyDT = DT_Policy()
#
#      if policyDTIn is not None:
#        policySAM = policySAMIn
#      else:
#        # use standard Command
#        from DIRAC.ResourceStatusSystem.Policy.SAMResults_Policy import SAMResults_Policy
#        policySAM = SAMResults_Policy()
#
#      policyDT_res = policyDT.evaluate(args)
#      policySAM_res = policySAM.evaluate(args)
#      
#    
#    result = {}
#    
#    if policyDT_res['SAT']:
#      if policyDT_res['Status'] == 'Banned':
#        return policyDT_res
#
#    return result