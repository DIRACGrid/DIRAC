"""
    Module used for calling policies. Its class is used for invoking
    real policies, based on the policy name
"""

from DIRAC.ResourceStatusSystem.Policy.PolicyInvoker import PolicyInvoker
from DIRAC.ResourceStatusSystem.Policy import Configurations


class PolicyCaller:

#############################################################################

  def __init__(self, commandCallerIn = None):
    
    if commandCallerIn is not None:
      self.cc = commandCallerIn
    else:
      from DIRAC.ResourceStatusSystem.Client.Command.CommandCaller import CommandCaller
      self.cc = CommandCaller()
      
    self.policyInvoker = PolicyInvoker() 

#############################################################################

  def policyInvocation(self, granularity = None, name = None, status = None, policy = None,  
                       args = None, pol = None, extraArgs = None, commandIn = None):
    
    p = policy
    a = args
    
    if p is None:
      if pol == 'DT_OnGoing_Only':
        from DIRAC.ResourceStatusSystem.Policy.DT_Policy import DT_Policy 
        p = DT_Policy()
        
      elif pol == 'DT_Scheduled':
        from DIRAC.ResourceStatusSystem.Policy.DT_Policy import DT_Policy 
        p = DT_Policy()
        
      elif pol == 'GGUSTickets':
        from DIRAC.ResourceStatusSystem.Policy.GGUSTickets_Policy import GGUSTickets_Policy 
        p = GGUSTickets_Policy()
    
      elif pol == 'SAM':
        from DIRAC.ResourceStatusSystem.Policy.SAMResults_Policy import SAMResults_Policy 
        p = SAMResults_Policy()
    
      elif pol == 'SAM_CE':
        from DIRAC.ResourceStatusSystem.Policy.SAMResults_Policy import SAMResults_Policy 
        p = SAMResults_Policy()
    
      elif pol == 'SAM_CREAMCE':
        from DIRAC.ResourceStatusSystem.Policy.SAMResults_Policy import SAMResults_Policy 
        p = SAMResults_Policy()
  
      elif pol == 'SAM_SE':
        from DIRAC.ResourceStatusSystem.Policy.SAMResults_Policy import SAMResults_Policy 
        p = SAMResults_Policy()
    
      elif pol == 'SAM_LFC_C':
        from DIRAC.ResourceStatusSystem.Policy.SAMResults_Policy import SAMResults_Policy 
        p = SAMResults_Policy()
    
      elif pol == 'SAM_LFC_L':
        from DIRAC.ResourceStatusSystem.Policy.SAMResults_Policy import SAMResults_Policy 
        p = SAMResults_Policy()
    
      elif pol == 'PilotsEfficiency':
        from DIRAC.ResourceStatusSystem.Policy.PilotsEfficiency_Policy import PilotsEfficiency_Policy 
        p = PilotsEfficiency_Policy()
    
      elif pol == 'PilotsEfficiencySimple_Service':
        from DIRAC.ResourceStatusSystem.Policy.PilotsEfficiency_Simple_Policy import PilotsEfficiency_Simple_Policy 
        p = PilotsEfficiency_Simple_Policy()
    
      elif pol == 'PilotsEfficiencySimple_Resource':
        from DIRAC.ResourceStatusSystem.Policy.PilotsEfficiency_Simple_Policy import PilotsEfficiency_Simple_Policy 
        p = PilotsEfficiency_Simple_Policy()
  
      elif pol == 'JobsEfficiency':
        from DIRAC.ResourceStatusSystem.Policy.JobsEfficiency_Policy import JobsEfficiency_Policy 
        p = JobsEfficiency_Policy()
    
      elif pol == 'JobsEfficiencySimple':
        from DIRAC.ResourceStatusSystem.Policy.JobsEfficiency_Simple_Policy import JobsEfficiency_Simple_Policy 
        p = JobsEfficiency_Simple_Policy()
    
      elif pol == 'OnSitePropagation':
        from DIRAC.ResourceStatusSystem.Policy.Propagation_Policy import Propagation_Policy 
        p = Propagation_Policy()
  
      elif pol == 'OnComputingServicePropagation':
        from DIRAC.ResourceStatusSystem.Policy.Propagation_Policy import Propagation_Policy 
        p = Propagation_Policy()
  
      elif pol == 'OnStorageServicePropagation_Res':
        from DIRAC.ResourceStatusSystem.Policy.Propagation_Policy import Propagation_Policy 
        p = Propagation_Policy()
      
      elif pol == 'OnStorageServicePropagation_SE':
        from DIRAC.ResourceStatusSystem.Policy.Propagation_Policy import Propagation_Policy 
        p = Propagation_Policy()
      
      elif pol == 'OnServicePropagation':
        from DIRAC.ResourceStatusSystem.Policy.OnServicePropagation_Policy import OnServicePropagation_Policy 
        p = OnServicePropagation_Policy()
    
      elif pol == 'OnSENodePropagation':
        from DIRAC.ResourceStatusSystem.Policy.OnSENodePropagation_Policy import OnSENodePropagation_Policy 
        p = OnSENodePropagation_Policy()
    
      elif pol == 'OnStorageElementPropagation':
        from DIRAC.ResourceStatusSystem.Policy.OnStorageElementPropagation_Policy import OnStorageElementPropagation_Policy 
        p = OnStorageElementPropagation_Policy()
        
      elif pol == 'TransferQuality':
        from DIRAC.ResourceStatusSystem.Policy.TransferQuality_Policy import TransferQuality_Policy
        p = TransferQuality_Policy()
      
      elif pol == 'SEOccupancy':
        from DIRAC.ResourceStatusSystem.Policy.SEOccupancy_Policy import SEOccupancy_Policy 
        p = SEOccupancy_Policy()
      
      elif pol == 'SEQueuedTransfers':
        from DIRAC.ResourceStatusSystem.Policy.SEQueuedTransfers_Policy import SEQueuedTransfers_Policy 
        p = SEQueuedTransfers_Policy()
      
      else:
        from DIRAC.ResourceStatusSystem.Policy.AlwaysFalse_Policy import AlwaysFalse_Policy 
        p = AlwaysFalse_Policy()
  
    if a is None:
      a = (granularity, name, status)

    if extraArgs is not None:
      a = a + extraArgs
    
    if commandIn is not None:
      commandIn = self.cc.setCommandObject(commandIn)
    
    res = self._innerEval(p, a, commandIn = commandIn)

    res['PolicyName'] = pol
  
    return res

        
#############################################################################

  def _innerEval(self, p, a, commandIn = None, knownInfo = None):
    """ policy evaluation
    """

    self.policyInvoker.setPolicy(p)
    
    res = self.policyInvoker.evaluatePolicy(a, commandIn = commandIn, knownInfo = knownInfo)
    return res 
      
#############################################################################