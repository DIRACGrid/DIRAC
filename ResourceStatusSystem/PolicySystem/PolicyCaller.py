"""
    Module used for calling policies. Its class is used for invoking
    real policies, based on the policy name
"""

from DIRAC.ResourceStatusSystem.Policy.PolicyInvoker import PolicyInvoker
from DIRAC.ResourceStatusSystem.Policy import Configurations


class PolicyCaller:


  def policyInvocation(self, granularity = None, name = None, status = None, 
                        policy = None, args = None, pol = None, extraArgs = None):
    
    p = policy
    a = args
    
    
    if pol == 'DT_OnGoing_Only':
      if p is None:
        from DIRAC.ResourceStatusSystem.Policy.DT_Policy import DT_Policy 
        p = DT_Policy()
      
    elif pol == 'DT_Scheduled':
      if p is None:
        from DIRAC.ResourceStatusSystem.Policy.DT_Policy import DT_Policy 
        p = DT_Policy()
      
    elif pol == 'GGUSTickets':
      if p is None:
        from DIRAC.ResourceStatusSystem.Policy.GGUSTickets_Policy import GGUSTickets_Policy 
        p = GGUSTickets_Policy()
  
    elif pol == 'SAM':
      if p is None:
        from DIRAC.ResourceStatusSystem.Policy.SAMResults_Policy import SAMResults_Policy 
        p = SAMResults_Policy()
  
    elif pol == 'SAM_CE':
      if p is None:
        from DIRAC.ResourceStatusSystem.Policy.SAMResults_Policy import SAMResults_Policy 
        p = SAMResults_Policy()
  
    elif pol == 'SAM_CREAMCE':
      if p is None:
        from DIRAC.ResourceStatusSystem.Policy.SAMResults_Policy import SAMResults_Policy 
        p = SAMResults_Policy()

    elif pol == 'SAM_SE':
      if p is None:
        from DIRAC.ResourceStatusSystem.Policy.SAMResults_Policy import SAMResults_Policy 
        p = SAMResults_Policy()
  
    elif pol == 'SAM_LFC_C':
      if p is None:
        from DIRAC.ResourceStatusSystem.Policy.SAMResults_Policy import SAMResults_Policy 
        p = SAMResults_Policy()
  
    elif pol == 'SAM_LFC_L':
      if p is None:
        from DIRAC.ResourceStatusSystem.Policy.SAMResults_Policy import SAMResults_Policy 
        p = SAMResults_Policy()
  
    elif pol == 'PilotsEfficiency':
      if p is None:
        from DIRAC.ResourceStatusSystem.Policy.PilotsEfficiency_Policy import PilotsEfficiency_Policy 
        p = PilotsEfficiency_Policy()
  
    elif pol == 'PilotsEfficiencySimple_Service':
      if p is None:
        from DIRAC.ResourceStatusSystem.Policy.PilotsEfficiency_Simple_Policy import PilotsEfficiency_Simple_Policy 
        p = PilotsEfficiency_Simple_Policy()
  
    elif pol == 'PilotsEfficiencySimple_Resource':
      if p is None:
        from DIRAC.ResourceStatusSystem.Policy.PilotsEfficiency_Simple_Policy import PilotsEfficiency_Simple_Policy 
        p = PilotsEfficiency_Simple_Policy()

    elif pol == 'JobsEfficiency':
      if p is None:
        from DIRAC.ResourceStatusSystem.Policy.JobsEfficiency_Policy import JobsEfficiency_Policy 
        p = JobsEfficiency_Policy()
  
    elif pol == 'JobsEfficiencySimple':
      if p is None:
        from DIRAC.ResourceStatusSystem.Policy.JobsEfficiency_Simple_Policy import JobsEfficiency_Simple_Policy 
        p = JobsEfficiency_Simple_Policy()
  
    elif pol == 'OnSitePropagation':
      if p is None:
        from DIRAC.ResourceStatusSystem.Policy.Propagation_Policy import Propagation_Policy 
        p = Propagation_Policy()

    elif pol == 'OnComputingServicePropagation':
      if p is None:
        from DIRAC.ResourceStatusSystem.Policy.Propagation_Policy import Propagation_Policy 
        p = Propagation_Policy()

    elif pol == 'OnStorageServicePropagation_Res':
      if p is None:
        from DIRAC.ResourceStatusSystem.Policy.Propagation_Policy import Propagation_Policy 
        p = Propagation_Policy()
    
    elif pol == 'OnStorageServicePropagation_SE':
      if p is None:
        from DIRAC.ResourceStatusSystem.Policy.Propagation_Policy import Propagation_Policy 
        p = Propagation_Policy()
    
    elif pol == 'OnServicePropagation':
      if p is None:
        from DIRAC.ResourceStatusSystem.Policy.OnServicePropagation_Policy import OnServicePropagation_Policy 
        p = OnServicePropagation_Policy()
  
    elif pol == 'OnSENodePropagation':
      if p is None:
        from DIRAC.ResourceStatusSystem.Policy.OnSENodePropagation_Policy import OnSENodePropagation_Policy 
        p = OnSENodePropagation_Policy()
  
    elif pol == 'OnStorageElementPropagation':
      if p is None:
        from DIRAC.ResourceStatusSystem.Policy.OnStorageElementPropagation_Policy import OnStorageElementPropagation_Policy 
        p = OnStorageElementPropagation_Policy()
      
    elif pol == 'TransferQuality':
      if p is None:
        from DIRAC.ResourceStatusSystem.Policy.TransferQuality_Policy import TransferQuality_Policy 
        p = TransferQuality_Policy()
    
    elif pol == 'SEOccupancy':
      if p is None:
        from DIRAC.ResourceStatusSystem.Policy.SEOccupancy_Policy import SEOccupancy_Policy 
        p = SEOccupancy_Policy()
    
    else:
      if p is None:
        from DIRAC.ResourceStatusSystem.Policy.AlwaysFalse_Policy import AlwaysFalse_Policy 
        p = AlwaysFalse_Policy()
  
    if a is None:
      a = (granularity, name, status)

    if extraArgs is not None:
      a = a + extraArgs
    
    res = self._innerEval(p, a)

    res['PolicyName'] = pol
  
    return res

        
#############################################################################

  def _innerEval(self, p, a, knownInfo=None):
    """ policy evaluation
    """
    policyInvoker = PolicyInvoker()
  
    policyInvoker.setPolicy(p)
    res = policyInvoker.evaluatePolicy(a, knownInfo = knownInfo)
    return res 
      
#############################################################################