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
    
    
    if pol == 'DT_Policy_OnGoing_Only':
      if p is None:
        from DIRAC.ResourceStatusSystem.Policy.DT_Policy import DT_Policy 
        p = DT_Policy()
      
    elif pol == 'DT_Policy_Scheduled':
      if p is None:
        from DIRAC.ResourceStatusSystem.Policy.DT_Policy import DT_Policy 
        p = DT_Policy()
      
    elif pol == 'GGUSTickets_Policy':
      if p is None:
        from DIRAC.ResourceStatusSystem.Policy.GGUSTickets_Policy import GGUSTickets_Policy 
        p = GGUSTickets_Policy()
  
    elif pol == 'SAM_Policy':
      if p is None:
        from DIRAC.ResourceStatusSystem.Policy.SAMResults_Policy import SAMResults_Policy 
        p = SAMResults_Policy()
  
    elif pol == 'SAM_CE_Policy':
      if p is None:
        from DIRAC.ResourceStatusSystem.Policy.SAMResults_Policy import SAMResults_Policy 
        p = SAMResults_Policy()
  
    elif pol == 'SAM_CREAMCE_Policy':
      if p is None:
        from DIRAC.ResourceStatusSystem.Policy.SAMResults_Policy import SAMResults_Policy 
        p = SAMResults_Policy()

    elif pol == 'SAM_SE_Policy':
      if p is None:
        from DIRAC.ResourceStatusSystem.Policy.SAMResults_Policy import SAMResults_Policy 
        p = SAMResults_Policy()
  
    elif pol == 'SAM_LFC_C_Policy':
      if p is None:
        from DIRAC.ResourceStatusSystem.Policy.SAMResults_Policy import SAMResults_Policy 
        p = SAMResults_Policy()
  
    elif pol == 'SAM_LFC_L_Policy':
      if p is None:
        from DIRAC.ResourceStatusSystem.Policy.SAMResults_Policy import SAMResults_Policy 
        p = SAMResults_Policy()
  
    elif pol == 'PilotsEfficiency_Policy':
      if p is None:
        from DIRAC.ResourceStatusSystem.Policy.PilotsEfficiency_Policy import PilotsEfficiency_Policy 
        p = PilotsEfficiency_Policy()
  
    elif pol == 'PilotsEfficiencySimple_Policy_Service':
      if p is None:
        from DIRAC.ResourceStatusSystem.Policy.PilotsEfficiency_Simple_Policy import PilotsEfficiency_Simple_Policy 
        p = PilotsEfficiency_Simple_Policy()
  
    elif pol == 'PilotsEfficiencySimple_Policy_Resource':
      if p is None:
        from DIRAC.ResourceStatusSystem.Policy.PilotsEfficiency_Simple_Policy import PilotsEfficiency_Simple_Policy 
        p = PilotsEfficiency_Simple_Policy()

    elif pol == 'JobsEfficiency_Policy':
      if p is None:
        from DIRAC.ResourceStatusSystem.Policy.JobsEfficiency_Policy import JobsEfficiency_Policy 
        p = JobsEfficiency_Policy()
  
    elif pol == 'JobsEfficiencySimple_Policy':
      if p is None:
        from DIRAC.ResourceStatusSystem.Policy.JobsEfficiency_Simple_Policy import JobsEfficiency_Simple_Policy 
        p = JobsEfficiency_Simple_Policy()
  
    elif pol == 'OnSitePropagation_Policy':
      if p is None:
        from DIRAC.ResourceStatusSystem.Policy.Propagation_Policy import Propagation_Policy 
        p = Propagation_Policy()

    elif pol == 'OnComputingServicePropagation_Policy':
      if p is None:
        from DIRAC.ResourceStatusSystem.Policy.Propagation_Policy import Propagation_Policy 
        p = Propagation_Policy()

    elif pol == 'OnStorageServicePropagation_Policy_Resources':
      if p is None:
        from DIRAC.ResourceStatusSystem.Policy.Propagation_Policy import Propagation_Policy 
        p = Propagation_Policy()
    
    elif pol == 'OnStorageServicePropagation_Policy_StorageElements':
      if p is None:
        from DIRAC.ResourceStatusSystem.Policy.Propagation_Policy import Propagation_Policy 
        p = Propagation_Policy()
    
    elif pol == 'OnServicePropagation_Policy':
      if p is None:
        from DIRAC.ResourceStatusSystem.Policy.OnServicePropagation_Policy import OnServicePropagation_Policy 
        p = OnServicePropagation_Policy()
  
    elif pol == 'OnSENodePropagation_Policy':
      if p is None:
        from DIRAC.ResourceStatusSystem.Policy.OnSENodePropagation_Policy import OnSENodePropagation_Policy 
        p = OnSENodePropagation_Policy()
  
    elif pol == 'OnStorageElementPropagation_Policy':
      if p is None:
        from DIRAC.ResourceStatusSystem.Policy.OnStorageElementPropagation_Policy import OnStorageElementPropagation_Policy 
        p = OnStorageElementPropagation_Policy()
      
    elif pol == 'TransferQuality_Policy':
      if p is None:
        from DIRAC.ResourceStatusSystem.Policy.TransferQuality_Policy import TransferQuality_Policy 
        p = TransferQuality_Policy()
    
    elif pol == 'SEOccupancy_Policy':
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