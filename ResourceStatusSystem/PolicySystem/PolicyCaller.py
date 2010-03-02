"""
    Module used for calling policies. Its class is used for invoking
    real policies, based on the policy name
"""

from DIRAC.ResourceStatusSystem.Policy.PolicyInvoker import PolicyInvoker
from DIRAC.ResourceStatusSystem.Policy import Configurations


class PolicyCaller:


  def policyInvocation(self, granularity = None, name = None, status = None, 
                        policy = None, args = None, pol = None):
    
    p = policy
    a = args
    
    if pol == 'DT_Policy_OnGoing_Only':
      if p is None:
        from DIRAC.ResourceStatusSystem.Policy.DT_Policy import DT_Policy 
        p = DT_Policy()
      if a is None:
        a = (granularity, name, status)
      
    elif pol == 'DT_Policy_Scheduled':
      if p is None:
        from DIRAC.ResourceStatusSystem.Policy.DT_Policy import DT_Policy 
        p = DT_Policy()
      if a is None:
        a = (granularity, name, status, Configurations.DTinHours)
      
    elif pol == 'SAM_Policy':
      if p is None:
        from DIRAC.ResourceStatusSystem.Policy.SAMResults_Policy import SAMResults_Policy 
        p = SAMResults_Policy()
      if a is None:
        a = (granularity, name, status)
  
    elif pol == 'SAM_CE_Policy':
      if p is None:
        from DIRAC.ResourceStatusSystem.Policy.SAMResults_Policy import SAMResults_Policy 
        p = SAMResults_Policy()
      if a is None:
        a = (granularity, name, status, None, 
             ['LHCb CE-lhcb-availability', 'LHCb CE-lhcb-install', 'LHCb CE-lhcb-job-Boole', 
              'LHCb CE-lhcb-job-Brunel', 'LHCb CE-lhcb-job-DaVinci', 'LHCb CE-lhcb-job-Gauss', 'LHCb CE-lhcb-os', 
              'LHCb CE-lhcb-queues', 'bi', 'csh', 'js', 'gfal', 'swdir', 'voms'])
  
    elif pol == 'SAM_CREAMCE_Policy':
      if p is None:
        from DIRAC.ResourceStatusSystem.Policy.SAMResults_Policy import SAMResults_Policy 
        p = SAMResults_Policy()
      if a is None:
        a = (granularity, name, status, None, 
             ['bi', 'csh', 'gfal', 'swdir', 'creamvoms'])
  
    elif pol == 'SAM_SE_Policy':
      if p is None:
        from DIRAC.ResourceStatusSystem.Policy.SAMResults_Policy import SAMResults_Policy 
        p = SAMResults_Policy()
      if a is None:
        a = (granularity, name, status, None, 
             ['DiracTestUSER', 'FileAccessV2'])
  
    elif pol == 'SAM_LFC_C_Policy':
      if p is None:
        from DIRAC.ResourceStatusSystem.Policy.SAMResults_Policy import SAMResults_Policy 
        p = SAMResults_Policy()
      if a is None:
        a = (granularity, name, status, None, 
             ['lfcwf', 'lfclr', 'lfcls', 'lfcping'])
  
    elif pol == 'SAM_LFC_L_Policy':
      if p is None:
        from DIRAC.ResourceStatusSystem.Policy.SAMResults_Policy import SAMResults_Policy 
        p = SAMResults_Policy()
      if a is None:
        a = (granularity, name, status, None, 
             ['lfcstreams', 'lfclr', 'lfcls', 'lfcping'])
  
    elif pol == 'GGUSTickets_Policy':
      if p is None:
        from DIRAC.ResourceStatusSystem.Policy.GGUSTickets_Policy import GGUSTickets_Policy 
        p = GGUSTickets_Policy()
      if a is None:
        a = (granularity, name, status)
  
    elif pol == 'PilotsEfficiency_Policy':
      if p is None:
        from DIRAC.ResourceStatusSystem.Policy.PilotsEfficiency_Policy import PilotsEfficiency_Policy 
        p = PilotsEfficiency_Policy()
      if a is None:
        a = (granularity, name, status)
  
    elif pol == 'PilotsEfficiencySimple_Policy_Service':
      if p is None:
        from DIRAC.ResourceStatusSystem.Policy.PilotsEfficiency_Simple_Policy import PilotsEfficiency_Simple_Policy 
        p = PilotsEfficiency_Simple_Policy()
      if a is None:
        a = (granularity, name, status)
  
    elif pol == 'PilotsEfficiencySimple_Policy_Resource':
      if p is None:
        from DIRAC.ResourceStatusSystem.Policy.PilotsEfficiency_Simple_Policy import PilotsEfficiency_Simple_Policy 
        p = PilotsEfficiency_Simple_Policy()
      if a is None:
        a = (granularity, name, status)
  
    elif pol == 'JobsEfficiency_Policy':
      if p is None:
        from DIRAC.ResourceStatusSystem.Policy.JobsEfficiency_Policy import JobsEfficiency_Policy 
        p = JobsEfficiency_Policy()
      if a is None:
        a = (granularity, name, status)
  
    elif pol == 'JobsEfficiencySimple_Policy':
      if p is None:
        from DIRAC.ResourceStatusSystem.Policy.JobsEfficiency_Simple_Policy import JobsEfficiency_Simple_Policy 
        p = JobsEfficiency_Simple_Policy()
      if a is None:
        a = (granularity, name, status)
  
    elif pol == 'OnSitePropagation_Policy':
      if p is None:
        from DIRAC.ResourceStatusSystem.Policy.Propagation_Policy import Propagation_Policy 
        p = Propagation_Policy()
      if a is None:
        a = (granularity, name, status, 'Service')

    elif pol == 'OnComputingServicePropagation_Policy':
      if p is None:
        from DIRAC.ResourceStatusSystem.Policy.Propagation_Policy import Propagation_Policy 
        p = Propagation_Policy()
      if a is None:
        a = (granularity, name, status, 'Resource')

    elif pol == 'OnStorageServicePropagation_Policy_Resources':
      if p is None:
        from DIRAC.ResourceStatusSystem.Policy.Propagation_Policy import Propagation_Policy 
        p = Propagation_Policy()
      if a is None:
        a = (granularity, name, status, 'Resource')
    
    elif pol == 'OnStorageServicePropagation_Policy_StorageElements':
      if p is None:
        from DIRAC.ResourceStatusSystem.Policy.Propagation_Policy import Propagation_Policy 
        p = Propagation_Policy()
      if a is None:
        a = (granularity, name, status, 'StorageElement')
    
    elif pol == 'OnServicePropagation_Policy':
      if p is None:
        from DIRAC.ResourceStatusSystem.Policy.OnServicePropagation_Policy import OnServicePropagation_Policy 
        p = OnServicePropagation_Policy()
      if a is None:
        a = (granularity, name, status)
  
    elif pol == 'OnSENodePropagation_Policy':
      if p is None:
        from DIRAC.ResourceStatusSystem.Policy.OnSENodePropagation_Policy import OnSENodePropagation_Policy 
        p = OnSENodePropagation_Policy()
      if a is None:
        a = (granularity, name, status)
  
    elif pol == 'TransferQuality_Policy':
      if p is None:
        from DIRAC.ResourceStatusSystem.Policy.TransferQuality_Policy import TransferQuality_Policy 
        p = TransferQuality_Policy()
      if a is None:
        a = (granularity, name, status)
    
    else:
      if p is None:
        from DIRAC.ResourceStatusSystem.Policy.AlwaysFalse_Policy import AlwaysFalse_Policy 
        p = AlwaysFalse_Policy()
      if a is None:
        a = (granularity, name, status)
  
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