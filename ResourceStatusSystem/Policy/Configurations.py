""" DIRAC.ResourceStatusSystem.Policy.Configuration Module

    collects everything needed to configure policies
"""

from DIRAC.ResourceStatusSystem.Utilities.Utils import *

#############################################################################
# site/resource checking frequency
#############################################################################

notified_users = 'fstagni'

#from DIRAC.FrameworkSystem.Client.NotificationClient import NotificationClient
#nc = NotificationClient()
#notified_users = nc.getAssigneeGroups()['Value']['RSS_alarms']

#############################################################################
# policies evaluated
#############################################################################

Policies = { 
  'DT_Policy' : 
    { 'Granularity' : ['Site', 'Resource'], 
      'Status' : ValidStatus, 
      'FormerStatus' : ValidStatus,
      'SiteType' : ValidSiteType,
      'ServiceType' : ValidServiceType,
      'ResourceType' : ValidResourceType,
     },
  'SAM_Policy' : 
    { 'Granularity' : ['Resource'], 
      'Status' : ValidStatus, 
      'FormerStatus' : ValidStatus,
      'SiteType' : ValidSiteType,
      'ServiceType' : ValidServiceType,
      'ResourceType' : ValidResourceType,
     },
  'JobsEfficiencySimple_Policy' :  
    { 'Granularity' : ['Site'], 
      'Status' : ValidStatus, 
      'FormerStatus' : ValidStatus,
      'SiteType' : ValidSiteType,
      'ServiceType' : ValidServiceType,
      'ResourceType' : ValidResourceType,
     },
  'PilotsEfficiencySimple_Policy' : 
    { 'Granularity' : ['Site', 'Resource'], 
      'Status' : ValidStatus, 
      'FormerStatus' : ValidStatus,
      'SiteType' : ValidSiteType,
      'ServiceType' : ValidServiceType,
      'ResourceType' : ['CE'],
     },
  'OnServicePropagation_Policy' :
    { 'Granularity' : ['Service'], 
      'Status' : ValidStatus, 
      'FormerStatus' : ValidStatus,
      'SiteType' : ValidSiteType,
      'ServiceType' : ValidServiceType,
      'ResourceType' : ValidResourceType,
     },
  'TransferQuality_Policy' :
    { 'Granularity' : ['StorageElement'], 
      'Status' : ValidStatus, 
      'FormerStatus' : ValidStatus,
      'SiteType' : ValidSiteType,
      'ServiceType' : ValidServiceType,
      'ResourceType' : ValidResourceType,
     },
  'AlwaysFalse_Policy' :
    { 'Granularity' : [], 
      'Status' : ValidStatus, 
      'FormerStatus' : ValidStatus,
      'SiteType' : ValidSiteType,
      'ServiceType' : ValidServiceType,
      'ResourceType' : ValidResourceType,
     }
}


Policy_Types = {
  'Resource_PolType' : 
    { 'Granularity' : ['Site', 'Service', 'Resource', 'StorageElement'], 
      'Status' : ValidStatus, 
      'FormerStatus' : ValidStatus,
      'SiteType' : ValidSiteType,
      'ServiceType' : ValidServiceType,
      'ResourceType' : ValidResourceType,
     },
  'Alarm_PolType' : 
    { 'Granularity' : ValidRes, 
      'Status' : ValidStatus, 
      'FormerStatus' : ValidStatus,
      'SiteType' : ValidSiteType,
      'ServiceType' : ValidServiceType,
      'ResourceType' : ValidResourceType,
     },
  'Collective_PolType' :
    { 'Granularity' : [], 
      'Status' : ValidStatus, 
      'FormerStatus' : ValidStatus,
      'SiteType' : ValidSiteType,
      'ServiceType' : ValidServiceType,
      'ResourceType' : ValidResourceType,
     }
}




def getPolicyToApply(granularity, status = None, formerStatus = None, 
                     siteType = None, serviceType = None, resourceType = None ):
  
  pol_to_eval = []
  pol_types = []
  
  for p in Policies.keys():
    if granularity in Policies[p]['Granularity']:
      pol_to_eval.append(p)
      
      if status is not None:
        if status not in Policies[p]['Status']:
          pol_to_eval.remove(p)
      
      if formerStatus is not None:
        if formerStatus not in Policies[p]['FormerStatus']:
          try:
            pol_to_eval.remove(p)
          except Exception:
            continue
          
      if siteType is not None:
        if siteType not in Policies[p]['SiteType']:
          try:
            pol_to_eval.remove(p)
          except Exception:
            continue
          
      if serviceType is not None:
        if serviceType not in Policies[p]['ServiceType']:
          try:
            pol_to_eval.remove(p)
          except Exception:
            continue
          
      if resourceType is not None:
        if resourceType not in Policies[p]['ResourceType']:
          try:
            pol_to_eval.remove(p)
          except Exception:
            continue
          
   
  for pt in Policy_Types.keys():
    if granularity in Policy_Types[pt]['Granularity']:
      pol_types.append(pt)  
  
      if status is not None:
        if status not in Policy_Types[pt]['Status']:
          pol_to_eval.remove(pt)
      
      if formerStatus is not None:
        if formerStatus not in Policy_Types[pt]['FormerStatus']:
          try:
            pol_to_eval.remove(pt)
          except Exception:
            continue
          
      if siteType is not None:
        if siteType not in Policy_Types[pt]['SiteType']:
          try:
            pol_to_eval.remove(pt)
          except Exception:
            continue
          
      if serviceType is not None:
        if serviceType not in Policy_Types[pt]['ServiceType']:
          try:
            pol_to_eval.remove(pt)
          except Exception:
            continue
          
      if resourceType is not None:
        if resourceType not in Policy_Types[pt]['ResourceType']:
          try:
            pol_to_eval.remove(pt)
          except Exception:
            continue
            
  EVAL = [{'PolicyType':pol_types, 'Policies':pol_to_eval}]    
  
  return EVAL


#############################################################################
# policies parameters
#############################################################################

# --- Pilots Efficiency policy --- #
HIGH_PILOTS_NUMBER = 60
MEDIUM_PILOTS_NUMBER = 20
GOOD_PILOTS_EFFICIENCY = 90
MEDIUM_PILOTS_EFFICIENCY = 30
MAX_PILOTS_PERIOD_WINDOW = 720
SHORT_PILOTS_PERIOD_WINDOW = 2
MEDIUM_PILOTS_PERIOD_WINDOW = 8
LARGE_PILOTS_PERIOD_WINDOW = 48

# --- Jobs Efficiency policy --- #
HIGH_JOBS_NUMBER = 60
MEDIUM_JOBS_NUMBER = 20
GOOD_JOBS_EFFICIENCY = 90
MEDIUM_JOBS_EFFICIENCY = 30
MAX_JOBS_PERIOD_WINDOW = 720
SHORT_JOBS_PERIOD_WINDOW = 2
MEDIUM_JOBS_PERIOD_WINDOW = 8
LARGE_JOBS_PERIOD_WINDOW = 48

# --- GGUS Tickets policy --- #
HIGH_TICKTES_NUMBER = 2

# --- SE transfer quality --- #
SE_QUALITY_LOW = 0.60
SE_QUALITY_HIGH = 0.90


#############################################################################
# site/services/resource checking frequency
#############################################################################

Sites_check_freq = {  'T0_ACTIVE_CHECK_FREQUENCY': 5, \
                      'T0_PROBING_CHECK_FREQUENCY': 5, \
                      'T0_BANNED_CHECK_FREQUENCY' : 5, \
                      'T1_ACTIVE_CHECK_FREQUENCY' : 8, \
                      'T1_PROBING_CHECK_FREQUENCY' : 5, \
                      'T1_BANNED_CHECK_FREQUENCY' : 8, \
                      'T2_ACTIVE_CHECK_FREQUENCY' : 40, \
                      'T2_PROBING_CHECK_FREQUENCY' : 20, \
                      'T2_BANNED_CHECK_FREQUENCY' : 40 }

Resources_check_freq = {'T0_ACTIVE_CHECK_FREQUENCY': 10, \
                        'T0_PROBING_CHECK_FREQUENCY': 5, \
                        'T0_BANNED_CHECK_FREQUENCY' : 8, \
                        'T1_ACTIVE_CHECK_FREQUENCY' : 12, \
                        'T1_PROBING_CHECK_FREQUENCY' : 10, \
                        'T1_BANNED_CHECK_FREQUENCY' : 12, \
                        'T2_ACTIVE_CHECK_FREQUENCY' : 40, \
                        'T2_PROBING_CHECK_FREQUENCY' : 20, \
                        'T2_BANNED_CHECK_FREQUENCY' : 40 }

StorageElements_check_freq = {'T0_ACTIVE_CHECK_FREQUENCY': 12, \
                              'T0_PROBING_CHECK_FREQUENCY': 10, \
                              'T0_BANNED_CHECK_FREQUENCY' : 12, \
                              'T1_ACTIVE_CHECK_FREQUENCY' : 15, \
                              'T1_PROBING_CHECK_FREQUENCY' : 10, \
                              'T1_BANNED_CHECK_FREQUENCY' : 12, \
                              'T2_ACTIVE_CHECK_FREQUENCY' : 40, \
                              'T2_PROBING_CHECK_FREQUENCY' : 20, \
                              'T2_BANNED_CHECK_FREQUENCY' : 40 }

#############################################################################
# policies invocation configuration
#############################################################################

def policyInvocation(granularity = None, name = None, status = None, policy = None, 
                     args = None, pol = None, DBIn = None):
  
  if pol == 'DT_Policy':
    p = policy
    a = args
    if policy is None:
      from DIRAC.ResourceStatusSystem.Policy.DT_Policy import DT_Policy 
      p = DT_Policy()
    if args is None:
      a = (granularity, name, status)
    res = _innerEval(p, a)
    
  if pol == 'AlwaysFalse_Policy':
    p = policy
    a = args
    if policy is None:
      from DIRAC.ResourceStatusSystem.Policy.AlwaysFalse_Policy import AlwaysFalse_Policy 
      p = AlwaysFalse_Policy()
    if args is None:
      a = (granularity, name, status)
    res = _innerEval(p, a)

  if pol == 'SAM_Policy':
    p = policy
    a = args
    rsDB = DBIn
    if policy is not None:
      if DBIn is None:
        from DIRAC.ResourceStatusSystem.DB.ResourceStatusDB import ResourceStatusDB
        rsDB = ResourceStatusDB()
      if args is None:
        site = rsDB.getGeneralName(name, 'Resource', 'Site')
        a = (site, name, status)
      res = _innerEval(p, a)
    else:
      from DIRAC.ResourceStatusSystem.Policy.SAMResults_Policy import SAMResults_Policy 
      p = SAMResults_Policy()
      if DBIn is None:
        from DIRAC.ResourceStatusSystem.DB.ResourceStatusDB import ResourceStatusDB
        rsDB = ResourceStatusDB()
      if args is None:
        site = rsDB.getGeneralName(name, 'Resource', 'Site')
        a = (site, name, status)
      res = _innerEval(p, a)

  if pol == 'GGUSTickets_Policy':
    p = policy
    a = args
    if policy is None:
      from DIRAC.ResourceStatusSystem.Policy.GGUSTickets_Policy import GGUSTickets_Policy 
      p = GGUSTickets_Policy()
    if args is None:
      a = (name, status)
    res = _innerEval(p, a)

  if pol == 'PilotsEfficiency_Policy':
    p = policy
    a = args
    if policy is None:
      from DIRAC.ResourceStatusSystem.Policy.PilotsEfficiency_Policy import PilotsEfficiency_Policy 
      p = PilotsEfficiency_Policy()
    if args is None:
      if granularity == 'Resource':
        name = (name, rsDB.getGeneralName(name, 'Resource', 'Site'))
      a = (granularity, name, status)
    res = _innerEval(p, a)

  if pol == 'PilotsEfficiencySimple_Policy':
    p = policy
    a = args
    if policy is None:
      from DIRAC.ResourceStatusSystem.Policy.PilotsEfficiency_Simple_Policy import PilotsEfficiency_Simple_Policy 
      p = PilotsEfficiency_Simple_Policy()
    if args is None:
      a = (granularity, name, status)
    res = _innerEval(p, a)

  if pol == 'JobsEfficiency_Policy':
    p = policy
    a = args
    if policy is None:
      from DIRAC.ResourceStatusSystem.Policy.JobsEfficiency_Policy import JobsEfficiency_Policy 
      p = JobsEfficiency_Policy()
    if args is None:
      a = (granularity, name, status)
    res = _innerEval(p, a)

  if pol == 'JobsEfficiencySimple_Policy':
    p = policy
    a = args
    if policy is None:
      from DIRAC.ResourceStatusSystem.Policy.JobsEfficiency_Simple_Policy import JobsEfficiency_Simple_Policy 
      p = JobsEfficiency_Simple_Policy()
    if args is None:
      a = (granularity, name, status)
    res = _innerEval(p, a)

  if pol == 'OnServicePropagation_Policy':
    p = policy
    a = args
    if policy is None:
      from DIRAC.ResourceStatusSystem.Policy.OnServicePropagation_Policy import OnServicePropagation_Policy 
      p = OnServicePropagation_Policy()
    if args is None:
      a = (name, status)
    res = _innerEval(p, a)

  if pol == 'TransferQuality_Policy':
    p = policy
    a = args
    if policy is None:
      from DIRAC.ResourceStatusSystem.Policy.TransferQuality_Policy import TransferQuality_Policy 
      p = TransferQuality_Policy()
    if args is None:
      a = (name, status)
    res = _innerEval(p, a)

  return res

        
#############################################################################

def _innerEval(p, a, ki=None):
  """ policy evaluation
  """
  from DIRAC.ResourceStatusSystem.Policy.PolicyInvoker import PolicyInvoker
  policyInvoker = PolicyInvoker()

  policyInvoker.setPolicy(p)
  res = policyInvoker.evaluatePolicy(a, ki)
  return res 
      
#############################################################################
