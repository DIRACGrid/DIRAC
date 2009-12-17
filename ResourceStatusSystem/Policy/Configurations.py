""" DIRAC.ResourceStatusSystem.Policy.Configuration Module

    collects everything needed to configure policies
"""

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

#when a site, now active, was probing
#SAP = ['DT_Policy', 'False_Policy']
SAP = [{'PolicyType':['Resource_PolType', 'Alarm_PolType'], \
        'Policies': ['DT_Policy', 'JobsEfficiencySimple_Policy', 'PilotsEfficiencySimple_Policy']}]
#SAP = ['DT_Policy', 'SAM_Policy']
#SAP = ['SAM_Policy']

#when a site, now active, was banned
#SAB = ['DT_Policy', 'False_Policy']
SAB = [{'PolicyType':['Resource_PolType', 'Alarm_PolType'], \
        'Policies': ['DT_Policy', 'JobsEfficiencySimple_Policy', 'PilotsEfficiencySimple_Policy']}]

#when a site, now probing, was active
#SPA = ['DT_Policy', 'False_Policy']
SPA = [{'PolicyType':['Resource_PolType', 'Alarm_PolType'], \
        'Policies': ['DT_Policy', 'JobsEfficiencySimple_Policy', 'PilotsEfficiencySimple_Policy']}]

#when a site, now probing, was banned
#SPB = ['DT_Policy', 'False_Policy']
SPB = [{'PolicyType':['Resource_PolType', 'Alarm_PolType'], \
        'Policies': ['DT_Policy', 'JobsEfficiencySimple_Policy', 'PilotsEfficiencySimple_Policy']}]
       
#when a site, now banned, was active
#SBA = ['DT_Policy', 'False_Policy']
SBA = [{'PolicyType':['Resource_PolType', 'Alarm_PolType'], \
        'Policies': ['DT_Policy', 'JobsEfficiencySimple_Policy', 'PilotsEfficiencySimple_Policy']}]
       
#when a site, now banned, was probing
#SBP = ['DT_Policy', 'False_Policy']
SBP = [{'PolicyType':['Resource_PolType', 'Alarm_PolType'], \
        'Policies': ['DT_Policy', 'JobsEfficiencySimple_Policy', 'PilotsEfficiencySimple_Policy']}]       


#when a service, now active, was probing
SeAP = [{'PolicyType':['Resource_PolType', 'Alarm_PolType'], \
         'Policies': ['OnServicePropagation_Policy']}]

#when a service, now active, was banned
SeAB = [{'PolicyType':['Resource_PolType', 'Alarm_PolType'], \
         'Policies': ['OnServicePropagation_Policy']}]

#when a service, now probing, was active
SePA = [{'PolicyType':['Resource_PolType', 'Alarm_PolType'], \
         'Policies': ['OnServicePropagation_Policy']}]

#when a service, now probing, was banned
SePB = [{'PolicyType':['Resource_PolType', 'Alarm_PolType'], \
         'Policies': ['OnServicePropagation_Policy']}]

#when a service, now banned, was active
SeBA = [{'PolicyType':['Resource_PolType', 'Alarm_PolType'], \
         'Policies': ['OnServicePropagation_Policy']}]

#when a service, now banned, was probing
SeBP = [{'PolicyType':['Resource_PolType', 'Alarm_PolType'], \
         'Policies': ['OnServicePropagation_Policy']}]



#when a resource, now active, was probing
RAP = [{'PolicyType':['Resource_PolType', 'Alarm_PolType'], \
        'Policies': ['DT_Policy', 'SAM_Policy', 'PilotsEfficiencySimple_Policy']}]

#when a resource, now active, was banned
RAB = [{'PolicyType':['Resource_PolType', 'Alarm_PolType'], \
        'Policies': ['DT_Policy', 'SAM_Policy', 'PilotsEfficiencySimple_Policy']}]

#when a resource, now probing, was active
RPA = [{'PolicyType':['Resource_PolType', 'Alarm_PolType'], \
        'Policies': ['DT_Policy', 'SAM_Policy', 'PilotsEfficiencySimple_Policy']}]

#when a resource, now probing, was banned
RPB = [{'PolicyType':['Resource_PolType', 'Alarm_PolType'], \
        'Policies': ['DT_Policy', 'SAM_Policy', 'PilotsEfficiencySimple_Policy']}]

#when a resource, now banned, was active
RBA = [{'PolicyType':['Resource_PolType', 'Alarm_PolType'], \
        'Policies': ['DT_Policy', 'SAM_Policy', 'PilotsEfficiencySimple_Policy']}]

#when a resource, now banned, was probing
RBP = [{'PolicyType':['Resource_PolType', 'Alarm_PolType'], \
        'Policies': ['DT_Policy', 'SAM_Policy', 'PilotsEfficiencySimple_Policy']}]



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

#############################################################################
# policies invocation configuration
#############################################################################

def policyInvocation(granularity = None, name = None, status = None, policy = None, args = None, pol = None, DBIn = None):
  
  if pol == 'DT_Policy':
    p = policy
    a = args
    if policy is None:
      from DIRAC.ResourceStatusSystem.Policy.DT_Policy import DT_Policy 
      p = DT_Policy()
    if args is None:
      a = (granularity, name, status)
    res = _innerEval(p, a)
    
  if pol == 'False_Policy':
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
