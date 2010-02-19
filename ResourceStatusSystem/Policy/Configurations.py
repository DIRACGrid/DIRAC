""" DIRAC.ResourceStatusSystem.Policy.Configuration Module

    collects everything needed to configure policies
"""

from DIRAC.ResourceStatusSystem.Utilities.Utils import *
from DIRAC import gConfig

#############################################################################
# alarms and notifications
#############################################################################

notified_users = ['fstagni', 'roma']

#from DIRAC.FrameworkSystem.Client.NotificationClient import NotificationClient
#nc = NotificationClient()
#notified_users = nc.getAssigneeGroups()['Value']['RSS_alarms']

AssigneeGroups = {
  'VladRob_PROD-Mail': 
  {'Users': ['roma', 'santinel'],
   'Setup': ['LHCb-Production'],
   'Granularity': ValidRes,
   'SiteType': ['T0', 'T1'], 
   'Notifications': ['Mail']
   }, 
  'VladRob_PROD-Web': 
  {'Users': ['roma', 'santinel'],
   'Setup': ['LHCb-Production'],
   'Granularity': ValidRes,
   'SiteType': ValidSiteType, 
   'Notifications': ['Web']
   }, 
  'VladRob_DEV': 
  {'Users': ['roma', 'santinel'],
   'Setup': ['LHCb-Development', 'LHCb-Certification'], 
   'Granularity': ValidRes,
   'SiteType': [], 
   'Notifications': ['Web']
   }, 
  'me_PROD-Mail': 
  {'Users': ['fstagni'],
   'Setup': ['LHCb-Production'],
   'Granularity': ValidRes,
   'SiteType': ['T0', 'T1'],
   'Notifications': ['Mail']
   }, 
  'me_PROD-Web': 
  {'Users': ['fstagni'],
   'Setup': ['LHCb-Production'],
   'Granularity': ValidRes,
   'SiteType': ValidSiteType, 
   'Notifications': ['Web']
   }, 
  'me_DEV': 
  {'Users': ['fstagni'],
   'Setup': ['LHCb-Development', 'LHCb-Certification'], 
   'Granularity': ValidRes,
   'SiteType': ValidSiteType, 
   'Notifications': ['Web']
   }, 
  'Andrew_PROD': 
  {'Users': ['acsmith'],
   'Setup': ['LHCb-Production'],
   'Granularity': ['StorageElement'],
   'SiteType': ValidSiteType, 
   'Notifications': ['Web', 'Mail']
   }, 
  'Andrew_DEV': 
  {'Users': ['acsmith'],
   'Setup': ['LHCb-Development', 'LHCb-Certification'], 
   'Granularity': ['StorageElement'],
   'SiteType': ValidSiteType, 
   'Notifications': ['Web']
   }, 
}


#_setup = gConfig.getValue("DIRAC/Setup")

#############################################################################
# policies evaluated
#############################################################################

Policies = { 
  'DT_Policy_OnGoing_Only' : 
    { 'Granularity' : [], 
      'Status' : ValidStatus, 
      'FormerStatus' : ValidStatus,
      'SiteType' : ValidSiteType,
      'ServiceType' : ValidServiceType,
      'ResourceType' : ValidResourceType,
     },
  'DT_Policy_Scheduled' : 
    { 'Granularity' : ['Site', 'Resource'], 
      'Status' : ValidStatus, 
      'FormerStatus' : ValidStatus,
      'SiteType' : ValidSiteType,
      'ServiceType' : ValidServiceType,
      'ResourceType' : ValidResourceType,
     },
    'SAM_Policy' : 
    { 'Granularity' : [], 
      'Status' : ValidStatus, 
      'FormerStatus' : ValidStatus,
      'SiteType' : ValidSiteType,
      'ServiceType' : ValidServiceType,
      'ResourceType' : ['SE', 'LFC'],
     },
  'SAM_CE_Policy' : 
    { 'Granularity' : ['Resource'], 
      'Status' : ValidStatus, 
      'FormerStatus' : ValidStatus,
      'SiteType' : ValidSiteType,
      'ServiceType' : ValidServiceType,
      'ResourceType' : ['CE'],
     },     
  'SAM_CREAMCE_Policy' : 
    { 'Granularity' : ['Resource'], 
      'Status' : ValidStatus, 
      'FormerStatus' : ValidStatus,
      'SiteType' : ValidSiteType,
      'ServiceType' : ValidServiceType,
      'ResourceType' : ['CREAMCE'],
     },     
  'SAM_SE_Policy' : 
    { 'Granularity' : ['Resource'], 
      'Status' : ValidStatus, 
      'FormerStatus' : ValidStatus,
      'SiteType' : ValidSiteType,
      'ServiceType' : ValidServiceType,
      'ResourceType' : ['SE'],
     },     
  'SAM_LFC_C_Policy' : 
    { 'Granularity' : ['Resource'], 
      'Status' : ValidStatus, 
      'FormerStatus' : ValidStatus,
      'SiteType' : ValidSiteType,
      'ServiceType' : ValidServiceType,
      'ResourceType' : ['LFC_C'],
     },     
  'SAM_LFC_L_Policy' : 
    { 'Granularity' : ['Resource'], 
      'Status' : ValidStatus, 
      'FormerStatus' : ValidStatus,
      'SiteType' : ValidSiteType,
      'ServiceType' : ValidServiceType,
      'ResourceType' : ['LFC_L'],
     },     
  'JobsEfficiencySimple_Policy' :  
    { 'Granularity' : ['Service'], 
      'Status' : ValidStatus, 
      'FormerStatus' : ValidStatus,
      'SiteType' : ValidSiteType,
      'ServiceType' : ['Computing'],
      'ResourceType' : ValidResourceType,
     },
  'PilotsEfficiencySimple_Policy_Service' : 
    { 'Granularity' : ['Service'], 
      'Status' : ValidStatus, 
      'FormerStatus' : ValidStatus,
      'SiteType' : ValidSiteType,
      'ServiceType' : ['Computing'],
      'ResourceType' : ValidResourceType,
     },
  'PilotsEfficiencySimple_Policy_Resource' : 
    { 'Granularity' : ['Resource'], 
      'Status' : ValidStatus, 
      'FormerStatus' : ValidStatus,
      'SiteType' : ValidSiteType,
      'ServiceType' : ValidServiceType,
      'ResourceType' : ['CE', 'CREAMCE'],
     },
  'OnSitePropagation_Policy' :
    { 'Granularity' : ['Site'], 
      'Status' : ValidStatus, 
      'FormerStatus' : ValidStatus,
      'SiteType' : ValidSiteType,
      'ServiceType' : ValidServiceType,
      'ResourceType' : ValidResourceType,
     },
  'OnComputingServicePropagation_Policy' :
    { 'Granularity' : ['Service'], 
      'Status' : ValidStatus, 
      'FormerStatus' : ValidStatus,
      'SiteType' : ValidSiteType,
      'ServiceType' : ['Computing'],
      'ResourceType' : ValidResourceType,
     },
  'OnStorageServicePropagation_Policy_Resources' :
    { 'Granularity' : ['Service'], 
      'Status' : ValidStatus, 
      'FormerStatus' : ValidStatus,
      'SiteType' : ValidSiteType,
      'ServiceType' : ['Storage'],
      'ResourceType' : ValidResourceType,
     },
  'OnStorageServicePropagation_Policy_StorageElements' :
    { 'Granularity' : ['Service'], 
      'Status' : ValidStatus, 
      'FormerStatus' : ValidStatus,
      'SiteType' : ValidSiteType,
      'ServiceType' : ['Storage'],
      'ResourceType' : ValidResourceType,
     },
  'OnServicePropagation_Policy' :
    { 'Granularity' : [], 
      'Status' : ValidStatus, 
      'FormerStatus' : ValidStatus,
      'SiteType' : ValidSiteType,
      'ServiceType' : ValidServiceType,
      'ResourceType' : ValidResourceType,
     },
  'OnSENodePropagation_Policy' :
    { 'Granularity' : [], 
      'Status' : ValidStatus, 
      'FormerStatus' : ValidStatus,
      'SiteType' : ValidSiteType,
      'ServiceType' : ValidServiceType,
      'ResourceType' : ['SE'],
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


#############################################################################
# policies parameters
#############################################################################

DTinHours = 12

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
Transfer_QUALITY_LOW = 0.60
Transfer_QUALITY_HIGH = 0.90


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

Services_check_freq = {'T0_ACTIVE_CHECK_FREQUENCY': 10, \
                       'T0_PROBING_CHECK_FREQUENCY': 5, \
                       'T0_BANNED_CHECK_FREQUENCY' : 8, \
                       'T1_ACTIVE_CHECK_FREQUENCY' : 12, \
                       'T1_PROBING_CHECK_FREQUENCY' : 10, \
                       'T1_BANNED_CHECK_FREQUENCY' : 12, \
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

