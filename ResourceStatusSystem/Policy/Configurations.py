""" DIRAC.ResourceStatusSystem.Policy.Configuration Module

    collects everything needed to configure policies
"""

from DIRAC.ResourceStatusSystem.Utilities.Utils import *
from DIRAC import gConfig

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
                      'T2_ACTIVE_CHECK_FREQUENCY' : 30, \
                      'T2_PROBING_CHECK_FREQUENCY' : 20, \
                      'T2_BANNED_CHECK_FREQUENCY' : 30 }

Services_check_freq = {'T0_ACTIVE_CHECK_FREQUENCY': 10, \
                       'T0_PROBING_CHECK_FREQUENCY': 5, \
                       'T0_BANNED_CHECK_FREQUENCY' : 8, \
                       'T1_ACTIVE_CHECK_FREQUENCY' : 12, \
                       'T1_PROBING_CHECK_FREQUENCY' : 10, \
                       'T1_BANNED_CHECK_FREQUENCY' : 12, \
                       'T2_ACTIVE_CHECK_FREQUENCY' : 30, \
                       'T2_PROBING_CHECK_FREQUENCY' : 20, \
                       'T2_BANNED_CHECK_FREQUENCY' : 30 }

Resources_check_freq = {'T0_ACTIVE_CHECK_FREQUENCY': 10, \
                        'T0_PROBING_CHECK_FREQUENCY': 8, \
                        'T0_BANNED_CHECK_FREQUENCY' : 10, \
                        'T1_ACTIVE_CHECK_FREQUENCY' : 12, \
                        'T1_PROBING_CHECK_FREQUENCY' : 10, \
                        'T1_BANNED_CHECK_FREQUENCY' : 12, \
                        'T2_ACTIVE_CHECK_FREQUENCY' : 30, \
                        'T2_PROBING_CHECK_FREQUENCY' : 20, \
                        'T2_BANNED_CHECK_FREQUENCY' : 30 }

StorageElements_check_freq = {'T0_ACTIVE_CHECK_FREQUENCY': 10, \
                              'T0_PROBING_CHECK_FREQUENCY': 8, \
                              'T0_BANNED_CHECK_FREQUENCY' : 10, \
                              'T1_ACTIVE_CHECK_FREQUENCY' : 12, \
                              'T1_PROBING_CHECK_FREQUENCY' : 10, \
                              'T1_BANNED_CHECK_FREQUENCY' : 12, \
                              'T2_ACTIVE_CHECK_FREQUENCY' : 30, \
                              'T2_PROBING_CHECK_FREQUENCY' : 20, \
                              'T2_BANNED_CHECK_FREQUENCY' : 30 }

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

#############################################################################
# policies evaluated
#############################################################################

Policies = { 
  'DT_Policy_OnGoing_Only' : 
    { 'Granularity' : ['Site', 'Resource'], 
      'Status' : ValidStatus, 
      'FormerStatus' : ValidStatus,
      'SiteType' : ValidSiteType,
      'ServiceType' : ValidServiceType,
      'ResourceType' : ValidResourceType,
      'args' : None,  
      'Site_Panel' : [ {'WebLink': {'Command': 'DT_link', 
                                    'args': None}}
                      ], 
      'Resource_Panel' : [ {'WebLink': {'Command': 'DT_link', 
                                        'args': None}}
                      ]
     },
  'DT_Policy_Scheduled' : 
    { 'Granularity' : [], 
      'Status' : ValidStatus, 
      'FormerStatus' : ValidStatus,
      'SiteType' : ValidSiteType,
      'ServiceType' : ValidServiceType,
      'ResourceType' : ValidResourceType,
      'args' : (DTinHours, )
#      'Site_Panel' : {'WebLink':'DT_link'},
#      'Resource_Panel' : {'WebLink':'DT_link'}
     },
  'GGUS_Policy' : 
    { 'Granularity' : [], 
      'Status' : ValidStatus, 
      'FormerStatus' : ValidStatus,
      'SiteType' : ValidSiteType,
      'ServiceType' : ValidServiceType,
      'ResourceType' : ValidResourceType,
      'args' : None,  
      'Site_Panel' : [ {'WebLink': {'Command': 'GGUS_link', 
                                    'args': None}}
                      ]
     },
  'SAM_Policy' : 
    { 'Granularity' : [], 
      'Status' : ValidStatus, 
      'FormerStatus' : ValidStatus,
      'SiteType' : ValidSiteType,
      'ServiceType' : ValidServiceType,
      'ResourceType' : ['SE', 'LFC'],
      'args' : None,  
     },
  'SAM_CE_Policy' : 
    { 'Granularity' : ['Resource'], 
      'Status' : ValidStatus, 
      'FormerStatus' : ValidStatus,
      'SiteType' : ValidSiteType,
      'ServiceType' : ValidServiceType,
      'ResourceType' : ['CE'],
      'args' : ( None, ['LHCb CE-lhcb-availability', 'LHCb CE-lhcb-install', 'LHCb CE-lhcb-job-Boole', 
              'LHCb CE-lhcb-job-Brunel', 'LHCb CE-lhcb-job-DaVinci', 'LHCb CE-lhcb-job-Gauss', 'LHCb CE-lhcb-os', 
              'LHCb CE-lhcb-queues', 'bi', 'csh', 'js', 'gfal', 'swdir', 'voms'] ), 
      'Resource_Panel' : [ {'SAM': {'Command':'SAM_tests', 
                                    'args': ( None, ['LHCb CE-lhcb-availability', 'LHCb CE-lhcb-install', 
                                                     'LHCb CE-lhcb-job-Boole', 'LHCb CE-lhcb-job-Brunel', 
                                                     'LHCb CE-lhcb-job-DaVinci', 'LHCb CE-lhcb-job-Gauss', 
                                                     'LHCb CE-lhcb-os', 'LHCb CE-lhcb-queues', 
                                                     'bi', 'csh', 'js', 'gfal', 'swdir', 'voms'] ) }},
                           {'WebLink': {'Command':'SAM_link',
                                        'args': None}}
                         ]
     },     
  'SAM_CREAMCE_Policy' : 
    { 'Granularity' : ['Resource'], 
      'Status' : ValidStatus, 
      'FormerStatus' : ValidStatus,
      'SiteType' : ValidSiteType,
      'ServiceType' : ValidServiceType,
      'ResourceType' : ['CREAMCE'],
      'args' : ( None, ['bi', 'csh', 'gfal', 'swdir', 'creamvoms'] ), 
      'Resource_Panel' : [ {'SAM': {'Command':'SAM_tests', 
                                    'args': ( None, ['bi', 'csh', 'gfal', 'swdir', 'creamvoms'] ) }},
                           {'WebLink': {'Command':'SAM_link',
                                        'args': None}}
                         ]
     },     
  'SAM_SE_Policy' : 
    { 'Granularity' : ['Resource'], 
      'Status' : ValidStatus, 
      'FormerStatus' : ValidStatus,
      'SiteType' : ValidSiteType,
      'ServiceType' : ValidServiceType,
      'ResourceType' : ['SE'],
      'args' : ( None, ['DiracTestUSER', 'FileAccessV2'] ), 
      'Resource_Panel' : [ {'SAM': {'Command':'SAM_tests', 
                                    'args': ( None, ['DiracTestUSER', 'FileAccessV2'] ) }},
                           {'WebLink': {'Command':'SAM_link',
                                        'args': None}}
                         ]
     },     
  'SAM_LFC_C_Policy' : 
    { 'Granularity' : ['Resource'], 
      'Status' : ValidStatus, 
      'FormerStatus' : ValidStatus,
      'SiteType' : ValidSiteType,
      'ServiceType' : ValidServiceType,
      'ResourceType' : ['LFC_C'],
      'args' : ( None, ['lfcwf', 'lfclr', 'lfcls', 'lfcping'] ),
      'Resource_Panel' : [ {'SAM': {'Command':'SAM_tests', 
                                    'args': ( None, ['lfcwf', 'lfclr', 'lfcls', 'lfcping'] ) }},
                           {'WebLink': {'Command':'SAM_link',
                                        'args': None}}
                          ]
     },     
  'SAM_LFC_L_Policy' : 
    { 'Granularity' : ['Resource'], 
      'Status' : ValidStatus, 
      'FormerStatus' : ValidStatus,
      'SiteType' : ValidSiteType,
      'ServiceType' : ValidServiceType,
      'ResourceType' : ['LFC_L'],
      'args' : ( None, ['lfcstreams', 'lfclr', 'lfcls', 'lfcping'] ),
      'Resource_Panel' : [ {'SAM': {'Command':'SAM_tests', 
                                    'args': ( None, ['lfcstreams', 'lfclr', 'lfcls', 'lfcping'] ) }},
                           {'WebLink': {'Command':'SAM_link',
                                        'args': None}}
                          ]
     },     
  'JobsEfficiencySimple_Policy' :  
    { 'Granularity' : ['Service'], 
      'Status' : ValidStatus, 
      'FormerStatus' : ValidStatus,
      'SiteType' : ValidSiteType,
      'ServiceType' : ['Computing'],
      'ResourceType' : ValidResourceType,
      'args' : None,  
      'Service_Computing_Panel' : [ {'Graph': {'Command': 'DiracAccountingGraph', 
                                               'args': ('Job', 'CumulativeNumberOfJobs', 
                                                        {'Format': 'LastHours', 'hours': 24}, 
                                                        'FinalMajorStatus', None)}},
                                    {'Graph': {'Command': 'DiracAccountingGraph', 
                                               'args': ('Job', 'TotalNumberOfJobs', 
                                                        {'Format': 'LastHours', 'hours': 24}, 
                                                        'JobType', {'FinalMajorStatus':'Failed'})}}
                                    ]                                  
   },
  'PilotsEfficiencySimple_Policy_Service' : 
    { 'Granularity' : ['Service'], 
      'Status' : ValidStatus, 
      'FormerStatus' : ValidStatus,
      'SiteType' : ValidSiteType,
      'ServiceType' : ['Computing'],
      'ResourceType' : ValidResourceType,
      'args' : None,  
      'Service_Computing_Panel' : [ {'Graph': {'Command' : 'DiracAccountingGraph', 
                                               'args': ('Pilot', 'CumulativeNumberOfPilots', 
                                                         {'Format': 'LastHours', 'hours': 24}, 
                                                         'GridStatus', None)}},
                                    {'Graph': {'Command':  'DiracAccountingGraph',
                                               'args': ('Pilot', 'TotalNumberOfPilots', 
                                                        {'Format': 'LastHours', 'hours': 24}, 
                                                        'GridCE', None)}}
                                    ]
     },
  'PilotsEfficiencySimple_Policy_Resource' : 
    { 'Granularity' : ['Resource'], 
      'Status' : ValidStatus, 
      'FormerStatus' : ValidStatus,
      'SiteType' : ValidSiteType,
      'ServiceType' : ValidServiceType,
      'ResourceType' : ['CE', 'CREAMCE'],
      'args' : None,  
      'Resource_Panel' : [ {'Graph': {'Command': 'DiracAccountingGraph',
                                      'args': ('Pilot', 'CumulativeNumberOfPilots', 
                                               {'Format': 'LastHours', 'hours': 24}, 
                                               'GridStatus', None)}}
                          ]
     },
  'OnSitePropagation_Policy' :
    { 'Granularity' : ['Site'], 
      'Status' : ValidStatus, 
      'FormerStatus' : ValidStatus,
      'SiteType' : ValidSiteType,
      'ServiceType' : ValidServiceType,
      'ResourceType' : ValidResourceType,
      'args' : ('Service', ),
      'Site_Panel' : {'RSS':'ServiceOfSite'}
     },
  'OnComputingServicePropagation_Policy' :
    { 'Granularity' : ['Service'], 
      'Status' : ValidStatus, 
      'FormerStatus' : ValidStatus,
      'SiteType' : ValidSiteType,
      'ServiceType' : ['Computing'],
      'ResourceType' : ValidResourceType,
      'args' : ('Resource', ),
      'Service_Computing_Panel' : {'RSS':'ResOfCompService'}
     },
  'OnStorageServicePropagation_Policy_Resources' :
    { 'Granularity' : ['Service'], 
      'Status' : ValidStatus, 
      'FormerStatus' : ValidStatus,
      'SiteType' : ValidSiteType,
      'ServiceType' : ['Storage'],
      'ResourceType' : ValidResourceType,
      'args' : ('Resource', ),
      'Service_Storage_Panel' : {'RSS':'ResOfStorService'}
     },
  'OnStorageServicePropagation_Policy_StorageElements' :
    { 'Granularity' : ['Service'], 
      'Status' : ValidStatus, 
      'FormerStatus' : ValidStatus,
      'SiteType' : ValidSiteType,
      'ServiceType' : ['Storage'],
      'ResourceType' : ValidResourceType,
      'args' : ('StorageElement', ),
      'Service_Storage_Panel' : {'RSS':'StorageElementsOfSite'}
     },
  'OnServicePropagation_Policy' :
    { 'Granularity' : [], 
      'Status' : ValidStatus, 
      'FormerStatus' : ValidStatus,
      'SiteType' : ValidSiteType,
      'ServiceType' : ValidServiceType,
      'ResourceType' : ValidResourceType,
      'args' : None,  
     },
  'OnSENodePropagation_Policy' :
    { 'Granularity' : [], 
      'Status' : ValidStatus, 
      'FormerStatus' : ValidStatus,
      'SiteType' : ValidSiteType,
      'ServiceType' : ValidServiceType,
      'args' : None,  
      'ResourceType' : ['SE'],
     },
  'TransferQuality_Policy' :
    { 'Granularity' : ['StorageElement'], 
      'Status' : ValidStatus, 
      'FormerStatus' : ValidStatus,
      'SiteType' : ValidSiteType,
      'ServiceType' : ValidServiceType,
      'ResourceType' : ValidResourceType,
      'args' : None,  
      'SE_Panel' : [ {'Graph': {'Command':'DiracAccountingGraph', 
                                'args': ('DataOperation', 'Quality', 
                                         {'Format': 'LastHours', 'hours': 24}, 
                                         'Channel', {'OperationType':'putAndRegister'})}}
                      ]
     },
  'AlwaysFalse_Policy' :
    { 'Granularity' : [], 
      'Status' : ValidStatus, 
      'FormerStatus' : ValidStatus,
      'SiteType' : ValidSiteType,
      'ServiceType' : ValidServiceType,
      'args' : None,  
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
#  'View_PolType' : 
#    { 'Granularity' : ValidRes, 
#      'Status' : ValidStatus, 
#      'FormerStatus' : ValidStatus,
#      'SiteType' : ValidSiteType,
#      'ServiceType' : ValidServiceType,
#      'ResourceType' : ValidResourceType,
#     },
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
# Web views 
#############################################################################

views_panels = {
  'Site_View' : ['Site_Panel', 'Service_Computing_Panel', 'Service_Storage_Panel', 'OtherServices_Panel'],
  'Resource_View' : ['Resource_Panel'],
  'SE_View' : ['SE_Panel']
}


