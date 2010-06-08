""" DIRAC.ResourceStatusSystem.Policy.Configurations Module

    collects everything needed to configure policies
"""

from DIRAC.ResourceStatusSystem.Utilities.Utils import *

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

# --- SE transfer quality --- #
Transfer_QUALITY_LOW = 60
Transfer_QUALITY_HIGH = 90


#############################################################################
# site/services/resource checking frequency
#############################################################################

Sites_check_freq = {  'T0_ACTIVE_CHECK_FREQUENCY': 6, \
                      'T0_PROBING_CHECK_FREQUENCY': 5, \
                      'T0_BAD_CHECK_FREQUENCY' : 5, \
                      'T0_BANNED_CHECK_FREQUENCY' : 5, \
                      'T1_ACTIVE_CHECK_FREQUENCY' : 8, \
                      'T1_PROBING_CHECK_FREQUENCY' : 7, \
                      'T1_BAD_CHECK_FREQUENCY' : 7, \
                      'T1_BANNED_CHECK_FREQUENCY' : 8, \
                      'T2_ACTIVE_CHECK_FREQUENCY' : 30, \
                      'T2_PROBING_CHECK_FREQUENCY' : 25, \
                      'T2_BAD_CHECK_FREQUENCY' : 20 , \
                      'T2_BANNED_CHECK_FREQUENCY' : 30 }

Services_check_freq = {'T0_ACTIVE_CHECK_FREQUENCY': 10, \
                       'T0_PROBING_CHECK_FREQUENCY': 7, \
                       'T0_BAD_CHECK_FREQUENCY' : 7, \
                       'T0_BANNED_CHECK_FREQUENCY' : 8, \
                       'T1_ACTIVE_CHECK_FREQUENCY' : 12, \
                       'T1_PROBING_CHECK_FREQUENCY' : 10, \
                       'T1_BAD_CHECK_FREQUENCY' : 10, \
                       'T1_BANNED_CHECK_FREQUENCY' : 12, \
                       'T2_ACTIVE_CHECK_FREQUENCY' : 30, \
                       'T2_PROBING_CHECK_FREQUENCY' : 20, \
                       'T2_BAD_CHECK_FREQUENCY' : 20, \
                       'T2_BANNED_CHECK_FREQUENCY' : 30 }

Resources_check_freq = {'T0_ACTIVE_CHECK_FREQUENCY': 10, \
                        'T0_PROBING_CHECK_FREQUENCY': 8, \
                        'T0_BAD_CHECK_FREQUENCY' : 8, \
                        'T0_BANNED_CHECK_FREQUENCY' : 10, \
                        'T1_ACTIVE_CHECK_FREQUENCY' : 12, \
                        'T1_PROBING_CHECK_FREQUENCY' : 10, \
                        'T1_BAD_CHECK_FREQUENCY' : 10, \
                        'T1_BANNED_CHECK_FREQUENCY' : 12, \
                        'T2_ACTIVE_CHECK_FREQUENCY' : 30, \
                        'T2_PROBING_CHECK_FREQUENCY' : 20, \
                        'T2_BAD_CHECK_FREQUENCY' : 20, \
                        'T2_BANNED_CHECK_FREQUENCY' : 30 }

StorageElements_check_freq = {'T0_ACTIVE_CHECK_FREQUENCY': 12, \
                              'T0_PROBING_CHECK_FREQUENCY': 10, \
                              'T0_BAD_CHECK_FREQUENCY' : 10, \
                              'T0_BANNED_CHECK_FREQUENCY' : 12, \
                              'T1_ACTIVE_CHECK_FREQUENCY' : 15, \
                              'T1_PROBING_CHECK_FREQUENCY' : 12, \
                              'T1_BAD_CHECK_FREQUENCY' : 12, \
                              'T1_BANNED_CHECK_FREQUENCY' : 15, \
                              'T2_ACTIVE_CHECK_FREQUENCY' : 40, \
                              'T2_PROBING_CHECK_FREQUENCY' : 30, \
                              'T2_BAD_CHECK_FREQUENCY' : 30, \
                              'T2_BANNED_CHECK_FREQUENCY' : 40 }

#############################################################################
# alarms and notifications
#############################################################################

notified_users = ['fstagni', 'roma']

#from DIRAC.FrameworkSystem.Client.NotificationClient import NotificationClient
#nc = NotificationClient()
#notified_users = nc.getAssigneeGroups()['Value']['RSS_alarms']

AssigneeGroups = {
  'VladRobGreigJoel_PROD-Mail': 
  {'Users': ['roma', 'santinel', 'gcowan', 'joel'],
   'Setup': ['LHCb-Production'],
   'Granularity': ['Site', 'Service'],
   'SiteType': ValidSiteType, 
   'ServiceType': ValidServiceType, 
   'ResourceType': ValidResourceType, 
   'Notifications': ['Mail']
   }, 
  'VladRobGreigJoel_PROD-Web': 
  {'Users': ['roma', 'santinel', 'gcowan', 'joel'],
   'Setup': ['LHCb-Production'],
   'Granularity': ['Site', 'Service'],
   'SiteType': ValidSiteType, 
   'ServiceType': ValidServiceType, 
   'ResourceType': ValidResourceType, 
   'Notifications': ['Web']
   }, 
  'VladRobGreigJoel_PROD-Mail-2': 
  {'Users': ['roma', 'santinel', 'gcowan', 'joel'],
   'Setup': ['LHCb-Production'],
   'Granularity': ['Resource'],
   'SiteType': ValidSiteType, 
   'ServiceType': ValidServiceType, 
   'ResourceType': ['SE', 'LFC_C', 'LFC_L', 'FTS'], 
   'Notifications': ['Mail']
   }, 
  'VladRobGreigJoel_PROD-Web-2': 
  {'Users': ['roma', 'santinel', 'gcowan', 'joel'],
   'Setup': ['LHCb-Production'],
   'Granularity': ['Resource'],
   'SiteType': ValidSiteType, 
   'ServiceType': ValidServiceType, 
   'ResourceType': ['SE', 'LFC_C', 'LFC_L', 'FTS'], 
   'Notifications': ['Web']
   }, 
  'VladRob_DEV': 
  {'Users': ['roma', 'santinel'],
   'Setup': ['LHCb-Development', 'LHCb-Certification'], 
   'Granularity': ValidRes,
   'SiteType': [], 
   'ServiceType': ValidServiceType, 
   'ResourceType': ValidResourceType, 
   'Notifications': ['Web']
   }, 
  'me_PROD-Mail': 
  {'Users': ['fstagni'],
   'Setup': ['LHCb-Production'],
   'Granularity': ValidRes,
   'SiteType': ValidSiteType,
   'ServiceType': ValidServiceType, 
   'ResourceType': ValidResourceType, 
   'Notifications': ['Mail']
   }, 
  'me_PROD-Web': 
  {'Users': ['fstagni'],
   'Setup': ['LHCb-Production'],
   'Granularity': ValidRes,
   'SiteType': ValidSiteType, 
   'ServiceType': ValidServiceType, 
   'ResourceType': ValidResourceType, 
   'Notifications': ['Web']
   }, 
  'me_DEV': 
  {'Users': ['fstagni'],
   'Setup': ['LHCb-Development', 'LHCb-Certification'], 
   'Granularity': ValidRes,
   'SiteType': ValidSiteType, 
   'ServiceType': ValidServiceType, 
   'ResourceType': ValidResourceType, 
   'Notifications': ['Mail']
   }, 
  'Andrew_PROD_SE': 
  {'Users': ['acsmith'],
   'Setup': ['LHCb-Production'],
   'Granularity': ['StorageElement'],
   'SiteType': ValidSiteType, 
   'ServiceType': ValidServiceType, 
   'ResourceType': ValidResourceType, 
   'Notifications': ['Web', 'Mail']
   }, 
  'Andrew_PROD_Res': 
  {'Users': ['acsmith'],
   'Setup': ['LHCb-Production'],
   'Granularity': ['Resource'],
   'SiteType': ValidSiteType, 
   'ServiceType': ValidServiceType, 
   'ResourceType': ['SE', 'LFC_C', 'LFC_L', 'FTS'], 
   'Notifications': ['Web', 'Mail']
   }, 
  'Andrew_DEV': 
  {'Users': ['acsmith'],
   'Setup': ['LHCb-Development', 'LHCb-Certification'], 
   'Granularity': ['StorageElement'],
   'SiteType': ValidSiteType, 
   'ServiceType': ValidServiceType, 
   'ResourceType': ValidResourceType, 
   'Notifications': ['Web']
   }, 
}

#############################################################################
# policies evaluated
#############################################################################

Policies = { 
  'DT_OnGoing_Only' : 
    { 'Description' : "Evaluates on possible ongoing down-times", 
      'Granularity' : [], 
      'Status' : ValidStatus, 
      'FormerStatus' : ValidStatus,
      'SiteType' : ValidSiteType,
      'ServiceType' : ValidServiceType,
      'ResourceType' : ValidResourceType,
      'module' : 'DT_Policy',
      'commandIn' : ('GOCDBStatus_Command', 'GOCDBStatus_Command'),
      'args' : None,  
#      'Site_Panel' : [ {'WebLink': {'Command': 'DT_Link', 
#                                    'args': None}}
#                      ], 
#      'Resource_Panel' : [ {'WebLink': {'Command': 'DT_Link', 
#                                        'args': None}}
#                      ]
     },
  'DT_Scheduled' : 
    { 'Description' : "Evaluates on possible ongoing and scheduled down-times", 
      'Granularity' : ['Site', 'Resource'], 
      'Status' : ValidStatus, 
      'FormerStatus' : ValidStatus,
      'SiteType' : ValidSiteType,
      'ServiceType' : ValidServiceType,
      'ResourceType' : ValidResourceType,
      'module' : 'DT_Policy',
      'commandIn' : ('GOCDBStatus_Command', 'GOCDBStatus_Command'),
      'args' : (DTinHours, ),
      'Site_Panel' : [ {'WebLink': {'Command': ('GOCDBStatus_Command', 'GOCDBInfo_Command'), 
                                    'args': None}}, 
                      ], 
      'Resource_Panel' : [ {'WebLink': {'Command': ('GOCDBStatus_Command', 'GOCDBInfo_Command'), 
                                        'args': None}}, 
                      ]
     },
  'GGUSTickets' : 
    { 'Description' : "Evaluates the number of open GGUS tickets", 
      'Granularity' : ['Site'], 
      'Status' : ValidStatus, 
      'FormerStatus' : ValidStatus,
      'SiteType' : ValidSiteType,
      'ServiceType' : ValidServiceType,
      'ResourceType' : ValidResourceType,
      'module' : 'GGUSTickets_Policy',
      'commandIn' : ('GGUSTickets_Command', 'GGUSTickets_Open'),
      'args' : None,  
      'Site_Panel' : [ {'WebLink': {'Command': ('GGUSTickets_Command','GGUSTickets_Link'), 
                                    'args': None}}, 
                       {'TextInfo': {'Command': ('GGUSTickets_Command', 'GGUSTickets_Info'), 
                                    'args': None}},
                     ]
     },
#  'SAM' : 
#    { 'Description' : "Evaluates latest SAM results", 
#      'Granularity' : [], 
#      'Status' : ValidStatus, 
#      'FormerStatus' : ValidStatus,
#      'SiteType' : ValidSiteType,
#      'ServiceType' : ValidServiceType,
#      'ResourceType' : [],
#      'module': 'SAMResults_Policy', 
#      'commandIn' : 'SAM_Tests',
#      'args' : None,  
#     },
  'SAM_CE' : 
    { 'Description' : "Evaluates latest SAM results on the LCG Computing Element", 
      'Granularity' : ['Resource'], 
      'Status' : ValidStatus, 
      'FormerStatus' : ValidStatus,
      'SiteType' : ValidSiteType,
      'ServiceType' : ValidServiceType,
      'ResourceType' : ['CE'],
      'module': 'SAMResults_Policy', 
      'commandIn' : ('SAMResults_Command', 'SAMResults_Command'),
      'args' : ( None, ['LHCb CE-lhcb-availability', 'LHCb CE-lhcb-install', 'LHCb CE-lhcb-job-Boole', 
              'LHCb CE-lhcb-job-Brunel', 'LHCb CE-lhcb-job-DaVinci', 'LHCb CE-lhcb-job-Gauss', 'LHCb CE-lhcb-os', 
              'LHCb CE-lhcb-queues', 'bi', 'csh', 'js', 'gfal', 'swdir', 'voms'] ), 
      'Resource_Panel' : [ {'SAM': {'Command':('SAMResults_Command', 'SAMResults_Command'), 
                                    'args': ( None, ['LHCb CE-lhcb-availability', 'LHCb CE-lhcb-install', 
                                                     'LHCb CE-lhcb-job-Boole', 'LHCb CE-lhcb-job-Brunel', 
                                                     'LHCb CE-lhcb-job-DaVinci', 'LHCb CE-lhcb-job-Gauss', 
                                                     'LHCb CE-lhcb-os', 'LHCb CE-lhcb-queues', 
                                                     'LHCb CE-lhcb-queues', 'bi', 'csh', 'js', 'gfal', 
                                                     'swdir', 'voms'] ) }},
#                           {'WebLink': {'Command':'SAM_Link',
#                                        'args': None}}
                         ]
     },     
  'SAM_CREAMCE' : 
    { 'Description' : "Evaluates latest SAM results on the CREAM Computing Element", 
      'Granularity' : ['Resource'], 
      'Status' : ValidStatus, 
      'FormerStatus' : ValidStatus,
      'SiteType' : ValidSiteType,
      'ServiceType' : ValidServiceType,
      'ResourceType' : ['CREAMCE'],
      'module': 'SAMResults_Policy', 
      'commandIn' : ('SAMResults_Command', 'SAMResults_Command'),
      'args' : ( None, ['bi', 'csh', 'gfal', 'swdir', 'creamvoms'] ), 
      'Resource_Panel' : [ {'SAM': {'Command':('SAMResults_Command', 'SAMResults_Command'),
                                    'args': ( None, ['bi', 'csh', 'gfal', 'swdir', 'creamvoms'] ) }},
#                           {'WebLink': {'Command':'SAM_Link',
#                                        'args': None}}
                         ]
     },     
  'SAM_SE' : 
    { 'Description' : "Evaluates latest SAM results on the SRM nodes", 
      'Granularity' : ['Resource'], 
      'Status' : ValidStatus, 
      'FormerStatus' : ValidStatus,
      'SiteType' : ValidSiteType,
      'ServiceType' : ValidServiceType,
      'ResourceType' : ['SE'],
      'module': 'SAMResults_Policy', 
      'commandIn' : ('SAMResults_Command', 'SAMResults_Command'),
      'args' : ( None, ['DiracTestUSER', 'FileAccessV2'] ), 
      'Resource_Panel' : [ {'SAM': {'Command':('SAMResults_Command', 'SAMResults_Command'),
                                    'args': ( None, ['DiracTestUSER', 'FileAccessV2'] ) }},
#                           {'WebLink': {'Command':'SAM_Link',
#                                        'args': None}}
                         ]
     },     
  'SAM_LFC_C' : 
    { 'Description' : "Evaluates latest SAM results on the central LFC nodes", 
      'Granularity' : ['Resource'], 
      'Status' : ValidStatus, 
      'FormerStatus' : ValidStatus,
      'SiteType' : ValidSiteType,
      'ServiceType' : ValidServiceType,
      'ResourceType' : ['LFC_C'],
      'module': 'SAMResults_Policy', 
      'commandIn' : ('SAMResults_Command', 'SAMResults_Command'),
      'args' : ( None, ['lfcwf', 'lfclr', 'lfcls', 'lfcping'] ),
      'Resource_Panel' : [ {'SAM': {'Command':('SAMResults_Command', 'SAMResults_Command'),
                                    'args': ( None, ['lfcwf', 'lfclr', 'lfcls', 'lfcping'] ) }},
#                           {'WebLink': {'Command':'SAM_Link',
#                                        'args': None}}
                          ]
     },     
  'SAM_LFC_L' : 
    { 'Description' : "Evaluates latest SAM results on the slave LFC nodes", 
      'Granularity' : ['Resource'], 
      'Status' : ValidStatus, 
      'FormerStatus' : ValidStatus,
      'SiteType' : ValidSiteType,
      'ServiceType' : ValidServiceType,
      'ResourceType' : ['LFC_L'],
      'module': 'SAMResults_Policy', 
      'commandIn' : ('SAMResults_Command', 'SAMResults_Command'),
      'args' : ( None, ['lfcstreams', 'lfclr', 'lfcls', 'lfcping'] ),
      'Resource_Panel' : [ {'SAM': {'Command':('SAMResults_Command', 'SAMResults_Command'),
                                    'args': ( None, ['lfcstreams', 'lfclr', 'lfcls', 'lfcping'] ) }},
#                           {'WebLink': {'Command':'SAM_Link',
#                                        'args': None}}
                          ]
     },     
  'JobsEfficiencySimple' :  
    { 'Description' : "Evaluates a simple jobs efficiency", 
      'Granularity' : ['Service'], 
      'Status' : ValidStatus, 
      'FormerStatus' : ValidStatus,
      'SiteType' : ValidSiteType,
      'ServiceType' : ['Computing'],
      'ResourceType' : ValidResourceType,
      'module': 'JobsEfficiency_Simple_Policy', 
      'commandInNewRes' : ('Jobs_Command', 'JobsEffSimple_Command'),
      'commandIn' : ('Jobs_Command', 'JobsEffSimpleCached_Command'),
      'args' : None,  
      'Service_Computing_Panel' : [ {'FillChart': {'Command': ('DIRACAccounting_Command', 'DIRACAccounting_Command'), 
                                                   'args': ('Job', 'CumulativeNumberOfJobs', 
                                                            {'Format': 'LastHours', 'hours': 24}, 
                                                            'FinalMajorStatus', None)}},
                                    {'PieChart': {'Command': ('DIRACAccounting_Command', 'DIRACAccounting_Command'), 
                                                  'args': ('Job', 'TotalNumberOfJobs', 
                                                           {'Format': 'LastHours', 'hours': 24}, 
                                                           'JobType', {'FinalMajorStatus':'Failed'})}}
                                    ]                                  
   },
  'PilotsEfficiencySimple_Service' : 
    { 'Description' : "Evaluates a simple pilots efficiency", 
      'Granularity' : ['Service'], 
      'Status' : ValidStatus, 
      'FormerStatus' : ValidStatus,
      'SiteType' : ValidSiteType,
      'ServiceType' : ['Computing'],
      'ResourceType' : ValidResourceType,
      'module': 'PilotsEfficiency_Simple_Policy', 
      'commandInNewRes' : ('Pilots_Command', 'PilotsEffSimple_Command'),
      'commandIn' : ('Pilots_Command', 'PilotsEffSimpleCached_Command'),
      'args' : None,  
      'Service_Computing_Panel' : [ {'FillChart': {'Command' : ('DIRACAccounting_Command', 'DIRACAccounting_Command'), 
                                                   'args': ('Pilot', 'CumulativeNumberOfPilots', 
                                                            {'Format': 'LastHours', 'hours': 24}, 
                                                            'GridStatus', None)}},
                                    {'PieChart': {'Command':  ('DIRACAccounting_Command', 'DIRACAccounting_Command'),
                                                  'args': ('Pilot', 'TotalNumberOfPilots', 
                                                           {'Format': 'LastHours', 'hours': 24}, 
                                                           'GridCE', None)}}
                                    ]
     },
  'PilotsEfficiencySimple_Resource' : 
    { 'Description' : "Evaluates a simple pilots efficiency", 
      'Granularity' : ['Resource'], 
      'Status' : ValidStatus, 
      'FormerStatus' : ValidStatus,
      'SiteType' : ValidSiteType,
      'ServiceType' : ValidServiceType,
      'ResourceType' : ['CE', 'CREAMCE'],
      'module': 'PilotsEfficiency_Simple_Policy', 
      'commandIn' : ('Pilots_Command', 'PilotsEffSimple_Command'),
      'args' : None,  
      'Resource_Panel' : [ {'FillChart': {'Command': ('DIRACAccounting_Command', 'DIRACAccounting_Command'),
                                          'args': ('Pilot', 'CumulativeNumberOfPilots', 
                                                   {'Format': 'LastHours', 'hours': 24}, 
                                                   'GridStatus', None)}}, 
                          ]
     },
  'OnSitePropagation' :
    { 'Description' : "Evaluates how the site's services are behaving in the RSS", 
      'Granularity' : ['Site'], 
      'Status' : ValidStatus, 
      'FormerStatus' : ValidStatus,
      'SiteType' : ValidSiteType,
      'ServiceType' : ValidServiceType,
      'ResourceType' : ValidResourceType,
      'module' : 'Propagation_Policy',
      'commandIn' : ('RS_Command', 'ServiceStats_Command'),
      'args' : ('Service', ),
      'Site_Panel' : {'RSS':'ServiceOfSite'}
     },
  'OnComputingServicePropagation' :
    { 'Description' : "Evaluates how the service's computing resources are behaving in the RSS", 
      'Granularity' : ['Service'], 
      'Status' : ValidStatus, 
      'FormerStatus' : ValidStatus,
      'SiteType' : ValidSiteType,
      'ServiceType' : ['Computing'],
      'ResourceType' : ValidResourceType,
      'module' : 'Propagation_Policy',
      'commandIn' : ('RS_Command', 'ResourceStats_Command'),
      'args' : ('Resource', ),
      'Service_Computing_Panel' : {'RSS':'ResOfCompService'}
     },
  'OnStorageServicePropagation_Res' :
    { 'Description' : "Evaluates how the service's storage nodes are behaving in the RSS", 
      'Granularity' : ['Service'], 
      'Status' : ValidStatus, 
      'FormerStatus' : ValidStatus,
      'SiteType' : ValidSiteType,
      'ServiceType' : ['Storage'],
      'ResourceType' : ValidResourceType,
      'module' : 'Propagation_Policy',
      'commandIn' : ('RS_Command', 'ResourceStats_Command'),
      'args' : ('Resource', ),
      'Service_Storage_Panel' : {'RSS':'ResOfStorService'}
     },
  'OnStorageServicePropagation_SE' :
    { 'Description' : "Evaluates how the service's storage elements are behaving in the RSS", 
      'Granularity' : ['Service'], 
      'Status' : ValidStatus, 
      'FormerStatus' : ValidStatus,
      'SiteType' : ValidSiteType,
      'ServiceType' : ['Storage'],
      'ResourceType' : ValidResourceType,
      'module' : 'Propagation_Policy',
      'commandIn' : ('RS_Command', 'StorageElementsStats_Command'),
      'args' : ('StorageElement', ),
      'Service_Storage_Panel' : {'RSS':'StorageElementsOfSite'}
     },
#  'OnServicePropagation' :
#    { 'Granularity' : [], 
#      'Status' : ValidStatus, 
#      'FormerStatus' : ValidStatus,
#      'SiteType' : ValidSiteType,
#      'ServiceType' : ValidServiceType,
#      'ResourceType' : ValidResourceType,
#      'module' : 'OnServicePropagation_Policy',
#      'commandIn' : None,
#      'args' : None,  
#     },
  'OnStorageElementPropagation' :
    { 'Description' : "Evaluates how the storage elements' nodes are behaving in the RSS", 
      'Granularity' : ['StorageElement'], 
      'Status' : ValidStatus, 
      'FormerStatus' : ValidStatus,
      'SiteType' : ValidSiteType,
      'ServiceType' : ValidServiceType,
      'ResourceType' : ValidResourceType,
      'module': 'OnStorageElementPropagation_Policy',
      'commandIn' : ('RS_Command', 'MonitoredStatus_Command'),
      'args' : ('Resource', ),
      'SE_Panel' : {'RSS':'ResOfStorEl'}
     },
#  'OnSENodePropagation' :
#    { 'Granularity' : [], 
#      'Status' : ValidStatus, 
#      'FormerStatus' : ValidStatus,
#      'SiteType' : ValidSiteType,
#      'ServiceType' : ValidServiceType,
#      'module': 'OnSENodePropagation_Policy', 
#      'commandIn' : None,
#      'args' : None,  
#      'ResourceType' : ['SE'],
#     },
  'TransferQuality' :
    { 'Description' : "Evaluates the SE transfer quality", 
      'Granularity' : ['StorageElement'], 
      'Status' : ValidStatus, 
      'FormerStatus' : ValidStatus,
      'SiteType' : ValidSiteType,
      'ServiceType' : ValidServiceType,
      'ResourceType' : ValidResourceType,
      'module': 'TransferQuality_Policy', 
      'commandInNewRes' : ('DIRACAccounting_Command', 'TransferQuality_Command'),
      'commandIn' : ('DIRACAccounting_Command', 'TransferQualityCached_Command'),
      'args' : None,  
      'SE_Panel' : [ {'FillChart': {'Command':('DIRACAccounting_Command', 'DIRACAccounting_Command'),
#                                    'CommandNew':'DiracAccountingGraph-NEW', 
                                    'args': ('DataOperation', 'Quality', 
                                             {'Format': 'LastHours', 'hours': 24}, 
                                             'Channel', {'OperationType':'putAndRegister'})}}, 
                      ]
     },
  'SEOccupancy' :
    { 'Description' : "Evaluates the SE occupancy", 
      'Granularity' : ['StorageElement'], 
      'Status' : ValidStatus, 
      'FormerStatus' : ValidStatus,
      'SiteType' : ValidSiteType,
      'ServiceType' : ValidServiceType,
      'ResourceType' : ValidResourceType,
      'module': 'SEOccupancy_Policy', 
      'commandIn' : ('SLS_Command', 'SLSStatus_Command'),
      'args' : None,  
      'SE_Panel' : [ {'WebLink': {'Command':('SLS_Command', 'SLSLink_Command'),
                                  'args': None}}, 
                      ]
     },
  'SEQueuedTransfers' :
    { 'Description' : "Evaluates the queued transfers on the SE", 
      'Granularity' : ['StorageElement'], 
      'Status' : ValidStatus, 
      'FormerStatus' : ValidStatus,
      'SiteType' : ['T0'],
      'ServiceType' : ValidServiceType,
      'ResourceType' : ValidResourceType,
      'module': 'SEQueuedTransfers_Policy',
      'commandIn' : ('SLS_Command', 'SLSServiceInfo_Command'),
      'args' : (["Queued transfers"], ),
      'SE_Panel' : [ {'WebLink': {'Command':('SLS_Command', 'SLSLink_Command'),
                                  'args': None}}, 
                      ]
     },
  'AlwaysFalse' :
    { 'Granularity' : [], 
      'Status' : ValidStatus, 
      'FormerStatus' : ValidStatus,
      'SiteType' : ValidSiteType,
      'ServiceType' : ValidServiceType,
      'commandIn' : None,
      'args' : None,  
      'ResourceType' : ValidResourceType,
     }
}


Policy_Types = {
  'Resource_PolType' : 
    { 'Granularity' : ['Site', 'Service', 'Resource', 'StorageElement'], 
      'Status' : ValidStatus, 
      'FormerStatus' : ValidStatus,
      'NewStatus' : ValidStatus,
      'SiteType' : ValidSiteType,
      'ServiceType' : ValidServiceType,
      'ResourceType' : ValidResourceType,
     },
  'Alarm_PolType' : 
    { 'Granularity' : ['Site', 'Service', 'Resource'], 
      'Status' : ['Banned'],
      'FormerStatus' : ValidStatus,
      'NewStatus' : ['Banned'],
      'SiteType' : ValidSiteType,
      'ServiceType' : ValidServiceType,
      'ResourceType' : ValidResourceType,
     },
  'Alarm_PolType_SE' : 
    { 'Granularity' : ['StorageElement'], 
      'Status' : ['Bad', 'Banned'],
      'FormerStatus' : ValidStatus,
      'NewStatus' : ['Bad', 'Banned'],
      'SiteType' : ValidSiteType,
      'ServiceType' : ValidServiceType,
      'ResourceType' : ValidResourceType,
     },
  'RealBan_PolType' : 
    { 'Granularity' : [], 
      'Status' : ValidStatus, 
      'FormerStatus' : ValidStatus,
      'NewStatus' : ['Active', 'Banned'],
      'SiteType' : ValidSiteType,
      'ServiceType' : ValidServiceType,
      'ResourceType' : ValidResourceType,
     },
  'Collective_PolType' :
    { 'Granularity' : [], 
      'Status' : ValidStatus, 
      'FormerStatus' : ValidStatus,
      'NewStatus' : ValidStatus,
      'SiteType' : ValidSiteType,
      'ServiceType' : ValidServiceType,
      'ResourceType' : ValidResourceType,
     }
}

#############################################################################
# Web views 
#############################################################################

views_panels = {
  'Site' : ['Site_Panel', 'Service_Computing_Panel', 'Service_Storage_Panel', 'OtherServices_Panel'],
  'Resource' : ['Resource_Panel'],
  'StorageElement' : ['SE_Panel']
}


#############################################################################
# Clients cache 
#############################################################################

Commands_to_use = [('Collective_Command', 'JobsEffSimpleEveryOne_Command'), 
                   ('Collective_Command', 'PilotsEffSimpleEverySites_Command'),
                   ('Collective_Command', 'TransferQualityEverySEs_Command')
                   ]
