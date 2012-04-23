################################################################################
# $HeadURL $
################################################################################
"""
  DIRAC.ResourceStatusSystem package
"""

__RCSID__  = "$Id$"

from DIRAC.ResourceStatusSystem.Utilities import CS

try:
  gencfg = CS.getTypedDictRootedAtOperations( "GeneralConfig" )
except CS.CSError:
  gencfg = {}

ValidStatus       = gencfg.get("Status", ["Banned", "Probing", "Bad", "Active"])
ValidStatusTypes  = gencfg.get("Resources", {"Site": {"StatusType": "''"},
                                             "Service": {"StatusType": "''"},
                                             "Resource": {"StatusType": "''"},
                                             "StorageElement": {"StatusType": ["Read", "Write", "Remove", "Check"]}})
ValidRes = ValidStatusTypes.keys()

ValidPolicyResult = gencfg.get('PolicyResult', ["Error", "Unknown", "Banned", "Probing", "Bad", "Active"])
ValidSiteType     = gencfg.get('SiteType', ["T0", "T1", "T2", "T3"])
ValidServiceType  = gencfg.get('ServiceType', ["Computing", "Storage", "VO-BOX", "VOMS", "CondDB"])
ValidResourceType = gencfg.get('ResourceType', ["CE", "CREAMCE", "SE", "LFC_C", "LFC_L", "FTS", "VOMS"])
PolicyTypes       = gencfg.get('PolicyTypes', ["Resource_PolType", "Alarm_PolType", "Collective_PolType", "RealBan_PolType"])

################################################################################
# Web views
################################################################################

views_panels = {
  'Site' : ['Site_Panel', 'Service_Computing_Panel', 'Service_Storage_Panel',
            'Service_VOMS_Panel', 'Service_VO-BOX_Panel'],
  'Resource' : ['Resource_Panel'],
  'StorageElement' : ['SE_Panel']
}

################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF
