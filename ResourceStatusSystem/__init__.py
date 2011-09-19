"""
DIRAC.ResourceStatusSystem package

"""

from DIRAC.ResourceStatusSystem.Utilities import CS

__gencfg            = CS.getTypedDictRootedAt( "GeneralConfig" )

ValidRes          = __gencfg[ 'Resources' ].keys()
ValidStatus       = __gencfg[ 'Status' ]
ValidPolicyResult = __gencfg[ 'PolicyResult' ] + __gencfg[ 'Status' ]
ValidSiteType     = __gencfg[ 'SiteType' ]
ValidServiceType  = __gencfg[ 'ServiceType' ]
ValidResourceType = __gencfg[ 'ResourceType' ]
ValidService      = ValidServiceType
PolicyTypes       = __gencfg[ 'PolicyTypes' ]

CheckingFreqs     = CS.getTypedDictRootedAt("CheckingFreqs")

#############################################################################
# Web views
#############################################################################

views_panels = {
  'Site' : ['Site_Panel', 'Service_Computing_Panel', 'Service_Storage_Panel',
            'Service_VOMS_Panel', 'Service_VO-BOX_Panel'],
  'Resource' : ['Resource_Panel'],
  'StorageElement' : ['SE_Panel']
}
