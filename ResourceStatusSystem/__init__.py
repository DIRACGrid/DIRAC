"""
DIRAC.ResourceStatusSystem package

"""

from DIRAC.ResourceStatusSystem.Utilities import CS

__gencfg            = CS.getTypedDictRootedAt( "GeneralConfig" )

ValidRes          = __gencfg[ 'Resource' ]
ValidStatus       = __gencfg[ 'Status' ]
ValidPolicyResult = __gencfg[ 'PolicyResult' ] + __gencfg[ 'Status' ]
ValidSiteType     = __gencfg[ 'SiteType' ]
ValidServiceType  = __gencfg[ 'ServiceType' ]
ValidResourceType = __gencfg[ 'ResourceType' ]
ValidService      = ValidServiceType
PolicyTypes       = __gencfg[ 'PolicyTypes' ]

CheckingFreqs     = CS.getTypedDictRootedAt("CheckingFreqs")
