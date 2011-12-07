################################################################################
# $HeadURL $
################################################################################
__RCSID__  = "$Id$"

"""
  DIRAC.ResourceStatusSystem package
"""

from DIRAC.ResourceStatusSystem.Utilities import CS

try:
  gencfg            = CS.getTypedDictRootedAt( "GeneralConfig" )
except CS.CSError:
  print "Unable to connect to CS. Do you have a proxy ?"
  exit(1)

ValidRes          = gencfg[ 'Granularity' ]
ValidStatus       = gencfg[ 'Status' ]
ValidStatusTypes  = gencfg[ 'Resources' ]
ValidPolicyResult = gencfg[ 'PolicyResult' ] + gencfg[ 'Status' ]
ValidSiteType     = gencfg[ 'SiteType' ]
ValidServiceType  = gencfg[ 'ServiceType' ]
ValidResourceType = gencfg[ 'ResourceType' ]
PolicyTypes       = gencfg[ 'PolicyTypes' ]

CheckingFreqs     = CS.getTypedDictRootedAt("CheckingFreqs")

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
