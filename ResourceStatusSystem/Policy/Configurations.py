# $HeadURL $
''' Configurations

  Collects everything needed to configure policies.
  
'''

from DIRAC.ResourceStatusSystem.Utilities import Utils

__RCSID__ = '$Id: $'

#pp = CS.getTypedDictRootedAt( 'PolicyParameters' )

def getPolicyParameters():
  return Utils.getCSTree( 'RSSConfiguration2/PolicyParameters' )

Policies = {
            
  'DT_OnGoing_Only' :
    {
      'description' : "Ongoing down-times",
      'module'      : 'DT_Policy',
      'command'     : ( 'GOCDBStatus_Command', 'GOCDBStatus_Command' ),
      'args'        : None
    },

  'DT_Scheduled' :
    {
      'description'     : "Ongoing and scheduled down-times",
      'module'          : 'DT_Policy',
      'commandInNewRes' : ( 'GOCDBStatus_Command', 'GOCDBStatus_Command' ),
      'command'         : ( 'GOCDBStatus_Command', 'DTCached_Command' ),
      'args'            : ( 12, ),#Hacked to avoid executing code( pp["DTinHours"], ),
      'Site_Panel'      : [ {'WebLink': {'CommandIn': ( 'GOCDBStatus_Command', 'DTInfo_Cached_Command' ),
                                         'args': None}},],
      'Resource_Panel'  : [ {'WebLink': {'CommandIn': ( 'GOCDBStatus_Command', 'DTInfo_Cached_Command' ),
                                         'args': None}}]
    },

  'AlwaysActive' :
    {
      'description' : "A Policy that always returns Active",
      'module'      : 'AlwaysActive_Policy',
      'command'     : None,
      'args'        : None
    }
            
  }

################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF