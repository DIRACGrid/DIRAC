# $HeadURL $
''' Configurations

  Collects everything needed to configure policies.
  
'''

from DIRAC.ResourceStatusSystem.Utilities import CS

__RCSID__ = '$Id: $'

#pp = CS.getTypedDictRootedAt( 'PolicyParameters' )

def getPolicyParameters():
  return CS.getTypedDictRootedAtOperations( 'PolicyParameters' )

Policies = {
            
  'DT_OnGoing_Only' :
    {
      'Description' : "Ongoing down-times",
      'module'      : 'DT_Policy',
      'commandIn'   : ( 'GOCDBStatus_Command', 'GOCDBStatus_Command' ),
      'args'        : None
    },

  'DT_Scheduled' :
    {
      'Description'     : "Ongoing and scheduled down-times",
      'module'          : 'DT_Policy',
      'commandInNewRes' : ( 'GOCDBStatus_Command', 'GOCDBStatus_Command' ),
      'commandIn'       : ( 'GOCDBStatus_Command', 'DTCached_Command' ),
      'args'            : ( pp["DTinHours"], ),
      'Site_Panel'      : [ {'WebLink': {'CommandIn': ( 'GOCDBStatus_Command', 'DTInfo_Cached_Command' ),
                                         'args': None}},],
      'Resource_Panel'  : [ {'WebLink': {'CommandIn': ( 'GOCDBStatus_Command', 'DTInfo_Cached_Command' ),
                                         'args': None}}]
    },

  'AlwaysFalse' :
    {
      'Description' : "A Policy that always returns false",
      'commandIn'   : None,
      'args'        : None
    }
            
  }

################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF