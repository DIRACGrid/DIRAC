""" DIRAC.ResourceStatusSystem.Policy.Configurations Module

    collects everything needed to configure policies
"""

__RCSID__ = "$Id: "

from DIRAC.ResourceStatusSystem.Utilities import CS

pp = CS.getTypedDictRootedAt("PolicyParameters")

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
