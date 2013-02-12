# $HeadURL:  $
''' Configurations module

  Configuration to use policies.
  
  Follows the schema:
  
  <PolicyNameInCS> : {
             'description' : <some human readable description>,
             'module'      : <policy module name>,
             'command'     : ( <command module name >, < command class name > ),
             'args'        : { arguments for the command } or None 
                     }
  
'''

__RCSID__ = '$Id:  $'

POLICIESMETA = {
            
#  'DTOnGoingOnly' :
#    {
#      'description' : "Ongoing down-times",
#      'module'      : 'DTPolicy',
#      'command'     : ( 'DowntimeCommand', 'DowntimeCommand' ),
#      'args'        : None
#    },

  'DTScheduled' :
    {
      'description' : "Ongoing and scheduled down-times",
      'module'      : 'DowntimePolicy',
      'command'     : ( 'DowntimeCommand', 'DowntimeCommand' ),
      'args'        : { 'hours' : 12, 'onlyCache' : True },
    },

  'AlwaysActive' :
    {
      'description' : "A Policy that always returns Active",
      'module'      : 'AlwaysActivePolicy',
      'command'     : None,
      'args'        : None
    },

  'AlwaysDegraded' :
    {
      'description' : "A Policy that always returns Degraded",
      'module'      : 'AlwaysDegradedPolicy',
      'command'     : None,
      'args'        : None
    },
                
  'AlwaysProbing' :
    {
      'description' : "A Policy that always returns Probing",
      'module'      : 'AlwaysProbingPolicy',
      'command'     : None,
      'args'        : None
    },                

  'AlwaysBanned' :
    {
      'description' : "A Policy that always returns Banned",
      'module'      : 'AlwaysBannedPolicy',
      'command'     : None,
      'args'        : None
    }
                      
  }

################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF