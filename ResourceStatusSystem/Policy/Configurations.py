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
            
  'DTOngoing' :
    {
      'description' : "Ongoing and scheduled down-times",
      'module'      : 'DowntimePolicy',
      'command'     : ( 'DowntimeCommand', 'DowntimeCommand' ),
      'args'        : { 'hours' : 0, 'onlyCache' : True },
    },

  'DTScheduled' :
    {
      'description' : "Scheduled down-times, starting in <hours>",
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
    }
            
  }

################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF