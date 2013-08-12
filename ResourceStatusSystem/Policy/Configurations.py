# $HeadURL:  $
""" Configurations module

  Configuration to use policies.
  
  Follows the schema:
  
  <PolicyNameInCS> : {
             'description' : <some human readable description>,
             'module'      : <policy module name>,
             'command'     : ( <command module name >, < command class name > ),
             'args'        : { arguments for the command } or None 
                     }
  
"""

__RCSID__ = '$Id:  $'

POLICIESMETA = {


  # DownTime POLICIES...........................................................
            
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


  # Space Token POLICIES........................................................

  'SpaceTokenOccupancy' :
    { 
      'description' : "Space token occupancy",
      'module'      : 'SpaceTokenOccupancyPolicy',
      'command'     : ( 'SpaceTokenOccupancyCommand', 'SpaceTokenOccupancyCommand' ),
      'args'        : { 'onlyCache' : True },
     }, 


  # Job POLICIES..............................................................

  'JobDoneRatio' :
    {
      'description' : "done / ( completed + done ) jobs ( 30 min )",
      'module'      : 'JobDoneRatioPolicy',
      'command'     : ( 'JobCommand', 'JobCommand' ),
      'args'        : { 'onlyCache' : True, 'timespan' : 1800 },    
     },

  'JobEfficiency' :
    {
      'description' : "( completed + done ) / ( completed + done + failed ) jobs ( 30 min )",
      'module'      : 'JobEfficiencyPolicy',
      'command'     : ( 'JobCommand', 'JobCommand' ),
      'args'        : { 'onlyCache' : True, 'timespan' : 1800 },    
     },

  'JobRunningMatchedRatio' :
    {
      'description' : "running / ( running + matched + received + checking ) jobs ( 30 min )",
      'module'      : 'JobRunningMatchedRatioPolicy',
      'command'     : ( 'JobCommand', 'JobCommand' ),
      'args'        : { 'onlyCache' : True, 'timespan' : 1800 },    
     },

  'JobRunningWaitingRatio' :
    {
      'description' : "running / ( running + waiting + staging ) jobs ( 30 min )",
      'module'      : 'JobRunningWaitingRatioPolicy',
      'command'     : ( 'JobCommand', 'JobCommand' ),
      'args'        : { 'onlyCache' : True, 'timespan' : 1800 },    
     },


  # Pilot POLICIES..............................................................

  'PilotInstantEfficiency' :
    {
      'description' : "Pilots Instant Efficiency ( 30 min )",
      'module'      : 'PilotEfficiencyPolicy',
      'command'     : ( 'PilotCommand', 'PilotCommand' ),
      'args'        : { 'onlyCache' : True, 'timespan' : 1800 }
     },


  # ALWAYS SOMETHING POLICIES...................................................

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


#...............................................................................
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF