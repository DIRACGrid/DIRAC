""" JobRunningWaitingRatioPolicy
  
  Policy that calculates the efficiency following the formula:
    ( running ) / ( running + waiting + staging )
  if the denominator is smaller than 10, it does not take any decision.
  
"""

from DIRAC                                              import S_OK
from DIRAC.ResourceStatusSystem.PolicySystem.PolicyBase import PolicyBase

__RCSID__ = '$Id: JobRunningWaitingRatioPolicy.py 60769 2013-01-18 11:50:36Z ubeda $'


class JobRunningWaitingRatioPolicy( PolicyBase ):
  """
  The JobRunningWaitingRatioPolicy class is a policy that checks the efficiency of the 
  jobs according to what is on JobDB.
  
    Evaluates the JobRunningWaitingRatioPolicy results given by the JobCommand.JobCommand
  """
  
  
  @staticmethod
  def _evaluate( commandResult ):
    """ _evaluate
    
    efficiency < 0.5 :: Banned
    efficiency < 0.9 :: Degraded
    
    """

    result = { 
              'Status' : None,
              'Reason' : None
              }

    if not commandResult[ 'OK' ]:
      result[ 'Status' ] = 'Error'
      result[ 'Reason' ] = commandResult[ 'Message' ]
      return S_OK( result )

    commandResult = commandResult[ 'Value' ]

    if not commandResult:
      result[ 'Status' ] = 'Unknown'
      result[ 'Reason' ] = 'No values to take a decision'
      return S_OK( result )

    commandResult = commandResult[ 0 ]

    if not commandResult:
      result[ 'Status' ] = 'Unknown'
      result[ 'Reason' ] = 'No values to take a decision'
      return S_OK( result )

    running = float( commandResult[ 'Running' ] )
    waiting = float( commandResult[ 'Waiting' ] )
    staging = float( commandResult[ 'Staging' ] )

    total = running + waiting + staging

    #we want a minimum amount of jobs to take a decision ( at least 10 pilots )
    if total < 10:
      result[ 'Status' ] = 'Unknown'
      result[ 'Reason' ] = 'Not enough jobs to take a decision'
      return S_OK( result )
    
    efficiency = running / total

    if efficiency < 0.4:
      result[ 'Status' ] = 'Banned'
    elif efficiency < 0.65:
      result[ 'Status' ] = 'Degraded'  
    else:   
      result[ 'Status' ] = 'Active'    
          
    result[ 'Reason' ] = 'Job Running / Waiting ratio of %.2f' % efficiency
    return S_OK( result )


#...............................................................................
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF