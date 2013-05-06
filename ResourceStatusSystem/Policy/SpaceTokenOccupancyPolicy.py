# $HeadURL $
""" SpaceTokenOccupancyPolicy

   SpaceTokenOccupancyPolicy.__bases__:
     DIRAC.ResourceStatusSystem.PolicySystem.PolicyBase.PolicyBase
  
"""

from DIRAC                                              import S_OK
from DIRAC.ResourceStatusSystem.PolicySystem.PolicyBase import PolicyBase

__RCSID__ = '$Id: SpaceTokenOccupancyPolicy.py 61805 2013-02-15 12:47:51Z ubeda $'

class SpaceTokenOccupancyPolicy( PolicyBase ):
  """
  The SpaceTokenOccupancyPolicy class is a policy class satisfied when a SE has a 
  low occupancy.

  SpaceTokenOccupancyPolicy, given the space left at the element, proposes a new status.
  """

  @staticmethod
  def _evaluate( commandResult ):
    """
    Evaluate policy on SE occupancy: Use SLS_Command

    :Parameters:
      **commandResult** - S_OK / S_ERROR
        result of the command. It is expected ( iff S_OK ) a dictionary like
        { 'Total' : .., 'Free' : .., 'Guaranteed': .. }

    :return:
      {
        'Status':Error|Active|Bad|Banned,
        'Reason': Some lame statements that have to be updated
      }
    """

    result = {}

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

    for key in [ 'Total', 'Free', 'Guaranteed' ]:

      if key not in commandResult.keys(): 
        result[ 'Status' ] = 'Error'
        result[ 'Reason' ] = 'Key %s missing' % key.lower()
        return S_OK( result )
    
    percentage = ( float( commandResult[ 'Free' ]) / commandResult[ 'Total' ] ) * 100
    
    if percentage == 0: 
      result[ 'Status' ] = 'Banned'     
    elif percentage < 10: 
      result[ 'Status' ] = 'Degraded'
    else: 
      result[ 'Status' ] = 'Active'
      
    result[ 'Reason' ] = 'Space availability: %d %%' % percentage
    return S_OK( result )
  
################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF  