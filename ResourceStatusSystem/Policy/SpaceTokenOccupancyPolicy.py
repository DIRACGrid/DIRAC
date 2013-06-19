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
    Evaluate policy on SE occupancy: Use SpaceTokenOccupancyCommand

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
    
    free       = float( commandResult[ 'Free' ] )
    percentage = ( free / commandResult[ 'Total' ] ) * 100
    
    # Units are TB ! ( 0.01 == 10 GB )
    if free < 0.1: 
      result[ 'Status' ] = 'Banned'     
    elif percentage < 5: 
      result[ 'Status' ] = 'Degraded'
    else: 
      result[ 'Status' ] = 'Active'
      
    result[ 'Reason' ] = 'Free space %.2f TB (%.2f %%)' % ( free, percentage )
    return S_OK( result )
  
################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF  