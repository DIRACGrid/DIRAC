# $HeadURL:
''' DT_Policy

  The DT_Policy class is a policy class satisfied when a site is in downtime,
  or when a downtime is revoked.
  
'''

from DIRAC                                              import S_OK, S_ERROR
from DIRAC.ResourceStatusSystem.PolicySystem.PolicyBase import PolicyBase

__RCSID__ = '$Id: $'

class DT_Policy( PolicyBase ):

  def evaluate( self ):
    """
    Evaluate policy on possible ongoing or scheduled downtimes.

    :returns:
        {
          'Status':Unknown|Active|Bad|Banned,
          'Reason':'DT:None'|'DT:OUTAGE|'DT:AT_RISK',
          'EndDate':datetime (if needed)
        }
    """
    status = super( DT_Policy, self ).evaluate()

    if not status[ 'OK' ]:
      return status
    
    status = status[ 'Value' ]
    result = {}

    if status[ 'DT' ] == None:
      result[ 'Status' ] = 'Active'
      result[ 'Reason' ] = 'No DownTime announced'
      return S_OK( result )
    
    elif 'OUTAGE' in status[ 'DT' ]:
      result[ 'Status' ] = 'Banned'
      
    elif 'WARNING' in status['DT']:
      result[ 'Status' ] = 'Bad'

    else:
      return S_ERROR( 'DT_Policy: GOCDB returned an unknown value for DT: "%s"' % status[ 'DT' ] )

    result[ 'EndDate' ] = status[ 'EndDate' ]
    result[ 'Reason' ]  = 'DownTime found: %s' % status[ 'DT' ]
    return S_OK( result )

################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF