# $HeadURL:
''' DT_Policy

  The DT_Policy class is a policy class satisfied when a site is in downtime,
  or when a downtime is revoked.
  
'''

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
    result = {}

    if not status[ 'OK' ]:
      result[ 'Status' ] = 'Error'
      result[ 'Reason' ] = status[ 'Message' ]
      return result

    
    status = status[ 'Value' ]
    #elif status == 'Unknown':
    #  return { 'Status' : 'Unknown' }

    if status[ 'DT' ] == None:
      result[ 'Status' ]  = 'Active'
      result[ 'Reason' ]  = 'No DownTime announced'
      return result
    
    elif 'OUTAGE' in status[ 'DT' ]:
      result[ 'Status' ]  = 'Banned'
      
    elif 'WARNING' in status['DT']:
      result[ 'Status' ]  = 'Bad'

    else:
      return { 'Status' : 'Error', 'Reason' : 'GOCDB returned an unknown value for DT' }

    result[ 'EndDate' ] = status[ 'EndDate' ]
    result[ 'Reason' ]  = 'DownTime found: %s' % status[ 'DT' ]
    return result

  evaluate.__doc__ = PolicyBase.evaluate.__doc__ + evaluate.__doc__

################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF