# $HeadURL:  $
''' DTPolicy module
'''

from DIRAC                                              import S_OK, S_ERROR
from DIRAC.ResourceStatusSystem.PolicySystem.PolicyBase import PolicyBase

__RCSID__ = '$Id:  $'

class DTPolicy( PolicyBase ):
  '''
    The DTPolicy checks for downtimes, scheduled or ongoing, depending on the
    command parameters. 
  '''

  def evaluate( self ):
    '''
      It returns Active status if there is no downtime announced. 
      Banned if the element is in OUTAGE.
      Bad if it is on WARNING status.
      
      Otherwise, it returns error.
      #FIXME: use it or scratch it.
      On top of that, it also returns the downtime end date ( which is not currently
      used ).
    '''
    
    status = super( DTPolicy, self ).evaluate()

    if not status[ 'OK' ]:
      return status
    
    status = status[ 'Value' ]
    result = {}

    if not 'DT' in status:
      return S_ERROR( 'Expecting "DT" key on dictionary' )

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