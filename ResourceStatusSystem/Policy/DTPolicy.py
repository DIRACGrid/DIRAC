# $HeadURL:  $
''' DTPolicy module
'''

from DIRAC                                              import S_OK
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
    '''
    
    status = super( DTPolicy, self ).evaluate()

    result = { 
               'Status' : None,
               'Reason' : None
              }

    if not status[ 'OK' ]:
      result[ 'Status' ] = 'Error'
      result[ 'Reason' ] = status[ 'Message' ]
      return S_OK( result )
    
    status = status[ 'Value' ]

    if not status:
      result[ 'Status' ] = 'Error'
      result[ 'Reason' ] = 'Expecting a dictionary'
      return S_OK( result )

    if not 'DT' in status:
      result[ 'Status' ] = 'Error'
      result[ 'Reason' ] = 'Expecting DT key in dictionary'
      return S_OK( result )

    if status[ 'DT' ] == None:
      result[ 'Status' ] = 'Active'
      result[ 'Reason' ] = 'No DownTime announced'
      return S_OK( result )
    
    elif 'OUTAGE' in status[ 'DT' ]:
      result[ 'Status' ] = 'Banned'
      
    elif 'WARNING' in status['DT']:
      result[ 'Status' ] = 'Degraded'

    else:
      _reason = 'DT_Policy: GOCDB returned an unknown value for DT: "%s"' % status[ 'DT' ]
      result[ 'Status' ] = 'Error'
      result[ 'Reason' ] = _reason
      return S_OK( result )
      
    #result[ 'EndDate' ] = status[ 'EndDate' ]
    result[ 'Reason' ]  = 'DownTime found: %s' % status[ 'DT' ]
    return S_OK( result )

################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF