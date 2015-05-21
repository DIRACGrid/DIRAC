# $HeadURL:  $
''' CEAvailabilityPolicy module
'''

from DIRAC                                              import S_OK
from DIRAC.ResourceStatusSystem.PolicySystem.PolicyBase import PolicyBase

__RCSID__ = '$Id:  $'

class CEAvailabilityPolicy( PolicyBase ):
  '''
    The CEAvailabilityPolicy checks if the CE is in 'Production' or not on the BDII. 
  '''

  @staticmethod
  def _evaluate( commandResult ):
    '''
      It returns Active status if CE is in 'Production'. 
      Banned if the CE is different from 'Production'.
      
      Otherwise, it returns error.
    '''

    result = { 
               'Status' : None,
               'Reason' : None
              }

    if not commandResult[ 'OK' ]:
      result[ 'Status' ] = 'Error'
      result[ 'Reason' ] = commandResult[ 'Message' ]
      return S_OK( result )
    
    result = commandResult[ 'Value' ]

    if result['Status'] == 'Production':
      result[ 'Status' ] = 'Active'
    else:
      result[ 'Status' ] = 'Banned'
    
    result[ 'Reason' ] = result['Reason']

    return S_OK( result )

################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF