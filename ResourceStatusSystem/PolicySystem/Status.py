# $HeadURL $
''' Status

  Module that keeps the StateMachine.

'''

from DIRAC                                      import S_OK, S_ERROR
from DIRAC.ResourceStatusSystem.Utilities.Utils import id_fun

__RCSID__  = '$Id: $'

statesInfo = {
  'Banned'  : (0, set([0,1]), max),
  'Probing' : (1, set(), id_fun),
  'Bad'     : (2, set(), id_fun),
  'Active'  : (3, set(), id_fun)
  }

def value_of_policy( policy ):
  '''
  Given a policy, returns its status
  '''
  
  return statesInfo[ policy[ 'Status' ] ][ 0 ]

def status_of_value( value ):
  '''
  To be refactored
  
  '''
   
  for statusName, statusValue in statesInfo.items():
    
    if statusValue[ 0 ] == value:
      return S_OK( statusName )
  
  return S_ERROR( '%s not found as a valid weight' % value )
        
################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF