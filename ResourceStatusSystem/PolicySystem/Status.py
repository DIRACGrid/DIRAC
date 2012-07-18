# $HeadURL $
''' Status

  Module that keeps the StateMachine.

'''

from DIRAC                                      import gLogger, S_OK, S_ERROR
from DIRAC.ResourceStatusSystem.Utilities.Utils import id_fun
from DIRAC.ResourceStatusSystem.Utilities       import RssConfiguration 

__RCSID__  = '$Id: $'

statesInfo = {
  'Banned'  : (0, set([0,1]), max),
  'Probing' : (1, set(), id_fun),
  'Bad'     : (2, set(), id_fun),
  'Active'  : (3, set(), id_fun)
  }

def value_of_status( status ):
  '''
  Given an status, returns its index
  '''
  try:   
    return int( status )
  except ValueError:
    try:
      return statesInfo[ status ][ 0 ]
    except KeyError:
      #Temporary fix, not anymore InvalidStatus exception raising
      gLogger.error( 'value_of_status returning -1' )
      return -1

def value_of_policy( policy ):
  '''
  Given a policy, returns its status
  '''
  return value_of_status( policy[ 'Status' ] )

def status_of_value( value ):
  '''
  To be refactored
  
  '''
  #FIXME: Hack: rely on the order of values in ValidStatus
  
  validStatus = RssConfiguration.getValidStatus()
  if not validStatus[ 'OK' ]:
    return validStatus
  validStatus = validStatus[ 'Value' ]
  
  if not value in validStatus:
    return S_ERROR( '"%s" not in %s' % ( value, validStatus ) )
  
  return S_OK( validStatus[ value ] )  
  
#  try:
#    return validStatus[ value ]
#  except IndexError:
#    #Temporary fix, not anymore InvalidStatus exception raising
#    gLogger.error( 'status_of_value returning -1' )
#    return -1
        
################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF