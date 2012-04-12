# $HeadURL $
''' Status

  Module that keeps the StateMachine.

'''

from DIRAC                                      import gLogger
from DIRAC.ResourceStatusSystem.Utilities.Utils import id_fun
from DIRAC.ResourceStatusSystem                 import ValidStatus

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
  # Hack: rely on the order of values in ValidStatus
  try:
    return ValidStatus[ value ]
  except IndexError:
    #Temporary fix, not anymore InvalidStatus exception raising
    gLogger.error( 'status_of_value returning -1' )
    return -1
        
################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF