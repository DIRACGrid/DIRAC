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

################################################################################

def value_of_status(s):
  try:
    return int(s)
  except ValueError:
    try:
      return statesInfo[s][0]
    except KeyError:
      #Temporary fix, not anymore InvalidStatus exception raising
      gLogger.error( 'value_of_status returning -1')
      return -1

################################################################################

def value_of_policy(p):
  return value_of_status(p['Status'])

################################################################################

def status_of_value(v):
  # Hack: rely on the order of values in ValidStatus
  try:
    return ValidStatus[v]
  except IndexError:
    #Temporary fix, not anymore InvalidStatus exception raising
    gLogger.error( 'status_of_value returning -1')
    return -1
        
################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF