from DIRAC.ResourceStatusSystem.Utilities.Utils      import id_fun
from DIRAC.ResourceStatusSystem.Utilities.Exceptions import InvalidStatus
from DIRAC.ResourceStatusSystem                      import ValidStatus

statesInfo = {
  'Banned'  : (0, set([0,1]), max),
  'Probing' : (1, set(), id_fun),
  'Bad'     : (2, set(), id_fun),
  'Active'  : (3, set(), id_fun)
  }

def value_of_status(s):
  try:
    return int(s)
  except ValueError:
    try:
      return statesInfo[s][0]
    except KeyError:
      raise InvalidStatus

def value_of_policy(p):
  return value_of_status(p['Status'])

def status_of_value(v):
  # Hack: rely on the order of values in ValidStatus
  try:
    return ValidStatus[v]
  except IndexError:
    raise InvalidStatus
