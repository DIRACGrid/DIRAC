from DIRAC.ResourceStatusSystem.Utilities.Exceptions import InvalidStatus


def valueOfStatus(p,valDict=None):
  """
  p: a status or a policy

  Returns the value of that status or the value of the status of that
  policy
  """
  if valDict == None:
    valDict = {
      'Banned'  : 0,
      'Bad'     : 1,
      'Probing' : 2,
      'Active'  : 3
      }
  try: return valDict[p['Status']]
  except TypeError:
    try: return valDict[p]
    except KeyError: raise InvalidStatus()
  except KeyError: raise InvalidStatus()
