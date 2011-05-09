"""
This module collects utility functions
"""

#############################################################################
# useful functions
#############################################################################

def where(c, f):
  return "Class " + str(c.__class__.__name__) + ", in Function " + (f.__name__)

def whoRaised(x):
  return "Exception: " + str(x.__class__.__name__) +", raised by " + str(x)

def assignOrRaise(value, set_, exc, obj, fun):
  """
  Check that a value is in a set or raise the corresponding exception
  If value is not None and is not in set, raise the corresponding
  exception, else return it
  """
  if value is not None and value not in set_:
    raise exc, where(obj, fun)
  else: return value

def convertTime(t, inTo = None):

  if inTo is None or inTo in ('second', 'seconds'):

    sec = 0

    try:
      tms = t.milliseconds
      sec = sec + tms/1000
    except AttributeError:
      pass
    try:
      ts = t.seconds
      sec = sec + ts
    except AttributeError:
      pass
    try:
      tm = t.minutes
      sec = sec + tm * 60
    except AttributeError:
      pass
    try:
      th = t.hours
      sec = sec + th * 3600
    except AttributeError:
      pass
    try:
      td = t.days
      sec = sec + td * 86400
    except AttributeError:
      pass
    try:
      tw = t.weeks
      sec = sec + tw * 604800
    except AttributeError:
      pass

    return sec

  elif inTo in ('hour', 'hours'):

    hour = 0

    try:
      tms = t.milliseconds
      hour = hour + tms/36000
    except AttributeError:
      pass
    try:
      ts = t.seconds
      hour = hour + ts/3600
    except AttributeError:
      pass
    try:
      tm = t.minutes
      hour = hour + tm/60
    except AttributeError:
      pass
    try:
      th = t.hours
      hour = hour + th
    except AttributeError:
      pass
    try:
      td = t.days
      hour = hour + td * 24
    except AttributeError:
      pass
    try:
      tw = t.weeks
      hour = hour + tw * 168
    except AttributeError:
      pass

    return hour

# vibernar utils functions

id_fun = lambda x: x
