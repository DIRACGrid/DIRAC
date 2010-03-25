""" collects:

      - utility functions

      - parameters
"""
#############################################################################
# useful functions
#############################################################################

def where(c, f):
  return "Class " + str(c.__class__.__name__) + ", in Function " + (f.__name__)

#############################################################################

def whoRaised(x):
  return "Exception: " + str(x.__class__.__name__) +", raised by " + str(x)

#############################################################################

def getSiteRealName(s):
  from DIRAC.Interfaces.API.DiracAdmin import DiracAdmin
  DA = DiracAdmin()
  res = DA.getCSDict('/Resources/Sites/%s/%s' %(s.split('.')[0],s))
  if not res['OK']:
    return s
  else:
    return res['Value']['Name']

#############################################################################

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
    
#############################################################################
# general parameters
#############################################################################

ValidRes = ['Site', 'Service', 'Resource', 'StorageElement']
ValidStatus = ['Active', 'Probing', 'Bad', 'Banned']
PolicyTypes = ['Resource_PolType', 'Alarm_PolType', 'Collective_PolType', 'View_PolType']
ValidSiteType = ['T0', 'T1', 'T2']
ValidResourceType = ['CE', 'CREAMCE', 'SE', 'LFC_C', 'LFC_L', 'FTS']
ValidService = ValidServiceType = ['Computing', 'Storage']
ValidView = ['Site_View', 'Resource_View', 'StorageElement_View']

#############################################################################
