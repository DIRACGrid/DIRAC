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

#def getGeneralName(res, from_g, to_g):
#  """ 
#  get name of res, of granularity from_g, to the name of res with granularity to_g
#    
#  For a Resource, get the Site name, or the Service name.
#  For a Service name, get the Site name
#  
#  :params:
#    :attr:`res`: a string
#    :attr:`from_g`: a string
#    :attr:`to_g`: a string
#  """
#  from DIRAC.ResourceStatusSystem.DB.ResourceStatusDB import *
#  rsDB = ResourceStatusDB()
#  if from_g == 'Service':
#    if to_g is None or to_g == 'Site': 
#      return rsDB.get

#############################################################################
# general parameters
#############################################################################

ValidRes = ['Site', 'Service', 'Resource', 'StorageElement']
ValidStatus = ['Active', 'Probing', 'Banned']
PolicyTypes = ['Resource_PolType', 'Alarm_PolType', 'Collective_PolType']
ValidSiteType = ['T0', 'T1', 'T2']
ValidResourceType = ['CE', 'CREAMCE', 'SE', 'LFC_C', 'LFC_L', 'FTS']
ValidService = ValidServiceType = ['Computing', 'Storage']
#ValidReasons = ['init', 'DT:OUTAGE', 'DT:AT_RISK', 'DT:None']

#############################################################################
