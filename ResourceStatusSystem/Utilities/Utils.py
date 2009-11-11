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

ValidRes = ['Site', 'Service', 'Resource']
ValidStatus = ['Active', 'Probing', 'Banned']
PolicyTypes = ['Resource_PolType', 'Alarm_PolType', 'Collective_PolType']
ValidService = ['Computing', 'Storage']
ValidSiteType = ['T0', 'T1', 'T2']
ValidResourceType = ['CE', 'SE', 'CREAM CE']
#ValidReasons = ['init', 'DT:OUTAGE', 'DT:AT_RISK', 'DT:None']

#############################################################################
