""" ResourceStatusClient class is a client for requesting info from 
    the ResourceStatusService.
"""
from DIRAC import gLogger
from DIRAC.Core.DISET.RPCClient import RPCClient
from DIRAC.ResourceStatusSystem.Utilities.Exceptions import *
from DIRAC.ResourceStatusSystem.Utilities.Utils import *


class ResourceStatusClient:
  
#############################################################################

  def __init__(self, serviceIn = None):
    """ Constructor of the ResourceStatusClient class
    """
    if serviceIn == None:
      self.rsS = RPCClient("ResourceStatus/ResourceStatus")
    else:
      self.rsS = serviceIn

#############################################################################

  def getServiceStats(self, serviceType, siteName):
    """ returns simple statistics of active, probing and banned nodes of services;
            
        input:
          serviceType : string - a service type
          siteName : string - a site name
        
        returns:
          {
            'Computing: {'Active':xx, 'Probing':yy, 'Banned':zz, 'Total':xyz} (optional)
            'Storage: {'Active':xx, 'Probing':yy, 'Banned':zz, 'Total':xyz} (optional)
          }
    """

    if serviceType.capitalize() not in ValidService:
      raise InvalidService, where(self, self.evaluate)
    
    res = self.rsS.getServiceStats(serviceType, siteName)
    if res['OK']:
      return res['Value']

#############################################################################

  def getPeriods(self, granularity, name, status, hours):
    """ Returns a list of periods of time where name was in status
    
        returns:
          {
            'Periods':[list of periods]
          }

    """
    #da migliorare!
    
    if granularity not in ValidRes:
      raise InvalidRes, where(self, self.getPeriods)
    
    periods = self.rsS.getPeriods(granularity, name, status, hours)
    
    return {'Periods':periods['Value']}




#  
#  def addOrModifySite(self, siteName, siteType, description, status, reason, dateEffective, operatorCode, dateEnd):
#    try:
#      server = RPCClient('ResourceStatus/ResourceStatus', useCertificates=self.useCerts, timeout=120)
#      result = server.addOrModifySite(siteName, siteType, description, status, reason, dateEffective, operatorCode, dateEnd)
#      return result
#    except Exception, x:
#      errorStr = "ResourceStatusClient.addOrModifySite failed"
#      gLogger.exception(errorStr,lException=x)
#      return S_ERROR(errorStr+": "+str(x))
#
#  def addOrModifyResource(self, resourceName, resourceType, siteName, status, reason, dateEffective, operatorCode, dateEnd):
#    try:
#      server = RPCClient('ResourceStatus/ResourceStatus', useCertificates=self.useCerts, timeout=120)
#      result = server.addOrModifyResource(resourceName, resourceType, siteName, status, reason, dateEffective, operatorCode, dateEnd)
#      return result
#    except Exception, x:
#      errorStr = "ResourceStatusClient.addOrModifyResource failed"
#      gLogger.exception(errorStr,lException=x)
#      return S_ERROR(errorStr+": "+str(x))

