""" RequestManager is the implementation of the RequestDB service in the DISET framework
    A.Smith (23/05/07)
"""
from types import *
from DIRAC.Core.DISET.RequestHandler import RequestHandler
from DIRAC import gLogger, gConfig, S_OK, S_ERROR
from DIRAC.RequestManagementSystem.DB.RequestDB import RequestDB

#This is a global instance of the RequestDB class
requestDB = False

def initializeRequestManagerHandler(serviceInfo):
  global requestDB
  backend = gConfig.getValue('/Systems/RequestManagement/Development/Services/RequestManager/Backend')
  if not backend:
    fatStr = "RequestManager.initializeRequestManagerHandler: Failed to get backed for RequestDB from CS."
    gLogger.fatal(fatStr)
    return S_ERROR(fatStr)
  gLogger.info("RequestManager.initializeRequestManagerHandler: Initialising with backend",backend)
  requestDB = RequestDB(backend)
  return S_OK()

class RequestManagerHandler(RequestHandler):

  types_setRequest = [StringType,StringType]
  def export_setRequest(self,requestName,requestString):
    """ Set a new request
    """
    try:
      result = requestDB.setRequest(requestName,requestString)
      if not result['OK']:
        errKey = "Setting request failed"
        errExpl = " : for %s because %s" % (requestName,result['Message'])
        gLogger.error(errKey,errKey)
      return result
    except Exception,x:
      errKey = "Setting request failed"
      errExpl = " : for %s because %s" % (requestName,str(x))
      gLogger.exception(errKey,errExpl)
      return S_ERROR('Setting request failed: '+str(x))

  types_setRequestStatus = [StringType,StringType,StringType]
  def export_setRequestStatus(self,requestType,requestName,status):
    """ Set status of a request
    """
    gLogger.verbose("Setting request status for %s to %s" % (requestName,status))
    try:
      result = requestDB.setRequestStatus(requestType,requestName,status)
      if not result['OK']:
        errKey = "Setting request status failed"
        errExpl = " : for %s because %s" % (requestName,result['Message'])
        gLogger.error(errKey,errKey)
      return result
    except Exception,x:
      errKey = "Setting request status failed"
      errExpl = " for %s because %s" % (requestName,str(x))
      gLogger.exception(errKey,errExpl)
      return S_ERROR('Setting request status failed: '+str(x))

  types_getRequest = [StringType]
  def export_getRequest(self,requestType):
    """ Get a requests of given type from the DB
    """
    gLogger.info("RequestHandler.getRequest: Attempting to get request type", requestType)
    try:
      res = requestDB.getRequest(requestType)
      if not res['OK']:
        errKey = "RequestHandler.getRequest: Getting request failed"
        errExpl = "%s: %s" % (requestType,res['Message'])
        gLogger.error(errKey,errExpl)
        return res
      elif not res['Value']:
        gLogger.info("RequestHandler.getRequest: No requests found.")
        return res
      requestName = res['requestName']
      setStatus = requestDB.setRequestStatus(requestType,requestName,'Assigned')
      if setStatus['OK']:
        return res
      else:
        errKey = "RequestHandler.getRequest: Setting request status failed"
        errExpl = "%s: %s" % (requestName,setStatus['Message'])
        gLogger.error(errKey,errExpl)
        return S_ERROR(errKey)
    except Exception,x:
      errKey = "RequestManagerHandler.getRequest: Failed to get request type:"
      errExpl = "%s:  %s" % (requestType,str(x))
      gLogger.exception(errKey,errExpl)
      return S_ERROR("%s %s" % (errKey,errExpl))

  types_getRequestSummary = []
  def export_getDBSummary(self):
    """ Get the summary of requests in the Request DB
    """
    gLogger.verbose("Getting request summary")
    try:
      result = requestDB.getDBSummary()
      if not result['OK']:
        errKey = "Getting RequestDB summary failed"
        errExpl = " : because %s" % result['Message']
        gLogger.error(errKey,errKey)
      return result
    except Exception,x:
      errKey = "SGetting RequestDB summary failed"
      errExpl = " because %s" % str(x)
      gLogger.exception(errKey,errExpl)
      return S_ERROR('Getting RequestDB summary failed: '+str(x))
