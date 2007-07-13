""" RequestDBHandler is the implementation of the RequestDB in the DISET framework
    A.Smith (23/05/07)
"""
from types import *
from DIRAC.Core.DISET.RequestHandler import RequestHandler
from DIRAC import gLogger, gConfig, S_OK, S_ERROR
from DIRAC.RequestManagementSystem.DB.RequestDB import RequestDB

#This is a global instance of the RequestDB class
requestDB = False

def initializeRequestHandler(serviceInfo):
  global requestDB
  backend = gConfig.getValue('/Systems/RequestManagement/Development/Services/RequestHandler/Backend')
  requestDB = RequestDB(backend)
  return S_OK()

class RequestDBHandler(RequestHandler):

  types_setRequest = [StringType,StringType,StringType,StringType]
  def export_setRequest(self,requestType,requestName,requestStatus,requestString):
    """ Set a new request
    """
    gLogger.verbose("Setting request "+requestName+" of type "+requestType+" with "+requestString)
    try:
      result = requestDB.setRequest(requestType,requestName,requestStatus,requestString)
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

  types_getRequest = [StringType,StringType]
  def export_getRequest(self,requestType,status):
    """ Get a requests of given type from the DB
    """
    gLogger.verbose("Getting %s %s request from RequestDB" % (status,requestType))
    try:
      result = requestDB.getRequest(requestType,status)
      if not result['OK']:
        errKey = "Getting request failed"
        errExpl = " : for %s because %s" % (requestType,result['Message'])
        gLogger.error(errKey,errKey)
        return result
      requestName = result['requestName']
      setStatus = requestDB.setRequestStatus(requestType,requestName,'Assigned')
      if setStatus['OK']:
        return result
      else:
        errKey = "Setting request status failed"
        errExpl = " : for %s because %s" % (requestName,setStatus['Message'])
        gLogger.error(errKey,errKey)
        return S_ERROR(errKey)
    except Exception,x:
      errKey = "Getting request failed"
      errExpl = " for %s because %s" % (requestName,str(x))
      gLogger.exception(errKey,errExpl)
      return S_ERROR('Getting request failed: '+str(x))

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