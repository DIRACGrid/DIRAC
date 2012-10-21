""" RequestManager is the implementation of the RequestDB service in the DISET framework
"""

__RCSID__ = "$Id$"

from types import *
from DIRAC.Core.DISET.RequestHandler import RequestHandler
from DIRAC import gLogger, gConfig, S_OK, S_ERROR
from DIRAC.ConfigurationSystem.Client import PathFinder

requestDB = False

def initializeRequestManagerHandler(serviceInfo):
  global requestDB
  csSection = PathFinder.getServiceSection( "RequestManagement/RequestManager" )
  backend = gConfig.getValue('%s/Backend' % csSection)
  if not backend:
    fatStr = "RequestManager.initializeRequestManagerHandler: Failed to get backed for RequestDB from CS."
    gLogger.fatal(fatStr)
    return S_ERROR(fatStr)
  gLogger.info("RequestManager.initializeRequestManagerHandler: Initialising with backend",backend)
  if backend == 'file':
    from DIRAC.RequestManagementSystem.DB.RequestDBFile import RequestDBFile
    requestDB = RequestDBFile()
  elif backend == 'mysql':
    from DIRAC.RequestManagementSystem.DB.RequestDBMySQL import RequestDBMySQL
    requestDB = RequestDBMySQL()
  else:
    fatStr = "RequestManager.initializeRequestManagerHandler: Supplied backend is not supported."
    gLogger.fatal(fatStr,backend)
    return S_ERROR(fatStr)
  return S_OK()

class RequestManagerHandler(RequestHandler):

  types_setRequest = [StringTypes,StringTypes]
  def export_setRequest(self,requestName,requestString):
    """ Set a new request
    """
    gLogger.info("RequestManagerHandler.setRequest: Attempting to set %s." % requestName)
    try:
      res = requestDB.setRequest(requestName,requestString)
      return res
    except Exception,x:
      errStr = "RequestManagerHandler.setRequest: Exception while setting request."
      gLogger.exception(errStr,requestName,lException=x)
      return S_ERROR(errStr)

  types_setRequestStatus = [StringTypes,StringTypes]
  def export_setRequestStatus(self,requestName,requestStatus):
    """ Set status of a request
    """
    gLogger.info("RequestHandler.setRequestStatus: Setting status of %s to %s." % (requestName,requestStatus))
    try:
      res = requestDB.setRequestStatus(requestName,requestStatus)
      return res
    except Exception,x:
      errStr = "RequestHandler.setRequestStatus: Exception while setting request status."
      gLogger.exception(errStr,requestName,lException=x)
      return S_ERROR(errStr)

  types_updateRequest = [StringTypes,StringTypes]
  def export_updateRequest(self,requestName,requestString):
    """ Update the request with the supplied string
    """
    gLogger.info("RequestManagerHandler.updateRequest: Attempting to update %s." % requestName)
    try:
      res = requestDB.updateRequest(requestName,requestString)
      return res
    except Exception,x:
      errStr = "RequestManagerHandler.updateRequest: Exception which updating request."
      gLogger.exception(errStr,requestName,lException=x)
      return S_ERROR(errStr)

  types_getDBSummary = []
  def export_getDBSummary(self):
    """ Get the summary of requests in the Request DB
    """
    gLogger.info("RequestManagerHandler.getDBSummary: Attempting to obtain database summary.")
    try:
      res = requestDB.getDBSummary()
      return res
    except Exception,x:
      errStr = "RequestManagerHandler.getDBSummary: Exception while getting database summary."
      gLogger.exception(errStr,lException=x)
      return S_ERROR(errStr)

  types_getRequest = [StringTypes]
  def export_getRequest(self,requestType):
    """ Get a request of given type from the database
    """
    gLogger.info("RequestHandler.getRequest: Attempting to get request type", requestType)
    try:
      res = requestDB.getRequest(requestType)
      return res
    except Exception,x:
      errStr = "RequestManagerHandler.getRequest: Exception while getting request."
      gLogger.exception(errStr,requestType,lException=x)
      return S_ERROR(errStr)

  types_serveRequest = []
  def export_serveRequest(self,requestType):
    """ Serve a request of a given type from the database
    """
    gLogger.info("RequestHandler.serveRequest: Attempting to serve request type", requestType)
    try:
      res = requestDB.serveRequest(requestType)
      return res
    except Exception,x:
      errStr = "RequestManagerHandler.serveRequest: Exception while serving request."
      gLogger.exception(errStr,requestType,lException=x)
      return S_ERROR(errStr)

  types_getRequestSummaryWeb = [DictType, ListType, IntType, IntType]
  def export_getRequestSummaryWeb(self,selectDict, sortList, startItem, maxItems):
    """ Get summary of the request/subrequest info in the standard form for the web
    """

    result = requestDB.getRequestSummaryWeb(selectDict, sortList, startItem, maxItems)
    return result
  
  types_getDistinctValues = [StringTypes]
  def export_getDistinctValues(self,attribute):
    """ Get distinct values for a given (sub)request attribute
    """
    snames = ['RequestType','Operation','Status']
    rnames = ['OwnerDN','OwnerGroup']
    if attribute in snames:
      result = requestDB.getDistinctAttributeValues('SubRequests',attribute)
    elif attribute in rnames:
      result = requestDB.getDistinctAttributeValues('Requests',attribute)  
    else:
      result = S_ERROR('Invalid attribute %s' % attribute)
      
    return result

  types_getDigest = [list(StringTypes)+[IntType,LongType]]
  def export_getDigest(self,requestName):
    """ Get the digest of the request identified by its name
    """

    if type(requestName) in StringTypes:
      result = requestDB._getRequestAttribute('RequestID',requestName=requestName)
      if not result['OK']:
        return result
      requestID = result['Value']
    else:
      requestID = requestName
    result = requestDB.getDigest(requestID)
    return result

  types_getCurrentExecutionOrder = [list(StringTypes)+[IntType,LongType]]
  def export_getCurrentExecutionOrder(self,requestName):
    """ Get the current execution order of the given request
    """

    if type(requestName) in StringTypes:
      result = requestDB._getRequestAttribute('RequestID',requestName=requestName)
      if not result['OK']:
        return result
      requestID = result['Value']
    else:
      requestID = requestName

    result = requestDB.getCurrentExecutionOrder(requestID)
    return result

  types_getRequestFileStatus = [list(StringTypes)+[IntType,LongType],ListType]
  def export_getRequestFileStatus(self,requestName,lfns):
    """ Get the current status of the provided files for the request """
    if type(requestName) in StringTypes:
      result = requestDB._getRequestAttribute('RequestID',requestName=requestName)
      if not result['OK']:
        return result
      requestID = result['Value']
    else:
      requestID = requestName
    return requestDB.getRequestFileStatus(requestID,lfns)

  types_getRequestStatus = [list(StringTypes)+[IntType,LongType]]
  def export_getRequestStatus(self,requestName):
    """ Get the current execution order of the given request
    """

    if type(requestName) in StringTypes:
      result = requestDB._getRequestAttribute('RequestID',requestName=requestName)
      if not result['OK']:
        return result
      requestID = result['Value']
    else:
      requestID = requestName

    result = requestDB.getRequestStatus(requestID)
    return result

  types_getRequestInfo = [list(StringTypes)+[IntType,LongType]]
  def export_getRequestInfo(self,requestName):
    if type(requestName) in StringTypes:
      result = requestDB._getRequestAttribute('RequestID',requestName=requestName)
      if not result['OK']:
        return result
      requestID = result['Value']
    else:
      requestID = requestName
    result = requestDB.getRequestInfo(requestID)
    return result

  types_deleteRequest = [StringTypes]
  def export_deleteRequest(self,requestName):
    """ Delete the request with the supplied name
    """
    gLogger.info("RequestManagerHandler.deleteRequest: Attempting to delete %s." % requestName)
    try:
      res = requestDB.deleteRequest(requestName)
      return res
    except Exception,x:
      errStr = "RequestManagerHandler.deleteRequest: Exception which deleting request."
      gLogger.exception(errStr,requestName,lException=x)
      return S_ERROR(errStr)

  types_getRequestForJobs = [ListType]
  def export_getRequestForJobs(self,jobIDs):
    """ Select the request names for supplied jobIDs
    """
    gLogger.info("RequestManagerHandler.getRequestForJobs: Attempting to get request names for %s jobs." % len(jobIDs))
    try:
      res = requestDB.getRequestForJobs(jobIDs)
      return res
    except Exception,x:
      errStr = "RequestManagerHandler.getRequestForJobs: Exception which getting request names."
      gLogger.exception(errStr,'',lException=x)
      return S_ERROR(errStr)
    
  types_selectRequests = [DictType]
  def export_selectRequests(self,selectDict,limit=100):
    """ Select requests according to supplied criteria
    """
    gLogger.verbose("RequestManagerHandler.selectRequests: Attempting to select requests." )
    try:
      res = requestDB.selectRequests(selectDict,limit)
      return res
    except Exception,x:
      errStr = "RequestManagerHandler.selectRequests: Exception while selecting requests."
      gLogger.exception(errStr,'',lException=x)
      return S_ERROR(errStr)  

  types_readRequestsForJobs = [ ListType ]
  def export_readRequestsForJobs( self, jobIDs ):
    """ read requests for jobs given list of jobIDs """
    gLogger.verbose("RequestManagerHandler.readRequestsForJobs: Attepting to read requests associated to the jobs." )
    try:
      res = requestDB.readRequestsForJobs( jobIDs )
      return res
    except Exception, x:
      errStr = "RequestManagerHandler.readRequestsForJobs: Exception while selecting requests."
      gLogger.exception( errStr )
      return S_ERROR( errStr )  

