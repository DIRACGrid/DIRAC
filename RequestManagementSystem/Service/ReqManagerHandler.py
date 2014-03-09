#####################################################################
# $HeadURL $
# File: ReqManagerHandler.py
########################################################################
""" :mod: ReqManagerHandler
    =======================

    .. module: ReqManagerHandler
    :synopsis: Implementation of the RequestDB service in the DISET framework
"""
__RCSID__ = "$Id$"
# # imports
from types import DictType, IntType, LongType, ListType, StringTypes
# # from DIRAC
from DIRAC import gLogger, S_OK, S_ERROR
from DIRAC.Core.DISET.RequestHandler import RequestHandler
# # from RMS
from DIRAC.RequestManagementSystem.Client.Request import Request
from DIRAC.RequestManagementSystem.private.RequestValidator import RequestValidator
from DIRAC.RequestManagementSystem.DB.RequestDB import RequestDB

class ReqManagerHandler( RequestHandler ):
  """
  .. class:: ReqManagerHandler

  RequestDB interface in the DISET framework.
  """
  # # request validator
  __validator = None
  # # request DB instance
  __requestDB = None

  @classmethod
  def initializeHandler( cls, serviceInfoDict ):
    """ initialize handler """
    try:
      cls.__requestDB = RequestDB()
    except RuntimeError, error:
      gLogger.exception( error )
      return S_ERROR( error )

    # # create tables for empty db
    getTables = cls.__requestDB.getTables()
    if not getTables["OK"]:
      gLogger.error( getTables["Message"] )
      return getTables
    getTables = getTables["Value"]
    toCreate = [ tab for tab in cls.__requestDB.getTableMeta().keys() if tab not in getTables ]
    return cls.__requestDB.createTables( toCreate )

  # # helper functions
  @classmethod
  def validate( cls, request ):
    """ request validation """
    if not cls.__validator:
      cls.__validator = RequestValidator()
    return cls.__validator.validate( request )

  @classmethod
  def __getRequestID( cls, requestName ):
    """ get requestID for given :requestName: """
    requestID = requestName
    if type( requestName ) in StringTypes:
      result = cls.__requestDB.getRequestProperties( requestName, [ "RequestID" ] )
      if not result["OK"]:
        return result
      requestID = result["Value"]
    return S_OK( requestID )

  types_putRequest = [ DictType ]
  @classmethod
  def export_putRequest( cls, requestJSON ):
    """ put a new request into RequestDB

    :param cls: class ref
    :param str requestJSON: request serialized to JSON format
    """
    requestName = requestJSON.get( "RequestName", "***UNKNOWN***" )
    request = Request( requestJSON )
    valid = cls.validate( request )
    if not valid["OK"]:
      gLogger.error( "putRequest: request %s not valid: %s" % ( requestName, valid["Message"] ) )
      return valid
    requestName = request.RequestName
    gLogger.info( "putRequest: Attempting to set request '%s'" % requestName )
    return cls.__requestDB.putRequest( request )

  types_getScheduledRequest = [ ( IntType, LongType ) ]
  @classmethod
  def export_getScheduledRequest( cls , operationID ):
    """ read scheduled request given operationID """
    scheduled = cls.__requestDB.getScheduledRequest( operationID )
    if not scheduled["OK"]:
      gLogger.error( "getScheduledRequest: %s" % scheduled["Message"] )
      return scheduled
    if not scheduled["Value"]:
      return S_OK()
    requestJSON = scheduled["Value"].toJSON()
    if not requestJSON["OK"]:
      gLogger.error( "getScheduledRequest: %s" % requestJSON["Message"] )
    return requestJSON

  types_getDBSummary = []
  @classmethod
  def export_getDBSummary( cls ):
    """ Get the summary of requests in the Request DB """
    return cls.__requestDB.getDBSummary()

  types_getRequest = [ StringTypes ]
  @classmethod
  def export_getRequest( cls, requestName = "" ):
    """ Get a request of given type from the database """
    getRequest = cls.__requestDB.getRequest( requestName )
    if not getRequest["OK"]:
      gLogger.error( "getRequest: %s" % getRequest["Message"] )
      return getRequest
    if getRequest["Value"]:
      getRequest = getRequest["Value"]
      toJSON = getRequest.toJSON()
      if not toJSON["OK"]:
        gLogger.error( toJSON["Message"] )
      return toJSON
    return S_OK()

  types_peekRequest = [ StringTypes ]
  @classmethod
  def export_peekRequest( cls, requestName = "" ):
    """ peek request given its name """
    peekRequest = cls.__requestDB.peekRequest( requestName )
    if not peekRequest["OK"]:
      gLogger.error( "peekRequest: %s" % peekRequest["Message"] )
      return peekRequest
    if peekRequest["Value"]:
      peekRequest = peekRequest["Value"].toJSON()
      if not peekRequest["OK"]:
        gLogger.error( peekRequest["Message"] )
    return peekRequest

  types_getRequestSummaryWeb = [ DictType, ListType, IntType, IntType ]
  @classmethod
  def export_getRequestSummaryWeb( cls, selectDict, sortList, startItem, maxItems ):
    """ Get summary of the request/operations info in the standard form for the web

    :param dict selectDict: selection dict
    :param list sortList: whatever
    :param int startItem: start item
    :param int maxItems: max items
    """
    return cls.__requestDB.getRequestSummaryWeb( selectDict, sortList, startItem, maxItems )

  types_getDistinctValues = [ StringTypes ]
  @classmethod
  def export_getDistinctValues( cls, attribute ):
    """ Get distinct values for a given (sub)request attribute """
    onames = ['Type', 'Status']
    rnames = ['OwnerDN', 'OwnerGroup']
    if attribute in onames:
      return cls.__requestDB.getDistinctAttributeValues('Operation', attribute)
    elif attribute in rnames:
      return cls.__requestDB.getDistinctAttributeValues('Request', attribute)
    return S_ERROR('Invalid attribute %s' % attribute)

  types_deleteRequest = [ StringTypes ]
  @classmethod
  def export_deleteRequest( cls, requestName ):
    """ Delete the request with the supplied name"""
    return cls.__requestDB.deleteRequest( requestName )

  types_getRequestNamesList = [ ListType, IntType ]
  @classmethod
  def export_getRequestNamesList( cls, statusList = None, limit = None ):
    """ get requests' names with status in :statusList: """
    statusList = statusList if statusList else list( Request.FINAL_STATES )
    limit = limit if limit else 100
    reqNamesList = cls.__requestDB.getRequestNamesList( statusList, limit )
    if not reqNamesList["OK"]:
      gLogger.error( "getRequestNamesList: %s" % reqNamesList["Message"] )
    return reqNamesList

  types_getRequestNamesForJobs = [ ListType ]
  @classmethod
  def export_getRequestNamesForJobs( cls, jobIDs ):
    """ Select the request names for supplied jobIDs """
    return cls.__requestDB.getRequestNamesForJobs( jobIDs )

  types_readRequestsForJobs = [ ListType ]
  @classmethod
  def export_readRequestsForJobs( cls, jobIDs ):
    """ read requests for jobs given list of jobIDs """
    requests = cls.__requestDB.readRequestsForJobs( jobIDs )
    if not requests["OK"]:
      gLogger.error( "readRequestsForJobs: %s" % requests["Message"] )
      return requests
    for jobID, request in requests["Value"]["Successful"].items():
      requests["Value"]["Successful"][jobID] = request.toJSON()["Value"]
    return requests

  types_getDigest = [ StringTypes ]
  @classmethod
  def export_getDigest( cls, requestName ):
    """ get digest for a request given its name

    :param str requestName: request's name
    :return: S_OK( json_str )
    """
    return cls.__requestDB.getDigest( requestName )

  types_getRequestStatus = [ StringTypes ]
  @classmethod
  def export_getRequestStatus( cls, requestName ):
    """ get request status given its name """
    status = cls.__requestDB.getRequestStatus( requestName )
    if not status["OK"]:
      gLogger.error( "getRequestStatus: %s" % status["Message"] )
    return status

  types_getRequestFileStatus = [ list( StringTypes ) + [ IntType, LongType ], list( StringTypes ) + [ListType] ]
  @classmethod
  def export_getRequestFileStatus( cls, requestName, lfnList ):
    """ get request file status for a given LFNs list and requestID/Name """
    if type( lfnList ) == str:
      lfnList = [lfnList]
    res = cls.__requestDB.getRequestFileStatus( requestName, lfnList )
    if not res["OK"]:
      gLogger.error( "getRequestFileStatus: %s" % res["Message"] )
    return res

  types_getRequestName = [ ( IntType, LongType ) ]
  @classmethod
  def export_getRequestName( cls, requestID ):
    """ get request name for a given requestID """
    requestName = cls.__requestDB.getRequestName( requestID )
    if not requestName["OK"]:
      gLogger.error( "getRequestName: %s" % requestName["Message"] )
    return requestName

  types_getRequestInfo = [ list( StringTypes ) + [ IntType, LongType ] ]
  @classmethod
  def export_getRequestInfo( cls, requestName ):
    """ get request info for a given requestID/Name """
    requestInfo = cls.__requestDB.getRequestInfo( requestName )
    if not requestInfo["OK"]:
      gLogger.error( "getRequestInfo: %s" % requestInfo["Message"] )
    return requestInfo
