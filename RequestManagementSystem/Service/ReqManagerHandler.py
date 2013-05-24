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
from types import DictType, IntType, ListType, StringTypes
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
    requestName = "***UNKNOWN***"
    try:
      request = Request( requestJSON )
      if not request["OK"]:
        gLogger.error( "putRequest: %s" % request["Message"] )
        return request
      request = request["Value"]
      valid = cls.validate( request )
      if not valid["OK"]:
        gLogger.error( "putRequest: request not valid: %s" % valid["Message"] )
        return valid
      requestName = request.RequestName
      gLogger.info( "putRequest: Attempting to set request '%s'" % requestName )
      return cls.__requestDB.putRequest( request )
    except Exception, error:
      errStr = "putRequest: Exception while setting request."
      gLogger.exception( errStr, requestName, lException = error )
      return S_ERROR( errStr )

  types_getDBSummary = []
  @classmethod
  def export_getDBSummary( cls ):
    """ Get the summary of requests in the Request DB """
    try:
      return cls.__requestDB.getDBSummary()
    except Exception, error:
      errStr = "getDBSummary: Exception while getting database summary."
      gLogger.exception( errStr, lException = error )
      return S_ERROR( errStr )

  types_getRequest = [ StringTypes ]
  @classmethod
  def export_getRequest( cls, requestName = "" ):
    """ Get a request of given type from the database """
    try:
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
    except Exception, error:
      errStr = "getRequest: Exception while getting request."
      gLogger.exception( errStr, lException = error )
      return S_ERROR( errStr )

  types_peekRequest = [ StringTypes ]
  @classmethod
  def export_peekRequest( cls, requestName = "" ):
    """ peek request given its name """
    try:
      peekRequest = cls.__requestDB.peekRequest( requestName )
      if not peekRequest["OK"]:
        gLogger.error( "peekRequest: %s" % peekRequest["Message"] )
        return peekRequest
      if peekRequest["Value"]:
        peekRequest = peekRequest["Value"].toJSON()
        if not peekRequest["OK"]:
          gLogger.error( peekRequest["Message"] )
      return peekRequest
    except Exception, error:
      errStr = "peekRequest: Exception while getting request."
      gLogger.exception( errStr, lException = error )
      return S_ERROR( errStr )

  types_getRequestSummaryWeb = [ DictType, ListType, IntType, IntType ]
  @classmethod
  def export_getRequestSummaryWeb( cls, selectDict, sortList, startItem, maxItems ):
    """ Get summary of the request/operations info in the standard form for the web

    :param dict selectDict: selection dict
    :param list sortList: ???
    :param int startItem: start item
    :param int maxItems: max items
    """
    try:
      return cls.__requestDB.getRequestSummaryWeb( selectDict, sortList, startItem, maxItems )
    except Exception, error:
      errStr = "getRequestSummaryWeb: Exception while getting request."
      gLogger.exception( errStr, lException = error )
      return S_ERROR( errStr )

  types_deleteRequest = [ StringTypes ]
  @classmethod
  def export_deleteRequest( cls, requestName ):
    """ Delete the request with the supplied name"""
    try:
      return cls.__requestDB.deleteRequest( requestName )
    except Exception, error:
      errStr = "deleteRequest: Exception which deleting request '%s'." % requestName
      gLogger.exception( errStr, lException = error )
      return S_ERROR( errStr )

  types_getRequestNameList = [ ListType, IntType ]
  @classmethod
  def export_getRequestNamesList( cls, statusList = None, limit = None ):
    """ get requests' names with status in :statusList: """
    statusList = statusList if statusList else list( Request.FINAL_STATES )
    limit = limit if limit else 100
    try:
      reqNamesList = cls.__requestDB.getRequestNamesList( statusList, limit )
      if not reqNamesList["OK"]:
        gLogger.error( "getRequestNamesList: %s" % reqNamesList["Message"] )
      return reqNamesList
    except Exception, error:
      gLogger.exception( error )
      return S_ERROR( str( error ) )

  types_getRequestNamesForJobs = [ ListType ]
  @classmethod
  def export_getRequestNamesForJobs( cls, jobIDs ):
    """ Select the request names for supplied jobIDs """
    try:
      return cls.__requestDB.getRequestNamesForJobs( jobIDs )
    except Exception, error:
      errStr = "getRequestNamesForJobs: Exception which getting request names."
      gLogger.exception( errStr, '', lException = error )
      return S_ERROR( errStr )

  types_readRequestsForJobs = [ ListType ]
  @classmethod
  def export_readRequestsForJobs( cls, jobIDs ):
    """ read requests for jobs given list of jobIDs """
    try:
      return cls.__requestDB.readRequestsForJobs( jobIDs )
    except Exception, error:
      errStr = "readRequestsForJobs: Exception while selecting requests."
      gLogger.exception( errStr, '', lException = error )
      return S_ERROR( errStr )

  types_getDigest = [ StringTypes ]
  @classmethod
  def export_getDigest( cls, requestName ):
    """ get digest for a request given its name

    :param str requestName: request's name
    :return: S_OK( json_str )
    """
    try:
      return cls.__requestDB.getDigest( requestName )
    except Exception , error:
      errStr = "getDigest: exception when getting digest for '%s'" % requestName
      gLogger.exception( errStr, '', lException = error )
      return S_ERROR( errStr )
