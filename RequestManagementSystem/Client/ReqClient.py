########################################################################################
# $HeadURL$
########################################################################################
"""
    :mod:  ReqClient
    ================

    .. module:  ReqClient
    :synopsis: implementation of client for RequestDB using DISET framework
"""
# # RCSID
__RCSID__ = "$Id$"
# # from DIRAC
from DIRAC import gLogger, S_OK, S_ERROR
from DIRAC.Core.DISET.RPCClient import RPCClient
from DIRAC.Core.Utilities.List import randomize, fromChar
from DIRAC.ConfigurationSystem.Client import PathFinder
from DIRAC.Core.Base.Client import Client
from DIRAC.RequestManagementSystem.Client.Request import Request
from DIRAC.RequestManagementSystem.private.RequestValidator import RequestValidator

class ReqClient( Client ):
  """
  .. class:: ReqClient

  ReqClient is a class manipulating and operation on Requests.

  :param RPCClient requestManager: RPC client to RequestManager
  :param dict requestProxiesDict: RPC client to ReqestProxy
  :param RequestValidator requestValidator: RequestValidator instance
  """
  __requestManager = None
  __requestProxiesDict = {}
  __requestValidator = None

  def __init__( self, useCertificates = False ):
    """c'tor

    :param self: self reference
    :param bool useCertificates: flag to enable/disable certificates
    """
    Client.__init__( self )
    self.log = gLogger.getSubLogger( "RequestManagement/ReqClient" )
    self.setServer( "RequestManagement/ReqManager" )

  def requestManager( self, timeout = 120 ):
    """ facade for RequestManager RPC client """
    if not self.__requestManager:
      url = PathFinder.getServiceURL( "RequestManagement/ReqManager" )
      if not url:
        raise RuntimeError( "CS option RequestManagement/ReqManager URL is not set!" )
      self.__requestManager = RPCClient( url, timeout = timeout )
    return self.__requestManager

  def requestProxies( self, timeout = 120 ):
    """ get request proxies dict """
    if not self.__requestProxiesDict:
      self.__requestProxiesDict = {}
      proxiesURLs = fromChar( PathFinder.getServiceURL( "RequestManagement/ReqProxyURLs" ) )
      if not proxiesURLs:
        self.log.warn( "CS option RequestManagement/ReqProxyURLs is not set!" )
      for proxyURL in randomize( proxiesURLs ):
        self.log.debug( "creating RequestProxy for url = %s" % proxyURL )
        self.__requestProxiesDict[proxyURL] = RPCClient( proxyURL, timeout = timeout )
    return self.__requestProxiesDict

  def requestValidator( self ):
    """ get request validator """
    if not self.__requestValidator:
      self.__requestValidator = RequestValidator()
    return self.__requestValidator

  def putRequest( self, request ):
    """ put request to RequestManager

    :param self: self reference
    :param Request request: Request instance
    """
    errorsDict = { "OK" : False }
    valid = self.requestValidator().validate( request )
    if not valid["OK"]:
      self.log.error( "putRequest: request not valid: %s" % valid["Message"] )
      return valid
    # # dump to xml string
    requestJSON = request.toJSON()
    if not requestJSON["OK"]:
      return requestJSON
    requestJSON = requestJSON["Value"]
    setRequestMgr = self.requestManager().putRequest( requestJSON )
    if setRequestMgr["OK"]:
      return setRequestMgr
    errorsDict["RequestManager"] = setRequestMgr["Message"]
    self.log.warn( "putRequest: unable to set request '%s' at RequestManager" % request.RequestName )
    proxies = self.requestProxies()
    for proxyURL, proxyClient in proxies.items():
      self.log.debug( "putRequest: trying RequestProxy at %s" % proxyURL )
      setRequestProxy = proxyClient.putRequest( requestJSON )
      if setRequestProxy["OK"]:
        if setRequestProxy["Value"]["set"]:
          self.log.info( "putRequest: request '%s' successfully set using RequestProxy %s" % ( request.RequestName,
                                                                                               proxyURL ) )
        elif setRequestProxy["Value"]["saved"]:
          self.log.info( "putRequest: request '%s' successfully forwarded to RequestProxy %s" % ( request.RequestName,
                                                                                                  proxyURL ) )
        return setRequestProxy
      else:
        self.log.warn( "putRequest: unable to set request using RequestProxy %s: %s" % ( proxyURL,
                                                                                         setRequestProxy["Message"] ) )
        errorsDict["RequestProxy(%s)" % proxyURL] = setRequestProxy["Message"]
    # # if we're here neither requestManager nor requestProxy were successful
    self.log.error( "putRequest: unable to set request '%s'" % request.RequestName )
    errorsDict["Message"] = "ReqClient.putRequest: unable to set request '%s'" % request.RequestName
    return errorsDict

  def getRequest( self, requestName = "" ):
    """ get request from RequestDB

    :param self: self reference
    :param str requestType: type of request

    :return: S_OK( Request instance ) or S_OK() or S_ERROR
    """
    self.log.debug( "getRequest: attempting to get request." )
    getRequest = self.requestManager().getRequest( requestName )
    if not getRequest["OK"]:
      self.log.error( "getRequest: unable to get '%s' request: %s" % ( requestName, getRequest["Message"] ) )
      return getRequest
    if not getRequest["Value"]:
      return getRequest
    return S_OK( Request( getRequest["Value"] ) )

  def peekRequest( self, requestName ):
    """ peek request """
    self.log.debug( "peekRequest: attempting to get request." )
    peekRequest = self.requestManager().peekRequest( requestName )
    if not peekRequest["OK"]:
      self.log.error( "peekRequest: unable to peek '%s' request: %s" % ( requestName, peekRequest["Message"] ) )
      return peekRequest
    if not peekRequest["Value"]:
      return peekRequest
    return S_OK( Request( peekRequest["Value"] ) )

  def deleteRequest( self, requestName ):
    """ delete request given it's name

    :param self: self reference
    :param str requestName: request name
    """
    self.log.debug( "deleteRequest: attempt to delete '%s' request" % requestName )
    deleteRequest = self.requestManager().deleteRequest( requestName )
    if not deleteRequest["OK"]:
      self.log.error( "deleteRequest: unable to delete '%s' request: %s" % ( requestName,
                                                                             deleteRequest["Message"] ) )
    return deleteRequest

  def getRequestNamesList( self, statusList = None, limit = None ):
    """ get at most :limit: request names with statuses in :statusList: """
    statusList = statusList if statusList else list( Request.FINAL_STATES )
    limit = limit if limit else 100
    return self.requestManager().getRequestNamesList( statusList, limit )

  def getScheduledRequest( self, operationID ):
    """ get scheduled request given its scheduled OperationID """
    self.log.debug( "getScheduledRequest: attempt to get scheduled request..." )
    scheduled = self.requestManager().getScheduledRequest( operationID )
    if not scheduled["OK"]:
      self.log.error( "getScheduledRequest: %s" % scheduled["Message"] )
      return scheduled
    if scheduled["Value"]:
      return S_OK( Request( scheduled["Value"] ) )
    return scheduled

  def getDBSummary( self ):
    """ Get the summary of requests in the RequestDBs. """
    self.log.debug( "getDBSummary: attempting to get RequestDB summary." )
    dbSummary = self.requestManager().getDBSummary()
    if not dbSummary["OK"]:
      self.log.error( "getDBSummary: unable to get RequestDB summary: %s" % dbSummary["Message"] )
    return dbSummary

  def getDigest( self, requestName ):
    """ Get the request digest given a request name.

    :param self: self reference
    :param str requestName: request name
    """
    self.log.debug( "getDigest: attempting to get digest for '%s' request." % requestName )
    digest = self.requestManager().getDigest( requestName )
    if not digest["OK"]:
      self.log.error( "getDigest: unable to get digest for '%s' request: %s" % ( requestName, digest["Message"] ) )
    return digest

  def getRequestStatus( self, requestName ):
    """ Get the request status given a request name.

    :param self: self reference
    :param str requestName: name of teh request
    """
    self.log.debug( "getRequestStatus: attempting to get status for '%s' request." % requestName )
    requestStatus = self.requestManager().getRequestStatus( requestName )
    if not requestStatus["OK"]:
      self.log.error( "getRequestStatus: unable to get status for '%s' request: %s" % ( requestName,
                                                                                        requestStatus["Message"] ) )
    return requestStatus

  def getRequestInfo( self, requestName ):
    """ The the request info given a request name.

    :param self: self reference
    :param str requestName: request name
    """
    self.log.debug( "getRequestInfo: attempting to get info for '%s' request." % requestName )
    requestInfo = self.requestManager().getRequestInfo( requestName )
    if not requestInfo["OK"]:
      self.log.error( "getRequestInfo: unable to get status for '%s' request: %s" % ( requestName,
                                                                                      requestInfo["Message"] ) )
    return requestInfo

  def getRequestFileStatus( self, requestName, lfns ):
    """ Get file status for request given a request name.

    :param self: self reference
    :param str requestName: request name
    :param list lfns: list of LFNs
    """
    self.log.debug( "getRequestFileStatus: attempting to get file statuses for '%s' request." % requestName )
    fileStatus = self.requestManager().getRequestFileStatus( requestName, lfns )
    if not fileStatus["OK"]:
      self.log.error( "getRequestFileStatus: unable to get file status for '%s' request: %s" % \
                        ( requestName, fileStatus["Message"] ) )
    return fileStatus

  def finalizeRequest( self, requestName, jobID ):
    """ check request status and perform finalization if necessary

    :param self: self reference
    :param str requestName: request name
    :param int jobID: job id
    """
    stateServer = RPCClient( "WorkloadManagement/JobStateUpdate", useCertificates = True )
    # update the request status and the corresponding job parameter
    res = self.getRequestStatus( requestName )
    if res["OK"]:
      subRequestStatus = res["Value"]["SubRequestStatus"]
      if subRequestStatus == "Done":
        res = self.setRequestStatus( requestName, "Done" )
        if not res["OK"]:
          self.log.error( "finalizeRequest: Failed to set request status" )
        # the request is completed, update the corresponding job status
        if jobID:
          monitorServer = RPCClient( "WorkloadManagement/JobMonitoring", useCertificates = True )
          res = monitorServer.getJobPrimarySummary( int( jobID ) )
          if not res["OK"] or not res["Value"]:
            self.log.error( "finalizeRequest: Failed to get job status" )
          else:
            jobStatus = res["Value"]["Status"]
            jobMinorStatus = res["Value"]["MinorStatus"]
            if jobMinorStatus == "Pending Requests":
              if jobStatus == "Completed":
                self.log.info( "finalizeRequest: Updating job status for %d to Done/Requests done" % jobID )
                res = stateServer.setJobStatus( jobID, "Done", "Requests done", "" )
                if not res["OK"]:
                  self.log.error( "finalizeRequest: Failed to set job status" )
              elif jobStatus == "Failed":
                self.log.info( "finalizeRequest: Updating job minor status for %d to Requests done" % jobID )
                res = stateServer.setJobStatus( jobID, "", "Requests done", "" )
                if not res["OK"]:
                  self.log.error( "finalizeRequest: Failed to set job status" )
    else:
      self.log.error( "finalizeRequest: failed to get request status: %s" % res["Message"] )

    # update the job pending request digest in any case since it is modified
    self.log.info( "finalizeRequest: Updating request digest for job %d" % jobID )
    digest = self.getDigest( requestName )
    if digest["OK"]:
      digest = digest["Value"]
      self.log.verbose( digest )
      res = stateServer.setJobParameter( jobID, "PendingRequest", digest )
      if not res["OK"]:
        self.log.error( "finalizeRequest: Failed to set job parameter: %s" % res["Message"] )
    else:
      self.log.error( "finalizeRequest: Failed to get request digest for %s: %s" % ( requestName,
                                                                                     digest["Message"] ) )

    return S_OK()

  def getRequestNamesForJobs( self, jobIDs ):
    """ get the request names for the supplied jobIDs.

    :param self: self reference
    :param list jobID: list of job IDs (integers)
    """
    self.log.info( "getRequestNamesForJobs: attempt to get request(s) for job %s" % jobIDs )
    requests = self.requestManager().getRequestNamesForJobs( jobIDs )
    if not requests["OK"]:
      self.log.error( "getRequestNamesForJobs: unable to get request(s) for jobs %s: %s" % ( jobIDs,
                                                                                             requests["Message"] ) )
    return requests

  def readRequestsForJobs( self, jobIDs ):
    """ read requests for jobs

    :param list jobIDs: list with jobIDs

    :return: S_OK( { "Successful" : { jobID1 : Request, ... },
                     "Failed" : { jobIDn : "Fail reason" } } )
    """
    readReqsForJobs = self.requestManager().readRequestsForJobs( jobIDs )
    if not readReqsForJobs["OK"]:
      return readReqsForJobs
    ret = readReqsForJobs["Value"] if readReqsForJobs["Value"] else None
    if not ret:
      return S_ERROR( "No values returned" )
    # # create Requests out of JSONs for successful reads
    if "Successful" in ret:
      for jobID, fromJSON in ret["Successful"].items():
        req = Request( fromJSON )
        if not req["OK"]:
          ret["Failed"][jobID] = req["Message"]
          continue
        req = req["Value"]
        ret["Successful"][jobID] = req
    return S_OK( ret )
