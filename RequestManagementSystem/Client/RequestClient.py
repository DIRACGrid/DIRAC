########################################################################################
# $HeadURL$
########################################################################################
"""
    :mod:  RequestClient
    ====================

    .. module:  RequestClient
    :synopsis: implementation of client for RequestDB using DISET framework
    :deprecated:
"""
# # RCSID
__RCSID__ = "$Id$"
# # from DIRAC
from DIRAC import gLogger, S_OK, S_ERROR
from DIRAC.Core.DISET.RPCClient import RPCClient
from DIRAC.Core.Utilities.List import randomize, fromChar
from DIRAC.ConfigurationSystem.Client import PathFinder
from DIRAC.Core.Base.Client import Client
from DIRAC.RequestManagementSystem.Client.RequestContainer import RequestContainer

class RequestClient( Client ):
  """
  .. class:: RequestClient

  RequestClient is a class manipulating and operation on Requests.

  :param RPCClient requestManager: RPC client to RequestManager
  :param dict requestProxiesDict: RPC client to ReqestProxy
  """
  __requestManager = None
  __requestProxiesDict = {}

  def __init__( self, **kwargs ):
    """c'tor

    :param self: self reference
    :param bool useCertificates: flag to enable/disable certificates
    """
    Client.__init__( self, **kwargs )
    self.log = gLogger.getSubLogger( "RequestManagement/RequestClient" )
    self.setServer( "RequestManagement/RequestManager" )

  def requestManager( self, timeout = 120 ):
    """ facade for RequestManager RPC client """
    if not self.__requestManager:
      url = PathFinder.getServiceURL( "RequestManagement/RequestManager" )
      if not url:
        raise RuntimeError( "CS option RequestManagement/RequestManager URL is not set!" )
      self.__requestManager = RPCClient( url, timeout = timeout )
    return self.__requestManager

  def requestProxies( self, timeout = 120 ):
    """ get request proxies dict """
    if not self.__requestProxiesDict:
      self.__requestProxiesDict = {}
      proxiesURLs = fromChar( PathFinder.getServiceURL( "RequestManagement/RequestProxyURLs" ) )
      if not proxiesURLs:
        self.log.warn( "CS option RequestManagement/RequestProxyURLs is not set!" )
      for proxyURL in randomize( proxiesURLs ):
        self.log.debug( "creating RequestProxy for url = %s" % proxyURL )
        self.__requestProxiesDict[proxyURL] = RPCClient( proxyURL, timeout = timeout )
    return self.__requestProxiesDict

  ########################################################################
  #
  # These are the methods operating on existing requests and have fixed URLs
  #

  def updateRequest( self, requestName, requestString ):
    """ update the request

    :param self: self reference
    :param str requestName: request name
    :param str requestString: xml string
    """
    self.log.debug( "updateRequest: attempt to update '%s' request" % requestName )
    return self.requestManager().updateRequest( requestName, requestString )

  def deleteRequest( self, requestName ):
    """ delete the request

    :param self: self reference
    :param str requestName: request name
    """
    self.log.debug( "deleteRequest: attempt to delete '%s' request" % requestName )
    return self.requestManager().deleteRequest( requestName )

  def setRequestStatus( self, requestName, requestStatus ):
    """ Set the status of a request. If url parameter is not present, the central
    request RPC client would be used.

    :param self: self reference
    :param str requestName: request name
    :param str requestStatus: new status
    """
    self.log.debug( "setRequestStatus: attempt to set '%s' status for '%s' request" % ( requestStatus, requestName ) )
    return self.requestManager().setRequestStatus( requestName, requestStatus )

  def getRequestForJobs( self, jobID ):
    """ Get the request names for the supplied jobIDs.

    :param self: self reference
    :param list jobID: list of job IDs (integers)
    """
    self.log.debug( "getRequestForJobs: attempt to get request(s) for job %s" % jobID )
    return self.requestManager().getRequestForJobs( jobID )

  def setRequest( self, requestName, requestString ):
    """ set request to RequestManager

    :param self: self reference
    :param str requestName: request name
    :param str requestString: xml string represenation of request
    """
    errorsDict = {}
    setRequestMgr = self.requestManager().setRequest( requestName, requestString )
    if setRequestMgr["OK"]:
      return setRequestMgr
    errorsDict["RequestManager"] = setRequestMgr["Message"]
    self.log.warn( "setRequest: unable to set request '%s' at RequestManager" % requestName )
    proxies = self.requestProxies()
    for proxyURL, proxyClient in proxies.items():
      self.log.debug( "setRequest: trying RequestProxy at %s" % proxyURL )
      setRequestProxy = proxyClient.setRequest( requestName, requestString )
      if setRequestProxy["OK"]:
        if setRequestProxy["Value"]["set"]:
          self.log.debug( "setRequest: request '%s' successfully set using RequestProxy %s" % ( requestName,
                                                                                               proxyURL ) )
        elif setRequestProxy["Value"]["saved"]:
          self.log.debug( "setRequest: request '%s' successfully forwarded to RequestProxy %s" % ( requestName,
                                                                                                  proxyURL ) )
        return setRequestProxy
      else:
        self.log.warn( "setRequest: unable to set request using RequestProxy %s: %s" % ( proxyURL,
                                                                                         setRequestProxy["Message"] ) )
        errorsDict["RequestProxy(%s)" % proxyURL] = setRequestProxy["Message"]
    # # if we're here neither requestManager nor requestProxy were successfull
    self.log.error( "setRequest: unable to set request '%s'" % requestName )
    errorsDict["OK"] = False
    errorsDict["Message"] = "RequestClient.setRequest: unable to set request '%s'"
    return errorsDict

  def getRequest( self, requestType ):
    """ get request from RequestDB

    :param self: self reference
    :param str requestType: type of request
    """
    self.log.debug( "getRequest: attempting to get '%s' request." % requestType )
    return self.requestManager().getRequest( requestType )

  def serveRequest( self, requestType = "" ):
    """ Get the request of type :requestType: from RequestDB.

    :param self: self reference
    :param str requestType: request type
    """
    return self.getRequest( requestType )

  def getDBSummary( self ):
    """ Get the summary of requests in the RequestDBs. """
    self.log.debug( "getDBSummary: attempting to get RequestDB summary." )
    return self.requestManager().getDBSummary()

  def getDigest( self, requestName ):
    """ Get the request digest given a request name.

    :param self: self reference
    :param str requestName: request name
    """
    self.log.debug( "getDigest: attempting to get digest for '%s' request." % requestName )
    return self.requestManager().getDigest( requestName )

  def getCurrentExecutionOrder( self, requestName ):
    """ Get the request execution order given a request name.

    :param self: self reference
    :param str requestName: name of the request
    """
    self.log.debug( "getCurrentExecutionOrder: attempt to get execution order for '%s' request." % requestName )
    return self.requestManager().getCurrentExecutionOrder( requestName )

  def getRequestStatus( self, requestName ):
    """ Get the request status given a request name.

    :param self: self reference
    :param str requestName: name of teh request
    """
    self.log.debug( "getRequestStatus: attempting to get status for '%s' request." % requestName )
    return self.requestManager().getRequestStatus( requestName )

  def getRequestInfo( self, requestName ):
    """ The the request info given a request name.

    :param self: self reference
    :param str requestName: request name
    """
    self.log.debug( "getRequestInfo: attempting to get info for '%s' request." % requestName )
    return self.requestManager().getRequestInfo( requestName )

  def getRequestFileStatus( self, requestName, lfns ):
    """ Get fiel status for request given a request name.

    :param self: self reference
    :param str requestName: request name
    :param list lfns: list of LFNs
    """
    self.log.debug( "getRequestFileStatus: attempting to get file statuses for '%s' request." % requestName )
    return self.requestManager().getRequestFileStatus( requestName, lfns )

  def finalizeRequest( self, requestName, jobID ):
    """ check request status and perform finalisation if necessary

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
                self.log.debug( "finalizeRequest: Updating job status for %d to Done/Requests done" % jobID )
                res = stateServer.setJobStatus( jobID, "Done", "Requests done", "" )
                if not res["OK"]:
                  self.log.error( "finalizeRequest: Failed to set job status" )
              elif jobStatus == "Failed":
                self.log.debug( "finalizeRequest: Updating job minor status for %d to Requests done" % jobID )
                res = stateServer.setJobStatus( jobID, "", "Requests done", "" )
                if not res["OK"]:
                  self.log.error( "finalizeRequest: Failed to set job status" )
    else:
      self.log.error( "finalizeRequest: failed to get request status: %s" % res["Message"] )

    # update the job pending request digest in any case since it is modified
    self.log.debug( "finalizeRequest: Updating request digest for job %d" % jobID )
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

  def readRequestsForJobs( self, jobIDs ):
    """ read requests for jobs

    :param list jobIDs: list with jobIDs

    :return: S_OK( { "Successful" : { jobID1 : RequestContainer, ... },
                     "Failed" : { jobIDn : "Fail reason" } } )
    """
    readReqsForJobs = self.requestManager().readRequestsForJobs( jobIDs )
    if not readReqsForJobs["OK"]:
      return readReqsForJobs
    ret = readReqsForJobs["Value"] if readReqsForJobs["Value"] else None
    if not ret:
      return S_ERROR( "No values returned" )
    # # create RequestContainers out of xml strings for successful reads
    if "Successful" in ret:
      for jobID, xmlStr in ret["Successful"].items():
        req = RequestContainer( init = False )
        req.parseRequest( request = xmlStr )
        ret["Successful"][jobID] = req
    return S_OK( ret )
