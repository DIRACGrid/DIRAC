########################################################################################
# $HeadURL$
########################################################################################

""" This is the client implementation for the RequestDB using the DISET framework. """

__RCSID__ = "$Id$"

## imports
from DIRAC import gLogger, S_OK, S_ERROR
from DIRAC.Core.DISET.RPCClient import RPCClient
from DIRAC.Core.Utilities.List import randomize, fromChar
from DIRAC.ConfigurationSystem.Client import PathFinder
from DIRAC.Core.Base.Client import Client

class RequestClient( Client ):

  """ RequestClient is a class manipulating and operation on Requests. """

  def __init__( self, useCertificates = False ):
    """c'tor

    :param self: self reference
    :param bool useCertificates: flag to enable/disable certificates
    """
    Client.__init__( self )
    ## setup logger
    self.log = gLogger.getSubLogger( "RequestManagement/RequestClient" ) 

    ## dict to store all RPC clients for easy reuse
    self.__requestRPCClientsDict = {}
    ## local if any defined
    local = PathFinder.getServiceURL( "RequestManagement/localURL" )
    if local:
      self.__requestRPCClientsDict.setdefault( "local" , [ self.__requestRPCClient( local ) ] )
    ## central if any defined
    central = PathFinder.getServiceURL( "RequestManagement/centralURL" )
    if central:
      self.__requestRPCClientsDict.setdefault( "central", [ self.__requestRPCClient( central ) ] )
    ## voboxes if any defined
    voBoxUrls = fromChar( PathFinder.getServiceURL( "RequestManagement/voBoxURLs" ) )
    if voBoxUrls:
      self.__requestRPCClientsDict.setdefault( "voboxes", [] ) 
      for voBoxURL in randomize( voBoxUrls ):
        self.__requestRPCClientsDict["voboxes"].append( self.__requestRPCClient( voBoxURL ) )

    self.setServer( 'RequestManagement/centralURL' )

  def __requestRPCClient( self, url, timeout = 120 ):
    """ Create and return request RPC client for a given URL.

    :param self: self reference
    :param str url: request RPC client URL
    :param int timeout: time out for single RPC request in sec
    """
    rpcClient = RPCClient( url, timeout = timeout )
    rpcClient.thisURL = url
    return rpcClient

  def __getClientsList( self, url = "", servicePriority = [], timeout = 120 ):
    """Lazy creation of a valid list of requested RPC clients. 

    :param self: self reference
    :param str url: URL for newly created request RPC client 
    :param list serviceTypes: ordered list of client types, that should be used, if url is not present
    :param int timeout: RPC request timeout in sec
    """
    clients = []
    ## first create a new one, if url is present
    if url:
      clients.append( self.__requestRPCClient( url, timeout ) )
    ## loop over servicePriority and append clients from self.__requestRPCClientsDict
    for serviceType in servicePriority:
      if serviceType in self.__requestRPCClientsDict:
        clients += self.__requestRPCClientsDict[ serviceType ]
    return clients

  def requestRPCClients( self, url = "", servicePriority = [] ):
    """Generator of request RPC clients list.

    :param self: self reference
    :param str url: URL for new RPC client
    :param list servicePriority: a list of service types that should be used as replacement, if 
    url parameter is empty.

    If url is present, a new RPC client would be created and returned. In absence of url, an ordered 
    list :servicePriority: would be used to indicate which RPC clients should be asked to perform 
    requested operation. Here is an example::

      for rcpClient in self.requestRPCClients( url = "", servicePriority = [ "central", "local" ] )
        print rcpClient.thisURL
        
    should print only central and local RPC client URLs, as supplied url is empty, while in this case::

      for rcpClient in self.requestRPCClients( url = "dips://localhost:9999/RequestManagement/RequestManager" )
        print rcpClient.thisURL

    only string "dips://localhost:9999/RequestManagement/RequestManager" would be printed.
    """
    for client in self.__getClientsList( url, servicePriority ):
      yield client
    

  ########################################################################
  #
  # These are the methods operating on existing requests and have fixed URLs
  #

  def updateRequest( self, requestName, requestString, url = "" ):
    """ Update the request at the supplied URL. If URL is not present the central  
    request RPC client will be used.

    :param self: self reference
    :param str requestName: request name
    :param str requestString: xml string 
    :param str url: URL for request RPC client
    """
    try:
      for requestRPCClient in self.requestRPCClients( url, ["central"] ): 
        thisURL = requestRPCClient.thisURL
        self.log.verbose( "RequestClient.updateRequest: Attempting to update %s at %s." % ( requestName, 
                                                                                            thisURL ) )
        return requestRPCClient.updateRequest( requestName, requestString )
    except Exception, error:
      errMsg = "RequestClient.updateRequest: Exception while updating request: %s" % str(error)
      self.log.exception( errMsg )
      return S_ERROR( errMsg )
    ## if we are there, no request RPC clients were defined 
    return S_ERROR("RequestClient.updateRequest: Failed to update request, no RPC clients.")


  def deleteRequest( self, requestName, url = "" ):
    """ Delete the request at the supplied url. If parameter url is not present, the central 
    request RPC client will be used.

    :param self: self reference
    :param str requestName: request name
    :param str url: request RPC client URL 
    """
    try:
      for requestRPCClient in self.requestRPCClients( url, ["central"] ): 
        thisURL = requestRPCClient.thisURL
        self.log.verbose( "RequestClient.deleteRequest: Attempting to delete %s at %s." % ( requestName, 
                                                                                            thisURL ) )
        return requestRPCClient.deleteRequest( requestName )
    except Exception, error:
      errMsg = "RequestClient.deleteRequest: Exception while deleting request: %s" % str(error)
      self.log.exception( errMsg ) 
      return S_ERROR( errMsg )
    ## if we are there, no request RPC clients were defined 
    return S_ERROR("RequestClient.deleteRequest: Failed to delete the request, no RPC clients.")

  def setRequestStatus( self, requestName, requestStatus, url = "" ):
    """ Set the status of a request. If url parameter is not present, the central 
    request RPC client would be used.
    
    :param self: self reference
    :param str requestName: request name
    :param str requestStatus: new status
    :param str url: request RPC client URL
    """
    try:
      for requestRPCClient in self.requestRPCClients( url, ["central"] ):
        thisURL = requestRPCClient.thisURL
        self.log.verbose( "RequestClient.setRequestStatus: Attempting to set %s to %s at %s." % ( requestName, 
                                                                                                  requestStatus,
                                                                                                  thisURL ) )
        return requestRPCClient.setRequestStatus( requestName, requestStatus )
    except Exception, error:
      errMsg = "RequestClient.setRequestStatus: Exception while setting request status: %s" % str(error)
      self.log.exception( errMsg )
      return S_ERROR( errMsg )
    ## if we are there, no request RPC clients were defined 
    return S_ERROR("RequestClient.setRequestStatus: Failed to set the request status, no RPC clients.")


  def getRequestForJobs( self, jobID, url = "" ):
    """ Get the request names for the supplied jobIDs. If url parameter is not present, the central 
    request RPC client would be used.

    :param self: self reference
    :param list jobID: list of job IDs (integers)
    :param str url: request RPC client URL
    """
    try:
      for requestRPCClient in self.requestRPCClients( url, ["central"] ):
        self.log.verbose( "RequestClient.getRequestForJobs: Attempt to get request names for %d jobs." % len(jobID) )
        return requestRPCClient.getRequestForJobs( jobID )
    except Exception, error:
      errMsg = "RequestClient.getRequestForJobs: Exception while getting request names: %s" % str(error)
      self.log.exception( errMsg )
      return S_ERROR( errMsg )
    ## if we are there, no request RPC clients were defined 
    return S_ERROR("RequestClient.getRequestForJobs: Failed to get request for jobs, no RPC clients.")


  ##############################################################################
  #
  # These are the methods which require URL manipulation
  #

  def setRequest( self, requestName, requestString, url = "" ):
    """ Set request. URL can be supplied and if it is not present, a central and thne VOBOXes 
    will be tried in random order.
    
    :param self: self reference
    :param str requestName: request name
    :param str requestString: xml string represenation of request
    :param str url: request RPC client URL
    """
    try:
      for requestRPCClient in self.requestRPCClients( url, ["central", "voboxes"] ):
        thisURL = requestRPCClient.thisURL
        res = requestRPCClient.setRequest( requestName, requestString )
        if res['OK']:
          self.log.info( "RequestClient.setRequest: request '%s' at %s set" % ( requestName, thisURL ) )
          res["Server"] = thisURL
          return res
        errMsg = "RequestClient.setRequest: failed setting request '%s' at %s: %s " % ( requestName, thisURL, res["Message"] )
        self.log.error( errMsg )
    except Exception, error:
      errMsg = "RequestClient.setRequest: Exception while setting request: %s" % str(error)
      self.log.exception( errMsg )
      return S_ERROR( errMsg )
    ## if we are there, no request RPC clients were defined 
    errMsg = "RequestClient.setRequest: Failed setting request '%s', no RPC clients." % requestName
    self.log.error( errMsg )
    return S_ERROR( errMsg )
    
  def getRequest( self, requestType, url = "" ):
    """ Get request from RequestDB. 
    First try client passed as parameter, then in order: local, central and at the end one of voboxes. 
    
    :param self: self reference
    :param str requestType: type of request
    :param str url: reuqest RPC client URL
    """
    try:
      for requestRPCClient in self.requestRPCClients( url, [ "local", "central", "voboxes"] ):
        thisURL = requestRPCClient.thisURL
        self.log.info( "RequestClient.getRequest: Attempting to get request.", "%s %s" % ( thisURL, 
                                                                                           requestType ) )
        res = requestRPCClient.getRequest( requestType )
        if res["OK"]:
          if not res["Value"]:
            self.log.info( "RequestClient.getRequest: found no '%s' requests on RequestDB (%s)" % ( requestType, thisURL ) )
          else:  
            self.log.info( "RequestClient.getRequest: got '%s' request from RequestDB (%s)" % ( requestType, thisURL ) )
            res['Value']['Server'] = thisURL
          return res
        else:
          self.log.error( "RequestClient.getRequest: failed getting request of type '%s' from %s: %s" % ( requestType, 
                                                                                                          thisURL, 
                                                                                                          res["Message"] ) )
    except Exception, error:
      errMsg = "RequestClient.getRequest: Exception while getting request: %s" % str(error)
      self.log.exception( errMsg )
      return S_ERROR( errMsg )
    ## if we are there, no request RPC clients were defined 
    errMsg = "RequestClient.getRequest: Failed fo get request of type '%s', no RPC clients." % requestType
    self.log.error( errMsg )
    return S_ERROR( errMsg )


  def serveRequest( self, requestType = "", url = "" ):
    """ Get the request of type :requestType: from RequestDB.   

    :param self: self reference
    :param str requestType: request type
    :param str url: request RPC client URL
    """
    return self.getRequest( requestType, url )

  def getDBSummary( self, url = "" ):
    """ Get the summary of requests in the RequestDBs. 
    If a URL is not supplied will get status for all.
    """
    urlDict = {}
    try:
      for requestRPCClient in self.requestRPCClients( url, [ "local", "central", "voboxes"] ):
        thisURL = requestRPCClient.thisURL
        urlDict[thisURL] = {}
        res = requestRPCClient.getDBSummary()
        if res["OK"]:
          self.log.info( "RequestClient.getDBSummary: Succeded getting request summary at %s" % thisURL)
          urlDict[url] = res["Value"]
        else:
          errMsg = "RequestClient.getDBSummary: Failed getting request summary at %s: %s" % ( thisURL, res["Message"] ) 
          self.log.error( errMsg )
    except Exception, error:
      errMsg = "RequestClient.getDBSummary: Exception while getting summary: %s" % str(error)
      self.log.exception( errMsg )
      return S_ERROR( errMsg )
    return S_OK( urlDict )

  def getDigest( self, requestName, url = "" ):
    """ Get the request digest given a request name.

    :param self: self reference
    :param str requestName: request name
    :param str url: request RPC client URL
    """
    for requestRPCClient in self.requestRPCClients( url, ["central"] ):
      return requestRPCClient.getDigest( requestName )
    return S_ERROR("RequestClient.getDigest: no RPC clients.")
    
  def getCurrentExecutionOrder( self, requestName, url = "" ):
    """ Get the request execution order given a request name.

    :param self: self reference
    :param str requestName: name of the request
    :param str url: request RPC client URL
    """
    for requestRPCClient in self.requestRPCClients( url, ["central"] ):
      return requestRPCClient.getCurrentExecutionOrder( requestName )
    return S_ERROR( "RequestClient.getCurrentExecutionOrder: no RPC clients." )
    
  def getRequestStatus( self, requestName, url = "" ):
    """ Get the request status given a request name.

    :param self: self reference
    :param str requestName: name of teh request
    :param str url: request RPC client URL
    """
    for requestRPCClient in self.requestRPCClients( url, ["central"] ):
      return requestRPCClient.getRequestStatus( requestName )
    return S_ERROR( "RequestClient.getRequestStatus: no RPC clients." )

  def getRequestInfo( self, requestName, url = "" ):
    """ The the request info given a request name. 

    :param self: self reference
    :param str requestName: request name
    :param str url: request RPC client URL
    """
    for requestRPCClient in self.requestRPCClients( url, ["central"] ):
      return requestRPCClient.getRequestInfo( requestName )
    return S_ERROR( "RequestClient.getRequestInfo: no RPC clients." )

  def getRequestFileStatus( self, requestName, lfns, url = "" ):
    """ Get fiel status for request given a request name.

    :param self: self reference
    :param str requestName: request name
    :param list lfns: list of LFNs
    :param str url: request RPC client URL
    """
    for requestRPCClient in self.requestRPCClients( url, ["central"] ):
      return requestRPCClient.getRequestFileStatus( requestName, lfns )
    return S_ERROR( "RequestClient.getRequestFileStatus: no RPC clients." )

  def finalizeRequest( self, requestName, jobID, url = "" ):
    """ check request status and perform finalisation if necessary

    :param self: self reference
    :param str requestName: request name
    :param int jobID: job id
    :param str url: request RPC client URL 
    """
    stateServer = RPCClient( "WorkloadManagement/JobStateUpdate", useCertificates = True )
    # update the request status and the corresponding job parameter
    res = self.getRequestStatus( requestName, url )
    if res["OK"]:
      subRequestStatus = res["Value"]["SubRequestStatus"]
      if subRequestStatus == "Done":
        res = self.setRequestStatus( requestName, "Done", url )
        if not res["OK"]:
          self.log.error( "RequestClient.finalizeRequest: Failed to set request status", url ) 
        # the request is completed, update the corresponding job status
        if jobID:
          monitorServer = RPCClient( "WorkloadManagement/JobMonitoring", useCertificates = True )
          res = monitorServer.getJobPrimarySummary( int( jobID ) )
          if not res["OK"] or not res["Value"]:
            self.log.error( "RequestClient.finalizeRequest: Failed to get job status" )
          else:
            jobStatus = res["Value"]["Status"]
            jobMinorStatus = res["Value"]["MinorStatus"]
            if jobMinorStatus == "Pending Requests":
              if jobStatus == "Completed":
                self.log.info( "RequestClient.finalizeRequest: Updating job status for %d to Done/Requests done" % jobID )
                res = stateServer.setJobStatus( jobID, "Done", "Requests done", "" )
                if not res["OK"]:
                  self.log.error( "finalizeRequest: Failed to set job status" )
              elif jobStatus == "Failed":
                self.log.info( "RequestClient.finalizeRequest: Updating job minor status for %d to Requests done" % jobID )
                res = stateServer.setJobStatus( jobID, "", "Requests done", "" )
                if not res["OK"]:
                  self.log.error( "RequestClient.finalizeRequest: Failed to set job status" )
    else:
      self.log.error( "RequestClient.finalizeRequest: failed to get request status at", url )

    # update the job pending request digest in any case since it is modified
    self.log.info( "RequestClient.finalizeRequest: Updating request digest for job %d" % jobID )
    digest = self.getDigest( requestName, url )
    if digest["OK"]:
      digest = digest["Value"]
      self.log.verbose( digest )
      res = stateServer.setJobParameter( jobID, "PendingRequest", digest )
      if not res["OK"]:
        self.log.error( "RequestClient.finalizeRequest: Failed to set job parameter" )
    else:
      self.log.error( "RequestClient.finalizeRequest: Failed to get request digest for %s: %s" % ( requestName, 
                                                                                                   digest["Message"] ) )

    return S_OK()
