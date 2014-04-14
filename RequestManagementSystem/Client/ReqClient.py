"""
    :mod:  ReqClient
    ================

    .. module:  ReqClient
    :synopsis: implementation of client for RequestDB using DISET framework
"""
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
    # # dump to json
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
    try:
      requestName = int( requestName )
    except ValueError:
      pass
    if type( requestName ) == int:
      res = self.getRequestName( requestName )
      if not res['OK']:
        return res
      else:
        requestName = res['Value']
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

  def getRequestName( self, requestID ):
    """ get request name for a given requestID """
    return self.requestManager().getRequestName( requestID )

  def getRequestInfo( self, requestName ):
    """ The the request info given a request id.

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
    """ Get file status for request given a request id.

    :param self: self reference
    :param int requestID: request name
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
        update the request status and the corresponding job parameter

    :param self: self reference
    :param str requestName: request name
    :param int jobID: job id
    """
    stateServer = RPCClient( "WorkloadManagement/JobStateUpdate", useCertificates = True )

    # update the job pending request digest in any case since it is modified
    self.log.info( "finalizeRequest: Updating request digest for job %d" % jobID )
    digest = self.getDigest( requestName )
    if digest["OK"]:
      digest = digest["Value"]
      self.log.verbose( digest )
      res = stateServer.setJobParameter( jobID, "PendingRequest", digest )
      if not res["OK"]:
        self.log.error( "finalizeRequest: Failed to set job %d parameter: %s" % ( jobID, res["Message"] ) )
        return res
    else:
      self.log.error( "finalizeRequest: Failed to get request digest for %s: %s" % ( requestName,
                                                                                     digest["Message"] ) )

    # Checking if to update the job status - we should fail here, so it will be re-tried later
    # Checking the state, first
    res = self.getRequestStatus( requestName )
    if not res['OK']:
      self.log.error( "finalizeRequest: failed to get request %s status: %s" % ( requestName, res["Message"] ) )
      return res
    if res["Value"] != "Done":
      return S_ERROR( "The request %s isn't 'Done' but '%s', this should never happen, why are we here?" % ( requestName, res['Value'] ) )

    # The request is 'Done', let's update the job status. If we fail, we should re-try later
    monitorServer = RPCClient( "WorkloadManagement/JobMonitoring", useCertificates = True )
    res = monitorServer.getJobPrimarySummary( int( jobID ) )
    if not res["OK"]:
      self.log.error( "finalizeRequest: Failed to get job %d status" % jobID )
      return S_ERROR( "finalizeRequest: Failed to get job %d status" % jobID )
    elif not res['Value']:
      self.log.info( "finalizeRequest: job %d does not exist (anymore): finalizing" % jobID )
      return S_OK()
    else:
      jobStatus = res["Value"]["Status"]
      jobMinorStatus = res["Value"]["MinorStatus"]

      stateUpdate = None
      if jobStatus == 'Completed':
        # What to do? Depends on what we have in the minorStatus
        if jobMinorStatus == "Pending Requests":
          self.log.info( "finalizeRequest: Updating job status for %d to Done/Requests done" % jobID )
          stateUpdate = stateServer.setJobStatus( jobID, "Done", "Requests done", "" )

        elif jobMinorStatus == "Application Finished With Errors":
          self.log.info( "finalizeRequest: Updating job status for %d to Failed/Requests done" % jobID )
          stateUpdate = stateServer.setJobStatus( jobID, "Failed", "Requests done", "" )

      if not stateUpdate:
        self.log.info( "finalizeRequest: Updating job minor status for %d to Requests done" % jobID )
        stateUpdate = stateServer.setJobStatus( jobID, "", "Requests done", "" )

      if not stateUpdate["OK"]:
        self.log.error( "finalizeRequest: Failed to set job %d status: %s" % ( jobID, stateUpdate['Message'] ) )
        return stateUpdate

    return S_OK()

  def getRequestNamesForJobs( self, jobIDs ):
    """ get the request names for the supplied jobIDs.

    :param self: self reference
    :param list jobID: list of job IDs (integers)
    :return: S_ERROR or S_OK( "Successful": { jobID1: requestName1, jobID2: requestName2, ... },
                              "Failed" : { jobIDn: errMsg, jobIDm: errMsg, ...}  )
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
    ret = readReqsForJobs["Value"]
    # # create Requests out of JSONs for successful reads
    if "Successful" in ret:
      for jobID, fromJSON in ret["Successful"].items():
        ret["Successful"][jobID] = Request( fromJSON )
    return S_OK( ret )

  def resetFailedRequest( self, requestName, all = False ):
    """ Reset a failed request to "Waiting" status - requestName can also be the RequestID
    """
    try:
      requestName = self.getRequestName( int( requestName ) )['Value']
    except ValueError:
      pass

    # # we can safely only peek the request as it is Failed and therefore not owned by an agent
    res = self.peekRequest( requestName )
    if not res['OK']:
      return res
    req = res['Value']
    if all or recoverableRequest( req ):
      # Only reset requests that can be recovered
      for i, op in enumerate( req ):
        op.Error = ' '
        if op.Status == 'Failed':
          printOperation( ( i, op ), onlyFailed = True )
        for f in op:
          if f.Status == 'Failed':
            if 'Max attempts limit reached' in f.Error:
              f.Attempt = 1
            else:
              f.Attempt += 1
            f.Error = ' '
            f.Status = 'Waiting'
        if op.Status == 'Failed':
          op.Status = 'Waiting'

      return self.putRequest( req )
    return S_OK( "Not reset" )

#============= Some useful functions to be shared ===========

output = ''
def prettyPrint( mainItem, key = '', offset = 0 ):
  global output
  if key:
    key += ': '
  blanks = offset * ' '
  if mainItem and type( mainItem ) == type( {} ):
    output += "%s%s%s\n" % ( blanks, key, '{' ) if blanks or key else ''
    for key in sorted( mainItem ):
      prettyPrint( mainItem[key], key = key, offset = offset )
    output += "%s%s\n" % ( blanks, '}' ) if blanks else ''
  elif mainItem and type( mainItem ) == type( [] ):
    output += "%s%s%s\n" % ( blanks, key, '[' )
    for item in mainItem:
      prettyPrint( item, offset = offset + 2 )
    output += "%s%s\n" % ( blanks, ']' )
  elif type( mainItem ) == type( '' ):
    output += "%s%s'%s'\n" % ( blanks, key, str( mainItem ) )
  else:
    output += "%s%s%s\n" % ( blanks, key, str( mainItem ) )
  output = output.replace( '[\n%s{' % blanks, '[{' ).replace( '}\n%s]' % blanks, '}]' )

def printRequest( request, status = None, full = False, verbose = True, terse = False ):
  from DIRAC.DataManagementSystem.Client.FTSClient                                  import FTSClient
  global output
  ftsClient = FTSClient()
  if full:
    output = ''
    prettyPrint( request.toJSON()['Value'] )
    gLogger.always( output )
  else:
    if not status:
      status = request.Status
    gLogger.always( "Request name='%s' ID=%s Status='%s'%s%s%s" % ( request.RequestName,
                                                                     request.RequestID,
                                                                     request.Status, " ('%s' in DB)" % status if status != request.Status else '',
                                                                     ( " Error='%s'" % request.Error ) if request.Error and request.Error.strip() else "" ,
                                                                     ( " Job=%s" % request.JobID ) if request.JobID else "" ) )
    if verbose:
      gLogger.always( "Created %s, Updated %s" % ( request.CreationTime, request.LastUpdate ) )
      if request.OwnerDN:
        gLogger.always( "Owner: '%s', Group: %s" % ( request.OwnerDN, request.OwnerGroup ) )
    for indexOperation in enumerate( request ):
      op = indexOperation[1]
      if not terse or op.Status == 'Failed':
        printOperation( indexOperation, verbose, onlyFailed = terse )
  # Check if FTS job exists
  res = ftsClient.getFTSJobsForRequest( request.RequestID )
  if res['OK']:
    ftsJobs = res['Value']
    if ftsJobs:
      gLogger.always( '         FTS jobs associated: %s' % ','.join( ['%s (%s)' % ( job.FTSGUID, job.Status ) \
                                                               for job in ftsJobs] ) )

def printOperation( indexOperation, verbose = True, onlyFailed = False ):
  i, op = indexOperation
  prStr = ''
  if 'Replicate' in op.Type:
    anyReplication = True
  if verbose:
    if op.SourceSE:
      prStr += 'SourceSE: %s' % op.SourceSE
    if op.TargetSE:
      prStr += ( ' - ' if prStr else '' ) + 'TargetSE: %s' % op.TargetSE
    if prStr:
      prStr += ' - '
    prStr += 'Created %s, Updated %s' % ( op.CreationTime, op.LastUpdate )
  gLogger.always( "  [%s] Operation Type='%s' ID=%s Order=%s Status='%s'%s%s" % ( i, op.Type, op.OperationID,
                                                                                       op.Order, op.Status,
                                                                                       ( " Error='%s'" % op.Error ) if op.Error and op.Error.strip() else "",
                                                                                       ( " Catalog=%s" % op.Catalog ) if op.Catalog else "" ) )
  if prStr:
    gLogger.always( "      %s" % prStr )
  for indexFile in enumerate( op ):
    if not onlyFailed or indexFile[1].Status == 'Failed':
      printFile( indexFile )

def printFile( indexFile ):
  j, f = indexFile
  gLogger.always( "    [%02d] ID=%s LFN='%s' Status='%s'%s%s" % ( j + 1, f.FileID, f.LFN, f.Status,
                                                                       ( " Error='%s'" % f.Error ) if f.Error and f.Error.strip() else "",
                                                                       ( " Attempts=%d" % f.Attempt ) if f.Attempt > 1 else "" ) )

def recoverableRequest( request ):
  excludedErrors = ( 'File does not exist', 'No such file or directory',
                     'sourceSURL equals to targetSURL',
                     'Max attempts limit reached', 'Max attempts reached' )
  for op in request:
    if op.Status == 'Failed':
      for f in op:
        if f.Status == 'Failed':
          if [str for str in excludedErrors if str in f.Error]:
            return False
          return True
