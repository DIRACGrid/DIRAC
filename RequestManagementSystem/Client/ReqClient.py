"""
:mod:  ReqClient

.. module:  ReqClient
  :synopsis: implementation of client for RequestDB using DISET framework

"""

import os
import time
import random
import json
import datetime
from six import add_metaclass

# # from DIRAC
from DIRAC import gLogger, S_OK, S_ERROR
from DIRAC.Core.DISET.RPCClient import RPCClient
from DIRAC.Core.Utilities.List import randomize, fromChar
from DIRAC.ConfigurationSystem.Client import PathFinder
from DIRAC.Core.Base.Client import Client, ClientCreator
from DIRAC.RequestManagementSystem.Client.Request import Request
from DIRAC.RequestManagementSystem.private.RequestValidator import RequestValidator


@add_metaclass(ClientCreator)
class ReqClient(Client):
  """ReqClient is a class manipulating and operation on Requests.

  :param ~RPCClient.RPCClient requestManager: RPC client to RequestManager
  :param dict requestProxiesDict: RPC client to ReqestProxy
  :param ~DIRAC.RequestManagementSystem.private.RequestValidator.RequestValidator requestValidator: RequestValidator instance
  """
  handlerModuleName = 'DIRAC.RequestManagementSystem.Service.ReqManagerHandler'
  handlerClassName = 'ReqManagerHandler'
  __requestProxiesDict = {}
  __requestValidator = None

  def __init__(self, url=None, **kwargs):
    """c'tor

    :param self: self reference
    :param url: url of the ReqManager
    :param kwargs: forwarded to the Base Client class
    """

    super(ReqClient, self).__init__(**kwargs)
    self.serverURL = 'RequestManagement/ReqManager' if not url else url

    self.log = gLogger.getSubLogger("RequestManagement/ReqClient/pid_%s" % (os.getpid()))

  def requestProxies(self, timeout=120):
    """ get request proxies dict """
    if not self.__requestProxiesDict:
      self.__requestProxiesDict = {}
      proxiesURLs = fromChar(PathFinder.getServiceURL("RequestManagement/ReqProxyURLs"))
      if not proxiesURLs:
        self.log.warn("CS option RequestManagement/ReqProxyURLs is not set!")
      for proxyURL in proxiesURLs:
        self.log.debug("creating RequestProxy for url = %s" % proxyURL)
        self.__requestProxiesDict[proxyURL] = RPCClient(proxyURL, timeout=timeout)
    return self.__requestProxiesDict

  def requestValidator(self):
    """ get request validator """
    if not self.__requestValidator:
      self.__requestValidator = RequestValidator()
    return self.__requestValidator

  def putRequest(self, request, useFailoverProxy=True, retryMainService=0):
    """Put request to RequestManager

      :param self: self reference
      :param ~Request.Request request: Request instance
      :param bool useFailoverProxy: if False, will not attempt to forward the request to ReqProxies
      :param int retryMainService: Amount of time we retry on the main ReqHandler in case of failures

      :return: S_OK/S_ERROR
    """
    errorsDict = {"OK": False}
    valid = self.requestValidator().validate(request)
    if not valid["OK"]:
      self.log.error("putRequest: request not valid", "%s" % valid["Message"])
      return valid
    # # dump to json
    requestJSON = request.toJSON()
    if not requestJSON["OK"]:
      return requestJSON
    requestJSON = requestJSON["Value"]

    retryMainService += 1

    while retryMainService:
      retryMainService -= 1
      setRequestMgr = self._getRPC().putRequest(requestJSON)
      if setRequestMgr["OK"]:
        return setRequestMgr
      errorsDict["RequestManager"] = setRequestMgr["Message"]
      # sleep a bit
      time.sleep(random.randint(1, 5))

    self.log.warn("putRequest: unable to set request '%s' at RequestManager" %
                  request.RequestName, setRequestMgr["Message"])
    proxies = self.requestProxies() if useFailoverProxy else {}
    for proxyURL in randomize(proxies.keys()):
      proxyClient = proxies[proxyURL]
      self.log.debug("putRequest: trying RequestProxy at %s" % proxyURL)
      setRequestProxy = proxyClient.putRequest(requestJSON)
      if setRequestProxy["OK"]:
        if setRequestProxy["Value"]["set"]:
          self.log.info("putRequest: request '%s' successfully set using RequestProxy %s" % (request.RequestName,
                                                                                             proxyURL))
        elif setRequestProxy["Value"]["saved"]:
          self.log.info("putRequest: request '%s' successfully forwarded to RequestProxy %s" % (request.RequestName,
                                                                                                proxyURL))
        return setRequestProxy
      else:
        self.log.warn("putRequest: unable to set request using RequestProxy %s: %s" % (proxyURL,
                                                                                       setRequestProxy["Message"]))
        errorsDict["RequestProxy(%s)" % proxyURL] = setRequestProxy["Message"]
    # # if we're here neither requestManager nor requestProxy were successful
    self.log.error("putRequest: unable to set request", "'%s'" % request.RequestName)
    errorsDict["Message"] = "ReqClient.putRequest: unable to set request '%s'" % request.RequestName
    return errorsDict

  def getRequest(self, requestID=0):
    """Get request from RequestDB

      :param self: self reference
      :param int requestID: ID of the request. If 0, choice is made for you

      :return: S_OK( Request instance ) or S_OK() or S_ERROR
    """
    self.log.debug("getRequest: attempting to get request.")
    getRequest = self._getRPC().getRequest(requestID)
    if not getRequest["OK"]:
      self.log.error("getRequest: unable to get request", "'%s' %s" % (requestID, getRequest["Message"]))
      return getRequest
    if not getRequest["Value"]:
      return getRequest
    return S_OK(Request(getRequest["Value"]))

  def getBulkRequests(self, numberOfRequest=10, assigned=True):
    """ get bulk requests from RequestDB

    :param self: self reference
    :param str numberOfRequest: size of the bulk (default 10)

    :return: S_OK( Successful : { requestID, RequestInstance }, Failed : message  ) or S_ERROR
    """
    self.log.debug("getRequests: attempting to get request.")
    getRequests = self._getRPC().getBulkRequests(numberOfRequest, assigned)
    if not getRequests["OK"]:
      self.log.error("getRequests: unable to get '%s' requests: %s" % (numberOfRequest, getRequests["Message"]))
      return getRequests
    # No Request returned
    if not getRequests["Value"]:
      return getRequests
    # No successful Request
    if not getRequests["Value"]["Successful"]:
      return getRequests

    jsonReq = getRequests["Value"]["Successful"]
    reqInstances = dict((rId, Request(jsonReq[rId])) for rId in jsonReq)
    return S_OK({"Successful": reqInstances, "Failed": getRequests["Value"]["Failed"]})

  def peekRequest(self, requestID):
    """ peek request """
    self.log.debug("peekRequest: attempting to get request.")
    peekRequest = self._getRPC().peekRequest(int(requestID))
    if not peekRequest["OK"]:
      self.log.error("peekRequest: unable to peek request", "request: '%s' %s" % (requestID, peekRequest["Message"]))
      return peekRequest
    if not peekRequest["Value"]:
      return peekRequest
    return S_OK(Request(peekRequest["Value"]))

  def deleteRequest(self, requestID):
    """ delete request given it's ID

    :param self: self reference
    :param str requestID: request ID
    """
    requestID = int(requestID)
    self.log.debug("deleteRequest: attempt to delete '%s' request" % requestID)
    deleteRequest = self._getRPC().deleteRequest(requestID)
    if not deleteRequest["OK"]:
      self.log.error("deleteRequest: unable to delete request",
                     "'%s' request: %s" % (requestID, deleteRequest["Message"]))
    return deleteRequest

  def getRequestIDsList(self, statusList=None, limit=None, since=None, until=None, getJobID=False):
    """ get at most :limit: request ids with statuses in :statusList: """
    statusList = statusList if statusList else list(Request.FINAL_STATES)
    limit = limit if limit else 100
    since = since.strftime('%Y-%m-%d') if since else ""
    until = until.strftime('%Y-%m-%d') if until else ""

    return self._getRPC().getRequestIDsList(statusList, limit, since, until, getJobID)

  def getScheduledRequest(self, operationID):
    """ get scheduled request given its scheduled OperationID """
    self.log.debug("getScheduledRequest: attempt to get scheduled request...")
    scheduled = self._getRPC().getScheduledRequest(operationID)
    if not scheduled["OK"]:
      self.log.error("getScheduledRequest failed", scheduled["Message"])
      return scheduled
    if scheduled["Value"]:
      return S_OK(Request(scheduled["Value"]))
    return scheduled

  def getDBSummary(self):
    """ Get the summary of requests in the RequestDBs. """
    self.log.debug("getDBSummary: attempting to get RequestDB summary.")
    dbSummary = self._getRPC().getDBSummary()
    if not dbSummary["OK"]:
      self.log.error("getDBSummary: unable to get RequestDB summary", dbSummary["Message"])
    return dbSummary

  def getDigest(self, requestID):
    """ Get the request digest given a request ID.

    :param self: self reference
    :param str requestID: request id
    """
    self.log.debug("getDigest: attempting to get digest for '%s' request." % requestID)
    digest = self._getRPC().getDigest(int(requestID))
    if not digest["OK"]:
      self.log.error("getDigest: unable to get digest for request",
                     "request: '%s' %s" % (requestID, digest["Message"]))

    return digest

  def getRequestStatus(self, requestID):
    """ Get the request status given a request id.

    :param self: self reference
    :param int requestID: id of the request
    """
    if isinstance(requestID, basestring):
      requestID = int(requestID)
    self.log.debug("getRequestStatus: attempting to get status for '%d' request." % requestID)
    requestStatus = self._getRPC().getRequestStatus(requestID)
    if not requestStatus["OK"]:
      self.log.error("getRequestStatus: unable to get status for request",
                     ": '%d' %s" % (requestID, requestStatus["Message"]))
    return requestStatus

#   def getRequestName( self, requestID ):
#     """ get request name for a given requestID """
#     return self._getRPC().getRequestName( requestID )

  def getRequestInfo(self, requestID):
    """ The the request info given a request id.

    :param self: self reference
    :param int requestID: request nid
    """
    self.log.debug("getRequestInfo: attempting to get info for '%s' request." % requestID)
    requestInfo = self._getRPC().getRequestInfo(int(requestID))
    if not requestInfo["OK"]:
      self.log.error("getRequestInfo: unable to get status for request",
                     "request: '%s' %s" % (requestID, requestInfo["Message"]))
    return requestInfo

  def getRequestFileStatus(self, requestID, lfns):
    """ Get file status for request given a request id.

    :param self: self reference
    :param int requestID: request id
    :param lfns: list of LFNs
    :type lfns: python:list
    """
    self.log.debug("getRequestFileStatus: attempting to get file statuses for '%s' request." % requestID)
    fileStatus = self._getRPC().getRequestFileStatus(int(requestID), lfns)
    if not fileStatus["OK"]:
      self.log.verbose("getRequestFileStatus: unable to get file status for request",
                       "request: '%s' %s" % (requestID, fileStatus["Message"]))
    return fileStatus

  def finalizeRequest(self, requestID, jobID, useCertificates=True):
    """ check request status and perform finalization if necessary
        update the request status and the corresponding job parameter

    :param self: self reference
    :param str requestID: request id
    :param int jobID: job id
    """
    stateServer = RPCClient("WorkloadManagement/JobStateUpdate", useCertificates=useCertificates)

    # Checking if to update the job status - we should fail here, so it will be re-tried later
    # Checking the state, first
    res = self.getRequestStatus(requestID)
    if not res['OK']:
      self.log.error("finalizeRequest: failed to get request",
                     "request: %s status: %s" % (requestID, res["Message"]))
      return res
    if res["Value"] != "Done":
      return S_ERROR("The request %s isn't 'Done' but '%s', this should never happen, why are we here?" %
                     (requestID, res['Value']))

    # The request is 'Done', let's update the job status. If we fail, we should re-try later
    monitorServer = RPCClient("WorkloadManagement/JobMonitoring", useCertificates=useCertificates)
    res = monitorServer.getJobPrimarySummary(int(jobID))
    if not res["OK"]:
      self.log.error("finalizeRequest: Failed to get job status", "JobID: %d" % jobID)
      return S_ERROR("finalizeRequest: Failed to get job %d status" % jobID)
    elif not res['Value']:
      self.log.info("finalizeRequest: job %d does not exist (anymore): finalizing" % jobID)
      return S_OK()
    else:
      jobStatus = res["Value"]["Status"]
      newJobStatus = jobStatus
      jobMinorStatus = res["Value"]["MinorStatus"]

      # update the job pending request digest in any case since it is modified
      self.log.info("finalizeRequest: Updating request digest for job %d" % jobID)

      digest = self.getDigest(requestID)
      if digest["OK"]:
        digest = digest["Value"]
        self.log.verbose(digest)
        res = stateServer.setJobParameter(jobID, "PendingRequest", digest)
        if not res["OK"]:
          self.log.info("finalizeRequest: Failed to set job %d parameter: %s" % (jobID, res["Message"]))
          return res
      else:
        self.log.error("finalizeRequest: Failed to get request digest for %s: %s" % (requestID,
                                                                                     digest["Message"]))
      stateUpdate = None
      if jobStatus == 'Completed':
        # What to do? Depends on what we have in the minorStatus
        if jobMinorStatus == "Pending Requests":
          newJobStatus = 'Done'

        elif jobMinorStatus == "Application Finished With Errors":
          newJobStatus = 'Failed'

      if newJobStatus != jobStatus:
        self.log.info("finalizeRequest: Updating job status for %d to %s/Requests done" % (jobID, newJobStatus))
        stateUpdate = stateServer.setJobStatus(jobID, newJobStatus, "Requests done", "")
      else:
        self.log.info(
            "finalizeRequest: Updating job minor status for %d to Requests done (status is %s)" %
            (jobID, jobStatus))
        stateUpdate = stateServer.setJobStatus(jobID, jobStatus, "Requests done", "")

      if not stateUpdate["OK"]:
        self.log.error("finalizeRequest: Failed to set job status",
                       "JobID: %d status: %s" % (jobID, stateUpdate['Message']))
        return stateUpdate

    return S_OK(newJobStatus)

  def getRequestIDsForJobs(self, jobIDs):
    """ get the request ids for the supplied jobIDs.

    :param self: self reference
    :param jobIDs: list of job IDs (integers)
    :type jobIDs: python:list
    :return: S_ERROR or S_OK( "Successful": { jobID1: reqID1, jobID2: requID2, ... },
                              "Failed" : { jobIDn: errMsg, jobIDm: errMsg, ...}  )
    """
    self.log.verbose("getRequestIDsForJobs: attempt to get request(s) for job %s" % jobIDs)
    requests = self._getRPC().getRequestIDsForJobs(jobIDs)
    if not requests["OK"]:
      self.log.error("getRequestIDsForJobs: unable to get request(s) for jobs",
                     "%s: %s" % (jobIDs, requests["Message"]))
    return requests

  def readRequestsForJobs(self, jobIDs):
    """ read requests for jobs

    :param jobIDs: list with jobIDs
    :type jobIDs: python:list
    :return: S_OK( { "Successful" : { jobID1 : Request, ... },
                     "Failed" : { jobIDn : "Fail reason" } } )
    """
    readReqsForJobs = self._getRPC().readRequestsForJobs(jobIDs)
    if not readReqsForJobs["OK"]:
      return readReqsForJobs
    ret = readReqsForJobs["Value"]
    # # create Requests out of JSONs for successful reads
    if "Successful" in ret:
      for jobID, fromJSON in ret["Successful"].items():
        ret["Successful"][jobID] = Request(fromJSON)
    return S_OK(ret)

  def resetFailedRequest(self, requestID, allR=False):
    """ Reset a failed request to "Waiting" status
    """

    # # we can safely only peek the request as it is Failed and therefore not owned by an agent
    res = self.peekRequest(requestID)
    if not res['OK']:
      return res
    req = res['Value']
    if allR or recoverableRequest(req):
      # Only reset requests that can be recovered
      if req.Status != 'Failed':
        gLogger.notice("Reset NotBefore time, was %s" % str(req.NotBefore))
      else:
        for i, op in enumerate(req):
          op.Error = ''
          if op.Status == 'Failed':
            printOperation((i, op), onlyFailed=True)
          for fi in op:
            if fi.Status == 'Failed':
              fi.Attempt = 1
              fi.Error = ''
              fi.Status = 'Waiting'
          if op.Status == 'Failed':
            op.Status = 'Waiting'

      # Reset also NotBefore
      req.NotBefore = datetime.datetime.utcnow().replace(microsecond=0)
      return self.putRequest(req)
    return S_OK("Not reset")

#============= Some useful functions to be shared ===========


output = ''


def prettyPrint(mainItem, key='', offset=0):
  global output
  if key:
    key += ': '
  blanks = offset * ' '
  if mainItem and isinstance(mainItem, dict):
    output += "%s%s%s\n" % (blanks, key, '{') if blanks or key else ''
    for key in sorted(mainItem):
      prettyPrint(mainItem[key], key=key, offset=offset)
    output += "%s%s\n" % (blanks, '}') if blanks else ''
  elif mainItem and isinstance(mainItem, list) or isinstance(mainItem, tuple):
    output += "%s%s%s\n" % (blanks, key, '[' if isinstance(mainItem, list) else '(')
    for item in mainItem:
      prettyPrint(item, offset=offset + 2)
    output += "%s%s\n" % (blanks, ']' if isinstance(mainItem, list) else ')')
  elif isinstance(mainItem, basestring):
    if '\n' in mainItem:
      prettyPrint(mainItem.strip('\n').split('\n'), offset=offset)
    else:
      output += "%s%s'%s'\n" % (blanks, key, mainItem)
  else:
    output += "%s%s%s\n" % (blanks, key, str(mainItem))
  output = output.replace('[\n%s{' % blanks, '[{').replace('}\n%s]' % blanks, '}]') \
                 .replace('(\n%s{' % blanks, '({').replace('}\n%s)' % blanks, '})') \
                 .replace('(\n%s(' % blanks, '((').replace(')\n%s)' % blanks, '))') \
                 .replace('(\n%s[' % blanks, '[').replace(']\n%s)' % blanks, ']')


def printFTSJobs(request):
  """ Prints the FTSJobs associated to a request

      :param request: Request object
  """

  try:
    if request.RequestID:

      # We try first the new FTS3 system

      from DIRAC.DataManagementSystem.Client.FTS3Client import FTS3Client
      fts3Client = FTS3Client()
      res = fts3Client.ping()

      if res['OK']:
        associatedFTS3Jobs = []
        for op in request:
          res = fts3Client.getOperationsFromRMSOpID(op.OperationID)
          if res['OK']:
            for fts3Op in res['Value']:
              associatedFTS3Jobs.extend(fts3Op.ftsJobs)
        if associatedFTS3Jobs:
          gLogger.always(
              '\n\nFTS3 jobs associated: \n%s' %
              '\n'.join(
                  '%s@%s (%s)' %
                  (job.ftsGUID,
                   job.ftsServer,
                   job.status) for job in associatedFTS3Jobs))
        return

      # If we are here, the attempt with the new FTS3 system did not work, let's try the old FTS system
      gLogger.debug("Could not instantiate FTS3Client", res)
      from DIRAC.DataManagementSystem.Client.FTSClient import FTSClient
      ftsClient = FTSClient()
      res = ftsClient.ping()
      if not res['OK']:
        gLogger.debug("Could not instantiate FtsClient", res)
        return

      res = ftsClient.getFTSJobsForRequest(request.RequestID)
      if res['OK']:
        ftsJobs = res['Value']
        if ftsJobs:
          gLogger.always('         FTS jobs associated: %s' % ','.join('%s (%s)' % (job.FTSGUID, job.Status)
                                                                       for job in ftsJobs))

  # ImportError can be thrown for the old client
  # AttributeError can be thrown because the deserialization will not have
  # happened correctly on the new fts3 (CC7 typically), and the error is not
  # properly propagated
  except (ImportError, AttributeError) as err:
    gLogger.debug("Could not instantiate FtsClient because of Exception", repr(err))


def printRequest(request, status=None, full=False, verbose=True, terse=False):
  global output

  if full:
    output = ''
    prettyPrint(json.loads(request.toJSON()['Value']))
    gLogger.always(output)
  else:
    if not status:
      status = request.Status
    gLogger.always("Request name='%s' ID=%s Status='%s'%s%s%s" %
                   (request.RequestName,
                    request.RequestID if hasattr(request, 'RequestID') else '(not set yet)',
                    request.Status, " ('%s' in DB)" % status if status != request.Status else '',
                    (" Error='%s'" % request.Error) if request.Error and request.Error.strip() else "",
                    (" Job=%s" % request.JobID) if request.JobID else ""))
    gLogger.always("Created %s, Updated %s%s" % (request.CreationTime,
                                                 request.LastUpdate,
                                                 (", NotBefore %s" % request.NotBefore) if request.NotBefore else ""))
    if request.OwnerDN:
      gLogger.always("Owner: '%s', Group: %s" % (request.OwnerDN, request.OwnerGroup))
    for indexOperation in enumerate(request):
      op = indexOperation[1]
      if not terse or op.Status == 'Failed':
        printOperation(indexOperation, verbose, onlyFailed=terse)

  printFTSJobs(request)


def printOperation(indexOperation, verbose=True, onlyFailed=False):
  global output
  i, op = indexOperation
  prStr = ''
  if op.SourceSE:
    prStr += 'SourceSE: %s' % op.SourceSE
  if op.TargetSE:
    prStr += (' - ' if prStr else '') + 'TargetSE: %s' % op.TargetSE
  if prStr:
    prStr += ' - '
  prStr += 'Created %s, Updated %s' % (op.CreationTime, op.LastUpdate)
  if op.Type == 'ForwardDISET' and op.Arguments:
    from DIRAC.Core.Utilities import DEncode
    decode, _length = DEncode.decode(op.Arguments)
    if verbose:
      output = ''
      prettyPrint(decode, offset=10)
      prStr += '\n      Arguments:\n' + output.strip('\n')
    else:
      prStr += '\n      Service: %s' % decode[0][0]
  gLogger.always("  [%s] Operation Type='%s' ID=%s Order=%s Status='%s'%s%s" %
                 (i, op.Type,
                  op.OperationID if hasattr(op, 'OperationID') else '(not set yet)',
                  op.Order, op.Status,
                  (" Error='%s'" % op.Error) if op.Error and op.Error.strip() else "",
                  (" Catalog=%s" % op.Catalog) if op.Catalog else ""))
  if prStr:
    gLogger.always("      %s" % prStr)
  for indexFile in enumerate(op):
    if not onlyFailed or indexFile[1].Status == 'Failed':
      printFile(indexFile)


def printFile(indexFile):
  ind, fi = indexFile
  gLogger.always("    [%02d] ID=%s LFN='%s' Status='%s'%s%s%s" %
                 (ind + 1, fi.FileID if hasattr(fi, 'FileID') else '(not set yet)', fi.LFN, fi.Status,
                  (" Checksum='%s'" % fi.Checksum) if fi.Checksum or
                  (fi.Error and 'checksum' in fi.Error.lower()) else "",
                  (" Error='%s'" % fi.Error) if fi.Error and fi.Error.strip() else "",
                  (" Attempts=%d" % fi.Attempt) if fi.Attempt > 1 else "")
                 )


def recoverableRequest(request):
  excludedErrors = ('File does not exist', 'No such file or directory',
                    'sourceSURL equals to targetSURL',
                    'Max attempts limit reached', 'Max attempts reached')
  operationErrorsOK = ('is banned for', 'Failed to perform exists from any catalog')
  for op in request:
    if op.Status == 'Failed' and (not op.Error or not [errStr for errStr in operationErrorsOK if errStr in op.Error]):
      for fi in op:
        if fi.Status == 'Failed':
          if [errStr for errStr in excludedErrors if errStr in fi.Error]:
            return False
          return True
  return True
