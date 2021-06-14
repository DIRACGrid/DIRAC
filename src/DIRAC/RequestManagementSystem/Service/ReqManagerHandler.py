#####################################################################
# File: ReqManagerHandler.py
########################################################################
"""Implementation of the RequestDB service in the DISET framework


.. literalinclude:: ../ConfigTemplate.cfg
  :start-after: ##BEGIN ReqManager
  :end-before: ##END
  :dedent: 2
  :caption: ReqManager options


"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

__RCSID__ = "$Id$"

# # imports
import six
import json
import datetime
import math
# # from DIRAC
from DIRAC import gLogger, S_OK, S_ERROR
from DIRAC.Core.DISET.RequestHandler import RequestHandler, getServiceOption
from DIRAC.Core.Utilities import DErrno
from DIRAC.Core.Utilities.DEncode import ignoreEncodeWarning
# # from RMS
from DIRAC.RequestManagementSystem.Client.Request import Request
from DIRAC.RequestManagementSystem.private.RequestValidator import RequestValidator
from DIRAC.RequestManagementSystem.DB.RequestDB import RequestDB


class ReqManagerHandler(RequestHandler):
  """
  .. class:: ReqManagerHandler

  RequestDB interface in the DISET framework.
  """
  # # request validator
  __validator = None
  # # request DB instance
  __requestDB = None

  @classmethod
  def initializeHandler(cls, serviceInfoDict):
    """ initialize handler """

    try:
      cls.__requestDB = RequestDB()
    except RuntimeError as error:
      gLogger.exception(error)
      return S_ERROR(error)

    # If there is a constant delay to be applied to each request
    cls.constantRequestDelay = getServiceOption(serviceInfoDict, 'ConstantRequestDelay', 0)

    # # create tables for empty db
    return cls.__requestDB.createTables()

  # # helper functions
  @classmethod
  def validate(cls, request):
    """ request validation """
    if not cls.__validator:
      cls.__validator = RequestValidator()
    return cls.__validator.validate(request)

  types_getRequestIDForName = [six.string_types]

  @classmethod
  def export_getRequestIDForName(cls, requestName):
    """ get requestID for given :requestName: """
    if isinstance(requestName, six.string_types):
      result = cls.__requestDB.getRequestIDForName(requestName)
      if not result["OK"]:
        return result
      requestID = result["Value"]
    return S_OK(requestID)

  types_cancelRequest = [six.integer_types]

  @classmethod
  def export_cancelRequest(cls, requestID):
    """ Cancel a request """
    return cls.__requestDB.cancelRequest(requestID)

  types_putRequest = [six.string_types]

  def export_putRequest(self, requestJSON):
    """ put a new request into RequestDB

    :param cls: class ref
    :param str requestJSON: request serialized to JSON format
    """
    requestDict = json.loads(requestJSON)
    requestName = requestDict.get("RequestID", requestDict.get('RequestName', "***UNKNOWN***"))
    request = Request(requestDict)

    # Check whether the credentials in the Requests are correct and allowed to be set
    isAuthorized = RequestValidator.setAndCheckRequestOwner(request, self.getRemoteCredentials())

    if not isAuthorized:
      return S_ERROR(DErrno.ENOAUTH, "Credentials in the requests are not allowed")

    optimized = request.optimize()
    if optimized.get("Value", False):
      gLogger.debug("putRequest: request was optimized")
    else:
      gLogger.debug(
          "putRequest: request unchanged",
          optimized.get(
              "Message",
              "Nothing could be optimized"))

    valid = self.validate(request)
    if not valid["OK"]:
      gLogger.error("putRequest: request %s not valid: %s" % (requestName, valid["Message"]))
      return valid

    # If NotBefore is not set or user defined, we calculate its value

    now = datetime.datetime.utcnow().replace(microsecond=0)
    extraDelay = datetime.timedelta(0)
    if request.Status not in Request.FINAL_STATES and (
            not request.NotBefore or request.NotBefore < now):
      # We don't delay if it is the first insertion
      if getattr(request, 'RequestID', 0):
        # If it is a constant delay, just set it
        if self.constantRequestDelay:
          extraDelay = datetime.timedelta(minutes=self.constantRequestDelay)
        else:
          # If there is a waiting Operation with Files
          op = request.getWaiting().get('Value')
          if op and len(op):
            attemptList = [opFile.Attempt for opFile in op if opFile.Status == "Waiting"]
            if attemptList:
              maxWaitingAttempt = max(
                  [opFile.Attempt for opFile in op if opFile.Status == "Waiting"])
              # In case it is the first attempt, extraDelay is 0
              # maxWaitingAttempt can be None if the operation has no File, like the ForwardDiset
              extraDelay = datetime.timedelta(
                  minutes=2 * math.log(maxWaitingAttempt) if maxWaitingAttempt else 0)

        request.NotBefore = now + extraDelay

    gLogger.info("putRequest: request %s not before %s (extra delay %s)" %
                 (request.RequestName, request.NotBefore, extraDelay))

    requestName = request.RequestName
    gLogger.info("putRequest: Attempting to set request '%s'" % requestName)
    return self.__requestDB.putRequest(request)

  types_getScheduledRequest = [six.integer_types]

  @classmethod
  def export_getScheduledRequest(cls, operationID):
    """ read scheduled request given operationID """
    scheduled = cls.__requestDB.getScheduledRequest(operationID)
    if not scheduled["OK"]:
      gLogger.error("getScheduledRequest: %s" % scheduled["Message"])
      return scheduled
    if not scheduled["Value"]:
      return S_OK()
    requestJSON = scheduled["Value"].toJSON()
    if not requestJSON["OK"]:
      gLogger.error("getScheduledRequest: %s" % requestJSON["Message"])
    return requestJSON

  types_getDBSummary = []

  @classmethod
  def export_getDBSummary(cls):
    """ Get the summary of requests in the Request DB """
    return cls.__requestDB.getDBSummary()

  types_getRequest = [six.integer_types]

  @classmethod
  def export_getRequest(cls, requestID=0):
    """ Get a request of given type from the database """
    getRequest = cls.__requestDB.getRequest(requestID)
    if not getRequest["OK"]:
      gLogger.error("getRequest: %s" % getRequest["Message"])
      return getRequest
    if getRequest["Value"]:
      getRequest = getRequest["Value"]
      toJSON = getRequest.toJSON()
      if not toJSON["OK"]:
        gLogger.error(toJSON["Message"])
      return toJSON
    return S_OK()

  types_getBulkRequests = [int, bool]

  @classmethod
  @ignoreEncodeWarning
  def export_getBulkRequests(cls, numberOfRequest, assigned):
    """ Get a request of given type from the database

        :warning: the dictionary may contain string keys instead of int (json serialization)
                  Do not forget to cast it back

        :param numberOfRequest: size of the bulk (default 10)
        :return: S_OK( {Failed : message, Successful : list of Request.toJSON()} )
    """
    getRequests = cls.__requestDB.getBulkRequests(
        numberOfRequest=numberOfRequest, assigned=assigned)
    if not getRequests["OK"]:
      gLogger.error("getRequests: %s" % getRequests["Message"])
      return getRequests
    if getRequests["Value"]:
      getRequests = getRequests["Value"]
      toJSONDict = {"Successful": {}, "Failed": {}}

      for rId in getRequests:
        toJSON = getRequests[rId].toJSON()
        if not toJSON["OK"]:
          gLogger.error(toJSON["Message"])
          toJSONDict["Failed"][rId] = toJSON["Message"]
        else:
          toJSONDict["Successful"][rId] = toJSON["Value"]
      return S_OK(toJSONDict)
    return S_OK()

  types_peekRequest = [six.integer_types]

  @classmethod
  def export_peekRequest(cls, requestID=0):
    """ peek request given its id """
    peekRequest = cls.__requestDB.peekRequest(requestID)
    if not peekRequest["OK"]:
      gLogger.error("peekRequest: %s" % peekRequest["Message"])
      return peekRequest
    if peekRequest["Value"]:
      peekRequest = peekRequest["Value"].toJSON()
      if not peekRequest["OK"]:
        gLogger.error(peekRequest["Message"])
    return peekRequest

  types_getRequestSummaryWeb = [dict, list, int, int]

  @classmethod
  def export_getRequestSummaryWeb(cls, selectDict, sortList, startItem, maxItems):
    """ Returns a list of Request for the web portal

        :param dict selectDict: parameter on which to restrain the query {key : Value}
                                key can be any of the Request columns, 'Type' (interpreted as Operation.Type)
                                and 'FromData' and 'ToData' are matched against the LastUpdate field
        :param sortList: [sorting column, ASC/DESC]
        :type sortList: python:list
        :param int startItem: start item (for pagination)
        :param int maxItems: max items (for pagination)
    """
    return cls.__requestDB.getRequestSummaryWeb(selectDict, sortList, startItem, maxItems)

  types_getDistinctValuesWeb = [six.string_types]

  @classmethod
  def export_getDistinctValuesWeb(cls, attribute):
    """ Get distinct values for a given request attribute. 'Type' is interpreted as
        the operation type """

    tableName = 'Request'
    if attribute == 'Type':
      tableName = 'Operation'
    return cls.__requestDB.getDistinctValues(tableName, attribute)

  types_getRequestCountersWeb = [six.string_types, dict]

  @classmethod
  def export_getRequestCountersWeb(cls, groupingAttribute, selectDict):
    """ For the web portal.
        Returns a dictionary {value : counts} for a given key.
        The key can be any field from the RequestTable. or "Type",
        which will be interpreted as 'Operation.Type'

        :param groupingAttribute: attribute used for grouping
        :param selectDict: selection criteria
    """

    return cls.__requestDB.getRequestCountersWeb(groupingAttribute, selectDict)

  types_deleteRequest = [six.integer_types]

  @classmethod
  def export_deleteRequest(cls, requestID):
    """ Delete the request with the supplied ID"""
    return cls.__requestDB.deleteRequest(requestID)

  types_getRequestIDsList = [list, int, six.string_types]

  @classmethod
  def export_getRequestIDsList(
          cls,
          statusList=None,
          limit=None,
          since=None,
          until=None,
          getJobID=False):
    """ get requests' IDs with status in :statusList: """
    statusList = statusList if statusList else list(Request.FINAL_STATES)
    limit = limit if limit else 100
    since = since if since else ""
    until = until if until else ""
    reqIDsList = cls.__requestDB.getRequestIDsList(
        statusList, limit, since=since, until=until, getJobID=getJobID)
    if not reqIDsList["OK"]:
      gLogger.error("getRequestIDsList: %s" % reqIDsList["Message"])
    return reqIDsList

  types_getRequestIDsForJobs = [list]

  @classmethod
  def export_getRequestIDsForJobs(cls, jobIDs):
    """ Select the request IDs for supplied jobIDs

        :warning: the dictionary may contain string keys instead of int (json serialization)
          Do not forget to cast it back

    """
    return cls.__requestDB.getRequestIDsForJobs(jobIDs)

  types_readRequestsForJobs = [list]

  @classmethod
  @ignoreEncodeWarning
  def export_readRequestsForJobs(cls, jobIDs):
    """ read requests for jobs given list of jobIDs """
    requests = cls.__requestDB.readRequestsForJobs(jobIDs)
    if not requests["OK"]:
      gLogger.error("readRequestsForJobs: %s" % requests["Message"])
      return requests
    for jobID, request in requests["Value"]["Successful"].items():
      requests["Value"]["Successful"][jobID] = request.toJSON()["Value"]
    return requests

  types_getDigest = [six.integer_types]

  @classmethod
  def export_getDigest(cls, requestID):
    """ get digest for a request given its id

    :param str requestID: request's id
    :return: S_OK( json_str )
    """
    return cls.__requestDB.getDigest(requestID)

  types_getRequestStatus = [six.integer_types]

  @classmethod
  def export_getRequestStatus(cls, requestID):
    """ get request status given its id """
    status = cls.__requestDB.getRequestStatus(requestID)
    if not status["OK"]:
      gLogger.error("getRequestStatus: %s" % status["Message"])
    return status

  types_getRequestFileStatus = [list(six.integer_types), list(six.string_types) + [list]]

  @classmethod
  def export_getRequestFileStatus(cls, requestID, lfnList):
    """ get request file status for a given LFNs list and requestID """
    if isinstance(lfnList, six.string_types):
      lfnList = [lfnList]
    res = cls.__requestDB.getRequestFileStatus(requestID, lfnList)
    if not res["OK"]:
      gLogger.error("getRequestFileStatus: %s" % res["Message"])
    return res

  types_getRequestInfo = [list(six.integer_types)]

  @classmethod
  def export_getRequestInfo(cls, requestID):
    """ get request info for a given requestID """
    requestInfo = cls.__requestDB.getRequestInfo(requestID)
    if not requestInfo["OK"]:
      gLogger.error("getRequestInfo: %s" % requestInfo["Message"])
    return requestInfo
