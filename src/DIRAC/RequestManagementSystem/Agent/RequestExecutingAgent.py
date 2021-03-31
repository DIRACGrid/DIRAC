########################################################################
# File: RequestExecutingAgent.py
# Author: Krzysztof.Ciba@NOSPAMgmail.com
# Date: 2013/03/12 15:36:47
########################################################################

"""Agent processing the requests

 .. moduleauthor:: Krzysztof.Ciba@NOSPAMgmail.com

See also the information about the :ref:`requestManagementSystem`.

The following options can be set for the RequestExecutingAgent. The configuration also includes the
``OperationHandlers`` available in DIRAC.

.. literalinclude:: ../ConfigTemplate.cfg
  :start-after: ##BEGIN RequestExecutingAgent
  :end-before: ##END
  :dedent: 2
  :caption: RequestExecutingAgent options

"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

__RCSID__ = '$Id$'

# #
# @file RequestExecutingAgent.py
# @author Krzysztof.Ciba@NOSPAMgmail.com
# @date 2013/03/12 15:36:56
# @brief Definition of RequestExecutingAgent class.

# # imports
import sys
import time
import errno

# # from DIRAC
from DIRAC import S_OK, S_ERROR, gConfig
from DIRAC.FrameworkSystem.Client.MonitoringClient import gMonitor
from DIRAC.Core.Base.AgentModule import AgentModule
from DIRAC.ConfigurationSystem.Client import PathFinder
from DIRAC.Core.Utilities.ProcessPool import ProcessPool
from DIRAC.RequestManagementSystem.Client.ReqClient import ReqClient
from DIRAC.RequestManagementSystem.private.RequestTask import RequestTask

from DIRAC.MonitoringSystem.Client.MonitoringReporter import MonitoringReporter
from DIRAC.Core.Utilities.ThreadScheduler import gThreadScheduler
from DIRAC.Core.Utilities import Time, Network

from DIRAC.Core.Utilities.DErrno import cmpError
# # agent name
AGENT_NAME = "RequestManagement/RequestExecutingAgent"


class AgentConfigError(Exception):
  """ misconfiguration error """

  def __init__(self, msg):
    """ ctor

    :param str msg: error string
    """
    Exception.__init__(self)
    self.msg = msg

  def __str__(self):
    """ str op """
    return self.msg

########################################################################


class RequestExecutingAgent(AgentModule):
  """
  .. class:: RequestExecutingAgent

  request processing agent using ProcessPool, Operation handlers and RequestTask
  """
  # # process pool
  __processPool = None
  # # request cache
  __requestCache = {}
  # # requests/cycle
  __requestsPerCycle = 100
  # # minimal nb of subprocess running
  __minProcess = 20
  # # maximal nb of subprocess executed same time
  __maxProcess = 20
  # # ProcessPool queue size
  __queueSize = 20
  # # file timeout
  __fileTimeout = 300
  # # operation timeout
  __operationTimeout = 300
  # # ProcessPool finalization timeout
  __poolTimeout = 900
  # # ProcessPool sleep time
  __poolSleep = 5
  # # placeholder for RequestClient instance
  __requestClient = None
  # # Size of the bulk if use of getRequests. If 0, use getRequest
  __bulkRequest = 0
  # # Send the monitoring data to ES rather than the Framework/Monitoring
  __rmsMonitoring = False

  def __init__(self, *args, **kwargs):
    """ c'tor """
    # # call base class ctor
    AgentModule.__init__(self, *args, **kwargs)
    # # ProcessPool related stuff
    self.__requestsPerCycle = self.am_getOption("RequestsPerCycle", self.__requestsPerCycle)
    self.log.info("Requests/cycle = %d" % self.__requestsPerCycle)
    self.__minProcess = self.am_getOption("MinProcess", self.__minProcess)
    self.log.info("ProcessPool min process = %d" % self.__minProcess)
    self.__maxProcess = self.am_getOption("MaxProcess", self.__maxProcess)
    self.log.info("ProcessPool max process = %d" % self.__maxProcess)
    self.__queueSize = self.am_getOption("ProcessPoolQueueSize", self.__queueSize)
    self.log.info("ProcessPool queue size = %d" % self.__queueSize)
    self.__poolTimeout = int(self.am_getOption("ProcessPoolTimeout", self.__poolTimeout))
    self.log.info("ProcessPool timeout = %d seconds" % self.__poolTimeout)
    self.__poolSleep = int(self.am_getOption("ProcessPoolSleep", self.__poolSleep))
    self.log.info("ProcessPool sleep time = %d seconds" % self.__poolSleep)
    self.__bulkRequest = self.am_getOption("BulkRequest", self.__bulkRequest)
    self.log.info("Bulk request size = %d" % self.__bulkRequest)
    self.__rmsMonitoring = self.am_getOption("EnableRMSMonitoring", self.__rmsMonitoring)
    self.log.info("Enable ES RMS Monitoring = %s" % self.__rmsMonitoring)

    # # keep config path and agent name
    self.agentName = self.am_getModuleParam("fullName")
    self.__configPath = PathFinder.getAgentSection(self.agentName)

    # # operation handlers over here
    opHandlersPath = "%s/%s" % (self.__configPath, "OperationHandlers")
    opHandlers = gConfig.getSections(opHandlersPath)
    if not opHandlers["OK"]:
      self.log.error(opHandlers["Message"])
      raise AgentConfigError("OperationHandlers section not found in CS under %s" % self.__configPath)
    opHandlers = opHandlers["Value"]

    self.timeOuts = dict()

    # # handlers dict
    self.handlersDict = dict()
    for opHandler in opHandlers:
      opHandlerPath = "%s/%s/Location" % (opHandlersPath, opHandler)
      opLocation = gConfig.getValue(opHandlerPath, "")
      if not opLocation:
        self.log.error("%s not set for %s operation handler" % (opHandlerPath, opHandler))
        continue
      self.timeOuts[opHandler] = {"PerFile": self.__fileTimeout, "PerOperation": self.__operationTimeout}

      opTimeout = gConfig.getValue("%s/%s/TimeOut" % (opHandlersPath, opHandler), 0)
      if opTimeout:
        self.timeOuts[opHandler]["PerOperation"] = opTimeout
      fileTimeout = gConfig.getValue("%s/%s/TimeOutPerFile" % (opHandlersPath, opHandler), 0)
      if fileTimeout:
        self.timeOuts[opHandler]["PerFile"] = fileTimeout

      self.handlersDict[opHandler] = opLocation

    self.log.info("Operation handlers:")
    for item in enumerate(self.handlersDict.items()):
      opHandler = item[1][0]
      self.log.info("[%s] %s: %s (timeout: %d s + %d s per file)" % (item[0], item[1][0], item[1][1],
                                                                     self.timeOuts[opHandler]['PerOperation'],
                                                                     self.timeOuts[opHandler]['PerFile']))

    if self.__rmsMonitoring:
      self.rmsMonitoringReporter = MonitoringReporter(monitoringType="RMSMonitoring")
      gThreadScheduler.addPeriodicTask(100, self.__rmsMonitoringReporting)
    else:
      # # common monitor activity
      gMonitor.registerActivity("Iteration", "Agent Loops",
                                "RequestExecutingAgent", "Loops/min", gMonitor.OP_SUM)
      gMonitor.registerActivity("Processed", "Request Processed",
                                "RequestExecutingAgent", "Requests/min", gMonitor.OP_SUM)
      gMonitor.registerActivity("Done", "Request Completed",
                                "RequestExecutingAgent", "Requests/min", gMonitor.OP_SUM)

    # # create request dict
    self.__requestCache = dict()

    # ?? Probably should be removed
    self.FTSMode = self.am_getOption("FTSMode", False)

  def processPool(self):
    """ facade for ProcessPool """
    if not self.__processPool:
      minProcess = max(1, self.__minProcess)
      maxProcess = max(self.__minProcess, self.__maxProcess)
      queueSize = abs(self.__queueSize)
      self.log.info("REA ProcessPool configuration", "minProcess = %d maxProcess = %d queueSize = %d" % (minProcess,
                                                                                                         maxProcess,
                                                                                                         queueSize))
      self.__processPool = ProcessPool(minProcess,
                                       maxProcess,
                                       queueSize,
                                       poolCallback=self.resultCallback,
                                       poolExceptionCallback=self.exceptionCallback)
      self.__processPool.daemonize()
    return self.__processPool

  def requestClient(self):
    """ RequestClient getter """
    if not self.__requestClient:
      self.__requestClient = ReqClient()
    return self.__requestClient

  def cacheRequest(self, request):
    """ put request into requestCache

    :param ~Request.Request request: Request instance
    """
    maxProcess = max(self.__minProcess, self.__maxProcess)
    if len(self.__requestCache) > maxProcess + 50:
      # For the time being we just print a warning... If the ProcessPool is working well, this is not needed
      # We don't know how much is acceptable as it depends on many factors
      self.log.warn("Too many requests in cache", ': %d' % len(self.__requestCache))
#      return S_ERROR( "Too many requests in cache" )
    if request.RequestID in self.__requestCache:
      # We don't call  putRequest as we have got back the request that is still being executed. Better keep it
      # The main reason for this is that it lasted longer than the kick time of CleanReqAgent
      self.log.warn("Duplicate request, keep it but don't execute",
                    ': %d/%s' % (request.RequestID, request.RequestName))
      return S_ERROR(errno.EALREADY, 'Request already in cache')
    self.__requestCache[request.RequestID] = request
    return S_OK()

  def putRequest(self, requestID, taskResult=None):
    """ put back :requestID: to RequestClient

    :param str requestID: request's id
    """
    if requestID in self.__requestCache:
      request = self.__requestCache.pop(requestID)
      if taskResult:
        if taskResult['OK']:
          request = taskResult['Value']
          # The RequestTask is putting back the Done tasks, no need to redo it
          if request.Status == 'Done':
            return S_OK()
        # In case of timeout, we need to increment ourselves all the attempts
        elif cmpError(taskResult, errno.ETIME):
          waitingOp = request.getWaiting()
          for rmsFile in waitingOp.get('Value', []):
            rmsFile.Attempt += 1

      reset = self.requestClient().putRequest(request, useFailoverProxy=False, retryMainService=2)
      if not reset["OK"]:
        return S_ERROR("putRequest: unable to reset request %s: %s" % (requestID, reset["Message"]))
    else:
      return S_ERROR('Not in cache')
    return S_OK()

  def putAllRequests(self):
    """ put back all requests without callback called into requestClient

    :param self: self reference
    """
    self.log.info("putAllRequests: will put back requests", "%s" % len(self.__requestCache))
    for requestID in self.__requestCache.keys():
      reset = self.putRequest(requestID)
      if not reset["OK"]:
        self.log.error('Failed to put request', reset["Message"])
      else:
        self.log.debug("putAllRequests: request %s has been put back with its initial state" % requestID)
    return S_OK()

  def initialize(self):
    """ initialize agent
    """
    return S_OK()

  def execute(self):
    """ read requests from RequestClient and enqueue them into ProcessPool """
    if not self.__rmsMonitoring:
      gMonitor.addMark("Iteration", 1)
    # # requests (and so tasks) counter
    taskCounter = 0
    while taskCounter < self.__requestsPerCycle:
      self.log.debug("execute: executing %d request in this cycle" % taskCounter)

      requestsToExecute = []

      if not self.__bulkRequest:
        self.log.info("execute: ask for a single request")
        getRequest = self.requestClient().getRequest()
        if not getRequest["OK"]:
          self.log.error("execute:", "%s" % getRequest["Message"])
          break
        if not getRequest["Value"]:
          self.log.info("execute: no more 'Waiting' requests to process")
          break
        requestsToExecute = [getRequest["Value"]]
      else:
        numberOfRequest = min(self.__bulkRequest, self.__requestsPerCycle - taskCounter)
        self.log.info("execute: ask for requests", "%s" % numberOfRequest)
        getRequests = self.requestClient().getBulkRequests(numberOfRequest)
        if not getRequests["OK"]:
          self.log.error("execute:", "%s" % getRequests["Message"])
          break
        if not getRequests["Value"]:
          self.log.info("execute: no more 'Waiting' requests to process")
          break
        for rId in getRequests["Value"]["Failed"]:
          self.log.error("execute:", "%s" % getRequests["Value"]["Failed"][rId])

        requestsToExecute = list(getRequests["Value"]["Successful"].values())

      self.log.info("execute: will execute requests ", "%s" % len(requestsToExecute))

      for request in requestsToExecute:
        # # set task id
        taskID = request.RequestID

        self.log.info("processPool status", "tasks idle = %s working = %s" %
                      (self.processPool().getNumIdleProcesses(), self.processPool().getNumWorkingProcesses()))

        looping = 0
        while True:
          if not self.processPool().getFreeSlots():
            if not looping:
              self.log.info(
                  "No free slots available in processPool",
                  "will wait %d seconds to proceed" %
                  self.__poolSleep)
            time.sleep(self.__poolSleep)
            looping += 1
          else:
            if looping:
              self.log.info("Free slot found", "after %d seconds" % looping * self.__poolSleep)
            looping = 0
            # # save current request in cache
            res = self.cacheRequest(request)
            if not res['OK']:
              if cmpError(res, errno.EALREADY):
                # The request is already in the cache, skip it. break out of the while loop to get next request
                break
              # There are too many requests in the cache, commit suicide
              self.log.error(
                  "Too many requests in cache",
                  '(%d requests): put back all requests and exit cycle. Error %s' % (
                      len(
                          self.__requestCache),
                      res['Message']))
              self.putAllRequests()
              return res
            # # serialize to JSON
            result = request.toJSON()
            if not result['OK']:
              continue
            requestJSON = result['Value']
            self.log.info("spawning task for request", "'%s/%s'" % (request.RequestID, request.RequestName))
            timeOut = self.getTimeout(request)
            enqueue = self.processPool().createAndQueueTask(RequestTask,
                                                            kwargs={"requestJSON": requestJSON,
                                                                    "handlersDict": self.handlersDict,
                                                                    "csPath": self.__configPath,
                                                                    "agentName": self.agentName,
                                                                    "rmsMonitoring": self.__rmsMonitoring},
                                                            taskID=taskID,
                                                            blocking=True,
                                                            usePoolCallbacks=True,
                                                            timeOut=timeOut)
            if not enqueue["OK"]:
              self.log.error("Could not enqueue task", enqueue["Message"])
            else:
              self.log.debug("successfully enqueued task", "'%s'" % taskID)
              # # update monitor
              if self.__rmsMonitoring:
                self.rmsMonitoringReporter.addRecord({
                    "timestamp": int(Time.toEpoch()),
                    "host": Network.getFQDN(),
                    "objectType": "Request",
                    "status": "Attempted",
                    "objectID": request.RequestID,
                    "nbObject": 1
                })
              else:
                gMonitor.addMark("Processed", 1)

              # # update request counter
              taskCounter += 1
              # # task created, a little time kick to proceed
              time.sleep(0.1)
              break

    self.log.info("Flushing callbacks", "(%d requests still in cache)" % len(self.__requestCache))
    processed = self.processPool().processResults()
    # This happens when the result queue is screwed up.
    # Returning S_ERROR proved not to be sufficient,
    # and when in this situation, there is nothing we can do.
    # So we just exit. runit will restart from scratch.
    if processed < 0:
      self.log.fatal("Results queue is screwed up")
      sys.exit(1)
    # # clean return
    return S_OK()

  def getTimeout(self, request):
    """ get timeout for request """
    timeout = 0
    for op in request:
      if op.Status not in ("Waiting", "Scheduled", 'Queued'):
        continue
      if op.Type not in self.timeOuts:
        timeout += self.__operationTimeout
      else:
        perOp = self.timeOuts[op.Type].get("PerOperation", self.__operationTimeout)
        perFiles = self.timeOuts[op.Type].get("PerFile", self.__fileTimeout) * len(op)
        timeout += perOp + perFiles
    self.log.info("estimated timeOut for request", "(%s/%s) is %s" % (request.RequestID, request.RequestName, timeout))
    return timeout

  def finalize(self):
    """ agent finalization """
    if self.__processPool:
      self.processPool().finalize(timeout=self.__poolTimeout)
    self.putAllRequests()
    return S_OK()

  def resultCallback(self, taskID, taskResult):
    """ definition of request callback function

    :param str taskID: Request.RequestID
    :param dict taskResult: task result S_OK(Request)/S_ERROR(Message)
    """
    # # clean cache
    res = self.putRequest(taskID, taskResult)
    self.log.info(
        "callback:",
        "%s result is %s(%s), put %s(%s)" %
        (taskID,
         "S_OK" if taskResult["OK"] else "S_ERROR",
         taskResult["Value"].Status if taskResult["OK"] else taskResult["Message"],
         "S_OK" if res['OK'] else 'S_ERROR',
         '' if res['OK'] else res['Message']))

  def exceptionCallback(self, taskID, taskException):
    """ definition of exception callback function

    :param str taskID: Request.RequestID
    :param Exception taskException: Exception instance
    """
    self.log.error("exceptionCallback:", "%s was hit by exception %s" % (taskID, taskException))
    self.putRequest(taskID)

  def __rmsMonitoringReporting(self):
    """ This method is called by the ThreadScheduler as a periodic task in order to commit the collected data which
        is done by the MonitoringReporter and is send to the 'RMSMonitoring' type.
        :return: True / False
    """
    result = self.rmsMonitoringReporter.commit()
    return result['OK']
