########################################################################
# File: FTSAgent.py
# Author: Krzysztof.Ciba@NOSPAMgmail.com
# Date: 2013/05/31 10:00:13
########################################################################
""" :mod: FTSAgent

    ==============

    .. module: FTSAgent

    :synopsis: agent propagating scheduled RMS request in FTS

    .. moduleauthor:: Krzysztof.Ciba@NOSPAMgmail.com

    DIRAC agent propagating scheduled RMS request in FTS

    Request processing phases (each in a separate thread):

      1. MONITOR

         ...active FTSJobs, prepare FTSFiles dictionary with files to submit, fail, register and reschedule

      2. CHECK REPLICAS

         ...just in case if all transfers are done, if yes, end processing

      3. FAILED FILES:

         ...if at least one Failed FTSFile is found, set Request.Operation.File to 'Failed', end processing

      4. UPDATE Waiting#SourceSE FTSFiles

         ...if any found in FTSDB

      5. REGISTER REPLICA

         ...insert RegisterReplica operation to request, if some FTSFiles failed to register, end processing

      6. RESCHEDULE FILES

         ...for FTSFiles failed with missing sources error

      7. SUBMIT

         ...but read 'Waiting' FTSFiles first from FTSDB and merge those with FTSFiles to retry

"""
__RCSID__ = "$Id: $"
# #
# @file FTSAgent.py
# @author Krzysztof.Ciba@NOSPAMgmail.com
# @date 2013/05/31 10:00:51
# @brief Definition of FTSAgent class.
# # imports
import time
import datetime
import re
import math
# # from DIRAC
from DIRAC import S_OK, S_ERROR, gLogger
# # from CS
from DIRAC.ConfigurationSystem.Client.Helpers.Registry import getUsernameForDN
from DIRAC.ConfigurationSystem.Client.Helpers.Operations import Operations
from DIRAC.ConfigurationSystem.Client.ConfigurationData import gConfigurationData

# # from Core
from DIRAC.FrameworkSystem.Client.MonitoringClient import gMonitor
from DIRAC.Core.Utilities.LockRing import LockRing
from DIRAC.Core.Utilities.ThreadPool import ThreadPool
from DIRAC.Core.Base.AgentModule import AgentModule
from DIRAC.Core.Utilities.Time import fromString
from DIRAC.Core.Utilities.List import breakListIntoChunks
# # from DMS
from DIRAC.DataManagementSystem.Client.FTSClient import FTSClient
from DIRAC.DataManagementSystem.Client.FTSJob import FTSJob
from DIRAC.DataManagementSystem.Client.DataManager import DataManager
from DIRAC.DataManagementSystem.private.FTSPlacement import FTSPlacement
from DIRAC.DataManagementSystem.private.FTSHistoryView import FTSHistoryView
from DIRAC.DataManagementSystem.Client.FTSFile import FTSFile
from DIRAC.DataManagementSystem.Utilities.DMSHelpers import DMSHelpers
# # from RMS
from DIRAC.RequestManagementSystem.Client.ReqClient import ReqClient
from DIRAC.RequestManagementSystem.Client.Operation import Operation
from DIRAC.RequestManagementSystem.Client.File import File
# # from RSS
# #from DIRAC.ConfigurationSystem.Client.Helpers.Resources import Resources
from DIRAC.ResourceStatusSystem.Client.ResourceStatus import ResourceStatus
# # from Resources
from DIRAC.Resources.Storage.StorageElement import StorageElement
from DIRAC.Resources.Catalog.FileCatalog import FileCatalog
from DIRAC.Core.Utilities.ReturnValues import returnSingleResult
# # from Accounting
from DIRAC.AccountingSystem.Client.Types.DataOperation import DataOperation


# # agent base name
AGENT_NAME = "DataManagement/FTSAgent"


class EscapeTryException(Exception):
  pass

########################################################################


class FTSAgent(AgentModule):
  """
  .. class:: FTSAgent

  Agent propagating Scheduled request to Done or Failed state in the FTS system.

  Requests and associated FTSJobs (and so FTSFiles) are kept in cache.

  """

  # request cache
  __reqCache = dict()

  # # fts placement refresh in seconds
  FTSPLACEMENT_REFRESH = FTSHistoryView.INTERVAL / 2
  # # placeholder for max job per channel
  MAX_ACTIVE_JOBS = 50
  # # min threads
  MIN_THREADS = 1
  # # max threads
  MAX_THREADS = 10
  # # files per job
  MAX_FILES_PER_JOB = 100
  # # MAX FTS transfer per FTSFile
  MAX_ATTEMPT = 256
  # # stage flag
  PIN_TIME = 0
  # # FTS submission command
  SUBMIT_COMMAND = 'glite-transfer-submit'
  # # FTS monitoring command
  MONITOR_COMMAND = 'glite-transfer-status'
  # Max number of requests fetched from the RMS
  MAX_REQUESTS = 100
  # Minimum interval (seconds) between 2 job monitoring
  MONITORING_INTERVAL = 600
  # Flag to know if one selects JOb requests (True) or not (False) or don't care (None)
  PROCESS_JOB_REQUESTS = None

  # # placeholder for FTS client
  __ftsClient = None
  # # placeholder for the FTS version
  __ftsVersion = None
  # # placeholder for request client
  __requestClient = None
  # # placeholder for resources helper
  __resources = None
  # # placeholder for RSS client
  __rssClient = None
  # # placeholder for FTSPlacement
  __ftsPlacement = None

  # # placement regeneration time delta
  __ftsPlacementValidStamp = None

  # # placeholder for threadPool
  __threadPool = None
  # # update lock
  __updateLock = None
  # # request cache
  __reqCache = dict()

  registrationProtocols = None


  def updateLock(self):
    """ update lock """
    if not self.__updateLock:
      self.__updateLock = LockRing().getLock("FTSAgentLock")
    return self.__updateLock

  @classmethod
  def requestClient(cls):
    """ request client getter """
    if not cls.__requestClient:
      cls.__requestClient = ReqClient()
    return cls.__requestClient

  @classmethod
  def ftsClient(cls):
    """ FTS client """
    if not cls.__ftsClient:
      cls.__ftsClient = FTSClient()
    return cls.__ftsClient

  @classmethod
  def rssClient(cls):
    """ RSS client getter """
    if not cls.__rssClient:
      cls.__rssClient = ResourceStatus()
    return cls.__rssClient

  @classmethod
  def getRequest(cls, reqID):
    """ get Requests systematically and refresh cache """

    # Make sure the request is Scheduled
    res = cls.requestClient().getRequestStatus(reqID)
    if not res['OK']:
      cls.__reqCache.pop(reqID, None)
      return res
    status = res['Value']
    if status != 'Scheduled':
      cls.__reqCache.pop(reqID, None)
      return S_ERROR("Request with id %s is not Scheduled:%s" % (reqID, status))

    getRequest = cls.requestClient().getRequest(reqID)
    if not getRequest["OK"]:
      cls.__reqCache.pop(reqID, None)
      return getRequest
    getRequest = getRequest["Value"]
    if not getRequest:
      cls.__reqCache.pop(reqID, None)
      return S_ERROR("request of id '%s' not found in ReqDB" % reqID)
    cls.__reqCache[reqID] = getRequest

    return S_OK(cls.__reqCache[reqID])

  @classmethod
  def putRequest(cls, request, clearCache=True):
    """ put request back to ReqDB

    :param ~DIRAC.RequestManagementSystem.Client.Request.Request request: Request instance
    :param bool clearCache: clear the cache?

    also finalize request if status == Done
    """
    # # put back request
    if request.RequestID not in cls.__reqCache:
      return S_OK()
    put = cls.requestClient().putRequest(request)
    if not put["OK"]:
      return put
    # # finalize first if possible
    if request.Status == "Done" and request.JobID:
      finalizeRequest = cls.requestClient().finalizeRequest(request.RequestID, request.JobID)
      if not finalizeRequest["OK"]:
        request.Status = "Scheduled"
    # # del request from cache if needed
    if clearCache:
      cls.__reqCache.pop(request.RequestID, None)
    return S_OK()

  @classmethod
  def putFTSJobs(cls, ftsJobsList):
    """ put back fts jobs to the FTSDB """
    for ftsJob in ftsJobsList:
      put = cls.ftsClient().putFTSJob(ftsJob)
      if not put["OK"]:
        return put
    return S_OK()

  @staticmethod
  def updateFTSFileDict(ftsFilesDict, toUpdateDict):
    """ update `ftsFilesDict` with FTSFiles in `toUpdateDict` """
    for category, ftsFileList in ftsFilesDict.iteritems():
      for ftsFile in toUpdateDict.get(category, []):
        if ftsFile not in ftsFileList:
          ftsFileList.append(ftsFile)
    return ftsFilesDict

  def threadPool(self):
    """ thread pool getter """
    if not self.__threadPool:
      self.__threadPool = ThreadPool(self.MIN_THREADS, self.MAX_THREADS)
      self.__threadPool.daemonize()
    return self.__threadPool

  def resetFTSPlacement(self):
    """ create fts Placement """

    ftsHistory = self.ftsClient().getFTSHistory()
    if not ftsHistory["OK"]:
      self.log.error("unable to get FTS history:", ftsHistory["Message"])
      return ftsHistory
    ftsHistory = ftsHistory["Value"]

    try:
      self.updateLock().acquire()
      if not self.__ftsPlacement:
        self.__ftsPlacement = FTSPlacement(csPath=None, ftsHistoryViews=ftsHistory)
      else:
        self.__ftsPlacement.refresh(ftsHistoryViews=ftsHistory)
    finally:
      self.updateLock().release()

    # # save time stamp
    self.__ftsPlacementValidStamp = datetime.datetime.now(
    ) + datetime.timedelta(seconds=self.FTSPLACEMENT_REFRESH)

    return S_OK()

  def __init__(self, agentName, loadName, baseAgentName=False, properties=None):

    if properties is None:
      properties = {}
    super(FTSAgent, self).__init__(agentName, loadName, baseAgentName=baseAgentName, properties=properties)

    self.__factorOnMaxRequest = 3.

  def initialize(self):
    """ agent's initialization """

    # # data manager
    self.dataManager = DataManager()

    log = self.log.getSubLogger("initialize")

    self.FTSPLACEMENT_REFRESH = self.am_getOption(
        "FTSPlacementValidityPeriod", self.FTSPLACEMENT_REFRESH)
    log.info("FTSPlacement validity period       = %s s" %
             self.FTSPLACEMENT_REFRESH)

    self.SUBMIT_COMMAND = self.am_getOption(
        "SubmitCommand", self.SUBMIT_COMMAND)
    log.info("FTS submit command = %s" % self.SUBMIT_COMMAND)
    self.MONITOR_COMMAND = self.am_getOption(
        "MonitorCommand", self.MONITOR_COMMAND)
    log.info("FTS commands: submit = %s monitor %s" %
             (self.SUBMIT_COMMAND, self.MONITOR_COMMAND))
    self.PIN_TIME = self.am_getOption("PinTime", self.PIN_TIME)
    log.info("Stage files before submission  = ", {
             True: "yes", False: "no"}[bool(self.PIN_TIME)])

    self.MAX_ACTIVE_JOBS = self.am_getOption(
        "MaxActiveJobsPerRoute", self.MAX_ACTIVE_JOBS)
    log.info("Max active FTSJobs/route       = ", str(self.MAX_ACTIVE_JOBS))
    self.MAX_FILES_PER_JOB = self.am_getOption(
        "MaxFilesPerJob", self.MAX_FILES_PER_JOB)
    log.info("Max FTSFiles/FTSJob            = ", str(self.MAX_FILES_PER_JOB))

    self.MAX_ATTEMPT = self.am_getOption(
        "MaxTransferAttempts", self.MAX_ATTEMPT)
    log.info("Max transfer attempts          = ", str(self.MAX_ATTEMPT))

    # # thread pool
    self.MIN_THREADS = self.am_getOption("MinThreads", self.MIN_THREADS)
    self.MAX_THREADS = self.am_getOption("MaxThreads", self.MAX_THREADS)
    minmax = (abs(self.MIN_THREADS), abs(self.MAX_THREADS))
    self.MIN_THREADS, self.MAX_THREADS = min(minmax), max(minmax)
    log.info("ThreadPool min threads         = ", str(self.MIN_THREADS))
    log.info("ThreadPool max threads         = ", str(self.MAX_THREADS))

    self.MAX_REQUESTS = self.am_getOption("MaxRequests", self.MAX_REQUESTS)
    log.info("Max Requests fetched           = ", str(self.MAX_REQUESTS))

    self.MONITORING_INTERVAL = self.am_getOption(
        "MonitoringInterval", self.MONITORING_INTERVAL)
    log.info("Minimum monitoring interval    = ",
             str(self.MONITORING_INTERVAL))

    self.PROCESS_JOB_REQUESTS = self.am_getOption(
        "ProcessJobRequests", self.PROCESS_JOB_REQUESTS)
    # We get a string as the default value is None... better than an eval()!
    self.PROCESS_JOB_REQUESTS = {'True': True, 'False': False}.get(
        self.PROCESS_JOB_REQUESTS, self.PROCESS_JOB_REQUESTS)
    if self.PROCESS_JOB_REQUESTS is not None:
      log.info("Process job requests           = ",
               str(self.PROCESS_JOB_REQUESTS))

    self.__ftsVersion = Operations().getValue('DataManagement/FTSVersion', 'FTS2')
    log.info("FTSVersion : %s" % self.__ftsVersion)
    log.info("initialize: creation of FTSPlacement...")
    createPlacement = self.resetFTSPlacement()
    if not createPlacement["OK"]:
      log.error("initialize:", createPlacement["Message"])
      return createPlacement

    # This sets the Default Proxy to used as that defined under
    # /Operations/Shifter/DataManager
    # the shifterProxy option in the Configuration can be used to change this default.
    self.am_setOption('shifterProxy', 'DataManager')
    log.info("will use DataManager proxy")

    self.registrationProtocols = DMSHelpers().getRegistrationProtocols()

    # # gMonitor stuff here
    gMonitor.registerActivity("RequestsAtt", "Attempted requests executions",
                              "FTSAgent", "Requests/min", gMonitor.OP_SUM)
    gMonitor.registerActivity("RequestsOK", "Successful requests executions",
                              "FTSAgent", "Requests/min", gMonitor.OP_SUM)
    gMonitor.registerActivity("RequestsFail", "Failed requests executions",
                              "FTSAgent", "Requests/min", gMonitor.OP_SUM)

    gMonitor.registerActivity("FTSJobsSubAtt", "FTSJobs creation attempts",
                              "FTSAgent", "Created FTSJobs/min", gMonitor.OP_SUM)
    gMonitor.registerActivity("FTSJobsSubOK", "FTSJobs submitted successfully",
                              "FTSAgent", "Successful FTSJobs submissions/min", gMonitor.OP_SUM)
    gMonitor.registerActivity("FTSJobsSubFail", "FTSJobs submissions failed",
                              "FTSAgent", "Failed FTSJobs submissions/min", gMonitor.OP_SUM)

    gMonitor.registerActivity("FTSJobsMonAtt", "FTSJobs monitored",
                              "FTSAgent", "FTSJobs/min", gMonitor.OP_SUM)
    gMonitor.registerActivity("FTSJobsMonOK", "FTSJobs monitored successfully",
                              "FTSAgent", "FTSJobs/min", gMonitor.OP_SUM)
    gMonitor.registerActivity("FTSJobsMonFail", "FTSJobs attempts failed",
                              "FTSAgent", "FTSJobs/min", gMonitor.OP_SUM)

    gMonitor.registerActivity("FTSMonitorFail", "Failed FTS monitor executions",
                              "FTSAgent", "Execution/mins", gMonitor.OP_SUM)

    pollingTime = self.am_getOption("PollingTime", 60)
    for status in list(FTSJob.INITSTATES + FTSJob.TRANSSTATES + FTSJob.FAILEDSTATES + FTSJob.FINALSTATES):
      gMonitor.registerActivity("FTSJobs%s" % status, "FTSJobs %s" % status,
                                "FTSAgent", "FTSJobs/cycle", gMonitor.OP_ACUM, pollingTime)

    gMonitor.registerActivity("FtSJobsPerRequest", "Average FTSJobs per request",
                              "FTSAgent", "FTSJobs/Request", gMonitor.OP_MEAN)
    gMonitor.registerActivity("FTSFilesPerJob", "FTSFiles per FTSJob",
                              "FTSAgent", "Number of FTSFiles per FTSJob", gMonitor.OP_MEAN)
    gMonitor.registerActivity("FTSSizePerJob", "Average FTSFiles size per FTSJob",
                              "FTSAgent", "Average submitted size per FTSJob", gMonitor.OP_MEAN)
    return S_OK()

  def finalize(self):
    """ finalize processing """
    # log = self.log.getSubLogger( "finalize" )
    # if self.__reqCache:
    #  log.info( 'putting back %d requests from cache' % len( self.__reqCache ) )
    # else:
    #  log.info( 'no requests to put back' )
    # for request in self.__reqCache.values():
    #  put = self.requestClient().putRequest( request )
    #  if not put["OK"]:
    #    log.error( "unable to put back request '%s': %s" % ( request.RequestName, put["Message"] ) )
    return S_OK()

  def execute(self):
    """ one cycle execution """

    # Don't use the server certificate otherwise the DFC wont let us write
    gConfigurationData.setOptionInCFG('/DIRAC/Security/UseServerCertificate', 'false')

    log = gLogger.getSubLogger("execute")
    # # reset FTSPlacement if expired
    now = datetime.datetime.now()
    if now > self.__ftsPlacementValidStamp:
      log.info("resetting expired FTS placement...")
      resetFTSPlacement = self.resetFTSPlacement()
      if not resetFTSPlacement["OK"]:
        log.error("FTSPlacement recreation error:", resetFTSPlacement["Message"])
        return resetFTSPlacement
      self.__ftsPlacementValidStamp = now + \
          datetime.timedelta(seconds=self.FTSPLACEMENT_REFRESH)

    # To be sure we have enough requests, ask for several times as much
    requestIDs = self.requestClient().getRequestIDsList(
        statusList=["Scheduled"], limit=int(
            self.__factorOnMaxRequest * self.MAX_REQUESTS), getJobID=True)
    if not requestIDs["OK"]:
      log.error("unable to read scheduled request ids", requestIDs["Message"])
      return requestIDs
    if not requestIDs["Value"]:
      requestIDs = []
    elif self.PROCESS_JOB_REQUESTS is None:
      requestIDs = [req[0] for req in requestIDs["Value"] if req[0] not in self.__reqCache]
    else:
      # If we want to process requests only with JobID or only without jobID, make a selection
      requestIDs = [req[0] for req in requestIDs["Value"]
                    if req[0] not in self.__reqCache and
                    len(req) >= 4 and bool(req[3]) == self.PROCESS_JOB_REQUESTS]

    # Correct the factor by the observed ratio between needed and obtained, but limit between 1 and 5
    gotRequests = len(requestIDs) + 1
    neededRequests = self.MAX_REQUESTS - len(self.__reqCache)
    self.__factorOnMaxRequest = max(1,
                                    min(10,
                                        math.ceil(self.__factorOnMaxRequest * neededRequests / float(gotRequests))))

    # We took more but keep only the maximum number
    requestIDs = requestIDs[:neededRequests] + self.__reqCache.keys()

    if not requestIDs:
      log.info("no 'Scheduled' requests to process")
      return S_OK()

    log.info("found %s requests to process:" % len(requestIDs))
    log.info(" => from internal cache: %s" % (len(self.__reqCache)))
    log.info(" =>   new read from RMS: %s" % (len(requestIDs) - len(self.__reqCache)))

    for requestID in requestIDs:
      request = self.getRequest(requestID)
      if not request["OK"]:
        log.error("Error getting request", "%s: %s" % (requestID, request["Message"]))
        continue
      request = request["Value"]
      sTJId = request.RequestID
      fullLogged = 0
      while True:
        queue = self.threadPool().generateJobAndQueueIt(self.processRequest,
                                                        args=(request, ),
                                                        sTJId=sTJId)
        if queue["OK"]:
          log.info("Request enqueued for execution%s" %
                   ((' (after waiting %d seconds)' % fullLogged) if fullLogged else ''),
                   sTJId)
          gMonitor.addMark("RequestsAtt", 1)
          break
        if not fullLogged:
          log.info("Queue is full, wait 1 second to enqueue")
        fullLogged += 1
        time.sleep(1)

    # # process all results
    self.threadPool().processAllResults()
    return S_OK()

  def processRequest(self, request):
    """ process one request

    :param ~DIRAC.RequestManagementSystem.Client.Request.Request request: ReqDB.Request
    """
    log = self.log.getSubLogger("req_%s/%s" % (request.RequestID, request.RequestName))

    operation = request.getWaiting()
    if not operation["OK"]:
      log.error("Unable to find 'Scheduled' ReplicateAndRegister operation in request")
      return self.putRequest(request)
    operation = operation["Value"]
    if not isinstance(operation, Operation):
      log.error("Waiting returned operation is not an operation:", type(operation))
      return self.putRequest(request)
    if operation.Type != "ReplicateAndRegister":
      log.error("operation to be executed is not a ReplicateAndRegister but", operation.Type)
      return self.putRequest(request)
    if operation.Status != "Scheduled":
      log.error("operation in a wrong state, expecting 'Scheduled', got", operation.Status)
      return self.putRequest(request)

    log.info('start processRequest')
    # # select  FTSJobs, by default all in TRANS_STATES and INIT_STATES
    ftsJobs = self.ftsClient().getFTSJobsForRequest(request.RequestID)
    if not ftsJobs["OK"]:
      log.error(ftsJobs["Message"])
      return ftsJobs
    ftsJobs = [ftsJob for ftsJob in ftsJobs.get("Value", []) if ftsJob.Status not in FTSJob.FINALSTATES]

    # # Use a try: finally: for making sure FTS jobs are put back before returning
    try:
      # # dict keeping info about files to reschedule, submit, fail and register
      ftsFilesDict = dict((k, list()) for k in ("toRegister", "toSubmit", "toFail", "toReschedule", "toUpdate"))

      now = datetime.datetime.utcnow()
      jobsToMonitor = [job for job in ftsJobs if
                       (now - job.LastUpdate).seconds >
                       (self.MONITORING_INTERVAL *
                        (3. if StorageElement(job.SourceSE).status()['TapeSE'] else 1.))
                       ]
      if jobsToMonitor:
        log.info("==> found %s FTSJobs to monitor" % len(jobsToMonitor))
        # # PHASE 0 = monitor active FTSJobs
        for ftsJob in jobsToMonitor:
          monitor = self.__monitorJob(request, ftsJob)
          if not monitor["OK"]:
            log.error("unable to monitor FTSJob", "%s: %s" % (ftsJob.FTSJobID, monitor["Message"]))
            ftsJob.Status = "Submitted"
          else:
            ftsFilesDict = self.updateFTSFileDict(ftsFilesDict, monitor["Value"])

        log.info("monitoring of FTSJobs completed")
        for key, ftsFiles in ftsFilesDict.iteritems():
          if ftsFiles:
            log.info(" => %d FTSFiles to %s" % (len(ftsFiles), key[2:].lower()))
      if len(ftsJobs) != len(jobsToMonitor):
        log.info("==> found %d FTSJobs that were monitored recently" % (len(ftsJobs) - len(jobsToMonitor)))
        if not jobsToMonitor:
          # Nothing to happen this time, escape
          raise EscapeTryException

      # # PHASE ONE - check ready replicas
      missingReplicas = self.__checkReadyReplicas(request, operation)
      if not missingReplicas["OK"]:
        log.error(missingReplicas["Message"])
      else:
        missingReplicas = missingReplicas["Value"]
        for opFile in operation:
          # Actually the condition below should never happen... Change printout for checking
          if opFile.LFN not in missingReplicas and opFile.Status not in ('Done', 'Failed'):
            log.warn("File should be set Done! %s is replicated at all targets" % opFile.LFN)
            opFile.Status = "Done"

        if missingReplicas:
          # Check if these files are in the FTSDB
          ftsFiles = self.ftsClient().getAllFTSFilesForRequest(request.RequestID)
          if not ftsFiles['OK']:
            log.error(ftsFiles['Message'])
          else:
            ftsFiles = ftsFiles['Value']
            ftsLfns = set(ftsFile.LFN for ftsFile in ftsFiles)
            # Recover files not in FTSDB
            toSchedule = set(missingReplicas) - ftsLfns
            if toSchedule:
              log.warn('%d files in operation are not in FTSDB, reset them Waiting' % len(toSchedule))
              for opFile in operation:
                if opFile.LFN in toSchedule and opFile.Status == 'Scheduled':
                  opFile.Status = 'Waiting'
            # Recover files with target not in FTSDB
            toSchedule = set(missing for missing, missingSEs in missingReplicas.iteritems()
                             if not [ftsFile for ftsFile in ftsFiles
                                     if ftsFile.LFN == missing and ftsFile.TargetSE in missingSEs])
            if toSchedule:
              log.warn('%d targets in operation are not in FTSDB, reset files Waiting' % len(toSchedule))
              for opFile in operation:
                if opFile.LFN in toSchedule and opFile.Status == 'Scheduled':
                  opFile.Status = 'Waiting'
            # identify missing LFNs that are waiting for a replication which is finished
            for ftsFile in [f for f in ftsFiles if f.LFN in missingReplicas and f.Status.startswith('Waiting#')]:
              targetSE = ftsFile.Status.split('#')[1]
              finishedFiles = [f for f in ftsFiles if
                               f.LFN == ftsFile.LFN and
                               f.Status == 'Finished' and
                               f.TargetSE == targetSE and
                               f not in ftsFilesDict['toUpdate']]
              if finishedFiles:
                log.warn(
                    "%s is %s while replication was Finished to %s, update" %
                    (ftsFile.LFN, ftsFile.Status, targetSE))
                ftsFilesDict['toUpdate'] += finishedFiles
            # identify Active transfers for which there is no FTS job any longer and reschedule them
            for ftsFile in [f for f in ftsFiles if f.Status ==
                            'Active' and f.TargetSE in missingReplicas.get(f.LFN, [])]:
              if not [ftsJob for ftsJob in ftsJobs if ftsJob.FTSGUID == ftsFile.FTSGUID]:
                ftsFilesDict['toReschedule'].append(ftsFile)
            # identify Finished transfer for which the replica is still missing
            for ftsFile in [f for f in ftsFiles if f.Status == 'Finished' and f.TargetSE in missingReplicas.get(
                    f.LFN, []) and f not in ftsFilesDict['toRegister']]:
              # Check if there is a registration operation for that file and that target
              regOp = [op for op in request if
                       op.Type == 'RegisterReplica' and
                       op.TargetSE == ftsFile.TargetSE and
                       [f for f in op if f.LFN == ftsFile.LFN]]
              if not regOp:
                ftsFilesDict['toReschedule'].append(ftsFile)

            # Recover files that are Failed but were not spotted
            for ftsFile in [f for f in ftsFiles if f.Status ==
                            'Failed' and f.TargetSE in missingReplicas.get(f.LFN, [])]:
              reschedule, submit, fail = self.__checkFailed(ftsFile)
              if fail and ftsFile not in ftsFilesDict['toFail']:
                ftsFilesDict['toFail'].append(ftsFile)
              elif reschedule and ftsFile not in ftsFilesDict['toReschedule']:
                ftsFilesDict['toReschedule'].append(ftsFile)
              elif submit and ftsFile not in ftsFilesDict['toSubmit']:
                ftsFilesDict['toSubmit'].append(ftsFile)

            # If all transfers are finished for unregistered files and there is
            # already a registration operation, set it Done
            ftsLFNs = [f.LFN for f in ftsFiles]
            for lfn in missingReplicas:
              # We make sure here that the file is being processed by FTS
              if lfn in ftsLFNs:
                if not [f for f in ftsFiles if
                        f.LFN == lfn and
                        (f.Status != 'Finished' or
                         f in ftsFilesDict['toReschedule'] or
                         f in ftsFilesDict['toRegister'])]:
                  for opFile in operation:
                    if opFile.LFN == lfn:
                      opFile.Status = 'Done'
                      break
              else:
                # Temporary log
                log.warn("File with missing replica not in FTS files", lfn)
          for key, ftsFiles in ftsFilesDict.iteritems():
            if ftsFiles:
              log.info(" => %d FTSFiles to %s" % (len(ftsFiles), key[2:].lower()))

      toFail = ftsFilesDict.get("toFail", [])
      toReschedule = ftsFilesDict.get("toReschedule", [])
      toSubmit = ftsFilesDict.get("toSubmit", [])
      toRegister = ftsFilesDict.get("toRegister", [])
      toUpdate = ftsFilesDict.get("toUpdate", [])

      # # PHASE TWO = Failed files? -> make request Failed and return
      if toFail:
        log.error("==> found %d 'Failed' FTSFiles, but maybe other files can be processed..." % len(toFail))
        for opFile in operation:
          for ftsFile in toFail:
            if opFile.FileID == ftsFile.FileID:
              opFile.Error = ftsFile.Error
              opFile.Status = "Failed"
        operation.Error = "%s files are missing any replicas" % len(toFail)
        # # requets.Status should be Failed if all files in the operation "Failed"
        if request.Status == "Failed":
          request.Error = "ReplicateAndRegister %s failed" % operation.Order
          log.error("request is set to 'Failed'")
          # # putRequest is done by the finally: clause... Not good to do it twice
          raise EscapeTryException

      # # PHASE THREE - update Waiting#TargetSE FTSFiles
      if toUpdate:
        log.info("==> found %s possible FTSFiles to update..." % (len(toUpdate)))
        byTarget = {}
        for ftsFile in toUpdate:
          byTarget.setdefault(ftsFile.TargetSE, []).append(ftsFile.FileID)
        for targetSE, fileIDList in byTarget.iteritems():
          update = self.ftsClient().setFTSFilesWaiting(operation.OperationID, targetSE, fileIDList)
          if not update["OK"]:
            log.error("update FTSFiles failed:", update["Message"])

      # # PHASE FOUR - add 'RegisterReplica' Operations
      if toRegister:
        log.info("==> found %d Files waiting for registration, adding 'RegisterReplica' operations" % len(toRegister))
        registerFiles = self.__insertRegisterOperation(request, operation, toRegister)
        if not registerFiles["OK"]:
          log.error("unable to create 'RegisterReplica' operations:", registerFiles["Message"])
        # if request.Status == "Waiting":
        #  log.info( "request is in 'Waiting' state, will put it back to RMS" )
        #  return self.putRequest( request )

      # # PHASE FIVE - reschedule operation files
      if toReschedule:
        log.info("==> found %s Files to reschedule" % len(toReschedule))
        rescheduleFiles = self.__reschedule(request, operation, toReschedule)
        if not rescheduleFiles["OK"]:
          log.error('Failed to reschedule files', rescheduleFiles["Message"])

      # # PHASE SIX - read Waiting ftsFiles and submit new FTSJobs. We get also Failed files to recover them if needed
      ftsFiles = self.ftsClient().getFTSFilesForRequest(
          request.RequestID, ["Waiting", "Failed", 'Submitted', 'Canceled'])
      if not ftsFiles["OK"]:
        log.error(ftsFiles["Message"])
      else:
        retryIds = set(ftsFile.FTSFileID for ftsFile in toSubmit)
        for ftsFile in ftsFiles["Value"]:
          if ftsFile.FTSFileID not in retryIds:
            if ftsFile.Status in ('Failed', 'Canceled'):
              # If the file was not unrecoverable failed and is not yet set toSubmit
              _reschedule, submit, _fail = self.__checkFailed(ftsFile)
            elif ftsFile.Status == 'Submitted':
              if ftsFile.FTSGUID not in [job.FTSGUID for job in ftsJobs]:
                log.warn('FTS GUID %s not found in FTS jobs, resubmit file transfer' % ftsFile.FTSGUID)
                ftsFile.Status = 'Waiting'
                submit = True
              else:
                submit = False
            else:
              submit = True
            if submit:
              toSubmit.append(ftsFile)
              retryIds.add(ftsFile.FTSFileID)

      # # should not put back jobs that have not been monitored this time
      ftsJobs = jobsToMonitor
      # # submit new ftsJobs
      if toSubmit:
        if request.Status != 'Scheduled':
          log.info("Found %d FTSFiles to submit while request is no longer in Scheduled status (%s)"
                   % (len(toSubmit), request.Status))
        else:
          self.__checkDuplicates(request.RequestID, toSubmit)
          log.info("==> found %s FTSFiles to submit" % len(toSubmit))
          submit = self.__submit(request, operation, toSubmit)
          if not submit["OK"]:
            log.error(submit["Message"])
          else:
            ftsJobs += submit["Value"]

      # # status change? - put back request
      if request.Status != "Scheduled":
        log.info("request no longer in 'Scheduled' state (%s), will put it back to RMS" % request.Status)

    except EscapeTryException:
      # This clause is raised when one wants to return from within the try: clause
      # only put back jobs that were monitored
      ftsJobs = jobsToMonitor
    except Exception as exceptMessage:
      log.exception("Exception in processRequest", lException=exceptMessage)
    finally:
      putRequest = self.putRequest(request, clearCache=(request.Status != "Scheduled"))
      if not putRequest["OK"]:
        log.error("unable to put back request:", putRequest["Message"])
      # #  put back jobs in all cases
      if ftsJobs:
        for ftsJob in list(ftsJobs):
          if not len(ftsJob):
            log.warn('FTS job empty, removed: %s' % ftsJob.FTSGUID)
            self.ftsClient().deleteFTSJob(ftsJob.FTSJobID)
            ftsJobs.remove(ftsJob)
        putJobs = self.putFTSJobs(ftsJobs)
        if not putJobs["OK"]:
          log.error("unable to put back FTSJobs:", putJobs["Message"])
          putRequest = putJobs
    # This is where one returns from after execution of the finally: block
    return putRequest

  def __checkDuplicates(self, reqID, toSubmit):
    """ Check in a list of FTSFiles whether there are duplicates
    """
    tupleList = []
    log = self.log.getSubLogger("%s/checkDuplicates" % reqID)
    for ftsFile in list(toSubmit):
      fTuple = (ftsFile.LFN, ftsFile.SourceSE, ftsFile.TargetSE)
      if fTuple in tupleList:
        log.warn("Duplicate file to submit, removed:", ', '.join(fTuple))
        toSubmit.remove(ftsFile)
        self.ftsClient().deleteFTSFiles(ftsFile.OperationID, [ftsFile.FileID])
      else:
        tupleList.append(fTuple)

  def __reschedule(self, request, operation, toReschedule):
    """ reschedule list of :toReschedule: files in request for operation :operation:

    :param Request request:
    :param Operation operation:
    :param list toReschedule: list of FTSFiles
    """
    log = self.log.getSubLogger("req_%s/%s/reschedule" % (request.RequestID, request.RequestName))

    ftsFileIDs = [ftsFile.FileID for ftsFile in toReschedule]
    for opFile in operation:
      if opFile.FileID in ftsFileIDs:
        opFile.Status = "Waiting"

    toSchedule = []

    # # filter files
    for opFile in [opf for opf in operation if opf.Status == "Waiting"]:

      replicas = self.__filterReplicas(opFile)
      if not replicas["OK"]:
        continue
      replicas = replicas["Value"]
      validReplicas = replicas.get("Valid")
      noMetaReplicas = replicas.get("NoMetadata")
      noReplicas = replicas.get('NoReplicas')
      badReplicas = replicas.get('Bad')
      noActiveReplicas = replicas.get('NoActiveReplicas')

      if validReplicas:
        validTargets = list(set(operation.targetSEList) - set(validReplicas))
        if not validTargets:
          log.info("file %s is already present at all targets" % opFile.LFN)
          opFile.Status = "Done"
        else:
          toSchedule.append((opFile.toJSON()["Value"], validReplicas, validTargets))
      elif noMetaReplicas:
        log.warn("unable to schedule '%s', couldn't get metadata at %s" %
                 (opFile.LFN, ','.join(noMetaReplicas)))
      elif noReplicas:
        log.warn("unable to schedule %s, file doesn't exist at %s" %
                 (opFile.LFN, ','.join(noReplicas)))
        opFile.Status = 'Failed'
      elif badReplicas:
        log.warn("unable to schedule %s, all replicas have a bad checksum at %s" %
                 (opFile.LFN, ','.join(badReplicas)))
        opFile.Status = 'Failed'
      elif noActiveReplicas:
        log.warn("unable to schedule '%s', couldn't find active replicas at %s" %
                 (opFile.LFN, ','.join(noActiveReplicas)))
        request.delayNextExecution(60)

    # # do real schedule here
    if toSchedule:
      log.info("Rescheduling %d files" % len(toReschedule))
      ftsSchedule = self.ftsClient().ftsSchedule(request.RequestID,
                                                 operation.OperationID,
                                                 toSchedule)
      if not ftsSchedule["OK"]:
        log.error("Error scheduling files", ftsSchedule["Message"])
        return ftsSchedule

      ftsSchedule = ftsSchedule["Value"]
      for opFile in operation:
        fileID = opFile.FileID
        if fileID in ftsSchedule["Successful"]:
          opFile.Status = "Scheduled"
        elif fileID in ftsSchedule["Failed"]:
          opFile.Error = ftsSchedule["Failed"][fileID]
          log.error("Error scheduling file %s" % opFile.LFN, opFile.Error)

    return S_OK()

  def __submit(self, request, operation, toSubmit):
    """ create and submit new FTSJobs using list of FTSFiles

    :param Request request: ReqDB.Request instance
    :param list ftsFiles: list of FTSFile instances

    :return: [ FTSJob, FTSJob, ...]
    """
    log = self.log.getSubLogger("req_%s/%s/submit" % (request.RequestID, request.RequestName))

    bySourceAndTarget = {}
    for ftsFile in toSubmit:
      if ftsFile.SourceSE not in bySourceAndTarget:
        bySourceAndTarget.setdefault(ftsFile.SourceSE, {})
      if ftsFile.TargetSE not in bySourceAndTarget[ftsFile.SourceSE]:
        bySourceAndTarget[ftsFile.SourceSE].setdefault(ftsFile.TargetSE, [])
      bySourceAndTarget[ftsFile.SourceSE][ftsFile.TargetSE].append(ftsFile)

    ftsJobs = []

    for source, targetDict in bySourceAndTarget.iteritems():

      for target, ftsFileList in targetDict.iteritems():

        log.info("found %s files to submit from %s to %s" % (len(ftsFileList), source, target))

        route = self.__ftsPlacement.findRoute(source, target)
        if not route["OK"]:
          log.error(route["Message"])
          continue
        route = route["Value"]

        routeValid = self.__ftsPlacement.isRouteValid(route)

        if not routeValid['OK']:
          log.error("Route invalid : %s" % routeValid['Message'])
          continue

        sourceSE = StorageElement(source)
        sourceToken = sourceSE.getStorageParameters(protocol='srm')
        if not sourceToken["OK"]:
          log.error("unable to get sourceSE parameters:", "(%s) %s" % (source, sourceToken["Message"]))
          continue
        seStatus = sourceSE.status()

        targetSE = StorageElement(target)
        targetToken = targetSE.getStorageParameters(protocol='srm')
        if not targetToken["OK"]:
          log.error("unable to get targetSE parameters:", "(%s) %s" % (target, targetToken["Message"]))
          continue

        # # create FTSJob
        for fileList in breakListIntoChunks(ftsFileList, self.MAX_FILES_PER_JOB):
          ftsJob = FTSJob()
          ftsJob.RequestID = request.RequestID
          ftsJob.OperationID = operation.OperationID
          ftsJob.SourceSE = source
          ftsJob.TargetSE = target
          ftsJob.SourceToken = sourceToken["Value"].get("SpaceToken", "")
          ftsJob.TargetToken = targetToken["Value"].get("SpaceToken", "")
          ftsJob.FTSServer = route.ftsServer

          for ftsFile in fileList:
            ftsFile.Attempt += 1
            ftsFile.Error = ""
            ftsJob.addFile(ftsFile)

          submit = ftsJob.submitFTS(self.__ftsVersion, command=self.SUBMIT_COMMAND,
                                    pinTime=self.PIN_TIME if seStatus['TapeSE'] else 0)
          if not submit["OK"]:
            log.error("unable to submit FTSJob:", submit["Message"])
            continue

          log.info("FTSJob '%s'@'%s' has been submitted" % (ftsJob.FTSGUID, ftsJob.FTSServer))

          # # update statuses for job files
          for ftsFile in ftsJob:
            ftsFile.FTSGUID = ftsJob.FTSGUID
            ftsFile.Status = "Submitted"
            ftsFile.Attempt += 1

          # # update placement route
          try:
            self.updateLock().acquire()
            self.__ftsPlacement.startTransferOnRoute(route)
          finally:
            self.updateLock().release()

          ftsJobs.append(ftsJob)

    log.info("%s new FTSJobs have been submitted" % len(ftsJobs))
    return S_OK(ftsJobs)

  def __monitorJob(self, request, ftsJob):
    """ execute FTSJob.monitorFTS for a given :ftsJob:
        if ftsJob is in a final state, finalize it

    :param Request request: ReqDB.Request instance
    :param FTSJob ftsJob: FTSDB.FTSJob instance
    """
    log = self.log.getSubLogger("req_%s/%s/monitor/%s" % (request.RequestID, request.RequestName, ftsJob.FTSGUID))
    log.info("FTSJob '%s'@'%s'" % (ftsJob.FTSGUID, ftsJob.FTSServer))

    # # this will be returned
    ftsFilesDict = dict((k, list()) for k in ("toRegister", "toSubmit", "toFail", "toReschedule", "toUpdate"))

    monitor = ftsJob.monitorFTS(
        self.__ftsVersion, command=self.MONITOR_COMMAND)
    if not monitor["OK"]:
      gMonitor.addMark("FTSMonitorFail", 1)
      log.error(monitor["Message"])
      if "getTransferJobSummary2: Not authorised to query request" in monitor["Message"] or\
         'was not found' in monitor['Message'] or\
         "Not found" in monitor['Message'] or\
         'Unknown transfer state' in monitor['Message']:
        log.error("FTSJob not known (expired on server?): delete it")
        for ftsFile in ftsJob:
          ftsFile.Status = "Waiting"
          ftsFilesDict["toSubmit"].append(ftsFile)
        # #  No way further for that job: delete it
        res = self.ftsClient().deleteFTSJob(ftsJob.FTSJobID)
        if not res['OK']:
          log.error("Unable to delete FTSJob", res['Message'])
        return S_OK(ftsFilesDict)
      return monitor

    monitor = monitor["Value"]
    log.info("FTSJob Status = %s Completeness = %s%%" % (ftsJob.Status, ftsJob.Completeness))

    # # monitor status change
    gMonitor.addMark("FTSJobs%s" % ftsJob.Status, 1)

    if ftsJob.Status in FTSJob.FINALSTATES:
      finalizeFTSJob = self.__finalizeFTSJob(request, ftsJob)
      if not finalizeFTSJob["OK"]:
        if 'Unknown transfer state' in finalizeFTSJob['Message']:
          for ftsFile in ftsJob:
            ftsFile.Status = "Waiting"
            ftsFilesDict["toSubmit"].append(ftsFile)
          # #  No way further for that job: delete it
          res = self.ftsClient().deleteFTSJob(ftsJob.FTSJobID)
          if not res['OK']:
            log.error("Unable to delete FTSJob", res['Message'])
        else:
          log.error(finalizeFTSJob["Message"])
          return finalizeFTSJob
      else:
        ftsFilesDict = self.updateFTSFileDict(ftsFilesDict, finalizeFTSJob["Value"])

    return S_OK(ftsFilesDict)

  def __finalizeFTSJob(self, request, ftsJob):
    """ finalize FTSJob

    :param ~DIRAC.RequestManagementSystem.Client.Request.Request request: ReqDB.Request instance
    :param FTSJob ftsJob: FTSDB.FTSJob instance
    """
    log = self.log.getSubLogger("req_%s/%s/monitor/%s/finalize" % (request.RequestID,
                                                                   request.RequestName,
                                                                   ftsJob.FTSJobID))
    log.info("finalizing FTSJob %s@%s" % (ftsJob.FTSGUID, ftsJob.FTSServer))

    # # this will be returned
    ftsFilesDict = dict((k, list()) for k in ("toRegister", "toSubmit", "toFail", "toReschedule", "toUpdate"))

    monitor = ftsJob.monitorFTS(
        self.__ftsVersion, command=self.MONITOR_COMMAND, full=True)
    if not monitor["OK"]:
      log.error(monitor["Message"])
      return monitor

    # # split FTSFiles to different categories
    processFiles = self.__filterFiles(ftsJob)
    if not processFiles["OK"]:
      log.error(processFiles["Message"])
      return processFiles
    processFiles = processFiles['Value']
    if processFiles['toRegister']:
      log.error("Some files could not be registered in FC:", len(processFiles['toRegister']))
    ftsFilesDict = self.updateFTSFileDict(ftsFilesDict, processFiles)

    # # send accounting record for this job
    self.__sendAccounting(ftsJob, request.OwnerDN)

    # # update placement - remove this job from placement
    route = self.__ftsPlacement.findRoute(ftsJob.SourceSE, ftsJob.TargetSE)
    if route["OK"]:
      try:
        self.updateLock().acquire()
        self.__ftsPlacement.finishTransferOnRoute(route['Value'])
      finally:
        self.updateLock().release()

    log.info("FTSJob is finalized")

    return S_OK(ftsFilesDict)

  def __checkFailed(self, ftsFile):
    reschedule = False
    submit = False
    fail = False
    if ftsFile.Status in ("Failed", 'Canceled'):
      if ftsFile.Error == "MissingSource":
        reschedule = True
      else:
        submit = bool(ftsFile.Attempt < self.MAX_ATTEMPT)
        fail = not submit
    return reschedule, submit, fail

  def __filterFiles(self, ftsJob):
    """ process ftsFiles from finished ftsJob

    :param FTSJob ftsJob: monitored FTSJob instance
    """
    # # lists for different categories
    toUpdate = []
    toReschedule = []
    toRegister = []
    toSubmit = []
    toFail = []

    # # loop over files in fts job
    for ftsFile in ftsJob:
      # # successful files
      if ftsFile.Status == "Finished":
        if ftsFile.Error == "AddCatalogReplicaFailed":
          toRegister.append(ftsFile)
        toUpdate.append(ftsFile)
        continue
      reschedule, submit, fail = self.__checkFailed(ftsFile)
      if reschedule:
        toReschedule.append(ftsFile)
      elif submit:
        toSubmit.append(ftsFile)
      elif fail:
        toFail.append(ftsFile)

    return S_OK({"toUpdate": toUpdate,
                 "toSubmit": toSubmit,
                 "toRegister": toRegister,
                 "toReschedule": toReschedule,
                 "toFail": toFail})

  def __insertRegisterOperation(self, request, operation, toRegister):
    """ add RegisterReplica operation

    :param Request request: request instance
    :param Operation transferOp: 'ReplicateAndRegister' operation for this FTSJob
    :param list toRegister: [ FTSDB.FTSFile, ... ] - files that failed to register
    """
    log = self.log.getSubLogger("req_%s/%s/registerFiles" % (request.RequestID, request.RequestName))

    byTarget = {}
    for ftsFile in toRegister:
      if ftsFile.TargetSE not in byTarget:
        byTarget.setdefault(ftsFile.TargetSE, [])
      byTarget[ftsFile.TargetSE].append(ftsFile)
    log.info("will create %s 'RegisterReplica' operations" % len(byTarget))

    for target, ftsFileList in byTarget.iteritems():
      log.info("creating 'RegisterReplica' operation for targetSE %s with %s files..." % (target,
                                                                                          len(ftsFileList)))
      registerOperation = Operation()
      registerOperation.Type = "RegisterReplica"
      registerOperation.Status = "Waiting"
      registerOperation.TargetSE = target
      targetSE = StorageElement(target)
      for ftsFile in ftsFileList:
        opFile = File()
        opFile.LFN = ftsFile.LFN
        pfn = returnSingleResult(targetSE.getURL(ftsFile.LFN, protocol=self.registrationProtocols))
        if not pfn["OK"]:
          continue
        opFile.PFN = pfn["Value"]
        registerOperation.addFile(opFile)
      request.insertBefore(registerOperation, operation)

    return S_OK()

  @staticmethod
  def __sendAccounting(ftsJob, ownerDN):
    """ prepare and send DataOperation to AccouringDB """

    dataOp = DataOperation()
    dataOp.setStartTime(fromString(ftsJob.SubmitTime))
    dataOp.setEndTime(fromString(ftsJob.LastUpdate))

    accountingDict = dict()
    accountingDict["OperationType"] = "ReplicateAndRegister"

    username = getUsernameForDN(ownerDN)
    if not username["OK"]:
      username = ownerDN
    else:
      username = username["Value"]

    accountingDict["User"] = username
    accountingDict["Protocol"] = "FTS3" if 'fts3' in ftsJob.FTSServer.lower() else 'FTS'
    accountingDict['ExecutionSite'] = ftsJob.FTSServer

    accountingDict['RegistrationTime'] = ftsJob._regTime  # pylint: disable=protected-access
    accountingDict['RegistrationOK'] = ftsJob._regSuccess  # pylint: disable=protected-access
    accountingDict['RegistrationTotal'] = ftsJob._regTotal  # pylint: disable=protected-access

    accountingDict["TransferOK"] = len([f for f in ftsJob if f.Status in FTSFile.SUCCESS_STATES])
    accountingDict["TransferTotal"] = len(ftsJob)
    accountingDict["TransferSize"] = ftsJob.Size - ftsJob.FailedSize
    accountingDict["FinalStatus"] = ftsJob.Status
    accountingDict["Source"] = ftsJob.SourceSE
    accountingDict["Destination"] = ftsJob.TargetSE
    accountingDict['TransferTime'] = sum(int(f._duration) for f in ftsJob  # pylint: disable=protected-access
                                         if f.Status in FTSFile.SUCCESS_STATES)
    dataOp.setValuesFromDict(accountingDict)
    dataOp.commit()

  def __checkReadyReplicas(self, request, operation):
    """ check ready replicas for transferOperation """
    log = self.log.getSubLogger("req_%s/%s/checkReadyReplicas" % (request.RequestID, request.RequestName))

    targetSESet = set(operation.targetSEList)

    # # { LFN: [ targetSE, ... ] }
    missingReplicas = {}

    scheduledFiles = dict((opFile.LFN, opFile) for opFile in operation if opFile.Status in ("Scheduled", "Waiting"))
    # # get replicas
    replicas = FileCatalog().getReplicas(scheduledFiles.keys())
    if not replicas["OK"]:
      self.log.error(replicas["Message"])
      return replicas
    replicas = replicas["Value"]

    fullyReplicated = 0
    missingSEs = {}
    for successfulLFN in replicas["Successful"]:
      reps = set(replicas['Successful'][successfulLFN])
      if targetSESet.issubset(reps):
        log.verbose("%s has been replicated to all targets" % successfulLFN)
        fullyReplicated += 1
        scheduledFiles[successfulLFN].Status = "Done"
      else:
        missingReplicas[successfulLFN] = sorted(targetSESet - reps)
        ses = ",".join(missingReplicas[successfulLFN])
        missingSEs[ses] = missingSEs.setdefault(ses, 0) + 1
        log.verbose("%s is still missing at %s" % (successfulLFN, ses))
    if fullyReplicated:
      log.info("%d new files have been replicated to all targets" % fullyReplicated)
    if missingSEs:
      for ses in missingSEs:
        log.info("%d replicas still missing at %s" % (missingSEs[ses], ses))

    reMissing = re.compile("no such file or directory")
    for failedLFN, errStr in replicas["Failed"].iteritems():
      scheduledFiles[failedLFN].Error = errStr
      if reMissing.search(errStr.lower()):
        log.error("%s is missing, setting its status to 'Failed'" % failedLFN)
        scheduledFiles[failedLFN].Status = "Failed"
      else:
        log.warn("unable to read replicas for %s: %s" % (failedLFN, errStr))

    return S_OK(missingReplicas)

  def __filterReplicas(self, opFile):
    """ filter out banned/invalid source SEs """
    from DIRAC.DataManagementSystem.Agent.RequestOperations.ReplicateAndRegister import filterReplicas
    return filterReplicas(opFile, logger=self.log, dataManager=self.dataManager)
