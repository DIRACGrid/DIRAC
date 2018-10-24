"""
 .. versionadded:: v6r20

  FTS3Agent implementation.
  It is in charge of submitting and monitoring all the transfers. It can be duplicated.


::

  FTS3Agent
  {
    PollingTime = 120
    MaxThreads = 10
    # How many Operation we will treat in one loop
    OperationBulkSize = 20
    # How many Job we will monitor in one loop
    JobBulkSize = 20
    # Max number of files to go in a single job
    MaxFilesPerJob = 100
    # Max number of attempt per file
    MaxAttemptsPerFile = 256
    # days before removing jobs
    DeleteGraceDays = 180
    # Max number of deletes per cycle
    DeleteLimitPerCycle = 100
    # hours before kicking jobs with old assignment tag
    KickAssignedHours  = 1
    # Max number of kicks per cycle
    KickLimitPerCycle = 100
  }

"""

__RCSID__ = "$Id$"

import time

# from threading import current_thread
from multiprocessing.pool import ThreadPool
# We use the dummy module because we use the ThreadPool
from multiprocessing.dummy import current_process
from socket import gethostname

from DIRAC import S_OK, S_ERROR

from DIRAC.AccountingSystem.Client.Types.DataOperation import DataOperation
from DIRAC.Core.Base.AgentModule import AgentModule
from DIRAC.Core.Utilities.DictCache import DictCache
from DIRAC.Core.Utilities.Time import fromString
from DIRAC.ConfigurationSystem.Client.Helpers.Resources import getFTS3ServerDict
from DIRAC.ConfigurationSystem.Client.Helpers.Operations import Operations as opHelper
from DIRAC.ConfigurationSystem.Client.Helpers.Registry import getDNForUsername
from DIRAC.FrameworkSystem.Client.Logger import gLogger
from DIRAC.FrameworkSystem.Client.ProxyManagerClient import gProxyManager
from DIRAC.DataManagementSystem.private import FTS3Utilities
from DIRAC.DataManagementSystem.DB.FTS3DB import FTS3DB
from DIRAC.DataManagementSystem.Client.FTS3Job import FTS3Job
from DIRAC.DataManagementSystem.Utilities.DMSHelpers import DMSHelpers


# pylint: disable=attribute-defined-outside-init

AGENT_NAME = "DataManagement/FTS3Agent"


class FTS3Agent(AgentModule):
  """
    This Agent is responsible of interacting with the FTS3 services.
    Several of them can run in parallel.
    It first treats the Operations, by creating new FTS jobs and performing
    callback.
    Then, it monitors the current jobs.

    CAUTION: This agent and the FTSAgent cannot run together.

  """

  def __readConf(self):
    """ read configurations """

    # Getting all the possible servers
    res = getFTS3ServerDict()
    if not res['OK']:
      gLogger.error(res['Message'])
      return res

    srvDict = res['Value']
    serverPolicyType = opHelper().getValue('DataManagement/FTSPlacement/FTS3/ServerPolicy', 'Random')
    self._serverPolicy = FTS3Utilities.FTS3ServerPolicy(srvDict, serverPolicy=serverPolicyType)

    # List of third party protocols for transfers
    self.thirdPartyProtocols = DMSHelpers().getThirdPartyProtocols()

    self.maxNumberOfThreads = self.am_getOption("MaxThreads", 10)

    # Number of Operation we treat in one loop
    self.operationBulkSize = self.am_getOption("OperationBulkSize", 20)
    # Number of Jobs we treat in one loop
    self.jobBulkSize = self.am_getOption("JobBulkSize", 20)
    self.maxFilesPerJob = self.am_getOption("MaxFilesPerJob", 100)
    self.maxAttemptsPerFile = self.am_getOption("MaxAttemptsPerFile", 256)
    self.kickDelay = self.am_getOption("KickAssignedHours", 1)
    self.maxKick = self.am_getOption("KickLimitPerCycle", 100)
    self.deleteDelay = self.am_getOption("DeleteGraceDays", 180)
    self.maxDelete = self.am_getOption("DeleteLimitPerCycle", 100)

    return S_OK()

  def initialize(self):
    """ agent's initialization """

    self._globalContextCache = {}

    # name that will be used in DB for assignment tag
    self.assignmentTag = gethostname().split('.')[0]

    res = self.__readConf()

    # We multiply by two because of the two threadPools
    self.fts3db = FTS3DB(pool_size=2 * self.maxNumberOfThreads)

    self.jobsThreadPool = ThreadPool(self.maxNumberOfThreads)
    self.opsThreadPool = ThreadPool(self.maxNumberOfThreads)

    return res

  def beginExecution(self):
    """ reload configurations before start of a cycle """
    return self.__readConf()

  def getFTS3Context(self, username, group, ftsServer, threadID):
    """ Returns an fts3 context for a given user, group and fts server

        The context pool is per thread, and there is one context
        per tuple (user, group, server).
        We dump the proxy of a user to a file (shared by all the threads),
        and use it to make the context.
        The proxy needs a lifetime of at least 2h, is cached for 1.5h, and
        the lifetime of the context is 45mn

        :param username: name of the user
        :param group: group of the user
        :param ftsServer: address of the server

        :returns: S_OK with the context object

    """

    log = gLogger.getSubLogger("getFTS3Context", child=True)

    contextes = self._globalContextCache.setdefault(threadID, DictCache())

    idTuple = (username, group, ftsServer)
    log.debug("Getting context for %s" % (idTuple, ))

    if not contextes.exists(idTuple, 2700):
      res = getDNForUsername(username)
      if not res['OK']:
        return res
      # We take the first DN returned
      userDN = res['Value'][0]

      log.debug("UserDN %s" % userDN)

      # We dump the proxy to a file.
      # It has to have a lifetime of at least 2 hours
      # and we cache it for 1.5 hours
      res = gProxyManager.downloadVOMSProxyToFile(
          userDN, group, requiredTimeLeft=7200, cacheTime=5400)
      if not res['OK']:
        return res

      proxyFile = res['Value']
      log.debug("Proxy file %s" % proxyFile)

      # We generate the context
      res = FTS3Job.generateContext(ftsServer, proxyFile)
      if not res['OK']:
        return res
      context = res['Value']

      # we add it to the cache for this thread for 1h
      contextes.add(idTuple, 3600, context)

    return S_OK(contextes.get(idTuple))

  def _monitorJob(self, ftsJob):
    """
        * query the FTS servers
        * update the FTSFile status
        * update the FTSJob status
    """
    # General try catch to avoid that the tread dies
    try:
      threadID = current_process().name
      log = gLogger.getSubLogger("_monitorJob/%s" % ftsJob.jobID, child=True)

      res = self.getFTS3Context(
          ftsJob.username, ftsJob.userGroup, ftsJob.ftsServer, threadID=threadID)

      if not res['OK']:
        log.error("Error getting context", res)
        return ftsJob, res

      context = res['Value']

      res = ftsJob.monitor(context=context)

      if not res['OK']:
        log.error("Error monitoring job", res)
        return ftsJob, res

      # { fileID : { Status, Error } }
      filesStatus = res['Value']

      res = self.fts3db.updateFileStatus(filesStatus)

      if not res['OK']:
        log.error("Error updating file fts status", "%s, %s" % (ftsJob.ftsGUID, res))
        return ftsJob, res

      upDict = {
          ftsJob.jobID: {
              'status': ftsJob.status,
              'error': ftsJob.error,
              'completeness': ftsJob.completeness,
              'operationID': ftsJob.operationID,
              'lastMonitor': True,
          }
      }
      res = self.fts3db.updateJobStatus(upDict)

      if ftsJob.status in ftsJob.FINAL_STATES:
        self.__sendAccounting(ftsJob)

      return ftsJob, res

    except Exception as e:
      return ftsJob, S_ERROR(0, "Exception %s" % repr(e))

  @staticmethod
  def _monitorJobCallback(returnedValue):
    """ Callback when a job has been monitored
        :param returnedValue: value returned by the _monitorJob method
                              (ftsJob, standard dirac return struct)
    """

    ftsJob, res = returnedValue
    log = gLogger.getSubLogger("_monitorJobCallback/%s" % ftsJob.jobID, child=True)
    if not res['OK']:
      log.error("Error updating job status", res)
    else:
      log.debug("Successfully updated job status")

  def monitorJobsLoop(self):
    """
        * fetch the active FTSJobs from the DB
        * spawn a thread to monitor each of them
    """

    log = gLogger.getSubLogger("monitorJobs", child=True)
    log.debug("Size of the context cache %s" % len(self._globalContextCache))

    log.debug("Getting active jobs")
    # get jobs from DB
    res = self.fts3db.getActiveJobs(limit=self.jobBulkSize, jobAssignmentTag=self.assignmentTag)

    if not res['OK']:
      log.error("Could not retrieve ftsJobs from the DB", res)
      return res

    activeJobs = res['Value']
    log.info("%s jobs to queue for monitoring" % len(activeJobs))

    # We store here the AsyncResult object on which we are going to wait
    applyAsyncResults = []

    # Starting the monitoring threads
    for ftsJob in activeJobs:
      log.debug("Queuing executing of ftsJob %s" % ftsJob.jobID)
      # queue the execution of self._monitorJob( ftsJob ) in the thread pool
      # The returned value is passed to _monitorJobCallback
      applyAsyncResults.append(self.jobsThreadPool.apply_async(
          self._monitorJob, (ftsJob, ), callback=self._monitorJobCallback))

    log.debug("All execution queued")

    # Waiting for all the monitoring to finish
    while not all([r.ready() for r in applyAsyncResults]):
      log.debug("Not all the tasks are finished")
      time.sleep(0.5)

    log.debug("All the tasks have completed")
    return S_OK()

  @staticmethod
  def _treatOperationCallback(returnedValue):
    """ Callback when an operation has been treated

        :param returnedValue: value returned by the _treatOperation method
                              (ftsOperation, standard dirac return struct)
    """

    operation, res = returnedValue
    log = gLogger.getSubLogger("_treatOperationCallback/%s" % operation.operationID, child=True)
    if not res['OK']:
      log.error("Error treating operation", res)
    else:
      log.debug("Successfully treated operation")

  def _treatOperation(self, operation):
    """ Treat one operation:
          * does the callback if the operation is finished
          * generate new jobs and submits them

          :param operation: the operation to treat
          :param threadId: the id of the tread, it just has to be unique (used for the context cache)
    """
    try:
      threadID = current_process().name
      log = gLogger.getSubLogger("treatOperation/%s" % operation.operationID, child=True)

      # If the operation is totally processed
      # we perform the callback
      if operation.isTotallyProcessed():
        log.debug("FTS3Operation %s is totally processed" % operation.operationID)
        res = operation.callback()

        if not res['OK']:
          log.error("Error performing the callback", res)
          log.info("Putting back the operation")
          dbRes = self.fts3db.persistOperation(operation)

          if not dbRes['OK']:
            log.error("Could not persist operation", dbRes)

          return operation, res

      else:
        log.debug("FTS3Operation %s is not totally processed yet" % operation.operationID)

        res = operation.prepareNewJobs(
            maxFilesPerJob=self.maxFilesPerJob, maxAttemptsPerFile=self.maxAttemptsPerFile)

        if not res['OK']:
          log.error("Cannot prepare new Jobs", "FTS3Operation %s : %s" %
                    (operation.operationID, res))
          return operation, res

        newJobs = res['Value']

        log.debug("FTS3Operation %s: %s new jobs to be submitted" %
                  (operation.operationID, len(newJobs)))

        for ftsJob in newJobs:
          res = self._serverPolicy.chooseFTS3Server()
          if not res['OK']:
            log.error(res)
            continue

          ftsServer = res['Value']
          log.debug("Use %s server" % ftsServer)

          ftsJob.ftsServer = ftsServer

          res = self.getFTS3Context(
              ftsJob.username, ftsJob.userGroup, ftsServer, threadID=threadID)

          if not res['OK']:
            log.error("Could not get context", res)
            continue

          context = res['Value']
          res = ftsJob.submit(context=context, protocols=self.thirdPartyProtocols)

          if not res['OK']:
            log.error("Could not submit FTS3Job", "FTS3Operation %s : %s" %
                      (operation.operationID, res))
            continue

          operation.ftsJobs.append(ftsJob)

          submittedFileIds = res['Value']
          log.info("FTS3Operation %s: Submitted job for %s transfers" %
                   (operation.operationID, len(submittedFileIds)))

        # new jobs are put in the DB at the same time
      res = self.fts3db.persistOperation(operation)

      if not res['OK']:
        log.error("Could not persist operation", res)

      return operation, res

    except Exception as e:
      log.exception('Exception in the thread', repr(e))
      return operation, S_ERROR("Exception %s" % repr(e))

  def treatOperationsLoop(self):
    """ * Fetch all the FTSOperations which are not finished
        * Spawn a thread to treat each operation
    """

    log = gLogger.getSubLogger("treatOperations", child=True)

    log.debug("Size of the context cache %s" % len(self._globalContextCache))

    log.info("Getting non finished operations")

    res = self.fts3db.getNonFinishedOperations(
        limit=self.operationBulkSize, operationAssignmentTag=self.assignmentTag)

    if not res['OK']:
      log.error("Could not get incomplete operations", res)
      return res

    incompleteOperations = res['Value']

    log.info("Treating %s incomplete operations" % len(incompleteOperations))

    applyAsyncResults = []

    for operation in incompleteOperations:
      log.debug("Queuing executing of operation %s" % operation.operationID)
      # queue the execution of self._treatOperation( operation ) in the thread pool
      # The returned value is passed to _treatOperationCallback
      applyAsyncResults.append(self.opsThreadPool.apply_async(
          self._treatOperation, (operation, ), callback=self._treatOperationCallback))

    log.debug("All execution queued")

    # Waiting for all the treatments to finish
    while not all([r.ready() for r in applyAsyncResults]):
      log.debug("Not all the tasks are finished")
      time.sleep(0.5)

    log.debug("All the tasks have completed")

    return S_OK()

  def kickOperations(self):
    """ kick stuck operations """

    log = gLogger.getSubLogger("kickOperations", child=True)

    res = self.fts3db.kickStuckOperations(limit=self.maxKick, kickDelay=self.kickDelay)
    if not res['OK']:
      return res

    kickedOperations = res['Value']
    log.info("Kicked %s stuck operations" % kickedOperations)

    return S_OK()

  def kickJobs(self):
    """ kick stuck jobs """

    log = gLogger.getSubLogger("kickJobs", child=True)

    res = self.fts3db.kickStuckJobs(limit=self.maxKick, kickDelay=self.kickDelay)
    if not res['OK']:
      return res

    kickedJobs = res['Value']
    log.info("Kicked %s stuck jobs" % kickedJobs)

    return S_OK()

  def deleteOperations(self):
    """ delete final operations """

    log = gLogger.getSubLogger("deleteOperations", child=True)

    res = self.fts3db.deleteFinalOperations(limit=self.maxDelete, deleteDelay=self.deleteDelay)
    if not res['OK']:
      return res

    deletedOperations = res['Value']
    log.info("Deleted %s final operations" % deletedOperations)

    return S_OK()

  def finalize(self):
    """ finalize processing """
    # Joining all the ThreadPools
    log = gLogger.getSubLogger("Finalize")

    log.debug("Closing jobsThreadPool")

    self.jobsThreadPool.close()
    self.jobsThreadPool.join()

    log.debug("jobsThreadPool joined")

    log.debug("Closing opsThreadPool")

    self.opsThreadPool.close()
    self.opsThreadPool.join()

    log.debug("opsThreadPool joined")

    return S_OK()

  def execute(self):
    """ one cycle execution """

    log = gLogger.getSubLogger("execute", child=True)

    log.info("Monitoring job")
    res = self.monitorJobsLoop()

    if not res['OK']:
      log.error("Error monitoring jobs", res)
      return res

    log.info("Treating operations")
    res = self.treatOperationsLoop()

    if not res['OK']:
      log.error("Error treating operations", res)
      return res

    log.info("Kicking stuck jobs")
    res = self.kickJobs()

    if not res['OK']:
      log.error("Error kicking jobs", res)
      return res

    log.info("Kicking stuck operations")
    res = self.kickOperations()

    if not res['OK']:
      log.error("Error kicking operations", res)
      return res

    log.info("Deleting final operations")
    res = self.deleteOperations()

    if not res['OK']:
      log.error("Error deleting operations", res)
      return res

    return S_OK()

  @staticmethod
  def __sendAccounting(ftsJob):
    """ prepare and send DataOperation to AccountingDB

        :param ftsJob: the FTS3Job from which we send the accounting info
    """

    dataOp = DataOperation()
    dataOp.setStartTime(fromString(ftsJob.submitTime))
    dataOp.setEndTime(fromString(ftsJob.lastUpdate))

    dataOp.setValuesFromDict(ftsJob.accountingDict)
    dataOp.delayedCommit()
