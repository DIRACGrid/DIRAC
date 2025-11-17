"""
FTS3Agent implementation.
It is in charge of submitting and monitoring all the transfers. It can be duplicated.


.. literalinclude:: ../ConfigTemplate.cfg
  :start-after: ##BEGIN FTS3Agent
  :end-before: ##END FTS3Agent
  :dedent: 2
  :caption: FTS3Agent options

"""

import datetime
import errno
import os
import time

# We use the dummy module because we use the ThreadPool
from multiprocessing.dummy import current_process

# from threading import current_thread
from multiprocessing.pool import ThreadPool
from socket import gethostname
from urllib import parse

from DIRAC import S_ERROR, S_OK
from DIRAC.ConfigurationSystem.Client.Helpers.Operations import Operations as opHelper
from DIRAC.ConfigurationSystem.Client.Helpers.Registry import getDNForUsername
from DIRAC.ConfigurationSystem.Client.Helpers.Resources import getFTS3ServerDict
from DIRAC.Core.Base.AgentModule import AgentModule
from DIRAC.Core.Utilities.DErrno import cmpError
from DIRAC.Core.Utilities.DictCache import DictCache
from DIRAC.Core.Utilities.TimeUtilities import fromString
from DIRAC.DataManagementSystem.Client.FTS3Job import FTS3Job
from DIRAC.DataManagementSystem.DB.FTS3DB import FTS3DB
from DIRAC.DataManagementSystem.private import FTS3Utilities
from DIRAC.FrameworkSystem.Client.Logger import gLogger
from DIRAC.FrameworkSystem.Client.ProxyManagerClient import gProxyManager
from DIRAC.MonitoringSystem.Client.DataOperationSender import DataOperationSender
from DIRAC.FrameworkSystem.Client.TokenManagerClient import gTokenManager
from DIRAC.DataManagementSystem.private import FTS3Utilities
from DIRAC.DataManagementSystem.DB.FTS3DB import FTS3DB
from DIRAC.DataManagementSystem.Client.FTS3Job import FTS3Job
from DIRAC.RequestManagementSystem.Client.ReqClient import ReqClient

# pylint: disable=attribute-defined-outside-init

AGENT_NAME = "DataManagement/FTS3Agent"

# Lifetime in seconds of the proxy we download for submission
# Because we force the redelegation if only a third is left,
# and we want to have a quiet night (~12h)
# let's make the lifetime 12*3 hours
PROXY_LIFETIME = 36 * 3600  # 36 hours

# Instead of querying many jobs at once,
# which maximizes the possibility of race condition
# when running multiple agents, we rather do it in steps
JOB_MONITORING_BATCH_SIZE = 20

# We do not monitor a job more often
# than MONITORING_DELAY in minutes
MONITORING_DELAY = 10


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
        """Read configurations

        :return: S_OK()/S_ERROR()
        """

        # Getting all the possible servers
        res = getFTS3ServerDict()
        if not res["OK"]:
            gLogger.error(res["Message"])
            return res

        srvDict = res["Value"]
        serverPolicyType = opHelper().getValue("DataManagement/FTSPlacement/FTS3/ServerPolicy", "Random")
        self._serverPolicy = FTS3Utilities.FTS3ServerPolicy(srvDict, serverPolicy=serverPolicyType)

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
        # lifetime of the proxy we download to delegate to FTS
        self.proxyLifetime = self.am_getOption("ProxyLifetime", PROXY_LIFETIME)
        self.jobMonitoringBatchSize = self.am_getOption("JobMonitoringBatchSize", JOB_MONITORING_BATCH_SIZE)
        self.useTokens = self.am_getOption("UseTokens", False)

        self.jobMonitoringBatchSize = self.am_getOption("JobMonitoringBatchSize", JOB_MONITORING_BATCH_SIZE)

        return S_OK()

    def initialize(self):
        """Agent's initialization

        :return: S_OK()/S_ERROR()
        """
        self._globalContextCache = {}

        # name that will be used in DB for assignment tag
        self.assignmentTag = gethostname().split(".")[0]

        self.workDirectory = self.am_getWorkDirectory()

        res = self.__readConf()

        # We multiply by two because of the two threadPools
        self.fts3db = FTS3DB(pool_size=2 * self.maxNumberOfThreads)

        self.jobsThreadPool = ThreadPool(self.maxNumberOfThreads)
        self.opsThreadPool = ThreadPool(self.maxNumberOfThreads)

        return res

    def beginExecution(self):
        """Reload configurations before start of a cycle

        :return: S_OK()/S_ERROR()
        """
        self.dataOpSender = DataOperationSender()
        return self.__readConf()

    def getFTS3Context(self, username, group, ftsServer, threadID):
        """Returns an fts3 context for a given user, group and fts server

        The context pool is per thread, and there is one context
        per tuple (user, group, server).
        We dump the proxy of a user to a file (shared by all the threads),
        and use it to make the context.
        The proxy needs a lifetime of self.proxyLifetime, is cached for cacheTime = (2*lifeTime/3) - 10mn,
        and the lifetime of the context is 45mn
        The reason for cacheTime to be what it is is because the FTS3 server will ask for a new proxy
        after 2/3rd of the existing proxy has expired, so we renew it just before

        :param str username: name of the user
        :param str group: group of the user
        :param str ftsServer: address of the server
        :param str threadID: thread ID

        :returns: S_OK with the context object

        """

        log = gLogger.getSubLogger("getFTS3Context")

        contextes = self._globalContextCache.setdefault(threadID, DictCache())

        idTuple = (username, group, ftsServer)
        log.debug(f"Getting context for {idTuple}")

        # We keep a context in the cache for 45 minutes
        # (so it needs to be valid at least 15 since we add it for one hour)
        if not contextes.exists(idTuple, 15 * 60):
            res = getDNForUsername(username)
            if not res["OK"]:
                return res
            # We take the first DN returned
            userDN = res["Value"][0]

            log.debug(f"UserDN {userDN}")

            # Chose a meaningful proxy name for easier debugging
            srvName = parse.urlparse(ftsServer).netloc.split(":")[0]
            proxyFile = os.path.join(
                self.workDirectory, f"{int(time.time())}_{username}_{group}_{srvName}_{threadID}.pem"
            )

            # We dump the proxy to a file.
            # It has to have a lifetime of self.proxyLifetime
            # Because the FTS3 servers cache it for 2/3rd of the lifetime
            # we should make our cache a bit less than 2/3rd of the lifetime
            cacheTime = int(2 * self.proxyLifetime / 3) - 600
            res = gProxyManager.downloadVOMSProxyToFile(
                userDN, group, requiredTimeLeft=self.proxyLifetime, cacheTime=cacheTime, filePath=proxyFile
            )
            if not res["OK"]:
                return res

            proxyFile = res["Value"]
            log.debug(f"Proxy file {proxyFile}")

            # We generate the context
            # In practice, the lifetime will be less than proxyLifetime
            # because we reuse a cached proxy. However, the cached proxy will
            # never forced a redelegation, because it is recent enough for FTS3 servers.
            # The delegation is forced when 2/3 rd of the lifetime are left, and we get a fresh
            # one just before. So no problem
            res = FTS3Job.generateContext(ftsServer, proxyFile, lifetime=self.proxyLifetime)

            if not res["OK"]:
                return res
            context = res["Value"]

            # we add it to the cache for this thread for 1h
            contextes.add(idTuple, 3600, context)

        return S_OK(contextes.get(idTuple))

    def _monitorJob(self, ftsJob):
        """* query the FTS servers
        * update the FTSFile status
        * update the FTSJob status

        :param ftsJob: FTS job

        :return: ftsJob, S_OK()/S_ERROR()
        """
        # General try catch to avoid that the tread dies
        try:
            threadID = current_process().name
            log = gLogger.getLocalSubLogger(f"_monitorJob/{ftsJob.jobID}")

            res = self.getFTS3Context(ftsJob.username, ftsJob.userGroup, ftsJob.ftsServer, threadID=threadID)

            if not res["OK"]:
                log.error("Error getting context", res)
                return ftsJob, res

            context = res["Value"]

            res = ftsJob.monitor(context=context)

            if not res["OK"]:
                log.error("Error monitoring job", res)

                # If the job was not found on the server, update the DB
                if cmpError(res, errno.ESRCH):
                    res = self.fts3db.cancelNonExistingJob(ftsJob.operationID, ftsJob.ftsGUID)

                return ftsJob, res

            # { fileID : { Status, Error } }
            filesStatus = res["Value"]

            # Specify the job ftsGUID to make sure we do not overwrite
            # status of files already taken by newer jobs
            res = self.fts3db.updateFileStatus(filesStatus, ftsGUID=ftsJob.ftsGUID)

            if not res["OK"]:
                log.error("Error updating file fts status", f"{ftsJob.ftsGUID}, {res}")
                return ftsJob, res

            upDict = {
                ftsJob.jobID: {
                    "status": ftsJob.status,
                    "error": ftsJob.error,
                    "completeness": ftsJob.completeness,
                    "operationID": ftsJob.operationID,
                    "lastMonitor": True,
                }
            }
            res = self.fts3db.updateJobStatus(upDict)

            if ftsJob.status in ftsJob.FINAL_STATES:
                self.__sendAccounting(ftsJob)

            return ftsJob, res

        except Exception as e:
            log.exception("Exception while monitoring job", repr(e))
            return ftsJob, S_ERROR(0, f"Exception {repr(e)}")

    @staticmethod
    def _monitorJobCallback(returnedValue):
        """Callback when a job has been monitored
        :param returnedValue: value returned by the _monitorJob method
                              (ftsJob, standard dirac return struct)
        """
        if isinstance(returnedValue, tuple) and len(returnedValue) == 2:
            ftsJob, res = returnedValue
            log = gLogger.getLocalSubLogger(f"_monitorJobCallback/{ftsJob.jobID}")
            if not res["OK"]:
                log.error("Error updating job status", res)
            else:
                log.debug("Successfully updated job status")
        else:
            log = gLogger.getLocalSubLogger("_monitorJobCallback")
            log.error("Invalid return value when monitoring job", f"{returnedValue!r}")

    def monitorJobsLoop(self):
        """* fetch the active FTSJobs from the DB
        * spawn a thread to monitor each of them

        :return: S_OK()/S_ERROR()
        """

        log = gLogger.getSubLogger("monitorJobs")
        log.debug(f"Size of the context cache {len(self._globalContextCache)}")

        # Find the number of loops
        nbOfLoops, mod = divmod(self.jobBulkSize, JOB_MONITORING_BATCH_SIZE)
        if mod:
            nbOfLoops += 1

        # Not only is it pointless to monitor right after submission
        # but also we would end up fetching multiple time the same job otherwise
        # as we call getActiveJobs by batch
        lastMonitor = datetime.datetime.utcnow() - datetime.timedelta(minutes=MONITORING_DELAY)

        log.debug("Getting active jobs")

        for loopId in range(nbOfLoops):
            log.info("Getting next batch of jobs to monitor", f"{loopId}/{nbOfLoops}")
            # get jobs from DB
            res = self.fts3db.getActiveJobs(
                limit=self.jobMonitoringBatchSize, lastMonitor=lastMonitor, jobAssignmentTag=self.assignmentTag
            )

            if not res["OK"]:
                log.error("Could not retrieve ftsJobs from the DB", res)
                return res

            activeJobs = res["Value"]
            if not activeJobs:
                log.info("No more jobs to monitor")
                break

            log.info("Jobs queued for monitoring", len(activeJobs))

            # We store here the AsyncResult object on which we are going to wait
            applyAsyncResults = []

            # Starting the monitoring threads
            for ftsJob in activeJobs:
                log.debug(f"Queuing executing of ftsJob {ftsJob.jobID}")
                # queue the execution of self._monitorJob( ftsJob ) in the thread pool
                # The returned value is passed to _monitorJobCallback
                applyAsyncResults.append(
                    self.jobsThreadPool.apply_async(self._monitorJob, (ftsJob,), callback=self._monitorJobCallback)
                )

            log.debug("All execution queued")

            # Waiting for all the monitoring to finish
            while not all([r.ready() for r in applyAsyncResults]):
                log.debug("Not all the tasks are finished")
                time.sleep(0.5)

            # If we got less to monitor than what we asked,
            # stop looping
            if len(activeJobs) < self.jobMonitoringBatchSize:
                break
        # Commit records after each loop
        self.dataOpSender.concludeSending()

        log.debug("All the tasks have completed")
        return S_OK()

    @staticmethod
    def _treatOperationCallback(returnedValue):
        """Callback when an operation has been treated

        :param returnedValue: value returned by the _treatOperation method
                              (ftsOperation, standard dirac return struct)
        """
        if isinstance(returnedValue, tuple) and len(returnedValue) == 2:
            operation, res = returnedValue
            log = gLogger.getLocalSubLogger(f"_treatOperationCallback/{operation.operationID}")
            if not res["OK"]:
                log.error("Error treating operation", res)
            else:
                log.debug("Successfully treated operation")
        else:
            log = gLogger.getLocalSubLogger("_treatOperationCallback")
            log.error("Invalid return value when treating operation", f"{returnedValue!r}")

    def _treatOperation(self, operation):
        """Treat one operation:
          * does the callback if the operation is finished
          * generate new jobs and submits them

        :param operation: the operation to treat

        :return: operation, S_OK()/S_ERROR()
        """
        try:
            threadID = current_process().name
            log = gLogger.getLocalSubLogger(f"treatOperation/{operation.operationID}")

            # If the operation is totally processed
            # we perform the callback
            if operation.isTotallyProcessed():
                log.debug(f"FTS3Operation {operation.operationID} is totally processed")
                res = operation.callback()

                if not res["OK"]:
                    log.error("Error performing the callback", res)
                    log.info("Putting back the operation")
                    dbRes = self.fts3db.persistOperation(operation)

                    if not dbRes["OK"]:
                        log.error("Could not persist operation", dbRes)

                    return operation, res

            else:
                log.debug(f"FTS3Operation {operation.operationID} is not totally processed yet")

                # This flag is set to False if we want to stop the ongoing processing
                # of an operation, typically when the matching RMS Request has been
                # canceled (see below)
                continueOperationProcessing = True

                # Check the status of the associated RMS Request.
                # If it is canceled or does not exist anymore then we will not create new FTS3Jobs, and mark
                # this as FTS3Operation canceled.

                if operation.rmsReqID:
                    res = ReqClient().getRequestStatus(operation.rmsReqID)
                    if not res["OK"]:
                        # If the Request does not exist anymore
                        if cmpError(res, errno.ENOENT):
                            log.info(
                                "The RMS Request does not exist anymore, canceling the FTS3Operation",
                                f"rmsReqID: {operation.rmsReqID}, FTS3OperationID: {operation.operationID}",
                            )
                            operation.status = "Canceled"
                            continueOperationProcessing = False
                        else:
                            log.error("Could not get request status", res)
                            return operation, res

                    else:
                        rmsReqStatus = res["Value"]

                        if rmsReqStatus == "Canceled":
                            log.info(
                                "The RMS Request is canceled, canceling the FTS3Operation",
                                f"rmsReqID: {operation.rmsReqID}, FTS3OperationID: {operation.operationID}",
                            )
                            operation.status = "Canceled"
                            continueOperationProcessing = False

                            log.info("Canceling the associated FTS3 jobs")

                            for ftsJob in operation.ftsJobs:
                                res = self.getFTS3Context(
                                    ftsJob.username, ftsJob.userGroup, ftsJob.ftsServer, threadID=threadID
                                )
                                if not res["OK"]:
                                    log.error("Error getting context", res)
                                    continue

                                context = res["Value"]
                                # We ignore the return on purpose
                                ftsJob.cancel(context)

                if continueOperationProcessing:
                    res = operation.prepareNewJobs(
                        maxFilesPerJob=self.maxFilesPerJob, maxAttemptsPerFile=self.maxAttemptsPerFile
                    )

                    if not res["OK"]:
                        log.error("Cannot prepare new Jobs", f"FTS3Operation {operation.operationID} : {res}")
                        return operation, res

                    newJobs = res["Value"]

                    log.debug(f"FTS3Operation {operation.operationID}: {len(newJobs)} new jobs to be submitted")

                    for ftsJob in newJobs:
                        res = self._serverPolicy.chooseFTS3Server()
                        if not res["OK"]:
                            log.error(res)
                            continue

                        ftsServer = res["Value"]
                        log.debug(f"Use {ftsServer} server")

                        ftsJob.ftsServer = ftsServer

                        res = self.getFTS3Context(ftsJob.username, ftsJob.userGroup, ftsServer, threadID=threadID)

                        if not res["OK"]:
                            log.error("Could not get context", res)
                            continue

                        context = res["Value"]

                        try:
                            tpcProtocols = operation.fts3Plugin.selectTPCProtocols(ftsJob=ftsJob)
                        except ValueError as e:
                            log.error("Could not select TPC list", repr(e))
                            continue

                        # If we use token, get an access token with the
                        # fts scope in it
                        # The FTS3Job will decide to use it or not
                        fts_access_token = None
                        if self.useTokens:
                            res = gTokenManager.getToken(
                                userGroup=ftsJob.userGroup,
                                requiredTimeLeft=3600,
                                scope=["fts"],
                            )
                            if not res["OK"]:
                                return operation, res

                            fts_access_token = res["Value"]["access_token"]

                        res = ftsJob.submit(context=context, protocols=tpcProtocols, fts_access_token=fts_access_token)

                        if not res["OK"]:
                            log.error("Could not submit FTS3Job", f"FTS3Operation {operation.operationID} : {res}")
                            continue

                        operation.ftsJobs.append(ftsJob)

                        submittedFileIds = res["Value"]
                        log.info(
                            "FTS3Operation %s: Submitted job for %s transfers"
                            % (operation.operationID, len(submittedFileIds))
                        )

            # new jobs are put in the DB at the same time
            res = self.fts3db.persistOperation(operation)

            if not res["OK"]:
                log.error("Could not persist operation", res)

            return operation, res

        except Exception as e:
            log.exception("Exception in the thread", repr(e))
            return operation, S_ERROR(f"Exception {repr(e)}")

    def treatOperationsLoop(self):
        """* Fetch all the FTSOperations which are not finished
        * Spawn a thread to treat each operation

        :return: S_OK()/S_ERROR()
        """

        log = gLogger.getSubLogger("treatOperations")

        log.debug(f"Size of the context cache {len(self._globalContextCache)}")

        log.info("Getting non finished operations")

        res = self.fts3db.getNonFinishedOperations(
            limit=self.operationBulkSize, operationAssignmentTag=self.assignmentTag
        )

        if not res["OK"]:
            log.error("Could not get incomplete operations", res)
            return res

        incompleteOperations = res["Value"]

        log.info(f"Treating {len(incompleteOperations)} incomplete operations")

        applyAsyncResults = []

        for operation in incompleteOperations:
            log.debug(f"Queuing executing of operation {operation.operationID}")
            # queue the execution of self._treatOperation( operation ) in the thread pool
            # The returned value is passed to _treatOperationCallback
            applyAsyncResults.append(
                self.opsThreadPool.apply_async(
                    self._treatOperation, (operation,), callback=self._treatOperationCallback
                )
            )

        log.debug("All execution queued")

        # Waiting for all the treatments to finish
        while not all([r.ready() for r in applyAsyncResults]):
            log.debug("Not all the tasks are finished")
            time.sleep(0.5)

        log.debug("All the tasks have completed")

        return S_OK()

    def kickOperations(self):
        """Kick stuck operations

        :return: S_OK()/S_ERROR()
        """

        log = gLogger.getSubLogger("kickOperations")

        res = self.fts3db.kickStuckOperations(limit=self.maxKick, kickDelay=self.kickDelay)
        if not res["OK"]:
            return res

        kickedOperations = res["Value"]
        log.info(f"Kicked {kickedOperations} stuck operations")

        return S_OK()

    def kickJobs(self):
        """Kick stuck jobs

        :return: S_OK()/S_ERROR()
        """

        log = gLogger.getSubLogger("kickJobs")

        res = self.fts3db.kickStuckJobs(limit=self.maxKick, kickDelay=self.kickDelay)
        if not res["OK"]:
            return res

        kickedJobs = res["Value"]
        log.info(f"Kicked {kickedJobs} stuck jobs")

        return S_OK()

    def deleteOperations(self):
        """Delete final operations

        :return: S_OK()/S_ERROR()
        """

        log = gLogger.getSubLogger("deleteOperations")

        res = self.fts3db.deleteFinalOperations(limit=self.maxDelete, deleteDelay=self.deleteDelay)
        if not res["OK"]:
            return res

        deletedOperations = res["Value"]
        log.info(f"Deleted {deletedOperations} final operations")

        return S_OK()

    def finalize(self):
        """Finalize processing

        :return: S_OK()/S_ERROR()
        """
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
        """One cycle execution

        :return: S_OK()/S_ERROR()
        """

        log = gLogger.getSubLogger("execute")

        log.info("Monitoring job")
        res = self.monitorJobsLoop()

        if not res["OK"]:
            log.error("Error monitoring jobs", res)
            return res

        log.info("Treating operations")
        res = self.treatOperationsLoop()

        if not res["OK"]:
            log.error("Error treating operations", res)
            return res

        log.info("Kicking stuck jobs")
        res = self.kickJobs()

        if not res["OK"]:
            log.error("Error kicking jobs", res)
            return res

        log.info("Kicking stuck operations")
        res = self.kickOperations()

        if not res["OK"]:
            log.error("Error kicking operations", res)
            return res

        log.info("Deleting final operations")
        res = self.deleteOperations()

        if not res["OK"]:
            log.error("Error deleting operations", res)
            return res

        return S_OK()

    def endExecution(self):
        return self.dataOpSender.concludeSending()

    def __sendAccounting(self, ftsJob):
        for accountingDict in ftsJob.accountingDicts:
            self.dataOpSender.sendData(
                accountingDict,
                commitFlag=True,
                delayedCommit=True,
                startTime=fromString(ftsJob.submitTime),
                endTime=fromString(ftsJob.lastUpdate),
            )
