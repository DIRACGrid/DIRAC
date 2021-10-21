"""
  The Push Job Agent class inherits from Job Agent and aims to support job submission in
  sites with no external connectivity (e.g. some supercomputers).
"""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

__RCSID__ = "$Id$"

import sys
import re
import time
import six
import random
from collections import defaultdict

from DIRAC import S_OK, S_ERROR, gConfig
from DIRAC.Core.Utilities.ObjectLoader import ObjectLoader
from DIRAC.ConfigurationSystem.Client.PathFinder import getSystemInstance
from DIRAC.ConfigurationSystem.Client.Helpers.Operations import Operations
from DIRAC.WorkloadManagementSystem.Client.JobReport import JobReport
from DIRAC.WorkloadManagementSystem.Client import JobStatus
from DIRAC.WorkloadManagementSystem.Utilities.QueueUtilities import getQueuesResolved
from DIRAC.WorkloadManagementSystem.Service.WMSUtilities import getGridEnv
from DIRAC.WorkloadManagementSystem.Agent.JobAgent import JobAgent
from DIRAC.RequestManagementSystem.Client.Request import Request

MAX_JOBS_MANAGED = 100


class PushJobAgent(JobAgent):
    """This agent runs on the DIRAC server side contrary to JobAgent, which runs on worker nodes.
    It fetches jobs, prepares them for sites with no external connectivity and submit locally.
    The remote execution is handled at the Workflow level"""

    def __init__(self, agentName, loadName, baseAgentName=False, properties=None):
        """Just defines some default parameters"""
        super(PushJobAgent, self).__init__(agentName, loadName, baseAgentName, properties)
        self.firstPass = True
        self.maxJobsToSubmit = MAX_JOBS_MANAGED
        self.queueDict = {}
        self.queueCECache = {}

        # self.failedQueueCycleFactor is the number of cycles a queue has to wait before getting pilots again
        self.failedQueueCycleFactor = 10
        self.failedQueues = defaultdict(int)
        self.failedQueueCycleFactor = self.am_getOption("FailedQueueCycleFactor", self.failedQueueCycleFactor)

    def initialize(self):
        """Sets default parameters and creates CE instance"""
        super(PushJobAgent, self).initialize()

        result = self._initializeComputingElement("Pool")
        if not result["OK"]:
            return result

        # on-the fly imports
        ol = ObjectLoader()
        res = ol.loadModule("ConfigurationSystem.Client.Helpers.Resources")
        if not res["OK"]:
            sys.exit(res["Message"])
        self.resourcesModule = res["Value"]
        self.opsHelper = Operations()

        # Disable Watchdog: we don't need it as pre/post processing occurs locally
        setup = gConfig.getValue("/DIRAC/Setup", "")
        if not setup:
            return S_ERROR("Cannot get the DIRAC Setup value")
        wms_instance = getSystemInstance("WorkloadManagement")
        if not wms_instance:
            return S_ERROR("Cannot get the WorkloadManagement system instance")
        section = "/Systems/WorkloadManagement/%s/JobWrapper" % wms_instance
        self._updateConfiguration("CheckWallClockFlag", 0, path=section)
        self._updateConfiguration("CheckDiskSpaceFlag", 0, path=section)
        self._updateConfiguration("CheckLoadAvgFlag", 0, path=section)
        self._updateConfiguration("CheckCPUConsumedFlag", 0, path=section)
        self._updateConfiguration("CheckCPULimitFlag", 0, path=section)
        self._updateConfiguration("CheckMemoryLimitFlag", 0, path=section)
        self._updateConfiguration("CheckTimeLeftFlag", 0, path=section)

        return S_OK()

    def beginExecution(self):
        """This is run at every cycles, as first thing.
        It gets site, CE and queue descriptions.
        """
        # Maximum number of jobs that can be handled at the same time by the agent
        self.maxJobsToSubmit = self.am_getOption("MaxJobsToSubmit", self.maxJobsToSubmit)
        self.computingElement.setParameters({"NumberOfProcessors": self.maxJobsToSubmit})

        # Get target queues from the configuration
        siteNames = None
        siteNamesOption = self.am_getOption("Site", ["any"])
        if siteNamesOption and "any" not in [sn.lower() for sn in siteNamesOption]:
            siteNames = siteNamesOption

        ceTypes = None
        ceTypesOption = self.am_getOption("CETypes", ["any"])
        if ceTypesOption and "any" not in [ct.lower() for ct in ceTypesOption]:
            ceTypes = ceTypesOption

        ces = None
        cesOption = self.am_getOption("CEs", ["any"])
        if cesOption and "any" not in [ce.lower() for ce in cesOption]:
            ces = cesOption

        self.log.info("Sites:", siteNames)
        self.log.info("CETypes:", ceTypes)
        self.log.info("CEs:", ces)

        result = self._buildQueueDict(siteNames, ces, ceTypes)
        if not result["OK"]:
            return result

        self.queueDict = result["Value"]

        if self.firstPass:
            if self.queueDict:
                self.log.always("Agent will serve queues:")
                for queue in self.queueDict:
                    self.log.always(
                        "Site: %s, CE: %s, Queue: %s"
                        % (self.queueDict[queue]["Site"], self.queueDict[queue]["CEName"], queue)
                    )
        self.firstPass = False
        return S_OK()

    def execute(self):
        """The JobAgent execution method."""
        self.log.verbose("Job Agent execution loop")

        queueDictItems = list(self.queueDict.items())
        random.shuffle(queueDictItems)

        # Check that there is enough slots locally
        result = self._checkCEAvailability(self.computingElement)
        if not result["OK"]:
            return result

        for queueName, queueDictionary in queueDictItems:

            # Make sure there is no problem with the queue before trying to submit
            if not self._allowedToSubmit(queueName):
                continue

            # Update the configuration with the names of the Site, CE and queue to target
            # This is used in the next stages
            self._updateConfiguration("Site", queueDictionary["Site"])
            self._updateConfiguration("GridCE", queueDictionary["CEName"])
            self._updateConfiguration("CEQueue", queueDictionary["QueueName"])
            self._updateConfiguration("RemoteExecution", True)

            # Check that there is enough slots in the remote CE to match a job
            ce = queueDictionary["CE"]
            result = self._checkCEAvailability(ce)
            if not result["OK"] or (result["OK"] and result["Value"]):
                self.failedQueues[queueName] += 1
                continue

            # Get environment details and enhance them
            result = self._getCEDict(ce)
            if not result["OK"]:
                self.failedQueues[queueName] += 1
                continue
            ceDictList = result["Value"]

            for ceDict in ceDictList:
                # Information about number of processors might not be returned in CE.getCEStatus()
                ceDict["NumberOfProcessors"] = ce.ceParameters.get("NumberOfProcessors")
                self._setCEDict(ceDict)

            # Try to match a job
            jobRequest = self._matchAJob(ceDictList)
            if not jobRequest["OK"]:
                self._checkMatchingIssues(jobRequest["Message"])
                self.failedQueues[queueName] += 1
                continue

            # Check matcher information returned
            matcherParams = ["JDL", "DN", "Group"]
            matcherInfo = jobRequest["Value"]
            jobID = matcherInfo["JobID"]
            jobReport = JobReport(jobID, "PushJobAgent@%s" % self.siteName)
            result = self._checkMatcherInfo(matcherInfo, matcherParams, jobReport)
            if not result["OK"]:
                self.failedQueues[queueName] += 1
                continue

            jobJDL = matcherInfo["JDL"]
            jobGroup = matcherInfo["Group"]
            ownerDN = matcherInfo["DN"]
            ceDict = matcherInfo["CEDict"]
            matchTime = matcherInfo["matchTime"]

            optimizerParams = {}
            for key in matcherInfo:
                if key not in matcherParams:
                    optimizerParams[key] = matcherInfo[key]

            # Get JDL paramters
            parameters = self._getJDLParameters(jobJDL)
            if not parameters["OK"]:
                jobReport.setJobStatus(status=JobStatus.FAILED, minorStatus="Could Not Extract JDL Parameters")
                self.log.warn("Could Not Extract JDL Parameters", parameters["Message"])
                self.failedQueues[queueName] += 1
                continue

            params = parameters["Value"]
            result = self._extractValuesFromJobParams(params, jobReport)
            if not result["OK"]:
                self.failedQueues[queueName] += 1
                continue
            submissionParams = result["Value"]
            jobID = submissionParams["jobID"]
            jobType = submissionParams["jobType"]

            self.log.verbose("Job request successful: \n", jobRequest["Value"])
            self.log.info(
                "Received", "JobID=%s, JobType=%s, OwnerDN=%s, JobGroup=%s" % (jobID, jobType, ownerDN, jobGroup)
            )
            try:
                jobReport.setJobParameter(par_name="MatcherServiceTime", par_value=str(matchTime), sendFlag=False)

                jobReport.setJobStatus(status=JobStatus.MATCHED, minorStatus="Job Received by Agent", sendFlag=False)

                # Setup proxy
                result_setupProxy = self._setupProxy(ownerDN, jobGroup)
                if not result_setupProxy["OK"]:
                    result = self._rescheduleFailedJob(jobID, result_setupProxy["Message"])
                    self.failedQueues[queueName] += 1
                    continue
                proxyChain = result_setupProxy.get("Value")

                # Check software and install them if required
                software = self._checkInstallSoftware(jobID, params, ceDict, jobReport)
                if not software["OK"]:
                    self.log.error("Failed to install software for job", "%s" % (jobID))
                    errorMsg = software["Message"]
                    if not errorMsg:
                        errorMsg = "Failed software installation"
                    result = self._rescheduleFailedJob(jobID, errorMsg)
                    self.failedQueues[queueName] += 1
                    continue

                # Submit the job to the CE
                self.log.debug("Before self._submitJob() (%sCE)" % (self.ceName))
                result_submitJob = self._submitJob(
                    jobID=jobID,
                    jobParams=params,
                    resourceParams=ceDict,
                    optimizerParams=optimizerParams,
                    proxyChain=proxyChain,
                    jobReport=jobReport,
                    processors=submissionParams["processors"],
                    wholeNode=submissionParams["wholeNode"],
                    maxNumberOfProcessors=submissionParams["maxNumberOfProcessors"],
                    mpTag=submissionParams["mpTag"],
                )

                # Committing the JobReport before evaluating the result of job submission
                res = jobReport.commit()
                if not res["OK"]:
                    resFD = jobReport.generateForwardDISET()
                    if not resFD["OK"]:
                        self.log.error("Error generating ForwardDISET operation", resFD["Message"])
                    elif resFD["Value"]:
                        # Here we create the Request.
                        op = resFD["Value"]
                        request = Request()
                        requestName = "jobAgent_%s" % jobID
                        request.RequestName = requestName.replace('"', "")
                        request.JobID = jobID
                        request.SourceComponent = "JobAgent_%s" % jobID
                        request.addOperation(op)
                        # This might fail, but only a message would be printed.
                        self._sendFailoverRequest(request)

                if not result_submitJob["OK"]:
                    self.log.error("Error during submission", result_submitJob["Message"])
                    self.failedQueues[queueName] += 1
                    continue
                elif "PayloadFailed" in result_submitJob:
                    # Do not keep running and do not overwrite the Payload error
                    message = "Payload execution failed with error code %s" % result_submitJob["PayloadFailed"]
                    self.log.info(message)

                self.log.debug("After %sCE submitJob()" % (self.ceName))
            except Exception as subExcept:  # pylint: disable=broad-except
                self.log.exception("Exception in submission", "", lException=subExcept, lExcInfo=True)
                result = self._rescheduleFailedJob(jobID, "Job processing failed with exception")
                self.failedQueues[queueName] += 1

        return S_OK("Job Agent cycle complete")

    #############################################################################
    def _buildQueueDict(self, siteNames, ces, ceTypes):
        """Get the queues and construct a queue dictionary

        :param str siteNames: name of the Sites to follow
        :param str ces: name of CEs to follow
        :param str ceTypes: type of CEs to follow

        :return: dictionary of queue parameters
        """
        result = self.resourcesModule.getQueues(
            community="", siteList=siteNames, ceList=ces, ceTypeList=ceTypes, mode="Direct"
        )
        if not result["OK"]:
            return result

        result = getQueuesResolved(
            siteDict=result["Value"],
            queueCECache=self.queueCECache,
            gridEnv=getGridEnv(),
            setup=gConfig.getValue("/DIRAC/Setup", "unknown"),
            instantiateCEs=True,
        )
        if not result["OK"]:
            return result

        return S_OK(result["Value"])

    def _allowedToSubmit(self, queue):
        """Check if we are allowed to submit to a certain queue

        :param str queue: the queue name

        :return: True/False
        """

        # Check if the queue failed previously
        failedCount = self.failedQueues[queue] % self.failedQueueCycleFactor
        if failedCount != 0:
            self.log.warn(
                "queue failed recently ==> number of cycles skipped",
                "%s ==> %d" % (queue, self.failedQueueCycleFactor - failedCount),
            )
            self.failedQueues[queue] += 1
            return False
        return True

    def _setCEDict(self, ceDict):
        """Set CEDict"""
        # Matcher will check that ReleaseVersion match the pilot version
        # It is not needed in this configuration so we set ReleaseVersion as the pilot version
        versions = self.opsHelper.getValue("Pilot/Version", [])
        if versions:
            if not isinstance(versions, list):
                versions = [versions]
            ceDict["ReleaseVersion"] = versions[0]
        project = self.opsHelper.getValue("Pilot/Project", "")
        if project:
            ceDict["ReleaseProject"] = project

    def _checkMatchingIssues(self, issueMessage):
        """Check the source of the matching issue

        :param str issueMessage: message returned by the matcher
        :return: S_OK/S_ERROR
        """
        matchingFailed = False
        if re.search("No match found", issueMessage):
            self.log.notice("Job request OK, but no match found", ": %s" % issueMessage)
        elif issueMessage.find("seconds timeout") != -1:
            self.log.error("Timeout while requesting job", issueMessage)
        else:
            self.log.notice("Failed to get jobs", ": %s" % issueMessage)

        return S_OK(issueMessage)
