"""  The Push Job Agent class inherits from Job Agent and aims to support job submission in
     sites with no external connectivity (e.g. some supercomputers).

.. literalinclude:: ../ConfigTemplate.cfg
  :start-after: ##BEGIN PushJobAgent
  :end-before: ##END
  :dedent: 2
  :caption: PushJobAgent options

"""

import hashlib
import json
import os
import random
import shutil
import sys
from collections import defaultdict
import time

from DIRAC import S_OK, S_ERROR
from DIRAC.ConfigurationSystem.Client.Helpers.Operations import Operations
from DIRAC.ConfigurationSystem.Client.Helpers.Registry import getDNForUsername
from DIRAC.Core.Utilities import DErrno
from DIRAC.Core.Utilities.ObjectLoader import ObjectLoader
from DIRAC.FrameworkSystem.Client.ProxyManagerClient import gProxyManager
from DIRAC.RequestManagementSystem.Client.Request import Request
from DIRAC.Resources.Computing import ComputingElement
from DIRAC.WorkloadManagementSystem.Client import JobMinorStatus, JobStatus, PilotStatus
from DIRAC.WorkloadManagementSystem.Client.JobReport import JobReport
from DIRAC.WorkloadManagementSystem.JobWrapper.JobWrapperUtilities import (
    getJobWrapper,
    resolveInputData,
    transferInputSandbox,
)
from DIRAC.WorkloadManagementSystem.Utilities.QueueUtilities import getQueuesResolved
from DIRAC.WorkloadManagementSystem.Agent.JobAgent import JobAgent
from DIRAC.WorkloadManagementSystem.Utilities.Utils import createJobWrapper
from DIRAC.WorkloadManagementSystem.private.ConfigHelper import findGenericPilotCredentials
from DIRAC.WorkloadManagementSystem.Utilities.QueueUtilities import getQueuesResolved

MAX_JOBS_MANAGED = 100


class PushJobAgent(JobAgent):
    """This agent runs on the DIRAC server side contrary to JobAgent, which runs on worker nodes.
    It fetches jobs, prepares them for sites with no external connectivity and submit locally.
    The remote execution is handled at the Workflow level"""

    def __init__(self, agentName, loadName, baseAgentName=False, properties=None):
        """Just defines some default parameters"""
        super().__init__(agentName, loadName, baseAgentName, properties)
        self.firstPass = True
        self.maxJobsToSubmit = MAX_JOBS_MANAGED
        self.queueDict = {}
        self.queueCECache = {}

        self.pilotDN = ""
        self.vo = ""

        # self.failedQueueCycleFactor is the number of cycles a queue has to wait before getting pilots again
        self.failedQueueCycleFactor = 10
        self.failedQueues = defaultdict(int)

        # Choose the submission policy
        # - Application: the agent will submit a workflow to a PoolCE, the workflow is responsible for interacting with the remote site
        # - JobWrapper: the agent will submit a JobWrapper directly to the remote site, it is responsible of the remote execution
        self.submissionPolicy = "Workflow"

        # cleanTask is used to clean the task in the remote site
        self.cleanTask = True

        self.payloadResultFile = "payloadResult.json"
        self.checkSumResultsFile = "checksums.json"

    def initialize(self):
        """Sets default parameters and creates CE instance"""
        super().initialize()

        # Get the submission policy
        # Initialized here because it cannot be dynamically modified during the execution
        self.submissionPolicy = self.am_getOption("SubmissionPolicy", self.submissionPolicy)
        if self.submissionPolicy not in ["Workflow", "JobWrapper"]:
            return S_ERROR("SubmissionPolicy must be either Workflow or JobWrapper")

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

        return S_OK()

    def beginExecution(self):
        """This is run at every cycles, as first thing.
        It gets site, CE and queue descriptions.
        """
        # Get the credentials to use
        # Can be specific to the agent, or generic
        self.vo = self.am_getOption("VO", self.vo)
        self.pilotDN = self.am_getOption("PilotDN", self.pilotDN)
        result = findGenericPilotCredentials(vo=self.vo, pilotDN=self.pilotDN)
        if not result["OK"]:
            return result
        self.pilotDN, _ = result["Value"]

        # Maximum number of jobs that can be handled at the same time by the agent
        self.maxJobsToSubmit = self.am_getOption("MaxJobsToSubmit", self.maxJobsToSubmit)
        self.computingElement.setParameters({"NumberOfProcessors": self.maxJobsToSubmit})

        self.failedQueueCycleFactor = self.am_getOption("FailedQueueCycleFactor", self.failedQueueCycleFactor)
        self.cleanTask = self.am_getOption("CleanTask", self.cleanTask)

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

        if self.submissionPolicy == "Application":
            # Check that there is enough slots locally
            result = self._checkCEAvailability(self.computingElement)
            if not result["OK"] or result["Value"]:
                return result
            # Check errors that could have occurred during job submission and/or execution
            # Status are handled internally, and therefore, not checked outside of the method
            result = self._checkSubmittedJobs()
            if not result["OK"]:
                return result

        for queueName, queueDictionary in queueDictItems:
            # Make sure there is no problem with the queue before trying to submit
            if not self._allowedToSubmit(queueName):
                continue

            # Get a working proxy
            ce = queueDictionary["CE"]
            cpuTime = 86400 * 3
            self.log.verbose("Getting pilot proxy", "for %s/%s %d long" % (self.pilotDN, self.vo, cpuTime))
            pilotGroup = Operations(vo=self.vo).getValue("Pilot/GenericPilotGroup")
            result = gProxyManager.getPilotProxyFromDIRACGroup(self.pilotDN, pilotGroup, cpuTime)
            if not result["OK"]:
                return result
            proxy = result["Value"]
            ce.setProxy(proxy)

            if self.submissionPolicy == "JobWrapper":
                # Check errors that could have occurred during job submission and/or execution
                result = self._checkSubmittedJobWrappers(ce)
                if not result["OK"]:
                    self.failedQueues[queueName] += 1

            # Check that there is enough slots in the remote CE to match a job
            result = self._checkCEAvailability(ce)
            if not result["OK"] or result["Value"]:
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
            while jobRequest["OK"]:
                # Check matcher information returned
                matcherParams = ["JDL", "Owner", "Group"]
                matcherInfo = jobRequest["Value"]
                jobID = matcherInfo["JobID"]
                self.jobs[jobID] = {}
                self.jobs[jobID]["JobReport"] = JobReport(jobID, f"{self.__class__.__name__}@{self.siteName}")
                result = self._checkMatcherInfo(jobID, matcherInfo, matcherParams)
                if not result["OK"]:
                    self.failedQueues[queueName] += 1
                    break

                jobJDL = matcherInfo["JDL"]
                jobGroup = matcherInfo["Group"]
                owner = matcherInfo["Owner"]
                ceDict = matcherInfo["CEDict"]
                matchTime = matcherInfo["matchTime"]

                optimizerParams = {}
                for key in matcherInfo:
                    if key not in matcherParams:
                        optimizerParams[key] = matcherInfo[key]

                # Get JDL paramters
                parameters = self._getJDLParameters(jobJDL)
                if not parameters["OK"]:
                    self.jobs[jobID]["JobReport"].setJobStatus(
                        status=JobStatus.FAILED, minorStatus="Could Not Extract JDL Parameters"
                    )
                    self.log.warn("Could Not Extract JDL Parameters", parameters["Message"])
                    self.failedQueues[queueName] += 1
                    break

                params = parameters["Value"]
                result = self._extractValuesFromJobParams(params)
                if not result["OK"]:
                    self.failedQueues[queueName] += 1
                    break
                submissionParams = result["Value"]
                jobID = submissionParams["jobID"]
                jobType = submissionParams["jobType"]

                self.log.verbose("Job request successful: \n", jobRequest["Value"])
                self.log.info("Received", f"JobID={jobID}, JobType={jobType}, Owner={owner}, JobGroup={jobGroup}")

                self.jobs[jobID]["JobReport"].setJobParameter(
                    par_name="MatcherServiceTime", par_value=str(matchTime), sendFlag=False
                )
                self.jobs[jobID]["JobReport"].setJobStatus(
                    status=JobStatus.MATCHED, minorStatus="Job Received by Agent", sendFlag=False
                )

                # Setup proxy
                ownerDN = getDNForUsername(owner)["Value"][0]
                result_setupProxy = self._setupProxy(ownerDN, jobGroup)
                if not result_setupProxy["OK"]:
                    result = self._rescheduleFailedJob(jobID, result_setupProxy["Message"])
                    self.failedQueues[queueName] += 1
                    break
                proxyChain = result_setupProxy.get("Value")

                # Check software and install them if required
                self.jobs[jobID]["JobReport"].setJobStatus(minorStatus="Installing Software", sendFlag=False)
                software = self._checkInstallSoftware(params, ceDict)
                if not software["OK"]:
                    self.log.error("Failed to install software for job", f"{jobID}")
                    errorMsg = software["Message"]
                    if not errorMsg:
                        errorMsg = "Failed software installation"
                    result = self._rescheduleFailedJob(jobID, errorMsg)
                    self.failedQueues[queueName] += 1
                    break

                # Submit the job to the CE
                self.log.debug(f"Before self._submitJob() ({self.ceName}CE)")
                if self.submissionPolicy == "Workflow":
                    resultSubmission = self._submitJob(
                        jobID=jobID,
                        jobParams=params,
                        resourceParams=ceDict,
                        optimizerParams=optimizerParams,
                        proxyChain=proxyChain,
                        processors=submissionParams["processors"],
                        wholeNode=submissionParams["wholeNode"],
                        maxNumberOfProcessors=submissionParams["maxNumberOfProcessors"],
                        mpTag=submissionParams["mpTag"],
                    )
                    if not result["OK"]:
                        result = self._rescheduleFailedJob(jobID, resultSubmission["Message"])
                        self.failedQueues[queueName] += 1
                        break
                else:
                    resultSubmission = self._submitJobWrapper(
                        jobID=jobID,
                        ce=ce,
                        jobParams=params,
                        resourceParams=ceDict,
                        optimizerParams=optimizerParams,
                        processors=submissionParams["processors"],
                    )
                    if not result["OK"]:
                        self.failedQueues[queueName] += 1
                        break

                self.log.debug(f"After {self.ceName}CE submitJob()")

                # Check that there is enough slots locally
                result = self._checkCEAvailability(self.computingElement)
                if not result["OK"] or result["Value"]:
                    return result

                # Check that there is enough slots in the remote CE to match a new job
                result = self._checkCEAvailability(ce)
                if not result["OK"] or result["Value"]:
                    self.failedQueues[queueName] += 1
                    break

                # Try to match a new job
                jobRequest = self._matchAJob(ceDictList)

            if not jobRequest["OK"]:
                self._checkMatchingIssues(jobRequest)
                self.failedQueues[queueName] += 1
                continue

        return S_OK("Push Job Agent cycle complete")

    #############################################################################
    def _buildQueueDict(self, siteNames, ces, ceTypes):
        """Get the queues and construct a queue dictionary

        :param str siteNames: name of the Sites to follow
        :param str ces: name of CEs to follow
        :param str ceTypes: type of CEs to follow

        :return: dictionary of queue parameters
        """
        result = self.resourcesModule.getQueues(community="", siteList=siteNames, ceList=ces, ceTypeList=ceTypes)
        if not result["OK"]:
            return result

        result = getQueuesResolved(
            siteDict=result["Value"],
            queueCECache=self.queueCECache,
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
        # Matcher will check that ReleaseVersion matches the pilot version
        # It is not needed in this configuration so we set ReleaseVersion as the pilot version
        # Also, DIRACVersion should be equals to ReleaseVersion, so we modify it
        versions = self.opsHelper.getValue("Pilot/Version", [])
        if versions:
            if not isinstance(versions, list):
                versions = [versions]
            ceDict["ReleaseVersion"] = versions[0]
            ceDict["DIRACVersion"] = versions[0]
        project = self.opsHelper.getValue("Pilot/Project", "")
        if project:
            ceDict["ReleaseProject"] = project

        # Add a RemoteExecution entry, which can be used in the next stages
        if self.submissionPolicy == "Workflow":
            ceDict["RemoteExecution"] = True

    def _checkMatchingIssues(self, jobRequest):
        """Check the source of the matching issue

        :param dict jobRequest: S_ERROR returned by the matcher
        :return: S_OK
        """
        if DErrno.cmpError(jobRequest, DErrno.EWMSNOMATCH):
            self.log.notice("Job request OK, but no match found", jobRequest["Message"])
        elif jobRequest["Message"].find("seconds timeout") != -1:
            self.log.error("Timeout while requesting job", jobRequest["Message"])
        else:
            self.log.notice("Failed to get jobs", jobRequest["Message"])

        return S_OK()

    def _submitJobWrapper(
        self,
        jobID: str,
        ce: ComputingElement,
        jobParams: dict,
        resourceParams: dict,
        optimizerParams: dict,
        processors: int,
    ):
        """Submit a JobWrapper to the remote site

        :param jobID: job ID
        :param ce: ComputingElement instance
        :param jobParams: job parameters
        :param resourceParams: resource parameters
        :param optimizerParams: optimizer parameters
        :param proxyChain: proxy chain
        :param processors: number of processors

        :return: S_OK
        """
        # Add the number of requested processors to the job environment
        if "ExecutionEnvironment" in jobParams:
            if isinstance(jobParams["ExecutionEnvironment"], str):
                jobParams["ExecutionEnvironment"] = jobParams["ExecutionEnvironment"].split(";")
        jobParams.setdefault("ExecutionEnvironment", []).append("DIRAC_JOB_PROCESSORS=%d" % processors)

        # Add necessary parameters to get the payload result and analyze its integrity
        jobParams["PayloadResults"] = self.payloadResultFile
        jobParams["Checksum"] = self.checkSumResultsFile

        # Save the current directory location, as getJobWrapper is going to change it
        agentWorkingDirectory = os.getcwd()

        # Prepare the job for submission
        self.log.verbose("Getting a JobWrapper")
        arguments = {"Job": jobParams, "CE": resourceParams, "Optimizer": optimizerParams}
        job = getJobWrapper(jobID, arguments, self.jobReport)
        if not job:
            os.chdir(agentWorkingDirectory)
            return S_ERROR(f"Cannot get a JobWrapper instance for job {jobID}")

        if "InputSandbox" in jobParams:
            self.log.verbose("Getting the inputSandbox of the job")
            if not transferInputSandbox(job, jobParams["InputSandbox"], self.jobReport):
                os.chdir(agentWorkingDirectory)
                return S_ERROR(f"Cannot get input sandbox of job {jobID}")
            self.jobReport.commit()

        if "InputData" in jobParams and jobParams["InputData"]:
            self.log.verbose("Getting the inputData of the job")
            if not resolveInputData(job, self.jobReport):
                os.chdir(agentWorkingDirectory)
                return S_ERROR(f"Cannot get input data of job {jobID}")
            self.jobReport.commit()

        # Preprocess the payload
        result = job.preProcess()
        if not result["OK"]:
            os.chdir(agentWorkingDirectory)
            return result
        payloadParams = result["Value"]
        self.jobReport.commit()

        # Save the current directory location, which should be <jobID>
        jobWrapperWorkingDirectory = os.getcwd()
        # Restore the agent working directory
        os.chdir(agentWorkingDirectory)

        # Generate a light JobWrapper executor script
        jobDesc = {
            "jobID": jobID,
            "jobParams": jobParams,
            "resourceParams": resourceParams,
            "optimizerParams": optimizerParams,
            "payloadParams": payloadParams,
            "extraOptions": self.extraOptions,
        }
        result = createJobWrapper(
            log=self.log,
            logLevel=self.logLevel,
            defaultWrapperLocation="DIRAC/WorkloadManagementSystem/JobWrapper/JobWrapperLightTemplate.py",
            pythonPath="python",
            rootLocation=".",
            **jobDesc,
        )
        if not result["OK"]:
            return result
        jobWrapperRunner = result["Value"]["JobExecutablePath"]
        jobWrapperCode = result["Value"]["JobWrapperPath"]
        jobWrapperConfig = result["Value"]["JobWrapperConfigPath"]

        # Get inputs from the JobWrapper working directory and add the JobWrapper deps
        jobInputs = os.listdir(jobWrapperWorkingDirectory)
        inputs = [os.path.join(jobWrapperWorkingDirectory, input) for input in jobInputs]
        inputs.append(jobWrapperCode)
        inputs.append(jobWrapperConfig)
        self.log.verbose("The executable will be sent along with the following inputs:", ",".join(inputs))

        # Request the whole directory as output
        outputs = ["/"]

        self.jobReport.setJobStatus(minorStatus="Submitting To CE")
        self.log.info("Submitting JobWrapper", f"{os.path.basename(jobWrapperRunner)} to {ce.ceName}CE")

        # Submit the job
        result = ce.submitJob(
            executableFile=jobWrapperRunner,
            proxy=None,
            inputs=inputs,
            outputs=outputs,
        )
        if not result["OK"]:
            self._rescheduleFailedJob(jobID, result["Message"])
            return result

        taskID = result["Value"][0]
        stamp = result["PilotStampDict"][taskID]
        self.log.info("Job being submitted", f"(DIRAC JobID: {jobID}; Task ID: {taskID})")

        self.submissionDict[taskID] = {"JobWrapper": job, "Stamp": stamp}
        time.sleep(self.jobSubmissionDelay)
        return S_OK()

    def _checkOutputIntegrity(self, workingDirectory: str):
        """Make sure that output files are not corrupted.

        :param workingDirectory: path of the outputs
        """
        checkSumOutput = os.path.join(workingDirectory, self.checkSumResultsFile)
        if not os.path.exists(checkSumOutput):
            return S_ERROR(f"Cannot guarantee the integrity of the outputs: {checkSumOutput} unavailable")

        with open(checkSumOutput) as f:
            checksums = json.load(f)

        # for each output file, compute the md5 checksum
        for output, checksum in checksums.items():
            hash = hashlib.md5()
            localOutput = os.path.join(workingDirectory, output)
            if not os.path.exists(localOutput):
                return S_ERROR(f"{localOutput} was expected but not found")

            with open(localOutput, "rb") as f:
                while chunk := f.read(128 * hash.block_size):
                    hash.update(chunk)
            if checksum != hash.hexdigest():
                return S_ERROR(f"{localOutput} is corrupted")

        return S_OK()

    def _checkSubmittedJobWrappers(self, ce: ComputingElement):
        """Check the status of the submitted tasks.
        If the task is finished, get the output and post process the job.
        Finally, remove from the submission dictionary.

        :return: S_OK/S_ERROR
        """
        if not self.submissionDict:
            return S_OK()

        if not (result := ce.getJobStatus(list(self.submissionDict.keys())))["OK"]:
            self.log.error("Failed to get job status", result["Message"])
            return result

        for taskID, status in result["Value"].items():
            job = self.submissionDict[taskID]["JobWrapper"]
            stamp = self.submissionDict[taskID]["Stamp"]
            if status not in PilotStatus.PILOT_FINAL_STATES:
                self.log.info("Job still running", f"(JobID: {job.jobID}, DIRAC taskID: {taskID}; Status: {status})")
                continue

            self.log.info("Job execution finished", f"(JobID: {job.jobID}, DIRAC taskID: {taskID}; Status: {status})")

            # Save the current directory location, and change to the job directory
            agentWorkingDirectory = os.getcwd()
            os.chdir(job.jobID)

            # Get the output of the job
            self.log.info(f"Getting the outputs of taskID {taskID}")
            if not (result := ce.getJobOutput(f"{taskID}:::{stamp}", os.path.abspath(".")))["OK"]:
                os.chdir(agentWorkingDirectory)
                self.log.error("Failed to get the output of taskID", f"{taskID}: {result['Message']}")
                return result

            # Make sure the output is correct
            self.log.info(f"Checking the integrity of the outputs of {taskID}")
            if not (result := self._checkOutputIntegrity("."))["OK"]:
                os.chdir(agentWorkingDirectory)
                self.log.error(
                    "Failed to check the integrity of the output of taskID", f"{taskID}: {result['Message']}"
                )
                self._rescheduleFailedJob(
                    job.jobID, message=f"{JobMinorStatus.JOB_WRAPPER_INITIALIZATION}: {result['Message']}"
                )
                shutil.rmtree(job.jobID)
                del self.submissionDict[taskID]
                return result
            self.log.info("The output has been retrieved and declared complete")

            with open(self.payloadResultFile) as f:
                payloadResults = json.load(f)
            result = job.postProcess(**payloadResults)
            if not result["OK"]:
                os.chdir(agentWorkingDirectory)
                self.log.error("Failed to post process the job", f"{job.jobID}: {result['Message']}")
                return result

            # Restore the agent working directory
            os.chdir(agentWorkingDirectory)

            # Clean job wrapper locally
            job.finalize()

            # Clean job in the remote resource
            if self.cleanTask:
                if not (result := ce.cleanJob(taskID))["OK"]:
                    self.log.warn("Failed to clean the output remotely", result["Message"])
                self.log.info(f"TaskID {taskID} has been remotely removed")

            # Remove the job from the submission dictionary
            del self.submissionDict[taskID]
        return S_OK()

    def finalize(self):
        """PushJob Agent finalization method"""

        if self.submissionPolicy == "Application":
            # wait for all jobs to be completed
            res = self.computingElement.shutdown()
            if not res["OK"]:
                self.log.error("CE could not be properly shut down", res["Message"])

            # Check the submitted jobs a last time
            result = self._checkSubmittedJobs()
            if not result["OK"]:
                self.log.error("Problem while trying to get status of the last submitted jobs")
        else:
            for _, queueDictionary in self.queueDict.items():
                ce = queueDictionary["CE"]
                # Check the submitted JobWrappers a last time
                result = self._checkSubmittedJobWrappers(ce)
                if not result["OK"]:
                    self.log.error("Problem while trying to get status of the last submitted JobWrappers")

        return S_OK()
