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
import time
from collections import defaultdict
from pathlib import Path

from DIRAC import S_ERROR, S_OK, gConfig
from DIRAC.ConfigurationSystem.Client.Helpers.Operations import Operations
from DIRAC.ConfigurationSystem.Client.Helpers.Resources import getQueues
from DIRAC.Core.Utilities import DErrno
from DIRAC.Core.Utilities.ObjectLoader import ObjectLoader
from DIRAC.Core.Utilities.Proxy import executeWithUserProxy
from DIRAC.Core.Utilities.Version import getVersion
from DIRAC.FrameworkSystem.Client.ProxyManagerClient import gProxyManager
from DIRAC.Resources.Computing import ComputingElement
from DIRAC.WorkloadManagementSystem.Agent.JobAgent import JobAgent
from DIRAC.WorkloadManagementSystem.Client import JobMinorStatus, JobStatus, PilotStatus
from DIRAC.WorkloadManagementSystem.Client.JobMonitoringClient import JobMonitoringClient
from DIRAC.WorkloadManagementSystem.Client.JobReport import JobReport
from DIRAC.WorkloadManagementSystem.JobWrapper.JobWrapper import JobWrapper
from DIRAC.WorkloadManagementSystem.JobWrapper.JobWrapperUtilities import (
    getJobWrapper,
    processJobOutputs,
    rescheduleFailedJob,
    resolveInputData,
    transferInputSandbox,
)
from DIRAC.WorkloadManagementSystem.private.ConfigHelper import findGenericPilotCredentials
from DIRAC.WorkloadManagementSystem.Utilities.JobParameters import getJobParameters
from DIRAC.WorkloadManagementSystem.Utilities.QueueUtilities import getQueuesResolved
from DIRAC.WorkloadManagementSystem.Utilities.Utils import createJobWrapper

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

        # Clients to interact with DIRAC services
        self.jobMonitoring = None
        self.resourcesModule = None
        self.opsHelper = None

        # Choose the submission policy
        # - Application (deprecated): the agent will submit a workflow to a PoolCE, the workflow is responsible for
        #   interacting with the remote site
        # - JobWrapper: the agent will submit a JobWrapper directly to the remote site, it is responsible of the
        #   remote execution
        self.submissionPolicy = "JobWrapper"

        # Options related to the "JobWrapper" submission policy
        # The location of Dirac on the subset of CVMFS, such as: /cvmfs/<repo>/<vo>dirac/<version>
        # To avoid having to manually update the version, we recommend to use a symlink
        # pointing to the latest release/pre-release
        self.cvmfsLocation = "/cvmfs/dirac.egi.eu/dirac/pro"

        # cleanTask is used to clean the task in the remote site
        self.cleanTask = True
        # The results of the payload execution will be stored in this file
        self.payloadResultFile = "payloadResult.json"
        # The results of the checksums will be stored in this file
        self.checkSumResultsFile = "checksums.json"

    def initialize(self):
        """Sets default parameters and creates CE instance"""
        super().initialize()

        self.jobMonitoring = JobMonitoringClient()

        # Get the submission policy
        # Initialized here because it cannot be dynamically modified during the execution
        self.submissionPolicy = self.am_getOption("SubmissionPolicy", self.submissionPolicy)
        if self.submissionPolicy not in ["Application", "JobWrapper"]:
            return S_ERROR("SubmissionPolicy must be either Workflow or JobWrapper")

        if self.submissionPolicy == "Application":
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

        # (only for Application submission) Maximum number of jobs that can be handled at the same time by the agent
        self.maxJobsToSubmit = self.am_getOption("MaxJobsToSubmit", self.maxJobsToSubmit)
        if self.submissionPolicy == "Application":
            self.computingElement.setParameters({"NumberOfProcessors": self.maxJobsToSubmit})

        self.failedQueueCycleFactor = self.am_getOption("FailedQueueCycleFactor", self.failedQueueCycleFactor)
        self.cleanTask = self.am_getOption("CleanTask", self.cleanTask)

        # (only for JobWrapper submission) Get the location of the CVMFS installation
        if self.submissionPolicy == "JobWrapper":
            self.cvmfsLocation = self.am_getOption("CVMFSLocation", self.cvmfsLocation)
            self.log.info("CVMFS location:", self.cvmfsLocation)

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

        # Get the version:
        # If the webapp is running on the same machine, the version is the one of the webapp so we should ignore it
        versions = getVersion()["Value"]["Extensions"]
        for extension, version in versions.items():
            if extension not in ["WebAppDIRAC", "DIRACWebAppResources"]:
                self.version = version
                break

        if not self.version:
            self.log.error("Cannot get the version of the agent")
            return S_ERROR("Cannot get the version of the agent")

        for queue in self.queueDict:
            ce = self.queueDict[queue]["CE"]
            architecture = f"Linux-{ce.ceParameters.get('architecture', 'x86_64')}"
            diracInstallLocation = os.path.join(self.cvmfsLocation, architecture)
            self.queueDict[queue]["ParametersDict"]["DIRACInstallLocation"] = diracInstallLocation

            if self.firstPass:
                self.log.always(
                    "Will serve Site: %s, CE: %s, Queue: %s"
                    % (self.queueDict[queue]["Site"], self.queueDict[queue]["CEName"], queue)
                )
                # Add preamble based on the CVMFS location to the CE
                ce.preamble = f"source {os.path.join(diracInstallLocation, 'diracosrc')}"

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

        # Get a pilot proxy
        cpuTime = 86400 * 3
        self.log.verbose("Getting pilot proxy", "for %s/%s %d long" % (self.pilotDN, self.vo, cpuTime))
        pilotGroup = Operations(vo=self.vo).getValue("Pilot/GenericPilotGroup")
        result = gProxyManager.getPilotProxyFromDIRACGroup(self.pilotDN, pilotGroup, cpuTime)
        if not result["OK"]:
            return result
        pilotProxy = result["Value"]

        # Dump the proxy to a file to get DiracX token (it's later used by DiracX)
        result = gProxyManager.dumpProxyToFile(pilotProxy)
        if not result["OK"]:
            return result
        os.environ["X509_USER_PROXY"] = result["Value"]

        for queueName, queueDictionary in queueDictItems:
            # Make sure there is no problem with the queue before trying to submit
            if not self._allowedToSubmit(queueName):
                continue

            # Get the CE instance
            ce = queueDictionary["CE"]
            ce.setProxy(pilotProxy)

            if self.submissionPolicy == "JobWrapper":
                # Check errors that could have occurred during job submission and/or execution
                result = self._checkSubmittedJobWrappers(ce, queueDictionary["ParametersDict"]["Site"])
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
                jobID = str(matcherInfo["JobID"])
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
                jobType = submissionParams["jobType"]

                self.log.verbose("Job request successful: \n", jobRequest["Value"])
                self.log.info("Received", f"JobID={jobID}, JobType={jobType}, Owner={owner}, JobGroup={jobGroup}")

                self.jobs[jobID]["JobReport"].setJobStatus(
                    status=JobStatus.MATCHED, minorStatus="Job Received by Agent", sendFlag=False
                )

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

                self.jobs[jobID]["JobReport"].setJobParameter(par_name="GridCE", par_value=ce.ceName, sendFlag=False)
                self.jobs[jobID]["JobReport"].setJobParameter(par_name="CEQueue", par_value=queueName, sendFlag=False)

                # Submit the job to the CE
                self.log.debug(f"Before self._submitJob() ({self.ceName}CE)")
                if self.submissionPolicy == "Application":
                    # Setup proxy
                    result_setupProxy = self._setupProxy(owner, jobGroup)
                    if not result_setupProxy["OK"]:
                        result = self._rescheduleFailedJob(jobID, result_setupProxy["Message"])
                        self.failedQueues[queueName] += 1
                        break
                    proxyChain = result_setupProxy.get("Value")

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
                        self._rescheduleFailedJob(jobID, resultSubmission["Message"])
                        self.failedQueues[queueName] += 1
                        break
                else:
                    resultSubmission = self._submitJobWrapper(
                        jobID=jobID,
                        ce=ce,
                        diracInstallLocation=queueDictionary["ParametersDict"]["DIRACInstallLocation"],
                        jobParams=params,
                        resourceParams=ceDict,
                        optimizerParams=optimizerParams,
                        processors=submissionParams["processors"],
                    )
                    if not result["OK"]:
                        self.failedQueues[queueName] += 1
                        break

                self.log.debug(f"After {self.ceName}CE submitJob()")

                if self.submissionPolicy == "Application":
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
        result = getQueues(community="", siteList=siteNames, ceList=ces, ceTypeList=ceTypes)
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

        ceDict["SubmissionPolicy"] = self.submissionPolicy

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

    #############################################################################

    @executeWithUserProxy
    def preProcessJob(self, job: JobWrapper):
        """Preprocess the job before submission: should be executed with the user proxy associated with the payload

        :param JobWrapper job: job to preprocess
        """
        if "InputSandbox" in job.jobArgs:
            self.log.verbose("Getting the inputSandbox of job", job.jobID)
            if not transferInputSandbox(job, job.jobArgs["InputSandbox"]):
                return S_ERROR(f"Cannot get input sandbox of job {job.jobID}")
            job.jobReport.commit()

        if "InputData" in job.jobArgs and job.jobArgs["InputData"]:
            self.log.verbose("Getting the inputData of job", job.jobID)
            if not resolveInputData(job):
                return S_ERROR(f"Cannot get input data of job {job.jobID}")
            job.jobReport.commit()

        # Preprocess the payload
        try:
            self.log.verbose("Pre-processing job", job.jobID)
            result = job.preProcess()
            if not result["OK"]:
                self.log.error("JobWrapper failed the preprocessing phase for", f"{job.jobID}: {result['Message']}")
                rescheduleResult = rescheduleFailedJob(
                    jobID=job.jobID, minorStatus=JobMinorStatus.JOB_WRAPPER_EXECUTION, jobReport=job.jobReport
                )
                job.sendJobAccounting(status=rescheduleResult, minorStatus=JobMinorStatus.JOB_WRAPPER_EXECUTION)
                return S_ERROR(JobMinorStatus.JOB_WRAPPER_EXECUTION)
        except Exception:
            self.log.exception("JobWrapper failed the preprocessing phase for", job.jobID)
            rescheduleResult = rescheduleFailedJob(
                jobID=job.jobID, minorStatus=JobMinorStatus.JOB_WRAPPER_EXECUTION, jobReport=job.jobReport
            )
            job.sendJobAccounting(status=rescheduleResult, minorStatus=JobMinorStatus.JOB_WRAPPER_EXECUTION)
            return S_ERROR(f"JobWrapper failed the preprocessing phase for {job.jobID}")

        job.jobReport.commit()
        return S_OK(result["Value"])

    def _submitJobWrapper(
        self,
        jobID: str,
        ce: ComputingElement,
        diracInstallLocation: str,
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
        :param owner: owner of the job
        :param jobGroup: group of the job
        :param processors: number of processors

        :return: S_OK
        """
        jobReport = self.jobs[jobID]["JobReport"]
        jobReport.commit()

        # Add the number of requested processors to the job environment
        if "ExecutionEnvironment" in jobParams:
            if isinstance(jobParams["ExecutionEnvironment"], str):
                jobParams["ExecutionEnvironment"] = jobParams["ExecutionEnvironment"].split(";")
        jobParams.setdefault("ExecutionEnvironment", []).append("DIRAC_JOB_PROCESSORS=%d" % processors)

        # Add necessary parameters to get the payload result and analyze its integrity
        jobParams["PayloadResults"] = self.payloadResultFile
        jobParams["Checksum"] = self.checkSumResultsFile
        # The dirac-jobexec executable is available through CVMFS in the remote site
        # So we need to change the path to the executable
        if "Executable" in jobParams and jobParams["Executable"] == "dirac-jobexec":
            jobParams["Executable"] = os.path.join(diracInstallLocation, "bin", "dirac-jobexec")
            jobParams["Arguments"] += " --cfg=dirac.cfg"

        # Prepare the job for submission
        self.log.verbose("Getting a JobWrapper")
        arguments = {"Job": jobParams, "CE": resourceParams, "Optimizer": optimizerParams}

        job = getJobWrapper(int(jobID), arguments, jobReport)
        if not job:
            return S_ERROR(f"Cannot get a JobWrapper instance for job {jobID}")

        result = self.preProcessJob(  # pylint: disable=unexpected-keyword-arg
            job, proxyUserName=job.owner, proxyUserGroup=job.userGroup
        )
        if not result["OK"]:
            return result
        payloadParams = result["Value"]

        # Dump the remote CFG config into the job directory: it is needed for the JobWrapperTemplate
        cfgFilename = Path(job.jobIDPath) / "dirac.cfg"
        gConfig.dumpRemoteCFGToFile(cfgFilename)

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
            cfgPath=cfgFilename.name,
            defaultWrapperLocation="DIRAC/WorkloadManagementSystem/JobWrapper/JobWrapperOfflineTemplate.py",
            pythonPath="python",
            rootLocation=".",
            **jobDesc,
        )
        if not result["OK"]:
            rescheduleResult = rescheduleFailedJob(
                jobID=job.jobID, minorStatus=JobMinorStatus.JOB_WRAPPER_EXECUTION, jobReport=jobReport
            )
            job.sendJobAccounting(status=rescheduleResult, minorStatus=JobMinorStatus.JOB_WRAPPER_EXECUTION)
            return result
        jobWrapperRunner = result["Value"]["JobExecutablePath"]
        jobWrapperCode = result["Value"]["JobWrapperPath"]
        jobWrapperConfig = result["Value"]["JobWrapperConfigPath"]

        # Get inputs from the JobWrapper working directory and add the JobWrapper deps
        jobInputs = os.listdir(job.jobIDPath)
        inputs = [os.path.join(job.jobIDPath, input) for input in jobInputs]
        inputs.append(jobWrapperCode)
        inputs.append(jobWrapperConfig)
        self.log.verbose("The executable will be sent along with the following inputs:", ",".join(inputs))

        # Request the whole directory as output
        outputs = ["/"]

        self.log.info("Submitting JobWrapper", f"{os.path.basename(jobWrapperRunner)} to {ce.ceName}CE")

        # Submit the job
        result = ce.submitJob(
            executableFile=jobWrapperRunner,
            proxy=None,
            inputs=inputs,
            outputs=outputs,
        )
        if not result["OK"]:
            rescheduleResult = rescheduleFailedJob(
                jobID=job.jobID, minorStatus=JobMinorStatus.JOB_WRAPPER_EXECUTION, jobReport=jobReport
            )
            job.sendJobAccounting(status=rescheduleResult, minorStatus=JobMinorStatus.JOB_WRAPPER_EXECUTION)
            return result

        taskID = result["Value"][0]
        stamp = result["PilotStampDict"][taskID]
        self.log.info("Job being submitted", f"(DIRAC JobID: {jobID}; Task ID: {taskID})")

        jobReport.setJobParameter(par_name="TaskID", par_value=taskID, sendFlag=False)
        jobReport.setJobParameter(par_name="Stamp", par_value=stamp, sendFlag=False)
        jobReport.commit()

        time.sleep(self.jobSubmissionDelay)
        return S_OK()

    #############################################################################

    def _checkOutputIntegrity(self, workingDirectory: str):
        """Make sure that output files are not corrupted.

        :param workingDirectory: path of the outputs
        """
        checkSumOutput = os.path.join(workingDirectory, self.checkSumResultsFile)
        if not os.path.exists(checkSumOutput):
            return S_ERROR(
                DErrno.EWMSRESC, f"Cannot guarantee the integrity of the outputs: {checkSumOutput} unavailable"
            )

        with open(checkSumOutput) as f:
            checksums = json.load(f)

        # for each output file, compute the md5 checksum
        for output, checksum in checksums.items():
            localOutput = os.path.join(workingDirectory, output)
            if not os.path.exists(localOutput):
                return S_ERROR(f"{localOutput} was expected but not found")

            with open(localOutput, "rb") as f:
                digest = hashlib.file_digest(f, "sha256")

            if checksum != digest.hexdigest():
                return S_ERROR(f"{localOutput} is corrupted")

        return S_OK()

    @executeWithUserProxy
    def postProcessJob(self, job: JobWrapper, payloadResults):
        """Perform post-processing for a job: should be executed with the user proxy associated with the payload.

        :param job: JobWrapper instance
        :param payloadResults: Payload results
        """
        try:
            result = job.postProcess(**payloadResults)
            if not result["OK"]:
                self.log.error("Failed to post process the job", f"{job.jobID}: {result['Message']}")
                if result["Errno"] == DErrno.EWMSRESC:
                    self.log.warn("Asked to reschedule job")
                    rescheduleResult = rescheduleFailedJob(
                        jobID=job.jobID, minorStatus=JobMinorStatus.JOB_WRAPPER_EXECUTION, jobReport=job.jobReport
                    )
                    job.sendJobAccounting(status=rescheduleResult, minorStatus=JobMinorStatus.JOB_WRAPPER_EXECUTION)
                    shutil.rmtree(job.jobIDPath)
                    return

                job.jobReport.setJobParameter("Error Message", result["Message"], sendFlag=False)
                job.jobReport.setJobStatus(
                    status=JobStatus.FAILED, minorStatus=JobMinorStatus.EXCEPTION_DURING_EXEC, sendFlag=False
                )
                job.sendFailoverRequest()
                job.sendJobAccounting(status=JobStatus.FAILED, minorStatus=JobMinorStatus.EXCEPTION_DURING_EXEC)
                shutil.rmtree(job.jobIDPath)
                return
        except Exception as exc:  # pylint: disable=broad-except
            self.log.exception("Job raised exception during post processing phase")
            job.jobReport.setJobParameter("Error Message", repr(exc), sendFlag=False)
            job.jobReport.setJobStatus(
                status=JobStatus.FAILED, minorStatus=JobMinorStatus.EXCEPTION_DURING_EXEC, sendFlag=False
            )
            job.sendFailoverRequest()
            job.sendJobAccounting(status=JobStatus.FAILED, minorStatus=JobMinorStatus.EXCEPTION_DURING_EXEC)
            shutil.rmtree(job.jobIDPath)
            return

        if "OutputSandbox" in job.jobArgs or "OutputData" in job.jobArgs:
            self.log.verbose("Uploading the outputSandbox/Data of the job")
            if not processJobOutputs(job):
                shutil.rmtree(job.jobIDPath)
                return
            job.jobReport.commit()

        # Clean job wrapper locally
        job.finalize()

    def _checkSubmittedJobWrappers(self, ce: ComputingElement, site: str):
        """Check the status of the submitted tasks.
        If the task is finished, get the output and post process the job.
        Finally, remove from the submission dictionary.

        :return: S_OK/S_ERROR
        """
        # Get the list of running jobs
        if not (result := self.jobMonitoring.getJobs({"Status": JobStatus.RUNNING, "Site": site}))["OK"]:
            self.log.error("Failed to get the list of running jobs", result["Message"])
            return result

        jobs = result["Value"]
        if not jobs:
            self.log.info("No running jobs")
            return S_OK()

        # Get their parameters
        if not (result := getJobParameters(jobs, ["GridCE", "TaskID", "Stamp"]))["OK"]:
            self.log.error("Failed to get the list of taskIDs", result["Message"])
            return result

        # Filter the jobs that are running on the current CE
        taskIDs = {}
        for jobID, values in result["Value"].items():
            if "GridCE" not in values or values["GridCE"] != ce.ceName:
                continue
            if "TaskID" not in values:
                continue
            if "Stamp" not in values:
                continue
            taskIDs[values["TaskID"]] = {"JobID": jobID, "Stamp": values["Stamp"]}

        if not taskIDs:
            self.log.info("No running jobs on the current CE")
            return S_OK()

        # Get the status of the jobs
        if not (result := ce.getJobStatus(list(taskIDs.keys())))["OK"]:
            self.log.error("Failed to get job status", result["Message"])
            return result

        statusDict = result["Value"]
        for taskID, status in statusDict.items():
            # Get the jobID and the stamp
            jobID = taskIDs[taskID]["JobID"]
            stamp = taskIDs[taskID]["Stamp"]

            # Check if the job is still running
            if status not in PilotStatus.PILOT_FINAL_STATES:
                self.log.info("Job not finished", f"(JobID: {jobID}, DIRAC taskID: {taskID}; Status: {status})")
                continue
            self.log.info("Job execution finished", f"(JobID: {jobID}, DIRAC taskID: {taskID}; Status: {status})")

            # Get the JDL of the job
            if not (result := self.jobMonitoring.getJobJDL(int(jobID), False))["OK"]:
                self.log.error("Failed to get the JDL of job", f"{jobID}: {result['Message']}")
                continue

            if not (result := self._getJDLParameters(result["Value"]))["OK"]:
                self.log.error("Failed to extract the JDL parameters of job", f"{jobID}: {result['Message']}")
                continue

            # Get the job and ce parameters
            jobParams = result["Value"]

            result = self._getCEDict(ce)
            if not (result := self._getCEDict(ce))["OK"]:
                self.log.error("Failed to get the CE parameters of job", f"{jobID}: {result['Message']}")
                continue
            ceDict = result["Value"][0]
            ceDict["NumberOfProcessors"] = ce.ceParameters.get("NumberOfProcessors")

            self.log.verbose(f"Restoring the JobWrapper of job {jobID}")
            arguments = {"Job": jobParams, "CE": ceDict, "Optimizer": {}}

            # Get the job wrapper
            jobReport = JobReport(jobID, f"{self.__class__.__name__}@{self.siteName}")
            try:
                job = JobWrapper(int(jobID), jobReport)
                job.initialize(arguments)
            except Exception:
                self.log.exception("JobWrapper failed the initialization phase", jobID)
                continue

            # Get the output of the job
            self.log.info(f"Getting the outputs of taskID {taskID} for {jobID}")
            if not (result := ce.getJobOutput(f"{taskID}:::{stamp}", job.jobIDPath))["OK"]:
                self.log.error("Failed to get the output of taskID", f"{taskID}: {result['Message']}")
                continue

            # Make sure the output is correct
            self.log.info(f"Checking the integrity of the outputs of {taskID} for {jobID}")
            if not (result := self._checkOutputIntegrity(job.jobIDPath))["OK"]:
                self.log.error(
                    "Failed to check the integrity of the output of taskID", f"{taskID}: {result['Message']}"
                )
                if result["Errno"] == DErrno.EWMSRESC:
                    self.log.warn("Asked to reschedule job")
                    rescheduleResult = rescheduleFailedJob(
                        jobID=jobID, minorStatus=JobMinorStatus.JOB_WRAPPER_EXECUTION, jobReport=jobReport
                    )
                    job.sendJobAccounting(status=rescheduleResult, minorStatus=JobMinorStatus.JOB_WRAPPER_EXECUTION)
                    shutil.rmtree(job.jobIDPath)
                continue
            self.log.info("The output has been retrieved and declared complete")

            with open(os.path.join(job.jobIDPath, self.payloadResultFile)) as f:
                result = json.load(f)

            if not result["OK"]:
                self.log.error("Failed to get the payload results of job", f"{jobID}: {result['Message']}")
                self.log.warn("Asked to reschedule job")
                rescheduleResult = rescheduleFailedJob(
                    jobID=jobID, minorStatus=JobMinorStatus.JOB_WRAPPER_EXECUTION, jobReport=jobReport
                )
                job.sendJobAccounting(status=rescheduleResult, minorStatus=JobMinorStatus.JOB_WRAPPER_EXECUTION)
                shutil.rmtree(job.jobIDPath)
                continue

            payloadResults = result["Value"]
            self.postProcessJob(  # pylint: disable=unexpected-keyword-arg
                job, payloadResults, proxyUserName=job.owner, proxyUserGroup=job.userGroup
            )

            # Clean job in the remote resource
            if self.cleanTask:
                if not (result := ce.cleanJob(taskID))["OK"]:
                    self.log.warn("Failed to clean the output remotely", result["Message"])
                self.log.info(f"TaskID {taskID} has been remotely removed")

        return S_OK()

    def finalize(self):
        """PushJob Agent finalization method"""
        if self.submissionPolicy == "Application":
            # wait for all jobs to be completed
            res = self.computingElement.shutdown()
            if not res["OK"]:
                self.log.error("CE could not be properly shut down", res["Message"])

            # Check the latest submitted jobs
            while self.jobs:
                result = self._checkSubmittedJobs()
                if not result["OK"]:
                    self.log.error("Problem while trying to get status of the last submitted jobs")
                    break
                time.sleep(int(self.am_getOption("PollingTime")))

        return S_OK()
