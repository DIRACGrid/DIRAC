"""
  The Job Agent class instantiates a CE that acts as a client to a
  compute resource and also to the WMS.
  The Job Agent constructs a classAd based on the local resource description in the CS
  and the current resource status that is used for matching.
"""
import os
import re
import sys
import time
from pathlib import Path

from diraccfg import CFG

from DIRAC import S_ERROR, S_OK, gConfig, rootPath, siteName
from DIRAC.ConfigurationSystem.Client.Helpers.Registry import getDNForUsername
from DIRAC.Core.Base.AgentModule import AgentModule
from DIRAC.Core.Security import Properties
from DIRAC.Core.Security.ProxyFile import writeChainToTemporaryFile
from DIRAC.Core.Security.ProxyInfo import getProxyInfo
from DIRAC.Core.Utilities import DErrno
from DIRAC.Core.Utilities.CGroups2 import CG2Manager
from DIRAC.Core.Utilities.ClassAd.ClassAdLight import ClassAd
from DIRAC.Core.Utilities.ObjectLoader import ObjectLoader
from DIRAC.FrameworkSystem.Client.ProxyManagerClient import gProxyManager
from DIRAC.RequestManagementSystem.Client.ReqClient import ReqClient
from DIRAC.RequestManagementSystem.Client.Request import Request
from DIRAC.RequestManagementSystem.private.RequestValidator import RequestValidator
from DIRAC.Resources.Computing.BatchSystems.TimeLeft.TimeLeft import TimeLeft
from DIRAC.Resources.Computing.ComputingElementFactory import ComputingElementFactory
from DIRAC.WorkloadManagementSystem.Client import JobStatus, PilotStatus
from DIRAC.WorkloadManagementSystem.Client.JobManagerClient import JobManagerClient
from DIRAC.WorkloadManagementSystem.Client.JobMonitoringClient import JobMonitoringClient
from DIRAC.WorkloadManagementSystem.Client.JobReport import JobReport
from DIRAC.WorkloadManagementSystem.Client.MatcherClient import MatcherClient
from DIRAC.WorkloadManagementSystem.Client.PilotManagerClient import PilotManagerClient
from DIRAC.WorkloadManagementSystem.Utilities.Utils import createJobWrapper


class JobAgent(AgentModule):
    """This agent is what runs in a worker node. The pilot runs it, after having prepared its configuration."""

    def __init__(self, agentName, loadName, baseAgentName=False, properties=None):
        """Just defines some default parameters"""

        if not properties:
            properties = {}
        super().__init__(agentName, loadName, baseAgentName, properties)

        # disable activity monitoring for this agent
        self.activityMonitoring = False

        # Inner CE
        # CE type the JobAgent submits to. It can be "InProcess" or "Pool" or "Singularity".
        self.ceName = "InProcess"
        # "Inner" CE submission type (e.g. for Pool CE). It can be "InProcess" or "Singularity".
        self.innerCESubmissionType = "InProcess"
        self.computingElement = None  # The ComputingElement object, e.g. SingularityComputingElement()

        # Localsite options
        self.siteName = "Unknown"
        self.pilotReference = "Unknown"
        self.defaultProxyLength = 86400 * 5

        # Agent options
        # This is the factor to convert raw CPU to Normalized units (based on the CPU Model)
        self.cpuFactor = 0.0
        self.jobSubmissionDelay = 10
        self.fillingMode = True
        self.minimumTimeLeft = 5000
        self.stopOnApplicationFailure = True
        self.hostFailureCount = 0
        self.stopAfterHostFailures = 3
        self.matchFailedCount = 0
        self.stopAfterFailedMatches = 10
        self.jobCount = 0
        self.extraOptions = ""
        self.logLevel = "INFO"
        self.defaultWrapperLocation = "DIRAC/WorkloadManagementSystem/JobWrapper/JobWrapperTemplate.py"

        # Timeleft
        self.initTimes = os.times()
        self.initTimeLeft = 0.0
        self.timeLeft = self.initTimeLeft
        self.timeLeftUtil = None
        self.pilotInfoReportedFlag = False

        # Attributes related to the processed jobs, it should take the following form:
        # {"<jobID>": {"jobReport": JobReport(), "taskID": "<taskID>"}}
        # where taskID is the ID of the job as seen by the CE
        # and jobReport is the JobReport instance for the job
        # (one instance per job to avoid any discrepancy when communicating with the WMS)
        self.jobs = {}

    #############################################################################
    def initialize(self):
        """Sets default parameters and creates CE instance"""

        self.am_setOption("MonitoringEnabled", False)

        localCE = gConfig.getValue("/LocalSite/LocalCE", self.ceName)
        if localCE != self.ceName:
            self.log.info("Defining Inner CE from local configuration", f"= {localCE}")

        # Create backend Computing Element
        result = self._initializeComputingElement(localCE)
        if not result["OK"]:
            return result

        result = self._getCEDict(self.computingElement)
        if not result["OK"]:
            return result
        ceDict = result["Value"][0]

        self.initTimeLeft = ceDict.get("CPUTime", self.initTimeLeft)
        self.initTimeLeft = gConfig.getValue("/Resources/Computing/CEDefaults/MaxCPUTime", self.initTimeLeft)
        self.timeLeft = self.initTimeLeft

        self.initTimes = os.times()
        # Localsite options
        self.siteName = siteName()
        self.pilotReference = gConfig.getValue("/LocalSite/PilotReference", self.pilotReference)
        self.defaultProxyLength = gConfig.getValue("/Registry/DefaultProxyLifeTime", self.defaultProxyLength)
        # Agent options
        # This is the factor to convert raw CPU to Normalized units (based on the CPU Model)
        self.cpuFactor = gConfig.getValue("/LocalSite/CPUNormalizationFactor", self.cpuFactor)
        self.jobSubmissionDelay = self.am_getOption("SubmissionDelay", self.jobSubmissionDelay)
        self.fillingMode = self.am_getOption("FillingModeFlag", self.fillingMode)
        self.minimumTimeLeft = self.am_getOption("MinimumTimeLeft", self.minimumTimeLeft)
        self.stopOnApplicationFailure = self.am_getOption("StopOnApplicationFailure", self.stopOnApplicationFailure)
        self.stopAfterHostFailures = self.am_getOption("StopAfterHostFailures", self.stopAfterHostFailures)
        self.stopAfterFailedMatches = self.am_getOption("StopAfterFailedMatches", self.stopAfterFailedMatches)
        self.extraOptions = gConfig.getValue("/AgentJobRequirements/ExtraOptions", self.extraOptions)
        self.logLevel = self.am_getOption("DefaultLogLevel", self.logLevel)
        self.defaultWrapperLocation = self.am_getOption("JobWrapperTemplate", self.defaultWrapperLocation)

        # Utilities
        self.timeLeftUtil = TimeLeft()

        # Some innerCEs may want to make use of CGroup2 support, so we prepare it globally here
        res = CG2Manager().setUp()
        if res["OK"]:
            self.log.info("CGroup2 support configured successfully.")
        else:
            self.log.info("CGroup2 support unavailable:", res["Message"])

        return S_OK()

    def _initializeComputingElement(self, localCE):
        """Generate a ComputingElement and configure it"""
        ceFactory = ComputingElementFactory()
        self.ceName = localCE.split("/")[0]  # It might be "Pool/Singularity", or simply "Pool"
        self.innerCESubmissionType = (
            localCE.split("/")[1] if len(localCE.split("/")) == 2 else self.innerCESubmissionType
        )
        ceInstance = ceFactory.getCE(self.ceName)
        if not ceInstance["OK"]:
            self.log.warn("Can't instantiate a CE", ceInstance["Message"])
            return ceInstance
        self.computingElement = ceInstance["Value"]
        self.computingElement.setParameters({"InnerCESubmissionType": self.innerCESubmissionType})

        return S_OK()

    #############################################################################
    def execute(self):
        """The JobAgent execution method."""

        # Temporary mechanism to pass a shutdown message to the agent
        if os.path.exists("/var/lib/dirac_drain"):
            return self._finish("Node is being drained by an operator")

        self.log.verbose("Job Agent execution loop")
        # Check that there is enough slots to match a job
        result = self._checkCEAvailability(self.computingElement)
        if not result["OK"]:
            return self._finish(result["Message"])
        if result["OK"] and result["Value"]:
            return result

        # Check that we are allowed to continue and that time left is sufficient
        if self.jobCount:
            cpuWorkLeft = self._computeCPUWorkLeft()
            result = self._checkCPUWorkLeft(cpuWorkLeft)
            if not result["OK"]:
                return result
            result = self._setCPUWorkLeft(cpuWorkLeft)
            if not result["OK"]:
                return result

        # Get environment details and enhance them
        result = self._getCEDict(self.computingElement)
        if not result["OK"]:
            return result
        ceDictList = result["Value"]

        for ceDict in ceDictList:
            self._setCEDict(ceDict)

        # Try to match a job
        jobRequest = self._matchAJob(ceDictList)

        if not jobRequest["OK"]:
            res = self._checkMatchingIssues(jobRequest)
            if not res["OK"]:
                self._finish(res["Message"])
                return res

            # if we don't match a job, independently from the reason,
            # we wait a bit longer before trying again
            time.sleep(int(self.am_getOption("PollingTime")) * (self.matchFailedCount + 1) * 2)
            return res

        # If we are, we matched a job
        # Reset the Counter
        self.matchFailedCount = 0

        # Check matcher information returned
        matcherParams = ["JDL", "Owner", "Group"]
        matcherInfo = jobRequest["Value"]
        jobID = str(matcherInfo["JobID"])

        self.jobs[jobID] = {}
        self.jobs[jobID]["JobReport"] = JobReport(jobID, f"{self.__class__.__name__}@{self.siteName}")

        result = self._checkMatcherInfo(jobID, matcherInfo, matcherParams)
        if not result["OK"]:
            return self._finish(result["Message"])

        # Get matcher information
        if not self.pilotInfoReportedFlag:
            # Check the flag after the first access to the Matcher
            self.pilotInfoReportedFlag = matcherInfo.get("PilotInfoReportedFlag", False)

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
            return self._finish("JDL Problem", self.stopOnApplicationFailure)

        params = parameters["Value"]
        result = self._extractValuesFromJobParams(params)
        if not result["OK"]:
            self.jobs[jobID]["JobReport"].setJobStatus(status=JobStatus.FAILED, minorStatus=result["Message"])
            return self._finish(result["Value"], self.stopOnApplicationFailure)
        submissionParams = result["Value"]
        jobType = submissionParams["jobType"]

        self.log.verbose("Job request successful: \n", jobRequest["Value"])
        self.log.info("Received", f"JobID={jobID}, JobType={jobType}, Owner={owner}, JobGroup={jobGroup}")
        self.jobCount += 1

        self.jobs[jobID]["JobReport"].setJobStatus(minorStatus="Job Received by Agent", sendFlag=False)
        result_setupProxy = self._setupProxy(owner, jobGroup)
        if not result_setupProxy["OK"]:
            result = self._rescheduleFailedJob(jobID, result_setupProxy["Message"])
            return self._finish(result["Message"], self.stopOnApplicationFailure)
        proxyChain = result_setupProxy.get("Value")

        # Save the job jdl for external monitoring
        self._saveJobJDLRequest(jobID, jobJDL)

        # Check software and install them if required
        self.jobs[jobID]["JobReport"].setJobStatus(minorStatus="Installing Software", sendFlag=False)
        software = self._checkInstallSoftware(params, ceDict)
        if not software["OK"]:
            self.log.error("Failed to install software for job", f"{jobID}")
            errorMsg = software["Message"]
            if not errorMsg:
                errorMsg = "Failed software installation"
            result = self._rescheduleFailedJob(jobID, errorMsg)
            return self._finish(result["Message"], self.stopOnApplicationFailure)

        gridCE = gConfig.getValue("/LocalSite/GridCE", "")
        if gridCE:
            self.jobs[jobID]["JobReport"].setJobParameter(par_name="GridCE", par_value=gridCE, sendFlag=False)

        queue = gConfig.getValue("/LocalSite/CEQueue", "")
        if queue:
            self.jobs[jobID]["JobReport"].setJobParameter(par_name="CEQueue", par_value=queue, sendFlag=False)

        if batchSystem := gConfig.getValue("/LocalSite/BatchSystemInfo/Type", ""):
            self.jobs[jobID]["JobReport"].setJobParameter(par_name="BatchSystem", par_value=batchSystem, sendFlag=False)

        self.log.debug(f"Before self._submitJob() ({self.ceName}CE)")
        result = self._submitJob(
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
            # This should only happen if an error occurred before the actual submission to the CE
            result = self._rescheduleFailedJob(jobID, f"Job submission failed: {result['Message']}")
            return self._finish(result["Message"])
        self.log.debug(f"After {self.ceName}CE submitJob()")

        # Checking errors that could have occurred during the job submission and/or execution
        result = self._checkSubmittedJobs()
        if not result["OK"]:
            return result

        submissionErrors = result["Value"][0]
        payloadErrors = result["Value"][1]
        if submissionErrors:
            # Stop the JobAgent if too many CE errors occurred
            return self._finish(
                "Error during the submission process", self.hostFailureCount > self.stopAfterHostFailures
            )
        if payloadErrors:
            return self._finish("Error during a payload execution", self.stopOnApplicationFailure)

        return S_OK("Job Agent cycle complete")

    #############################################################################
    def _saveJobJDLRequest(self, jobID, jobJDL):
        """Save job JDL local to JobAgent."""
        classAdJob = ClassAd(jobJDL)
        classAdJob.insertAttributeString("LocalCE", self.ceName)
        jdlFileName = jobID + ".jdl"
        jdlFile = open(jdlFileName, "w")
        jdl = classAdJob.asJDL()
        jdlFile.write(jdl)
        jdlFile.close()

    #############################################################################
    def _getCEDict(self, computingElement):
        """Get CE description

        :param ComputingElement computingElement: ComputingElement instance
        :return: list of dict of attributes
        """
        # if we are here we assume that a job can be matched
        result = computingElement.getDescription()
        if not result["OK"]:
            self.log.warn("Can not get the CE description")
            return result

        # We can have several prioritized job retrieval strategies
        if isinstance(result["Value"], dict):
            ceDictList = [result["Value"]]
        else:
            # This is the case for Pool ComputingElement, and parameter 'MultiProcessorStrategy'
            ceDictList = result["Value"]

        return S_OK(ceDictList)

    def _setCEDict(self, ceDict):
        """Set CEDict"""
        # Add pilot information
        gridCE = gConfig.getValue("LocalSite/GridCE", "Unknown")
        if gridCE != "Unknown":
            ceDict["GridCE"] = gridCE
        if "PilotReference" not in ceDict:
            ceDict["PilotReference"] = str(self.pilotReference)
        ceDict["PilotBenchmark"] = self.cpuFactor
        ceDict["PilotInfoReportedFlag"] = self.pilotInfoReportedFlag

        # Add possible job requirements
        result = gConfig.getOptionsDict("/AgentJobRequirements")
        if result["OK"]:
            requirementsDict = result["Value"]
            ceDict.update(requirementsDict)
            self.log.info("Requirements:", requirementsDict)

    def _checkCEAvailability(self, computingElement):
        """Check availability of computingElement"""
        result = computingElement.available()
        if not result["OK"]:
            self.log.info("Resource is not available", result["Message"])
            return S_ERROR("CE Not Available")

        ceInfoDict = result["CEInfoDict"]
        runningJobs = ceInfoDict.get("RunningJobs")
        availableSlots = result["Value"]

        if not availableSlots:
            if runningJobs:
                self.log.info("No available slots", ": %d running jobs" % runningJobs)
                return S_OK("Job Agent cycle complete with %d running jobs" % runningJobs)
            self.log.info("CE is not available (and there are no running jobs)")
            return S_ERROR("CE Not Available")
        return S_OK()

    #############################################################################
    def _computeCPUWorkLeft(self, processors=1):
        """
        Compute CPU Work Left in hepspec06 seconds

        :param int processors: number of processors available
        :return: cpu work left (cpu time left * cpu power of the cpus)
        """
        # Sum all times but the last one (elapsed_time) and remove times at init (is this correct?)
        cpuTimeConsumed = sum(os.times()[:-1]) - sum(self.initTimes[:-1])
        result = self.timeLeftUtil.getTimeLeft(cpuTimeConsumed, processors)
        if not result["OK"]:
            self.log.warn("There were errors calculating time left using the Timeleft utility", result["Message"])
            self.log.warn("The time left will be calculated using os.times() and the info in our possession")
            self.log.info(f"Current raw CPU time consumed is {cpuTimeConsumed}")
            if self.cpuFactor:
                return self.initTimeLeft - cpuTimeConsumed * self.cpuFactor
            return self.timeLeft
        return result["Value"]

    def _checkCPUWorkLeft(self, cpuWorkLeft):
        """Check that fillingMode is enabled and time left is sufficient to continue the execution"""
        # Only call timeLeft utility after a job has been picked up
        self.log.info("Attempting to check CPU time left for filling mode")
        if not self.fillingMode:
            return self._finish("Filling Mode is Disabled")

        self.log.info("normalized CPU units remaining in slot", cpuWorkLeft)
        if cpuWorkLeft <= self.minimumTimeLeft:
            return self._finish("No more time left")
        return S_OK()

    def _setCPUWorkLeft(self, cpuWorkLeft):
        """Update the TimeLeft within the CE and the configuration for next matching request"""
        self.timeLeft = cpuWorkLeft

        result = self.computingElement.setCPUTimeLeft(cpuTimeLeft=self.timeLeft)
        if not result["OK"]:
            return self._finish(result["Message"])

        self._updateConfiguration("CPUTimeLeft", self.timeLeft)
        return S_OK()

    #############################################################################
    def _updateConfiguration(self, key, value, path="/LocalSite"):
        """Update local configuration to be used by submitted job wrappers"""
        localCfg = CFG()
        if self.extraOptions:
            localConfigFile = os.path.join(".", self.extraOptions)
        else:
            localConfigFile = os.path.join(rootPath, "etc", "dirac.cfg")
        localCfg.loadFromFile(localConfigFile)

        section = "/"
        for p in path.split("/")[1:]:
            section = os.path.join(section, p)
            if not localCfg.isSection(section):
                localCfg.createNewSection(section)

        localCfg.setOption(f"{section}/{key}", value)
        localCfg.writeToFile(localConfigFile)

    #############################################################################
    def _setupProxy(self, owner, ownerGroup):
        """
        Retrieve a proxy for the execution of the job
        """
        ownerDN = getDNForUsername(owner)["Value"][0]
        if gConfig.getValue("/DIRAC/Security/UseServerCertificate", False):
            proxyResult = self._requestProxyFromProxyManager(ownerDN, ownerGroup)
            if not proxyResult["OK"]:
                self.log.error("Failed to setup proxy", proxyResult["Message"])
                return S_ERROR(f"Failed to setup proxy: {proxyResult['Message']}")
            return S_OK(proxyResult["Value"])

        ret = getProxyInfo(disableVOMS=True)
        if not ret["OK"]:
            self.log.error("Invalid Proxy", ret["Message"])
            return S_ERROR("Invalid Proxy")

        proxyChain = ret["Value"]["chain"]
        if "groupProperties" not in ret["Value"]:
            self.log.error("Invalid Proxy", "Group has no properties defined")
            return S_ERROR("Proxy has no group properties defined")

        groupProps = ret["Value"]["groupProperties"]
        if Properties.GENERIC_PILOT in groupProps:
            proxyResult = self._requestProxyFromProxyManager(ownerDN, ownerGroup)
            if not proxyResult["OK"]:
                self.log.error("Invalid Proxy", proxyResult["Message"])
                return S_ERROR(f"Failed to setup proxy: {proxyResult['Message']}")
            proxyChain = proxyResult["Value"]

        return S_OK(proxyChain)

    def _requestProxyFromProxyManager(self, ownerDN, ownerGroup):
        """Retrieves user proxy with correct role for job and sets up environment to
        run job locally.
        """

        self.log.info(f"Requesting proxy', 'for {ownerDN}@{ownerGroup}")
        retVal = gProxyManager.getPayloadProxyFromDIRACGroup(ownerDN, ownerGroup, self.defaultProxyLength)
        if not retVal["OK"]:
            self.log.error("Could not retrieve payload proxy", retVal["Message"])
            os.system("dirac-proxy-info")
            sys.stdout.flush()
            return S_ERROR("Error retrieving proxy")

        chain = retVal["Value"]
        return S_OK(chain)

    #############################################################################
    def _checkInstallSoftware(self, jobParams, resourceParams):
        """Checks software requirement of job and whether this is already present
        before installing software locally.
        """
        if "SoftwareDistModule" not in jobParams:
            msg = "Job has no software installation requirement"
            self.log.verbose(msg)
            return S_OK(msg)

        softwareDist = jobParams["SoftwareDistModule"]
        self.log.verbose("Found VO Software Distribution module", f": {softwareDist}")
        argumentsDict = {"Job": jobParams, "CE": resourceParams}

        result = ObjectLoader().loadObject(softwareDist)
        if not result["OK"]:
            return result
        module = result["Value"](argumentsDict)

        return module.execute()

    #############################################################################
    def _matchAJob(self, ceDictList):
        """Call the Matcher with each ceDict until we get a job"""
        jobRequest = S_ERROR("No CE Dictionary available")
        for ceDict in ceDictList:
            self.log.verbose("CE dict", ceDict)

            start = time.time()
            jobRequest = MatcherClient().requestJob(ceDict)
            matchTime = time.time() - start

            self.log.verbose("MatcherTime", f"= {matchTime:.2f} (s)")
            if jobRequest["OK"]:
                jobRequest["Value"]["matchTime"] = matchTime
                jobRequest["Value"]["CEDict"] = ceDict
                break
        return jobRequest

    def _checkMatchingIssues(self, jobRequest):
        """Check the source of the matching issue

        :param dict jobRequest: S_ERROR returned by the matcher
        :return: S_OK/S_ERROR
        """
        if DErrno.cmpError(jobRequest, DErrno.EWMSPLTVER):
            self.log.error("Pilot version mismatch", jobRequest["Message"])
            return jobRequest

        if DErrno.cmpError(jobRequest, DErrno.EWMSNOMATCH):
            self.log.notice("Job request OK, but no match found", jobRequest["Message"])
        elif jobRequest["Message"].find("seconds timeout") != -1:
            self.log.error("Timeout while requesting job", jobRequest["Message"])
        else:
            self.log.notice("Failed to get jobs", jobRequest["Message"])

        self.matchFailedCount += 1
        if self.matchFailedCount > self.stopAfterFailedMatches:
            return self._finish("Nothing to do for more than %d cycles" % self.stopAfterFailedMatches)
        return S_OK()

    def _checkMatcherInfo(self, jobID, matcherInfo, matcherParams):
        """Check that all relevant information about the job are available"""
        for param in matcherParams:
            if param not in matcherInfo:
                self.jobs[jobID]["JobReport"].setJobStatus(
                    status=JobStatus.FAILED, minorStatus=f"Matcher did not return {param}"
                )
                return S_ERROR("Matcher Failed")

            if not matcherInfo[param]:
                self.jobs[jobID]["JobReport"].setJobStatus(
                    status=JobStatus.FAILED, minorStatus=f"Matcher returned null {param}"
                )
                return S_ERROR("Matcher Failed")

            self.log.verbose("Matcher returned", f"{param} = {matcherInfo[param]} ")
        return S_OK()

    #############################################################################
    def _submitJob(
        self,
        jobID,
        jobParams,
        resourceParams,
        optimizerParams,
        proxyChain,
        processors=1,
        wholeNode=False,
        maxNumberOfProcessors=0,
        mpTag=False,
    ):
        """Submit job to the Computing Element instance after creating a custom
        Job Wrapper with the available job parameters.
        """
        # Add the number of requested processors to the job environment
        if "ExecutionEnvironment" in jobParams:
            if isinstance(jobParams["ExecutionEnvironment"], str):
                jobParams["ExecutionEnvironment"] = jobParams["ExecutionEnvironment"].split(";")
        jobParams.setdefault("ExecutionEnvironment", []).append("DIRAC_JOB_PROCESSORS=%d" % processors)

        jobDesc = {
            "jobID": jobID,
            "jobParams": jobParams,
            "resourceParams": resourceParams,
            "optimizerParams": optimizerParams,
            "extraOptions": self.extraOptions,
            "defaultWrapperLocation": self.defaultWrapperLocation,
        }
        result = createJobWrapper(log=self.log, logLevel=self.logLevel, **jobDesc)
        if not result["OK"]:
            return result

        wrapperFile = result["Value"]["JobExecutablePath"]
        inputs = [result["Value"]["JobWrapperPath"], result["Value"]["JobWrapperConfigPath"]]
        self.jobs[jobID]["JobReport"].setJobStatus(minorStatus="Submitting To CE")

        self.log.info("Submitting JobWrapper", f"{os.path.basename(wrapperFile)} to {self.ceName}CE")

        # Pass proxy to the CE, writing it to a temporary file to ensure the DiracX token is included
        retVal = writeChainToTemporaryFile(proxyChain)
        if not retVal["OK"]:
            self.log.error("Invalid proxy", retVal["Message"])
            return S_ERROR("Failed to write proxy to temporary file")
        proxyLocation = Path(retVal["Value"])
        payloadProxy = proxyLocation.read_text()
        proxyLocation.unlink()

        try:
            result = self.computingElement.submitJob(
                wrapperFile,
                payloadProxy,
                numberOfProcessors=processors,
                maxNumberOfProcessors=maxNumberOfProcessors,
                wholeNode=wholeNode,
                mpTag=mpTag,
                jobDesc=jobDesc,
                log=self.log,
                logLevel=self.logLevel,
                inputs=inputs,
            )
        except Exception as unexpectedSubmitException:
            # This should almost never happen in theory
            self.log.exception("Exception occurred when submitting", f"JobID: {jobID}")
            taskID = 0
            # We create a S_ERROR from the exception to compute it as a normal error
            self.computingElement.taskResults[taskID] = S_ERROR(str(unexpectedSubmitException))
            self.jobs[jobID]["TaskID"] = taskID
            return S_OK()

        # Submission results are processed in _checkSubmittedJobs
        # If the submission is done synchronously, the result should be provided in result
        # We add it to the taskResults dictionary
        # taskID is always 0 because it means the JobAgent manages a single job per cycle
        if not self.computingElement.ceParameters.get("AsyncSubmission", False):
            taskID = 0
            self.computingElement.taskResults[taskID] = result
        # If the submission is done asynchronously,
        # the result of the submission is already in the computingElement.taskResults dict
        # In this case, result contains the CE-specific jobID (described as taskID here)
        else:
            taskID = result.get("Value")

        self.log.info("Job being submitted", f"(DIRAC JobID: {jobID}; Task ID: {taskID})")

        self.jobs[jobID]["TaskID"] = taskID
        time.sleep(self.jobSubmissionDelay)
        return S_OK()

    def _checkSubmittedJobs(self):
        """Get the status of the submitted jobs and/or the submission process itself."""
        # We expect the computingElement to have a taskResult dictionary.
        submissionErrors = []
        payloadErrors = []
        # Loop over the jobIDs submitted to the CE
        # Here we iterate over a copy of the keys because we are modifying the dictionary within the loop
        for jobID in list(self.jobs.keys()):
            taskID = self.jobs[jobID].get("TaskID")
            if taskID is None:
                # This generally means that there was an error before the submission
                # and the TaskID was not set and will never be.
                self.log.info("No taskID found for job", jobID)
                del self.jobs[jobID]
                continue
            if taskID not in self.computingElement.taskResults:
                continue

            result = self.computingElement.taskResults[taskID]

            # The submission process failed
            if not result["OK"]:
                self.log.error("Job submission failed", jobID)
                self.jobs[jobID]["JobReport"].setJobParameter(
                    par_name="ErrorMessage", par_value=f"{self.ceName} CE Submission Error", sendFlag=False
                )

                self.log.error("Error in DIRAC JobWrapper or inner CE execution:", result["Message"])
                submissionErrors.append(result["Message"])
                self._rescheduleFailedJob(jobID, result["Message"])
                self.hostFailureCount += 1

            # The payload failed (if result["Value"] is not 0)
            elif result["Value"]:
                # In order to avoid overriding perfectly valid states, the status is updated iff the job was running
                res = JobMonitoringClient().getJobsStatus(jobID)
                if not res["OK"]:
                    return res
                if res["Value"][int(jobID)]["Status"] == JobStatus.RUNNING:
                    self.jobs[jobID]["JobReport"].setJobStatus(
                        status=JobStatus.FAILED, minorStatus="Payload failed", sendFlag=False
                    )

                # Do not keep running and do not overwrite the Payload error
                message = f"Payload execution failed with error code {result['Value']}"
                payloadErrors.append(message)
                self.log.info(message)

            # The job has been treated, we can commit the JobReport
            res = self.jobs[jobID]["JobReport"].commit()
            if not res["OK"]:
                resFD = self.jobs[jobID]["JobReport"].generateForwardDISET()
                if not resFD["OK"]:
                    self.log.error("Error generating ForwardDISET operation", resFD["Message"])
                elif resFD["Value"]:
                    # Here we create the Request.
                    op = resFD["Value"]
                    request = Request()
                    requestName = f"jobAgent_{jobID}"
                    request.RequestName = requestName.replace('"', "")
                    request.JobID = jobID
                    request.SourceComponent = f"JobAgent_{jobID}"
                    request.addOperation(op)
                    # This might fail, but only a message would be printed.
                    self._sendFailoverRequest(request)

            # Remove taskID from computingElement.taskResults as it has been treated
            # Remove jobID from jobs as it has been treated
            del self.computingElement.taskResults[taskID]
            del self.jobs[jobID]

        return S_OK((submissionErrors, payloadErrors))

    #############################################################################
    def _getJDLParameters(self, jdl):
        """Returns a dictionary of JDL parameters."""
        try:
            parameters = {}
            #      print jdl
            if not re.search(r"\[", jdl):
                jdl = "[" + jdl + "]"
            classAdJob = ClassAd(jdl)
            paramsDict = classAdJob.contents
            for param, value in paramsDict.items():
                if value.strip().startswith("{"):
                    self.log.debug(f"Found list type parameter {param}")
                    rawValues = value.replace("{", "").replace("}", "").replace('"', "").split()
                    valueList = []
                    for val in rawValues:
                        if re.search(",$", val):
                            valueList.append(val[:-1])
                        else:
                            valueList.append(val)
                    parameters[param] = valueList
                else:
                    parameters[param] = value.replace('"', "").replace("{", '"{').replace("}", '}"')
                    self.log.debug(f"Found standard parameter {param}: {parameters[param]}")
            return S_OK(parameters)
        except Exception as x:
            self.log.exception(lException=x)
            return S_ERROR("Exception while extracting JDL parameters for job")

    def _extractValuesFromJobParams(self, params):
        """Extract values related to the job from the job parameter dictionary"""
        submissionDict = {}

        submissionDict["jobID"] = params.get("JobID")
        if not submissionDict["jobID"]:
            msg = "Job has not JobID defined in JDL parameters"
            self.log.warn(msg)
            return S_ERROR(msg)

        submissionDict["jobType"] = params.get("JobType", "Unknown")
        if submissionDict["jobType"] == "Unknown":
            self.log.warn("Job has no JobType defined in JDL parameters")

        if "CPUTime" not in params:
            self.log.warn("Job has no CPU requirement defined in JDL parameters")

        # Job requirements for determining the number of processors
        # the minimum number of processors requested
        submissionDict["processors"] = int(
            params.get("NumberOfProcessors", int(params.get("MinNumberOfProcessors", 1)))
        )
        # the maximum number of processors allowed to the payload
        submissionDict["maxNumberOfProcessors"] = int(params.get("MaxNumberOfProcessors", 0))
        # need or not the whole node for the job
        submissionDict["wholeNode"] = "WholeNode" in params
        submissionDict["mpTag"] = "MultiProcessor" in params.get("Tags", [])

        if self.extraOptions and "dirac-jobexec" in params.get("Executable", "").strip():
            params["Arguments"] = (params.get("Arguments", "") + " " + self.extraOptions).strip()
            params["ExtraOptions"] = self.extraOptions

        return S_OK(submissionDict)

    #############################################################################
    def _finish(self, message, stop=True):
        """Force the JobAgent to complete gracefully."""
        if stop:
            self.log.info("JobAgent will stop", f'with message "{message}", execution complete.')
            self.am_stopExecution()
            return S_ERROR(message)

        return S_OK(message)

    #############################################################################
    def _rescheduleFailedJob(self, jobID, message):
        """
        Set Job Status to "Rescheduled" and issue a reschedule command to the Job Manager
        """

        self.log.warn("Failure ==> rescheduling", f"(during {message})")

        # Setting a job parameter does not help since the job will be rescheduled,
        # instead set the status with the cause and then another status showing the
        # reschedule operation.
        self.jobs[jobID]["JobReport"].setJobStatus(
            status=JobStatus.RESCHEDULED, applicationStatus=message, sendFlag=True
        )

        self.log.info("Job will be rescheduled")
        result = JobManagerClient().rescheduleJob(jobID)
        if not result["OK"]:
            self.log.error("Failed to reschedule job", result["Message"])
            return S_ERROR("Problem Rescheduling Job")

        self.log.info("Job Rescheduled", jobID)
        return S_ERROR("Job Rescheduled")

    #############################################################################
    def _sendFailoverRequest(self, request):
        """Send failover reques per Job.
        This request would basically be a DISET request for setting the job status.

        If this fails, it only prints a message.

        :param Request request: Request() object
        :return: None
        """
        if len(request):
            self.log.info("Trying to send the failover request")
            # The request is ready, send it now
            isValid = RequestValidator().validate(request)
            if not isValid["OK"]:
                self.log.error("Failover request is not valid", isValid["Message"])
                self.log.error("Printing out the content of the request")
                reqToJSON = request.toJSON()
                if reqToJSON["OK"]:
                    print(str(reqToJSON["Value"]))
                else:
                    self.log.error("Something went wrong creating the JSON from request", reqToJSON["Message"])
            else:
                # Now trying to send the request
                requestClient = ReqClient()
                result = requestClient.putRequest(request)
                if not result["OK"]:
                    self.log.error("Failed to set failover request", result["Message"])

    def finalize(self):
        """Job Agent finalization method"""

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

        # Set the pilot status to Done
        gridCE = gConfig.getValue("/LocalSite/GridCE", "")
        queue = gConfig.getValue("/LocalSite/CEQueue", "")
        result = PilotManagerClient().setPilotStatus(
            str(self.pilotReference), PilotStatus.DONE, gridCE, "Report from JobAgent", self.siteName, queue
        )
        if not result["OK"]:
            self.log.warn("Issue setting the pilot status", result["Message"])

        return S_OK()
