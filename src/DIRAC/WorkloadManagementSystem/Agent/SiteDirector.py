"""  The Site Director is an agent performing pilot job submission to particular sites/Computing Elements.

.. literalinclude:: ../ConfigTemplate.cfg
  :start-after: ##BEGIN SiteDirector
  :end-before: ##END
  :dedent: 2
  :caption: SiteDirector options

"""
import datetime
import os
import socket
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed

import DIRAC
from DIRAC import S_ERROR, S_OK, gConfig
from DIRAC.AccountingSystem.Client.DataStoreClient import gDataStoreClient
from DIRAC.AccountingSystem.Client.Types.Pilot import Pilot as PilotAccounting
from DIRAC.AccountingSystem.Client.Types.PilotSubmission import (
    PilotSubmission as PilotSubmissionAccounting,
)
from DIRAC.ConfigurationSystem.Client.Helpers import CSGlobals, Registry
from DIRAC.ConfigurationSystem.Client.Helpers.Operations import Operations
from DIRAC.ConfigurationSystem.Client.Helpers.Resources import getCESiteMapping, getQueues
from DIRAC.Core.Base.AgentModule import AgentModule
from DIRAC.Core.Utilities.TimeUtilities import second, toEpochMilliSeconds
from DIRAC.FrameworkSystem.Client.ProxyManagerClient import gProxyManager
from DIRAC.FrameworkSystem.Client.TokenManagerClient import gTokenManager
from DIRAC.MonitoringSystem.Client.MonitoringReporter import MonitoringReporter
from DIRAC.Resources.Computing.ComputingElement import ComputingElement
from DIRAC.ResourceStatusSystem.Client.ResourceStatus import ResourceStatus
from DIRAC.ResourceStatusSystem.Client.SiteStatus import SiteStatus
from DIRAC.Resources.Computing.ComputingElement import ComputingElement
from DIRAC.WorkloadManagementSystem.Client import PilotStatus
from DIRAC.WorkloadManagementSystem.Client.PilotScopes import PILOT_SCOPES
from DIRAC.WorkloadManagementSystem.Client.ServerUtils import getPilotAgentsDB
from DIRAC.WorkloadManagementSystem.private.ConfigHelper import findGenericPilotCredentials
from DIRAC.WorkloadManagementSystem.Utilities.PilotWrapper import (
    _writePilotWrapperFile,
    getPilotFilesCompressedEncodedDict,
    pilotWrapperScript,
)
from DIRAC.WorkloadManagementSystem.Utilities.QueueUtilities import getQueuesResolved

MAX_PILOTS_TO_SUBMIT = 100

# Submission policies
AGGRESSIVE_FILLING = "AggressiveFilling"
WAITING_SUPPORTED_JOBS = "WaitingSupportedJobs"
SUBMISSION_POLICIES = [AGGRESSIVE_FILLING, WAITING_SUPPORTED_JOBS]


class SiteDirector(AgentModule):
    """SiteDirector class provides an implementation of a DIRAC agent.

    Used for submitting pilots to Computing Elements.
    """

    def __init__(self, *args, **kwargs):
        """c'tor"""
        super().__init__(*args, **kwargs)

        self.queueDict = {}
        # self.queueCECache aims at saving CEs information over the cycles to avoid to create the exact same CEs each cycle
        self.queueCECache = {}
        self.failedQueues = defaultdict(int)
        self.maxPilotsToSubmit = MAX_PILOTS_TO_SUBMIT

        self.vo = ""
        self.pilotDN = ""
        self.pilotGroup = ""

        self.sendAccounting = True
        self.sendSubmissionAccounting = True
        self.sendSubmissionMonitoring = False

        self.siteClient = None
        self.rssClient = None
        self.pilotAgentsDB = None
        self.rssFlag = None

        # self.failedQueueCycleFactor is the number of cycles a queue has to wait before getting pilots again
        self.failedQueueCycleFactor = 10
        self.submissionPolicyName = WAITING_SUPPORTED_JOBS
        self.submissionPolicy = None

        self.workingDirectory = None
        self.maxQueueLength = 86400 * 3

    def initialize(self):
        """Initial settings"""

        # The SiteDirector is for a particular user community
        self.vo = self.am_getOption("VO", self.am_getOption("Community", ""))
        if not self.vo:
            self.vo = CSGlobals.getVO()
        if not self.vo:
            return S_ERROR("Need a VO")

        # Get the clients
        self.siteClient = SiteStatus()
        self.rssClient = ResourceStatus()
        self.pilotAgentsDB = getPilotAgentsDB()

        return S_OK()

    #####################################################################################

    def beginExecution(self):
        """This is run at every cycle, as first thing.

        1. Check the pilots credentials.
        2. Get some flags and options used later
        3. Get the site description dictionary
        4. Get what to send in pilot wrapper
        """
        # Which credentials to use?
        # Are they specific to the SD? (if not, get the generic ones)
        self.pilotDN = self.am_getOption("PilotDN", self.pilotDN)
        self.pilotGroup = self.am_getOption("PilotGroup", self.pilotGroup)
        result = findGenericPilotCredentials(vo=self.vo, pilotDN=self.pilotDN, pilotGroup=self.pilotGroup)
        if not result["OK"]:
            return result
        self.pilotDN, self.pilotGroup = result["Value"]

        # Parameters
        self.workingDirectory = self.am_getOption("WorkDirectory", self.workingDirectory)
        self.maxQueueLength = self.am_getOption("MaxQueueLength", self.maxQueueLength)
        self.maxPilotsToSubmit = self.am_getOption("MaxPilotsToSubmit", self.maxPilotsToSubmit)
        self.failedQueueCycleFactor = self.am_getOption("FailedQueueCycleFactor", self.failedQueueCycleFactor)

        # Load submission policy
        self.submissionPolicyName = self.am_getOption("SubmissionPolicy", self.submissionPolicyName)
        result = self._loadSubmissionPolicy()
        if not result:
            return result
        self.submissionPolicy = result["Value"]()

        # Flags
        self.sendAccounting = self.am_getOption("SendPilotAccounting", self.sendAccounting)
        self.rssFlag = self.rssClient.rssFlag

        # Check whether to send to Monitoring or Accounting or both
        monitoringOption = Operations().getMonitoringBackends(monitoringType="PilotSubmissionMonitoring")
        if "Monitoring" in monitoringOption:
            self.sendSubmissionMonitoring = True
        if "Accounting" in monitoringOption:
            self.sendSubmissionAccounting = True

        # Get the site description dictionary
        siteNames = self.am_getOption("Site", [])
        ceTypes = self.am_getOption("CETypes", [])
        ces = self.am_getOption("CEs", [])
        tags = self.am_getOption("Tags", [])

        # Display options used
        self.log.always("VO:", self.vo)
        self.log.always("Sites:", siteNames)
        self.log.always("CETypes:", ceTypes)
        self.log.always("CEs:", ces)
        self.log.always("PilotDN:", self.pilotDN)
        self.log.always("PilotGroup:", self.pilotGroup)
        if self.sendAccounting:
            self.log.always("Pilot accounting sending requested")
        if self.sendSubmissionAccounting:
            self.log.always("Pilot submission accounting sending requested")
        if self.sendSubmissionMonitoring:
            self.log.always("Pilot submission monitoring sending requested")

        self.log.always("MaxPilotsToSubmit:", self.maxPilotsToSubmit)

        # Build the dictionary of queues that are going to be used: self.queueDict
        result = self._buildQueueDict(siteNames, ceTypes, ces, tags)
        if not result:
            return result

        # Stop the execution if there is no usable queue
        if not self.queueDict:
            self.log.error("No usable queue, exiting the cycle")
            return S_ERROR("No usable queue, exiting the cycle")

        self.log.always("Agent will serve queues:")
        for queue in self.queueDict:
            self.log.always(
                f"Site: {self.queueDict[queue]['Site']}, CE: {self.queueDict[queue]['CEName']}, Queue: {queue}"
            )

        return S_OK()

    def _loadSubmissionPolicy(self):
        """Load a submission policy"""
        objectLoader = ObjectLoader()
        result = objectLoader.loadObject(
            f"WorkloadManagementSystem.Agent.SiteDirector.{self.submissionPolicyName}", self.submissionPolicyName
        )
        if not result["OK"]:
            self.log.error(f"Failed to load submission policy: {result['Message']}")
            return result
        return S_OK(result["Value"])

    def _buildQueueDict(self, siteNames, ces, ceTypes, tags):
        """Build the queueDict dictionary containing information about the queues that will be provisioned"""
        # Get details about the resources
        result = getQueues(community=self.vo, siteList=siteNames, ceList=ces, ceTypeList=ceTypes, tags=tags)
        if not result["OK"]:
            return result

        # Set up the queue dictionary
        result = getQueuesResolved(
            siteDict=result["Value"],
            queueCECache=self.queueCECache,
            instantiateCEs=True,
        )
        if not result["OK"]:
            return result
        self.queueDict = result["Value"]

        # Get list of usable sites within this cycle
        result = self.siteClient.getUsableSites(siteNames)
        if not result["OK"]:
            return result
        siteMaskList = result.get("Value", [])

        # Get list of usable CEs
        ceMaskList = []
        if self.rssFlag:
            ceNamesList = [queue["CEName"] for queue in self.queueDict.values()]
            result = self.rssClient.getElementStatus(ceNamesList, "ComputingElement", vO=self.vo)
            if not result["OK"]:
                self.log.error("Can not get the status of computing elements: ", result["Message"])
                return result
            # Try to get CEs which have been probed and those unprobed (vO='all').
            ceMaskList = [
                ceName for ceName in result["Value"] if result["Value"][ceName]["all"] in ("Active", "Degraded")
            ]

        # Filter the unusable queues
        for queueName in list(self.queueDict.keys()):
            site = self.queueDict[queueName]["Site"]
            ce = self.queueDict[queueName]["CEName"]

            # Check the status of the Site
            if site in siteMaskList:
                continue

            # Check the status of the CE (only for RSS=Active)
            if not self.rssFlag or (self.rssFlag and ce in ceMaskList):
                continue

            self.log.warn("Queue not considered because not usable:", queueName)
            self.queueDict.pop(queueName)

        return S_OK()

    #####################################################################################

    def execute(self):
        """Main execution method (what is called at each agent cycle).

        It basically just submits pilots and gets their status
        """
        result = self.submitPilots()
        if not result["OK"]:
            self.log.error("Errors in the pilot submission:", result["Message"])
            return result

        result = self.monitorPilots()
        if not result["OK"]:
            self.log.error("Errors in pilot monitoring:", result["Message"])
            return result

        return S_OK()

    #####################################################################################

    def submitPilots(self):
        """Go through defined computing elements and submit pilots if necessary and possible"""
        # Getting the status of pilots in a queue implies the use of remote CEs and may lead to network latency
        # Threads aim at overcoming such issues and thus 1 thread per queue is created to submit pilots
        self.log.verbose("Submission: Queues treated are", ",".join(self.queueDict))

        errors = []
        with ThreadPoolExecutor(max_workers=len(self.queueDict)) as executor:
            futures = []
            for queue in self.queueDict:
                futures.append(executor.submit(self._submitPilotsPerQueue, queue))

            for future in as_completed(futures):
                result = future.result()
                if not result["OK"]:
                    errors.append(result["Message"])

        if errors:
            self.log.error("The following errors occurred during the pilot submission operation", "\n".join(errors))
            return S_ERROR("Pilot submission: errors occurred")

        return S_OK()

    def _submitPilotsPerQueue(self, queueName):
        """Submit pilots within a given computing elements

        :return: S_OK/S_ERROR
        """
        queueDictionary = self.queueDict[queueName]

        # Are we allowed to submit pilots to this specific queue?
        failedCount = self.failedQueues[queueName] % self.failedQueueCycleFactor
        if failedCount != 0:
            self.log.warn(
                "Queue failed recently ==> number of cycles skipped",
                f"{queueName} ==> {self.failedQueueCycleFactor - failedCount}",
            )
            self.failedQueues[queueName] += 1
            return S_OK()

        # Adjust queueCPUTime: needed to generate the proxy
        if "CPUTime" not in queueDictionary["ParametersDict"]:
            self.log.error("CPU time limit is not specified, skipping", f"queue {queueName}")
            return S_ERROR(f"CPU time limit is not specified, skipping queue {queueName}")

        queueCPUTime = int(queueDictionary["ParametersDict"]["CPUTime"])
        if queueCPUTime > self.maxQueueLength:
            queueCPUTime = self.maxQueueLength

        # Get CE instance
        ce = self.queueDict[queueName]["CE"]

        # Get the number of available slots on the target site/queue
        totalSlots = self._getQueueSlots(queueName)
        if totalSlots <= 0:
            self.log.verbose(f"{queueName}: No slot available")
            return S_ERROR(f"{queueName}: No slot available")
        self.log.info(f"{queueName}: to submit={totalSlots}")

        # Apply the submission policy
        totalSlots = self.submissionPolicy.apply(totalSlots)

        # Limit the number of pilots to submit to self.maxPilotsToSubmit
        pilotsToSubmit = min(self.maxPilotsToSubmit, totalSlots)

        # Set credentials
        cpuTime = queueCPUTime + 86400
        result = self._setCredentials(ce, cpuTime)
        if not result["OK"]:
            self.log.error("Failed to set credentials:", result["Message"])
            return result

        # Now really submitting
        result = self._submitPilotsToQueue(pilotsToSubmit, ce, queueName)
        if not result["OK"]:
            self.log.info("Failed pilot submission", f"Queue: {queueName}")
            return result
        pilotList, stampDict = result["Value"]

        # updating the pilotAgentsDB... done by default but maybe not strictly necessary
        result = self._addPilotReference(queueName, pilotList, stampDict)
        if not result["OK"]:
            return result

        # Summary after the cycle over queues
        self.log.info("Total number of pilots submitted in this cycle", f"{len(pilotList)} to {queueName}")
        return S_OK()

    def _getQueueSlots(self, queue):
        """Get the number of available slots in the queue"""
        ce = self.queueDict[queue]["CE"]
        ceName = self.queueDict[queue]["CEName"]
        queueName = self.queueDict[queue]["QueueName"]

        # First, try to get available slots from the CE
        result = ce.available()
        if result["OK"]:
            ceInfoDict = result["CEInfoDict"]
            self.log.info(
                "CE queue report",
                f"({ceName}_{queueName}): Wait={ceInfoDict['WaitingJobs']}, Run={ceInfoDict['RunningJobs']}, Max={ceInfoDict['MaxTotalJobs']}",
            )
            return result["Value"]

        # If we cannot get available slots from the CE, then we get them from the pilotAgentsDB
        maxWaitingJobs = int(self.queueDict[queue]["ParametersDict"].get("MaxWaitingJobs", 10))
        maxTotalJobs = int(self.queueDict[queue]["ParametersDict"].get("MaxTotalJobs", 10))

        # Get the number of transient pilots
        result = self.pilotAgentsDB.countPilots(
            {"DestinationSite": ceName, "Queue": queueName, "Status": PilotStatus.PILOT_TRANSIENT_STATES}
        )
        if not result["OK"]:
            self.log.warn("Failed to check PilotAgentsDB", f"for queue {queue}: \n{result['Message']}")
            self.failedQueues[queue] += 1
            return 0
        totalJobs = result["Value"]

        # Get the number of waiting pilots
        result = self.pilotAgentsDB.countPilots(
            {"DestinationSite": ceName, "Queue": queueName, "Status": PilotStatus.PILOT_WAITING_STATES}
        )
        if not result["OK"]:
            self.log.warn("Failed to check PilotAgentsDB", f"for queue {queue}: \n{result['Message']}")
            self.failedQueues[queue] += 1
            return 0
        waitingJobs = result["Value"]

        runningJobs = totalJobs - waitingJobs
        self.log.info(
            "PilotAgentsDB report",
            f"({ceName}_{queueName}): Wait={waitingJobs}, Run={runningJobs}, Max={maxTotalJobs}",
        )

        totalSlots = min((maxTotalJobs - totalJobs), (maxWaitingJobs - waitingJobs))
        return totalSlots

    def _submitPilotsToQueue(self, pilotsToSubmit, ce, queue):
        """Method that really submits the pilots to the ComputingElements' queue

        :param pilotsToSubmit: number of pilots to submit.
        :type pilotsToSubmit: int
        :param ce: computing element object to where we submit
        :type ce: ComputingElement
        :param str queue: queue where to submit

        :return: S_OK/S_ERROR.
                 If S_OK, returns tuple with (pilotList, stampDict)
                 where
                   pilotsList is the list of pilots submitted
                   stampDict is a dict of timestamps of pilots submission
        :rtype: dict
        """
        self.log.info("Going to submit pilots", f"(a maximum of {pilotsToSubmit} pilots to {queue} queue)")

        # Get parameters to generate the pilot executable
        bundleProxy = self.queueDict[queue].get("BundleProxy", False)
        proxy = None
        if bundleProxy:
            proxy = ce.proxy
        jobExecDir = self.queueDict[queue]["ParametersDict"].get("JobExecDir", "")
        envVariables = self.queueDict[queue]["ParametersDict"].get("EnvironmentVariables", None)

        # Generate the executable
        executable = self._getExecutable(queue, proxy=proxy, jobExecDir=jobExecDir, envVariables=envVariables)

        # Submit the job
        submitResult = ce.submitJob(executable, "", pilotsToSubmit)
        # In case the CE does not need the executable after the submission, we delete it
        # Else, we keep it, the CE will delete it after the end of the pilot execution
        if submitResult.get("ExecutableToKeep") != executable:
            os.unlink(executable)

        siteName = self.queueDict[queue]["Site"]
        ceName = self.queueDict[queue]["CEName"]
        queueName = self.queueDict[queue]["QueueName"]
        if not submitResult["OK"]:
            self.log.error("Failed submission to queue", f"Queue {queue}:\n{submitResult['Message']}")

            if self.sendSubmissionAccounting:
                result = self._sendPilotSubmissionAccounting(
                    siteName,
                    ceName,
                    queueName,
                    pilotsToSubmit,
                    0,
                    "Failed",
                )
                if not result["OK"]:
                    self.log.error("Failure submitting Accounting report", result["Message"])

            if self.sendSubmissionMonitoring:
                result = self._sendPilotSubmissionMonitoring(
                    siteName,
                    ceName,
                    queueName,
                    pilotsToSubmit,
                    0,
                    "Failed",
                )
                if not result["OK"]:
                    self.log.error("Failure submitting Monitoring report", result["Message"])

            self.failedQueues[queue] += 1
            return submitResult

        # Add pilots to the PilotAgentsDB
        pilotList = submitResult["Value"]

        self.log.info(
            f"Submitted {len(pilotList)} pilots to {self.queueDict[queue]['QueueName']}@{self.queueDict[queue]['CEName']}"
        )
        stampDict = submitResult.get("PilotStampDict", {})
        if self.sendSubmissionAccounting:
            result = self._sendPilotSubmissionAccounting(
                siteName,
                ceName,
                queueName,
                len(pilotList),
                len(pilotList),
                "Succeeded",
            )
            if not result["OK"]:
                self.log.error("Failure submitting Accounting report", result["Message"])

        if self.sendSubmissionMonitoring:
            result = self._sendPilotSubmissionMonitoring(
                siteName,
                ceName,
                queueName,
                len(pilotList),
                len(pilotList),
                "Succeeded",
            )
            if not result["OK"]:
                self.log.error("Failure submitting Monitoring report", result["Message"])

        return S_OK((pilotList, stampDict))

    def _addPilotReference(self, queue, pilotList, stampDict):
        """Add pilotReference to pilotAgentsDB

        :param str queue: the queue name
        :param pilotList: list of pilots
        :type pilotList: list
        :param stampDict: dictionary of pilots timestamps
        :type stampDict: dict

        :return: None
        """
        result = self.pilotAgentsDB.addPilotReference(
            pilotList,
            self.pilotGroup,
            self.queueDict[queue]["CEType"],
            stampDict,
        )
        if not result["OK"]:
            self.log.error("Failed add pilots to the PilotAgentsDB", result["Message"])
            return result

        for pilot in pilotList:
            result = self.pilotAgentsDB.setPilotStatus(
                pilot,
                PilotStatus.SUBMITTED,
                self.queueDict[queue]["CEName"],
                "Successfully submitted by the SiteDirector",
                self.queueDict[queue]["Site"],
                self.queueDict[queue]["QueueName"],
            )
            if not result["OK"]:
                self.log.error("Failed to set pilot status", result["Message"])
                return result
        return S_OK()

    def _getExecutable(self, queue, proxy=None, jobExecDir="", envVariables=None, **kwargs):
        """Prepare the full executable for queue

        :param str queue: queue name
        :param bool proxy: flag that say if to bundle or not the proxy
        :param str jobExecDir: pilot execution dir (normally an empty string)

        :returns: a string the options for the pilot
        :rtype: str
        """

        pilotOptions = self._getPilotOptions(queue, **kwargs)
        if not pilotOptions:
            self.log.warn("Pilots will be submitted without additional options")
            pilotOptions = []
        pilotOptions = " ".join(pilotOptions)
        self.log.verbose(f"pilotOptions: {pilotOptions}")

        # if a global workingDirectory is defined for the CEType (like HTCondor)
        # use it (otherwise the __cleanup done by HTCondor will be in the wrong folder !)
        # Note that this means that if you run multiple HTCondorCE
        # in your machine, the executable files will be in the same place
        # but it does not matter since they are very temporary

        ce = self.queueCECache[queue]["CE"]
        workingDirectory = getattr(ce, "workingDirectory", self.workingDirectory)

        executable = self._writePilotScript(
            workingDirectory=workingDirectory,
            pilotOptions=pilotOptions,
            proxy=proxy,
            pilotExecDir=jobExecDir,
            envVariables=envVariables,
        )
        return executable

    def _getPilotOptions(self, queue, **kwargs):
        """Prepare pilot options

        :param str queue: queue name

        :returns: pilotOptions is a list of strings, each one is an option to the dirac-pilot script invocation
        :rtype: list
        """
        queueDict = self.queueDict[queue]["ParametersDict"]
        pilotOptions = []

        setup = gConfig.getValue("/DIRAC/Setup", "unknown")
        if setup == "unknown":
            self.log.error("Setup is not defined in the configuration")
            return [None, None]
        pilotOptions.append(f"-S {setup}")
        opsHelper = Operations(group=self.pilotGroup, setup=setup)

        # Installation defined?
        installationName = opsHelper.getValue("Pilot/Installation", "")
        if installationName:
            pilotOptions.append(f"-V {installationName}")

        # Project defined?
        projectName = opsHelper.getValue("Pilot/Project", "")
        if projectName:
            pilotOptions.append(f"-l {projectName}")
        else:
            self.log.info("DIRAC project will be installed by pilots")

        # Preinstalled environment defined ?
        preinstalledEnv = opsHelper.getValue("Pilot/PreinstalledEnv", "")
        preinstalledEnvPrefix = opsHelper.getValue("Pilot/PreinstalledEnvPrefix", "")
        if preinstalledEnv:
            pilotOptions.append(f"--preinstalledEnv={preinstalledEnv}")
        elif preinstalledEnvPrefix:
            pilotOptions.append(f"--preinstalledEnvPrefix={preinstalledEnvPrefix}")

        pilotOptions.append("--pythonVersion=3")

        # DIRAC Extensions to be used in pilots
        pilotExtensionsList = opsHelper.getValue("Pilot/Extensions", [])
        extensionsList = []
        if pilotExtensionsList and pilotExtensionsList[0] != "None":
            extensionsList = pilotExtensionsList
        else:
            extensionsList = [ext for ext in CSGlobals.getCSExtensions() if "Web" not in ext]
        if extensionsList:
            pilotOptions.append(f"-e {','.join(extensionsList)}")

        # CEName
        pilotOptions.append(f"-N {self.queueDict[queue]['CEName']}")
        # Queue
        pilotOptions.append(f"-Q {self.queueDict[queue]['QueueName']}")
        # SiteName
        pilotOptions.append(f"-n {queueDict['Site']}")
        # VO
        pilotOptions.append(f"--wnVO={self.vo}")

        # Generic Options
        if "GenericOptions" in queueDict:
            for genericOption in queueDict["GenericOptions"].split(","):
                pilotOptions.append(f"-o {genericOption.strip()}")

        if "SharedArea" in queueDict:
            pilotOptions.append(f"-o '/LocalSite/SharedArea={queueDict['SharedArea']}'")

        if "ExtraPilotOptions" in queueDict:
            for extraPilotOption in queueDict["ExtraPilotOptions"].split(","):
                pilotOptions.append(extraPilotOption.strip())

        if "Modules" in queueDict:
            pilotOptions.append(f"--modules={queueDict['Modules']}")

        if "PipInstallOptions" in queueDict:
            pilotOptions.append(f"--pipInstallOptions={queueDict['PipInstallOptions']}")

        return pilotOptions

    def _writePilotScript(self, workingDirectory, pilotOptions, proxy=None, pilotExecDir="", envVariables=None):
        """Bundle together and write out the pilot executable script, admix the proxy if given

        :param str workingDirectory: pilot wrapper working directory
        :param str pilotOptions: options with which to start the pilot
        :param str proxy: proxy file we are going to bundle
        :param str pilotExecDir: pilot executing directory

        :returns: file name of the pilot wrapper created
        :rtype: str
        """

        try:
            pilotFilesCompressedEncodedDict = getPilotFilesCompressedEncodedDict([], proxy)
        except Exception as be:
            self.log.exception("Exception during pilot modules files compression", lException=be)

        location = Operations().getValue("Pilot/pilotFileServer", "")
        localPilot = pilotWrapperScript(
            pilotFilesCompressedEncodedDict=pilotFilesCompressedEncodedDict,
            pilotOptions=pilotOptions,
            pilotExecDir=pilotExecDir,
            envVariables=envVariables,
            location=location,
        )

        return _writePilotWrapperFile(workingDirectory=workingDirectory, localPilot=localPilot)

    #####################################################################################

    def monitorPilots(self):
        """Update status of pilots in transient and final states"""
        self.log.verbose("Monitoring: Queues treated are", ",".join(self.queueDict))

        # Getting the status of pilots in a queue implies the use of remote CEs and may lead to network latency
        # Threads aim at overcoming such issues and thus 1 thread per queue is created to
        # update the status of pilots in transient states
        errors = []
        with ThreadPoolExecutor(max_workers=len(self.queueDict)) as executor:
            futures = []
            for queue in self.queueDict:
                futures.append(executor.submit(self._monitorPilotsPerQueue, queue))

            for future in as_completed(futures):
                result = future.result()
                if not result["OK"]:
                    errors.append(result["Message"])

        if errors:
            self.log.error("The following errors occurred during the pilot monitoring operation", "\n".join(errors))
            return S_ERROR("Pilot monitoring: errors occurred")

        return S_OK()

    def _monitorPilotsPerQueue(self, queueName):
        """Update status of pilots in transient state for a given queue

        :param queue: queue name
        :param proxy: proxy to check the pilot status and renewals
        """
        ce = self.queueDict[queueName]["CE"]
        ceName = self.queueDict[queueName]["CEName"]
        queueName = self.queueDict[queueName]["QueueName"]
        ceType = self.queueDict[queueName]["CEType"]
        siteName = self.queueDict[queueName]["Site"]

        # Select pilots in a transient states
        result = self.pilotAgentsDB.selectPilots(
            {
                "DestinationSite": ceName,
                "Queue": queueName,
                "GridType": ceType,
                "GridSite": siteName,
                "Status": PilotStatus.PILOT_TRANSIENT_STATES,
                "OwnerGroup": self.pilotGroup,
            }
        )
        if not result["OK"]:
            self.log.error("Failed to select pilots", f": {result['Message']}")
            return result
        pilotRefs = result["Value"]
        if not pilotRefs:
            return S_OK()

        # Get their information
        result = self.pilotAgentsDB.getPilotInfo(pilotRefs)
        if not result["OK"]:
            self.log.error("Failed to get pilots info from DB", result["Message"])
            return result
        pilotDict = result["Value"]

        # The proxy is used for checking the pilot status and renewals
        # We really need at least a few hours otherwise the renewed
        # proxy may expire before we check again...
        result = self._setCredentials(ce, 3 * 3600)
        if not result["OK"]:
            self.log.error("Failed to set credentials:", result["Message"])
            return result

        # Get an update of the pilot by interrogating the CEs
        result = ce.getJobStatus(pilotRefs)
        if not result["OK"]:
            self.log.error("Failed to get pilots status from CE", f"{ceName}: {result['Message']}")
            return result
        pilotCEDict = result["Value"]

        # Update pilot status in DB
        abortedPilots = self._updatePilotStatus(pilotRefs, pilotDict, pilotCEDict)
        # If something wrong in the queue, make a pause for the job submission
        if abortedPilots:
            self.failedQueues[queueName] += 1

        # FIXME: seems like it is only used by the CloudCE? Couldn't it be called from CloudCE.getJobStatus()?
        if callable(getattr(ce, "cleanupPilots", None)):
            ce.cleanupPilots()

        # Check if the accounting is to be sent
        if self.sendAccounting:
            result = self.pilotAgentsDB.selectPilots(
                {
                    "DestinationSite": ceName,
                    "Queue": queueName,
                    "GridType": ceType,
                    "GridSite": siteName,
                    "AccountingSent": "False",
                    "Status": PilotStatus.PILOT_FINAL_STATES,
                }
            )
            if not result["OK"]:
                self.log.error("Failed to select pilots", result["Message"])
                return result

            pilotRefs = result["Value"]
            if not pilotRefs:
                return S_OK()

            result = self.pilotAgentsDB.getPilotInfo(pilotRefs)
            if not result["OK"]:
                self.log.error("Failed to get pilots info from DB", result["Message"])
                return result

            pilotDict = result["Value"]
            result = self._sendPilotAccounting(pilotDict)
            if not result["OK"]:
                self.log.error("Failed to send pilot agent accounting")
                return result

        return S_OK()

    def _updatePilotStatus(self, pilotRefs, pilotDict, pilotCEDict):
        """Really updates the pilots status

        :return: number of aborted pilots, flag for getting the pilot output
        """
        abortedPilots = 0

        for pRef in pilotRefs:
            newStatus = ""
            oldStatus = pilotDict[pRef]["Status"]
            lastUpdateTime = pilotDict[pRef]["LastUpdateTime"]
            sinceLastUpdate = datetime.datetime.utcnow() - lastUpdateTime

            ceStatus = pilotCEDict.get(pRef, oldStatus)

            if oldStatus == ceStatus and ceStatus != PilotStatus.UNKNOWN:
                # Normal status did not change, continue
                continue
            if ceStatus == oldStatus == PilotStatus.UNKNOWN:
                if sinceLastUpdate < 3600 * second:
                    # Allow 1 hour of Unknown status assuming temporary problems on the CE
                    continue
                newStatus = PilotStatus.ABORTED
            elif ceStatus == PilotStatus.UNKNOWN and oldStatus not in PilotStatus.PILOT_FINAL_STATES:
                # Possible problems on the CE, let's keep the Unknown status for a while
                newStatus = PilotStatus.UNKNOWN
            elif ceStatus != PilotStatus.UNKNOWN:
                # Update the pilot status to the new value
                newStatus = ceStatus

            if newStatus:
                self.log.info("Updating status", f"to {newStatus} for pilot {pRef}")
                result = self.pilotAgentsDB.setPilotStatus(pRef, newStatus, "", "Updated by SiteDirector")
                if not result["OK"]:
                    self.log.error(result["Message"])
                if newStatus == "Aborted":
                    abortedPilots += 1

        return abortedPilots

    #####################################################################################

    def __getPilotToken(self, audience: str, scope: list[str] = None):
        """Get the token corresponding to the pilot user identity

        :param audience: Token audience, targeting a single CE
        :param scope: list of permissions needed to interact with a CE
        :return: S_OK/S_ERROR, Token object as Value
        """
        if not audience:
            return S_ERROR("Audience is not defined")

        if not scope:
            scope = PILOT_SCOPES

        return gTokenManager.getToken(userGroup=self.pilotGroup, requiredTimeLeft=600, scope=scope, audience=audience)

    def __supportToken(self, ce: ComputingElement) -> bool:
        """Check whether the SiteDirector is able to submit pilots with tokens.

        * the CE is able to receive any token. Validation: Tag = Token should be included in the CE parameters.
        * the CE is able to receive VO-specifc tokens. Validation: Tag = Token:<VO> should be included in the CE parameters.
        """
        return "Token" in ce.ceParameters.get("Tag", []) or f"Token:{self.vo}" in ce.ceParameters.get("Tag", [])

    def _setCredentials(self, ce: ComputingElement, proxyMinimumRequiredValidity: int):
        """

        :param ce: ComputingElement instance
        :param proxyMinimumRequiredValidity: number of seconds needed to perform an operation with the proxy
        :param tokenMinimumRequiredValidity: number of seconds needed to perform an operation with the token
        """
        getNewProxy = False

        # If the CE does not already embed a proxy, we need one
        if not ce.proxy:
            getNewProxy = True

        # If the CE embeds a proxy that is too short to perform a given operation, we need a new one
        if ce.proxy:
            result = ce.proxy.getRemainingSecs()
            if not result["OK"]:
                return result

            if result["Value"] < proxyMinimumRequiredValidity:
                getNewProxy = True

        # Generate a new proxy if needed
        if getNewProxy:
            self.log.verbose(
                "Getting pilot proxy", f"for {self.pilotDN}/{self.pilotGroup} {proxyMinimumRequiredValidity} long"
            )
            result = gProxyManager.getPilotProxyFromDIRACGroup(
                self.pilotDN, self.pilotGroup, proxyMinimumRequiredValidity
            )
            if not result["OK"]:
                return result
            ce.setProxy(result["Value"])

        # Get valid token if needed
        if self.__supportToken(ce):
            result = self.__getPilotToken(audience=ce.audienceName)
            if not result["OK"]:
                self.log.error("Failed to get token", f"{ce.ceName}: {result['Message']}")
                return
            ce.setToken(result["Value"])

        return S_OK()

    #####################################################################################

    def _sendPilotAccounting(self, pilotDict):
        """Send pilot accounting record"""
        for pRef in pilotDict:
            self.log.verbose("Preparing accounting record", f"for pilot {pRef}")
            pA = PilotAccounting()
            pA.setEndTime(pilotDict[pRef]["LastUpdateTime"])
            pA.setStartTime(pilotDict[pRef]["SubmissionTime"])
            retVal = Registry.getUsernameForDN(self.pilotDN)
            if not retVal["OK"]:
                username = "unknown"
                self.log.error("Can't determine username for dn", pilotDict[pRef]["OwnerDN"])
            else:
                username = retVal["Value"]
            pA.setValueByKey("User", username)
            pA.setValueByKey("UserGroup", pilotDict[pRef]["OwnerGroup"])
            result = getCESiteMapping(pilotDict[pRef]["DestinationSite"])
            if result["OK"] and result["Value"]:
                pA.setValueByKey("Site", result["Value"][pilotDict[pRef]["DestinationSite"]].strip())
            else:
                pA.setValueByKey("Site", "Unknown")
            pA.setValueByKey("GridCE", pilotDict[pRef]["DestinationSite"])
            pA.setValueByKey("GridMiddleware", pilotDict[pRef]["GridType"])
            pA.setValueByKey("GridResourceBroker", "DIRAC")
            pA.setValueByKey("GridStatus", pilotDict[pRef]["Status"])
            if "Jobs" not in pilotDict[pRef]:
                pA.setValueByKey("Jobs", 0)
            else:
                pA.setValueByKey("Jobs", len(pilotDict[pRef]["Jobs"]))
            self.log.verbose("Adding accounting record", f"for pilot {pilotDict[pRef]['PilotID']}")
            retVal = gDataStoreClient.addRegister(pA)
            if not retVal["OK"]:
                self.log.error("Failed to send accounting info for pilot ", pRef)
            else:
                # Set up AccountingSent flag
                result = self.pilotAgentsDB.setAccountingFlag(pRef)
                if not result["OK"]:
                    self.log.error("Failed to set accounting flag for pilot ", pRef)

        self.log.info("Committing accounting records", f"for {len(pilotDict)} pilots")
        result = gDataStoreClient.commit()
        if result["OK"]:
            for pRef in pilotDict:
                self.log.verbose("Setting AccountingSent flag", f"for pilot {pRef}")
                result = self.pilotAgentsDB.setAccountingFlag(pRef)
                if not result["OK"]:
                    self.log.error("Failed to set accounting flag for pilot ", pRef)
        else:
            return result

        return S_OK()

    def _sendPilotSubmissionAccounting(self, siteName, ceName, queueName, numTotal, numSucceeded, status):
        """Send pilot submission accounting record

        :param str siteName:     Site name
        :param str ceName:       CE name
        :param str queueName:    queue Name
        :param int numTotal:     Total number of submission
        :param int numSucceeded: Total number of submission succeeded
        :param str status:       'Succeeded' or 'Failed'

        :returns: S_OK / S_ERROR
        """

        pA = PilotSubmissionAccounting()
        pA.setStartTime(datetime.datetime.utcnow())
        pA.setEndTime(datetime.datetime.utcnow())
        pA.setValueByKey("HostName", DIRAC.siteName())
        pA.setValueByKey("SiteDirector", self.am_getModuleParam("agentName"))
        pA.setValueByKey("Site", siteName)
        pA.setValueByKey("CE", ceName)
        pA.setValueByKey("Queue", ceName + ":" + queueName)
        pA.setValueByKey("Status", status)
        pA.setValueByKey("NumTotal", numTotal)
        pA.setValueByKey("NumSucceeded", numSucceeded)
        result = gDataStoreClient.addRegister(pA)

        if not result["OK"]:
            self.log.warn("Error in add Register:" + result["Message"])
            return result

        self.log.verbose("Committing pilot submission to accounting")
        result = gDataStoreClient.delayedCommit()
        if not result["OK"]:
            self.log.error("Could not commit pilot submission to accounting", result["Message"])
            return result
        return S_OK()

    def _sendPilotSubmissionMonitoring(self, siteName, ceName, queueName, numTotal, numSucceeded, status):
        """Sends pilot submission records to monitoring

        :param str siteName:     Site name
        :param str ceName:       CE name
        :param str queueName:    queue Name
        :param int numTotal:     Total number of submission
        :param int numSucceeded: Total number of submission succeeded
        :param str status:       'Succeeded' or 'Failed'

        :returns: S_OK / S_ERROR
        """

        pilotMonitoringReporter = MonitoringReporter(monitoringType="PilotSubmissionMonitoring")

        pilotMonitoringData = {
            "HostName": DIRAC.siteName(),
            "SiteDirector": self.am_getModuleParam("agentName"),
            "Site": siteName,
            "CE": ceName,
            "Queue": ceName + ":" + queueName,
            "Status": status,
            "NumTotal": numTotal,
            "NumSucceeded": numSucceeded,
            "timestamp": int(toEpochMilliSeconds(datetime.datetime.utcnow())),
        }
        pilotMonitoringReporter.addRecord(pilotMonitoringData)

        self.log.verbose("Committing pilot submission to monitoring")
        result = pilotMonitoringReporter.commit()
        if not result["OK"]:
            self.log.error("Could not commit pilot submission to monitoring", result["Message"])
            return S_ERROR()
        self.log.verbose("Done committing to monitoring")
        return S_OK()
