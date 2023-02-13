"""  The Site Director is an agent performing pilot job submission to particular sites/Computing Elements.

.. literalinclude:: ../ConfigTemplate.cfg
  :start-after: ##BEGIN SiteDirector
  :end-before: ##END
  :dedent: 2
  :caption: SiteDirector options

"""
import datetime
import os
import random
import socket
import sys
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed

import DIRAC
from DIRAC import S_ERROR, S_OK, gConfig
from DIRAC.AccountingSystem.Client.DataStoreClient import gDataStoreClient
from DIRAC.AccountingSystem.Client.Types.Pilot import Pilot as PilotAccounting
from DIRAC.AccountingSystem.Client.Types.PilotSubmission import PilotSubmission as PilotSubmissionAccounting
from DIRAC.ConfigurationSystem.Client.Helpers import CSGlobals, Registry
from DIRAC.ConfigurationSystem.Client.Helpers.Operations import Operations
from DIRAC.ConfigurationSystem.Client.Helpers.Resources import getCESiteMapping
from DIRAC.Core.Base.AgentModule import AgentModule
from DIRAC.Core.Utilities.ObjectLoader import ObjectLoader
from DIRAC.Core.Utilities.TimeUtilities import second, toEpochMilliSeconds
from DIRAC.FrameworkSystem.Client.ProxyManagerClient import gProxyManager
from DIRAC.MonitoringSystem.Client.MonitoringReporter import MonitoringReporter
from DIRAC.ResourceStatusSystem.Client.ResourceStatus import ResourceStatus
from DIRAC.ResourceStatusSystem.Client.SiteStatus import SiteStatus
from DIRAC.WorkloadManagementSystem.Client import PilotStatus
from DIRAC.WorkloadManagementSystem.Client.MatcherClient import MatcherClient
from DIRAC.WorkloadManagementSystem.Client.ServerUtils import pilotAgentsDB
from DIRAC.WorkloadManagementSystem.private.ConfigHelper import findGenericPilotCredentials
from DIRAC.WorkloadManagementSystem.Service.WMSUtilities import getGridEnv
from DIRAC.WorkloadManagementSystem.Utilities.PilotWrapper import (
    _writePilotWrapperFile,
    getPilotFilesCompressedEncodedDict,
    pilotWrapperScript,
)
from DIRAC.WorkloadManagementSystem.Utilities.QueueUtilities import getQueuesResolved

MAX_PILOTS_TO_SUBMIT = 100


class SiteDirector(AgentModule):
    """SiteDirector class provides an implementation of a DIRAC agent.

    Used for submitting pilots to Computing Elements.
    """

    def __init__(self, *args, **kwargs):
        """c'tor"""
        super().__init__(*args, **kwargs)

        # on-the fly imports
        ol = ObjectLoader()
        res = ol.loadModule("ConfigurationSystem.Client.Helpers.Resources")
        if not res["OK"]:
            sys.exit(res["Message"])
        self.resourcesModule = res["Value"]

        self.queueDict = {}
        # self.queueCECache aims at saving CEs information over the cycles to avoid to create the exact same CEs each cycle
        self.queueCECache = {}
        self.queueSlots = {}
        self.failedQueues = defaultdict(int)
        # failedPilotOutput stores the number of times the Site Director failed to get a given pilot output
        self.failedPilotOutput = defaultdict(int)
        self.firstPass = True
        self.maxPilotsToSubmit = MAX_PILOTS_TO_SUBMIT

        self.gridEnv = ""
        self.vo = ""
        self.group = ""
        # self.voGroups contain all the eligible user groups for pilots submitted by this SiteDirector
        self.voGroups = []
        self.pilotDN = ""
        self.pilotGroup = ""
        self.platforms = []
        self.sites = []
        self.totalSubmittedPilots = 0

        self.addPilotsToEmptySites = False
        self.checkPlatform = False
        self.updateStatus = True
        self.getOutput = False
        self.sendAccounting = True
        self.sendSubmissionAccounting = True
        self.sendSubmissionMonitoring = False
        self.siteClient = None
        self.rssClient = None
        self.rssFlag = None

        self.globalParameters = {"NumberOfProcessors": 1, "MaxRAM": 2048}
        # self.failedQueueCycleFactor is the number of cycles a queue has to wait before getting pilots again
        self.failedQueueCycleFactor = 10
        # Every N cycles, the status of the pilots are updated by the SiteDirector
        self.pilotStatusUpdateCycleFactor = 10
        # Every N cycles, the number of slots available in the queues is updated
        self.availableSlotsUpdateCycleFactor = 10
        self.maxQueueLength = 86400 * 3
        # Maximum number of times the Site Director is going to try to get a pilot output before stopping
        self.maxRetryGetPilotOutput = 3

        self.pilotWaitingFlag = True
        self.pilotLogLevel = "INFO"
        self.matcherClient = None
        self.siteMaskList = []
        self.ceMaskList = []

        self.localhost = socket.getfqdn()

    def initialize(self):
        """Initial settings"""

        self.gridEnv = self.am_getOption("GridEnv", "")
        if not self.gridEnv:
            self.gridEnv = getGridEnv()

        # The SiteDirector is for a particular user community
        self.vo = self.am_getOption("VO", "")
        if not self.vo:
            self.vo = self.am_getOption("Community", "")
        if not self.vo:
            self.vo = CSGlobals.getVO()
        # The SiteDirector is for a particular user group
        self.group = self.am_getOption("Group", "")

        # Choose the group for which pilots will be submitted. This is a hack until
        # we will be able to match pilots to VOs.
        if not self.group:
            if self.vo:
                result = Registry.getGroupsForVO(self.vo)
                if not result["OK"]:
                    return result
                self.voGroups = []
                for group in result["Value"]:
                    if "NormalUser" in Registry.getPropertiesForGroup(group):
                        self.voGroups.append(group)
        else:
            self.voGroups = [self.group]

        # Get the clients
        self.siteClient = SiteStatus()
        self.rssClient = ResourceStatus()
        self.matcherClient = MatcherClient()

        return S_OK()

    def beginExecution(self):
        """This is run at every cycle, as first thing.

        1. Check the pilots credentials.
        2. Get some flags and options used later
        3. Get the site description dictionary
        4. Get what to send in pilot wrapper
        """

        self.rssFlag = self.rssClient.rssFlag

        # Which credentials to use?
        # are they specific to the SD? (if not, get the generic ones)
        self.pilotDN = self.am_getOption("PilotDN", self.pilotDN)
        self.pilotGroup = self.am_getOption("PilotGroup", self.pilotGroup)
        result = findGenericPilotCredentials(vo=self.vo, pilotDN=self.pilotDN, pilotGroup=self.pilotGroup)
        if not result["OK"]:
            return result
        self.pilotDN, self.pilotGroup = result["Value"]

        # Parameters
        self.workingDirectory = self.am_getOption("WorkDirectory")
        self.maxQueueLength = self.am_getOption("MaxQueueLength", self.maxQueueLength)
        self.pilotLogLevel = self.am_getOption("PilotLogLevel", self.pilotLogLevel)
        self.maxPilotsToSubmit = self.am_getOption("MaxPilotsToSubmit", self.maxPilotsToSubmit)
        self.pilotWaitingFlag = self.am_getOption("PilotWaitingFlag", self.pilotWaitingFlag)
        self.failedQueueCycleFactor = self.am_getOption("FailedQueueCycleFactor", self.failedQueueCycleFactor)
        self.pilotStatusUpdateCycleFactor = self.am_getOption(
            "PilotStatusUpdateCycleFactor", self.pilotStatusUpdateCycleFactor
        )
        self.availableSlotsUpdateCycleFactor = self.am_getOption(
            "AvailableSlotsUpdateCycleFactor", self.availableSlotsUpdateCycleFactor
        )
        self.maxRetryGetPilotOutput = self.am_getOption("MaxRetryGetPilotOutput", self.maxRetryGetPilotOutput)

        # Flags
        self.addPilotsToEmptySites = self.am_getOption("AddPilotsToEmptySites", self.addPilotsToEmptySites)
        self.checkPlatform = self.am_getOption("CheckPlatform", self.checkPlatform)
        self.updateStatus = self.am_getOption("UpdatePilotStatus", self.updateStatus)
        self.getOutput = self.am_getOption("GetPilotOutput", self.getOutput)
        self.sendAccounting = self.am_getOption("SendPilotAccounting", self.sendAccounting)

        # Check whether to send to Monitoring or Accounting or both
        monitoringOption = Operations().getMonitoringBackends(monitoringType="PilotSubmissionMonitoring")
        if "Monitoring" in monitoringOption:
            self.sendSubmissionMonitoring = True
        if "Accounting" in monitoringOption:
            self.sendSubmissionAccounting = True
        # Get the site description dictionary
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

        self.log.always("VO:", self.vo)
        if self.voGroups:
            self.log.always("Group(s):", self.voGroups)
        self.log.always("Sites:", siteNames)
        self.log.always("CETypes:", ceTypes)
        self.log.always("CEs:", ces)
        self.log.always("PilotDN:", self.pilotDN)
        self.log.always("PilotGroup:", self.pilotGroup)

        result = self.resourcesModule.getQueues(community=self.vo, siteList=siteNames, ceList=ces, ceTypeList=ceTypes)
        if not result["OK"]:
            return result
        result = getQueuesResolved(
            siteDict=result["Value"],
            queueCECache=self.queueCECache,
            gridEnv=self.gridEnv,
            setup=gConfig.getValue("/DIRAC/Setup", "unknown"),
            workingDir=self.workingDirectory,
            checkPlatform=self.checkPlatform,
            instantiateCEs=True,
        )
        if not result["OK"]:
            return result

        self.queueDict = result["Value"]
        for __queueName, queueDict in self.queueDict.items():

            # Update self.sites
            if queueDict["Site"] not in self.sites:
                self.sites.append(queueDict["Site"])

            # Update self.platforms, keeping entries unique and squashing lists
            self.platforms = []
            if "Platform" in queueDict["ParametersDict"]:
                platform = queueDict["ParametersDict"]["Platform"]
                oldPlatforms = set(self.platforms)
                if isinstance(platform, list):
                    oldPlatforms.update(set(platform))
                else:
                    oldPlatforms.add(platform)
                self.platforms = list(oldPlatforms)

            # Update self.globalParameters
            if "WholeNode" in queueDict["ParametersDict"]:
                self.globalParameters["WholeNode"] = "True"
            for parameter in ["MaxRAM", "NumberOfProcessors"]:
                if parameter in queueDict["ParametersDict"]:
                    self.globalParameters[parameter] = max(
                        self.globalParameters[parameter], int(queueDict["ParametersDict"][parameter])
                    )

        if self.updateStatus:
            self.log.always("Pilot status update requested")
        if self.getOutput:
            self.log.always("Pilot output retrieval requested")
        if self.sendAccounting:
            self.log.always("Pilot accounting sending requested")
        if self.sendSubmissionAccounting:
            self.log.always("Pilot submission accounting sending requested")
        if self.sendSubmissionMonitoring:
            self.log.always("Pilot submission monitoring sending requested")

        self.log.always("MaxPilotsToSubmit:", self.maxPilotsToSubmit)

        if self.firstPass:
            if self.queueDict:
                self.log.always("Agent will serve queues:")
                for queue in self.queueDict:
                    self.log.always(
                        f"Site: {self.queueDict[queue]['Site']}, CE: {self.queueDict[queue]['CEName']}, Queue: {queue}"
                    )
        self.firstPass = False

        return S_OK()

    def execute(self):
        """Main execution method (what is called at each agent cycle).

        It basically just calls self.submitPilots() method
        """

        if not self.queueDict:
            self.log.warn("No site defined, exiting the cycle")
            return S_OK()

        # get list of usable sites within this cycle
        result = self.siteClient.getUsableSites()
        if not result["OK"]:
            return result
        self.siteMaskList = result.get("Value", [])

        if self.rssFlag:
            ceNamesList = [queue["CEName"] for queue in self.queueDict.values()]
            result = self.rssClient.getElementStatus(ceNamesList, "ComputingElement", vO=self.vo)
            if not result["OK"]:
                self.log.error("Can not get the status of computing elements: ", result["Message"])
                return result
            # Try to get CEs which have been probed and those unprobed (vO='all').
            self.ceMaskList = [
                ceName for ceName in result["Value"] if result["Value"][ceName]["all"] in ("Active", "Degraded")
            ]
            self.log.debug("CE list with status Active or Degraded: ", self.ceMaskList)

        result = self.submitPilots()
        if not result["OK"]:
            self.log.error("Errors in the job submission: ", result["Message"])
            return result

        # Every N cycles we update the pilots status
        cyclesDone = self.am_getModuleParam("cyclesDone")
        if self.updateStatus and cyclesDone % self.pilotStatusUpdateCycleFactor == 0:
            result = self.updatePilotStatus()
            if not result["OK"]:
                self.log.error("Errors in updating pilot status: ", result["Message"])
                return result

        return S_OK()

    def submitPilots(self):
        """Go through defined computing elements and submit pilots if necessary and possible

        :return: S_OK/S_ERROR
        """

        # First, we check if we want to submit pilots at all, and also where
        submit, anySite, jobSites, testSites = self._ifAndWhereToSubmit()
        if not submit:
            self.log.notice("Not submitting any pilots at this cycle")
            return S_OK()

        # From here on we assume we are going to (try to) submit some pilots
        self.log.debug("Going to try to submit some pilots")

        self.log.verbose("Queues treated", ",".join(self.queueDict))

        self.totalSubmittedPilots = 0

        queueDictItems = list(self.queueDict.items())
        random.shuffle(queueDictItems)

        for queueName, queueDictionary in queueDictItems:
            # now submitting to the single queues
            self.log.verbose("Evaluating queue", queueName)

            # are we going to submit pilots to this specific queue?
            if not self._allowedToSubmit(queueName, anySite, jobSites, testSites):
                continue

            if "CPUTime" in queueDictionary["ParametersDict"]:
                queueCPUTime = int(queueDictionary["ParametersDict"]["CPUTime"])
            else:
                self.log.warn("CPU time limit is not specified, skipping", f"queue {queueName}")
                continue
            if queueCPUTime > self.maxQueueLength:
                queueCPUTime = self.maxQueueLength

            ce, ceDict = self._getCE(queueName)

            # additionalInfo is normally taskQueueDict
            pilotsWeMayWantToSubmit, additionalInfo = self._getPilotsWeMayWantToSubmit(ceDict)
            self.log.debug(f"{pilotsWeMayWantToSubmit} pilotsWeMayWantToSubmit are eligible for {queueName} queue")
            if not pilotsWeMayWantToSubmit:
                self.log.debug(f"...so skipping {queueName}")
                continue

            # Get the number of already waiting pilots for the queue
            totalWaitingPilots = 0
            manyWaitingPilotsFlag = False
            if self.pilotWaitingFlag:
                tqIDList = list(additionalInfo)
                result = pilotAgentsDB.countPilots(
                    {"TaskQueueID": tqIDList, "Status": PilotStatus.PILOT_WAITING_STATES}, None
                )
                if not result["OK"]:
                    self.log.error("Failed to get Number of Waiting pilots", result["Message"])
                    totalWaitingPilots = 0
                else:
                    totalWaitingPilots = result["Value"]
                    self.log.debug(f"Waiting Pilots: {totalWaitingPilots}")
            if totalWaitingPilots >= pilotsWeMayWantToSubmit:
                self.log.verbose("Possibly enough pilots already waiting", f"({totalWaitingPilots})")
                manyWaitingPilotsFlag = True
                if not self.addPilotsToEmptySites:
                    continue

            self.log.debug(
                f"{totalWaitingPilots} waiting pilots for the total of {pilotsWeMayWantToSubmit} eligible pilots for {queueName}"
            )

            # Get the number of available slots on the target site/queue
            totalSlots = self.getQueueSlots(queueName, manyWaitingPilotsFlag)
            if totalSlots <= 0:
                self.log.debug(f"{queueName}: No slots available")
                continue

            if manyWaitingPilotsFlag:
                # Throttle submission of extra pilots to empty sites
                pilotsToSubmit = int(self.maxPilotsToSubmit / 10) + 1
            else:
                pilotsToSubmit = max(0, min(totalSlots, pilotsWeMayWantToSubmit - totalWaitingPilots))
                self.log.info(
                    f"{queueName}: Slots={totalSlots}, TQ jobs(pilotsWeMayWantToSubmit)={pilotsWeMayWantToSubmit}, Pilots: waiting {totalWaitingPilots}, to submit={pilotsToSubmit}"
                )

            # Limit the number of pilots to submit to MAX_PILOTS_TO_SUBMIT
            pilotsToSubmit = min(self.maxPilotsToSubmit, pilotsToSubmit)

            # Get the working proxy
            cpuTime = queueCPUTime + 86400
            self.log.verbose("Getting pilot proxy", f"for {self.pilotDN}/{self.pilotGroup} {cpuTime} long")
            result = gProxyManager.getPilotProxyFromDIRACGroup(self.pilotDN, self.pilotGroup, cpuTime)
            if not result["OK"]:
                return result
            proxy = result["Value"]
            # Check returned proxy lifetime
            result = proxy.getRemainingSecs()  # pylint: disable=no-member
            if not result["OK"]:
                return result
            lifetime_secs = result["Value"]
            ce.setProxy(proxy, lifetime_secs)

            # now really submitting
            res = self._submitPilotsToQueue(pilotsToSubmit, ce, queueName)
            if not res["OK"]:
                self.log.info("Failed pilot submission", f"Queue: {queueName}")
            else:
                pilotList, stampDict = res["Value"]

                # updating the pilotAgentsDB... done by default but maybe not strictly necessary
                self._addPilotTQReference(queueName, additionalInfo, pilotList, stampDict)

        # Summary after the cycle over queues
        self.log.info("Total number of pilots submitted in this cycle", f"{self.totalSubmittedPilots}")

        return S_OK()

    def _ifAndWhereToSubmit(self):
        """Return a tuple that says if and where to submit pilots:

        (submit, anySite, jobSites, testSites)
        e.g.
        (True, False, {'Site1', 'Site2'}, {'Test1', 'Test2'})

        VOs may want to replace this method with different strategies
        """

        tqDict = self._getTQDictForMatching()
        if not tqDict:
            return True, True, set(), set()

        # the tqDict used here is a very generic one, not specific to one CE/queue only
        self.log.verbose("Checking overall TQ availability with requirements")
        self.log.verbose(tqDict)

        # Check that there is some work at all
        result = self.matcherClient.getMatchingTaskQueues(tqDict)
        if not result["OK"]:
            self.log.error("Matcher error: ", result["Message"])
            return False, True, set(), set()
        matchingTQs = result["Value"]
        if not matchingTQs:
            self.log.notice("No Waiting jobs suitable for the director, so nothing to submit")
            return False, True, set(), set()

        # If we are here there's some work to do, now let's see for where
        jobSites = set()
        testSites = set()
        anySite = False

        for tqDescription in matchingTQs.values():
            siteList = tqDescription.get("Sites", [])
            if siteList:
                jobSites |= set(siteList)
            else:
                anySite = True

            if "JobTypes" in tqDescription:
                if "Sites" in tqDescription:
                    for site in tqDescription["Sites"]:
                        if site.lower() != "any":
                            testSites.add(site)

        self.monitorJobsQueuesPilots(matchingTQs)

        return True, anySite, jobSites, testSites

    def monitorJobsQueuesPilots(self, matchingTQs):
        """Just printout of jobs queues and pilots status in TQ"""
        tqIDList = list(matchingTQs)
        result = pilotAgentsDB.countPilots({"TaskQueueID": tqIDList, "Status": PilotStatus.PILOT_WAITING_STATES}, None)

        totalWaitingJobs = 0
        for tqDescription in matchingTQs.values():
            totalWaitingJobs += tqDescription["Jobs"]

        if not result["OK"]:
            self.log.error("Can't count pilots", result["Message"])
        else:
            self.log.info(
                "Total jobs : number of task queues : number of waiting pilots",
                f"{totalWaitingJobs} : {len(tqIDList)} : {result['Value']}",
            )

    def _getTQDictForMatching(self):
        """Just construct a dictionary (tqDict)
        that will be used to check with Matcher if there's anything to submit.

        If extensions want, they can replace partly or fully this method.
        If it returns just an empty dict, the assuption is that we'll submit pilots no matters what.

        :returns dict: tqDict of task queue descriptions
        """
        tqDict = {"Setup": CSGlobals.getSetup(), "CPUTime": 9999999}
        if self.vo:
            tqDict["Community"] = self.vo
        if self.voGroups:
            tqDict["OwnerGroup"] = self.voGroups

        if self.checkPlatform:
            platforms = self._getPlatforms()
            if platforms:
                tqDict["Platform"] = platforms

        tqDict["Site"] = self.sites

        # Get a union of all tags
        tags = []
        for queue in self.queueDict:
            tags += self.queueDict[queue]["ParametersDict"].get("Tag", [])
        tqDict["Tag"] = list(set(tags))

        # Add overall max values for all queues
        tqDict.update(self.globalParameters)

        return tqDict

    def _getPlatforms(self):
        """Get the platforms used for TQ match
        Here for extension purpose.

        :return: list of platforms
        """
        result = self.resourcesModule.getCompatiblePlatforms(self.platforms)
        if not result["OK"]:
            self.log.error(
                "Issue getting compatible platforms, will skip check of platforms",
                self.platforms + " : " + result["Message"],
            )
        return result["Value"]

    def _allowedToSubmit(self, queue, anySite, jobSites, testSites):
        """Check if we are allowed to submit to a certain queue

        :param str queue: the queue name
        :param bool anySite: submitting anywhere?
        :param set jobSites: set of job site names (only considered if anySite is False)
        :param set testSites: set of test site names

        :return: True/False
        """

        # Check if the queue failed previously
        failedCount = self.failedQueues[queue] % self.failedQueueCycleFactor
        if failedCount != 0:
            self.log.warn("queue failed recently ==> number of cycles skipped", f"{queue} ==> {10 - failedCount}")
            self.failedQueues[queue] += 1
            return False

        # Check the status of the site
        if self.queueDict[queue]["Site"] not in self.siteMaskList and self.queueDict[queue]["Site"] not in testSites:
            self.log.verbose(
                "Queue skipped (site not in mask)",
                f"{self.queueDict[queue]['QueueName']} ({self.queueDict[queue]['Site']})",
            )
            return False

        # Check that there are task queues waiting for this site
        if not anySite and self.queueDict[queue]["Site"] not in jobSites:
            self.log.verbose(
                "Queue skipped: no workload expected",
                f"{self.queueDict[queue]['CEName']} at {self.queueDict[queue]['Site']}",
            )
            return False

        # Check the status of the CE (only for RSS=Active)
        if self.rssFlag:
            if self.queueDict[queue]["CEName"] not in self.ceMaskList:
                self.log.verbose(
                    "Skipping computing element: resource not usable",
                    f"{self.queueDict[queue]['CEName']} at {self.queueDict[queue]['Site']}",
                )
                return False

        # if we are here, it means that we are allowed to submit to the queue
        return True

    def _getCE(self, queue):
        """Prepare the queue description to look for eligible jobs

        :param str queue: queue name

        :return: ce (ComputingElement object), ceDict (dict)
        """

        ce = self.queueDict[queue]["CE"]
        ceDict = ce.ceParameters
        ceDict["GridCE"] = self.queueDict[queue]["CEName"]

        if self.queueDict[queue]["Site"] not in self.siteMaskList:
            ceDict["JobType"] = "Test"
        if self.vo:
            ceDict["Community"] = self.vo
        if self.voGroups:
            ceDict["OwnerGroup"] = self.voGroups

        if self.checkPlatform:
            platform = self.queueDict[queue]["ParametersDict"].get("Platform")
            if not platform:
                self.log.error("No platform set for CE %s, returning 'ANY'" % ce)
                ceDict["Platform"] = "ANY"
                return ce, ceDict
            result = self.resourcesModule.getCompatiblePlatforms(platform)
            if result["OK"]:
                ceDict["Platform"] = result["Value"]
            else:
                self.log.error(
                    "Issue getting compatible platforms, returning 'ANY'", f"{self.platforms}: {result['Message']}"
                )
                ceDict["Platform"] = "ANY"

        return ce, ceDict

    def _getPilotsWeMayWantToSubmit(self, ceDict):
        """Returns the number of pilots that we may want to submit to the ce described in ceDict

        This implementation is based on the number of eligible WMS taskQueues for the target site/queue.
        VOs are free to override this method and to provide a different implementation.

        :param ceDict: dictionary describing CE
        :type ceDict: dict

        :return: pilotsWeMayWantToSubmit (int), taskQueueDict (dict)
        :rType: tuple
        """

        pilotsWeMayWantToSubmit = 0

        result = self.matcherClient.getMatchingTaskQueues(ceDict)
        if not result["OK"]:
            self.log.error("Could not retrieve TaskQueues from TaskQueueDB", result["Message"])
            return 0, {}
        taskQueueDict = result["Value"]
        if not taskQueueDict:
            self.log.verbose("No matching TQs found", f"for {ceDict}")

        for tq in taskQueueDict.values():
            pilotsWeMayWantToSubmit += tq["Jobs"]

        return pilotsWeMayWantToSubmit, taskQueueDict

    def _submitPilotsToQueue(self, pilotsToSubmit, ce, queue):
        """Method that really submits the pilots to the ComputingElements' queue

        :param pilotsToSubmit: number of pilots to submit.
        :type pilotsToSubmit: int
        :param ce: computing element object to where we submit
        :type ce: ComputingElement
        :param str queue: queue where to submit

        :return: S_OK/S_ERROR.
                 If S_OK, returns tuple with (pilotsToSubmit, pilotList, stampDict)
                 where
                   pilotsToSubmit is the pilots still to submit (maybe 0)
                   pilotsList is the list of pilots submitted
                   stampDict is a dict of timestamps of pilots submission
        :rtype: dict
        """
        self.log.info("Going to submit pilots", f"(a maximum of {pilotsToSubmit} pilots to {queue} queue)")

        bundleProxy = self.queueDict[queue].get("BundleProxy", False)
        proxy = None
        if bundleProxy:
            proxy = ce.proxy

        jobExecDir = self.queueDict[queue]["ParametersDict"].get("JobExecDir", "")
        envVariables = self.queueDict[queue]["ParametersDict"].get("EnvironmentVariables", None)

        executable = self.getExecutable(queue, proxy=proxy, jobExecDir=jobExecDir, envVariables=envVariables)

        submitResult = ce.submitJob(executable, "", pilotsToSubmit)
        # In case the CE does not need the executable after the submission, we delete it
        # Else, we keep it, the CE will delete it after the end of the pilot execution
        if submitResult.get("ExecutableToKeep") != executable:
            os.unlink(executable)

        if not submitResult["OK"]:
            self.log.error("Failed submission to queue", f"Queue {queue}:\n{submitResult['Message']}")

            if self.sendSubmissionAccounting:
                result = self.sendPilotSubmissionAccounting(
                    self.queueDict[queue]["Site"],
                    self.queueDict[queue]["CEName"],
                    self.queueDict[queue]["QueueName"],
                    pilotsToSubmit,
                    0,
                    "Failed",
                )
                if not result["OK"]:
                    return result

            if self.sendSubmissionMonitoring:
                result = self.sendPilotSubmissionMonitoring(
                    self.queueDict[queue]["Site"],
                    self.queueDict[queue]["CEName"],
                    self.queueDict[queue]["QueueName"],
                    pilotsToSubmit,
                    0,
                    "Failed",
                )
                if not result["OK"]:
                    return result

            self.failedQueues[queue] += 1
            return submitResult

        # Add pilots to the PilotAgentsDB: assign pilots to TaskQueue proportionally to the task queue priorities
        pilotList = submitResult["Value"]
        self.queueSlots[queue]["AvailableSlots"] -= len(pilotList)

        self.totalSubmittedPilots += len(pilotList)
        self.log.info(
            f"Submitted {len(pilotList)} pilots to {self.queueDict[queue]['QueueName']}@{self.queueDict[queue]['CEName']}"
        )
        stampDict = submitResult.get("PilotStampDict", {})
        if self.sendSubmissionAccounting:
            result = self.sendPilotSubmissionAccounting(
                self.queueDict[queue]["Site"],
                self.queueDict[queue]["CEName"],
                self.queueDict[queue]["QueueName"],
                len(pilotList),
                len(pilotList),
                "Succeeded",
            )
            if not result["OK"]:
                return result

        if self.sendSubmissionMonitoring:
            result = self.sendPilotSubmissionMonitoring(
                self.queueDict[queue]["Site"],
                self.queueDict[queue]["CEName"],
                self.queueDict[queue]["QueueName"],
                len(pilotList),
                len(pilotList),
                "Succeeded",
            )
            if not result["OK"]:
                return result

        return S_OK((pilotList, stampDict))

    def _addPilotTQReference(self, queue, taskQueueDict, pilotList, stampDict):
        """Add to pilotAgentsDB the reference of for which TqID the pilots have been sent

        :param str queue: the queue name
        :param taskQueueDict: dict of task queues
        :type taskQueueDict: dict
        :param pilotList: list of pilots
        :type pilotList: list
        :param stampDict: dictionary of pilots timestamps
        :type stampDict: dict

        :return: None
        """

        tqPriorityList = []
        sumPriority = 0.0
        for tq in taskQueueDict:
            sumPriority += taskQueueDict[tq]["Priority"]
            tqPriorityList.append((tq, sumPriority))
        tqDict = {}
        for pilotID in pilotList:
            rndm = random.random() * sumPriority
            for tq, prio in tqPriorityList:
                if rndm < prio:
                    tqID = tq
                    break
            if tqID not in tqDict:
                tqDict[tqID] = []
            tqDict[tqID].append(pilotID)

        for tqID, pilotsList in tqDict.items():
            result = pilotAgentsDB.addPilotTQReference(
                pilotsList,
                tqID,
                self.pilotDN,
                self.pilotGroup,
                self.localhost,
                self.queueDict[queue]["CEType"],
                stampDict,
            )
            if not result["OK"]:
                self.log.error("Failed add pilots to the PilotAgentsDB", result["Message"])
                continue
            for pilot in pilotsList:
                result = pilotAgentsDB.setPilotStatus(
                    pilot,
                    PilotStatus.SUBMITTED,
                    self.queueDict[queue]["CEName"],
                    "Successfully submitted by the SiteDirector",
                    self.queueDict[queue]["Site"],
                    self.queueDict[queue]["QueueName"],
                )
                if not result["OK"]:
                    self.log.error("Failed to set pilot status", result["Message"])
                    continue

    def getQueueSlots(self, queue, manyWaitingPilotsFlag):
        """Get the number of available slots in the queue"""
        ce = self.queueDict[queue]["CE"]
        ceName = self.queueDict[queue]["CEName"]
        queueName = self.queueDict[queue]["QueueName"]
        queryCEFlag = self.queueDict[queue]["QueryCEFlag"].lower() in ["1", "yes", "true"]

        self.queueSlots.setdefault(queue, {})
        totalSlots = self.queueSlots[queue].get("AvailableSlots", 0)

        # See if there are waiting pilots for this queue. If not, allow submission
        if totalSlots and manyWaitingPilotsFlag:
            result = pilotAgentsDB.selectPilots(
                {"DestinationSite": ceName, "Queue": queueName, "Status": PilotStatus.PILOT_WAITING_STATES}
            )
            if result["OK"]:
                jobIDList = result["Value"]
                if not jobIDList:
                    return totalSlots
            return 0

        availableSlotsCount = self.queueSlots[queue].setdefault("AvailableSlotsCount", 0)
        waitingJobs = 1
        if totalSlots == 0:
            if availableSlotsCount % self.availableSlotsUpdateCycleFactor == 0:

                # Get the list of already existing pilots for this queue
                jobIDList = None
                result = pilotAgentsDB.selectPilots(
                    {"DestinationSite": ceName, "Queue": queueName, "Status": PilotStatus.PILOT_TRANSIENT_STATES}
                )

                if result["OK"]:
                    jobIDList = result["Value"]

                if queryCEFlag:
                    result = ce.available(jobIDList)
                    if not result["OK"]:
                        self.log.warn("Failed to check the availability of queue", f"{queue}: \n{result['Message']}")
                        self.failedQueues[queue] += 1
                    else:
                        ceInfoDict = result["CEInfoDict"]
                        self.log.info(
                            "CE queue report",
                            f"({ceName}_{queueName}): Wait={ceInfoDict['WaitingJobs']}, Run={ceInfoDict['RunningJobs']}, Submitted={ceInfoDict['SubmittedJobs']}, Max={ceInfoDict['MaxTotalJobs']}",
                        )
                        totalSlots = result["Value"]
                        self.queueSlots[queue]["AvailableSlots"] = totalSlots
                        waitingJobs = ceInfoDict["WaitingJobs"]
                else:
                    maxWaitingJobs = int(self.queueDict[queue]["ParametersDict"].get("MaxWaitingJobs", 10))
                    maxTotalJobs = int(self.queueDict[queue]["ParametersDict"].get("MaxTotalJobs", 10))
                    waitingToRunningRatio = float(
                        self.queueDict[queue]["ParametersDict"].get("WaitingToRunningRatio", 0.0)
                    )
                    waitingJobs = 0
                    totalJobs = 0
                    if jobIDList:
                        result = pilotAgentsDB.getPilotInfo(jobIDList)
                        if not result["OK"]:
                            self.log.warn("Failed to check PilotAgentsDB", f"for queue {queue}: \n{result['Message']}")
                            self.failedQueues[queue] += 1
                        else:
                            for _pilotRef, pilotDict in result["Value"].items():
                                if pilotDict["Status"] in PilotStatus.PILOT_TRANSIENT_STATES:
                                    totalJobs += 1
                                    if pilotDict["Status"] in PilotStatus.PILOT_WAITING_STATES:
                                        waitingJobs += 1
                            runningJobs = totalJobs - waitingJobs
                            self.log.info(
                                "PilotAgentsDB report",
                                f"({ceName}_{queueName}): Wait={waitingJobs}, Run={runningJobs}, Max={maxTotalJobs}",
                            )
                            maxWaitingJobs = int(max(maxWaitingJobs, runningJobs * waitingToRunningRatio))

                    totalSlots = min((maxTotalJobs - totalJobs), (maxWaitingJobs - waitingJobs))
                    self.queueSlots[queue]["AvailableSlots"] = max(totalSlots, 0)

        self.queueSlots[queue]["AvailableSlotsCount"] += 1

        if manyWaitingPilotsFlag and waitingJobs:
            return 0
        return totalSlots

    #####################################################################################
    def getExecutable(self, queue, proxy=None, jobExecDir="", envVariables=None, **kwargs):
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

    #####################################################################################

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

        # Pilot Logging defined?
        pilotLogging = opsHelper.getValue("/Services/JobMonitoring/usePilotsLoggingFlag", False)
        if pilotLogging:
            pilotOptions.append("-z ")

        pilotOptions.append("--pythonVersion=3")

        # Debug
        if self.pilotLogLevel.lower() == "debug":
            pilotOptions.append("-ddd")

        # DIRAC Extensions to be used in pilots
        pilotExtensionsList = opsHelper.getValue("Pilot/Extensions", [])
        extensionsList = []
        if pilotExtensionsList:
            if pilotExtensionsList[0] != "None":
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

        if "SharedArea" in queueDict:
            pilotOptions.append(f"-o '/LocalSite/SharedArea={queueDict['SharedArea']}'")

        if "ExtraPilotOptions" in queueDict:
            for extraPilotOption in queueDict["ExtraPilotOptions"].split(","):
                pilotOptions.append(extraPilotOption.strip())

        if "Modules" in queueDict:
            pilotOptions.append(f"--modules={queueDict['Modules']}")

        if "PipInstallOptions" in queueDict:
            pilotOptions.append(f"--pipInstallOptions={queueDict['PipInstallOptions']}")

        if self.group:
            pilotOptions.append(f"-G {self.group}")

        return pilotOptions

    ####################################################################################

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

    def updatePilotStatus(self):
        """Update status of pilots in transient and final states"""

        # Generate a proxy before feeding the threads to renew the ones of the CEs to perform actions
        result = gProxyManager.getPilotProxyFromDIRACGroup(self.pilotDN, self.pilotGroup, 23400)
        if not result["OK"]:
            return result
        proxy = result["Value"]

        # Getting the status of pilots in a queue implies the use of remote CEs and may lead to network latency
        # Threads aim at overcoming such issues and thus 1 thread per queue is created to
        # update the status of pilots in transient states
        with ThreadPoolExecutor(max_workers=len(self.queueDict)) as executor:
            futures = []
            for queue in self.queueDict:
                futures.append(executor.submit(self._updatePilotStatusPerQueue, queue, proxy))
            for res in as_completed(futures):
                err = res.exception()
                if err:
                    self.log.exception("Update pilot status thread failed", lException=err)

        # The pilot can be in Done state set by the job agent check if the output is retrieved
        for queue in self.queueDict:
            ce = self.queueDict[queue]["CE"]

            if not ce.isProxyValid(120)["OK"]:
                result = gProxyManager.getPilotProxyFromDIRACGroup(self.pilotDN, self.pilotGroup, 1000)
                if not result["OK"]:
                    return result
                proxy = result["Value"]
                ce.setProxy(proxy, 940)

            if callable(getattr(ce, "cleanupPilots", None)):
                ce.cleanupPilots()

            ceName = self.queueDict[queue]["CEName"]
            queueName = self.queueDict[queue]["QueueName"]
            ceType = self.queueDict[queue]["CEType"]
            siteName = self.queueDict[queue]["Site"]
            result = pilotAgentsDB.selectPilots(
                {
                    "DestinationSite": ceName,
                    "Queue": queueName,
                    "GridType": ceType,
                    "GridSite": siteName,
                    "OutputReady": "False",
                    "Status": PilotStatus.PILOT_FINAL_STATES,
                }
            )

            if not result["OK"]:
                self.log.error("Failed to select pilots", result["Message"])
                continue
            pilotRefs = result["Value"]
            if not pilotRefs:
                continue
            result = pilotAgentsDB.getPilotInfo(pilotRefs)
            if not result["OK"]:
                self.log.error("Failed to get pilots info from DB", result["Message"])
                continue
            pilotDict = result["Value"]
            if self.getOutput:
                for pRef in pilotRefs:
                    self._getPilotOutput(pRef, pilotDict, ce, ceName)

            # Check if the accounting is to be sent
            if self.sendAccounting:
                result = pilotAgentsDB.selectPilots(
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
                    continue
                pilotRefs = result["Value"]
                if not pilotRefs:
                    continue
                result = pilotAgentsDB.getPilotInfo(pilotRefs)
                if not result["OK"]:
                    self.log.error("Failed to get pilots info from DB", result["Message"])
                    continue
                pilotDict = result["Value"]
                result = self.sendPilotAccounting(pilotDict)
                if not result["OK"]:
                    self.log.error("Failed to send pilot agent accounting")

        return S_OK()

    def _updatePilotStatusPerQueue(self, queue, proxy):
        """Update status of pilots in transient state for a given queue

        :param queue: queue name
        :param proxy: proxy to check the pilot status and renewals
        """
        ce = self.queueDict[queue]["CE"]
        ceName = self.queueDict[queue]["CEName"]
        queueName = self.queueDict[queue]["QueueName"]
        ceType = self.queueDict[queue]["CEType"]
        siteName = self.queueDict[queue]["Site"]

        result = pilotAgentsDB.selectPilots(
            {
                "DestinationSite": ceName,
                "Queue": queueName,
                "GridType": ceType,
                "GridSite": siteName,
                "Status": PilotStatus.PILOT_TRANSIENT_STATES,
                "OwnerDN": self.pilotDN,
                "OwnerGroup": self.pilotGroup,
            }
        )
        if not result["OK"]:
            self.log.error("Failed to select pilots", f": {result['Message']}")
            return
        pilotRefs = result["Value"]
        if not pilotRefs:
            return

        result = pilotAgentsDB.getPilotInfo(pilotRefs)
        if not result["OK"]:
            self.log.error("Failed to get pilots info from DB", result["Message"])
            return
        pilotDict = result["Value"]

        stampedPilotRefs = []
        for pRef in pilotDict:
            if pilotDict[pRef]["PilotStamp"]:
                stampedPilotRefs.append(pRef + ":::" + pilotDict[pRef]["PilotStamp"])
            else:
                stampedPilotRefs = list(pilotRefs)
                break

        # This proxy is used for checking the pilot status and renewals
        # We really need at least a few hours otherwise the renewed
        # proxy may expire before we check again...
        result = ce.isProxyValid(3 * 3600)
        if not result["OK"]:
            ce.setProxy(proxy, 23300)

        result = ce.getJobStatus(stampedPilotRefs)
        if not result["OK"]:
            self.log.error("Failed to get pilots status from CE", f"{ceName}: {result['Message']}")
            return
        pilotCEDict = result["Value"]

        abortedPilots, getPilotOutput = self._updatePilotStatus(pilotRefs, pilotDict, pilotCEDict)
        for pRef in getPilotOutput:
            self._getPilotOutput(pRef, pilotDict, ce, ceName)

        # If something wrong in the queue, make a pause for the job submission
        if abortedPilots:
            self.failedQueues[queue] += 1

    def _updatePilotStatus(self, pilotRefs, pilotDict, pilotCEDict):
        """Really updates the pilots status

        :return: number of aborted pilots, flag for getting the pilot output
        """

        abortedPilots = 0
        getPilotOutput = []

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
                result = pilotAgentsDB.setPilotStatus(pRef, newStatus, "", "Updated by SiteDirector")
                if not result["OK"]:
                    self.log.error(result["Message"])
                if newStatus == "Aborted":
                    abortedPilots += 1
            # Set the flag to retrieve the pilot output now or not
            if newStatus in PilotStatus.PILOT_FINAL_STATES:
                if pilotDict[pRef]["OutputReady"].lower() == "false" and self.getOutput:
                    getPilotOutput.append(pRef)

        return abortedPilots, getPilotOutput

    def _getPilotOutput(self, pRef, pilotDict, ce, ceName):
        """Retrieves the pilot output for a pilot and stores it in the pilotAgentsDB"""
        self.log.info(f"Retrieving output for pilot {pRef}")
        output = None
        error = None

        pilotStamp = pilotDict[pRef]["PilotStamp"]
        pRefStamp = pRef
        if pilotStamp:
            pRefStamp = pRef + ":::" + pilotStamp

        result = ce.getJobOutput(pRefStamp)
        if not result["OK"]:
            self.failedPilotOutput[pRefStamp] += 1
            self.log.error("Failed to get pilot output", f"{ceName}: {result['Message']}")
            self.log.verbose(f"Retries left: {max(0, self.maxRetryGetPilotOutput - self.failedPilotOutput[pRefStamp])}")

            if (self.maxRetryGetPilotOutput - self.failedPilotOutput[pRefStamp]) <= 0:
                output = "Output is no longer available"
                error = "Error is no longer available"
                self.failedPilotOutput.pop(pRefStamp)
            else:
                return
        else:
            output, error = result["Value"]

        if output:
            result = pilotAgentsDB.storePilotOutput(pRef, output, error)
            if not result["OK"]:
                self.log.error("Failed to store pilot output", result["Message"])
        else:
            self.log.warn("Empty pilot output not stored to PilotDB")

    def sendPilotAccounting(self, pilotDict):
        """Send pilot accounting record"""
        for pRef in pilotDict:
            self.log.verbose("Preparing accounting record", f"for pilot {pRef}")
            pA = PilotAccounting()
            pA.setEndTime(pilotDict[pRef]["LastUpdateTime"])
            pA.setStartTime(pilotDict[pRef]["SubmissionTime"])
            retVal = Registry.getUsernameForDN(pilotDict[pRef]["OwnerDN"])
            if not retVal["OK"]:
                userName = "unknown"
                self.log.error("Can't determine username for dn", pilotDict[pRef]["OwnerDN"])
            else:
                userName = retVal["Value"]
            pA.setValueByKey("User", userName)
            pA.setValueByKey("UserGroup", pilotDict[pRef]["OwnerGroup"])
            result = getCESiteMapping(pilotDict[pRef]["DestinationSite"])
            if result["OK"] and result["Value"]:
                pA.setValueByKey("Site", result["Value"][pilotDict[pRef]["DestinationSite"]].strip())
            else:
                pA.setValueByKey("Site", "Unknown")
            pA.setValueByKey("GridCE", pilotDict[pRef]["DestinationSite"])
            pA.setValueByKey("GridMiddleware", pilotDict[pRef]["GridType"])
            pA.setValueByKey("GridResourceBroker", pilotDict[pRef]["Broker"])
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
                result = pilotAgentsDB.setAccountingFlag(pRef)
                if not result["OK"]:
                    self.log.error("Failed to set accounting flag for pilot ", pRef)

        self.log.info("Committing accounting records", f"for {len(pilotDict)} pilots")
        result = gDataStoreClient.commit()
        if result["OK"]:
            for pRef in pilotDict:
                self.log.verbose("Setting AccountingSent flag", f"for pilot {pRef}")
                result = pilotAgentsDB.setAccountingFlag(pRef)
                if not result["OK"]:
                    self.log.error("Failed to set accounting flag for pilot ", pRef)
        else:
            return result

        return S_OK()

    def sendPilotSubmissionAccounting(self, siteName, ceName, queueName, numTotal, numSucceeded, status):
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
        if hasattr(self, "_AgentModule__moduleProperties"):
            pA.setValueByKey("SiteDirector", self.am_getModuleParam("agentName"))
        else:  # In case it is not executed as agent
            pA.setValueByKey("SiteDirector", "Client")

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

    def sendPilotSubmissionMonitoring(self, siteName, ceName, queueName, numTotal, numSucceeded, status):
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

        if hasattr(self, "_AgentModule__moduleProperties"):
            siteDirName = self.am_getModuleParam("agentName")
        else:  # In case it is not executed as agent
            siteDirName = "Client"

        pilotMonitoringData = {
            "HostName": DIRAC.siteName(),
            "SiteDirector": siteDirName,
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
