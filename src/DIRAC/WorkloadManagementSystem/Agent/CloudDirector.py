"""  The Cloud Director is a simple agent performing VM instantiations
"""
import random
import socket
import hashlib
from collections import defaultdict

from DIRAC import S_OK, S_ERROR, gConfig
from DIRAC.Core.Base.AgentModule import AgentModule
from DIRAC.ConfigurationSystem.Client.Helpers import CSGlobals, Registry, Resources
from DIRAC.WorkloadManagementSystem.Client.MatcherClient import MatcherClient
from DIRAC.Core.Utilities.List import fromChar
from DIRAC.WorkloadManagementSystem.Client.ServerUtils import pilotAgentsDB
from DIRAC.ResourceStatusSystem.Client.SiteStatus import SiteStatus
from DIRAC.Resources.Cloud.EndpointFactory import EndpointFactory
from DIRAC.ConfigurationSystem.Client.Helpers.Resources import (
    findGenericCloudCredentials,
    getVMTypes,
    getPilotBootstrapParameters,
)
from DIRAC.WorkloadManagementSystem.Client.ServerUtils import virtualMachineDB
from DIRAC.WorkloadManagementSystem.Utilities.Utils import getProxyFileForCloud


class CloudDirector(AgentModule):
    """The CloudDirector works like a SiteDirector for cloud sites:
    It looks at the queued jobs in the task queues and attempts to
    start VM instances to meet the current demand.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.vmTypeDict = {}
        self.vmTypeCECache = {}
        self.vmTypeSlots = {}
        self.failedVMTypes = defaultdict(int)
        self.firstPass = True

        self.vo = ""
        self.group = ""
        # self.voGroups contain all the eligible user groups for clouds submitted by this SiteDirector
        self.voGroups = []
        self.cloudDN = ""
        self.cloudGroup = ""
        self.platforms = []
        self.sites = []
        self.siteClient = None

        self.proxy = None

        self.updateStatus = True
        self.getOutput = False
        self.sendAccounting = True

    def initialize(self):
        self.siteClient = SiteStatus()
        return S_OK()

    def beginExecution(self):

        # The Director is for a particular user community
        self.vo = self.am_getOption("VO", "")
        if not self.vo:
            self.vo = CSGlobals.getVO()
        # The SiteDirector is for a particular user group
        self.group = self.am_getOption("Group", "")

        # Choose the group for which clouds will be submitted. This is a hack until
        # we will be able to match clouds to VOs.
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

        result = findGenericCloudCredentials(vo=self.vo)
        if not result["OK"]:
            return result
        self.cloudDN, self.cloudGroup = result["Value"]
        self.maxVMsToSubmit = self.am_getOption("MaxVMsToSubmit", 1)
        self.runningPod = self.am_getOption("RunningPod", self.vo)

        # Get the site description dictionary
        siteNames = None
        if not self.am_getOption("Site", "Any").lower() == "any":
            siteNames = self.am_getOption("Site", [])
            if not siteNames:
                siteNames = None
        ces = None
        if not self.am_getOption("CEs", "Any").lower() == "any":
            ces = self.am_getOption("CEs", [])
            if not ces:
                ces = None

        result = getVMTypes(vo=self.vo, siteList=siteNames)
        if not result["OK"]:
            return result
        resourceDict = result["Value"]
        result = self.getEndpoints(resourceDict)
        if not result["OK"]:
            return result

        # if not siteNames:
        #  siteName = gConfig.getValue( '/DIRAC/Site', 'Unknown' )
        #  if siteName == 'Unknown':
        #    return S_OK( 'No site specified for the SiteDirector' )
        #  else:
        #    siteNames = [siteName]
        # self.siteNames = siteNames

        self.log.always("Sites:", siteNames)
        self.log.always("CEs:", ces)
        self.log.always("CloudDN:", self.cloudDN)
        self.log.always("CloudGroup:", self.cloudGroup)

        self.localhost = socket.getfqdn()
        self.proxy = ""

        if self.firstPass:
            if self.vmTypeDict:
                self.log.always("Agent will serve VM types:")
                for vmType in self.vmTypeDict:
                    self.log.always(
                        "Site: %s, CE: %s, VMType: %s"
                        % (self.vmTypeDict[vmType]["Site"], self.vmTypeDict[vmType]["CEName"], vmType)
                    )
        self.firstPass = False
        return S_OK()

    def __generateVMTypeHash(self, vmTypeDict):
        """Generate a hash of the queue description"""
        myMD5 = hashlib.md5()
        myMD5.update(str(sorted(vmTypeDict.items())).encode())
        hexstring = myMD5.hexdigest()
        return hexstring

    def getEndpoints(self, resourceDict):
        """Get the list of relevant CEs and their descriptions"""

        self.vmTypeDict = {}
        ceFactory = EndpointFactory()

        result = getPilotBootstrapParameters(vo=self.vo, runningPod=self.runningPod)
        if not result["OK"]:
            return result
        opParameters = result["Value"]

        for site in resourceDict:
            for ce in resourceDict[site]:
                ceDict = resourceDict[site][ce]
                ceTags = ceDict.get("Tag", [])
                if isinstance(ceTags, str):
                    ceTags = fromChar(ceTags)
                ceMaxRAM = ceDict.get("MaxRAM", None)
                qDict = ceDict.pop("VMTypes")
                for vmType in qDict:
                    vmTypeName = f"{ce}_{vmType}"
                    self.vmTypeDict[vmTypeName] = {}
                    self.vmTypeDict[vmTypeName]["ParametersDict"] = qDict[vmType]
                    self.vmTypeDict[vmTypeName]["ParametersDict"]["VMType"] = vmType
                    self.vmTypeDict[vmTypeName]["ParametersDict"]["Site"] = site
                    self.vmTypeDict[vmTypeName]["ParametersDict"]["Setup"] = gConfig.getValue("/DIRAC/Setup", "unknown")
                    self.vmTypeDict[vmTypeName]["ParametersDict"]["CPUTime"] = 99999999

                    vmTypeTags = self.vmTypeDict[vmTypeName]["ParametersDict"].get("Tag")
                    if vmTypeTags and isinstance(vmTypeTags, str):
                        vmTypeTags = fromChar(vmTypeTags)
                        self.vmTypeDict[vmTypeName]["ParametersDict"]["Tag"] = vmTypeTags
                    if ceTags:
                        if vmTypeTags:
                            allTags = list(set(ceTags + vmTypeTags))
                            self.vmTypeDict[vmTypeName]["ParametersDict"]["Tag"] = allTags
                        else:
                            self.vmTypeDict[vmTypeName]["ParametersDict"]["Tag"] = ceTags

                    maxRAM = self.vmTypeDict[vmTypeName]["ParametersDict"].get("MaxRAM")
                    maxRAM = ceMaxRAM if not maxRAM else maxRAM
                    if maxRAM:
                        self.vmTypeDict[vmTypeName]["ParametersDict"]["MaxRAM"] = maxRAM

                    ceWholeNode = ceDict.get("WholeNode", "true")
                    wholeNode = self.vmTypeDict[vmTypeName]["ParametersDict"].get("WholeNode", ceWholeNode)
                    if wholeNode.lower() in ("yes", "true"):
                        self.vmTypeDict[vmTypeName]["ParametersDict"].setdefault("Tag", [])
                        self.vmTypeDict[vmTypeName]["ParametersDict"]["Tag"].append("WholeNode")

                    platform = ""
                    if "Platform" in self.vmTypeDict[vmTypeName]["ParametersDict"]:
                        platform = self.vmTypeDict[vmTypeName]["ParametersDict"]["Platform"]
                    elif "Platform" in ceDict:
                        platform = ceDict["Platform"]
                    if platform and platform not in self.platforms:
                        self.platforms.append(platform)

                    if "Platform" not in self.vmTypeDict[vmTypeName]["ParametersDict"] and platform:
                        result = Resources.getDIRACPlatform(platform)
                        if result["OK"]:
                            self.vmTypeDict[vmTypeName]["ParametersDict"]["Platform"] = result["Value"][0]

                    ceVMTypeDict = dict(ceDict)
                    ceVMTypeDict["CEName"] = ce
                    ceVMTypeDict["VO"] = self.vo
                    ceVMTypeDict["VMType"] = vmType
                    ceVMTypeDict["RunningPod"] = self.runningPod
                    ceVMTypeDict["CSServers"] = gConfig.getValue("/DIRAC/Configuration/Servers", [])
                    ceVMTypeDict.update(self.vmTypeDict[vmTypeName]["ParametersDict"])

                    # Allow a resource-specifc CAPath to be set (as some clouds have their own CAs)
                    # Otherwise fall back to the system-wide default(s)
                    if "CAPath" not in ceVMTypeDict:
                        ceVMTypeDict["CAPath"] = gConfig.getValue(
                            "/DIRAC/Security/CAPath", "/opt/dirac/etc/grid-security/certificates/cas.pem"
                        )

                    # Generate the CE object for the vmType or pick the already existing one
                    # if the vmType definition did not change
                    vmTypeHash = self.__generateVMTypeHash(ceVMTypeDict)
                    if vmTypeName in self.vmTypeCECache and self.vmTypeCECache[vmTypeName]["Hash"] == vmTypeHash:
                        vmTypeCE = self.vmTypeCECache[vmTypeName]["CE"]
                    else:
                        result = ceFactory.getCEObject(parameters=ceVMTypeDict)
                        if not result["OK"]:
                            return result
                        self.vmTypeCECache.setdefault(vmTypeName, {})
                        self.vmTypeCECache[vmTypeName]["Hash"] = vmTypeHash
                        self.vmTypeCECache[vmTypeName]["CE"] = result["Value"]
                        vmTypeCE = self.vmTypeCECache[vmTypeName]["CE"]
                        vmTypeCE.setBootstrapParameters(opParameters)

                    self.vmTypeDict[vmTypeName]["CE"] = vmTypeCE
                    self.vmTypeDict[vmTypeName]["CEName"] = ce
                    self.vmTypeDict[vmTypeName]["CEType"] = ceDict["CEType"]
                    self.vmTypeDict[vmTypeName]["Site"] = site
                    self.vmTypeDict[vmTypeName]["VMType"] = vmType
                    self.vmTypeDict[vmTypeName]["Platform"] = platform
                    self.vmTypeDict[vmTypeName]["MaxInstances"] = ceDict["MaxInstances"]
                    if not self.vmTypeDict[vmTypeName]["CE"].isValid():
                        self.log.error("Failed to instantiate CloudEndpoint for %s" % vmTypeName)
                        continue

                    if site not in self.sites:
                        self.sites.append(site)

        return S_OK()

    def execute(self):
        """Main execution method"""

        if not self.vmTypeDict:
            self.log.warn("No site defined, exiting the cycle")
            return S_OK()

        result = self.createVMs()
        if not result["OK"]:
            self.log.error("Errors in the job submission: ", result["Message"])

        # cyclesDone = self.am_getModuleParam( 'cyclesDone' )
        # if self.updateStatus and cyclesDone % self.cloudStatusUpdateCycleFactor == 0:
        #  result = self.updatePilotStatus()
        #  if not result['OK']:
        #    self.log.error( 'Errors in updating cloud status: ', result['Message'] )

        return S_OK()

    def createVMs(self):
        """Go through defined computing elements and submit jobs if necessary"""

        vmTypeList = list(self.vmTypeDict.keys())

        # Check that there is some work at all
        setup = CSGlobals.getSetup()
        tqDict = {"Setup": setup, "CPUTime": 9999999}
        if self.vo:
            tqDict["VO"] = self.vo
        if self.voGroups:
            tqDict["OwnerGroup"] = self.voGroups

        result = Resources.getCompatiblePlatforms(self.platforms)
        if not result["OK"]:
            return result
        tqDict["Platform"] = result["Value"]
        tqDict["Site"] = self.sites
        tags = []
        for vmType in vmTypeList:
            if "Tag" in self.vmTypeDict[vmType]["ParametersDict"]:
                tags += self.vmTypeDict[vmType]["ParametersDict"]["Tag"]
        tqDict["Tag"] = list(set(tags))

        self.log.verbose("Checking overall TQ availability with requirements")
        self.log.verbose(tqDict)

        matcherClient = MatcherClient()
        result = matcherClient.getMatchingTaskQueues(tqDict)
        if not result["OK"]:
            return result
        if not result["Value"]:
            self.log.verbose("No Waiting jobs suitable for the director")
            return S_OK()

        jobSites = set()
        anySite = False
        testSites = set()
        totalWaitingJobs = 0
        for tqID in result["Value"]:
            if "Sites" in result["Value"][tqID]:
                for site in result["Value"][tqID]["Sites"]:
                    if site.lower() != "any":
                        jobSites.add(site)
                    else:
                        anySite = True
            else:
                anySite = True
            if "JobTypes" in result["Value"][tqID]:
                if "Sites" in result["Value"][tqID]:
                    for site in result["Value"][tqID]["Sites"]:
                        if site.lower() != "any":
                            testSites.add(site)
            totalWaitingJobs += result["Value"][tqID]["Jobs"]

        tqIDList = list(result["Value"].keys())

        result = virtualMachineDB.getInstanceCounters("Status", {})
        totalVMs = 0
        if result["OK"]:
            for status in result["Value"]:
                if status in ["New", "Submitted", "Running"]:
                    totalVMs += result["Value"][status]
        self.log.info("Total %d jobs in %d task queues with %d VMs" % (totalWaitingJobs, len(tqIDList), totalVMs))

        # Check if the site is allowed in the mask
        result = self.siteClient.getUsableSites()
        if not result["OK"]:
            return S_ERROR("Can not get the site mask")
        siteMaskList = result.get("Value", [])

        vmTypeList = list(self.vmTypeDict.keys())
        random.shuffle(vmTypeList)
        totalSubmittedPilots = 0
        matchedQueues = 0
        for vmType in vmTypeList:
            ce = self.vmTypeDict[vmType]["CE"]
            ceName = self.vmTypeDict[vmType]["CEName"]
            vmTypeName = self.vmTypeDict[vmType]["VMType"]
            siteName = self.vmTypeDict[vmType]["Site"]
            platform = self.vmTypeDict[vmType]["Platform"]
            vmTypeTags = self.vmTypeDict[vmType]["ParametersDict"].get("Tag", [])
            siteMask = siteName in siteMaskList
            endpoint = f"{siteName}::{ceName}"
            maxInstances = int(self.vmTypeDict[vmType]["MaxInstances"])
            processorTags = []

            # vms support WholeNode naturally
            processorTags.append("WholeNode")

            if not anySite and siteName not in jobSites:
                self.log.verbose(f"Skipping queue {vmTypeName} at {siteName}: no workload expected")
                continue
            if not siteMask and siteName not in testSites:
                self.log.verbose(f"Skipping queue {vmTypeName}: site {siteName} not in the mask")
                continue

            if "CPUTime" in self.vmTypeDict[vmType]["ParametersDict"]:
                vmTypeCPUTime = int(self.vmTypeDict[vmType]["ParametersDict"]["CPUTime"])
            else:
                self.log.warn("CPU time limit is not specified for queue %s, skipping..." % vmType)
                continue

            # Prepare the queue description to look for eligible jobs
            ceDict = ce.getParameterDict()

            if not siteMask:
                ceDict["JobType"] = "Test"
            if self.vo:
                ceDict["VO"] = self.vo
            if self.voGroups:
                ceDict["OwnerGroup"] = self.voGroups

            result = Resources.getCompatiblePlatforms(platform)
            if not result["OK"]:
                continue
            ceDict["Platform"] = result["Value"]

            ceDict["Tag"] = list(set(processorTags + vmTypeTags))

            # Get the number of eligible jobs for the target site/queue

            result = matcherClient.getMatchingTaskQueues(ceDict)
            if not result["OK"]:
                self.log.error("Could not retrieve TaskQueues from TaskQueueDB", result["Message"])
                return result
            taskQueueDict = result["Value"]
            if not taskQueueDict:
                self.log.verbose("No matching TQs found for %s" % vmType)
                continue

            matchedQueues += 1
            totalTQJobs = 0
            tqIDList = list(taskQueueDict.keys())
            for tq in taskQueueDict:
                totalTQJobs += taskQueueDict[tq]["Jobs"]

            self.log.verbose(
                "%d job(s) from %d task queue(s) are eligible for %s queue" % (totalTQJobs, len(tqIDList), vmType)
            )

            # Get the number of already instantiated VMs for these task queues
            totalWaitingVMs = 0
            result = virtualMachineDB.getInstanceCounters("Status", {"Endpoint": endpoint})
            if result["OK"]:
                for status in result["Value"]:
                    if status in ["New", "Submitted"]:
                        totalWaitingVMs += result["Value"][status]
            if totalWaitingVMs >= totalTQJobs:
                self.log.verbose("%d VMs already for all the available jobs" % totalWaitingVMs)

            self.log.verbose("%d VMs for the total of %d eligible jobs for %s" % (totalWaitingVMs, totalTQJobs, vmType))

            # Get proxy to be used to connect to the cloud endpoint
            authType = ce.parameters.get("Auth")
            if authType and authType.lower() in ["x509", "voms"]:
                self.log.verbose(f"Getting cloud proxy for {siteName}/{ceName}")
                result = getProxyFileForCloud(ce)
                if not result["OK"]:
                    continue
                ce.setProxy(result["Value"])

            # Get the number of available slots on the target site/endpoint
            totalSlots = self.getVMInstances(endpoint, maxInstances)
            if totalSlots == 0:
                self.log.debug("%s: No slots available" % vmType)
                continue

            vmsToSubmit = max(0, min(totalSlots, totalTQJobs - totalWaitingVMs))
            self.log.info(
                "%s: Slots=%d, TQ jobs=%d, VMs: %d, to submit=%d"
                % (vmType, totalSlots, totalTQJobs, totalWaitingVMs, vmsToSubmit)
            )

            # Limit the number of VM instances to create to vmsToSubmit
            vmsToSubmit = min(self.maxVMsToSubmit, vmsToSubmit)
            if vmsToSubmit == 0:
                continue

            self.log.info("Going to submit %d VMs to %s queue" % (vmsToSubmit, vmType))
            result = ce.createInstances(vmsToSubmit)

            # result = S_OK()
            if not result["OK"]:
                self.log.error("Failed submission to queue %s:\n" % vmType, result["Message"])
                self.failedVMTypes.setdefault(vmType, 0)
                self.failedVMTypes[vmType] += 1
                continue

            # Add VMs to the VirtualMachineDB
            vmDict = result["Value"]
            totalSubmittedPilots += len(vmDict)
            self.log.info("Submitted %d VMs to %s@%s" % (len(vmDict), vmTypeName, ceName))

            pilotList = []
            for uuID in vmDict:
                diracUUID = vmDict[uuID]["InstanceID"]
                endpoint = "{}::{}".format(self.vmTypeDict[vmType]["Site"], ceName)
                result = virtualMachineDB.insertInstance(uuID, vmTypeName, diracUUID, endpoint, self.vo)
                if not result["OK"]:
                    continue
                pRef = "vm://" + ceName + "/" + diracUUID + ":00"
                pilotList.append(pRef)

            stampDict = {}
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

            for tqID, pilotList in tqDict.items():
                result = pilotAgentsDB.addPilotTQReference(pilotList, tqID, "", "", self.localhost, "Cloud", stampDict)
                if not result["OK"]:
                    self.log.error("Failed to insert pilots into the PilotAgentsDB: %s" % result["Message"])

        self.log.info(
            "%d VMs submitted in total in this cycle, %d matched queues" % (totalSubmittedPilots, matchedQueues)
        )
        return S_OK()

    def getVMInstances(self, endpoint, maxInstances):

        result = virtualMachineDB.getInstanceCounters("Status", {"Endpoint": endpoint})
        if not result["OK"]:
            return result

        count = 0
        for status in result["Value"]:
            if status in ["New", "Submitted", "Running"]:
                count += int(result["Value"][status])

        return max(0, maxInstances - count)
