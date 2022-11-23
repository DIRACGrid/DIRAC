"""Module collecting functions dealing with the GLUE2 information schema

:author: A.Sailer

Known problems:

 * ARC CEs do not seem to publish wall or CPU time per queue anywhere
 * There is no consistency between which memory information is provided where,
   execution environment vs. information for a share
 * Some execution environment IDs are used more than once

Print outs with "SCHEMA PROBLEM" point -- in my opinion -- to errors in the
published information, like a foreign key pointing to non-existent entry.

"""
from pprint import pformat

from DIRAC import gLogger, gConfig
from DIRAC.ConfigurationSystem.Client.Helpers.Resources import getCESiteMapping, getGOCSiteName
from DIRAC.Core.Utilities.ReturnValues import S_OK, S_ERROR
from DIRAC.Core.Utilities.List import breakListIntoChunks
from DIRAC.Core.Utilities.Grid import ldapsearchBDII

sLog = gLogger.getSubLogger(__name__)


def getGlue2CEInfo(vo, host=None):
    """call ldap for GLUE2 and get information

    :param str vo: Virtual Organisation
    :param str host: host to query for information
    :returns: result structure with result['Value'][siteID]['CEs'][ceID]['Queues'][queueName]. For
                 each siteID, ceID, queueName all the GLUE2 parameters are retrieved
    """

    # get all Policies allowing given VO
    filt = f"(&(objectClass=GLUE2Policy)(|(GLUE2PolicyRule=VO:{vo})(GLUE2PolicyRule=vo:{vo})))"
    polRes = ldapsearchBDII(filt=filt, attr=None, host=host, base="o=glue", selectionString="GLUE2")

    if not polRes["OK"]:
        return S_ERROR("Failed to get policies for this VO")
    polRes = polRes["Value"]

    sLog.notice(f"Found {len(polRes)} policies for this VO {vo}")
    # get all shares for this policy
    # create an or'ed list of all the shares and then call the search
    listOfSitesWithPolicies = set()
    shareFilter = ""
    for policyValues in polRes:
        # skip entries without GLUE2DomainID in the DN because we cannot associate them to a site
        if "GLUE2DomainID" not in policyValues["attr"]["dn"]:
            continue
        shareID = policyValues["attr"].get("GLUE2MappingPolicyShareForeignKey", None)
        policyID = policyValues["attr"]["GLUE2PolicyID"]
        siteName = policyValues["attr"]["dn"].split("GLUE2DomainID=")[1].split(",", 1)[0]
        listOfSitesWithPolicies.add(siteName)
        if shareID is None:  # policy not pointing to ComputingInformation
            sLog.debug(f"Policy {policyID} does not point to computing information")
            continue
        sLog.verbose(f"{siteName} policy {policyID} pointing to {shareID} ")
        sLog.debug("Policy values:\n%s" % pformat(policyValues))
        shareFilter += "(GLUE2ShareID=%s)" % shareID

    filt = "(&(objectClass=GLUE2Share)(|%s))" % shareFilter
    shareRes = ldapsearchBDII(filt=filt, attr=None, host=host, base="o=glue", selectionString="GLUE2")
    if not shareRes["OK"]:
        sLog.error("Could not get share information", shareRes["Message"])
        return shareRes
    shareInfoLists = {}
    for shareInfo in shareRes["Value"]:
        if "GLUE2DomainID" not in shareInfo["attr"]["dn"]:
            continue
        if "GLUE2ComputingShare" not in shareInfo["objectClass"]:
            sLog.debug(f"Share {shareID!r} is not a ComputingShare: \n{pformat(shareInfo)}")
            continue
        sLog.debug("Found computing share:\n%s" % pformat(shareInfo))
        siteName = shareInfo["attr"]["dn"].split("GLUE2DomainID=")[1].split(",", 1)[0]
        shareInfoLists.setdefault(siteName, []).append(shareInfo["attr"])

    siteInfo = __getGlue2ShareInfo(host, shareInfoLists)
    if not siteInfo["OK"]:
        sLog.error("Could not get CE info for", "{}: {}".format(shareID, siteInfo["Message"]))
        return siteInfo
    siteDict = siteInfo["Value"]
    sLog.debug("Found Sites:\n%s" % pformat(siteDict))
    sitesWithoutShares = set(siteDict) - listOfSitesWithPolicies
    if sitesWithoutShares:
        sLog.error("Found some sites without any shares", pformat(sitesWithoutShares))
    else:
        sLog.notice("Found information for all known sites")

    # remap siteDict to assign CEs to known sites,
    # in case their names differ from the "gocdb name" in the CS.
    newSiteDict = {}
    ceSiteMapping = getCESiteMapping().get("Value", {})
    # pylint thinks siteDict is a tuple, so we cast
    for siteName, infoDict in dict(siteDict).items():
        for ce, ceInfo in infoDict.get("CEs", {}).items():
            ceSiteName = ceSiteMapping.get(ce, siteName)
            gocSiteName = getGOCSiteName(ceSiteName).get("Value", siteName)
            newSiteDict.setdefault(gocSiteName, {}).setdefault("CEs", {})[ce] = ceInfo

    return S_OK(newSiteDict)


def __getGlue2ShareInfo(host, shareInfoLists):
    """get information from endpoints, which are the CE at a Site

    :param str host: BDII host to query
    :param dict shareInfoDict: dictionary of GLUE2 parameters belonging to the ComputingShare
    :returns: result structure S_OK/S_ERROR
    """
    executionEnvironments = []
    for _siteName, shareInfoDicts in shareInfoLists.items():
        for shareInfoDict in shareInfoDicts:
            executionEnvironment = shareInfoDict.get("GLUE2ComputingShareExecutionEnvironmentForeignKey", [])
            if not executionEnvironment:
                sLog.error("No entry for GLUE2ComputingShareExecutionEnvironmentForeignKey", pformat(shareInfoDict))
                continue
            if isinstance(executionEnvironment, str):
                executionEnvironment = [executionEnvironment]
            executionEnvironments.extend(executionEnvironment)
    resExeInfo = __getGlue2ExecutionEnvironmentInfo(host, executionEnvironments)
    if not resExeInfo["OK"]:
        sLog.error(
            "Cannot get execution environment info for:",
            str(executionEnvironments)[:100] + "  " + resExeInfo["Message"],
        )
        return resExeInfo
    exeInfos = resExeInfo["Value"]

    siteDict = {}
    for siteName, shareInfoDicts in shareInfoLists.items():
        siteDict[siteName] = {"CEs": {}}
        cesDict = siteDict[siteName]["CEs"]
        for shareInfoDict in shareInfoDicts:
            ceInfo = {}
            ceInfo["MaxWaitingJobs"] = shareInfoDict.get("GLUE2ComputingShareMaxWaitingJobs", "-1")  # This is not used
            ceInfo["Queues"] = {}
            queueInfo = {}
            queueInfo["GlueCEStateStatus"] = shareInfoDict["GLUE2ComputingShareServingState"]
            queueInfo["GlueCEPolicyMaxCPUTime"] = str(
                int(int(shareInfoDict.get("GLUE2ComputingShareMaxCPUTime", 86400)) / 60)
            )
            queueInfo["GlueCEPolicyMaxWallClockTime"] = str(
                int(int(shareInfoDict.get("GLUE2ComputingShareMaxWallTime", 86400)) / 60)
            )
            queueInfo["GlueCEInfoTotalCPUs"] = shareInfoDict.get("GLUE2ComputingShareMaxRunningJobs", "10000")
            queueInfo["GlueCECapability"] = ["CPUScalingReferenceSI00=2552"]

            try:
                maxNOPfromCS = gConfig.getValue(
                    "/Resources/Computing/CEDefaults/GLUE2ComputingShareMaxSlotsPerJob_limit", 8
                )
                maxNOPfromGLUE = int(shareInfoDict.get("GLUE2ComputingShareMaxSlotsPerJob", 1))
                numberOfProcs = min(maxNOPfromGLUE, maxNOPfromCS)
                queueInfo["NumberOfProcessors"] = numberOfProcs
                if numberOfProcs != maxNOPfromGLUE:
                    sLog.info("Limited NumberOfProcessors for", f"{siteName} from {maxNOPfromGLUE} to {numberOfProcs}")
            except ValueError:
                sLog.error(
                    "Bad content for GLUE2ComputingShareMaxSlotsPerJob:",
                    siteName + " " + shareInfoDict.get("GLUE2ComputingShareMaxSlotsPerJob"),
                )
                queueInfo["NumberOfProcessors"] = 1

            executionEnvironment = shareInfoDict.get("GLUE2ComputingShareExecutionEnvironmentForeignKey", [])
            if isinstance(executionEnvironment, str):
                executionEnvironment = [executionEnvironment]
            resExeInfo = __getGlue2ExecutionEnvironmentInfoForSite(siteName, executionEnvironment, exeInfos)
            if not resExeInfo["OK"]:
                continue

            exeInfo = resExeInfo.get("Value")
            if not exeInfo:
                sLog.error("Using dummy values. Did not find information for execution environment", siteName)
                exeInfo = {
                    "GlueHostMainMemoryRAMSize": "1999",  # intentionally identifiably dummy value
                    "GlueHostOperatingSystemVersion": "",
                    "GlueHostOperatingSystemName": "",
                    "GlueHostOperatingSystemRelease": "",
                    "GlueHostArchitecturePlatformType": "x86_64",
                    "GlueHostBenchmarkSI00": "2500",  # needed for the queue to be used by the sitedirector
                    "MANAGER": "manager:unknownBatchSystem",  # need some value for ARC
                }
            else:
                sLog.info("Found information for execution environment for", siteName)

            # sometimes the time is still in hours
            maxCPUTime = int(queueInfo["GlueCEPolicyMaxCPUTime"])
            if maxCPUTime in [12, 24, 36, 48, 168]:
                queueInfo["GlueCEPolicyMaxCPUTime"] = str(maxCPUTime * 60)
                queueInfo["GlueCEPolicyMaxWallClockTime"] = str(int(queueInfo["GlueCEPolicyMaxWallClockTime"]) * 60)

            ceInfo.update(exeInfo)
            shareEndpoints = shareInfoDict.get("GLUE2ShareEndpointForeignKey", [])
            if isinstance(shareEndpoints, str):
                shareEndpoints = [shareEndpoints]
            for endpoint in shareEndpoints:
                ceType = endpoint.rsplit(".", 1)[1]
                # get queue Name, in CREAM this is behind GLUE2entityOtherInfo...
                if ceType == "CREAM":
                    for otherInfo in shareInfoDict["GLUE2EntityOtherInfo"]:
                        if otherInfo.startswith("CREAMCEId"):
                            queueName = otherInfo.split("/", 1)[1]
                            # creamCEs are EOL soon, ignore any info they have
                            if queueInfo.pop("NumberOfProcessors", 1) != 1:
                                sLog.verbose("Ignoring MaxSlotsPerJob option for CreamCE", endpoint)

                # HTCondorCE, htcondorce
                elif ceType.lower().endswith("htcondorce"):
                    ceType = "HTCondorCE"
                    queueName = "condor"

                else:
                    sLog.error("Unknown CE Type, please check the available information", ceType)
                    continue

                queueInfo["GlueCEImplementationName"] = ceType
                ceName = endpoint.split("_", 1)[0]
                cesDict.setdefault(ceName, {})
                existingQueues = dict(cesDict[ceName].get("Queues", {}))
                existingQueues[queueName] = queueInfo
                ceInfo["Queues"] = existingQueues
                cesDict[ceName].update(ceInfo)

            # ARC CEs do not have share(?) endpoints, we have to try something else to get the information about the
            # queue etc.
            try:
                if not shareEndpoints and shareInfoDict["GLUE2ShareID"].startswith("urn:ogf"):
                    exeInfo = dict(exeInfo)  # silence pylint about tuples
                    isCEType = dict(arex=False, arc=False)
                    computingInfo = shareInfoDict["GLUE2ComputingShareComputingEndpointForeignKey"]
                    computingInfo = computingInfo if isinstance(computingInfo, list) else [computingInfo]
                    for entry in computingInfo:
                        if "gridftpjob" in entry:
                            # has an entry like
                            # urn:ogf:ComputingEndpoint:ce01.tier2.hep.manchester.ac.uk:gridftpjob:gsiftp://ce01.tier2.hep.manchester.ac.uk:2811/jobs
                            isCEType["arc"] = True
                        if "emies" in entry:
                            # has an entry like
                            # urn:ogf:ComputingEndpoint:ce01.tier2.hep.manchester.ac.uk:emies:https://ce01.tier2.hep.manchester.ac.uk:443/arex
                            isCEType["arex"] = True
                    if isCEType["arex"]:  # preferred solution AREX
                        queueInfo["GlueCEImplementationName"] = "AREX"
                    elif isCEType["arc"]:  # use ARC6 now, instead of ARC (5)
                        queueInfo["GlueCEImplementationName"] = "ARC6"
                    else:
                        sLog.error("Neither ARC nor AREX for", siteName)
                        raise AttributeError()
                    exeInfo = dict(exeInfo)  # silence pylint about tuples
                    managerName = exeInfo.pop("MANAGER", "").split(" ", 1)[0].rsplit(":", 1)[1]
                    managerName = managerName.capitalize() if managerName == "condor" else managerName
                    queueName = "nordugrid-{}-{}".format(managerName, shareInfoDict["GLUE2ComputingShareMappingQueue"])
                    ceName = shareInfoDict["GLUE2ShareID"].split("ComputingShare:")[1].split(":")[0]
                    cesDict.setdefault(ceName, {})
                    existingQueues = dict(cesDict[ceName].get("Queues", {}))
                    existingQueues[queueName] = queueInfo
                    ceInfo["Queues"] = existingQueues
                    cesDict[ceName].update(ceInfo)
            except Exception:
                sLog.error("Exception in ARC part for site:", siteName)

    return S_OK(siteDict)


def __getGlue2ExecutionEnvironmentInfo(host, executionEnvironments):
    """Find all the executionEnvironments.

    :param str host: BDII host to query
    :param list executionEnvironments: list of the execution environments to get some information from
    :returns: result of the ldapsearch for all executionEnvironments, Glue2 schema
    """
    listOfValues = []
    # break up to avoid argument list too long, it started failing at about 1900 entries
    for exeEnvs in breakListIntoChunks(executionEnvironments, 1000):
        exeFilter = ""
        for execEnv in exeEnvs:
            exeFilter += "(GLUE2ResourceID=%s)" % execEnv
        filt = "(&(objectClass=GLUE2ExecutionEnvironment)(|%s))" % exeFilter
        response = ldapsearchBDII(filt=filt, attr=None, host=host, base="o=glue", selectionString="GLUE2")
        if not response["OK"]:
            return response
        if not response["Value"]:
            sLog.error("No information found for %s" % executionEnvironments)
            continue
        listOfValues += response["Value"]
    if not listOfValues:
        return S_ERROR("No information found for executionEnvironments")
    return S_OK(listOfValues)


def __getGlue2ExecutionEnvironmentInfoForSite(sitename, foreignKeys, exeInfos):
    """Get the information about the execution environment for a specific site or ce or something.

    :param str sitename: Name of the site we are looking at
    :param list foreignKeys: list of ExecutionEnvironmentForeignkeys linked by the site
    :param list exeInfos: bdii list of dictionaries containing all the ExecutionEnvironment information for all sites
    :return: Dictionary with the information as required by the Bdii2CSagent for this site
    """
    # filter those that we want
    exeInfos = [exeInfo for exeInfo in exeInfos if exeInfo["attr"]["GLUE2ResourceID"] in foreignKeys]
    # take the CE with the lowest MainMemory
    exeInfo = sorted(exeInfos, key=lambda k: int(k["attr"]["GLUE2ExecutionEnvironmentMainMemorySize"]))
    if not exeInfo:
        sLog.error(
            "SCHEMA PROBLEM: Did not find execution info for site", sitename + " and keys: " + " ".join(foreignKeys)
        )
        return S_OK()
    sLog.debug("Found ExecutionEnvironments", pformat(exeInfo[0]))
    exeInfo = exeInfo[0]["attr"]  # pylint: disable=unsubscriptable-object
    maxRam = exeInfo.get("GLUE2ExecutionEnvironmentMainMemorySize", "")
    architecture = exeInfo.get("GLUE2ExecutionEnvironmentPlatform", "")
    architecture = "x86_64" if architecture == "amd64" else architecture
    architecture = "x86_64" if architecture == "UNDEFINEDVALUE" else architecture
    architecture = "x86_64" if "Intel(R) Xeon(R)" in architecture else architecture
    osFamily = exeInfo.get("GLUE2ExecutionEnvironmentOSFamily", "")  # e.g. linux
    osName = exeInfo.get("GLUE2ExecutionEnvironmentOSName", "")
    osVersion = exeInfo.get("GLUE2ExecutionEnvironmentOSVersion", "")
    manager = exeInfo.get("GLUE2ExecutionEnvironmentComputingManagerForeignKey", "manager:unknownBatchSystem")
    # translate to Glue1 like keys, because that is used later on
    infoDict = {
        "GlueHostMainMemoryRAMSize": maxRam,
        "GlueHostOperatingSystemVersion": osName,
        "GlueHostOperatingSystemName": osFamily,
        "GlueHostOperatingSystemRelease": osVersion,
        "GlueHostArchitecturePlatformType": architecture.lower(),
        "GlueHostBenchmarkSI00": "2500",  # needed for the queue to be used by the sitedirector
        "MANAGER": manager,  # to create the ARC QueueName mostly
    }

    return S_OK(infoDict)
