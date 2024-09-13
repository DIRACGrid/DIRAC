"""Utilities to help Computing Element Queues manipulation
"""
import hashlib

from DIRAC import S_OK, S_ERROR
from DIRAC.Core.Utilities.List import fromChar
from DIRAC.Core.Utilities.ClassAd.ClassAdLight import ClassAd
from DIRAC.ConfigurationSystem.Client.Helpers.Resources import getDIRACPlatform
from DIRAC.ConfigurationSystem.Client.Helpers.Operations import Operations
from DIRAC.Core.Security.ProxyInfo import getProxyInfo
from DIRAC.Resources.Computing.ComputingElementFactory import ComputingElementFactory


def getQueuesResolved(siteDict, queueCECache, vo=None, checkPlatform=False, instantiateCEs=False):
    """Get the list of relevant CEs (what is in siteDict) and their descriptions.
    The main goal of this method is to return a dictionary of queues
    """
    queueDict = {}
    ceFactory = ComputingElementFactory()

    for site in siteDict:
        for ce in siteDict[site]:
            ceDict = siteDict[site][ce]
            pilotRunDirectory = ceDict.get("PilotRunDirectory", "")
            qDict = ceDict.pop("Queues")
            for queue in qDict:
                queueName = f"{ce}_{queue}"
                queueDict[queueName] = {}
                queueDict[queueName]["ParametersDict"] = qDict[queue]
                queueDict[queueName]["ParametersDict"]["Queue"] = queue
                queueDict[queueName]["ParametersDict"]["GridCE"] = ce
                queueDict[queueName]["ParametersDict"]["Site"] = site
                if vo:
                    queueDict[queueName]["ParametersDict"]["Community"] = vo

                # Evaluate the CPU limit of the queue according to the Glue convention
                computeQueueCPULimit(queueDict[queueName]["ParametersDict"])

                # Tags & RequiredTags defined on the Queue level and on the CE level are concatenated
                # This also converts them from a string to a list if required.
                resolveTags(ceDict, queueDict[queueName]["ParametersDict"])

                # Some parameters can be defined on the CE level and are inherited by all Queues
                setAdditionalParams(ceDict, queueDict[queueName]["ParametersDict"])

                if pilotRunDirectory:
                    queueDict[queueName]["ParametersDict"]["JobExecDir"] = pilotRunDirectory

                ceQueueDict = dict(ceDict)
                ceQueueDict.update(queueDict[queueName]["ParametersDict"])

                if instantiateCEs:
                    # Generate the CE object for the queue or pick the already existing one
                    # if the queue definition did not change
                    queueHash = generateQueueHash(ceQueueDict)
                    if queueName in queueCECache and queueCECache[queueName]["Hash"] == queueHash:
                        queueCE = queueCECache[queueName]["CE"]
                    else:
                        result = ceFactory.getCE(ceName=ce, ceType=ceDict["CEType"], ceParametersDict=ceQueueDict)
                        if not result["OK"]:
                            queueDict.pop(queueName)
                            continue
                        queueCECache.setdefault(queueName, {})
                        queueCECache[queueName]["Hash"] = queueHash
                        queueCECache[queueName]["CE"] = result["Value"]
                        queueCE = queueCECache[queueName]["CE"]

                    queueDict[queueName]["ParametersDict"].update(queueCE.ceParameters)
                    queueDict[queueName]["CE"] = queueCE
                    result = queueDict[queueName]["CE"].isValid()
                    if not result["OK"]:
                        queueDict.pop(queueName)
                        queueCECache.pop(queueName)
                        continue

                queueDict[queueName]["CEName"] = ce
                queueDict[queueName]["CEType"] = ceDict["CEType"]
                queueDict[queueName]["Site"] = site
                queueDict[queueName]["QueueName"] = queue

                if checkPlatform:
                    setPlatform(ceDict, queueDict[queueName]["ParametersDict"])

    return S_OK(queueDict)


def computeQueueCPULimit(queueDict):
    """Evaluate the CPU limit of the queue according to the Glue convention"""
    if "maxCPUTime" in queueDict and "SI00" in queueDict:
        maxCPUTime = float(queueDict["maxCPUTime"])
        # For some sites there are crazy values in the CS
        maxCPUTime = max(maxCPUTime, 0)
        maxCPUTime = min(maxCPUTime, 86400 * 12.5)
        si00 = float(queueDict["SI00"])
        queueCPUTime = 60 / 250 * maxCPUTime * si00
        queueDict["CPUTime"] = int(queueCPUTime)


def resolveTags(ceDict, queueDict):
    """Tags & RequiredTags defined on the Queue level and on the CE level are concatenated.
    This also converts them from a string to a list if required.
    """
    for tagFieldName in ("Tag", "RequiredTag"):
        ceTags = ceDict.get(tagFieldName, [])
        if isinstance(ceTags, str):
            ceTags = fromChar(ceTags)
        queueTags = queueDict.get(tagFieldName, [])
        if isinstance(queueTags, str):
            queueTags = fromChar(queueTags)
        queueDict[tagFieldName] = list(set(ceTags) | set(queueTags))


def setPlatform(ceDict, queueDict):
    """Set platform according to CE parameters if not defined"""
    platform = queueDict.get("Platform", ceDict.get("Platform", ""))
    if not platform and "OS" in ceDict:
        architecture = ceDict.get("architecture", "x86_64")
        platform = "_".join([architecture, ceDict["OS"]])

    if "Platform" not in queueDict and platform:
        result = getDIRACPlatform(platform)
        if result["OK"]:
            queueDict["Platform"] = result["Value"][0]
        else:
            queueDict["Platform"] = platform


def setAdditionalParams(ceDict, queueDict):
    """Some parameters can be defined on the CE level and are inherited by all Queues"""
    for parameter in ["MaxRAM", "NumberOfProcessors", "WholeNode"]:
        queueParameter = queueDict.get(parameter, ceDict.get(parameter))
        if queueParameter:
            queueDict[parameter] = queueParameter


def generateQueueHash(queueDict):
    """Generate a hash of the queue description"""
    myMD5 = hashlib.md5()
    myMD5.update(str(queueDict).encode())
    hexstring = myMD5.hexdigest()
    return hexstring


def matchQueue(jobJDL, queueDict, fullMatch=False):
    """
    Match the job description to the queue definition

    :param str job: JDL job description
    :param bool fullMatch: test matching on all the criteria
    :param dict queueDict: queue parameters dictionary

    :return: S_OK/S_ERROR, Value - result of matching, S_OK if matched or
             S_ERROR with the reason for no match
    """

    # Check the job description validity
    job = ClassAd(jobJDL)
    if not job.isOK():
        return S_ERROR("Invalid job description")

    noMatchReasons = []

    # Check job requirements to resource
    # 1. CPUTime
    cpuTime = job.getAttributeInt("CPUTime")
    if not cpuTime:
        cpuTime = 84600
    if cpuTime > int(queueDict.get("CPUTime", 0)):
        noMatchReasons.append("Job CPUTime requirement not satisfied")
        if not fullMatch:
            return S_OK({"Match": False, "Reason": noMatchReasons[0]})

    # 2. Multi-value match requirements
    for parameter in ["Site", "GridCE", "Platform", "JobType"]:
        if parameter in queueDict:
            valueSet = set(job.getListFromExpression(parameter))
            if not valueSet:
                valueSet = set(job.getListFromExpression(f"{parameter}s"))
            queueSet = set(fromChar(queueDict[parameter]))
            if valueSet and queueSet and not valueSet.intersection(queueSet):
                valueToPrint = ",".join(valueSet)
                if len(valueToPrint) > 20:
                    valueToPrint = f"{valueToPrint[:20]}..."
                noMatchReasons.append(f"Job {parameter} {valueToPrint} requirement not satisfied")
                if not fullMatch:
                    return S_OK({"Match": False, "Reason": noMatchReasons[0]})

    # 3. Banned multi-value match requirements
    for par in ["Site", "GridCE", "Platform", "JobType"]:
        parameter = f"Banned{par}"
        if par in queueDict:
            valueSet = set(job.getListFromExpression(parameter))
            if not valueSet:
                valueSet = set(job.getListFromExpression(f"{parameter}s"))
            queueSet = set(fromChar(queueDict[par]))
            if valueSet and queueSet and valueSet.issubset(queueSet):
                valueToPrint = ",".join(valueSet)
                if len(valueToPrint) > 20:
                    valueToPrint = f"{valueToPrint[:20]}..."
                noMatchReasons.append(f"Job {parameter} {valueToPrint} requirement not satisfied")
                if not fullMatch:
                    return S_OK({"Match": False, "Reason": noMatchReasons[0]})

    # 4. Tags
    tags = set(job.getListFromExpression("Tag"))
    nProc = job.getAttributeInt("NumberOfProcessors")
    if nProc and nProc > 1:
        tags.add("MultiProcessor")
    wholeNode = job.getAttributeString("WholeNode")
    if wholeNode:
        tags.add("WholeNode")
    queueTags = set(queueDict.get("Tag", []))
    if not tags.issubset(queueTags):
        noMatchReasons.append(f"Job Tag {','.join(tags)} not satisfied")
        if not fullMatch:
            return S_OK({"Match": False, "Reason": noMatchReasons[0]})

    # 4. MultiProcessor requirements
    if nProc and nProc > int(queueDict.get("NumberOfProcessors", 1)):
        noMatchReasons.append("Job NumberOfProcessors %d requirement not satisfied" % nProc)
        if not fullMatch:
            return S_OK({"Match": False, "Reason": noMatchReasons[0]})

    # 5. RAM
    ram = job.getAttributeInt("RAM")
    # If MaxRAM is not specified in the queue description, assume 2GB
    if ram and ram > int(queueDict.get("MaxRAM", 2048) / 1024):
        noMatchReasons.append("Job RAM %d requirement not satisfied" % ram)
        if not fullMatch:
            return S_OK({"Match": False, "Reason": noMatchReasons[0]})

    # Check resource requirements to job
    # 1. OwnerGroup - rare case but still
    if "OwnerGroup" in queueDict:
        result = getProxyInfo(disableVOMS=True)
        if not result["OK"]:
            return S_ERROR("No valid proxy available")
        ownerGroup = result["Value"]["group"]
        if ownerGroup != queueDict["OwnerGroup"]:
            noMatchReasons.append(f"Resource OwnerGroup {queueDict['OwnerGroup']} requirement not satisfied")
            if not fullMatch:
                return S_OK({"Match": False, "Reason": noMatchReasons[0]})

    # 2. Required tags
    requiredTags = set(queueDict.get("RequiredTags", []))
    if not requiredTags.issubset(tags):
        noMatchReasons.append(f"Resource RequiredTags {','.join(requiredTags)} not satisfied")
        if not fullMatch:
            return S_OK({"Match": False, "Reason": noMatchReasons[0]})

    # 3. RunningLimit
    site = queueDict["Site"]
    ce = queueDict.get("GridCE")
    opsHelper = Operations()
    result = opsHelper.getSections("JobScheduling/RunningLimit")
    if result["OK"] and site in result["Value"]:
        result = opsHelper.getSections(f"JobScheduling/RunningLimit/{site}")
        if result["OK"]:
            for parameter in result["Value"]:
                value = job.getAttributeString(parameter)
                if (
                    value
                    and (
                        opsHelper.getValue(f"JobScheduling/RunningLimit/{site}/{parameter}/{value}", 1)
                        or opsHelper.getValue(f"JobScheduling/RunningLimit/{site}/CEs/{ce}/{parameter}/{value}", 1)
                    )
                    == 0
                ):
                    noMatchReasons.append(f"Resource operational {parameter} requirement not satisfied")
                    if not fullMatch:
                        return S_OK({"Match": False, "Reason": noMatchReasons[0]})

    return S_OK({"Match": not bool(noMatchReasons), "Reason": noMatchReasons})
