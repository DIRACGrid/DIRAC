"""Stateless JobDB utilities extracted from DIRAC for DIRACCommon"""

from __future__ import annotations

import base64
import zlib
from typing import TypedDict

from DIRACCommon.Core.Utilities.ReturnValues import S_OK, DOKReturnType, S_ERROR
from DIRACCommon.Core.Utilities.DErrno import EWMSSUBM
from DIRACCommon.WorkloadManagementSystem.Client import JobStatus
from DIRACCommon.WorkloadManagementSystem.Client.JobState.JobManifest import JobManifest, JobManifestConfig


def compressJDL(jdl):
    """Return compressed JDL string."""
    return base64.b64encode(zlib.compress(jdl.encode(), -1)).decode()


def extractJDL(compressedJDL):
    """Return decompressed JDL string."""
    # the starting bracket is guaranteeed by JobManager.submitJob
    # we need the check to be backward compatible
    if isinstance(compressedJDL, bytes):
        if compressedJDL.startswith(b"["):
            return compressedJDL.decode()
    else:
        if compressedJDL.startswith("["):
            return compressedJDL
    return zlib.decompress(base64.b64decode(compressedJDL)).decode()


def fixJDL(jdl: str) -> str:
    """Fix possible lack of brackets in JDL"""
    # 1.- insert original JDL on DB and get new JobID
    # Fix the possible lack of the brackets in the JDL
    if jdl.strip()[0].find("[") != 0:
        jdl = "[" + jdl + "]"
    return jdl


class CheckAndPrepareJobConfig(TypedDict):
    """Dictionary type for defining the information checkAndPrepareJob needs from the CS"""

    inputDataPolicyForVO: str
    softwareDistModuleForVO: str
    defaultCPUTimeForOwnerGroup: int
    getDIRACPlatform: callable[list[str], DOKReturnType[list[str]]]


def checkAndPrepareJob(
    jobID, classAdJob, classAdReq, owner, ownerGroup, jobAttrs, vo, *, config: CheckAndPrepareJobConfig
):
    error = ""

    jdlOwner = classAdJob.getAttributeString("Owner")
    jdlOwnerGroup = classAdJob.getAttributeString("OwnerGroup")
    jdlVO = classAdJob.getAttributeString("VirtualOrganization")

    # The below is commented out since this is always overwritten by the submitter IDs
    # but the check allows to findout inconsistent client environments
    if jdlOwner and jdlOwner != owner:
        error = "Wrong Owner in JDL"
    elif jdlOwnerGroup and jdlOwnerGroup != ownerGroup:
        error = "Wrong Owner Group in JDL"
    elif jdlVO and jdlVO != vo:
        error = "Wrong Virtual Organization in JDL"

    classAdJob.insertAttributeString("Owner", owner)
    classAdJob.insertAttributeString("OwnerGroup", ownerGroup)

    if vo:
        classAdJob.insertAttributeString("VirtualOrganization", vo)

    classAdReq.insertAttributeString("Owner", owner)
    classAdReq.insertAttributeString("OwnerGroup", ownerGroup)
    if vo:
        classAdReq.insertAttributeString("VirtualOrganization", vo)

    if config["inputDataPolicyForVO"] and not classAdJob.lookupAttribute("InputDataModule"):
        classAdJob.insertAttributeString("InputDataModule", config["inputDataPolicyForVO"])

    if config["softwareDistModuleForVO"] and not classAdJob.lookupAttribute("SoftwareDistModule"):
        classAdJob.insertAttributeString("SoftwareDistModule", config["softwareDistModuleForVO"])

    # priority
    priority = classAdJob.getAttributeInt("Priority")
    if priority is None:
        priority = 0
    classAdReq.insertAttributeInt("UserPriority", priority)

    # CPU time
    cpuTime = classAdJob.getAttributeInt("CPUTime")
    if cpuTime is None:
        cpuTime = config["defaultCPUTimeForOwnerGroup"]
    classAdReq.insertAttributeInt("CPUTime", cpuTime)

    # platform(s)
    platformList = classAdJob.getListFromExpression("Platform")
    if platformList:
        result = config["getDIRACPlatform"](platformList)
        if not result["OK"]:
            return result
        if result["Value"]:
            classAdReq.insertAttributeVectorString("Platforms", result["Value"])
        else:
            error = "OS compatibility info not found"
    if error:
        retVal = S_ERROR(EWMSSUBM, error)
        retVal["JobId"] = jobID
        retVal["Status"] = JobStatus.FAILED
        retVal["MinorStatus"] = error

        jobAttrs["Status"] = JobStatus.FAILED

        jobAttrs["MinorStatus"] = error
        return retVal
    return S_OK()


def checkAndAddOwner(
    jdl: str, owner: str, ownerGroup: str, *, job_manifest_config: JobManifestConfig
) -> DOKReturnType[JobManifest]:
    jobManifest = JobManifest()
    res = jobManifest.load(jdl)
    if not res["OK"]:
        return res

    jobManifest.setOptionsFromDict({"Owner": owner, "OwnerGroup": ownerGroup})
    res = jobManifest.check(config=job_manifest_config)
    if not res["OK"]:
        return res

    return S_OK(jobManifest)


def createJDLWithInitialStatus(
    classAdJob, classAdReq, jdl2DBParameters, jobAttrs, initialStatus, initialMinorStatus, *, modern=False
):
    """
    :param modern: if True, store boolean instead of string for VerifiedFlag (used by diracx only)
    """
    priority = classAdJob.getAttributeInt("Priority")
    if priority is None:
        priority = 0
    jobAttrs["UserPriority"] = priority

    for jdlName in jdl2DBParameters:
        # Defaults are set by the DB.
        jdlValue = classAdJob.getAttributeString(jdlName)
        if jdlValue:
            jobAttrs[jdlName] = jdlValue

    jdlValue = classAdJob.getAttributeString("Site")
    if jdlValue:
        if jdlValue.find(",") != -1:
            jobAttrs["Site"] = "Multiple"
        else:
            jobAttrs["Site"] = jdlValue

    jobAttrs["VerifiedFlag"] = True if modern else "True"

    jobAttrs["Status"] = initialStatus

    jobAttrs["MinorStatus"] = initialMinorStatus

    reqJDL = classAdReq.asJDL()
    classAdJob.insertAttributeInt("JobRequirements", reqJDL)

    return classAdJob.asJDL()
