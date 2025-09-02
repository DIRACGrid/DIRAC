from __future__ import annotations

import base64
import zlib

from DIRAC.ConfigurationSystem.Client.Helpers.Operations import Operations
from DIRAC.Core.Utilities.DErrno import EWMSSUBM
from DIRAC.Core.Utilities.ObjectLoader import ObjectLoader
from DIRAC.Core.Utilities.ReturnValues import S_ERROR, S_OK, returnValueOrRaise
from DIRAC.WorkloadManagementSystem.Client import JobStatus
from DIRAC.WorkloadManagementSystem.Client.JobState.JobManifest import JobManifest

# Import stateless functions from DIRACCommon for backward compatibility
from DIRACCommon.WorkloadManagementSystem.DB.JobDBUtils import compressJDL, extractJDL, fixJDL

getDIRACPlatform = returnValueOrRaise(
    ObjectLoader().loadObject("ConfigurationSystem.Client.Helpers.Resources", "getDIRACPlatform")
)


def checkAndAddOwner(jdl: str, owner: str, ownerGroup: str) -> JobManifest:
    jobManifest = JobManifest()
    res = jobManifest.load(jdl)
    if not res["OK"]:
        return res

    jobManifest.setOptionsFromDict({"Owner": owner, "OwnerGroup": ownerGroup})
    res = jobManifest.check()
    if not res["OK"]:
        return res

    return S_OK(jobManifest)


def checkAndPrepareJob(jobID, classAdJob, classAdReq, owner, ownerGroup, jobAttrs, vo):
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

    inputDataPolicy = Operations(vo=vo).getValue("InputDataPolicy/InputDataModule")
    if inputDataPolicy and not classAdJob.lookupAttribute("InputDataModule"):
        classAdJob.insertAttributeString("InputDataModule", inputDataPolicy)

    softwareDistModule = Operations(vo=vo).getValue("SoftwareDistModule")
    if softwareDistModule and not classAdJob.lookupAttribute("SoftwareDistModule"):
        classAdJob.insertAttributeString("SoftwareDistModule", softwareDistModule)

    # priority
    priority = classAdJob.getAttributeInt("Priority")
    if priority is None:
        priority = 0
    classAdReq.insertAttributeInt("UserPriority", priority)

    # CPU time
    cpuTime = classAdJob.getAttributeInt("CPUTime")
    if cpuTime is None:
        opsHelper = Operations(group=ownerGroup)
        cpuTime = opsHelper.getValue("JobDescription/DefaultCPUTime", 86400)
    classAdReq.insertAttributeInt("CPUTime", cpuTime)

    # platform(s)
    platformList = classAdJob.getListFromExpression("Platform")
    if platformList:
        result = getDIRACPlatform(platformList)
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
