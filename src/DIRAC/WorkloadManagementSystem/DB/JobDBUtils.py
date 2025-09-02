from __future__ import annotations

# Import stateless functions from DIRACCommon for backward compatibility
from DIRACCommon.WorkloadManagementSystem.DB.JobDBUtils import *

from DIRAC.ConfigurationSystem.Client.Helpers.Operations import Operations
from DIRAC.Core.Utilities.ObjectLoader import ObjectLoader
from DIRAC.Core.Utilities.ReturnValues import returnValueOrRaise
from DIRAC.WorkloadManagementSystem.Client.JobState.JobManifest import makeJobManifestConfig

getDIRACPlatform = returnValueOrRaise(
    ObjectLoader().loadObject("ConfigurationSystem.Client.Helpers.Resources", "getDIRACPlatform")
)


def checkAndPrepareJob(
    jobID, classAdJob, classAdReq, owner, ownerGroup, jobAttrs, vo
):  # pylint: disable=function-redefined
    from DIRACCommon.WorkloadManagementSystem.DB.JobDBUtils import checkAndPrepareJob

    config = {
        "inputDataPolicyForVO": Operations(vo=vo).getValue("InputDataPolicy/InputDataModule"),
        "softwareDistModuleForVO": Operations(vo=vo).getValue("SoftwareDistModule"),
        "defaultCPUTimeForOwnerGroup": Operations(group=ownerGroup).getValue("JobDescription/DefaultCPUTime", 86400),
        "getDIRACPlatform": getDIRACPlatform,
    }
    return checkAndPrepareJob(jobID, classAdJob, classAdReq, owner, ownerGroup, jobAttrs, vo, config=config)


def checkAndAddOwner(jdl: str, owner: str, ownerGroup: str):  # pylint: disable=function-redefined
    from DIRACCommon.WorkloadManagementSystem.DB.JobDBUtils import checkAndAddOwner

    return checkAndAddOwner(jdl, owner, ownerGroup, job_manifest_config=makeJobManifestConfig(ownerGroup))
