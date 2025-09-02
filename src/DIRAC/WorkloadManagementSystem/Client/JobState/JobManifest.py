from __future__ import annotations

from DIRACCommon.WorkloadManagementSystem.Client.JobState.JobManifest import *  # noqa: F401, F403

from DIRAC.ConfigurationSystem.Client.Helpers.Operations import Operations


def makeJobManifestConfig(ownerGroup: str) -> JobManifestConfig:
    ops = Operations(group=ownerGroup)

    allowedJobTypesForGroup = ops.getValue(
        "JobDescription/ChoicesJobType",
        ops.getValue("JobDescription/AllowedJobTypes", ["User", "Test", "Hospital"])
        + ops.getValue("Transformations/DataProcessing", []),
    )

    return {
        "defaultForGroup": {
            "CPUTime": ops.getValue("JobDescription/DefaultCPUTime", 86400),
            "Priority": ops.getValue("JobDescription/DefaultPriority", 1),
        },
        "minForGroup": {
            "CPUTime": ops.getValue("JobDescription/MinCPUTime", 100),
            "Priority": ops.getValue("JobDescription/MinPriority", 0),
        },
        "maxForGroup": {
            "CPUTime": ops.getValue("JobDescription/MaxCPUTime", 500000),
            "Priority": ops.getValue("JobDescription/MaxPriority", 10),
        },
        "allowedJobTypesForGroup": allowedJobTypesForGroup,
        "maxInputData": Operations().getValue("JobDescription/MaxInputData", 500),
    }


class JobManifest(JobManifest):  # noqa: F405 pylint: disable=function-redefined
    def check(self):
        return super().check(config=makeJobManifestConfig(self.__manifest["OwnerGroup"]))
