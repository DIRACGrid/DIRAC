from __future__ import annotations

from typing import ClassVar
from pydantic import PrivateAttr
from DIRACCommon.WorkloadManagementSystem.Utilities.JobModel import *  # noqa: F401, F403

from DIRAC import gLogger
from DIRAC.ConfigurationSystem.Client.Helpers.Resources import getSites


def _make_model_config(cls=None) -> BaseJobDescriptionModelConfg:
    from DIRAC.ConfigurationSystem.Client.Helpers.Operations import Operations

    ops = Operations()
    allowedJobTypes = ops.getValue("JobDescription/AllowedJobTypes", ["User", "Test", "Hospital"])
    allowedJobTypes += ops.getValue("Transformations/DataProcessing", [])
    return {
        "cpuTime": ops.getValue("JobDescription/DefaultCPUTime", 86400),
        "priority": ops.getValue("JobDescription/DefaultPriority", 1),
        "minCPUTime": ops.getValue("JobDescription/MinCPUTime", 100),
        "maxCPUTime": ops.getValue("JobDescription/MaxCPUTime", 500000),
        "allowedJobTypes": allowedJobTypes,
        "maxInputDataFiles": ops.getValue("JobDescription/MaxInputData", 500),
        "minNumberOfProcessors": ops.getValue("JobDescription/MinNumberOfProcessors", 1),
        "maxNumberOfProcessors": ops.getValue("JobDescription/MaxNumberOfProcessors", 1024),
        "minPriority": ops.getValue("JobDescription/MinPriority", 0),
        "maxPriority": ops.getValue("JobDescription/MaxPriority", 10),
        "possibleLogLevels": gLogger.getAllPossibleLevels(),
        "sites": getSites(),
    }


class BaseJobDescriptionModel(BaseJobDescriptionModel):  # noqa: F405 pylint: disable=function-redefined
    _config_builder: ClassVar = _make_model_config


class JobDescriptionModel(JobDescriptionModel):  # noqa: F405 pylint: disable=function-redefined
    _config_builder: ClassVar = _make_model_config
