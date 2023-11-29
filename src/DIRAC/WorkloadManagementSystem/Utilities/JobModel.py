""" This module contains the JobModel class, which is used to validate the job description """

# pylint: disable=no-self-argument, no-self-use, invalid-name, missing-function-docstring

from typing import Any
from pydantic import BaseModel, root_validator, validator

from DIRAC import gLogger
from DIRAC.ConfigurationSystem.Client.Helpers.Operations import Operations
from DIRAC.ConfigurationSystem.Client.Helpers.Resources import getDIRACPlatforms, getSites


class BaseJobDescriptionModel(BaseModel):
    """Base model for the job description (not parametric)"""

    arguments: str = None
    bannedSites: set[str] = None
    cpuTime: int = Operations().getValue("JobDescription/DefaultCPUTime", 86400)
    executable: str
    executionEnvironment: dict = None
    gridCE: str = None
    inputSandbox: set[str] = None
    inputData: set[str] = None
    inputDataPolicy: str = None
    jobConfigArgs: str = None
    jobGroup: str = None
    jobType: str = "User"
    jobName: str = "Name"
    logLevel: str = "INFO"
    maxNumberOfProcessors: int = None
    minNumberOfProcessors: int = 1
    outputData: set[str] = None
    outputPath: str = None
    outputSandbox: set[str] = None
    outputSE: str = None
    platform: str = None
    priority: int = Operations().getValue("JobDescription/DefaultPriority", 1)
    sites: set[str] = None
    stderr: str = "std.err"
    stdout: str = "std.out"
    tags: set[str] = None
    extraFields: dict[str, Any] = None

    @validator("cpuTime")
    def checkCPUTimeBounds(cls, v):
        minCPUTime = Operations().getValue("JobDescription/MinCPUTime", 100)
        maxCPUTime = Operations().getValue("JobDescription/MaxCPUTime", 500000)
        if not minCPUTime <= v <= maxCPUTime:
            raise ValueError(f"cpuTime out of bounds (must be between {minCPUTime} and {maxCPUTime})")
        return v

    @validator("executable")
    def checkExecutableIsNotAnEmptyString(cls, v: str):
        if not v:
            raise ValueError("executable must not be an empty string")
        return v

    @validator("jobType")
    def checkJobTypeIsAllowed(cls, v: str):
        jobTypes = Operations().getValue("JobDescription/AllowedJobTypes", ["User", "Test", "Hospital"])
        transformationTypes = Operations().getValue("Transformations/DataProcessing", [])
        allowedTypes = jobTypes + transformationTypes
        if v not in allowedTypes:
            raise ValueError(f"jobType '{v}' is not allowed for this kind of user (must be in {allowedTypes})")
        return v

    @validator("inputData")
    def checkInputDataDoesntContainDoubleSlashes(cls, v):
        if v:
            for lfn in v:
                if lfn.find("//") > -1:
                    raise ValueError("Input data contains //")
        return v

    @validator("inputData")
    def addLFNPrefixIfStringStartsWithASlash(cls, v: set[str]):
        if v:
            v = {lfn.strip() for lfn in v if lfn.strip()}
            v = {f"LFN:{lfn}" if lfn.startswith("/") else lfn for lfn in v}

            for lfn in v:
                if not lfn.startswith("LFN:/"):
                    raise ValueError("Input data files must start with LFN:/")
        return v

    @root_validator
    def checkNumberOfInputDataFiles(cls, values):
        if "inputData" in values and values["inputData"]:
            maxInputDataFiles = Operations().getValue("JobDescription/MaxInputData", 500)
            if values["jobType"] == "User" and len(values["inputData"]) >= maxInputDataFiles:
                raise ValueError(f"inputData contains too many files (must contain at most {maxInputDataFiles})")
        return values

    @validator("inputSandbox")
    def checkLFNSandboxesAreWellFormated(cls, v: set[str]):
        for inputSandbox in v:
            if inputSandbox.startswith("LFN:") and not inputSandbox.startswith("LFN:/"):
                raise ValueError("LFN files must start by LFN:/")
        return v

    @validator("logLevel")
    def checkLogLevelIsValid(cls, v: str):
        v = v.upper()
        possibleLogLevels = gLogger.getAllPossibleLevels()
        if v not in possibleLogLevels:
            raise ValueError(f"Log level {v} not in {possibleLogLevels}")
        return v

    @validator("minNumberOfProcessors")
    def checkMinNumberOfProcessorsBounds(cls, v):
        minNumberOfProcessors = Operations().getValue("JobDescription/MinNumberOfProcessors", 1)
        maxNumberOfProcessors = Operations().getValue("JobDescription/MaxNumberOfProcessors", 1024)
        if not minNumberOfProcessors <= v <= maxNumberOfProcessors:
            raise ValueError(
                f"minNumberOfProcessors out of bounds (must be between {minNumberOfProcessors} and {maxNumberOfProcessors})"
            )
        return v

    @validator("maxNumberOfProcessors")
    def checkMaxNumberOfProcessorsBounds(cls, v):
        minNumberOfProcessors = Operations().getValue("JobDescription/MinNumberOfProcessors", 1)
        maxNumberOfProcessors = Operations().getValue("JobDescription/MaxNumberOfProcessors", 1024)
        if not minNumberOfProcessors <= v <= maxNumberOfProcessors:
            raise ValueError(
                f"minNumberOfProcessors out of bounds (must be between {minNumberOfProcessors} and {maxNumberOfProcessors})"
            )
        return v

    @root_validator
    def checkThatMaxNumberOfProcessorsIsGreaterThanMinNumberOfProcessors(cls, values):
        if "maxNumberOfProcessors" in values and values["maxNumberOfProcessors"]:
            if values["maxNumberOfProcessors"] < values["minNumberOfProcessors"]:
                raise ValueError("maxNumberOfProcessors must be greater than minNumberOfProcessors")
        return values

    @root_validator
    def addTagsDependingOnNumberOfProcessors(cls, values):
        if "maxNumberOfProcessors" in values and values["minNumberOfProcessors"] == values["maxNumberOfProcessors"]:
            if values["tags"] is None:
                values["tags"] = set()
            values["tags"].add(f"{values['minNumberOfProcessors']}Processors")
        if values["minNumberOfProcessors"] > 1:
            if values["tags"] is None:
                values["tags"] = set()
            values["tags"].add("MultiProcessor")

        return values

    @validator("sites")
    def checkSites(cls, v: set[str]):
        if v:
            res = getSites()
            if not res["OK"]:
                raise ValueError(res["Message"])
            invalidSites = v - set(res["Value"]).union({"ANY"})
            if invalidSites:
                raise ValueError(f"Invalid sites: {' '.join(invalidSites)}")
        return v

    @root_validator
    def checkThatSitesAndBannedSitesAreNotMutuallyExclusive(cls, values):
        if "sites" in values and values["sites"] and "bannedSites" in values and values["bannedSites"]:
            values["sites"] -= values["bannedSites"]
            values["bannedSites"] = None
            if not values["sites"]:
                raise ValueError("sites and bannedSites are mutually exclusive")
        return values

    @validator("platform")
    def checkPlatform(cls, v: str):
        if v:
            res = getDIRACPlatforms()
            if not res["OK"]:
                raise ValueError(res["Message"])
            if v not in res["Value"]:
                raise ValueError("Invalid platform")
        return v

    @validator("priority")
    def checkPriorityBounds(cls, v):
        minPriority = Operations().getValue("JobDescription/MinPriority", 0)
        maxPriority = Operations().getValue("JobDescription/MaxPriority", 10)
        if not minPriority <= v <= maxPriority:
            raise ValueError(f"priority out of bounds (must be between {minPriority} and {maxPriority})")
        return v


class JobDescriptionModel(BaseJobDescriptionModel):
    """Model for the job description (non parametric job with user credentials, i.e server side)"""

    owner: str
    ownerGroup: str
    vo: str

    @root_validator
    def checkLFNMatchesREGEX(cls, values):
        if "inputData" in values and values["inputData"]:
            for lfn in values["inputData"]:
                if not lfn.startswith(f"LFN:/{values['vo']}/"):
                    raise ValueError(f"Input data not correctly specified (must start with LFN:/{values['vo']}/)")
        return values
