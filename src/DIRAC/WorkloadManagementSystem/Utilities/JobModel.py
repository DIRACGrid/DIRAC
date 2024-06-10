""" This module contains the JobModel class, which is used to validate the job description """

# pylint: disable=no-self-argument, no-self-use, invalid-name, missing-function-docstring

from collections.abc import Iterable
from typing import Any, Annotated, TypeAlias, Self

from pydantic import BaseModel, BeforeValidator, model_validator, field_validator, ConfigDict

from DIRAC import gLogger
from DIRAC.ConfigurationSystem.Client.Helpers.Operations import Operations
from DIRAC.ConfigurationSystem.Client.Helpers.Resources import getDIRACPlatforms, getSites


# HACK: Convert appropriate iterables into sets
def default_set_validator(value):
    if value is None:
        return set()
    elif not isinstance(value, Iterable):
        return value
    elif isinstance(value, (str, bytes, bytearray)):
        return value
    else:
        return set(value)


CoercibleSetStr: TypeAlias = Annotated[set[str], BeforeValidator(default_set_validator)]


class BaseJobDescriptionModel(BaseModel):
    """Base model for the job description (not parametric)"""

    model_config = ConfigDict(validate_assignment=True)

    arguments: str = ""
    bannedSites: CoercibleSetStr = set()
    # TODO: This should use a field factory
    cpuTime: int = Operations().getValue("JobDescription/DefaultCPUTime", 86400)
    executable: str
    executionEnvironment: dict = None
    gridCE: str = ""
    inputSandbox: CoercibleSetStr = set()
    inputData: CoercibleSetStr = set()
    inputDataPolicy: str = ""
    jobConfigArgs: str = ""
    jobGroup: str = ""
    jobType: str = "User"
    jobName: str = "Name"
    # TODO: This should be an StrEnum
    logLevel: str = "INFO"
    # TODO: This can't be None with this type hint
    maxNumberOfProcessors: int = None
    minNumberOfProcessors: int = 1
    outputData: CoercibleSetStr = set()
    outputPath: str = ""
    outputSandbox: CoercibleSetStr = set()
    outputSE: str = ""
    platform: str = ""
    # TODO: This should use a field factory
    priority: int = Operations().getValue("JobDescription/DefaultPriority", 1)
    sites: CoercibleSetStr = set()
    stderr: str = "std.err"
    stdout: str = "std.out"
    tags: CoercibleSetStr = set()
    extraFields: dict[str, Any] = {}

    @field_validator("cpuTime")
    def checkCPUTimeBounds(cls, v):
        minCPUTime = Operations().getValue("JobDescription/MinCPUTime", 100)
        maxCPUTime = Operations().getValue("JobDescription/MaxCPUTime", 500000)
        if not minCPUTime <= v <= maxCPUTime:
            raise ValueError(f"cpuTime out of bounds (must be between {minCPUTime} and {maxCPUTime})")
        return v

    @field_validator("executable")
    def checkExecutableIsNotAnEmptyString(cls, v: str):
        if not v:
            raise ValueError("executable must not be an empty string")
        return v

    @field_validator("jobType")
    def checkJobTypeIsAllowed(cls, v: str):
        jobTypes = Operations().getValue("JobDescription/AllowedJobTypes", ["User", "Test", "Hospital"])
        transformationTypes = Operations().getValue("Transformations/DataProcessing", [])
        allowedTypes = jobTypes + transformationTypes
        if v not in allowedTypes:
            raise ValueError(f"jobType '{v}' is not allowed for this kind of user (must be in {allowedTypes})")
        return v

    @field_validator("inputData")
    def checkInputDataDoesntContainDoubleSlashes(cls, v):
        if v:
            for lfn in v:
                if lfn.find("//") > -1:
                    raise ValueError("Input data contains //")
        return v

    @field_validator("inputData")
    def addLFNPrefixIfStringStartsWithASlash(cls, v: set[str]):
        if v:
            v = {lfn.strip() for lfn in v if lfn.strip()}
            v = {f"LFN:{lfn}" if lfn.startswith("/") else lfn for lfn in v}

            for lfn in v:
                if not lfn.startswith("LFN:/"):
                    raise ValueError("Input data files must start with LFN:/")
        return v

    @model_validator(mode="after")
    def checkNumberOfInputDataFiles(self) -> Self:
        if self.inputData:
            maxInputDataFiles = Operations().getValue("JobDescription/MaxInputData", 500)
            if self.jobType == "User" and len(self.inputData) >= maxInputDataFiles:
                raise ValueError(f"inputData contains too many files (must contain at most {maxInputDataFiles})")
        return self

    @field_validator("inputSandbox")
    def checkLFNSandboxesAreWellFormated(cls, v: set[str]):
        for inputSandbox in v:
            if inputSandbox.startswith("LFN:") and not inputSandbox.startswith("LFN:/"):
                raise ValueError("LFN files must start by LFN:/")
        return v

    @field_validator("logLevel")
    def checkLogLevelIsValid(cls, v: str):
        v = v.upper()
        possibleLogLevels = gLogger.getAllPossibleLevels()
        if v not in possibleLogLevels:
            raise ValueError(f"Log level {v} not in {possibleLogLevels}")
        return v

    @field_validator("minNumberOfProcessors")
    def checkMinNumberOfProcessorsBounds(cls, v):
        minNumberOfProcessors = Operations().getValue("JobDescription/MinNumberOfProcessors", 1)
        maxNumberOfProcessors = Operations().getValue("JobDescription/MaxNumberOfProcessors", 1024)
        if not minNumberOfProcessors <= v <= maxNumberOfProcessors:
            raise ValueError(
                f"minNumberOfProcessors out of bounds (must be between {minNumberOfProcessors} and {maxNumberOfProcessors})"
            )
        return v

    @field_validator("maxNumberOfProcessors")
    def checkMaxNumberOfProcessorsBounds(cls, v):
        minNumberOfProcessors = Operations().getValue("JobDescription/MinNumberOfProcessors", 1)
        maxNumberOfProcessors = Operations().getValue("JobDescription/MaxNumberOfProcessors", 1024)
        if not minNumberOfProcessors <= v <= maxNumberOfProcessors:
            raise ValueError(
                f"minNumberOfProcessors out of bounds (must be between {minNumberOfProcessors} and {maxNumberOfProcessors})"
            )
        return v

    @model_validator(mode="after")
    def checkThatMaxNumberOfProcessorsIsGreaterThanMinNumberOfProcessors(self) -> Self:
        if self.maxNumberOfProcessors:
            if self.maxNumberOfProcessors < self.minNumberOfProcessors:
                raise ValueError("maxNumberOfProcessors must be greater than minNumberOfProcessors")
        return self

    @model_validator(mode="after")
    def addTagsDependingOnNumberOfProcessors(self) -> Self:
        if self.minNumberOfProcessors == self.maxNumberOfProcessors:
            self.tags.add(f"{self.minNumberOfProcessors}Processors")
        if self.minNumberOfProcessors > 1:
            self.tags.add("MultiProcessor")
        return self

    @field_validator("sites")
    def checkSites(cls, v: set[str]):
        if v:
            res = getSites()
            if not res["OK"]:
                raise ValueError(res["Message"])
            invalidSites = v - set(res["Value"]).union({"ANY"})
            if invalidSites:
                raise ValueError(f"Invalid sites: {' '.join(invalidSites)}")
        return v

    @model_validator(mode="after")
    def checkThatSitesAndBannedSitesAreNotMutuallyExclusive(self) -> Self:
        if self.sites and self.bannedSites:
            self.sites -= self.bannedSites
            self.bannedSites = set()
            if not self.sites:
                raise ValueError("sites and bannedSites are mutually exclusive")
        return self

    @field_validator("platform")
    def checkPlatform(cls, v: str):
        if v:
            res = getDIRACPlatforms()
            if not res["OK"]:
                raise ValueError(res["Message"])
            if v not in res["Value"]:
                raise ValueError("Invalid platform")
        return v

    @field_validator("priority")
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

    @model_validator(mode="after")
    def checkLFNMatchesREGEX(self) -> Self:
        if self.inputData:
            for lfn in self.inputData:
                if not lfn.startswith(f"LFN:/{self.vo}/"):
                    raise ValueError(f"Input data not correctly specified (must start with LFN:/{self.vo}/)")
        return self
