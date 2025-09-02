""" This module contains the JobModel class, which is used to validate the job description """

# pylint: disable=no-self-argument, no-self-use, invalid-name, missing-function-docstring

from collections.abc import Iterable
from typing import Annotated, Any, Callable, ClassVar, Self, TypeAlias, TypedDict

from pydantic import BaseModel, BeforeValidator, ConfigDict, field_validator, model_validator

from DIRACCommon.Core.Utilities.ReturnValues import DErrorReturnType


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


class BaseJobDescriptionModelConfg(TypedDict):
    """Dictionary type for defining the information JobDescriptionModel needs from the CS"""

    # Default values
    cpuTime: int
    priority: int
    # Bounds
    minCPUTime: int
    maxCPUTime: int
    allowedJobTypes: list[str]
    maxInputDataFiles: int
    minNumberOfProcessors: int
    maxNumberOfProcessors: int
    minPriority: int
    maxPriority: int
    possibleLogLevels: list[str]
    sites: DErrorReturnType[list[str]]


class BaseJobDescriptionModel(BaseModel):
    """Base model for the job description (not parametric)"""

    model_config = ConfigDict(validate_assignment=True)

    # This must be overridden in subclasses
    _config_builder: ClassVar[Callable[[], BaseJobDescriptionModelConfg] | None] = None

    @model_validator(mode="before")
    def injectDefaultValues(cls, values: dict[str, Any]) -> dict[str, Any]:
        if cls._config_builder is None:
            raise NotImplementedError("You must define a _config_builder class attribute")
        config = cls._config_builder()
        values.setdefault("cpuTime", config["cpuTime"])
        values.setdefault("priority", config["priority"])
        return values

    arguments: str = ""
    bannedSites: CoercibleSetStr = set()
    # TODO: This should use a field factory
    cpuTime: int
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
    priority: int
    sites: CoercibleSetStr = set()
    stderr: str = "std.err"
    stdout: str = "std.out"
    tags: CoercibleSetStr = set()
    extraFields: dict[str, Any] = {}

    @field_validator("cpuTime")
    def checkCPUTimeBounds(cls, v):
        minCPUTime = cls._config_builder()["minCPUTime"]
        maxCPUTime = cls._config_builder()["maxCPUTime"]
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
        allowedTypes = cls._config_builder()["allowedJobTypes"]
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
            maxInputDataFiles = self._config_builder()["maxInputDataFiles"]
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
        possibleLogLevels = cls._config_builder()["possibleLogLevels"]
        if v not in possibleLogLevels:
            raise ValueError(f"Log level {v} not in {possibleLogLevels}")
        return v

    @field_validator("minNumberOfProcessors")
    def checkMinNumberOfProcessorsBounds(cls, v):
        minNumberOfProcessors = cls._config_builder()["minNumberOfProcessors"]
        maxNumberOfProcessors = cls._config_builder()["maxNumberOfProcessors"]
        if not minNumberOfProcessors <= v <= maxNumberOfProcessors:
            raise ValueError(
                f"minNumberOfProcessors out of bounds (must be between {minNumberOfProcessors} and {maxNumberOfProcessors})"
            )
        return v

    @field_validator("maxNumberOfProcessors")
    def checkMaxNumberOfProcessorsBounds(cls, v):
        minNumberOfProcessors = cls._config_builder()["minNumberOfProcessors"]
        maxNumberOfProcessors = cls._config_builder()["maxNumberOfProcessors"]
        if not minNumberOfProcessors <= v <= maxNumberOfProcessors:
            raise ValueError(
                f"maxNumberOfProcessors out of bounds (must be between {minNumberOfProcessors} and {maxNumberOfProcessors})"
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
            res = cls._config_builder()["sites"]
            if not res["OK"]:
                raise ValueError(res["Message"])
            invalidSites = v - set(res["Value"]).union({"ANY"})
            if invalidSites:
                raise ValueError(f"Invalid sites: {' '.join(invalidSites)}")
        return v

    @model_validator(mode="after")
    def checkThatSitesAndBannedSitesAreNotMutuallyExclusive(self) -> Self:
        if self.sites and self.bannedSites:
            while self.bannedSites:
                self.sites.discard(self.bannedSites.pop())
            if not self.sites:
                raise ValueError("sites and bannedSites are mutually exclusive")
        return self

    @field_validator("priority")
    def checkPriorityBounds(cls, v):
        minPriority = cls._config_builder()["minPriority"]
        maxPriority = cls._config_builder()["maxPriority"]
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
