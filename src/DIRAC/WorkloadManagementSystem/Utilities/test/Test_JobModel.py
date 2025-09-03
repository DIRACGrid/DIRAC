"""Test the JobModel class and its validators."""

# pylint: disable=invalid-name

from unittest.mock import patch
import pytest
from pydantic import ValidationError

from DIRAC import S_OK
from DIRAC.WorkloadManagementSystem.Utilities.JobModel import BaseJobDescriptionModel, JobDescriptionModel

EXECUTABLE = "dirac-jobexec"
OWNER = "owner"
OWNERGROUP = "ownerGroup"
VO = "vo"


@pytest.mark.parametrize(
    "cpuTime",
    [
        100,  # Lower bound
        86400,  # Default
        500000,  # Higher bound
    ],
)
def test_cpuTimeValidator_valid(cpuTime: int):
    """Test the cpuTime validator."""
    BaseJobDescriptionModel(executable=EXECUTABLE, cpuTime=cpuTime)


@pytest.mark.parametrize(
    "cpuTime",
    [
        1,  # Too low
        100000000,  # Too high
        None,  # None
        "qwerty",  # Not an int
    ],
)
def test_cpuTimeValidator_invalid(cpuTime: int):
    """Test the cpuTime validator with invalid input."""
    with pytest.raises(ValidationError):
        BaseJobDescriptionModel(executable=EXECUTABLE, cpuTime=cpuTime)


def test_jobType_valid():
    """Test the jobType validator with valid input."""
    BaseJobDescriptionModel(executable=EXECUTABLE, jobType="User")


def test_jobType_invalid():
    """Test the jobType validator with invalid input."""
    with pytest.raises(ValidationError):
        BaseJobDescriptionModel(executable=EXECUTABLE, jobType="Production")


@pytest.mark.parametrize(
    "priority",
    [
        0,  # Lower bound
        1,  # Default
        10,  # Higher bound
    ],
)
def test_priorityValidator_valid(priority):
    """Test the priority validator with valid input."""
    BaseJobDescriptionModel(
        executable=EXECUTABLE,
        priority=priority,
    )


@pytest.mark.parametrize(
    "priority",
    [
        -1,  # Too low
        11,  # Too high
        None,  # None
        "qwerty",  # Not an int
    ],
)
def test_priorityValidator_invalid(priority):
    """Test the priority validator with invalid input"""
    with pytest.raises(ValidationError):
        BaseJobDescriptionModel(
            executable=EXECUTABLE,
            priority=priority,
        )


@pytest.mark.parametrize(
    "inputData,parsedInputData,jobType",
    [
        ({f"  /{VO}/1", "   "}, {f"LFN:/{VO}/1"}, "User"),
        ({f"/{VO}/1", f"LFN:/{VO}/2"}, {f"LFN:/{VO}/1", f"LFN:/{VO}/2"}, "User"),
        ({f"LFN:/{VO}/1", f"LFN:/{VO}/2"}, {f"LFN:/{VO}/1", f"LFN:/{VO}/2"}, "User"),
        ({f"LFN:/{VO}/{i}" for i in range(1000)}, {f"LFN:/{VO}/{i}" for i in range(1000)}, "Test"),
    ],
)
def test_inputDataValidator_valid(inputData: set[str], parsedInputData: set[str], jobType: str):
    """Test the inputData validator with valid input."""
    job = BaseJobDescriptionModel(executable=EXECUTABLE, inputData=inputData, jobType=jobType)
    assert job.inputData == parsedInputData


@pytest.mark.parametrize(
    "inputData,jobType",
    [
        ({f"SB:/{VO}/1", f"LFN:/{VO}/2"}, "User"),  # Wrong prefix
        ({f"LFN:/{VO}//1"}, "User"),  # Double slash
        ({f"LFN:/{VO}/{i}" for i in range(1000)}, "User"),  # Too many files
    ],
)
def test_inputDataValidator_invalid(inputData: set[str], jobType: str):
    """Test the inputData validator with invalid input."""
    with pytest.raises(ValidationError):
        BaseJobDescriptionModel(executable=EXECUTABLE, inputData=inputData, jobType=jobType)


def test_rootInputDataValidator_valid():
    """Test that the files starts with LFN:/vo/"""
    JobDescriptionModel(
        executable=EXECUTABLE,
        owner=OWNER,
        ownerGroup=OWNERGROUP,
        vo=VO,
        inputData={f"/{VO}/1", f"LFN:/{VO}/2"},
    )


def test_rootInputDataValidator_invalid():
    """Test the validator with no VO set in the inputData path"""
    with pytest.raises(ValidationError):
        JobDescriptionModel(
            executable=EXECUTABLE,
            owner=OWNER,
            ownerGroup=OWNERGROUP,
            vo=VO,
            inputData={f"/{VO}/1", "LFN:/2"},
        )


def test_inputSandboxValidator_valid():
    """Test the inputSandbox validator with valid input."""
    BaseJobDescriptionModel(
        executable=EXECUTABLE,
        inputSandbox={"SB:/file1", "LFN:/file2"},
    )


def test_inputSandboxValidator_invalid():
    """Test the inputSandbox validator with invalid input."""
    with pytest.raises(ValidationError):
        BaseJobDescriptionModel(
            executable=EXECUTABLE,
            inputSandbox={"LFN:"},
        )


@pytest.mark.parametrize(
    "logLevel, parsedLogLevel",
    [("DEBUG", "DEBUG"), ("debug", "DEBUG"), ("INFO", "INFO")],
)
def test_logLevelValidator_valid(logLevel: str, parsedLogLevel: str):
    """Test the logLevel validator with valid input."""
    job = BaseJobDescriptionModel(executable=EXECUTABLE, logLevel=logLevel)
    assert job.logLevel == parsedLogLevel


def test_logLevelValidator_invalid():
    """Test the logLevel validator with invalid input."""
    with pytest.raises(ValidationError):
        BaseJobDescriptionModel(executable=EXECUTABLE, logLevel="DEBUGGG")


def test_platformValidator_valid():
    """Test the platform validator with valid input."""
    job = BaseJobDescriptionModel(executable=EXECUTABLE, platform="x86_64-slc6-gcc62-opt")
    assert job.platform == "x86_64-slc6-gcc62-opt"


@pytest.mark.parametrize(
    "validSites, selectedSites",
    [
        ([], None),
        (["LCG.PIC.es", "LCG.CNAF.it", "LCG.IN2P3.fr"], None),
        (["LCG.PIC.es", "LCG.CNAF.it", "LCG.IN2P3.fr"], {"LCG.PIC.es", "LCG.CNAF.it"}),
        (["LCG.PIC.es", "LCG.CNAF.it", "LCG.IN2P3.fr"], {"LCG.PIC.es", "LCG.CNAF.it", "LCG.IN2P3.fr"}),
    ],
)
def test_sitesValidator_valid(validSites: list[str], selectedSites: set[str]):
    """Test the sites validator with valid input."""
    with patch(
        "DIRAC.WorkloadManagementSystem.Utilities.JobModel.getSites",
        return_value=S_OK(validSites),
    ):
        BaseJobDescriptionModel(executable=EXECUTABLE, sites=selectedSites)


@pytest.mark.parametrize(
    "validSites, selectedSites",
    [
        ([], {"LCG.PIC.es"}),
        ([], {"LCG.PIC.es", "LCG.CNAF.it", "LCG.IN2P3.fr"}),
    ],
)
def test_sitesValidator_invalid(validSites, selectedSites):
    """Test the sites validator with invalid input"""
    with patch(
        "DIRAC.WorkloadManagementSystem.Utilities.JobModel.getSites",
        return_value=S_OK(validSites),
    ):
        with pytest.raises(ValidationError):
            BaseJobDescriptionModel(executable=EXECUTABLE, sites=selectedSites)


@pytest.mark.parametrize(
    "sites, bannedSites, parsedSites, parsedBannedSites",
    [
        ({"LCG.PIC.es", "LCG.CNAF.it", "LCG.IN2P3.fr"}, None, {"LCG.PIC.es", "LCG.CNAF.it", "LCG.IN2P3.fr"}, set()),
        (None, {"LCG.PIC.es", "LCG.CNAF.it", "LCG.IN2P3.fr"}, set(), {"LCG.PIC.es", "LCG.CNAF.it", "LCG.IN2P3.fr"}),
        ({"LCG.PIC.es", "LCG.CNAF.it", "LCG.IN2P3.fr"}, {"LCG.PIC.es", "LCG.CNAF.it"}, {"LCG.IN2P3.fr"}, set()),
    ],
)
def test_checkThatSitesAndBannedSitesAreNotMutuallyExclusive_valid(
    sites: set[str], bannedSites: set[str], parsedSites: set[str], parsedBannedSites: set[str]
):
    """Test the sites validator with valid input."""
    with patch(
        "DIRAC.WorkloadManagementSystem.Utilities.JobModel.getSites",
        return_value=S_OK(list(sites) if sites else []),
    ):
        job = BaseJobDescriptionModel(executable=EXECUTABLE, sites=sites, bannedSites=bannedSites)
        assert job.sites == parsedSites
        assert job.bannedSites == parsedBannedSites


def test_checkThatSitesAndBannedSitesAreNotMutuallyExclusive_invalid():
    """Test the sites validator with invalid input"""
    with patch(
        "DIRAC.WorkloadManagementSystem.Utilities.JobModel.getSites",
        return_value=S_OK(["LCG.PIC.es", "LCG.CNAF.it"]),
    ):
        with pytest.raises(ValidationError):
            BaseJobDescriptionModel(
                executable=EXECUTABLE, sites={"LCG.PIC.es", "LCG.CNAF.it"}, bannedSites={"LCG.PIC.es", "LCG.CNAF.it"}
            )
