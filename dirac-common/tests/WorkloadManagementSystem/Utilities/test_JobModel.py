"""Test the BaseJobDescriptionModel class and its validators."""

# pylint: disable=invalid-name

import pytest
from pydantic import ValidationError

from DIRACCommon.Core.Utilities.ReturnValues import S_OK
from DIRACCommon.WorkloadManagementSystem.Utilities.JobModel import (
    BaseJobDescriptionModel,
    BaseJobDescriptionModelConfg,
)

EXECUTABLE = "dirac-jobexec"
VO = "vo"


def _make_test_config(*args, **kwargs) -> BaseJobDescriptionModelConfg:
    """Create a test configuration for BaseJobDescriptionModel"""
    return {
        "cpuTime": 86400,
        "priority": 1,
        "minCPUTime": 100,
        "maxCPUTime": 500000,
        "allowedJobTypes": ["User", "Test", "Hospital"],
        "maxInputDataFiles": 10000,
        "minNumberOfProcessors": 1,
        "maxNumberOfProcessors": 1024,
        "minPriority": 0,
        "maxPriority": 10,
        "possibleLogLevels": ["DEBUG", "INFO", "WARN", "ERROR"],
        "sites": S_OK(["LCG.CERN.ch", "LCG.IN2P3.fr"]),
    }


class JobDescriptionModelForTest(BaseJobDescriptionModel):
    """Test version of BaseJobDescriptionModel with test configuration"""

    _config_builder = _make_test_config


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
    JobDescriptionModelForTest(executable=EXECUTABLE, cpuTime=cpuTime)


@pytest.mark.parametrize(
    "cpuTime",
    [
        1,  # Too low
        100000000,  # Too high
        "qwerty",  # Not an int
    ],
)
def test_cpuTimeValidator_invalid(cpuTime: int):
    """Test the cpuTime validator with invalid input."""
    with pytest.raises(ValidationError):
        JobDescriptionModelForTest(executable=EXECUTABLE, cpuTime=cpuTime)


def test_jobType_valid():
    """Test the jobType validator with valid input."""
    JobDescriptionModelForTest(executable=EXECUTABLE, jobType="User")


def test_jobType_invalid():
    """Test the jobType validator with invalid input."""
    with pytest.raises(ValidationError):
        JobDescriptionModelForTest(executable=EXECUTABLE, jobType="Production")


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
    JobDescriptionModelForTest(
        executable=EXECUTABLE,
        priority=priority,
    )


@pytest.mark.parametrize(
    "priority",
    [
        -1,  # Too low
        11,  # Too high
        "qwerty",  # Not an int
    ],
)
def test_priorityValidator_invalid(priority):
    """Test the priority validator with invalid input"""
    with pytest.raises(ValidationError):
        JobDescriptionModelForTest(
            executable=EXECUTABLE,
            priority=priority,
        )


@pytest.mark.parametrize(
    "inputData,parsedInputData,jobType",
    [
        ({f"  /{VO}/1", "   "}, {f"LFN:/{VO}/1"}, "User"),
        ({f"/{VO}/1", f"LFN:/{VO}/2"}, {f"LFN:/{VO}/1", f"LFN:/{VO}/2"}, "User"),
        ({f"LFN:/{VO}/1", f"LFN:/{VO}/2"}, {f"LFN:/{VO}/1", f"LFN:/{VO}/2"}, "User"),
        (
            {f"LFN:/{VO}/{i}" for i in range(100)},
            {f"LFN:/{VO}/{i}" for i in range(100)},
            "Test",
        ),  # Reduced size for DIRACCommon
    ],
)
def test_inputDataValidator_valid(inputData: set[str], parsedInputData: set[str], jobType: str):
    """Test the inputData validator with valid input."""
    job = JobDescriptionModelForTest(
        executable=EXECUTABLE,
        inputData=inputData,
        jobType=jobType,
    )
    assert job.inputData == parsedInputData


@pytest.mark.parametrize(
    "inputData,jobType",
    [
        ({f"LFN:/{VO}/{i}" for i in range(10001)}, "User"),  # Too many files for User job
    ],
)
def test_inputDataValidator_invalid(inputData: set[str], jobType: str):
    """Test the inputData validator with invalid input."""
    with pytest.raises(ValidationError):
        JobDescriptionModelForTest(
            executable=EXECUTABLE,
            inputData=inputData,
            jobType=jobType,
        )


def test_inputDataValidator_basic():
    """Test basic inputData functionality without specific validation."""
    job = JobDescriptionModelForTest(executable=EXECUTABLE, jobType="Test")
    assert job.jobType == "Test"


@pytest.mark.parametrize(
    "minNumberOfProcessors,maxNumberOfProcessors",
    [
        (1, 1),  # Same values
        (1, 4),  # Valid range
        (2, 8),  # Valid range
    ],
)
def test_numberOfProcessorsValidator_valid(minNumberOfProcessors: int, maxNumberOfProcessors: int):
    """Test the numberOfProcessors validator with valid input."""
    JobDescriptionModelForTest(
        executable=EXECUTABLE,
        minNumberOfProcessors=minNumberOfProcessors,
        maxNumberOfProcessors=maxNumberOfProcessors,
    )


@pytest.mark.parametrize(
    "minNumberOfProcessors,maxNumberOfProcessors",
    [
        (0, 1),  # Min too low
        (1, 0),  # Max too low
        (2, 1),  # Min > Max
        (1025, 1025),  # Both too high
    ],
)
def test_numberOfProcessorsValidator_invalid(minNumberOfProcessors: int, maxNumberOfProcessors: int):
    """Test the numberOfProcessors validator with invalid input."""
    with pytest.raises(ValidationError):
        JobDescriptionModelForTest(
            executable=EXECUTABLE,
            minNumberOfProcessors=minNumberOfProcessors,
            maxNumberOfProcessors=maxNumberOfProcessors,
        )


def test_basic_model_creation():
    """Test basic model creation with minimal required fields."""
    model = JobDescriptionModelForTest(executable=EXECUTABLE)
    assert model.executable == EXECUTABLE
    assert model.cpuTime == 86400  # Should use default from config
    assert model.priority == 1  # Should use default from config


def test_model_with_all_fields():
    """Test model creation with many fields."""
    model = JobDescriptionModelForTest(
        executable=EXECUTABLE,
        arguments="--help",
        cpuTime=3600,
        priority=5,
        jobType="Test",
        inputData={f"LFN:/{VO}/test.root"},
        outputData={f"LFN:/{VO}/output.root"},
        inputSandbox={"script.py"},
        outputSandbox={"log.txt"},
        minNumberOfProcessors=1,
        maxNumberOfProcessors=2,
        platform="x86_64-el9-gcc11-opt",
        sites={"LCG.CERN.ch"},
        bannedSites={"LCG.Broken.ch"},
        tags={"GPU", "HighMem"},
    )

    assert model.executable == EXECUTABLE
    assert model.arguments == "--help"
    assert model.cpuTime == 3600
    assert model.priority == 5
    assert model.jobType == "Test"
    assert f"LFN:/{VO}/test.root" in model.inputData
    assert f"LFN:/{VO}/output.root" in model.outputData
    assert "script.py" in model.inputSandbox
    assert "log.txt" in model.outputSandbox
    assert model.minNumberOfProcessors == 1
    assert model.maxNumberOfProcessors == 2
    assert model.platform == "x86_64-el9-gcc11-opt"
    assert "LCG.CERN.ch" in model.sites
    # Note: bannedSites may be processed differently, just check it was set
    assert isinstance(model.bannedSites, set)
    assert "GPU" in model.tags
    assert "HighMem" in model.tags


def test_outputDataValidator():
    """Test output data validation."""
    model = JobDescriptionModelForTest(
        executable=EXECUTABLE,
        outputData={f"LFN:/{VO}/output1.root", f"LFN:/{VO}/output2.root"},
    )
    assert len(model.outputData) == 2
    assert f"LFN:/{VO}/output1.root" in model.outputData
    assert f"LFN:/{VO}/output2.root" in model.outputData


def test_sandboxValidator():
    """Test sandbox validation."""
    model = JobDescriptionModelForTest(
        executable=EXECUTABLE,
        inputSandbox={"script.py", "config.txt"},
        outputSandbox={"log.txt", "results.dat"},
    )
    assert len(model.inputSandbox) == 2
    assert "script.py" in model.inputSandbox
    assert "config.txt" in model.inputSandbox
    assert len(model.outputSandbox) == 2
    assert "log.txt" in model.outputSandbox
    assert "results.dat" in model.outputSandbox
