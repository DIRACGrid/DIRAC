from mock import MagicMock
import pytest
from DIRAC.Core.Utilities.ClassAd import ClassAd

from DIRAC.WorkloadManagementSystem.Utilities.JobDescriptionUtility import (
    resolveCpuTime,
    resolvePriority,
    resolveSites,
    resolveTags,
)


@pytest.mark.parametrize(
    "manifestOptions, expected",
    [
        ({}, []),
        ({"Tags": "bof"}, ["bof"]),
        ({"Tags": "bof, bif"}, ["bof", "bif"]),
        ({"MaxRAM": 2}, ["2GB"]),
        ({"Tags": "bof, bif", "MaxRAM": 2}, ["bof", "bif", "2GB"]),
        ({"WholeNode": "yes", "MaxRAM": 2}, ["WholeNode", "MultiProcessor", "2GB"]),
        ({"NumberOfProcessors": 1}, []),
        ({"NumberOfProcessors": 4}, ["MultiProcessor", "4Processors"]),
        ({"NumberOfProcessors": 4, "MinNumberOfProcessors": 2}, ["MultiProcessor", "4Processors"]),
        ({"NumberOfProcessors": 4, "MaxNumberOfProcessors": 12}, ["MultiProcessor", "4Processors"]),
        ({"NumberOfProcessors": 4, "MaxNumberOfProcessors": 12}, ["MultiProcessor", "4Processors"]),
        ({"MinNumberOfProcessors": 4, "MaxNumberOfProcessors": 12}, ["MultiProcessor", "4Processors"]),
        ({"MinNumberOfProcessors": 4, "MaxNumberOfProcessors": 4}, ["MultiProcessor", "4Processors"]),
        ({"MinNumberOfProcessors": 4}, ["MultiProcessor", "4Processors"]),
    ],
)
def test_resolveTags(manifestOptions: dict, expected: list):
    # Arrange
    jobDescription = ClassAd("[]")
    for varName, varValue in manifestOptions.items():
        if varName in {"Tags", "WholeNode"}:
            jobDescription.insertAttributeString(varName, varValue)
        else:
            jobDescription.insertAttributeInt(varName, varValue)

    # Act
    resolveTags(jobDescription)

    # Assert
    if expected:
        assert jobDescription.lookupAttribute("Tags") is True
        assert set(jobDescription.getListFromExpression("Tags")) == set(expected)
    else:
        assert jobDescription.lookupAttribute("Tags") is False


@pytest.mark.parametrize(
    "priority, expected",
    [(None, 1), (-1000, 0), (0, 0), (1, 1), (5, 5), (10, 10), (1000000000, 10)],
)
def test_resolvePriority(priority: int, expected: int):
    # Arrange
    jobDescription = ClassAd("[]")
    if priority is not None:
        jobDescription.insertAttributeInt("Priority", priority)

    opsHelper = MagicMock()
    if priority is None:
        opsHelper.getValue = MagicMock(return_value=1)
    else:
        opsHelper.getValue = MagicMock(side_effect=[0, 10])

    # Act
    resolvePriority(jobDescription, opsHelper)

    # Assert
    assert jobDescription.lookupAttribute("Priority") is True
    assert jobDescription.getAttributeInt("Priority") == expected


@pytest.mark.parametrize(
    "cpuTime, expected",
    [
        (None, 86400),
        (-1000, 100),
        (0, 100),
        (100, 100),
        (123456, 123456),
        (500000, 500000),
        (1000000000, 500000),
    ],
)
def test_resolveCpuTime(cpuTime: int, expected: int):
    # Arrange
    jobDescription = ClassAd("[]")
    if cpuTime is not None:
        jobDescription.insertAttributeInt("CPUTime", cpuTime)

    opsHelper = MagicMock()
    if cpuTime is None:
        opsHelper.getValue = MagicMock(return_value=86400)
    else:
        opsHelper.getValue = MagicMock(side_effect=[100, 500000])

    # Act
    resolveCpuTime(jobDescription, opsHelper)

    # Assert
    assert jobDescription.lookupAttribute("CPUTime") is True
    assert jobDescription.getAttributeInt("CPUTime") == expected


@pytest.mark.parametrize(
    "sites, present",
    [(None, False), ("", False), ("any", False), ("Any", False), ("ANY", False), ("bar", True)],
)
def test_resolveSites(sites: str, present: bool):

    # Arrange
    jobDescription = ClassAd("[]")
    if sites is not None:
        jobDescription.insertAttributeString("Sites", sites)

    # Act
    resolveSites(jobDescription)

    # Assert
    assert jobDescription.lookupAttribute("Sites") is present
