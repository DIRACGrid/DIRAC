"""Test for JobReport"""
# pylint: disable=missing-docstring

import decimal
import math
from unittest.mock import MagicMock

import pytest

# sut
from DIRAC.WorkloadManagementSystem.Client.JobReport import JobReport, isFiniteParameterValue


def test_jobReport(mocker):
    mocker.patch("DIRAC.WorkloadManagementSystem.Client.JobStateUpdateClient", side_effect=MagicMock())

    jr = JobReport(123)
    res = jr.setJobStatus("Matched", "minor_matched", "app_matched", sendFlag=False)
    assert res["OK"]
    res = jr.setJobStatus("Running", "minor_running", "app_running", sendFlag=False)
    assert res["OK"]
    res = jr.setJobParameter("par_1", "value_1", sendFlag=False)
    assert res["OK"]
    res = jr.setJobParameter("par_2", "value_2", sendFlag=False)
    assert res["OK"]
    res = jr.setJobParameters([("par_3", "value_3"), ("par_4", "value_4")], sendFlag=False)
    print(jr.jobParameters)
    jr.dump()


@pytest.mark.parametrize(
    "value, expected",
    [
        # finite values of every shape are kept
        (0.0, True),
        (9.5, True),
        (-1, True),
        (True, True),
        (10**400, True),  # too large for a float, but exact and finite
        ("nan", True),  # a string that merely looks like one, e.g. an application message
        ([1.0, 2.0], True),
        ({"a": {"b": [1, 2.5]}}, True),
        (None, True),
        (decimal.Decimal("1.5"), True),
        # non-finite ones are rejected, wherever they sit
        (math.nan, False),
        (math.inf, False),
        (-math.inf, False),
        ([1.0, math.nan], False),
        ((1.0, math.inf), False),
        ({"nested": [math.nan]}, False),
        ({"a": {"b": {"c": -math.inf}}}, False),
        (decimal.Decimal("NaN"), False),
        (decimal.Decimal("Infinity"), False),
    ],
)
def test_isFiniteParameterValue(value, expected):
    """Containers are inspected recursively, mirroring the check on the diracx side."""
    assert isFiniteParameterValue(value) is expected


def test_jobReportDropsNonFiniteParameters(mocker):
    """Non-finite floats cannot be represented in JSON nor stored in the backends."""
    mocker.patch("DIRAC.WorkloadManagementSystem.Client.JobStateUpdateClient", side_effect=MagicMock())

    jr = JobReport(123)
    res = jr.setJobParameter("LoadAverage", math.nan, sendFlag=False)
    assert res["OK"]
    res = jr.setJobParameters(
        [("MemoryUsed(MB)", math.inf), ("DiskSpace(MB)", -math.inf), ("CPUNormalizationFactor", 9.5)],
        sendFlag=False,
    )
    assert res["OK"]
    assert jr.jobParameters == [("CPUNormalizationFactor", 9.5)]


def test_jobReportDropsNonFiniteParametersInContainers(mocker):
    """A single non-finite value nested in a container invalidates the whole payload."""
    mocker.patch("DIRAC.WorkloadManagementSystem.Client.JobStateUpdateClient", side_effect=MagicMock())

    jr = JobReport(123)
    res = jr.setJobParameter("NodeInformation", {"LoadAverage": math.nan}, sendFlag=False)
    assert res["OK"]
    res = jr.setJobParameter("Samples", [1.0, 2.0, math.inf], sendFlag=False)
    assert res["OK"]
    res = jr.setJobParameter("InitialValues", {"DiskSpace": 1024.0}, sendFlag=False)
    assert res["OK"]
    assert jr.jobParameters == [("InitialValues", {"DiskSpace": 1024.0})]
