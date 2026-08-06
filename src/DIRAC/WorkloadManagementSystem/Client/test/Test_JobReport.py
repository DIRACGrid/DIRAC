"""Test for JobReport"""
# pylint: disable=missing-docstring

from unittest.mock import MagicMock

# sut
from DIRAC.WorkloadManagementSystem.Client.JobReport import JobReport


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


def test_jobReportDropsNonFiniteParameters(mocker):
    """Non-finite floats cannot be represented in JSON nor stored in the backends."""
    mocker.patch("DIRAC.WorkloadManagementSystem.Client.JobStateUpdateClient", side_effect=MagicMock())

    jr = JobReport(123)
    res = jr.setJobParameter("LoadAverage", float("nan"), sendFlag=False)
    assert res["OK"]
    res = jr.setJobParameters(
        [("MemoryUsed(MB)", float("inf")), ("DiskSpace(MB)", float("-inf")), ("CPUNormalizationFactor", 9.5)],
        sendFlag=False,
    )
    assert res["OK"]
    assert jr.jobParameters == [("CPUNormalizationFactor", 9.5)]
