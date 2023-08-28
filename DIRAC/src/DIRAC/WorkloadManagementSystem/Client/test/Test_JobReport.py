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
