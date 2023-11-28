from functools import partial

import pytest

import DIRAC

DIRAC.initialize()
from DIRAC.WorkloadManagementSystem.Client.JobStateUpdateClient import JobStateUpdateClient
from ..utils import compare_results


def test_sendHeartBeat(monkeypatch):
    # JobStateUpdateClient().sendHeartBeat(jobID: str | int, dynamicData: dict, staticData: dict)
    method = JobStateUpdateClient().sendHeartBeat
    pytest.skip()


def test_setJobApplicationStatus(monkeypatch):
    # JobStateUpdateClient().setJobApplicationStatus(jobID: str | int, appStatus: str, source: str = Unknown)
    method = JobStateUpdateClient().setJobApplicationStatus
    pytest.skip()


def test_setJobAttribute(monkeypatch):
    # JobStateUpdateClient().setJobAttribute(jobID: str | int, attribute: str, value: str)
    method = JobStateUpdateClient().setJobAttribute
    pytest.skip()


def test_setJobFlag(monkeypatch):
    # JobStateUpdateClient().setJobFlag(jobID: str | int, flag: str)
    method = JobStateUpdateClient().setJobFlag
    pytest.skip()


def test_setJobParameter(monkeypatch):
    # JobStateUpdateClient().setJobParameter(jobID: str | int, name: str, value: str)
    method = JobStateUpdateClient().setJobParameter
    pytest.skip()


def test_setJobParameters(monkeypatch):
    # JobStateUpdateClient().setJobParameters(jobID: str | int, parameters: list)
    method = JobStateUpdateClient().setJobParameters
    pytest.skip()


def test_setJobSite(monkeypatch):
    # JobStateUpdateClient().setJobSite(jobID: str | int, site: str)
    method = JobStateUpdateClient().setJobSite
    pytest.skip()


def test_setJobStatus(monkeypatch):
    # JobStateUpdateClient().setJobStatus(jobID: str | int, status: str = , minorStatus: str = , source: str = Unknown, datetime = None, force = False)
    method = JobStateUpdateClient().setJobStatus
    pytest.skip()


def test_setJobStatusBulk(monkeypatch):
    # JobStateUpdateClient().setJobStatusBulk(jobID: str | int, statusDict: dict, force = False)
    method = JobStateUpdateClient().setJobStatusBulk
    pytest.skip()


def test_setJobsParameter(monkeypatch):
    # JobStateUpdateClient().setJobsParameter(jobsParameterDict: dict)
    method = JobStateUpdateClient().setJobsParameter
    pytest.skip()


def test_unsetJobFlag(monkeypatch):
    # JobStateUpdateClient().unsetJobFlag(jobID: str | int, flag: str)
    method = JobStateUpdateClient().unsetJobFlag
    pytest.skip()


def test_updateJobFromStager(monkeypatch):
    # JobStateUpdateClient().updateJobFromStager(jobID: str | int, status: str)
    method = JobStateUpdateClient().updateJobFromStager
    pytest.skip()
