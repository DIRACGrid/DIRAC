from datetime import datetime
from functools import partial
from textwrap import dedent

import pytest

import DIRAC

DIRAC.initialize()
from DIRAC.Core.Security.DiracX import DiracXClient
from DIRAC.WorkloadManagementSystem.Client.JobStateUpdateClient import JobStateUpdateClient
from ..utils import compare_results2

test_jdl = """
Arguments = "Hello world from DiracX";
Executable = "echo";
JobGroup = jobGroup;
JobName = jobName;
JobType = User;
LogLevel = INFO;
MinNumberOfProcessors = 1000;
OutputSandbox =
    {
        std.err,
        std.out
    };
Priority = 1;
Sites = ANY;
StdError = std.err;
StdOutput = std.out;
"""


@pytest.fixture()
def example_jobids():
    from DIRAC.Interfaces.API.Dirac import Dirac
    from DIRAC.Core.Utilities.ReturnValues import returnValueOrRaise

    d = Dirac()
    job_id_1 = returnValueOrRaise(d.submitJob(test_jdl))
    job_id_2 = returnValueOrRaise(d.submitJob(test_jdl))
    return job_id_1, job_id_2


def test_sendHeartBeat(monkeypatch):
    # JobStateUpdateClient().sendHeartBeat(jobID: str | int, dynamicData: dict, staticData: dict)
    method = JobStateUpdateClient().sendHeartBeat
    pytest.skip()


def test_setJobApplicationStatus(monkeypatch, example_jobids):
    # JobStateUpdateClient().setJobApplicationStatus(jobID: str | int, appStatus: str, source: str = Unknown)
    method = JobStateUpdateClient().setJobApplicationStatus
    args = ["MyApplicationStatus"]
    test_func1 = partial(method, example_jobids[0], *args)
    test_func2 = partial(method, example_jobids[1], *args)
    compare_results2(monkeypatch, test_func1, test_func2)


@pytest.mark.parametrize("args", [["Status", "Killed"], ["JobGroup", "newJobGroup"]])
def test_setJobAttribute(monkeypatch, example_jobids, args):
    # JobStateUpdateClient().setJobAttribute(jobID: str | int, attribute: str, value: str)
    method = JobStateUpdateClient().setJobAttribute
    test_func1 = partial(method, example_jobids[0], *args)
    test_func2 = partial(method, example_jobids[1], *args)
    compare_results2(monkeypatch, test_func1, test_func2)


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


@pytest.mark.parametrize("jobid_type", [int, str])
def test_setJobSite(monkeypatch, example_jobids, jobid_type):
    # JobStateUpdateClient().setJobSite(jobID: str | int, site: str)
    method = JobStateUpdateClient().setJobSite
    args = ["LCG.CERN.ch"]
    test_func1 = partial(method, jobid_type(example_jobids[0]), *args)
    test_func2 = partial(method, jobid_type(example_jobids[1]), *args)
    compare_results2(monkeypatch, test_func1, test_func2)


def test_setJobStatus(monkeypatch, example_jobids):
    # JobStateUpdateClient().setJobStatus(jobID: str | int, status: str = , minorStatus: str = , source: str = Unknown, datetime = None, force = False)
    method = JobStateUpdateClient().setJobStatus
    args = ["", "My Minor"]
    test_func1 = partial(method, example_jobids[0], *args)
    test_func2 = partial(method, example_jobids[1], *args)
    compare_results2(monkeypatch, test_func1, test_func2)


def test_setJobStatusBulk(monkeypatch, example_jobids):
    # JobStateUpdateClient().setJobStatusBulk(jobID: str | int, statusDict: dict, force = False)
    method = JobStateUpdateClient().setJobStatusBulk
    args = [
        {
            datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S.%f"): {"ApplicationStatus": "SomethingElse"},
            datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S.%f"): {"ApplicationStatus": "Something"},
        }
    ]
    test_func1 = partial(method, example_jobids[0], *args)
    test_func2 = partial(method, example_jobids[1], *args)
    compare_results2(monkeypatch, test_func1, test_func2)


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
