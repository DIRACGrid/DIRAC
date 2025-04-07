from datetime import datetime
from functools import partial
import os
import time

import pytest

import DIRAC

DIRAC.initialize()
from DIRAC.Core.Utilities.ReturnValues import returnValueOrRaise
from DIRAC.WorkloadManagementSystem.Client.JobStateUpdateClient import JobStateUpdateClient
from DIRAC.WorkloadManagementSystem.Client.JobMonitoringClient import JobMonitoringClient
from DIRAC.WorkloadManagementSystem.Client.JobManagerClient import JobManagerClient
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


def test_sendHeartBeat(monkeypatch, example_jobids):
    # JobStateUpdateClient().sendHeartBeat(jobID: str | int, dynamicData: dict, staticData: dict)
    heartBeatDict = {
        "LoadAverage": float(os.getloadavg()[0]),
        "MemoryUsed": 123.4,
        "Vsize": 1024.0,
        "AvailableDiskSpace": 1024.6,
        "CPUConsumed": 123.456,
        "WallClockTime": 123.456,
    }
    staticParamDict = {"StandardOutput": "recentStdOut"}

    # Give the optimisers a chance to do their thing
    time.sleep(5)

    # Force the job to be in the Running state
    args = ("", "Future Tests", None, True)
    returnValueOrRaise(JobStateUpdateClient().setJobStatus(example_jobids[0], "Running", *args))
    returnValueOrRaise(JobStateUpdateClient().setJobStatus(example_jobids[1], "Running", *args))

    # Check that the heartbeat is sent correctly
    method = JobStateUpdateClient().sendHeartBeat
    test_func1 = partial(method, example_jobids[0], heartBeatDict, staticParamDict)
    test_func2 = partial(method, example_jobids[1], heartBeatDict, staticParamDict)
    future_result, _ = compare_results2(monkeypatch, test_func1, test_func2)
    assert list(returnValueOrRaise(future_result)) == []

    # Check the heartbeat data
    data0 = returnValueOrRaise(JobMonitoringClient().getJobHeartBeatData(example_jobids[0]))
    data1 = returnValueOrRaise(JobMonitoringClient().getJobHeartBeatData(example_jobids[1]))
    # Instead of doing asssert data0 == data1, we check that the values are the
    # same as the third value is a timestamp which might be different
    for (k1, v1, _), (k2, v2, _) in zip(data0, data1, strict=True):
        assert k1 == k2
        assert v1 == v2

    result = returnValueOrRaise(JobMonitoringClient().getJobParameters(example_jobids))
    result[example_jobids[0]].pop("timestamp")
    result[example_jobids[0]].pop("JobID")
    result[example_jobids[1]].pop("timestamp")
    result[example_jobids[1]].pop("JobID")
    assert result[example_jobids[0]] == result[example_jobids[1]]

    # Wait a few seconds so the next heartbeat is different
    time.sleep(3)

    # Send kill command and check again
    returnValueOrRaise(JobManagerClient().killJob(example_jobids[0]))
    returnValueOrRaise(JobManagerClient().killJob(example_jobids[1]))

    # Check that the heartbeat is sent correctly the second time
    future_result, _ = compare_results2(monkeypatch, test_func1, test_func2)
    assert list(returnValueOrRaise(future_result)) == ["Kill"]

    data2 = returnValueOrRaise(JobMonitoringClient().getJobHeartBeatData(example_jobids[0]))
    data3 = returnValueOrRaise(JobMonitoringClient().getJobHeartBeatData(example_jobids[1]))
    # Instead of doing asssert data0 == data1, we check that the values are the
    # same as the third value is a timestamp which might be different
    for (k1, v1, _), (k2, v2, _) in zip(data0, data1, strict=True):
        assert k1 == k2
        assert v1 == v2

    result = returnValueOrRaise(JobMonitoringClient().getJobParameters(example_jobids))
    result[example_jobids[0]].pop("timestamp")
    result[example_jobids[0]].pop("JobID")
    result[example_jobids[1]].pop("timestamp")
    result[example_jobids[1]].pop("JobID")
    assert result[example_jobids[0]] == result[example_jobids[1]]

    assert len(data2) > len(data0)
    assert len(data3) > len(data1)


def test_setJobApplicationStatus(monkeypatch, example_jobids):
    # JobStateUpdateClient().setJobApplicationStatus(jobID: str | int, appStatus: str, source: str = Unknown)
    method = JobStateUpdateClient().setJobApplicationStatus
    args = ["Killed"]
    test_func1 = partial(method, example_jobids[0], *args)
    test_func2 = partial(method, example_jobids[1], *args)
    compare_results2(monkeypatch, test_func1, test_func2)

    result = returnValueOrRaise(JobMonitoringClient().getJobsStates(example_jobids))
    assert result[example_jobids[0]] == result[example_jobids[1]]


@pytest.mark.parametrize("args", [["Status", "Killed"], ["JobGroup", "newJobGroup"]])
def test_setJobAttribute(monkeypatch, example_jobids, args):
    # JobStateUpdateClient().setJobAttribute(jobID: str | int, attribute: str, value: str)
    method = JobStateUpdateClient().setJobAttribute
    test_func1 = partial(method, example_jobids[0], *args)
    test_func2 = partial(method, example_jobids[1], *args)
    compare_results2(monkeypatch, test_func1, test_func2)

    result = returnValueOrRaise(JobMonitoringClient().getJobsStates(example_jobids))
    assert result[example_jobids[0]] == result[example_jobids[1]]


def test_setJobFlag(monkeypatch, example_jobids):
    # JobStateUpdateClient().setJobFlag(jobID: str | int, flag: str)
    assert returnValueOrRaise(JobMonitoringClient().getJobAttribute(example_jobids[0], "AccountedFlag")) == "False"
    assert returnValueOrRaise(JobMonitoringClient().getJobAttribute(example_jobids[1], "AccountedFlag")) == "False"

    method = JobStateUpdateClient().setJobFlag
    test_func1 = partial(method, example_jobids[0], "AccountedFlag")
    test_func2 = partial(method, example_jobids[1], "AccountedFlag")
    compare_results2(monkeypatch, test_func1, test_func2)

    assert returnValueOrRaise(JobMonitoringClient().getJobAttribute(example_jobids[0], "AccountedFlag")) == "True"
    assert returnValueOrRaise(JobMonitoringClient().getJobAttribute(example_jobids[1], "AccountedFlag")) == "True"


def test_unsetJobFlag(monkeypatch, example_jobids):
    # JobStateUpdateClient().unsetJobFlag(jobID: str | int, flag: str)
    assert returnValueOrRaise(JobMonitoringClient().getJobAttribute(example_jobids[0], "AccountedFlag")) == "False"
    assert returnValueOrRaise(JobMonitoringClient().getJobAttribute(example_jobids[1], "AccountedFlag")) == "False"

    method = JobStateUpdateClient().setJobFlag
    test_func1 = partial(method, example_jobids[0], "AccountedFlag")
    test_func2 = partial(method, example_jobids[1], "AccountedFlag")
    compare_results2(monkeypatch, test_func1, test_func2)

    assert returnValueOrRaise(JobMonitoringClient().getJobAttribute(example_jobids[0], "AccountedFlag")) == "True"
    assert returnValueOrRaise(JobMonitoringClient().getJobAttribute(example_jobids[1], "AccountedFlag")) == "True"

    method = JobStateUpdateClient().unsetJobFlag
    test_func1 = partial(method, example_jobids[0], "AccountedFlag")
    test_func2 = partial(method, example_jobids[1], "AccountedFlag")
    compare_results2(monkeypatch, test_func1, test_func2)

    assert returnValueOrRaise(JobMonitoringClient().getJobAttribute(example_jobids[0], "AccountedFlag")) == "False"
    assert returnValueOrRaise(JobMonitoringClient().getJobAttribute(example_jobids[1], "AccountedFlag")) == "False"


@pytest.mark.parametrize(
    "args",
    [
        [
            "PendingRequest",
            "Name:00281689_00036158_job_1085017579\n"
            "LogUpload:LogUpload:Waiting:0:LogSE-EOS:/lhcb/MC/2012/LOG/00281689/0003/00036158.zip\n"
            "RemoveFile:RemoveFile:Queued:1:RAL-FAILOVER:/lhcb/MC/2012/LOG/00281689/0003/00036158.zip\n",
        ],
    ],
)
def test_setJobParameter(monkeypatch, example_jobids, args):
    # JobStateUpdateClient().setJobParameter(jobID: str | int, name: str, value: str)
    assert not returnValueOrRaise(JobMonitoringClient().getJobParameter(example_jobids[0], args[0]))
    assert not returnValueOrRaise(JobMonitoringClient().getJobParameter(example_jobids[1], args[0]))

    method = JobStateUpdateClient().setJobParameter
    test_func1 = partial(method, example_jobids[0], *args)
    test_func2 = partial(method, example_jobids[1], *args)
    compare_results2(monkeypatch, test_func1, test_func2)

    assert returnValueOrRaise(JobMonitoringClient().getJobParameter(example_jobids[0], args[0]))[args[0]] == args[1]
    assert returnValueOrRaise(JobMonitoringClient().getJobParameter(example_jobids[1], args[0]))[args[0]] == args[1]


@pytest.mark.parametrize(
    "args",
    [
        [[("par1", "par1Value"), ("par2", "par2Value")]],
        [[("par2", "par1Value"), ("par1", "par2Value")]],
    ],
)
def test_setJobParameters(monkeypatch, example_jobids, args):
    # JobStateUpdateClient().setJobParameters(jobID: str | int, parameters: list)
    result = returnValueOrRaise(JobMonitoringClient().getJobParameters(example_jobids))
    for key, _ in args[0]:
        assert key not in result[example_jobids[0]]
        assert key not in result[example_jobids[1]]

    method = JobStateUpdateClient().setJobParameters
    test_func1 = partial(method, example_jobids[0], *args)
    test_func2 = partial(method, example_jobids[1], *args)
    compare_results2(monkeypatch, test_func1, test_func2)

    result = returnValueOrRaise(JobMonitoringClient().getJobParameters(example_jobids))
    for key, value in args[0]:
        assert result[example_jobids[0]][key] == value
        assert result[example_jobids[1]][key] == value


@pytest.mark.parametrize("jobid_type", [int, str])
def test_setJobSite(monkeypatch, example_jobids, jobid_type):
    # JobStateUpdateClient().setJobSite(jobID: str | int, site: str)
    assert returnValueOrRaise(JobMonitoringClient().getJobAttribute(example_jobids[0], "Site")) == "ANY"

    method = JobStateUpdateClient().setJobSite
    args = ["LCG.CERN.ch"]
    test_func1 = partial(method, jobid_type(example_jobids[0]), *args)
    test_func2 = partial(method, jobid_type(example_jobids[1]), *args)
    compare_results2(monkeypatch, test_func1, test_func2)

    assert returnValueOrRaise(JobMonitoringClient().getJobAttribute(example_jobids[0], "Site")) == "LCG.CERN.ch"


def test_setJobStatus(monkeypatch, example_jobids):
    # JobStateUpdateClient().setJobStatus(jobID: str | int, status: str = , minorStatus: str = , source: str = Unknown, datetime = None, force = False)
    result = returnValueOrRaise(JobMonitoringClient().getJobsStates(example_jobids))
    assert result[example_jobids[0]] == result[example_jobids[1]]

    method = JobStateUpdateClient().setJobStatus
    args = ["", "My Minor"]
    test_func1 = partial(method, example_jobids[0], *args)
    test_func2 = partial(method, example_jobids[1], *args)
    compare_results2(monkeypatch, test_func1, test_func2)

    result = returnValueOrRaise(JobMonitoringClient().getJobsStates(example_jobids))
    assert result[example_jobids[0]] == result[example_jobids[1]]


def test_setJobStatusBulk(monkeypatch, example_jobids):
    # JobStateUpdateClient().setJobStatusBulk(jobID: str | int, statusDict: dict, force = False)
    result = returnValueOrRaise(JobMonitoringClient().getJobsStates(example_jobids))
    assert result[example_jobids[0]] == result[example_jobids[1]]

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

    result = returnValueOrRaise(JobMonitoringClient().getJobsStates(example_jobids))
    assert result[example_jobids[0]] == result[example_jobids[1]]

    # Strip the timestamp from the logging info and compare
    get_logging_info = lambda jid: returnValueOrRaise(JobMonitoringClient().getJobLoggingInfo(jid))
    info1 = [(a, b, c, e) for a, b, c, _, e in get_logging_info(example_jobids[0])]
    info2 = [(a, b, c, e) for a, b, c, _, e in get_logging_info(example_jobids[1])]
    assert info1 == info2


def test_setJobsParameter(monkeypatch, example_jobids):
    # JobStateUpdateClient().setJobsParameter(jobsParameterDict: dict)
    result = returnValueOrRaise(JobMonitoringClient().getJobParameters(example_jobids))
    assert "Whatever" not in result[example_jobids[0]]
    assert "Whatever" not in result[example_jobids[1]]

    method = JobStateUpdateClient().setJobsParameter
    test_func1 = partial(method, {example_jobids[0]: ["Whatever", "booh"]})
    test_func2 = partial(method, {example_jobids[1]: ["Whatever", "booh"]})
    compare_results2(monkeypatch, test_func1, test_func2)

    result = returnValueOrRaise(JobMonitoringClient().getJobParameters(example_jobids))
    assert result[example_jobids[0]]["Whatever"] == "booh"
    assert result[example_jobids[1]]["Whatever"] == "booh"


@pytest.mark.parametrize(
    "initial_status,callback_status",
    [
        ["Waiting", "Error"],
        ["Waiting", "Done"],
        ["Staging", "Error"],
        ["Staging", "Done"],
    ],
)
def test_updateJobFromStager(monkeypatch, example_jobids, initial_status, callback_status):
    # JobStateUpdateClient().updateJobFromStager(jobID: str | int, status: str)
    method = JobStateUpdateClient().updateJobFromStager
    # Give the optimisers a chance to do their thing
    time.sleep(5)
    # Set the initial job status
    res = JobStateUpdateClient().setJobStatus(example_jobids[0], initial_status, "", "Future Tests", None, True)
    returnValueOrRaise(res)
    returnValueOrRaise(
        JobStateUpdateClient().setJobStatus(example_jobids[1], initial_status, "", "Future Tests", None, True)
    )

    result = returnValueOrRaise(JobMonitoringClient().getJobsStates(example_jobids))
    assert result[example_jobids[0]] == result[example_jobids[1]]

    # Do the test
    test_func1 = partial(method, example_jobids[0], callback_status)
    test_func2 = partial(method, example_jobids[1], callback_status)
    compare_results2(monkeypatch, test_func1, test_func2)

    result = returnValueOrRaise(JobMonitoringClient().getJobsStates(example_jobids))
    assert result[example_jobids[0]] == result[example_jobids[1]]
