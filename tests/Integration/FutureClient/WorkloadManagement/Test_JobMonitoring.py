from functools import partial

import pytest

import DIRAC

DIRAC.initialize()
from DIRAC.WorkloadManagementSystem.Client.JobMonitoringClient import JobMonitoringClient
from ..utils import compare_results

MISSING_JOB_ID = 7
TEST_JOBS = [7470, 7471, 7469]
TEST_JOB_IDS = [TEST_JOBS] + TEST_JOBS + [str(TEST_JOBS[0]), MISSING_JOB_ID, TEST_JOBS + [MISSING_JOB_ID]]


@pytest.mark.parametrize(
    "condDict", [{}, None, {"Owner": "chaen"}, {"Owner": "chaen,cburr"}, {"Owner": ["chaen", "cburr"]}]
)
@pytest.mark.parametrize("older", [None, "2023-09-01", "2023-09-01 00:00", "2023-09-01 00:00:00"])
@pytest.mark.parametrize("newer", [None, "2023-09-01", "2023-09-01 00:00:00"])
def test_getApplicationStates(monkeypatch, condDict, older, newer):
    # JobMonitoringClient().getApplicationStates(condDict = None, older = None, newer = None)
    method = JobMonitoringClient().getApplicationStates
    compare_results(monkeypatch, partial(method, condDict, older, newer))


def test_getAtticJobParameters(monkeypatch):
    # JobMonitoringClient().getAtticJobParameters(jobID: int, parameters = None, rescheduleCycle = -1)
    method = JobMonitoringClient().getAtticJobParameters
    pytest.skip()


@pytest.mark.parametrize(
    "attrList",
    [
        # [],
        # ["Owner"],
        # ["Status"],
        # ["Owner", "Status"],
        ["Owner", "Status", "BadAttribute"],
        ["BadAttribute"],
    ],
)
@pytest.mark.parametrize(
    "attrDict", [{}, None, {"Owner": "chaen"}, {"Owner": "chaen,cburr"}, {"Owner": ["chaen", "cburr"]}]
)
@pytest.mark.parametrize("cutDate", [None, "", "2023-09-01", "2023-09-01 00:00:00"])
def test_getCounters(monkeypatch, attrList, attrDict, cutDate):
    # JobMonitoringClient().getCounters(attrList: list, attrDict = None, cutDate = "")
    method = JobMonitoringClient().getCounters
    compare_results(monkeypatch, partial(method, attrList, attrDict, cutDate))


def test_getInputData(monkeypatch):
    # JobMonitoringClient().getInputData(jobID: int)
    method = JobMonitoringClient().getInputData
    pytest.skip()


@pytest.mark.parametrize("jobID", [TEST_JOBS[0], MISSING_JOB_ID])
@pytest.mark.parametrize("attribute", ["JobID", "Status", "Owner", "BadAttribute"])
def test_getJobAttribute(monkeypatch, jobID, attribute):
    # JobMonitoringClient().getJobAttribute(jobID: int, attribute: str)
    method = JobMonitoringClient().getJobAttribute
    compare_results(monkeypatch, partial(method, jobID, attribute))


@pytest.mark.parametrize("jobID", [TEST_JOBS[0], MISSING_JOB_ID])
@pytest.mark.parametrize(
    "attrList",
    [
        ["JobID", "Status", "Owner", "BadAttribute"],
        ["Status"],
        ["BadAttribute"],
        None,
    ],
)
def test_getJobAttributes(monkeypatch, jobID, attrList):
    # JobMonitoringClient().getJobAttributes(jobID: int, attrList = None)
    method = JobMonitoringClient().getJobAttributes
    compare_results(monkeypatch, partial(method, jobID, attrList))


@pytest.mark.parametrize(
    "condDict", [{}, None, {"Owner": "chaen"}, {"Owner": "chaen,cburr"}, {"Owner": ["chaen", "cburr"]}]
)
@pytest.mark.parametrize("older", [None, "2023-09-01", "2023-09-01 00:00:00"])
@pytest.mark.parametrize("cutDate", [None, "2023-09-01", "2023-09-01 00:00:00"])
def test_getJobGroups(monkeypatch, condDict, older, cutDate):
    # JobMonitoringClient().getJobGroups(condDict = None, older = None, cutDate = None)
    method = JobMonitoringClient().getJobGroups
    compare_results(monkeypatch, partial(method, condDict, older, cutDate))


@pytest.mark.parametrize("jobID", [TEST_JOBS[0], MISSING_JOB_ID])
def test_getJobHeartBeatData(monkeypatch, jobID):
    # JobMonitoringClient().getJobHeartBeatData(jobID: int)
    pytest.skip()
    method = JobMonitoringClient().getJobHeartBeatData
    compare_results(monkeypatch, partial(method, jobID))


@pytest.mark.parametrize("jobID", [TEST_JOBS[0], MISSING_JOB_ID])
@pytest.mark.parametrize("original", [True, False])
def test_getJobJDL(monkeypatch, jobID, original):
    # JobMonitoringClient().getJobJDL(jobID: int, original: bool)
    pytest.skip()
    method = JobMonitoringClient().getJobJDL
    compare_results(monkeypatch, partial(method, jobID, original))


def test_getJobLoggingInfo(monkeypatch):
    # JobMonitoringClient().getJobLoggingInfo(jobID: int)
    method = JobMonitoringClient().getJobLoggingInfo
    pytest.skip()


def test_getJobOptParameters(monkeypatch):
    # JobMonitoringClient().getJobOptParameters(jobID: int)
    method = JobMonitoringClient().getJobOptParameters
    pytest.skip()


@pytest.mark.parametrize("jobID", [TEST_JOBS[0], MISSING_JOB_ID])
def test_getJobOwner(monkeypatch, jobID):
    # JobMonitoringClient().getJobOwner(jobID: int)
    method = JobMonitoringClient().getJobOwner
    compare_results(monkeypatch, partial(method, jobID))


def test_getJobPageSummaryWeb(monkeypatch):
    # JobMonitoringClient().getJobPageSummaryWeb(self: dict, selectDict: list, sortList: int, startItem: int, maxItems, selectJobs = True)
    method = JobMonitoringClient().getJobPageSummaryWeb
    pytest.skip()


def test_getJobParameter(monkeypatch):
    # JobMonitoringClient().getJobParameter(jobID: str | int, parName: str)
    method = JobMonitoringClient().getJobParameter
    pytest.skip()


def test_getJobParameters(monkeypatch):
    # JobMonitoringClient().getJobParameters(jobIDs: str | int | list, parName = None)
    method = JobMonitoringClient().getJobParameters
    pytest.skip()


@pytest.mark.parametrize("jobID", [TEST_JOBS[0], MISSING_JOB_ID])
def test_getJobSite(monkeypatch, jobID):
    # JobMonitoringClient().getJobSite(jobID: int)
    method = JobMonitoringClient().getJobSite
    compare_results(monkeypatch, partial(method, jobID))


def test_getJobStats(monkeypatch):
    # JobMonitoringClient().getJobStats(attribute: str, selectDict: dict)
    method = JobMonitoringClient().getJobStats
    pytest.skip()


def test_getJobSummary(monkeypatch):
    # JobMonitoringClient().getJobSummary(jobID: int)
    method = JobMonitoringClient().getJobSummary
    pytest.skip()


@pytest.mark.parametrize(
    "condDict", [{}, None, {"Owner": "chaen"}, {"Owner": "chaen,cburr"}, {"Owner": ["chaen", "cburr"]}]
)
@pytest.mark.parametrize("older", [None, "2023-09-01", "2023-09-01 00:00:00"])
@pytest.mark.parametrize("newer", [None, "2023-09-01", "2023-09-01 00:00:00"])
def test_getJobTypes(monkeypatch, condDict, older, newer):
    # JobMonitoringClient().getJobTypes(condDict = None, older = None, newer = None)
    method = JobMonitoringClient().getJobTypes
    compare_results(monkeypatch, partial(method, condDict, older, newer))


def test_getJobs(monkeypatch):
    # JobMonitoringClient().getJobs(attrDict = None, cutDate = None)
    method = JobMonitoringClient().getJobs
    pytest.skip()


@pytest.mark.parametrize("jobIDs", TEST_JOB_IDS)
def test_getJobsApplicationStatus(monkeypatch, jobIDs):
    # JobMonitoringClient().getJobsApplicationStatus(jobIDs: str | int | list)
    method = JobMonitoringClient().getJobsApplicationStatus
    compare_results(monkeypatch, partial(method, jobIDs))


@pytest.mark.parametrize("jobIDs", TEST_JOB_IDS)
def test_getJobsMinorStatus(monkeypatch, jobIDs):
    # JobMonitoringClient().getJobsMinorStatus(jobIDs: str | int | list)
    method = JobMonitoringClient().getJobsMinorStatus
    compare_results(monkeypatch, partial(method, jobIDs))


def test_getJobsParameters(monkeypatch):
    # JobMonitoringClient().getJobsParameters(jobIDs: str | int | list, parameters: list)
    method = JobMonitoringClient().getJobsParameters
    pytest.skip()


@pytest.mark.parametrize("jobIDs", TEST_JOB_IDS)
def test_getJobsSites(monkeypatch, jobIDs):
    # JobMonitoringClient().getJobsSites(jobIDs: str | int | list)
    method = JobMonitoringClient().getJobsSites
    compare_results(monkeypatch, partial(method, jobIDs))


@pytest.mark.parametrize("jobIDs", TEST_JOB_IDS)
def test_getJobsStates(monkeypatch, jobIDs):
    # JobMonitoringClient().getJobsStates(jobIDs: str | int | list)
    method = JobMonitoringClient().getJobsStates
    compare_results(monkeypatch, partial(method, jobIDs))


@pytest.mark.parametrize("jobIDs", TEST_JOB_IDS)
def test_getJobsStatus(monkeypatch, jobIDs):
    # JobMonitoringClient().getJobsStatus(jobIDs: str | int | list)
    method = JobMonitoringClient().getJobsStatus
    compare_results(monkeypatch, partial(method, jobIDs))


def test_getJobsSummary(monkeypatch):
    # JobMonitoringClient().getJobsSummary(jobIDs: list)
    method = JobMonitoringClient().getJobsSummary
    # TODO: Handle missing case
    compare_results(monkeypatch, partial(method, TEST_JOBS))


@pytest.mark.parametrize(
    "condDict", [{}, None, {"Owner": "chaen"}, {"Owner": "chaen,cburr"}, {"Owner": ["chaen", "cburr"]}]
)
@pytest.mark.parametrize("older", [None, "2023-09-01", "2023-09-01 00:00:00"])
@pytest.mark.parametrize("newer", [None, "2023-09-01", "2023-09-01 00:00:00"])
def test_getMinorStates(monkeypatch, condDict, older, newer):
    # JobMonitoringClient().getMinorStates(condDict = None, older = None, newer = None)
    method = JobMonitoringClient().getMinorStates
    compare_results(monkeypatch, partial(method, condDict, older, newer))


def test_getOwnerGroup(monkeypatch):
    # JobMonitoringClient().getOwnerGroup()
    method = JobMonitoringClient().getOwnerGroup
    compare_results(monkeypatch, method)


@pytest.mark.parametrize(
    "condDict", [{}, None, {"Owner": "chaen"}, {"Owner": "chaen,cburr"}, {"Owner": ["chaen", "cburr"]}]
)
@pytest.mark.parametrize("older", [None, "2023-09-01", "2023-09-01 00:00:00"])
@pytest.mark.parametrize("newer", [None, "2023-09-01", "2023-09-01 00:00:00"])
def test_getOwners(monkeypatch, condDict, older, newer):
    # JobMonitoringClient().getOwners(condDict = None, older = None, newer = None)
    method = JobMonitoringClient().getOwners
    compare_results(monkeypatch, partial(method, condDict, older, newer))


def test_getSiteSummary(monkeypatch):
    # JobMonitoringClient().getSiteSummary()
    method = JobMonitoringClient().getSiteSummary
    compare_results(monkeypatch, method)


@pytest.mark.parametrize(
    "condDict", [{}, None, {"Owner": "chaen"}, {"Owner": "chaen,cburr"}, {"Owner": ["chaen", "cburr"]}]
)
@pytest.mark.parametrize("older", [None, "2023-09-01", "2023-09-01 00:00:00"])
@pytest.mark.parametrize("newer", [None, "2023-09-01", "2023-09-01 00:00:00"])
def test_getSites(monkeypatch, condDict, older, newer):
    # JobMonitoringClient().getSites(condDict = None, older = None, newer = None)
    method = JobMonitoringClient().getSites
    compare_results(monkeypatch, partial(method, condDict, older, newer))


@pytest.mark.parametrize(
    "condDict", [{}, None, {"Owner": "chaen"}, {"Owner": "chaen,cburr"}, {"Owner": ["chaen", "cburr"]}]
)
@pytest.mark.parametrize("older", [None, "2023-09-01", "2023-09-01 00:00:00"])
@pytest.mark.parametrize("newer", [None, "2023-09-01", "2023-09-01 00:00:00"])
def test_getStates(monkeypatch, condDict, older, newer):
    # JobMonitoringClient().getStates(condDict = None, older = None, newer = None)
    method = JobMonitoringClient().getStates
    compare_results(monkeypatch, partial(method, condDict, older, newer))
