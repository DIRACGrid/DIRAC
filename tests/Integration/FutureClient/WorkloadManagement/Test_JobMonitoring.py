from functools import partial

import pytest

import DIRAC

DIRAC.initialize()
from DIRAC.WorkloadManagementSystem.Client.JobMonitoringClient import JobMonitoringClient
from ..utils import compare_results

TEST_JOBS = [7470, 7471, 7469]
TEST_JOB_IDS = [TEST_JOBS] + TEST_JOBS + [str(x) for x in TEST_JOBS]


def test_getCounters():
    # JobMonitoringClient().getCounters(attrList: list, attrDict = None, cutDate = )
    method = JobMonitoringClient().getCounters
    pytest.skip()


def test_getInputData():
    # JobMonitoringClient().getInputData(jobID: int)
    method = JobMonitoringClient().getInputData
    pytest.skip()


def test_getJobAttribute():
    # JobMonitoringClient().getJobAttribute(jobID: int, attribute: str)
    method = JobMonitoringClient().getJobAttribute
    pytest.skip()


def test_getJobAttributes():
    # JobMonitoringClient().getJobAttributes(jobID: int, attrList = None)
    method = JobMonitoringClient().getJobAttributes
    pytest.skip()


def test_getJobHeartBeatData():
    # JobMonitoringClient().getJobHeartBeatData(jobID: int)
    method = JobMonitoringClient().getJobHeartBeatData
    pytest.skip()


def test_getJobJDL():
    # JobMonitoringClient().getJobJDL(jobID: int, original: bool)
    method = JobMonitoringClient().getJobJDL
    pytest.skip()


def test_getJobLoggingInfo():
    # JobMonitoringClient().getJobLoggingInfo(jobID: int)
    method = JobMonitoringClient().getJobLoggingInfo
    pytest.skip()


def test_getJobPageSummaryWeb():
    # JobMonitoringClient().getJobPageSummaryWeb(self: dict, selectDict: list, sortList: int, startItem: int, maxItems, selectJobs = True)
    method = JobMonitoringClient().getJobPageSummaryWeb
    pytest.skip()


def test_getJobParameter():
    # JobMonitoringClient().getJobParameter(jobID: str | int, parName: str)
    method = JobMonitoringClient().getJobParameter
    pytest.skip()


def test_getJobParameters():
    # JobMonitoringClient().getJobParameters(jobIDs: str | int | list, parName = None)
    method = JobMonitoringClient().getJobParameters
    pytest.skip()


def test_getJobSummary():
    # JobMonitoringClient().getJobSummary(jobID: int)
    method = JobMonitoringClient().getJobSummary
    pytest.skip()


def test_getJobs():
    # JobMonitoringClient().getJobs(attrDict = None, cutDate = None)
    method = JobMonitoringClient().getJobs
    pytest.skip()


@pytest.mark.parametrize("jobIDs", TEST_JOB_IDS)
def test_getJobsApplicationStatus(jobIDs):
    # JobMonitoringClient().getJobsApplicationStatus(jobIDs: str | int | list)
    method = JobMonitoringClient().getJobsApplicationStatus
    compare_results(partial(method, jobIDs))


@pytest.mark.parametrize("jobIDs", TEST_JOB_IDS)
def test_getJobsMinorStatus(jobIDs):
    # JobMonitoringClient().getJobsMinorStatus(jobIDs: str | int | list)
    method = JobMonitoringClient().getJobsMinorStatus
    compare_results(partial(method, jobIDs))


@pytest.mark.parametrize("jobIDs", TEST_JOB_IDS)
def test_getJobsSites(jobIDs):
    # JobMonitoringClient().getJobsSites(jobIDs: str | int | list)
    method = JobMonitoringClient().getJobsSites
    compare_results(partial(method, jobIDs))


@pytest.mark.parametrize("jobIDs", TEST_JOB_IDS)
def test_getJobsStates(jobIDs):
    # JobMonitoringClient().getJobsStates(jobIDs: str | int | list)
    method = JobMonitoringClient().getJobsStates
    compare_results(partial(method, jobIDs))


@pytest.mark.parametrize("jobIDs", TEST_JOB_IDS)
def test_getJobsStatus(jobIDs):
    # JobMonitoringClient().getJobsStatus(jobIDs: str | int | list)
    method = JobMonitoringClient().getJobsStatus
    compare_results(partial(method, jobIDs))


def test_getJobsSummary():
    # JobMonitoringClient().getJobsSummary(jobIDs: list)
    method = JobMonitoringClient().getJobsSummary
    pytest.skip()
