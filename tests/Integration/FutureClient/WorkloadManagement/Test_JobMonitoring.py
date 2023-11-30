from functools import partial

import pytest

import DIRAC

DIRAC.initialize()
from DIRAC.WorkloadManagementSystem.Client.JobMonitoringClient import JobMonitoringClient
from ..utils import compare_results

TEST_JOBS = [7470, 7471, 7469]
TEST_JOB_IDS = [TEST_JOBS] + TEST_JOBS + [str(x) for x in TEST_JOBS]


def test_getApplicationStates():
    # JobMonitoringClient().getApplicationStates(condDict = None, older = None, newer = None)
    method = JobMonitoringClient().getApplicationStates
    pytest.skip()


def test_getAtticJobParameters():
    # JobMonitoringClient().getAtticJobParameters(jobID: int, parameters = None, rescheduleCycle = -1)
    method = JobMonitoringClient().getAtticJobParameters
    pytest.skip()


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


def test_getJobGroups():
    # JobMonitoringClient().getJobGroups(condDict = None, older = None, cutDate = None)
    method = JobMonitoringClient().getJobGroups
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


def test_getJobOptParameters():
    # JobMonitoringClient().getJobOptParameters(jobID: int)
    method = JobMonitoringClient().getJobOptParameters
    pytest.skip()


def test_getJobOwner():
    # JobMonitoringClient().getJobOwner(jobID: int)
    method = JobMonitoringClient().getJobOwner
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


def test_getJobSite():
    # JobMonitoringClient().getJobSite(jobID: int)
    method = JobMonitoringClient().getJobSite
    pytest.skip()


def test_getJobStats():
    # JobMonitoringClient().getJobStats(attribute: str, selectDict: dict)
    method = JobMonitoringClient().getJobStats
    pytest.skip()


def test_getJobSummary():
    # JobMonitoringClient().getJobSummary(jobID: int)
    method = JobMonitoringClient().getJobSummary
    pytest.skip()


def test_getJobTypes():
    # JobMonitoringClient().getJobTypes(condDict = None, older = None, newer = None)
    method = JobMonitoringClient().getJobTypes
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


def test_getJobsParameters():
    # JobMonitoringClient().getJobsParameters(jobIDs: str | int | list, parameters: list)
    method = JobMonitoringClient().getJobsParameters
    pytest.skip()


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


def test_getMinorStates():
    # JobMonitoringClient().getMinorStates(condDict = None, older = None, newer = None)
    method = JobMonitoringClient().getMinorStates
    pytest.skip()


def test_getOwnerGroup():
    # JobMonitoringClient().getOwnerGroup()
    method = JobMonitoringClient().getOwnerGroup
    pytest.skip()


def test_getOwners():
    # JobMonitoringClient().getOwners(condDict = None, older = None, newer = None)
    method = JobMonitoringClient().getOwners
    pytest.skip()


def test_getSiteSummary():
    # JobMonitoringClient().getSiteSummary()
    method = JobMonitoringClient().getSiteSummary
    pytest.skip()


def test_getSites():
    # JobMonitoringClient().getSites(condDict = None, older = None, newer = None)
    method = JobMonitoringClient().getSites
    pytest.skip()


def test_getStates():
    # JobMonitoringClient().getStates(condDict = None, older = None, newer = None)
    method = JobMonitoringClient().getStates
    pytest.skip()
