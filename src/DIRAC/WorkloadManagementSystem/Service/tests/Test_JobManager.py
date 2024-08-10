""" unit test (pytest) of JobManager service
"""

from unittest.mock import MagicMock
import pytest

from DIRAC import gLogger

gLogger.setLevel("DEBUG")

from DIRAC.WorkloadManagementSystem.Service.JobPolicy import (
    RIGHT_DELETE,
    RIGHT_KILL,
)

# sut
from DIRAC.WorkloadManagementSystem.Service.JobManagerHandler import JobManagerHandlerMixin

# mocks
jobPolicy_mock = MagicMock()
jobDB_mock = MagicMock()
jobDB_mock.getJobsAttributes.return_value = {"OK": True, "Value": {}}


@pytest.mark.parametrize(
    "jobIDs_list, right, lists, filteredJobsList, expected_res, expected_value",
    [
        ([], RIGHT_KILL, ([], [], [], []), [], True, []),
        ([], RIGHT_DELETE, ([], [], [], []), [], True, []),
        (1, RIGHT_KILL, ([], [], [], []), [], True, []),
        (1, RIGHT_KILL, ([1], [], [], []), [], True, []),
        ([1, 2], RIGHT_KILL, ([], [], [], []), [], True, []),
        ([1, 2], RIGHT_KILL, ([1], [], [], []), [], True, []),
        (1, RIGHT_KILL, ([1], [], [], []), [1], True, [1]),
        ([1, 2], RIGHT_KILL, ([1], [], [], []), [1], True, [1]),
        ([1, 2], RIGHT_KILL, ([1], [2], [], []), [1], True, [1]),
        ([1, 2], RIGHT_KILL, ([1], [2], [], []), [], True, []),
        ([1, 2], RIGHT_KILL, ([1, 2], [], [], []), [1, 2], True, [1, 2]),
    ],
)
def test___kill_delete_jobs(mocker, jobIDs_list, right, lists, filteredJobsList, expected_res, expected_value):
    mocker.patch(
        "DIRAC.WorkloadManagementSystem.Service.JobManagerHandler.filterJobStateTransition",
        return_value={"OK": True, "Value": filteredJobsList},
    )

    JobManagerHandlerMixin.log = gLogger
    JobManagerHandlerMixin.jobPolicy = jobPolicy_mock
    JobManagerHandlerMixin.jobDB = jobDB_mock
    JobManagerHandlerMixin.taskQueueDB = MagicMock()

    jobPolicy_mock.evaluateJobRights.return_value = lists

    jm = JobManagerHandlerMixin()

    res = jm._kill_delete_jobs(jobIDs_list, right)
    assert res["OK"] == expected_res
    assert res["Value"] == expected_value
