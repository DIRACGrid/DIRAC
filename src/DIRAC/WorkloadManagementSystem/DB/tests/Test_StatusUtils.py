""" unit test (pytest) of JobAdministration module
"""

from unittest.mock import MagicMock

import pytest

# sut
from DIRAC.WorkloadManagementSystem.DB.StatusUtils import kill_delete_jobs


@pytest.mark.parametrize(
    "jobIDs_list, right",
    [
        ([], "Kill"),
        ([], "Delete"),
        (1, "Kill"),
        ([1, 2], "Kill"),
    ],
)
def test___kill_delete_jobs(mocker, jobIDs_list, right):
    mocker.patch("DIRAC.WorkloadManagementSystem.DB.StatusUtils.JobDB", MagicMock())
    mocker.patch("DIRAC.WorkloadManagementSystem.DB.StatusUtils.TaskQueueDB", MagicMock())
    mocker.patch("DIRAC.WorkloadManagementSystem.DB.StatusUtils.PilotAgentsDB", MagicMock())
    mocker.patch("DIRAC.WorkloadManagementSystem.DB.StatusUtils.StorageManagementDB", MagicMock())

    res = kill_delete_jobs(right, jobIDs_list)
    assert res["OK"]
