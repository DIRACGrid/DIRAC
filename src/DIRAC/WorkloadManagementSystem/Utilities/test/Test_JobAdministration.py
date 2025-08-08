""" unit test (pytest) of JobAdministration module
"""

from unittest.mock import MagicMock

import pytest

# sut
from DIRAC.WorkloadManagementSystem.Utilities.jobAdministration import kill_delete_jobs


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
    mocker.patch("DIRAC.WorkloadManagementSystem.Utilities.jobAdministration.JobDB", MagicMock())
    mocker.patch("DIRAC.WorkloadManagementSystem.Utilities.jobAdministration.TaskQueueDB", MagicMock())
    mocker.patch("DIRAC.WorkloadManagementSystem.Utilities.jobAdministration.PilotAgentsDB", MagicMock())
    mocker.patch("DIRAC.WorkloadManagementSystem.Utilities.jobAdministration.StorageManagementDB", MagicMock())

    res = kill_delete_jobs(right, jobIDs_list)
    assert res["OK"]
