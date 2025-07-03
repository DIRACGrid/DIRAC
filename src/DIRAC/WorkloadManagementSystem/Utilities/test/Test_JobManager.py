""" unit test (pytest) of JobManager service
"""

from unittest.mock import MagicMock
import pytest


# sut
from DIRAC.WorkloadManagementSystem.Utilities.jobAdministration import kill_delete_jobs


@pytest.mark.parametrize(
    "jobIDs_list, right, expected_res, expected_value",
    [
        ([], 'Kill', True, []),
        ([], 'Delete', True, []),
        (1, 'Kill', True, []),
        (1, 'Kill', True, []),
        ([1, 2], 'Kill', True, []),
        ([1, 2], 'Kill', True, []),
        (1, 'Kill', True, [1]),
        ([1, 2], 'Kill', True, [1]),
        ([1, 2], 'Kill', True, [1]),
        ([1, 2], 'Kill', True, []),
        ([1, 2], 'Kill', True, [1, 2]),
    ],
)
def test___kill_delete_jobs(mocker, jobIDs_list, right, expected_res, expected_value):
    mocker.patch("DIRAC.WorkloadManagementSystem.Utilities.jobAdministration.JobDB", MagicMock())
    mocker.patch("DIRAC.WorkloadManagementSystem.Utilities.jobAdministration.TaskQueueDB", MagicMock())
    mocker.patch("DIRAC.WorkloadManagementSystem.Utilities.jobAdministration.PilotAgentsDB", MagicMock())
    mocker.patch("DIRAC.WorkloadManagementSystem.Utilities.jobAdministration.StorageManagementDB", MagicMock())
    mocker.patch("DIRAC.WorkloadManagementSystem.Utilities.jobAdministration.filterJobStateTransition", MagicMock())

    res = kill_delete_jobs(right, jobIDs_list)
    assert res["OK"] == expected_res
    assert res["Value"] == expected_value
