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
    # Mock ObjectLoader to return mock DB instances
    mockJobDB = MagicMock()
    mockTaskQueueDB = MagicMock()
    mockPilotAgentsDB = MagicMock()
    mockStorageManagementDB = MagicMock()

    def mock_load_object(module_path, class_name):
        mocks = {
            "JobDB": mockJobDB,
            "TaskQueueDB": mockTaskQueueDB,
            "PilotAgentsDB": mockPilotAgentsDB,
            "StorageManagementDB": mockStorageManagementDB,
        }
        return {"OK": True, "Value": lambda: mocks[class_name]}

    mocker.patch(
        "DIRAC.WorkloadManagementSystem.DB.StatusUtils.ObjectLoader.loadObject",
        side_effect=mock_load_object,
    )

    res = kill_delete_jobs(right, jobIDs_list)
    assert res["OK"]
