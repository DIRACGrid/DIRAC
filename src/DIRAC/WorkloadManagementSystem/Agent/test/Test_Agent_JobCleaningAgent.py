"""Test class for Job Cleaning Agent"""

from unittest.mock import MagicMock

import pytest

# DIRAC Components
from DIRAC import S_OK, gLogger
from DIRAC.WorkloadManagementSystem.Agent.JobCleaningAgent import JobCleaningAgent

gLogger.setLevel("DEBUG")

# Mock Objects
mockAM = MagicMock()
mockNone = MagicMock()
mockNone.return_value = None


@pytest.fixture
def jca(mocker):
    mocker.patch("DIRAC.WorkloadManagementSystem.Agent.JobCleaningAgent.AgentModule.__init__")
    mocker.patch(
        "DIRAC.WorkloadManagementSystem.Agent.JobCleaningAgent.AgentModule._AgentModule__moduleProperties",
        side_effect=lambda x, y=None: y,
        create=True,
    )

    def mock_am_getOption(option, default=None):
        defaults = {
            "ProductionTypes": [],
            "MaxJobsAtOnce": 500,
            "RemoveStatusDelay/Done": 7,
            "RemoveStatusDelay/Killed": 7,
            "RemoveStatusDelay/Failed": 7,
            "RemoveStatusDelay/Any": -1,
            "RemoveStatusDelayHB/Done": -1,
            "RemoveStatusDelayHB/Killed": -1,
            "RemoveStatusDelayHB/Failed": -1,
            "MaxHBJobsAtOnce": 0,
        }
        return defaults.get(option, default)

    mocker.patch(
        "DIRAC.WorkloadManagementSystem.Agent.JobCleaningAgent.AgentModule.am_getOption",
        side_effect=mock_am_getOption,
    )

    mocker.patch("DIRAC.WorkloadManagementSystem.Agent.JobCleaningAgent.Operations")

    def mock_load_object(module_path, class_name):
        mocks = {
            "JobDB": MagicMock(),
            "TaskQueueDB": MagicMock(),
            "PilotAgentsDB": MagicMock(),
            "SandboxMetadataDB": MagicMock(),
            "StorageManagementDB": MagicMock(),
        }
        return {"OK": True, "Value": lambda: mocks[class_name]}

    mocker.patch(
        "DIRAC.WorkloadManagementSystem.Agent.JobCleaningAgent.ObjectLoader.loadObject",
        side_effect=mock_load_object,
    )
    jca = JobCleaningAgent()
    jca.log = gLogger
    jca.log.setLevel("DEBUG")
    jca._AgentModule__configDefaults = mockAM
    jca.initialize()

    return jca


@pytest.mark.parametrize(
    "dbReply, expected",
    [
        ({"OK": True, "Value": []}, {"OK": True, "Value": []}),
        ({"OK": False, "Message": "error"}, {"OK": False, "Message": "error"}),
    ],
)
def test__getAllowedJobTypes(jca, mocker, dbReply, expected):
    """Testing JobCleaningAgent()._getAllowedJobTypes()"""

    jca.jobDB.getDistinctJobAttributes.return_value = dbReply
    result = jca._getAllowedJobTypes()
    assert result["OK"] == expected["OK"]
    if result["OK"]:
        assert result["Value"] == expected["Value"]
    else:
        assert result["Message"] == expected["Message"]


@pytest.mark.parametrize(
    "mockReplyInput, expected",
    [
        ({"OK": True, "Value": []}, {"OK": True, "Value": None}),
        ({"OK": False, "Message": "error"}, {"OK": False, "Message": "error"}),
    ],
)
def test_removeJobsByStatus(jca, mocker, mockReplyInput, expected):
    """Testing JobCleaningAgent().removeDeletedJobs()"""

    jca.jobDB.selectJobs.return_value = mockReplyInput
    result = jca.removeDeletedJobs()
    assert result["OK"] == expected["OK"]
    if result["OK"]:
        assert result["Value"] is None
    else:
        assert result["Message"] == expected["Message"]


@pytest.mark.parametrize(
    "conditions, mockReplyInput, expected",
    [
        ({"JobType": [], "Status": "Deleted"}, {"OK": True, "Value": []}, {"OK": True, "Value": None}),
        ({"JobType": [], "Status": "Deleted"}, {"OK": False, "Message": "error"}, {"OK": False, "Message": "error"}),
        (
            {"JobType": ["User"], "Status": ["Deleted", "Killed"]},
            {"OK": True, "Value": []},
            {"OK": True, "Value": None},
        ),
    ],
)
def test_deleteJobsByStatus(jca, mocker, conditions, mockReplyInput, expected):
    """Testing JobCleaningAgent().deleteJobsByStatus()"""

    jca.jobDB.selectJobs.return_value = mockReplyInput
    result = jca.deleteJobsByStatus(conditions)
    assert result["OK"] == expected["OK"]
    if result["OK"]:
        assert result["Value"] is None
    else:
        assert result["Message"] == expected["Message"]


@pytest.mark.parametrize(
    "inputs, params, expected",
    [
        ([], {"OK": True, "Value": {}}, {"OK": True, "Value": {"Failed": {}, "Successful": {}}}),
        (["123", "456"], {"OK": True, "Value": {}}, {"OK": True, "Value": {"Failed": {}, "Successful": {}}}),
        (
            [],
            {"OK": True, "Value": {1: {"OutputSandboxLFN": "/some/lfn/1.txt"}}},
            {"OK": True, "Value": {"Failed": {}, "Successful": {1: "/some/lfn/1.txt"}}},
        ),
        (
            [],
            {
                "OK": True,
                "Value": {1: {"OutputSandboxLFN": "/some/lfn/1.txt"}, 2: {"OutputSandboxLFN": "/some/other/lfn/2.txt"}},
            },
            {"OK": True, "Value": {"Failed": {}, "Successful": {1: "/some/lfn/1.txt", 2: "/some/other/lfn/2.txt"}}},
        ),
        (
            ["123", "456"],
            {"OK": True, "Value": {1: {"OutputSandboxLFN": "/some/lfn/1.txt"}}},
            {"OK": True, "Value": {"Failed": {}, "Successful": {1: "/some/lfn/1.txt"}}},
        ),
        (["123", "456"], {"OK": False}, {"OK": False}),
    ],
)
def test_deleteJobOversizedSandbox(mocker, inputs, params, expected):
    """Testing JobCleaningAgent().deleteJobOversizedSandbox()"""

    mocker.patch("DIRAC.WorkloadManagementSystem.Agent.JobCleaningAgent.AgentModule.__init__")
    mocker.patch("DIRAC.WorkloadManagementSystem.Agent.JobCleaningAgent.AgentModule.am_getOption", return_value=mockAM)
    mocker.patch("DIRAC.WorkloadManagementSystem.Agent.JobCleaningAgent.ReqClient", return_value=mockNone)
    mocker.patch(
        "DIRAC.WorkloadManagementSystem.Agent.JobCleaningAgent.getDNForUsername", return_value=S_OK(["/bih/boh/DN"])
    )
    mockJobParamsDB = mocker.patch("DIRAC.WorkloadManagementSystem.Agent.JobCleaningAgent.JobParametersDB")
    mockJobParamsDB.return_value.getJobParameters.return_value = params

    def mock_load_object(module_path, class_name):
        mocks = {
            "JobDB": MagicMock(),
            "TaskQueueDB": MagicMock(),
            "PilotAgentsDB": MagicMock(),
            "SandboxMetadataDB": MagicMock(),
            "StorageManagementDB": MagicMock(),
        }
        return {"OK": True, "Value": lambda: mocks[class_name]}

    mocker.patch(
        "DIRAC.WorkloadManagementSystem.Agent.JobCleaningAgent.ObjectLoader.loadObject",
        side_effect=mock_load_object,
    )
    jobCleaningAgent = JobCleaningAgent()

    jobCleaningAgent.log = gLogger
    jobCleaningAgent.log.setLevel("DEBUG")
    jobCleaningAgent._AgentModule__configDefaults = mockAM
    jobCleaningAgent.initialize()

    result = jobCleaningAgent.deleteJobOversizedSandbox(inputs)

    assert result == expected
