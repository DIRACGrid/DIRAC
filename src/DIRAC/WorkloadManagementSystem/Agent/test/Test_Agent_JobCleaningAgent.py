""" Test class for Job Cleaning Agent
"""
from unittest.mock import MagicMock

import pytest

# DIRAC Components
from DIRAC import S_OK, gLogger
from DIRAC.WorkloadManagementSystem.Agent.JobCleaningAgent import JobCleaningAgent

gLogger.setLevel("DEBUG")

# Mock Objects
mockReply = MagicMock()
mockAM = MagicMock()
mockNone = MagicMock()
mockNone.return_value = None
mockJMC = MagicMock()


@pytest.fixture
def jca(mocker):
    mocker.patch("DIRAC.WorkloadManagementSystem.Agent.JobCleaningAgent.AgentModule.__init__")
    mocker.patch(
        "DIRAC.WorkloadManagementSystem.Agent.JobCleaningAgent.AgentModule._AgentModule__moduleProperties",
        side_effect=lambda x, y=None: y,
        create=True,
    )
    mocker.patch("DIRAC.WorkloadManagementSystem.Agent.JobCleaningAgent.AgentModule.am_getOption", return_value=mockAM)
    mocker.patch(
        "DIRAC.WorkloadManagementSystem.Agent.JobCleaningAgent.JobDB.getDistinctJobAttributes", side_effect=mockReply
    )
    mocker.patch("DIRAC.WorkloadManagementSystem.Agent.JobCleaningAgent.JobDB.selectJobs", side_effect=mockReply)
    mocker.patch("DIRAC.WorkloadManagementSystem.Agent.JobCleaningAgent.JobDB.__init__", side_effect=mockNone)
    mocker.patch("DIRAC.WorkloadManagementSystem.Agent.JobCleaningAgent.ReqClient", return_value=mockNone)

    jca = JobCleaningAgent()
    jca.log = gLogger
    jca.log.setLevel("DEBUG")
    jca._AgentModule__configDefaults = mockAM
    jca.initialize()

    return jca


@pytest.mark.parametrize(
    "mockReplyInput, expected",
    [
        ({"OK": True, "Value": ""}, {"OK": True, "Value": []}),
        ({"OK": False, "Message": ""}, {"OK": False, "Message": ""}),
    ],
)
def test__getAllowedJobTypes(jca, mockReplyInput, expected):
    """Testing JobCleaningAgent()._getAllowedJobTypes()"""

    mockReply.return_value = mockReplyInput
    result = jca._getAllowedJobTypes()
    assert result == expected


@pytest.mark.parametrize(
    "mockReplyInput, expected",
    [
        ({"OK": True, "Value": ""}, {"OK": True, "Value": None}),
        ({"OK": False, "Message": ""}, {"OK": False, "Message": ""}),
    ],
)
def test_removeJobsByStatus(jca, mockReplyInput, expected):
    """Testing JobCleaningAgent().removeDeletedJobs()"""

    mockReply.return_value = mockReplyInput
    result = jca.removeDeletedJobs()
    assert result == expected


@pytest.mark.parametrize(
    "conditions, mockReplyInput, expected",
    [
        ({"JobType": "", "Status": "Deleted"}, {"OK": True, "Value": ""}, {"OK": True, "Value": None}),
        ({"JobType": "", "Status": "Deleted"}, {"OK": False, "Message": ""}, {"OK": False, "Message": ""}),
        ({"JobType": [], "Status": "Deleted"}, {"OK": True, "Value": ""}, {"OK": True, "Value": None}),
        (
            {"JobType": ["some", "status"], "Status": ["Deleted", "Cancelled"]},
            {"OK": True, "Value": ""},
            {"OK": True, "Value": None},
        ),
    ],
)
def test_deleteJobsByStatus(jca, conditions, mockReplyInput, expected):
    """Testing JobCleaningAgent().deleteJobsByStatus()"""

    mockReply.return_value = mockReplyInput
    result = jca.deleteJobsByStatus(conditions)
    assert result == expected


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
    mocker.patch("DIRAC.WorkloadManagementSystem.Agent.JobCleaningAgent.JobDB", return_value=mockNone)
    mocker.patch("DIRAC.WorkloadManagementSystem.Agent.JobCleaningAgent.ReqClient", return_value=mockNone)
    mocker.patch(
        "DIRAC.WorkloadManagementSystem.Agent.JobCleaningAgent.getDNForUsername", return_value=S_OK(["/bih/boh/DN"])
    )
    mocker.patch("DIRAC.WorkloadManagementSystem.Agent.JobCleaningAgent.getJobParameters", return_value=params)

    jobCleaningAgent = JobCleaningAgent()
    jobCleaningAgent.log = gLogger
    jobCleaningAgent.log.setLevel("DEBUG")
    jobCleaningAgent._AgentModule__configDefaults = mockAM
    jobCleaningAgent.initialize()

    result = jobCleaningAgent.deleteJobOversizedSandbox(inputs)

    assert result == expected
