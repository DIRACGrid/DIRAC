""" Test class for Pilot Status Agent
"""
import pytest
from unittest.mock import MagicMock

# DIRAC Components
from DIRAC.WorkloadManagementSystem.Agent.PilotStatusAgent import PilotStatusAgent
from DIRAC import gLogger, S_OK

# Mock objects
mockReply = MagicMock()
mockReply.return_value = {"OK": True, "Value": []}
mockAM = MagicMock()
mockNone = MagicMock()
mockNone.return_value = None
mockOK = MagicMock()
mockOK.return_value = {"OK": False}

gLogger.setLevel("DEBUG")


@pytest.fixture
def psa(mocker):
    mocker.patch("DIRAC.WorkloadManagementSystem.Agent.PilotStatusAgent.AgentModule.__init__")
    mocker.patch(
        "DIRAC.WorkloadManagementSystem.Agent.PilotStatusAgent.AgentModule._AgentModule__moduleProperties",
        side_effect=lambda x, y=None: y,
        create=True,
    )
    module_str = "DIRAC.WorkloadManagementSystem.Agent.PilotStatusAgent.PilotAgentsDB.selectPilots"
    mocker.patch(module_str, side_effect=mockReply)
    mocker.patch("DIRAC.WorkloadManagementSystem.Agent.PilotStatusAgent.PilotAgentsDB.__init__", side_effect=mockNone)
    mocker.patch("DIRAC.WorkloadManagementSystem.Agent.PilotStatusAgent.JobDB.__init__", side_effect=mockNone)
    module_str = "DIRAC.WorkloadManagementSystem.Agent.PilotStatusAgent.PilotAgentsDB.buildCondition"
    mocker.patch(module_str, side_effect=mockNone)
    mocker.patch("DIRAC.WorkloadManagementSystem.Agent.PilotStatusAgent.PilotAgentsDB._query", side_effect=mockOK)
    mocker.patch(
        "DIRAC.WorkloadManagementSystem.Agent.PilotStatusAgent.PilotAgentsDB._escapeString",
        lambda s, c: S_OK(f'"{s}"'),
    )  # To bypass "connection.escape_string"

    psa = PilotStatusAgent()
    psa._AgentModule__configDefaults = mockAM
    psa.initialize()
    psa.log = gLogger
    psa.pilotDB.log = gLogger
    psa.pilotDB.logger = gLogger
    psa.pilotStalledDays = 3

    return psa


@pytest.mark.parametrize(
    "mockReplyInput, expected",
    [
        ({"OK": True, "Value": False}, {"OK": True, "Value": None}),
        (
            {"OK": True, "Value": ["Test"]},
            {
                "OK": False,
                "Message": "No pilots found for PilotJobReference(s): ['Test']",
            },
        ),
        ({"OK": False, "Message": "Test"}, {"OK": False, "Message": "Test"}),
        ({"OK": True, "Value": []}, {"OK": True, "Value": None}),
    ],
)
def test_handleOldPilots(psa, mockReplyInput, expected):
    """Testing PilotStatusAgent().handleOldPilots()"""

    mockReply.return_value = mockReplyInput
    connection = "Test"
    result = psa.handleOldPilots(connection)
    assert result["OK"] == expected["OK"]
    if result["OK"]:
        assert result["Value"] == expected["Value"]
    else:
        if "Message" in result:
            assert result["Message"] == expected["Message"]
