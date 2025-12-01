""" Test class for Stalled Job Agent
"""
import pytest
from unittest.mock import MagicMock

# DIRAC Components
from DIRAC.WorkloadManagementSystem.Agent.StalledJobAgent import StalledJobAgent
from DIRAC import gLogger

# Mock Objects
mockAM = MagicMock()
mockNone = MagicMock()
mockNone.return_value = None

gLogger.setLevel("DEBUG")


@pytest.fixture
def sja(mocker):
    mocker.patch("DIRAC.WorkloadManagementSystem.Agent.StalledJobAgent.AgentModule.__init__")
    mocker.patch(
        "DIRAC.WorkloadManagementSystem.Agent.StalledJobAgent.AgentModule._AgentModule__moduleProperties",
        side_effect=lambda x, y=None: y,
        create=True,
    )

    # Mock ObjectLoader to return mock DB instances
    mockJobDB = MagicMock()
    mockJobDB.log = gLogger
    mockJobLoggingDB = MagicMock()
    mockTaskQueueDB = MagicMock()
    mockPilotAgentsDB = MagicMock()
    mockStorageManagementDB = MagicMock()

    def mock_load_object(module_path, class_name):
        mocks = {
            "JobDB": mockJobDB,
            "JobLoggingDB": mockJobLoggingDB,
            "TaskQueueDB": mockTaskQueueDB,
            "PilotAgentsDB": mockPilotAgentsDB,
            "StorageManagementDB": mockStorageManagementDB,
        }
        return {"OK": True, "Value": lambda: mocks[class_name]}

    mocker.patch(
        "DIRAC.WorkloadManagementSystem.Agent.StalledJobAgent.ObjectLoader.loadObject",
        side_effect=mock_load_object,
    )

    mocker.patch("DIRAC.WorkloadManagementSystem.Agent.StalledJobAgent.rescheduleJobs", return_value=MagicMock())
    mocker.patch("DIRAC.WorkloadManagementSystem.Agent.StalledJobAgent.getJobParameters", return_value=MagicMock())
    mocker.patch("DIRAC.WorkloadManagementSystem.Agent.StalledJobAgent.kill_delete_jobs", return_value=MagicMock())

    stalledJobAgent = StalledJobAgent()
    stalledJobAgent._AgentModule__configDefaults = mockAM
    stalledJobAgent.log = gLogger
    stalledJobAgent.initialize()
    stalledJobAgent.log.setLevel("DEBUG")
    stalledJobAgent.stalledTime = 120

    return stalledJobAgent


def test__sjaFunctions(sja):
    """Testing StalledJobAgent()"""

    assert sja._failSubmittingJobs()["OK"]
    assert sja._kickStuckJobs()["OK"]
    assert sja._failStalledJobs(0)["OK"]
    assert not sja._markStalledJobs(0)["OK"]
