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
    mocker.patch("DIRAC.WorkloadManagementSystem.Agent.StalledJobAgent.JobDB")
    mocker.patch("DIRAC.WorkloadManagementSystem.Agent.StalledJobAgent.JobLoggingDB")
    mocker.patch("DIRAC.WorkloadManagementSystem.Agent.StalledJobAgent.getSystemInstance", side_effect=mockNone)
    mocker.patch("DIRAC.WorkloadManagementSystem.Agent.StalledJobAgent.JobMonitoringClient", return_value=MagicMock())
    mocker.patch("DIRAC.WorkloadManagementSystem.Agent.StalledJobAgent.PilotManagerClient", return_value=MagicMock())
    mocker.patch("DIRAC.WorkloadManagementSystem.Agent.StalledJobAgent.WMSClient", return_value=MagicMock())
    mocker.patch("DIRAC.WorkloadManagementSystem.Agent.StalledJobAgent.getSystemInstance", return_value="/bof/bih")

    stalledJobAgent = StalledJobAgent()
    stalledJobAgent._AgentModule__configDefaults = mockAM
    stalledJobAgent.log = gLogger
    stalledJobAgent.initialize()
    stalledJobAgent.jobDB.log = gLogger
    stalledJobAgent.log.setLevel("DEBUG")
    stalledJobAgent.stalledTime = 120

    return stalledJobAgent


def test__sjaFunctions(sja):
    """Testing StalledJobAgent()"""

    assert sja._failSubmittingJobs()["OK"]
    assert sja._kickStuckJobs()["OK"]
    assert sja._failStalledJobs(0)["OK"]
    assert not sja._markStalledJobs(0)["OK"]
