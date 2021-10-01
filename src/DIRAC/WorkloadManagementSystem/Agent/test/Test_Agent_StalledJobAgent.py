""" Test class for Stalled Job Agent
"""

# imports
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import pytest
from mock import MagicMock

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
    mocker.patch("DIRAC.WorkloadManagementSystem.Agent.StalledJobAgent.JobDB.__init__", side_effect=mockNone)
    mocker.patch("DIRAC.WorkloadManagementSystem.Agent.StalledJobAgent.JobLoggingDB.__init__", side_effect=mockNone)
    mocker.patch("DIRAC.WorkloadManagementSystem.Agent.StalledJobAgent.getSystemInstance", side_effect=mockNone)

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

    assert not sja._failSubmittingJobs()["OK"]
    assert not sja._kickStuckJobs()["OK"]
    assert not sja._failStalledJobs(0)["OK"]
    assert not sja._markStalledJobs(0)["OK"]
