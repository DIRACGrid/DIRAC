""" Test class for Pilot Status Agent
"""

# imports
from mock import MagicMock, patch

# DIRAC Components
from DIRAC.WorkloadManagementSystem.Agent.PilotStatusAgent import PilotStatusAgent
from DIRAC.WorkloadManagementSystem.DB.PilotAgentsDB import PilotAgentsDB
from DIRAC import gLogger

gLogger.setLevel('DEBUG')

# Mock Objects
mockAM = MagicMock()


class TestPilotStatusAgent(object):
  """ Testing the single methods of PilotStatusAgent
  """

  @patch("DIRAC.WorkloadManagementSystem.Agent.PilotStatusAgent.AgentModule", side_effect=mockAM)
  @patch("DIRAC.WorkloadManagementSystem.Agent.PilotStatusAgent.AgentModule.__init__", new=mockAM)
  def test_clearWaitingPilots(self, _patch1):
    """ Testing PilotStatusAgent().clearWaitingPilots()
    """

    pilotStatusAgent = PilotStatusAgent()
    pilotStatusAgent.pilotDB = PilotAgentsDB()

    condDict = {'OwnerDN': '', 'OwnerGroup': '', 'GridType': '', 'Broker': ''}

    result = pilotStatusAgent.clearWaitingPilots(condDict)

    assert result['OK']

  @patch("DIRAC.WorkloadManagementSystem.Agent.PilotStatusAgent.AgentModule", side_effect=mockAM)
  @patch("DIRAC.WorkloadManagementSystem.Agent.PilotStatusAgent.AgentModule.__init__", new=mockAM)
  def test_handleOldPilots(self, _patch1):
    """ Testing PilotStatusAgent().handleOldPilots()
    """

    pilotStatusAgent = PilotStatusAgent()
    pilotStatusAgent.pilotDB = PilotAgentsDB()

    condDict = {'OwnerDN': '', 'OwnerGroup': '', 'GridType': '', 'Broker': ''}

    result = pilotStatusAgent.clearWaitingPilots(condDict)

    assert result['OK']
