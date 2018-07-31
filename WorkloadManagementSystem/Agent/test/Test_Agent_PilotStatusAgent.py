""" Test class for Pilot Status Agent
"""

# imports
from mock import MagicMock

# DIRAC Components
from DIRAC.WorkloadManagementSystem.Agent.PilotStatusAgent import PilotStatusAgent
from DIRAC.WorkloadManagementSystem.DB.PilotAgentsDB import PilotAgentsDB
from DIRAC import gLogger

gLogger.setLevel('DEBUG')


class TestPilotStatusAgent(object):
  """ Testing the single methods of PilotStatusAgent
  """

  def test_clearWaitingPilots(self, mocker):
    """ Testing PilotStatusAgent().clearWaitingPilots()
    """

    mocker.patch("DIRAC.WorkloadManagementSystem.Agent.StalledJobAgent.AgentModule.__init__")

    pilotStatusAgent = PilotStatusAgent()
    pilotStatusAgent.pilotDB = PilotAgentsDB()

    condDict = {'OwnerDN': '', 'OwnerGroup': '', 'GridType': '', 'Broker': ''}

    result = pilotStatusAgent.clearWaitingPilots(condDict)

    assert result['OK']

  def test_handleOldPilots(self, mocker):
    """ Testing PilotStatusAgent().handleOldPilots()
    """

    mocker.patch("DIRAC.WorkloadManagementSystem.Agent.StalledJobAgent.AgentModule.__init__")

    pilotStatusAgent = PilotStatusAgent()
    pilotStatusAgent.pilotDB = PilotAgentsDB()

    condDict = {'OwnerDN': '', 'OwnerGroup': '', 'GridType': '', 'Broker': ''}

    result = pilotStatusAgent.clearWaitingPilots(condDict)

    assert result['OK']
