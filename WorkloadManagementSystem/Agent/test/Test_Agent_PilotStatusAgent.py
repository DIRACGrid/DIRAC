""" Test class for Pilot Status Agent
"""

# imports
import pytest
from mock import MagicMock

# DIRAC Components
from DIRAC.WorkloadManagementSystem.Agent.PilotStatusAgent import PilotStatusAgent
from DIRAC.WorkloadManagementSystem.DB.PilotAgentsDB import PilotAgentsDB
from DIRAC import gLogger

# Mock objects
mockReply = MagicMock()

gLogger.setLevel('DEBUG')


def test_clearWaitingPilots(mocker):
  """ Testing PilotStatusAgent().clearWaitingPilots()
  """

  mocker.patch("DIRAC.WorkloadManagementSystem.Agent.PilotStatusAgent.AgentModule.__init__")

  pilotStatusAgent = PilotStatusAgent()
  pilotStatusAgent.pilotDB = PilotAgentsDB()

  condDict = {'OwnerDN': '', 'OwnerGroup': '', 'GridType': '', 'Broker': ''}

  result = pilotStatusAgent.clearWaitingPilots(condDict)

  assert result['OK']


@pytest.mark.parametrize(
    "mockReplyInput, expected", [
        ({
            'OK': True, 'Value': False}, {
            'OK': True, 'Value': None}), ({
                'OK': True, 'Value': ['Test']}, {
                'OK': False, 'Message': "No pilots found for PilotJobReference(s): ['Test']"}), ({
                    'OK': False, 'Message': 'Test'}, {
                    'OK': False, 'Message': 'Test'}), ({
                        'OK': True, 'Value': []}, {
                        'OK': True, 'Value': None})])
def test_handleOldPilots(mocker, mockReplyInput, expected):
  """ Testing PilotStatusAgent().handleOldPilots()
  """

  mockReply.return_value = mockReplyInput

  mocker.patch("DIRAC.WorkloadManagementSystem.Agent.PilotStatusAgent.AgentModule.__init__")
  module_str = "DIRAC.WorkloadManagementSystem.Agent.PilotStatusAgent.PilotAgentsDB.selectPilots"
  mocker.patch(module_str, side_effect=mockReply)

  pilotStatusAgent = PilotStatusAgent()
  pilotStatusAgent.pilotDB = PilotAgentsDB()
  pilotStatusAgent.pilotStalledDays = 3
  pilotStatusAgent.log = gLogger

  connection = 'Test'

  result = pilotStatusAgent.handleOldPilots(connection)

  assert result['OK'] == expected['OK']

  if result['OK']:
    assert result['Value'] == expected['Value']

  else:
    assert result['Message'] == expected['Message']
