""" Test class for Pilot Status Agent
"""

# imports
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
import pytest
from mock import MagicMock

# DIRAC Components
from DIRAC.WorkloadManagementSystem.Agent.PilotStatusAgent import PilotStatusAgent
from DIRAC import gLogger, S_OK

# Mock objects
mockReply = MagicMock()
mockAM = MagicMock()
mockNone = MagicMock()
mockNone.return_value = None
mockOK = MagicMock()
mockOK.return_value = {'OK': False}

gLogger.setLevel('DEBUG')


def test_clearWaitingPilots(mocker):
  """ Testing PilotStatusAgent().clearWaitingPilots()
  """

  mocker.patch("DIRAC.WorkloadManagementSystem.Agent.PilotStatusAgent.AgentModule.__init__")
  mocker.patch(
      "DIRAC.WorkloadManagementSystem.Agent.PilotStatusAgent.AgentModule._AgentModule__moduleProperties",
      side_effect=lambda x, y=None: y, create=True
  )
  mocker.patch("DIRAC.WorkloadManagementSystem.Agent.PilotStatusAgent.PilotAgentsDB.__init__", side_effect=mockNone)
  mocker.patch("DIRAC.WorkloadManagementSystem.Agent.PilotStatusAgent.JobDB.__init__", side_effect=mockNone)
  module_str = "DIRAC.WorkloadManagementSystem.Agent.PilotStatusAgent.PilotAgentsDB.buildCondition"
  mocker.patch(module_str, side_effect=mockNone)
  mocker.patch("DIRAC.WorkloadManagementSystem.Agent.PilotStatusAgent.PilotAgentsDB._query", side_effect=mockOK)
  mocker.patch("DIRAC.WorkloadManagementSystem.Agent.PilotStatusAgent.PilotAgentsDB._escapeString",
               lambda s, c: S_OK('"%s"' % s))  # To bypass "connection.escape_string"

  pilotStatusAgent = PilotStatusAgent()
  pilotStatusAgent._AgentModule__configDefaults = mockAM
  pilotStatusAgent.initialize()
  pilotStatusAgent.log = gLogger
  pilotStatusAgent.pilotDB.log = gLogger
  pilotStatusAgent.pilotDB.logger = gLogger

  condDict = {'OwnerDN': '', 'OwnerGroup': '', 'GridType': '', 'Broker': ''}

  result = pilotStatusAgent.clearWaitingPilots(condDict)

  assert not result['OK']


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
  mocker.patch(
      "DIRAC.WorkloadManagementSystem.Agent.PilotStatusAgent.AgentModule._AgentModule__moduleProperties",
      side_effect=lambda x, y=None: y, create=True
  )
  module_str = "DIRAC.WorkloadManagementSystem.Agent.PilotStatusAgent.PilotAgentsDB.selectPilots"
  mocker.patch(module_str, side_effect=mockReply)
  mocker.patch("DIRAC.WorkloadManagementSystem.Agent.PilotStatusAgent.PilotAgentsDB.__init__", side_effect=mockNone)
  mocker.patch("DIRAC.WorkloadManagementSystem.Agent.PilotStatusAgent.JobDB.__init__", side_effect=mockNone)
  mocker.patch("DIRAC.WorkloadManagementSystem.Agent.PilotStatusAgent.PilotAgentsDB._query", side_effect=mockOK)
  mocker.patch("DIRAC.WorkloadManagementSystem.Agent.PilotStatusAgent.PilotAgentsDB._escapeString",
               lambda s, c: S_OK('"%s"' % s))  # To bypass "connection.escape_string"

  pilotStatusAgent = PilotStatusAgent()
  pilotStatusAgent._AgentModule__configDefaults = mockAM
  pilotStatusAgent.initialize()
  pilotStatusAgent.pilotStalledDays = 3
  pilotStatusAgent.log = gLogger
  pilotStatusAgent.pilotDB.logger = gLogger

  connection = 'Test'

  result = pilotStatusAgent.handleOldPilots(connection)

  assert result['OK'] == expected['OK']

  if result['OK']:
    assert result['Value'] == expected['Value']

  else:
    if 'Message' in result:
      assert result['Message'] == expected['Message']
