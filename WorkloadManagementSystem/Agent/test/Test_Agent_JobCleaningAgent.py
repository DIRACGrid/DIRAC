""" Test class for Job Cleaning Agent
"""

# imports
import pytest
from mock import MagicMock

# DIRAC Components
from DIRAC.WorkloadManagementSystem.Agent.JobCleaningAgent import JobCleaningAgent
from DIRAC.WorkloadManagementSystem.DB.JobDB import JobDB
from DIRAC import gLogger

gLogger.setLevel('DEBUG')

# Mock Objects
mockReply = MagicMock()


@pytest.mark.parametrize(
    "mockReplyInput, expected", [
        ({
            'OK': True, 'Value': ''}, {
            'OK': True, 'Value': []}), ({
                'OK': False, 'Value': ''}, {
                'OK': False, 'Value': ''})])
def test__getAllowedJobTypes(mocker, mockReplyInput, expected):
  """ Testing JobCleaningAgent()._getAllowedJobTypes()
  """

  mockReply.return_value = mockReplyInput

  mocker.patch("DIRAC.WorkloadManagementSystem.Agent.JobCleaningAgent.AgentModule.__init__")
  mocker.patch(
      "DIRAC.WorkloadManagementSystem.Agent.JobCleaningAgent.JobDB.getDistinctJobAttributes",
      side_effect=mockReply)

  jobCleaningAgent = JobCleaningAgent()
  jobCleaningAgent.log = gLogger
  jobCleaningAgent.log.setLevel('DEBUG')

  jobCleaningAgent.jobDB = JobDB()
  result = jobCleaningAgent._getAllowedJobTypes()

  assert result == expected


@pytest.mark.parametrize(
    "mockReplyInput, expected", [
        ({
            'OK': True, 'Value': ''}, {
            'OK': True, 'Value': None}), ({
                'OK': False, 'Value': ''}, {
                'OK': False, 'Value': ''})])
def test_removeJobsByStatus(mocker, mockReplyInput, expected):
  """ Testing JobCleaningAgent().removeJobsByStatus()
  """

  mockReply.return_value = mockReplyInput

  mocker.patch("DIRAC.WorkloadManagementSystem.Agent.JobCleaningAgent.AgentModule.__init__")
  mocker.patch("DIRAC.WorkloadManagementSystem.Agent.JobCleaningAgent.JobDB.selectJobs", side_effect=mockReply)

  jobCleaningAgent = JobCleaningAgent()
  jobCleaningAgent.log = gLogger
  jobCleaningAgent.log.setLevel('DEBUG')

  jobCleaningAgent.jobDB = JobDB()
  result = jobCleaningAgent.removeJobsByStatus({})

  assert result == expected


@pytest.mark.parametrize(
    "mockReplyInput, expected", [
        ({
            'OK': True, 'Value': ''}, {
            'OK': True, 'Value': {
                'Failed': {}, 'Successful': {}}}), ({
                    'OK': False, 'Value': ''}, {
                    'OK': True, 'Value': {
                        'Failed': {}, 'Successful': {}}})])
def test_deleteJobOversizedSandbox(mocker, mockReplyInput, expected):
  """ Testing JobCleaningAgent().deleteJobOversizedSandbox()
  """

  mockReply.return_value = mockReplyInput

  mocker.patch("DIRAC.WorkloadManagementSystem.Agent.JobCleaningAgent.AgentModule.__init__")
  mocker.patch("DIRAC.WorkloadManagementSystem.Agent.JobCleaningAgent.JobDB.selectJobs", side_effect=mockReply)

  jobCleaningAgent = JobCleaningAgent()
  jobCleaningAgent.log = gLogger
  jobCleaningAgent.log.setLevel('DEBUG')

  jobCleaningAgent.jobDB = JobDB()
  result = jobCleaningAgent.deleteJobOversizedSandbox([])

  assert result == expected
