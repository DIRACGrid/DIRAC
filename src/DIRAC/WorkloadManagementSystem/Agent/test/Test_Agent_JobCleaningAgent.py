""" Test class for Job Cleaning Agent
"""

# imports
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
import pytest
from mock import MagicMock

# DIRAC Components
from DIRAC.WorkloadManagementSystem.Agent.JobCleaningAgent import JobCleaningAgent
from DIRAC import gLogger

gLogger.setLevel('DEBUG')

# Mock Objects
mockReply = MagicMock()
mockAM = MagicMock()
mockNone = MagicMock()
mockNone.return_value = None
mockJMC = MagicMock()


@pytest.mark.parametrize(
    "mockReplyInput, expected", [
        ({'OK': True, 'Value': ''}, {'OK': True, 'Value': []}),
        ({'OK': False, 'Message': ''}, {'OK': False, 'Message': ''})
    ])
def test__getAllowedJobTypes(mocker, mockReplyInput, expected):
  """ Testing JobCleaningAgent()._getAllowedJobTypes()
  """

  mockReply.return_value = mockReplyInput

  mocker.patch("DIRAC.WorkloadManagementSystem.Agent.JobCleaningAgent.AgentModule.__init__")
  mocker.patch(
      "DIRAC.WorkloadManagementSystem.Agent.JobCleaningAgent.AgentModule._AgentModule__moduleProperties",
      side_effect=lambda x, y=None: y, create=True
  )
  mocker.patch(
      "DIRAC.WorkloadManagementSystem.Agent.JobCleaningAgent.JobDB.getDistinctJobAttributes",
      side_effect=mockReply)
  mocker.patch("DIRAC.WorkloadManagementSystem.Agent.JobCleaningAgent.JobDB.__init__", side_effect=mockNone)

  jobCleaningAgent = JobCleaningAgent()
  jobCleaningAgent.log = gLogger
  jobCleaningAgent.log.setLevel('DEBUG')
  jobCleaningAgent._AgentModule__configDefaults = mockAM
  jobCleaningAgent.initialize()

  result = jobCleaningAgent._getAllowedJobTypes()

  assert result == expected


@pytest.mark.parametrize(
    "mockReplyInput, expected", [
        ({'OK': True, 'Value': ''}, {'OK': True, 'Value': None}),
        ({'OK': False, 'Message': ''}, {'OK': False, 'Message': ''})
    ])
def test_removeJobsByStatus(mocker, mockReplyInput, expected):
  """ Testing JobCleaningAgent().removeDeletedJobs()
  """

  mockReply.return_value = mockReplyInput

  mocker.patch("DIRAC.WorkloadManagementSystem.Agent.JobCleaningAgent.AgentModule.__init__")
  mocker.patch(
      "DIRAC.WorkloadManagementSystem.Agent.JobCleaningAgent.AgentModule._AgentModule__moduleProperties",
      side_effect=lambda x, y=None: y, create=True
  )
  mocker.patch("DIRAC.WorkloadManagementSystem.Agent.JobCleaningAgent.JobDB.selectJobs", side_effect=mockReply)
  mocker.patch("DIRAC.WorkloadManagementSystem.Agent.JobCleaningAgent.JobDB.__init__", side_effect=mockNone)

  jobCleaningAgent = JobCleaningAgent()
  jobCleaningAgent.log = gLogger
  jobCleaningAgent.log.setLevel('DEBUG')
  jobCleaningAgent._AgentModule__configDefaults = mockAM
  jobCleaningAgent.initialize()

  result = jobCleaningAgent.removeDeletedJobs()

  assert result == expected


@pytest.mark.parametrize(
    "conditions, mockReplyInput, expected", [
        ({'JobType': '', 'Status': 'Deleted'}, {'OK': True, 'Value': ''}, {'OK': True, 'Value': None}),
        ({'JobType': '', 'Status': 'Deleted'}, {'OK': False, 'Message': ''}, {'OK': False, 'Message': ''}),
        ({'JobType': [], 'Status': 'Deleted'}, {'OK': True, 'Value': ''}, {'OK': True, 'Value': None}),
        ({'JobType': ['some', 'status'],
          'Status': ['Deleted', 'Cancelled']}, {'OK': True, 'Value': ''}, {'OK': True, 'Value': None})
    ])
def test_deleteJobsByStatus(mocker, conditions, mockReplyInput, expected):
  """ Testing JobCleaningAgent().deleteJobsByStatus()
  """

  mockReply.return_value = mockReplyInput

  mocker.patch("DIRAC.WorkloadManagementSystem.Agent.JobCleaningAgent.AgentModule.__init__")
  mocker.patch(
      "DIRAC.WorkloadManagementSystem.Agent.JobCleaningAgent.AgentModule._AgentModule__moduleProperties",
      side_effect=lambda x, y=None: y, create=True
  )
  mocker.patch("DIRAC.WorkloadManagementSystem.Agent.JobCleaningAgent.JobDB.selectJobs", side_effect=mockReply)
  mocker.patch("DIRAC.WorkloadManagementSystem.Agent.JobCleaningAgent.JobDB.__init__", side_effect=mockNone)

  jobCleaningAgent = JobCleaningAgent()
  jobCleaningAgent.log = gLogger
  jobCleaningAgent.log.setLevel('DEBUG')
  jobCleaningAgent._AgentModule__configDefaults = mockAM
  jobCleaningAgent.initialize()

  result = jobCleaningAgent.deleteJobsByStatus(conditions)

  assert result == expected


@pytest.mark.parametrize(
    "inputs, params, expected", [
        ([], {'OK': True, 'Value': {}}, {'OK': True, 'Value': {'Failed': {}, 'Successful': {}}}),
        (['a', 'b'], {'OK': True, 'Value': {}}, {'OK': True, 'Value': {'Failed': {}, 'Successful': {}}}),
        ([], {'OK': True, 'Value': {1: {'OutputSandboxLFN': '/some/lfn/1.txt'}}},
            {'OK': True, 'Value': {'Failed': {}, 'Successful': {1: '/some/lfn/1.txt'}}}),
        ([], {'OK': True, 'Value': {1: {'OutputSandboxLFN': '/some/lfn/1.txt'},
                                    2: {'OutputSandboxLFN': '/some/other/lfn/2.txt'}}},
            {'OK': True, 'Value': {'Failed': {}, 'Successful': {1: '/some/lfn/1.txt',
                                                                2: '/some/other/lfn/2.txt'}}}),
        (['a', 'b'], {'OK': True, 'Value': {1: {'OutputSandboxLFN': '/some/lfn/1.txt'}}},
            {'OK': True, 'Value': {'Failed': {}, 'Successful': {1: '/some/lfn/1.txt'}}}),
        (['a', 'b'], {'OK': False}, {'OK': False}),
    ])
def test_deleteJobOversizedSandbox(mocker, inputs, params, expected):
  """ Testing JobCleaningAgent().deleteJobOversizedSandbox()
  """

  mocker.patch("DIRAC.WorkloadManagementSystem.Agent.JobCleaningAgent.AgentModule.__init__")
  mocker.patch("DIRAC.WorkloadManagementSystem.Agent.JobCleaningAgent.AgentModule.am_getOption", return_value=mockAM)
  mocker.patch("DIRAC.WorkloadManagementSystem.Agent.JobCleaningAgent.JobDB", return_value=mockNone)
  mocker.patch("DIRAC.WorkloadManagementSystem.Agent.JobCleaningAgent.ReqClient", return_value=mockNone)
  mocker.patch("DIRAC.WorkloadManagementSystem.Agent.JobCleaningAgent.JobMonitoringClient", return_value=mockJMC)

  jobCleaningAgent = JobCleaningAgent()
  jobCleaningAgent.log = gLogger
  jobCleaningAgent.log.setLevel('DEBUG')
  jobCleaningAgent._AgentModule__configDefaults = mockAM
  jobCleaningAgent.initialize()

  mockJMC.getJobParameters.return_value = params

  result = jobCleaningAgent.deleteJobOversizedSandbox(inputs)

  assert result == expected
