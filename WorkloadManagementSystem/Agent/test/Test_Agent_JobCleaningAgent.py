""" Test class for Job Cleaning Agent
"""

# imports
from mock import MagicMock, patch

# DIRAC Components
from DIRAC.WorkloadManagementSystem.Agent.JobCleaningAgent import JobCleaningAgent
from DIRAC.WorkloadManagementSystem.DB.JobDB import JobDB
from DIRAC import gLogger

gLogger.setLevel('DEBUG')

# Mock Objects
mockAM = MagicMock()
mockReply = MagicMock()
mockReply.return_value = {'OK': True, 'Value': ''}


class TestJobCleaningAgent(object):
  """ Testing the single methods of JobCleaningAgent
  """

  @patch("DIRAC.WorkloadManagementSystem.Agent.JobCleaningAgent.JobDB.getDistinctJobAttributes", side_effect=mockReply)
  @patch("DIRAC.WorkloadManagementSystem.Agent.JobCleaningAgent.AgentModule", side_effect=mockAM)
  @patch("DIRAC.WorkloadManagementSystem.Agent.JobCleaningAgent.AgentModule.__init__", new=mockAM)
  def test__getAllowedJobTypes(self, _patch1, _patch2):
    """ Testing JobCleaningAgent()._getAllowedJobTypes()
    """

    jobCleaningAgent = JobCleaningAgent()
    jobCleaningAgent.log = gLogger
    jobCleaningAgent.log.setLevel('DEBUG')

    jobCleaningAgent.jobDB = JobDB()
    result = jobCleaningAgent._getAllowedJobTypes()

    assert result['OK']
    assert result['Value'] == []

  @patch("DIRAC.WorkloadManagementSystem.Agent.JobCleaningAgent.JobDB.selectJobs", side_effect=mockReply)
  @patch("DIRAC.WorkloadManagementSystem.Agent.JobCleaningAgent.AgentModule", side_effect=mockAM)
  @patch("DIRAC.WorkloadManagementSystem.Agent.JobCleaningAgent.AgentModule.__init__", new=mockAM)
  def test_removeJobsByStatus(self, _patch1, _patch2):
    """ Testing JobCleaningAgent().removeJobsByStatus()
    """

    jobCleaningAgent = JobCleaningAgent()
    jobCleaningAgent.log = gLogger
    jobCleaningAgent.log.setLevel('DEBUG')

    jobCleaningAgent.jobDB = JobDB()
    result = jobCleaningAgent.removeJobsByStatus({})

    assert result['OK']
    assert result['Value'] is None

  @patch("DIRAC.WorkloadManagementSystem.Agent.JobCleaningAgent.JobDB.selectJobs", side_effect=mockReply)
  @patch("DIRAC.WorkloadManagementSystem.Agent.JobCleaningAgent.AgentModule", side_effect=mockAM)
  @patch("DIRAC.WorkloadManagementSystem.Agent.JobCleaningAgent.AgentModule.__init__", new=mockAM)
  def test_deleteJobOversizedSandbox(self, _patch1, _patch2):
    """ Testing JobCleaningAgent().deleteJobOversizedSandbox()
    """

    jobCleaningAgent = JobCleaningAgent()
    jobCleaningAgent.log = gLogger
    jobCleaningAgent.log.setLevel('DEBUG')

    jobCleaningAgent.jobDB = JobDB()
    result = jobCleaningAgent.deleteJobOversizedSandbox([])

    assert result['OK']
    assert result['Value']['Successful'] == {}
    assert result['Value']['Failed'] == {}
