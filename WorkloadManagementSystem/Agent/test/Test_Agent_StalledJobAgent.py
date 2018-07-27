""" Test class for Stalled Job Agent
"""

# imports
from mock import MagicMock, patch

# DIRAC Components
from DIRAC.WorkloadManagementSystem.Agent.StalledJobAgent import StalledJobAgent
from DIRAC.WorkloadManagementSystem.DB.JobDB import JobDB
from DIRAC import gLogger

gLogger.setLevel('DEBUG')

# Mock Objects
mockAM = MagicMock()


class TestStalledJobAgent(object):
  """ Testing the single methods of StalledJobAgent
  """

  @patch("DIRAC.WorkloadManagementSystem.Agent.StalledJobAgent.AgentModule", side_effect=mockAM)
  @patch("DIRAC.WorkloadManagementSystem.Agent.StalledJobAgent.AgentModule.__init__", new=mockAM)
  def test__failSubmittingJobs(self, _patch1):
    """ Testing StalledJobAgent()._failSubmittingJobs()
    """

    stalledJobAgent = StalledJobAgent()
    stalledJobAgent.jobDB = JobDB()
    stalledJobAgent.log = gLogger
    stalledJobAgent.log.setLevel('DEBUG')

    result = stalledJobAgent._failSubmittingJobs()

    assert result['OK']

  @patch("DIRAC.WorkloadManagementSystem.Agent.StalledJobAgent.AgentModule", side_effect=mockAM)
  @patch("DIRAC.WorkloadManagementSystem.Agent.StalledJobAgent.AgentModule.__init__", new=mockAM)
  def test__failCompletedJobs(self, _patch1):
    """ Testing StalledJobAgent()._failCompletedJobs()
    """

    stalledJobAgent = StalledJobAgent()
    stalledJobAgent.jobDB = JobDB()
    stalledJobAgent.log = gLogger
    stalledJobAgent.log.setLevel('DEBUG')

    result = stalledJobAgent._failCompletedJobs()

    assert result['OK']

  @patch("DIRAC.WorkloadManagementSystem.Agent.StalledJobAgent.AgentModule", side_effect=mockAM)
  @patch("DIRAC.WorkloadManagementSystem.Agent.StalledJobAgent.AgentModule.__init__", new=mockAM)
  def test__kickStuckJobs(self, _patch1):
    """ Testing StalledJobAgent()._kickStuckJobs()
    """

    stalledJobAgent = StalledJobAgent()
    stalledJobAgent.jobDB = JobDB()
    stalledJobAgent.log = gLogger
    stalledJobAgent.log.setLevel('DEBUG')

    result = stalledJobAgent._kickStuckJobs()

    assert result['OK']

  @patch("DIRAC.WorkloadManagementSystem.Agent.StalledJobAgent.AgentModule", side_effect=mockAM)
  @patch("DIRAC.WorkloadManagementSystem.Agent.StalledJobAgent.AgentModule.__init__", new=mockAM)
  def test__failStalledJobs(self, _patch1):
    """ Testing StalledJobAgent()._failStalledJobs()
    """

    stalledJobAgent = StalledJobAgent()
    stalledJobAgent.jobDB = JobDB()
    stalledJobAgent.log = gLogger
    stalledJobAgent.log.setLevel('DEBUG')

    result = stalledJobAgent._failStalledJobs(0)

    assert result['OK']
    assert result['Value'] == 0

  @patch("DIRAC.WorkloadManagementSystem.Agent.StalledJobAgent.AgentModule", side_effect=mockAM)
  @patch("DIRAC.WorkloadManagementSystem.Agent.StalledJobAgent.AgentModule.__init__", new=mockAM)
  def test__markStalledJobs(self, _patch1):
    """ Testing StalledJobAgent()._markStalledJobs()
    """

    stalledJobAgent = StalledJobAgent()
    stalledJobAgent.jobDB = JobDB()
    stalledJobAgent.log = gLogger
    stalledJobAgent.log.setLevel('DEBUG')

    result = stalledJobAgent._markStalledJobs(0)

    assert result['OK']
