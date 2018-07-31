""" Test class for Stalled Job Agent
"""

# DIRAC Components
from DIRAC.WorkloadManagementSystem.Agent.StalledJobAgent import StalledJobAgent
from DIRAC.WorkloadManagementSystem.DB.JobDB import JobDB
from DIRAC import gLogger

gLogger.setLevel('DEBUG')


def test__failSubmittingJobs(mocker):
  """ Testing StalledJobAgent()._failSubmittingJobs()
  """

  mocker.patch("DIRAC.WorkloadManagementSystem.Agent.StalledJobAgent.AgentModule.__init__")

  stalledJobAgent = StalledJobAgent()
  stalledJobAgent.jobDB = JobDB()
  stalledJobAgent.log = gLogger
  stalledJobAgent.log.setLevel('DEBUG')

  result = stalledJobAgent._failSubmittingJobs()

  assert result['OK']


def test__failCompletedJobs(mocker):
  """ Testing StalledJobAgent()._failCompletedJobs()
  """

  mocker.patch("DIRAC.WorkloadManagementSystem.Agent.StalledJobAgent.AgentModule.__init__")

  stalledJobAgent = StalledJobAgent()
  stalledJobAgent.jobDB = JobDB()
  stalledJobAgent.log = gLogger
  stalledJobAgent.log.setLevel('DEBUG')

  result = stalledJobAgent._failCompletedJobs()

  assert result['OK']


def test__kickStuckJobs(mocker):
  """ Testing StalledJobAgent()._kickStuckJobs()
  """

  mocker.patch("DIRAC.WorkloadManagementSystem.Agent.StalledJobAgent.AgentModule.__init__")

  stalledJobAgent = StalledJobAgent()
  stalledJobAgent.jobDB = JobDB()
  stalledJobAgent.log = gLogger
  stalledJobAgent.log.setLevel('DEBUG')

  result = stalledJobAgent._kickStuckJobs()

  assert result['OK']


def test__failStalledJobs(mocker):
  """ Testing StalledJobAgent()._failStalledJobs()
  """

  mocker.patch("DIRAC.WorkloadManagementSystem.Agent.StalledJobAgent.AgentModule.__init__")

  stalledJobAgent = StalledJobAgent()
  stalledJobAgent.jobDB = JobDB()
  stalledJobAgent.log = gLogger
  stalledJobAgent.log.setLevel('DEBUG')

  result = stalledJobAgent._failStalledJobs(0)

  assert result['OK']
  assert result['Value'] == 0


def test__markStalledJobs(mocker):
  """ Testing StalledJobAgent()._markStalledJobs()
  """

  mocker.patch("DIRAC.WorkloadManagementSystem.Agent.StalledJobAgent.AgentModule.__init__")

  stalledJobAgent = StalledJobAgent()
  stalledJobAgent.jobDB = JobDB()
  stalledJobAgent.log = gLogger
  stalledJobAgent.log.setLevel('DEBUG')

  result = stalledJobAgent._markStalledJobs(0)

  assert result['OK']
