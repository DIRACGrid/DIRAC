""" Test class for Stalled Job Agent
"""

# imports
from __future__ import absolute_import
from mock import MagicMock

# DIRAC Components
from DIRAC.WorkloadManagementSystem.Agent.StalledJobAgent import StalledJobAgent
from DIRAC import gLogger

# Mock Objects
mockAM = MagicMock()
mockNone = MagicMock()
mockNone.return_value = None

gLogger.setLevel('DEBUG')


def test__failSubmittingJobs(mocker):
  """ Testing StalledJobAgent()._failSubmittingJobs()
  """

  mocker.patch("DIRAC.WorkloadManagementSystem.Agent.StalledJobAgent.AgentModule.__init__")
  mocker.patch("DIRAC.WorkloadManagementSystem.Agent.StalledJobAgent.AgentModule.am_getOption", side_effect=mockAM)
  mocker.patch("DIRAC.WorkloadManagementSystem.Agent.StalledJobAgent.JobDB.__init__", side_effect=mockNone)
  mocker.patch("DIRAC.WorkloadManagementSystem.Agent.StalledJobAgent.JobLoggingDB.__init__", side_effect=mockNone)

  stalledJobAgent = StalledJobAgent()
  stalledJobAgent._AgentModule__configDefaults = mockAM
  stalledJobAgent.initialize()
  stalledJobAgent.jobDB.log = gLogger
  stalledJobAgent.log = gLogger
  stalledJobAgent.log.setLevel('DEBUG')

  result = stalledJobAgent._failSubmittingJobs()

  assert not result['OK']


def test__failCompletedJobs(mocker):
  """ Testing StalledJobAgent()._failCompletedJobs()
  """

  mocker.patch("DIRAC.WorkloadManagementSystem.Agent.StalledJobAgent.AgentModule.__init__")
  mocker.patch("DIRAC.WorkloadManagementSystem.Agent.StalledJobAgent.AgentModule.am_getOption", side_effect=mockAM)
  mocker.patch("DIRAC.WorkloadManagementSystem.Agent.StalledJobAgent.JobDB.__init__", side_effect=mockNone)
  mocker.patch("DIRAC.WorkloadManagementSystem.Agent.StalledJobAgent.JobLoggingDB.__init__", side_effect=mockNone)

  stalledJobAgent = StalledJobAgent()
  stalledJobAgent._AgentModule__configDefaults = mockAM
  stalledJobAgent.initialize()
  stalledJobAgent.jobDB.log = gLogger
  stalledJobAgent.log = gLogger
  stalledJobAgent.log.setLevel('DEBUG')

  result = stalledJobAgent._failCompletedJobs()

  assert not result['OK']


def test__kickStuckJobs(mocker):
  """ Testing StalledJobAgent()._kickStuckJobs()
  """

  mocker.patch("DIRAC.WorkloadManagementSystem.Agent.StalledJobAgent.AgentModule.__init__")
  mocker.patch("DIRAC.WorkloadManagementSystem.Agent.StalledJobAgent.AgentModule.am_getOption", side_effect=mockAM)
  mocker.patch("DIRAC.WorkloadManagementSystem.Agent.StalledJobAgent.JobDB.__init__", side_effect=mockNone)
  mocker.patch("DIRAC.WorkloadManagementSystem.Agent.StalledJobAgent.JobLoggingDB.__init__", side_effect=mockNone)

  stalledJobAgent = StalledJobAgent()
  stalledJobAgent._AgentModule__configDefaults = mockAM
  stalledJobAgent.initialize()
  stalledJobAgent.jobDB.log = gLogger
  stalledJobAgent.log = gLogger
  stalledJobAgent.log.setLevel('DEBUG')

  result = stalledJobAgent._kickStuckJobs()

  assert not result['OK']


def test__failStalledJobs(mocker):
  """ Testing StalledJobAgent()._failStalledJobs()
  """

  mocker.patch("DIRAC.WorkloadManagementSystem.Agent.StalledJobAgent.AgentModule.__init__")
  mocker.patch("DIRAC.WorkloadManagementSystem.Agent.StalledJobAgent.AgentModule.am_getOption", side_effect=mockAM)
  mocker.patch("DIRAC.WorkloadManagementSystem.Agent.StalledJobAgent.JobDB.__init__", side_effect=mockNone)
  mocker.patch("DIRAC.WorkloadManagementSystem.Agent.StalledJobAgent.JobLoggingDB.__init__", side_effect=mockNone)

  stalledJobAgent = StalledJobAgent()
  stalledJobAgent._AgentModule__configDefaults = mockAM
  stalledJobAgent.initialize()
  stalledJobAgent.jobDB.log = gLogger
  stalledJobAgent.log = gLogger
  stalledJobAgent.log.setLevel('DEBUG')

  result = stalledJobAgent._failStalledJobs(0)

  assert not result['OK']


def test__markStalledJobs(mocker):
  """ Testing StalledJobAgent()._markStalledJobs()
  """

  mocker.patch("DIRAC.WorkloadManagementSystem.Agent.StalledJobAgent.AgentModule.__init__")
  mocker.patch("DIRAC.WorkloadManagementSystem.Agent.StalledJobAgent.AgentModule.am_getOption", side_effect=mockAM)
  mocker.patch("DIRAC.WorkloadManagementSystem.Agent.StalledJobAgent.JobDB.__init__", side_effect=mockNone)
  mocker.patch("DIRAC.WorkloadManagementSystem.Agent.StalledJobAgent.JobLoggingDB.__init__", side_effect=mockNone)

  stalledJobAgent = StalledJobAgent()
  stalledJobAgent._AgentModule__configDefaults = mockAM
  stalledJobAgent.initialize()
  stalledJobAgent.jobDB.log = gLogger
  stalledJobAgent.log = gLogger
  stalledJobAgent.log.setLevel('DEBUG')

  result = stalledJobAgent._markStalledJobs(0)

  assert not result['OK']
