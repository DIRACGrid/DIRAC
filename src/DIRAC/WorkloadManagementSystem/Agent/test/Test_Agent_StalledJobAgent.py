""" Test class for Stalled Job Agent
"""

# imports
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
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
  mocker.patch(
      "DIRAC.WorkloadManagementSystem.Agent.StalledJobAgent.AgentModule._AgentModule__moduleProperties",
      side_effect=lambda x, y=None: y, create=True
  )
  mocker.patch("DIRAC.WorkloadManagementSystem.Agent.StalledJobAgent.JobDB.__init__", side_effect=mockNone)
  mocker.patch("DIRAC.WorkloadManagementSystem.Agent.StalledJobAgent.JobLoggingDB.__init__", side_effect=mockNone)

  stalledJobAgent = StalledJobAgent()
  stalledJobAgent._AgentModule__configDefaults = mockAM
  stalledJobAgent.log = gLogger
  stalledJobAgent.initialize()
  stalledJobAgent.jobDB.log = gLogger
  stalledJobAgent.log.setLevel('DEBUG')

  result = stalledJobAgent._failSubmittingJobs()

  assert not result['OK']


def test__kickStuckJobs(mocker):
  """ Testing StalledJobAgent()._kickStuckJobs()
  """

  mocker.patch("DIRAC.WorkloadManagementSystem.Agent.StalledJobAgent.AgentModule.__init__")
  mocker.patch(
      "DIRAC.WorkloadManagementSystem.Agent.StalledJobAgent.AgentModule._AgentModule__moduleProperties",
      side_effect=lambda x, y=None: y, create=True
  )
  mocker.patch("DIRAC.WorkloadManagementSystem.Agent.StalledJobAgent.JobDB.__init__", side_effect=mockNone)
  mocker.patch("DIRAC.WorkloadManagementSystem.Agent.StalledJobAgent.JobLoggingDB.__init__", side_effect=mockNone)

  stalledJobAgent = StalledJobAgent()
  stalledJobAgent._AgentModule__configDefaults = mockAM
  stalledJobAgent.log = gLogger
  stalledJobAgent.initialize()
  stalledJobAgent.jobDB.log = gLogger
  stalledJobAgent.log.setLevel('DEBUG')

  result = stalledJobAgent._kickStuckJobs()

  assert not result['OK']


def test__failStalledJobs(mocker):
  """ Testing StalledJobAgent()._failStalledJobs()
  """

  mocker.patch("DIRAC.WorkloadManagementSystem.Agent.StalledJobAgent.AgentModule.__init__")
  mocker.patch(
      "DIRAC.WorkloadManagementSystem.Agent.StalledJobAgent.AgentModule._AgentModule__moduleProperties",
      side_effect=lambda x, y=None: y, create=True
  )
  mocker.patch("DIRAC.WorkloadManagementSystem.Agent.StalledJobAgent.JobDB.__init__", side_effect=mockNone)
  mocker.patch("DIRAC.WorkloadManagementSystem.Agent.StalledJobAgent.JobLoggingDB.__init__", side_effect=mockNone)

  stalledJobAgent = StalledJobAgent()
  stalledJobAgent._AgentModule__configDefaults = mockAM
  stalledJobAgent.log = gLogger
  stalledJobAgent.initialize()
  stalledJobAgent.jobDB.log = gLogger
  stalledJobAgent.log.setLevel('DEBUG')

  result = stalledJobAgent._failStalledJobs(0)

  assert not result['OK']


def test__markStalledJobs(mocker):
  """ Testing StalledJobAgent()._markStalledJobs()
  """

  mocker.patch("DIRAC.WorkloadManagementSystem.Agent.StalledJobAgent.AgentModule.__init__")
  mocker.patch(
      "DIRAC.WorkloadManagementSystem.Agent.StalledJobAgent.AgentModule._AgentModule__moduleProperties",
      side_effect=lambda x, y=None: y, create=True
  )
  mocker.patch("DIRAC.WorkloadManagementSystem.Agent.StalledJobAgent.JobDB.__init__", side_effect=mockNone)
  mocker.patch("DIRAC.WorkloadManagementSystem.Agent.StalledJobAgent.JobLoggingDB.__init__", side_effect=mockNone)

  stalledJobAgent = StalledJobAgent()
  stalledJobAgent._AgentModule__configDefaults = mockAM
  stalledJobAgent.log = gLogger
  stalledJobAgent.initialize()
  stalledJobAgent.jobDB.log = gLogger
  stalledJobAgent.log.setLevel('DEBUG')
  stalledJobAgent.stalledTime = 120

  result = stalledJobAgent._markStalledJobs(0)

  assert not result['OK']
