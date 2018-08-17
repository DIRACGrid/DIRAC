"""Check options for all agents."""

import inspect
import importlib
import logging
import pytest

from mock import patch, MagicMock as Mock

from DIRAC.tests.Utilities.assertingUtils import checkAgentOptions


AGENTS = [('DIRAC.ConfigurationSystem.Agent.Bdii2CSAgent', ['BannedCEs', 'BannedSEs', 'DryRun', 'AlternativeBDIIs',
                                                            'VO']),
          ('DIRAC.ConfigurationSystem.Agent.VOMS2CSAgent', ['mailFrom', 'DryRun', 'VO']),
          ('DIRAC.ConfigurationSystem.Agent.GOCDB2CSAgent', ['Cycles', 'DryRun']),
          ('DIRAC.RequestManagementSystem.Agent.CleanReqDBAgent', ['KickLimit', 'KickGraceHours', 'DeleteGraceDays']),
          ('DIRAC.RequestManagementSystem.Agent.RequestExecutingAgent', ['MaxProcess', 'ProcessTaskTimeout',
                                                                         'RequestsPerCycle', 'OperationHandlers',
                                                                         'MinProcess', 'MaxAttempts',
                                                                         'ProcessPoolQueueSize',
                                                                         'ProcessPoolSleep',
                                                                         'FTSMode', 'OperationHandlers']),
          ('DIRAC.FrameworkSystem.Agent.CAUpdateAgent', []),
          ('DIRAC.FrameworkSystem.Agent.MyProxyRenewalAgent', ['MinValidity', 'ValidityPeriod',
                                                               'MinimumLifeTime',
                                                               'RenewedLifeTime']),
          ('DIRAC.StorageManagementSystem.Agent.RequestPreparationAgent', []),
          ('DIRAC.StorageManagementSystem.Agent.RequestFinalizationAgent', []),
          ('DIRAC.StorageManagementSystem.Agent.StageMonitorAgent', []),
          ('DIRAC.StorageManagementSystem.Agent.StageRequestAgent', ['PinLifetime']),
          ('DIRAC.AccountingSystem.Agent.NetworkAgent', ['MaxCycles', 'MessageQueueURI', 'BufferTimeout']),
          ('DIRAC.WorkloadManagementSystem.Agent.JobCleaningAgent', []),
          ('DIRAC.WorkloadManagementSystem.Agent.JobAgent', ['FillingModeFlag', 'JobWrapperTemplate',
                                                             'MinimumTimeLeft']),
          ('DIRAC.WorkloadManagementSystem.Agent.StatesAccountingAgent', []),
          ('DIRAC.WorkloadManagementSystem.Agent.StalledJobAgent', ['StalledTimeHours', 'FailedTimeHours',
                                                                    'StalledJobsTolerantSites', 'Enable']),
          ('DIRAC.WorkloadManagementSystem.Agent.PilotStatusAgent', ['PilotAccountingEnabled', 'ClearPilotsDelay',
                                                                     'ClearAbortedPilotsDelay']),
          ('DIRAC.WorkloadManagementSystem.Agent.StatesMonitoringAgent', []),
          ('DIRAC.ResourceStatusSystem.Agent.SummarizeLogsAgent', []),
          ('DIRAC.ResourceStatusSystem.Agent.ElementInspectorAgent', ['elementType', 'maxNumberOfThreads',
                                                                      'limitQueueFeeder']),
          ('DIRAC.ResourceStatusSystem.Agent.EmailAgent', ['Status']),
          ('DIRAC.ResourceStatusSystem.Agent.TokenAgent', ['notifyHours', 'adminMail']),
          ('DIRAC.ResourceStatusSystem.Agent.CacheFeederAgent', []),
          ('DIRAC.ResourceStatusSystem.Agent.SiteInspectorAgent', ['elementType', 'maxNumberOfThreads',
                                                                   'limitQueueFeeder']),
          ('DIRAC.DataManagementSystem.Agent.FTSAgent', ['StageFiles', 'UseProxies', 'shifterProxy',
                                                         'FTSPlacementValidityPeriod', 'SubmitCommand',
                                                         'MonitorCommand', 'PinTime', 'MaxActiveJobsPerRoute',
                                                         'MaxRequests', 'MonitoringInterval', 'ProcessJobRequests']),
          ('DIRAC.DataManagementSystem.Agent.CleanFTSDBAgent', ['DeleteGraceDays']),
          ('DIRAC.DataManagementSystem.Agent.FTS3Agent', []),
          ('DIRAC.TransformationSystem.Agent.InputDataAgent', ['DateKey', 'TransformationTypes']),
          # ('DIRAC.TransformationSystem.Agent.WorkflowTaskAgent', []),  # not inheriting from AgentModule
          # ('DIRAC.TransformationSystem.Agent.RequestTaskAgent', []),  # not inheriting from AgentModule
          ('DIRAC.TransformationSystem.Agent.TaskManagerAgentBase', ['PluginLocation', 'BulkSubmission', 'shifterProxy',
                                                                     'ShifterCredentials', 'maxNumberOfThreads']),
          ('DIRAC.TransformationSystem.Agent.MCExtensionAgent', ['TransformationTypes', 'TasksPerIteration',
                                                                 'MaxFailureRate', 'MaxWaitingJobs']),
          ('DIRAC.TransformationSystem.Agent.TransformationCleaningAgent', ['EnableFlag', 'shifterProxy']),
          ('DIRAC.TransformationSystem.Agent.ValidateOutputDataAgent', ['TransformationTypes', 'DirectoryLocations',
                                                                        'ActiveSEs', 'TransfIDMeta']),
          ('DIRAC.TransformationSystem.Agent.TransformationAgent', ['PluginLocation', 'transformationStatus',
                                                                    'MaxFiles', 'MaxFilesToProcess',
                                                                    'TransformationTypes', 'ReplicaCacheValidity',
                                                                    'NoUnusedDelay', 'maxThreadsInPool']),
          ]

LOG = logging.getLogger('Test')


@pytest.mark.parametrize('agentPath, ignoreOptions', AGENTS)
def test_AgentOptions(caplog, agentPath, ignoreOptions):
  """Check that all options in ConfigTemplate are found in the initialize method, including default values."""
  caplog.set_level(logging.DEBUG)

  agentPathSplit = agentPath.split('.')
  systemName = agentPathSplit[1]
  agentName = agentPathSplit[-1]

  agentModule = importlib.import_module(agentPath)
  LOG.info("Agents: %s %s", agentPath, agentModule)
  agentClass = None

  # mock everything but the agentClass
  for name, member in inspect.getmembers(agentModule):
    LOG.info("Mocking? %s, %s, %s, isclass(%s)", name, callable(member), type(member), inspect.isclass(member))
    if name != 'AgentModule' and '_AgentModule__executeModuleCycle' in dir(member):
      LOG.info("Found the agent class %s, %s", name, member)
      agentClass = member
      continue
    elif name == 'AgentModule':
      continue
    if callable(member) or inspect.ismodule(member):
      LOG.info("Mocking: %s, %s, %s", name, member, type(member))
      agentModule.__dict__[name] = Mock(name=name)

  agentModule.__dict__['gConfig'] = Mock()
  agentModule.__dict__['gConfig'].getSections.return_value = dict(OK=True, Value=[])

  def returnDefault(*args):
    LOG.debug("ReturningDefault: %s, %s", args, type(args[1]))
    return args[1]

  getOptionMock = Mock(name="am_getOption", side_effect=returnDefault)

  def instrument(*args, **kwargs):
    """Mock some functions that come from the AgentModule and are not present otherwise."""
    args[0].am_getOption = getOptionMock
    args[0].log = Mock()
    args[0].am_getModuleParam = Mock()
    args[0].am_setOption = Mock()
    args[0].am_getWorkDirectory = Mock()
    args[0].am_getControlDirectory = Mock()
    return None
  initMock = Mock(side_effect=instrument)

  class MockAgentModule(object):
    def __init__(self, *args, **kwargs):
      instrument(self)

  patchBase = patch.object(agentClass, '__bases__', (MockAgentModule,))
  with \
          patchBase, \
          patch(agentPath + ".AgentModule.__init__", new=initMock), \
          patch("DIRAC.Core.Base.AgentModule.AgentModule.am_getOption", new=getOptionMock):
    patchBase.is_local = True
    agentInstance = agentClass(agentName="sys/name", loadName="sys/name")
    instrument(agentInstance)
  agentInstance.initialize()
  checkAgentOptions(getOptionMock, systemName, agentName, ignoreOptions=ignoreOptions)
