"""Check options for all agents."""

import logging
import pytest

from DIRAC.tests.Utilities.assertingUtils import AgentOptionsTest
from DIRAC import S_OK

AGENTS = [('DIRAC.AccountingSystem.Agent.NetworkAgent', {'IgnoreOptions': ['MaxCycles', 'MessageQueueURI',
                                                                           'BufferTimeout']}),
          ('DIRAC.ConfigurationSystem.Agent.Bdii2CSAgent', {'IgnoreOptions': ['BannedCEs', 'BannedSEs', 'DryRun',
                                                                              'AlternativeBDIIs', 'VO']}),
          ('DIRAC.ConfigurationSystem.Agent.GOCDB2CSAgent', {'IgnoreOptions': ['Cycles', 'DryRun']}),
          ('DIRAC.ConfigurationSystem.Agent.VOMS2CSAgent', {'IgnoreOptions': ['VO']}),
          ('DIRAC.DataManagementSystem.Agent.FTS3Agent', {}),
          ('DIRAC.FrameworkSystem.Agent.CAUpdateAgent', {}),
          ('DIRAC.FrameworkSystem.Agent.MyProxyRenewalAgent', {'IgnoreOptions': ['MinValidity', 'ValidityPeriod',
                                                                                 'MinimumLifeTime',
                                                                                 'RenewedLifeTime']}),
          ('DIRAC.FrameworkSystem.Agent.ErrorMessageMonitor', {}),
          ('DIRAC.FrameworkSystem.Agent.SystemLoggingDBCleaner', {'IgnoreOptions': ['RemoveDate']}),
          ('DIRAC.FrameworkSystem.Agent.TopErrorMessagesReporter', {}),
          ('DIRAC.RequestManagementSystem.Agent.CleanReqDBAgent', {'IgnoreOptions': ['KickLimit', 'KickGraceHours',
                                                                                     'DeleteGraceDays']}),
          ('DIRAC.RequestManagementSystem.Agent.RequestExecutingAgent', {'IgnoreOptions': ['MaxProcess',
                                                                                           'ProcessTaskTimeout',
                                                                                           'RequestsPerCycle',
                                                                                           'OperationHandlers',
                                                                                           'MinProcess', 'MaxAttempts',
                                                                                           'ProcessPoolQueueSize',
                                                                                           'ProcessPoolSleep',
                                                                                           'FTSMode',
                                                                                           'OperationHandlers'],
                                                                         'SpecialMocks': {'gConfig': S_OK([])}}),
          ('DIRAC.ResourceStatusSystem.Agent.CacheFeederAgent', {}),
          ('DIRAC.ResourceStatusSystem.Agent.ElementInspectorAgent', {}),
          ('DIRAC.ResourceStatusSystem.Agent.EmailAgent', {}),
          ('DIRAC.ResourceStatusSystem.Agent.SiteInspectorAgent', {}),
          ('DIRAC.ResourceStatusSystem.Agent.SummarizeLogsAgent', {}),
          ('DIRAC.ResourceStatusSystem.Agent.TokenAgent', {}),
          ('DIRAC.StorageManagementSystem.Agent.RequestFinalizationAgent', {}),
          ('DIRAC.StorageManagementSystem.Agent.RequestPreparationAgent', {}),
          ('DIRAC.StorageManagementSystem.Agent.StageMonitorAgent', {}),
          ('DIRAC.StorageManagementSystem.Agent.StageRequestAgent', {'IgnoreOptions': ['PinLifetime']}),
          ('DIRAC.TransformationSystem.Agent.DataRecoveryAgent', {}),
          ('DIRAC.TransformationSystem.Agent.InputDataAgent', {'IgnoreOptions': ['DateKey', 'TransformationTypes']}),
          ('DIRAC.TransformationSystem.Agent.MCExtensionAgent', {'IgnoreOptions': ['TransformationTypes',
                                                                                   'TasksPerIteration',
                                                                                   'MaxFailureRate',
                                                                                   'MaxWaitingJobs']}),
          ('DIRAC.TransformationSystem.Agent.TaskManagerAgentBase', {'IgnoreOptions': ['PluginLocation',
                                                                                       'BulkSubmission',
                                                                                       'shifterProxy',
                                                                                       'ShifterCredentials',
                                                                                       'maxNumberOfThreads']}),
          ('DIRAC.TransformationSystem.Agent.TransformationAgent', {'IgnoreOptions': ['PluginLocation',
                                                                                      'transformationStatus',
                                                                                      'MaxFiles', 'MaxFilesToProcess',
                                                                                      'TransformationTypes',
                                                                                      'ReplicaCacheValidity',
                                                                                      'NoUnusedDelay',
                                                                                      'maxThreadsInPool']}),
          ('DIRAC.TransformationSystem.Agent.TransformationCleaningAgent', {'IgnoreOptions': ['EnableFlag',
                                                                                              'shifterProxy']}),
          ('DIRAC.TransformationSystem.Agent.ValidateOutputDataAgent', {'IgnoreOptions': ['TransformationTypes',
                                                                                          'DirectoryLocations',
                                                                                          'TransfIDMeta']}),
          # ('DIRAC.TransformationSystem.Agent.RequestTaskAgent', {}),  # not inheriting from AgentModule
          # ('DIRAC.TransformationSystem.Agent.WorkflowTaskAgent', {}),  # not inheriting from AgentModule
          ('DIRAC.WorkloadManagementSystem.Agent.JobAgent', {'IgnoreOptions': ['FillingModeFlag', 'JobWrapperTemplate',
                                                                               'MinimumTimeLeft']}),
          ('DIRAC.WorkloadManagementSystem.Agent.JobCleaningAgent', {}),
          ('DIRAC.WorkloadManagementSystem.Agent.PilotStatusAgent', {'IgnoreOptions': ['PilotAccountingEnabled',
                                                                                       'ClearPilotsDelay',
                                                                                       'ClearAbortedPilotsDelay']}),
          ('DIRAC.WorkloadManagementSystem.Agent.StalledJobAgent', {'IgnoreOptions': ['StalledTimeHours',
                                                                                      'FailedTimeHours',
                                                                                      'StalledJobsTolerantSites',
                                                                                      'Enable']}),
          ('DIRAC.WorkloadManagementSystem.Agent.StatesAccountingAgent', {}),
          ('DIRAC.WorkloadManagementSystem.Agent.StatesMonitoringAgent', {}),
          ('DIRAC.WorkloadManagementSystem.Agent.SiteDirector',
           {'SpecialMocks': {'findGenericPilotCredentials': S_OK(('a', 'b', 'c'))}}),
          # ('DIRAC.WorkloadManagementSystem.Agent.MultiProcessorSiteDirector', {}),  # not inheriting from AgentModule
          ]


LOG = logging.getLogger('Test')


@pytest.mark.parametrize('agentPath, options', AGENTS)
def test_AgentOptions(agentPath, options, caplog, mocker):
  """Check that all options in ConfigTemplate are found in the initialize method, including default values."""
  caplog.set_level(logging.DEBUG)
  AgentOptionsTest(agentPath, options, mocker=mocker)
