"""Check options for all agents."""

import logging
import pytest

from DIRAC.tests.Utilities.assertingUtils import AgentOptionsTest


AGENTS = [('DIRAC.AccountingSystem.Agent.NetworkAgent', ['MaxCycles', 'MessageQueueURI', 'BufferTimeout']),
          ('DIRAC.ConfigurationSystem.Agent.Bdii2CSAgent', ['BannedCEs', 'BannedSEs', 'DryRun', 'AlternativeBDIIs',
                                                            'VO']),
          ('DIRAC.ConfigurationSystem.Agent.GOCDB2CSAgent', ['Cycles', 'DryRun']),
          ('DIRAC.ConfigurationSystem.Agent.VOMS2CSAgent', ['mailFrom', 'DryRun', 'VO']),
          ('DIRAC.DataManagementSystem.Agent.CleanFTSDBAgent', ['DeleteGraceDays']),
          ('DIRAC.DataManagementSystem.Agent.FTS3Agent', []),
          ('DIRAC.DataManagementSystem.Agent.FTSAgent', ['StageFiles', 'UseProxies', 'shifterProxy',
                                                         'FTSPlacementValidityPeriod', 'SubmitCommand',
                                                         'MonitorCommand', 'PinTime', 'MaxActiveJobsPerRoute',
                                                         'MaxRequests', 'MonitoringInterval',
                                                         'ProcessJobRequests']),
          ('DIRAC.FrameworkSystem.Agent.CAUpdateAgent', []),
          ('DIRAC.FrameworkSystem.Agent.MyProxyRenewalAgent', ['MinValidity', 'ValidityPeriod', 'MinimumLifeTime',
                                                               'RenewedLifeTime']),
          ('DIRAC.FrameworkSystem.Agent.ErrorMessageMonitor', []),
          ('DIRAC.FrameworkSystem.Agent.SystemLoggingDBCleaner', ['RemoveDate']),
          ('DIRAC.FrameworkSystem.Agent.TopErrorMessagesReporter', []),
          ('DIRAC.RequestManagementSystem.Agent.CleanReqDBAgent', ['KickLimit', 'KickGraceHours', 'DeleteGraceDays']),
          ('DIRAC.RequestManagementSystem.Agent.RequestExecutingAgent', ['MaxProcess', 'ProcessTaskTimeout',
                                                                         'RequestsPerCycle', 'OperationHandlers',
                                                                         'MinProcess', 'MaxAttempts',
                                                                         'ProcessPoolQueueSize', 'ProcessPoolSleep',
                                                                         'FTSMode', 'OperationHandlers']),
          ('DIRAC.ResourceStatusSystem.Agent.CacheFeederAgent', []),
          ('DIRAC.ResourceStatusSystem.Agent.ElementInspectorAgent', ['elementType', 'maxNumberOfThreads',
                                                                      'limitQueueFeeder']),
          ('DIRAC.ResourceStatusSystem.Agent.EmailAgent', ['Status']),
          ('DIRAC.ResourceStatusSystem.Agent.SiteInspectorAgent', ['elementType', 'maxNumberOfThreads',
                                                                   'limitQueueFeeder']),
          ('DIRAC.ResourceStatusSystem.Agent.SummarizeLogsAgent', []),
          ('DIRAC.ResourceStatusSystem.Agent.TokenAgent', ['notifyHours', 'adminMail']),
          ('DIRAC.StorageManagementSystem.Agent.RequestFinalizationAgent', []),
          ('DIRAC.StorageManagementSystem.Agent.RequestPreparationAgent', []),
          ('DIRAC.StorageManagementSystem.Agent.StageMonitorAgent', []),
          ('DIRAC.StorageManagementSystem.Agent.StageRequestAgent', ['PinLifetime']),
          ('DIRAC.TransformationSystem.Agent.InputDataAgent', ['DateKey', 'TransformationTypes']),
          ('DIRAC.TransformationSystem.Agent.MCExtensionAgent', ['TransformationTypes', 'TasksPerIteration',
                                                                 'MaxFailureRate', 'MaxWaitingJobs']),
          ('DIRAC.TransformationSystem.Agent.TaskManagerAgentBase', ['PluginLocation', 'BulkSubmission', 'shifterProxy',
                                                                     'ShifterCredentials', 'maxNumberOfThreads']),
          ('DIRAC.TransformationSystem.Agent.TransformationAgent', ['PluginLocation', 'transformationStatus',
                                                                    'MaxFiles', 'MaxFilesToProcess',
                                                                    'TransformationTypes', 'ReplicaCacheValidity',
                                                                    'NoUnusedDelay', 'maxThreadsInPool']),
          ('DIRAC.TransformationSystem.Agent.TransformationCleaningAgent', ['EnableFlag', 'shifterProxy']),
          ('DIRAC.TransformationSystem.Agent.ValidateOutputDataAgent', ['TransformationTypes', 'DirectoryLocations',
                                                                        'ActiveSEs', 'TransfIDMeta']),
          # ('DIRAC.TransformationSystem.Agent.RequestTaskAgent', []),  # not inheriting from AgentModule
          # ('DIRAC.TransformationSystem.Agent.WorkflowTaskAgent', []),  # not inheriting from AgentModule
          ('DIRAC.WorkloadManagementSystem.Agent.JobAgent', ['FillingModeFlag', 'JobWrapperTemplate',
                                                             'MinimumTimeLeft']),
          ('DIRAC.WorkloadManagementSystem.Agent.JobCleaningAgent', []),
          ('DIRAC.WorkloadManagementSystem.Agent.PilotStatusAgent', ['PilotAccountingEnabled', 'ClearPilotsDelay',
                                                                     'ClearAbortedPilotsDelay']),
          ('DIRAC.WorkloadManagementSystem.Agent.StalledJobAgent', ['StalledTimeHours', 'FailedTimeHours',
                                                                    'StalledJobsTolerantSites', 'Enable']),
          ('DIRAC.WorkloadManagementSystem.Agent.StatesAccountingAgent', []),
          ('DIRAC.WorkloadManagementSystem.Agent.StatesMonitoringAgent', []),
          ('DIRAC.WorkloadManagementSystem.Agent.SiteDirector', ['PilotLogLevel', 'SendPilotAccounting',
                                                                 'MaxJobsInFillMode', 'FailedQueueCycleFactor', 'Site',
                                                                 'GetPilotOutput', 'CETypes', 'ExtraPilotOptions',
                                                                 'GridEnv', 'VO', 'Community', 'Group', 'Pilot3']),
          # ('DIRAC.WorkloadManagementSystem.Agent.MultiProcessorSiteDirector', []),  #does not inherit from AgentModule
          ]


LOG = logging.getLogger('Test')


@pytest.mark.parametrize('agentPath, ignoreOptions', AGENTS)
def test_AgentOptions(agentPath, ignoreOptions, caplog, mocker):
  """Check that all options in ConfigTemplate are found in the initialize method, including default values."""
  caplog.set_level(logging.DEBUG)
  AgentOptionsTest(agentPath, ignoreOptions, mocker=mocker)
