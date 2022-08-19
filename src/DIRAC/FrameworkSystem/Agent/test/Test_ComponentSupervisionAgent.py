"""Test ComponentSupervisionAgent."""
import unittest
import sys
from datetime import datetime, timedelta
from unittest.mock import MagicMock, call, patch
import psutil

import DIRAC
from DIRAC import gLogger
from DIRAC import S_OK, S_ERROR

import DIRAC.FrameworkSystem.Agent.ComponentSupervisionAgent as MAA
from DIRAC.FrameworkSystem.Agent.ComponentSupervisionAgent import ComponentSupervisionAgent, NO_RESTART


def clientMock(ret):
    """Return an Client which returns **ret**."""
    clientModuleMock = MagicMock(name="Client Module")
    clientClassMock = MagicMock(name="Client Class")
    clientClassMock.ping.return_value = ret
    clientModuleMock.return_value = clientClassMock
    return clientModuleMock


def mockComponentSection(*_args, **kwargs):
    """Mock the PathFinder.getComponentSection to return individual componentSections."""
    system = kwargs.get("system")
    component = kwargs.get("component")
    return f"/Systems/{system}/Production/Services/{component}"


def mockURLSection(*_args, **kwargs):
    """Mock the PathFinder.getSystemURLSection to return individual componentSections."""
    system = kwargs.get("system")
    return "/Systems/%s/Production/URLs/" % system


class TestComponentSupervisionAgent(unittest.TestCase):
    """TestComponentSupervisionAgent class."""

    def setUp(self):
        """Set up test environment."""
        self.agent = MAA
        self.agent.AgentModule = MagicMock()
        self.agent.NotificationClient = MagicMock(
            spec=DIRAC.FrameworkSystem.Client.NotificationClient.NotificationClient
        )

        self.restartAgent = ComponentSupervisionAgent()
        self.restartAgent.log = gLogger
        self.restartAgent.log.setLevel("DEBUG")
        self.restartAgent.sysAdminClient = MagicMock()
        self.restartAgent.csAPI = MagicMock()
        self.restartAgent.enabled = True
        self.restartAgent.restartAgents = True
        self.restartAgent.doNotRestartInstancePattern = ["Foo"]

    @classmethod
    def tearDownClass(cls):
        """Remove monitoragent module after tests to avoid side effects."""
        sys.modules.pop("DIRAC.FrameworkSystem.Agent.ComponentSupervisionAgent")

    @staticmethod
    def getPSMock():
        """Mock psutil."""
        psMock = MagicMock(name="psutil")
        procMock2 = MagicMock(name="process2kill")
        psMock.wait_procs.return_value = ("gone", [procMock2])
        procMock = MagicMock(name="process")
        procMock.children.return_value = []
        psMock.Process.return_value = procMock
        return psMock

    def test_init(self):
        """Test the init function."""
        self.assertIsInstance(self.restartAgent, ComponentSupervisionAgent)
        self.assertIsInstance(self.restartAgent.nClient, MagicMock)
        self.assertIsInstance(self.restartAgent.sysAdminClient, MagicMock)
        self.assertTrue(self.restartAgent.enabled)
        self.assertEqual(self.restartAgent.addressFrom, "")

    def test_begin_execution(self):
        """Test for the beginExecution function."""
        self.restartAgent.accounting["Junk"]["Funk"] = 1
        self.restartAgent.am_getOption = MagicMock()
        getOptionCalls = [
            call("Setup", self.restartAgent.setup),
            call("EnableFlag", True),
            call("MailTo", self.restartAgent.addressTo),
            call("MailFrom", self.restartAgent.addressFrom),
        ]

        self.restartAgent.getRunningInstances = MagicMock(return_value=S_OK())
        with patch("DIRAC.FrameworkSystem.Agent.ComponentSupervisionAgent.CSAPI", new=MagicMock()):
            self.restartAgent.beginExecution()

        self.restartAgent.am_getOption.assert_has_calls(getOptionCalls, any_order=True)
        self.restartAgent.getRunningInstances.assert_called()

        # accounting dictionary should be cleared
        self.assertEqual(self.restartAgent.accounting, {})

    def test_send_notification(self):
        """Test for the sendNotification function."""
        self.restartAgent.addressTo = ["foo@bar.baz"]
        self.restartAgent.errors = []
        self.restartAgent.accounting = {}

        # send mail should not be called if there are no errors and accounting information
        self.restartAgent.sendNotification()
        self.restartAgent.nClient.sendMail.assert_not_called()

        # send mail should be called if there are errors but no accounting information
        self.restartAgent.errors = ["some error"]
        self.restartAgent.sendNotification()
        self.restartAgent.nClient.sendMail.assert_called()

        # send email should be called if there is accounting information but no errors
        self.restartAgent.nClient.sendMail.reset_mock()
        self.restartAgent.errors = []
        self.restartAgent.accounting = {"Agent1": {"LogAge": 123, "Treatment": "Agent Restarted"}}
        self.restartAgent.sendNotification()
        self.restartAgent.nClient.sendMail.assert_called()

        # try sending email to all addresses even if we get error for sending email to some address
        self.restartAgent.nClient.sendMail.reset_mock()
        self.restartAgent.errors = ["some error"]
        self.restartAgent.addressTo = ["name1@cern.ch", "name2@cern.ch"]
        self.restartAgent.nClient.sendMail.return_value = S_ERROR()
        self.restartAgent.sendNotification()
        self.assertEqual(len(self.restartAgent.nClient.sendMail.mock_calls), len(self.restartAgent.addressTo))

        # accounting dict and errors list should be cleared after notification is sent
        self.assertEqual(self.restartAgent.accounting, {})
        self.assertEqual(self.restartAgent.errors, [])

    @patch("DIRAC.FrameworkSystem.Agent.ComponentSupervisionAgent.gConfig", new=MagicMock())
    @patch("DIRAC.FrameworkSystem.Agent.ComponentSupervisionAgent.PathFinder.getComponentSection", new=MagicMock())
    def test_get_running_instances(self):
        """Test for the getRunningInstances function."""
        self.restartAgent.sysAdminClient.getOverallStatus = MagicMock()
        self.restartAgent.sysAdminClient.getOverallStatus.return_value = S_ERROR()

        res = self.restartAgent.getRunningInstances(instanceType="Agents")
        self.assertFalse(res["OK"])

        agents = {
            "Agents": {
                "DataManagement": {
                    "FTS3Agent": {
                        "MEM": "0.3",
                        "Setup": True,
                        "PID": "18128",
                        "RunitStatus": "Run",
                        "Module": "CleanFTSDBAgent",
                        "Installed": True,
                        "VSZ": "375576",
                        "Timeup": "29841",
                        "CPU": "0.0",
                        "RSS": "55452",
                    }
                },
                "Framework": {
                    "ErrorMessageMonitor": {
                        "MEM": "0.3",
                        "Setup": True,
                        "PID": "2303",
                        "RunitStatus": "Run",
                        "Module": "ErrorMessageMonitor",
                        "Installed": True,
                        "VSZ": "380392",
                        "Timeup": "3380292",
                        "CPU": "0.0",
                        "RSS": "56172",
                    }
                },
                "System": {
                    "Off": {
                        "MEM": "0.3",
                        "Setup": True,
                        "PID": "---",
                        "RunitStatus": "Down",
                        "Module": "ErrorMessageMonitor",
                        "Installed": True,
                        "VSZ": "380392",
                        "Timeup": "3380292",
                        "CPU": "0.0",
                        "RSS": "56172",
                    }
                },
            }
        }
        agents["Agents"]["DataManagement"]["FTSAgent"] = {
            "Setup": False,
            "PID": 0,
            "RunitStatus": "Unknown",
            "Module": "FTSAgent",
            "Installed": False,
            "Timeup": 0,
        }

        self.restartAgent.sysAdminClient.getOverallStatus.return_value = S_OK(agents)
        res = self.restartAgent.getRunningInstances(instanceType="Agents")

        # only insalled agents with RunitStatus RUN should be returned
        self.assertTrue("FTSAgent" not in res["Value"])
        self.assertTrue("FTS3Agent" in res["Value"])
        self.assertTrue("ErrorMessageMonitor" in res["Value"])
        for agent in res["Value"]:
            self.assertTrue("PollingTime" in res["Value"][agent])
            self.assertTrue("LogFileLocation" in res["Value"][agent])
            self.assertTrue("PID" in res["Value"][agent])

    @patch("DIRAC.FrameworkSystem.Agent.ComponentSupervisionAgent.gConfig", new=MagicMock())
    def test_execute(self):
        """Test for the execute function."""
        self.restartAgent.sendNotification = MagicMock()

        agentOne = "FTS3Agent"
        agentTwo = "FTSAgent"
        agentOnePollingTime = 100
        agentTwoPollingTime = 200
        agentOneLogLoc = "/fake/loc1"
        agentTwoLogLoc = "/fake/loc2"
        agentOnePID = "12345"
        agentTwoPID = "54321"

        self.restartAgent.agents = {
            agentOne: {"PollingTime": agentOnePollingTime, "LogFileLocation": agentOneLogLoc, "PID": agentOnePID},
            agentTwo: {"PollingTime": agentTwoPollingTime, "LogFileLocation": agentTwoLogLoc, "PID": agentTwoPID},
        }

        self.restartAgent.checkAgent = MagicMock(side_effect=[S_OK(), S_ERROR()])
        self.restartAgent.componentControl = MagicMock(return_value=S_OK())

        res = self.restartAgent.execute()

        self.assertFalse(res["OK"])

        calls = [call(agentOne, self.restartAgent.agents[agentOne]), call(agentTwo, self.restartAgent.agents[agentTwo])]
        self.restartAgent.checkAgent.assert_has_calls(calls, any_order=True)

        # email notification should be sent at the end of every agent cycle
        self.restartAgent.sendNotification.assert_called()

    @patch("DIRAC.FrameworkSystem.Agent.ComponentSupervisionAgent.gConfig", new=MagicMock())
    def test_execute_2a(self):
        """Test for the execute function."""
        self.restartAgent.sendNotification = MagicMock()
        self.restartAgent.componentControl = MagicMock(return_value=S_ERROR("Stopped does not exist"))
        self.restartAgent.checkURLs = MagicMock(return_value=S_ERROR("SomeFailure"))
        res = self.restartAgent.execute()
        self.assertFalse(res["OK"])
        # email notification should be sent at the end of every agent cycle
        self.restartAgent.sendNotification.assert_called()

    @patch("DIRAC.FrameworkSystem.Agent.ComponentSupervisionAgent.gConfig", new=MagicMock())
    def test_execute_2b(self):
        """Test for the execute function."""
        self.restartAgent.sendNotification = MagicMock()
        self.restartAgent.componentControl = MagicMock(return_value=S_ERROR())
        self.restartAgent.checkURLs = MagicMock()
        res = self.restartAgent.execute()
        self.assertFalse(res["OK"])
        self.assertIn("Failure to control components ", self.restartAgent.errors)
        # email notification should be sent at the end of every agent cycle
        self.restartAgent.sendNotification.assert_called()
        self.restartAgent.checkURLs.assert_not_called()

    @patch("DIRAC.FrameworkSystem.Agent.ComponentSupervisionAgent.gConfig", new=MagicMock())
    def test_execute_2c(self):
        """Test for the execute function."""
        self.restartAgent.sendNotification = MagicMock()
        self.restartAgent.componentControl = MagicMock(return_value=S_OK())
        self.restartAgent.checkURLs = MagicMock(return_value=S_OK())
        res = self.restartAgent.execute()
        self.assertEqual([], self.restartAgent.errors)
        self.assertTrue(res["OK"])
        # email notification should be sent at the end of every agent cycle
        self.restartAgent.sendNotification.assert_called()
        self.restartAgent.checkURLs.assert_called_once()

    def test_check_agent(self):
        """Test for the checkAgent function."""
        self.restartAgent.getLastAccessTime = MagicMock()
        self.restartAgent.restartInstance = MagicMock(return_value=S_OK())

        agentName = "agentX"
        options = dict(PollingTime=MAA.HOUR, LogFileLocation="/fake/log/file", PID="12345")

        self.restartAgent.getLastAccessTime.return_value = S_ERROR()
        res = self.restartAgent.checkAgent(agentName, options)
        self.assertFalse(res["OK"])

        # agents with log file age less than max(pollingTime+Hour, 2 Hour) should not be restarted
        logAge = timedelta(hours=1)
        self.restartAgent.getLastAccessTime.return_value = S_OK(logAge)
        res = self.restartAgent.checkAgent(agentName, options)
        self.assertTrue(res["OK"])
        self.restartAgent.restartInstance.assert_not_called()

        # agents with log file age of more than max(pollingTime+Hour, 2 Hour) should be restarted
        logAge = timedelta(hours=3)
        self.restartAgent.restartAgents = False
        self.restartAgent.getLastAccessTime.return_value = S_OK(logAge)
        res = self.restartAgent.checkAgent(agentName, options)
        self.assertTrue(res["OK"])
        self.restartAgent.restartInstance.assert_called_once_with(12345, agentName, False)

    def test_check_executors(self):
        """Test the checkExecutor function."""
        self.restartAgent.getLastAccessTime = MagicMock()
        self.restartAgent.restartInstance = MagicMock(return_value=S_OK())
        self.restartAgent.checkForCheckingJobs = MagicMock()
        self.restartAgent.restartExecutors = True

        agentName = "executorX"
        currentLogLocation = "/fake/log/file"
        pid = "12345"
        options = dict(PID=pid, LogFileLocation=currentLogLocation)
        self.restartAgent.getLastAccessTime.return_value = S_ERROR()
        res = self.restartAgent.checkExecutor(agentName, options)
        self.assertFalse(res["OK"])

        # log file ok
        logAge = timedelta(hours=1)
        self.restartAgent.getLastAccessTime.return_value = S_OK(logAge)
        res = self.restartAgent.checkExecutor(agentName, options)
        self.assertTrue(res["OK"])
        self.restartAgent.restartInstance.assert_not_called()

        # log file too old, no checking jobs
        logAge = timedelta(hours=3)
        self.restartAgent.getLastAccessTime.return_value = S_OK(logAge)
        self.restartAgent.checkForCheckingJobs.return_value = S_OK("NO_CHECKING_JOBS")
        res = self.restartAgent.checkExecutor(agentName, options)
        self.assertTrue(res["OK"])
        self.restartAgent.restartInstance.assert_not_called()

        # log file too old, checking jobs failed
        logAge = timedelta(hours=3)
        self.restartAgent.getLastAccessTime.return_value = S_OK(logAge)
        self.restartAgent.checkForCheckingJobs.return_value = S_ERROR()
        res = self.restartAgent.checkExecutor(agentName, options)
        self.assertFalse(res["OK"])
        self.restartAgent.restartInstance.assert_not_called()

        # log file too old, checking jobs OK
        logAge = timedelta(hours=3)
        self.restartAgent.getLastAccessTime.return_value = S_OK(logAge)
        self.restartAgent.checkForCheckingJobs.return_value = S_OK("CHECKING_JOBS")
        res = self.restartAgent.checkExecutor(agentName, options)
        self.assertTrue(res["OK"])
        self.restartAgent.restartInstance.assert_called_once_with(int(pid), agentName, True)

        # log file too old, checking jobs OK, restart failed
        self.restartAgent.restartInstance = MagicMock(return_value=S_ERROR())
        logAge = timedelta(hours=3)
        self.restartAgent.getLastAccessTime.return_value = S_OK(logAge)
        self.restartAgent.checkForCheckingJobs.return_value = S_OK("CHECKING_JOBS")
        res = self.restartAgent.checkExecutor(agentName, options)
        self.assertFalse(res["OK"])
        self.restartAgent.restartInstance.assert_called_once_with(int(pid), agentName, True)

        # log file too old, checking jobs OK, restart disabled
        self.restartAgent.restartExecutors = False
        self.restartAgent.restartInstance = MagicMock(return_value=S_OK("NO_RESTART"))
        logAge = timedelta(hours=3)
        self.restartAgent.getLastAccessTime.return_value = S_OK(logAge)
        self.restartAgent.checkForCheckingJobs.return_value = S_OK("CHECKING_JOBS")
        res = self.restartAgent.checkExecutor(agentName, options)
        self.assertTrue(res["OK"])
        self.restartAgent.restartInstance.assert_called_once_with(int(pid), agentName, False)

    def test_check_services(self):
        """Test for checkServices function."""
        self.restartAgent.getLastAccessTime = MagicMock()
        self.restartAgent.restartInstance = MagicMock(return_value=S_OK())

        agentName = "serviceX"
        pid = "12345"
        options = dict(PID=pid, System="Skynet", Port=999)
        self.restartAgent.restartServices = False

        # service responds ok
        with patch("DIRAC.FrameworkSystem.Agent.ComponentSupervisionAgent.Client", new=clientMock(S_OK())):
            res = self.restartAgent.checkService(agentName, options)
        self.assertTrue(res["OK"])
        self.restartAgent.restartInstance.assert_not_called()

        # service responds not ok
        with patch("DIRAC.FrameworkSystem.Agent.ComponentSupervisionAgent.Client", new=clientMock(S_ERROR())):
            res = self.restartAgent.checkService(agentName, options)
        self.assertTrue(res["OK"])
        self.restartAgent.restartInstance.assert_called_once_with(int(pid), agentName, False)

        # service responds not ok
        self.restartAgent.restartInstance.reset_mock()
        self.restartAgent.restartInstance.return_value = S_OK("NO_RESTART")
        self.restartAgent.restartServices = False
        with patch("DIRAC.FrameworkSystem.Agent.ComponentSupervisionAgent.Client", new=clientMock(S_ERROR())):
            res = self.restartAgent.checkService(agentName, options)
        self.assertTrue(res["OK"])
        self.restartAgent.restartInstance.assert_called_once_with(int(pid), agentName, False)

        # service responds not ok, restart Failed
        self.restartAgent.restartInstance.reset_mock()
        self.restartAgent.restartServices = True
        self.restartAgent.restartInstance.return_value = S_ERROR()
        with patch("DIRAC.FrameworkSystem.Agent.ComponentSupervisionAgent.Client", new=clientMock(S_ERROR())):
            res = self.restartAgent.checkService(agentName, options)
        self.assertFalse(res["OK"])
        self.restartAgent.restartInstance.assert_called_once_with(int(pid), agentName, True)

    def test_get_last_access_time(self):
        """Test for the getLastAccessTime function."""
        self.agent.os.path.getmtime = MagicMock()
        self.agent.datetime = MagicMock()
        self.agent.datetime.now = MagicMock()
        self.agent.datetime.fromtimestamp = MagicMock()

        now = datetime.now()
        self.agent.datetime.now.return_value = now
        self.agent.datetime.fromtimestamp.return_value = now - timedelta(hours=1)

        res = self.restartAgent.getLastAccessTime("/fake/file")
        self.assertTrue(res["OK"])
        self.assertIsInstance(res["Value"], timedelta)
        self.assertEqual(res["Value"].seconds, 3600)

    def test_restartInstance(self):
        """Test restartInstance function."""
        with patch("DIRAC.FrameworkSystem.Agent.ComponentSupervisionAgent.psutil", new=self.getPSMock()):
            res = self.restartAgent.restartInstance(12345, "agentX", True)
        self.assertTrue(res["OK"])

        psMock = self.getPSMock()
        psMock.Process = MagicMock("RaisingProc")
        psMock.Error = psutil.Error
        psMock.Process.side_effect = psutil.AccessDenied()
        with patch("DIRAC.FrameworkSystem.Agent.ComponentSupervisionAgent.psutil", new=psMock):
            res = self.restartAgent.restartInstance(12345, "agentX", True)
        self.assertFalse(res["OK"])

        # restarting forbidden
        with patch("DIRAC.FrameworkSystem.Agent.ComponentSupervisionAgent.psutil", new=self.getPSMock()):
            res = self.restartAgent.restartInstance(12345, "agentFoo", True)
        self.assertTrue(res["OK"])
        assert res["Value"] == NO_RESTART

        # restarting disabled
        with patch("DIRAC.FrameworkSystem.Agent.ComponentSupervisionAgent.psutil", new=self.getPSMock()):
            res = self.restartAgent.restartInstance(12345, "agentY", False)
        self.assertTrue(res["OK"])
        assert res["Value"] == NO_RESTART

    def test_checkingJobs(self):
        """Test checkForCheckingJobs function."""
        self.restartAgent.jobMonClient = MagicMock()

        # failed to get jobs
        self.restartAgent.jobMonClient.getJobs.return_value = S_ERROR()
        self.assertFalse(self.restartAgent.checkForCheckingJobs("executor")["OK"])

        # No checking jobs
        self.restartAgent.jobMonClient.getJobs.return_value = S_OK(0)
        self.assertTrue(self.restartAgent.checkForCheckingJobs("executor")["OK"])
        self.assertEqual(self.restartAgent.checkForCheckingJobs("executor")["Value"], "NO_CHECKING_JOBS")

        # checking jobs
        self.restartAgent.jobMonClient.getJobs.return_value = S_OK({1: 1, 2: 2})
        self.assertTrue(self.restartAgent.checkForCheckingJobs("executor")["OK"])
        self.assertEqual(self.restartAgent.checkForCheckingJobs("executor")["Value"], "CHECKING_JOBS")

    def test_componentControl_1(self):
        """Fail to get status."""
        self.restartAgent.controlComponents = True
        self.restartAgent.errors = []
        self.restartAgent.sysAdminClient.getOverallStatus.return_value = S_ERROR()
        self.assertFalse(self.restartAgent.componentControl()["OK"])

    def test_componentControl_2(self):
        """Total Success."""
        self.restartAgent.controlComponents = True
        self.restartAgent.errors = []

        def god(*args, **_kwargs):
            """Mock getOptionsDict."""
            if "running" in args[0].lower():
                return S_OK({"Framework__StartMe": "", "Framework__Running": ""})
            if "stopped" in args[0].lower():
                return S_OK({"Framework__StopMe": "", "Framework__Stopped": "", "Framework__Unknown": ""})
            return S_ERROR()

        gConfigMock = MagicMock()
        gConfigMock.getOptionsDict.side_effect = god

        agents = {
            "Agents": {
                "Framework": {
                    "StopMe": {"RunitStatus": "Run"},
                    "StartMe": {"RunitStatus": "Down"},
                    "Running": {"RunitStatus": "Run"},
                    "Stopped": {"RunitStatus": "Down"},
                    "Uninstalled": {"RunitStatus": "Unknown"},
                }
            }
        }
        self.restartAgent.sysAdminClient.getOverallStatus.return_value = S_OK(agents)
        self.restartAgent.sysAdminClient.startComponent = MagicMock()
        self.restartAgent.sysAdminClient.stopComponent = MagicMock()

        with patch("DIRAC.FrameworkSystem.Agent.ComponentSupervisionAgent.gConfig", new=gConfigMock):
            res = self.restartAgent.componentControl()
        self.assertTrue(res["OK"])
        self.restartAgent.sysAdminClient.startComponent.assert_called_with("Framework", "StartMe")
        self.restartAgent.sysAdminClient.stopComponent.assert_called_with("Framework", "StopMe")
        self.assertIn("Unknown instance", self.restartAgent.errors[0])

    def test_componentControl_2b(self):
        """Total Success."""
        self.restartAgent.controlComponents = False
        self.restartAgent.errors = []

        def god(*args, **_kwargs):
            """Mock getOptionsDict."""
            if "running" in args[0].lower():
                return S_OK({"Framework__StartMe": "", "Framework__Running": ""})
            if "stopped" in args[0].lower():
                return S_OK({"Framework__StopMe": "", "Framework__Stopped": "", "Framework__Unknown": ""})
            return S_ERROR()

        gConfigMock = MagicMock()
        gConfigMock.getOptionsDict.side_effect = god

        agents = {
            "Agents": {
                "Framework": {
                    "StopMe": {"RunitStatus": "Run"},
                    "StartMe": {"RunitStatus": "Down"},
                    "Running": {"RunitStatus": "Run"},
                    "Stopped": {"RunitStatus": "Down"},
                    "Uninstalled": {"RunitStatus": "Unknown"},
                }
            }
        }
        self.restartAgent.sysAdminClient.getOverallStatus.return_value = S_OK(agents)
        self.restartAgent.sysAdminClient.startComponent = MagicMock()
        self.restartAgent.sysAdminClient.stopComponent = MagicMock()

        with patch("DIRAC.FrameworkSystem.Agent.ComponentSupervisionAgent.gConfig", new=gConfigMock):
            res = self.restartAgent.componentControl()
        self.assertTrue(res["OK"])
        self.restartAgent.sysAdminClient.startComponent.assert_not_called()
        self.restartAgent.sysAdminClient.stopComponent.assert_not_called()
        self.assertIn("Unknown instance", self.restartAgent.errors[0])
        self.assertIn("should be started", self.restartAgent.accounting["Framework__StartMe"]["Treatment"])
        self.assertIn("should be stopped", self.restartAgent.accounting["Framework__StopMe"]["Treatment"])

    def test_componentControl_3(self):
        """Failed to get options."""
        self.restartAgent.controlComponents = True
        self.restartAgent.errors = []

        def god(*args, **_kwargs):
            """Mock getOptionsDict."""
            if "running" in args[0].lower():
                return S_OK({"Framework__StopMe": "", "Framework__Stopped": "", "Framework__Unknown": ""})
            if "stopped" in args[0].lower():
                return S_ERROR("No Stopped")
            return S_ERROR()

        gConfigMock = MagicMock()
        gConfigMock.getOptionsDict.side_effect = god

        self.restartAgent.sysAdminClient.getOverallStatus.return_value = S_OK({})
        self.restartAgent.sysAdminClient.startComponent = MagicMock()
        self.restartAgent.sysAdminClient.stopComponent = MagicMock()

        with patch("DIRAC.FrameworkSystem.Agent.ComponentSupervisionAgent.gConfig", new=gConfigMock):
            res = self.restartAgent.componentControl()
        self.assertFalse(res["OK"])
        self.assertIn("No Stopped", res["Message"])

    def test_componentControl_4(self):
        """Bad host config."""
        self.restartAgent.controlComponents = True
        self.restartAgent.errors = []

        def god(*args, **_kwargs):
            """Mock getOptionsDict."""
            if "running" in args[0].lower():
                return S_OK({"Framework__Twice": ""})
            if "stopped" in args[0].lower():
                return S_OK({"Framework__Twice": ""})
            return S_ERROR()

        gConfigMock = MagicMock()
        gConfigMock.getOptionsDict.side_effect = god

        self.restartAgent.sysAdminClient.getOverallStatus.return_value = S_OK({})
        self.restartAgent.sysAdminClient.startComponent = MagicMock()
        self.restartAgent.sysAdminClient.stopComponent = MagicMock()

        with patch("DIRAC.FrameworkSystem.Agent.ComponentSupervisionAgent.gConfig", new=gConfigMock):
            res = self.restartAgent.componentControl()
        self.assertFalse(res["OK"])
        self.assertIn("Bad host configuration", res["Message"])

    def test_checkURLs_1(self):
        """Success."""
        theTornadoPort = "000043000"
        self.restartAgent.errors = []
        self.restartAgent.accounting.clear()
        host = "foo.server"
        prot = ["", "dips", "dips", "https"]
        port = ["", "1001", "1002", theTornadoPort]
        urls, tempurls, newurls = [], [], []
        for i in [1, 2]:
            urls.append("%(prot)s://%(host)s:%(port)s/Sys/Serv%(i)s" % dict(i=i, host=host, port=port[i], prot=prot[i]))
        for i in [1]:
            tempurls.append(
                "%(prot)s://%(host)s:%(port)s/Sys/Serv%(i)s" % dict(i=i, host=host, port=port[i], prot=prot[i])
            )
        for i in [1, 3]:
            newurls.append(
                "%(prot)s://%(host)s:%(port)s/Sys/Serv%(i)s" % dict(i=i, host=host, port=port[i], prot=prot[i])
            )

        def gVal(*args, **_kwargs):
            """Mock getValue."""
            if "Tornado" in args[0]:
                return theTornadoPort
            if "PollingTime" in args[0]:
                return 365
            if "Port" in args[0]:
                return args[1] if "Serv3" in args[0] else "100" + args[0].rsplit("/Serv", 1)[1].split("/")[0]
            if "URLs" in args[0]:
                return urls
            if "Protocol" in args[0]:
                return "https" if "Serv3" in args[0] else args[1]
            else:
                assert False, "Unknown config option requested %s" % args[0]

        gConfigMock = MagicMock()
        gConfigMock.getValue.side_effect = gVal
        services = {
            "Services": {
                "Sys": {
                    "Serv1": {
                        "Setup": True,
                        "PID": "18128",
                        "Port": "1001",
                        "RunitStatus": "Run",
                        "Module": "Serv",
                        "Installed": True,
                    },
                    "Serv2": {
                        "Setup": True,
                        "PID": "18128",
                        "Port": "1002",
                        "RunitStatus": "Down",
                        "Module": "Serv",
                        "Installed": True,
                    },
                    "Serv3": {
                        "Setup": True,
                        "PID": "18128",
                        "RunitStatus": "Run",
                        "Protocol": "https",
                        "Module": "Serv",
                        "Installed": True,
                    },
                    "SystemAdministrator": {
                        "Setup": True,
                        "PID": "18128",
                        "Port": "1003",
                        "RunitStatus": "Run",
                        "Module": "Serv",
                        "Installed": True,
                    },
                }
            }
        }

        self.restartAgent.sysAdminClient.getOverallStatus.return_value = S_OK(services)

        with patch("DIRAC.FrameworkSystem.Agent.ComponentSupervisionAgent.gConfig", new=gConfigMock), patch(
            "DIRAC.FrameworkSystem.Agent.ComponentSupervisionAgent.socket.gethostname", return_value=host
        ), patch(
            "DIRAC.FrameworkSystem.Agent.ComponentSupervisionAgent.PathFinder.getSystemInstance",
            return_value="Decertification",
        ), patch(
            "DIRAC.FrameworkSystem.Agent.ComponentSupervisionAgent.PathFinder.getComponentSection",
            side_effect=mockComponentSection,
        ), patch(
            "DIRAC.FrameworkSystem.Agent.ComponentSupervisionAgent.PathFinder.getSystemURLSection",
            side_effect=mockURLSection,
        ):
            res = self.restartAgent.checkURLs()
        self.assertTrue(res["OK"])
        self.restartAgent.csAPI.modifyValue.assert_has_calls(
            [
                call("/Systems/Sys/Production/URLs/Serv", ",".join(tempurls)),
                call("/Systems/Sys/Production/URLs/Serv", ",".join(newurls)),
            ],
            any_order=False,
        )
        assert self.restartAgent._tornadoPort == theTornadoPort

    def test_checkURLs_2(self):
        """Test commit to CS."""
        self.restartAgent.errors = []
        self.restartAgent.accounting.clear()
        self.restartAgent.csAPI.csModified = True
        self.restartAgent.commitURLs = True
        self.restartAgent.sysAdminClient.getOverallStatus.return_value = S_OK(dict(Services={}))

        self.restartAgent.csAPI.commit = MagicMock(return_value=S_ERROR("Nope"))
        with patch("DIRAC.FrameworkSystem.Agent.ComponentSupervisionAgent.gConfig", new=MagicMock()), patch(
            "DIRAC.FrameworkSystem.Agent.ComponentSupervisionAgent.PathFinder.getSystemInstance",
            return_value="Decertification",
        ), patch(
            "DIRAC.FrameworkSystem.Agent.ComponentSupervisionAgent.PathFinder.getComponentSection",
            side_effect=mockComponentSection,
        ):
            res = self.restartAgent.checkURLs()
        self.assertFalse(res["OK"])
        self.assertIn("Failed to commit", res["Message"])
        self.assertIn("Commit to CS failed", self.restartAgent.errors[0])

        self.restartAgent.csAPI.commit = MagicMock(return_value=S_OK())
        with patch("DIRAC.FrameworkSystem.Agent.ComponentSupervisionAgent.gConfig", new=MagicMock()), patch(
            "DIRAC.FrameworkSystem.Agent.ComponentSupervisionAgent.PathFinder.getSystemInstance",
            return_value="Decertification",
        ), patch(
            "DIRAC.FrameworkSystem.Agent.ComponentSupervisionAgent.PathFinder.getComponentSection",
            side_effect=mockComponentSection,
        ):

            res = self.restartAgent.checkURLs()
        self.assertTrue(res["OK"])


if __name__ == "__main__":
    SUITE = unittest.defaultTestLoader.loadTestsFromTestCase(TestComponentSupervisionAgent)
    TESTRESULT = unittest.TextTestRunner(verbosity=3).run(SUITE)
