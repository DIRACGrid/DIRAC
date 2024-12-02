"""
  Base class for all agent modules
"""
import datetime
import importlib.metadata
import inspect
import os
import signal
import threading
import time

import psutil

import DIRAC
from DIRAC import S_ERROR, S_OK, gConfig, gLogger, rootPath
from DIRAC.ConfigurationSystem.Client import PathFinder
from DIRAC.ConfigurationSystem.Client.Helpers.Operations import Operations
from DIRAC.Core.Utilities import Network, TimeUtilities
from DIRAC.Core.Utilities.File import mkDir
from DIRAC.Core.Utilities.ReturnValues import isReturnStructure
from DIRAC.Core.Utilities.Shifter import setupShifterProxyInEnv
from DIRAC.Core.Utilities.ThreadScheduler import gThreadScheduler


class AgentModule:
    """Base class for all agent modules

    This class is used by the AgentReactor Class to steer the execution of
    DIRAC Agents.

    For this purpose the following methods are used:
    - am_initialize()      just after instantiated
    - am_getPollingTime()  to set the execution frequency
    - am_getMaxCycles()    to determine the number of cycles
    - am_go()              for the actual execution of one cycle

    Before each iteration, the following methods are used to determine
    if the new cycle is to be started.
    - am_getModuleParam( 'alive' )
    - am_checkStopAgentFile()
    - am_removeStopAgentFile()

    To start new execution cycle the following methods are used
    - am_getCyclesDone()
    - am_setOption( 'MaxCycles', maxCycles )

    At the same time it provides all Agents with common interface.
    All Agent class must inherit from this base class and must implement
    at least the following method:
    - execute()            main method called in the agent cycle

    Additionally they may provide:
    - initialize()         for initial settings
    - finalize()           the graceful exit

    - beginExecution()     before each execution cycle
    - endExecution()       at the end of each execution cycle

    The agent can be stopped either by a signal or by creating a 'stop_agent' file
    in the controlDirectory defined in the agent configuration

    """

    def __init__(self, agentName, loadName, baseAgentName=False, properties={}):
        """
        Common __init__ method for all Agents.
        All Agent modules must define: __doc__

        They are used to populate __codeProperties

        The following Options are used from the Configuration:
        - Status
        - Enabled
        - PollingTime            default = 120
        - MaxCycles              default = 500
        - WatchdogTime           default = 0 (disabled)
        - ControlDirectory       control/SystemName/AgentName
        - WorkDirectory          work/SystemName/AgentName
        - shifterProxy           ''
        - shifterProxyLocation   WorkDirectory/SystemName/AgentName/.shifterCred

        It defines the following default Options that can be set via Configuration (above):
        - MonitoringEnabled     True
        - Enabled               True if Status == Active
        - PollingTime           120
        - MaxCycles             500
        - ControlDirectory      control/SystemName/AgentName
        - WorkDirectory         work/SystemName/AgentName
        - shifterProxy          False
        - shifterProxyLocation  work/SystemName/AgentName/.shifterCred

        different defaults can be set in the initialize() method of the Agent using am_setOption()

        In order to get a shifter proxy in the environment during the execute()
        the configuration Option 'shifterProxy' must be set, a default may be given
        in the initialize() method.
        """
        self.log = gLogger.getSubLogger(agentName)

        # LocalSite/InstancePath is set only from within the Pilot,
        # and it is something like /pool/condor/dir_8273/DIRAC_bs1w3dcdpilot
        # it does not apply to server installations
        self.__basePath = gConfig.getValue("/LocalSite/InstancePath", rootPath)
        self.__agentModule = None
        self.agentName = agentName
        self.__codeProperties = {}
        self.__getCodeInfo()

        self.__moduleProperties = {
            "fullName": agentName,
            "loadName": loadName,
            "section": PathFinder.getAgentSection(agentName),
            "loadSection": PathFinder.getAgentSection(loadName),
            "cyclesDone": 0,
            "totalElapsedTime": 0,
            "alive": True,
        }
        self.__moduleProperties["system"], self.__moduleProperties["agentName"] = agentName.split("/")
        self.__configDefaults = {}
        self.__configDefaults["MonitoringEnabled"] = self.am_getOption("MonitoringEnabled", True)
        self.__configDefaults["Enabled"] = self.am_getOption("Status", "Active").lower() == "active"
        self.__configDefaults["PollingTime"] = self.am_getOption("PollingTime", 120)
        self.__configDefaults["MaxCycles"] = self.am_getOption("MaxCycles", 500)
        self.__configDefaults["WatchdogTime"] = self.am_getOption("WatchdogTime", 0)
        self.__configDefaults["ControlDirectory"] = os.path.join(self.__basePath, "control", *agentName.split("/"))
        self.__configDefaults["WorkDirectory"] = os.path.join(self.__basePath, "work", *agentName.split("/"))
        self.__configDefaults["shifterProxy"] = ""
        self.__configDefaults["shifterProxyLocation"] = os.path.join(
            self.__configDefaults["WorkDirectory"], ".shifterCred"
        )

        if isinstance(properties, dict):
            for key in properties:
                self.__moduleProperties[key] = properties[key]
            self.__moduleProperties["executors"] = [(self.execute, ())]
            self.__moduleProperties["shifterProxy"] = False

        self.__monitorLastStatsUpdate = -1

        self.activityMonitoring = False
        # Check if monitoring is enabled
        if (
            "Monitoring" in Operations().getMonitoringBackends(monitoringType="AgentMonitoring")
            and self.__configDefaults["MonitoringEnabled"]
        ):
            self.activityMonitoring = True

    def __getCodeInfo(self):
        try:
            self.__codeProperties["version"] = importlib.metadata.version(
                inspect.getmodule(self).__package__.split(".")[0]
            )
        except Exception:
            self.log.exception(f"Failed to find version for {self!r}")
            self.__codeProperties["version"] = "unset"

        try:
            try:
                self.__agentModule = importlib.import_module(self.__class__.__module__)
            except ImportError as e:
                self.log.exception("Cannot load agent module", lException=e)
                raise e
            try:
                self.__codeProperties["description"] = getattr(self.__agentModule, "__doc__")
            except AttributeError as e:
                self.log.error("Missing property __doc__", lException=e)
                raise e
        except (ImportError, AttributeError):
            self.__codeProperties["description"] = "unset"

        self.__codeProperties["DIRACVersion"] = DIRAC.version
        self.__codeProperties["platform"] = DIRAC.getPlatform()

    def am_initialize(self, *initArgs):
        """Common initialization for all the agents.

        This is executed every time an agent (re)starts.
        This is called by the AgentReactor, should not be overridden.
        """
        agentName = self.am_getModuleParam("fullName")

        self.__initializeMonitor()

        result = self.initialize(*initArgs)
        if not isReturnStructure(result):
            return S_ERROR("initialize must return S_OK/S_ERROR")
        if not result["OK"]:
            return S_ERROR(f"Error while initializing {agentName}: {result['Message']}")
        mkDir(self.am_getControlDirectory())
        workDirectory = self.am_getWorkDirectory()
        mkDir(workDirectory)
        # Set the work directory in an environment variable available to subprocesses if needed
        os.environ["AGENT_WORKDIRECTORY"] = workDirectory

        self.__moduleProperties["shifterProxy"] = self.am_getOption("shifterProxy")
        if len(self.__moduleProperties["executors"]) < 1:
            return S_ERROR("At least one executor method has to be defined")
        if not self.am_Enabled():
            return S_ERROR("Agent is disabled via the configuration")
        self.log.notice("=" * 40)
        self.log.notice(f"Loaded agent module {self.__moduleProperties['fullName']}")
        self.log.notice(f" Site: {DIRAC.siteName()}")
        self.log.notice(f" Agent version: {self.__codeProperties['version']}")
        self.log.notice(f" DIRAC version: {DIRAC.version}")
        self.log.notice(f" DIRAC platform: {DIRAC.getPlatform()}")
        pollingTime = int(self.am_getOption("PollingTime"))
        if pollingTime > 3600:
            self.log.notice(f" Polling time: {pollingTime / 3600.0} hours")
        else:
            self.log.notice(f" Polling time: {self.am_getOption('PollingTime')} seconds")
        self.log.notice(f" Control dir: {self.am_getControlDirectory()}")
        self.log.notice(f" Work dir: {self.am_getWorkDirectory()}")
        if self.am_getOption("MaxCycles") > 0:
            self.log.notice(f" Cycles: {self.am_getMaxCycles()}")
        else:
            self.log.notice(" Cycles: unlimited")
        if self.am_getWatchdogTime() > 0:
            self.log.notice(f" Watchdog interval: {self.am_getWatchdogTime()}")
        else:
            self.log.notice(" Watchdog interval: disabled ")
        self.log.notice("=" * 40)
        return S_OK()

    def am_getControlDirectory(self):
        return os.path.join(self.__basePath, str(self.am_getOption("ControlDirectory")))

    def am_getStopAgentFile(self):
        return os.path.join(self.am_getControlDirectory(), "stop_agent")

    def am_checkStopAgentFile(self):
        return os.path.isfile(self.am_getStopAgentFile())

    def am_createStopAgentFile(self):
        try:
            with open(self.am_getStopAgentFile(), "w") as fd:
                fd.write(f"Dirac site agent Stopped at {str(datetime.datetime.utcnow())}")
        except Exception:
            pass

    def am_removeStopAgentFile(self):
        try:
            os.unlink(self.am_getStopAgentFile())
        except Exception:
            pass

    def am_getWorkDirectory(self):
        return os.path.join(self.__basePath, str(self.am_getOption("WorkDirectory")))

    def am_getShifterProxyLocation(self):
        return os.path.join(self.__basePath, str(self.am_getOption("shifterProxyLocation")))

    def am_getOption(self, optionName, defaultValue=None):
        """Gets an option from the agent's configuration section.
        The section will be a subsection of the /Systems section in the CS.
        """
        if defaultValue is None:
            if optionName in self.__configDefaults:
                defaultValue = self.__configDefaults[optionName]
        if optionName and optionName[0] == "/":
            return gConfig.getValue(optionName, defaultValue)
        for section in (self.__moduleProperties["section"], self.__moduleProperties["loadSection"]):
            result = gConfig.getOption(f"{section}/{optionName}", defaultValue)
            if result["OK"]:
                return result["Value"]
        return defaultValue

    def am_setOption(self, optionName, value):
        self.__configDefaults[optionName] = value

    def am_getModuleParam(self, optionName):
        return self.__moduleProperties[optionName]

    def am_setModuleParam(self, optionName, value):
        self.__moduleProperties[optionName] = value

    def am_getPollingTime(self):
        return self.am_getOption("PollingTime")

    def am_getMaxCycles(self):
        return self.am_getOption("MaxCycles")

    def am_getWatchdogTime(self):
        return int(self.am_getOption("WatchdogTime"))

    def am_getCyclesDone(self):
        return self.am_getModuleParam("cyclesDone")

    def am_Enabled(self):
        return self.am_getOption("Enabled")

    def am_stopExecution(self):
        self.am_setModuleParam("alive", False)

    def __initializeMonitor(self):
        """
        Initialize the system monitoring.
        """
        # This flag is used to activate ES based monitoring
        if self.activityMonitoring:
            self.log.debug("Monitoring of the agent is enabled.")
            # The import needs to be here because of the CS must be initialized before importing
            # this class (see https://github.com/DIRACGrid/DIRAC/issues/4793)
            from DIRAC.MonitoringSystem.Client.MonitoringReporter import MonitoringReporter

            self.activityMonitoringReporter = MonitoringReporter(monitoringType="AgentMonitoring")
            # With the help of this periodic task we commit the data to ES at an interval of 100 seconds.
            gThreadScheduler.addPeriodicTask(100, self.__activityMonitoringReporting)
            self.__monitorLastStatsUpdate = time.time()

    def am_secureCall(self, functor, args=(), name=False):
        if not name:
            name = str(functor)
        try:
            result = functor(*args)
            if not isReturnStructure(result):
                raise Exception(
                    "{} method for {} module has to return S_OK/S_ERROR".format(
                        name, self.__moduleProperties["fullName"]
                    )
                )
            return result
        except Exception as e:
            self.log.exception(f"Agent exception while calling method {name}", lException=e)
            return S_ERROR(f"Exception while calling {name} method: {str(e)}")

    def _setShifterProxy(self):
        if self.__moduleProperties["shifterProxy"]:
            result = setupShifterProxyInEnv(self.__moduleProperties["shifterProxy"], self.am_getShifterProxyLocation())
            if not result["OK"]:
                self.log.error("Failed to set shifter proxy", result["Message"])
                return result
        return S_OK()

    def am_go(self):
        # Set the shifter proxy if required
        result = self._setShifterProxy()
        if not result["OK"]:
            return result
        self.log.notice("-" * 40)
        self.log.notice(f"Starting cycle for module {self.__moduleProperties['fullName']}")
        mD = self.am_getMaxCycles()
        if mD > 0:
            cD = self.__moduleProperties["cyclesDone"]
            self.log.notice(f"Remaining {mD - cD} of {mD} cycles")
        self.log.notice("-" * 40)
        # use SIGALARM as a watchdog interrupt if enabled
        watchdogInt = self.am_getWatchdogTime()
        if watchdogInt > 0:
            signal.signal(signal.SIGALRM, signal.SIG_DFL)
            signal.alarm(watchdogInt)
        elapsedTime = time.time()
        if self.activityMonitoring:
            initialWallTime, initialCPUTime, mem = self._startReportToMonitoring()
            cycleResult = self.__executeModuleCycle()
            cpuPercentage = self._endReportToMonitoring(initialWallTime, initialCPUTime)
        else:
            cycleResult = self.__executeModuleCycle()
        # Increment counters
        self.__moduleProperties["cyclesDone"] += 1
        # Show status
        elapsedTime = time.time() - elapsedTime
        self.__moduleProperties["totalElapsedTime"] += elapsedTime
        self.log.notice("-" * 40)
        self.log.notice(f"Agent module {self.__moduleProperties['fullName']} run summary")
        self.log.notice(f" Executed {self.__moduleProperties['cyclesDone']} times previously")
        self.log.notice(f" Cycle took {elapsedTime:.2f} seconds")
        averageElapsedTime = self.__moduleProperties["totalElapsedTime"] / self.__moduleProperties["cyclesDone"]
        self.log.notice(f" Average execution time: {averageElapsedTime:.2f} seconds")
        elapsedPollingRate = averageElapsedTime * 100 / self.am_getOption("PollingTime")
        self.log.notice(f" Polling time: {self.am_getOption('PollingTime')} seconds")
        self.log.notice(f" Average execution/polling time: {elapsedPollingRate:.2f}%")
        if cycleResult["OK"]:
            self.log.notice(" Cycle was successful")
            if self.activityMonitoring:
                # Here we record the data about the cycle duration along with some basic details about the
                # agent and right now it isn't committed to the ES backend.
                self.activityMonitoringReporter.addRecord(
                    {
                        "AgentName": self.agentName,
                        "timestamp": int(TimeUtilities.toEpochMilliSeconds()),
                        "Host": Network.getFQDN(),
                        "MemoryUsage": mem,
                        "CpuPercentage": cpuPercentage,
                        "CycleDuration": elapsedTime,
                    }
                )
        else:
            self.log.warn(" Cycle had an error:", cycleResult["Message"])
        self.log.notice("-" * 40)
        # Update number of cycles
        # cycle finished successfully, cancel watchdog
        if watchdogInt > 0:
            signal.alarm(0)
        return cycleResult

    def _startReportToMonitoring(self):
        now = time.time()
        stats = os.times()
        mem = None
        cpuTime = stats[0] + stats[2]
        if now - self.__monitorLastStatsUpdate < 10:
            return (now, cpuTime, mem)
        self.__monitorLastStatsUpdate = now
        mem = psutil.Process().memory_info().rss / (1024.0 * 1024.0)
        return (now, cpuTime, mem)

    def _endReportToMonitoring(self, initialWallTime, initialCPUTime):
        wallTime = time.time() - initialWallTime
        stats = os.times()
        cpuTime = stats[0] + stats[2] - initialCPUTime
        percentage = 0
        if wallTime:
            percentage = cpuTime / wallTime * 100.0
        return percentage

    def __executeModuleCycle(self):
        # Execute the beginExecution function
        result = self.am_secureCall(self.beginExecution, name="beginExecution")
        if not result["OK"]:
            return result
        # Launch executor functions
        executors = self.__moduleProperties["executors"]
        if len(executors) == 1:
            result = self.am_secureCall(executors[0][0], executors[0][1])
            if not result["OK"]:
                return result
        else:
            exeThreads = [threading.Thread(target=executor[0], args=executor[1]) for executor in executors]
            for thread in exeThreads:
                thread.daemon = True
                thread.start()
            for thread in exeThreads:
                thread.join()
        # Execute the endExecution function
        return self.am_secureCall(self.endExecution, name="endExecution")

    def initialize(self, *args, **kwargs):
        """Agents should override this method for specific initialization.
        Executed at every agent (re)start.
        """
        return S_OK()

    def beginExecution(self):
        return S_OK()

    def endExecution(self):
        return S_OK()

    def finalize(self):
        return S_OK()

    def execute(self):
        return S_ERROR("Execute method has to be overwritten by agent module")

    def __activityMonitoringReporting(self):
        """This method is called by the ThreadScheduler as a periodic task in order to commit the collected data which
        is done by the MonitoringReporter and is sent to the 'AgentMonitoring' type.

        :return: True / False
        """
        return self.activityMonitoringReporter.commit()["OK"]
