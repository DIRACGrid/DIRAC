"""Component Supervision Agent for monitoring agent, executor or service behaviour and intervene if
necessary.

This agent is designed to supervise the Agents, Executors and Services, and restarts them in case
they get stuck. It can only control components running on the same machine as the agent. One agent
per server is needed.

* The agent checks the age of the log file and if it is deemed too old will kill the agent so that
  it is restarted automatically. (Option RestartAgents)
* Executors will only be restarted if there are jobs in checking status (Option RestartExecutors)
* Services will be restarted if they don't answer a ping RPC (Option RestartServices)

* Check for running and stopped components and ensure they have the proper status as defined in the
  CS Registry/Hosts/_HOST_/[Running|Stopped] sections. (Option ControlComponents)
* If desired also service URLs can automatically be added or removed from the Configuration (Option CommitURLs)

The configuration for Running and Stopped components are two sub-sections in Registry/Hosts/<Host>::

    Running
    {
      Configuration__Server =
      Framework__SystemAdministrator =
      Framework__ComponentSupervisionAgent =
    }
    Stopped
    {
      DataManagement__FileCatalog2 =
      Framework__Monitoring =
    }

By moving from one to the other section we can make the ComponentSupervisionAgent Stop/Start the given
component. Values for the entries in the list are ignored, Syntax is ``<System>__<ComponentName>``.

For full functioning of the Agent a few additional permissions have to be granted for the ``Operator`` role.
In the ``SystemAdministrator/Authorization`` section of the relevant setup::

  getOverallStatus = Operator
  stopComponent = Operator
  startComponent = Operator

``getOverallStatus`` is needed for basic functioning of the Agent. ``stopComponent`` or ``startComponent`` are only
needed if ``ControlComponents`` is enabled.

.. literalinclude:: ../ConfigTemplate.cfg
  :start-after: ##BEGIN ComponentSupervisionAgent
  :end-before: ##END
  :dedent: 2
  :caption: ComponentSupervisionAgent options

"""
# imports
from collections import defaultdict
from datetime import datetime
from functools import partial

import os
import socket
import psutil

# from DIRAC
from DIRAC import S_OK, S_ERROR, gConfig, rootPath
from DIRAC.ConfigurationSystem.Client.CSAPI import CSAPI
from DIRAC.ConfigurationSystem.Client import PathFinder
from DIRAC.ConfigurationSystem.Client.Helpers import Path
from DIRAC.Core.Base.AgentModule import AgentModule
from DIRAC.Core.Base.Client import Client
from DIRAC.Core.Utilities.PrettyPrint import printTable
from DIRAC.FrameworkSystem.Client.SystemAdministratorClient import SystemAdministratorClient
from DIRAC.FrameworkSystem.Client.NotificationClient import NotificationClient
from DIRAC.WorkloadManagementSystem.Client.JobMonitoringClient import JobMonitoringClient

AGENT_NAME = "Framework/ComponentSupervisionAgent"

# Define units
HOUR = 3600
MINUTES = 60
SECONDS = 1

# Define constant
NO_CHECKING_JOBS = "NO_CHECKING_JOBS"
CHECKING_JOBS = "CHECKING_JOBS"
NO_RESTART = "NO_RESTART"


class ComponentSupervisionAgent(AgentModule):
    """ComponentSupervisionAgent class."""

    def __init__(self, *args, **kwargs):
        """Initialize the agent, clients, default values."""
        AgentModule.__init__(self, *args, **kwargs)
        self.name = "ComponentSupervisionAgent"
        self.enabled = False
        self.restartAgents = False
        self.restartExecutors = False
        self.restartServices = False
        self.controlComponents = False
        self.commitURLs = False
        self.doNotRestartInstancePattern = ["RequestExecutingAgent"]
        self.diracLocation = rootPath

        self.sysAdminClient = SystemAdministratorClient(socket.getfqdn())
        self.jobMonClient = JobMonitoringClient()
        self.nClient = NotificationClient()
        self.csAPI = None
        self.agents = dict()
        self.executors = dict()
        self.services = dict()
        self._tornadoPort = "8443"
        self.errors = list()
        self.accounting = defaultdict(dict)

        self.addressTo = []
        self.addressFrom = ""
        self.emailSubject = f"ComponentSupervisionAgent on {socket.getfqdn()}"

    def logError(self, errStr, varMsg=""):
        """Append errors to a list, which is sent in email notification."""
        self.log.error(errStr, varMsg)
        self.errors.append(errStr + " " + varMsg)

    def beginExecution(self):
        """Reload the configurations before every cycle."""
        self.enabled = self.am_getOption("EnableFlag", self.enabled)
        self.restartAgents = self.am_getOption("RestartAgents", self.restartAgents)
        self.restartExecutors = self.am_getOption("RestartExecutors", self.restartExecutors)
        self.restartServices = self.am_getOption("RestartServices", self.restartServices)
        self.addressTo = self.am_getOption("MailTo", self.addressTo)
        self.addressFrom = self.am_getOption("MailFrom", self.addressFrom)
        self.controlComponents = self.am_getOption("ControlComponents", self.controlComponents)
        self.commitURLs = self.am_getOption("CommitURLs", self.commitURLs)
        self.doNotRestartInstancePattern = self.am_getOption(
            "DoNotRestartInstancePattern", self.doNotRestartInstancePattern
        )

        self.csAPI = CSAPI()

        res = self.getRunningInstances(instanceType="Agents")
        if not res["OK"]:
            return S_ERROR("Failure to get running agents")
        self.agents = res["Value"]

        res = self.getRunningInstances(instanceType="Executors")
        if not res["OK"]:
            return S_ERROR("Failure to get running executors")
        self.executors = res["Value"]

        res = self.getRunningInstances(instanceType="Services")
        if not res["OK"]:
            return S_ERROR("Failure to get running services")
        self.services = res["Value"]

        self.accounting.clear()
        return S_OK()

    def sendNotification(self):
        """Send email notification about changes done in the last cycle."""
        if not (self.errors or self.accounting):
            return S_OK()

        emailBody = ""
        rows = []
        for instanceName, val in self.accounting.items():
            rows.append(
                [[instanceName], [val.get("Treatment", "No Treatment")], [str(val.get("LogAge", "Not Relevant"))]]
            )

        if rows:
            columns = ["Instance", "Treatment", "Log File Age (Minutes)"]
            emailBody += printTable(columns, rows, printOut=False, numbering=False, columnSeparator=" | ")

        if self.errors:
            emailBody += "\n\nErrors:"
            emailBody += "\n".join(self.errors)

        self.log.notice("Sending Email:\n" + emailBody)
        for address in self.addressTo:
            res = self.nClient.sendMail(address, self.emailSubject, emailBody, self.addressFrom, localAttempt=False)
            if not res["OK"]:
                self.log.error("Failure to send Email notification to ", address)
                continue

        self.errors = []
        self.accounting.clear()

        return S_OK()

    def getRunningInstances(self, instanceType="Agents", runitStatus="Run"):
        """Return a dict of running agents, executors or services.

        Key is component's name, value contains dict with PollingTime, PID, Port, Module, RunitStatus, LogFileLocation

        :param str instanceType: 'Agents', 'Executors', 'Services'
        :param str runitStatus: Return only those instances with given RunitStatus or 'All'
        :returns: Dictionary of running instances
        """
        res = self.sysAdminClient.getOverallStatus()
        if not res["OK"]:
            self.logError(f"Failure to get {instanceType} from system administrator client", res["Message"])
            return res

        val = res["Value"][instanceType]
        runningComponents = defaultdict(dict)
        for system, components in val.items():
            for componentName, componentInfo in components.items():
                fullName = f"{system}__{componentName}"
                if componentInfo["Installed"]:
                    if runitStatus != "All" and componentInfo["RunitStatus"] != runitStatus:
                        continue
                    for option, default in (("PollingTime", HOUR), ("Port", None), ("Protocol", None)):
                        runningComponents[fullName][option] = self._getComponentOption(
                            instanceType, system, componentName, option, default
                        )
                        # remove empty values so we can use defaults in _getURL
                        if not runningComponents[fullName][option]:
                            runningComponents[fullName].pop(option)
                    runningComponents[fullName]["LogFileLocation"] = os.path.join(
                        self.diracLocation, "runit", system, componentName, "log", "current"
                    )
                    runningComponents[fullName]["PID"] = componentInfo["PID"]
                    runningComponents[fullName]["Module"] = componentInfo["Module"]
                    runningComponents[fullName]["RunitStatus"] = componentInfo["RunitStatus"]
                    runningComponents[fullName]["System"] = system

        return S_OK(runningComponents)

    def _getComponentOption(self, instanceType, system, componentName, option, default):
        """Get component option from DIRAC CS, using components' base classes methods."""
        componentPath = PathFinder.getComponentSection(
            system=system,
            component=componentName,
            componentCategory=instanceType,
        )
        if instanceType != "Agents":
            return gConfig.getValue(Path.cfgPath(componentPath, option), default)
        # deal with agent configuration
        componentLoadModule = gConfig.getValue(Path.cfgPath(componentPath, "Module"), componentName)
        fullComponentName = Path.cfgPath(system, componentName)
        fullComponentLoadName = Path.cfgPath(system, componentLoadModule)
        return AgentModule(fullComponentName, fullComponentLoadName).am_getOption(option, default)

    def on_terminate(self, componentName, process):
        """Execute callback when a process terminates gracefully."""
        self.log.info(f"{componentName}'s process with ID: {process.pid} has been terminated successfully")

    def execute(self):
        """Execute checks for agents, executors, services."""
        for instanceType in ("executor", "agent", "service"):
            for name, options in getattr(self, instanceType + "s").items():
                # call checkAgent, checkExecutor, checkService
                res = getattr(self, "check" + instanceType.capitalize())(name, options)
                if not res["OK"]:
                    self.logError(f"Failure when checking {instanceType}", f"{name}, {res['Message']}")

        res = self.componentControl()
        if not res["OK"]:
            if "Stopped does not exist" not in res["Message"] and "Running does not exist" not in res["Message"]:
                self.logError("Failure to control components", res["Message"])

        if not self.errors:
            res = self.checkURLs()
            if not res["OK"]:
                self.logError("Failure to check URLs", res["Message"])
        else:
            self.logError("Something was wrong before, not checking URLs this time")

        self.sendNotification()

        if self.errors:
            return S_ERROR("Error during this cycle, check log")

        return S_OK()

    @staticmethod
    def getLastAccessTime(logFileLocation):
        """Return the age of log file."""
        lastAccessTime = 0
        try:
            lastAccessTime = os.path.getmtime(logFileLocation)
            lastAccessTime = datetime.fromtimestamp(lastAccessTime)
        except OSError as e:
            return S_ERROR(f"Failed to access logfile {logFileLocation}: {e!r}")

        now = datetime.now()
        age = now - lastAccessTime
        return S_OK(age)

    def restartInstance(self, pid, instanceName, enabled):
        """Kill a process which is then restarted automatically."""
        if not (self.enabled and enabled):
            self.log.info(f"Restarting is disabled, please restart {instanceName} manually")
            self.accounting[instanceName]["Treatment"] = "Please restart it manually"
            return S_OK(NO_RESTART)

        if any(pattern in instanceName for pattern in self.doNotRestartInstancePattern):
            self.log.info(f"Restarting for {instanceName} is disabled, please restart it manually")
            self.accounting[instanceName]["Treatment"] = "Please restart it manually"
            return S_OK(NO_RESTART)

        try:
            componentProc = psutil.Process(int(pid))
            processesToTerminate = componentProc.children(recursive=True)
            processesToTerminate.append(componentProc)

            for proc in processesToTerminate:
                proc.terminate()

            _gone, alive = psutil.wait_procs(
                processesToTerminate, timeout=5, callback=partial(self.on_terminate, instanceName)
            )
            for proc in alive:
                self.log.info(f"Forcefully killing process {proc.pid}")
                proc.kill()

            return S_OK()

        except psutil.Error as err:
            self.logError("Exception occurred in terminating processes", f"{err}")
            return S_ERROR()

    def checkService(self, serviceName, options):
        """Ping the service, restart if the ping does not respond."""
        if serviceName == "Tornado":
            return S_OK()
        url = self._getURL(serviceName, options)
        self.log.info("Pinging service", url)
        pingRes = Client().ping(url=url)
        if not pingRes["OK"]:
            self.log.warn("Failure pinging service", f": {url}: {pingRes['Message']}")
            res = self.restartInstance(int(options["PID"]), serviceName, self.restartServices)
            if not res["OK"]:
                return res
            if res["Value"] != NO_RESTART:
                self.accounting[serviceName]["Treatment"] = "Successfully Restarted"
                self.log.info(f"Service {serviceName} has been successfully restarted")
        self.log.info("Service responded OK")
        return S_OK()

    def checkAgent(self, agentName, options):
        """Check the age of agent's log file, if it is too old then restart the agent."""
        pollingTime, currentLogLocation, pid = (options["PollingTime"], options["LogFileLocation"], options["PID"])
        self.log.info(f"Checking Agent: {agentName}")
        self.log.info(f"Polling Time: {pollingTime}")
        self.log.info(f"Current Log File location: {currentLogLocation}")

        res = self.getLastAccessTime(currentLogLocation)
        if not res["OK"]:
            return res

        age = res["Value"]
        self.log.info("Current log file for %s is %d minutes old" % (agentName, (age.seconds / MINUTES)))

        maxLogAge = max(pollingTime + HOUR, 2 * HOUR)
        if age.seconds < maxLogAge:
            return S_OK()

        self.log.info(f"Current log file is too old for Agent {agentName}")
        self.accounting[agentName]["LogAge"] = age.seconds / MINUTES

        res = self.restartInstance(int(pid), agentName, self.restartAgents)
        if not res["OK"]:
            return res
        if res["Value"] != NO_RESTART:
            self.accounting[agentName]["Treatment"] = "Successfully Restarted"
            self.log.info(f"Agent {agentName} has been successfully restarted")

        return S_OK()

    def checkExecutor(self, executor, options):
        """Check the age of executor log file, if too old check for jobs in checking status, then restart the executors."""
        currentLogLocation = options["LogFileLocation"]
        pid = options["PID"]
        self.log.info(f"Checking executor: {executor}")
        self.log.info(f"Current Log File location: {currentLogLocation}")

        res = self.getLastAccessTime(currentLogLocation)
        if not res["OK"]:
            return res

        age = res["Value"]
        self.log.info("Current log file for %s is %d minutes old" % (executor, (age.seconds / MINUTES)))

        if age.seconds < 2 * HOUR:
            return S_OK()

        self.log.info(f"Current log file is too old for Executor {executor}")
        self.accounting[executor]["LogAge"] = age.seconds / MINUTES

        res = self.checkForCheckingJobs(executor)
        if not res["OK"]:
            return res
        if res["OK"] and res["Value"] == NO_CHECKING_JOBS:
            self.accounting.pop(executor, None)
            return S_OK(NO_RESTART)

        res = self.restartInstance(int(pid), executor, self.restartExecutors)
        if not res["OK"]:
            return res
        elif res["OK"] and res["Value"] != NO_RESTART:
            self.accounting[executor]["Treatment"] = "Successfully Restarted"
            self.log.info(f"Executor {executor} has been successfully restarted")

        return S_OK()

    def checkForCheckingJobs(self, executorName):
        """Check if there are checking jobs with the **executorName** as current MinorStatus."""
        attrDict = {"Status": "Checking", "MinorStatus": executorName}

        # returns list of jobs IDs
        resJobs = self.jobMonClient.getJobs(attrDict)
        if not resJobs["OK"]:
            self.logError("Could not get jobs for this executor", f"{executorName}: {resJobs['Message']}")
            return resJobs
        if resJobs["Value"]:
            self.log.info(f"Found {len(resJobs['Value'])} jobs in \"Checking\" status for {executorName}")
            return S_OK(CHECKING_JOBS)
        self.log.info(f'Found no jobs in "Checking" status for {executorName}')
        return S_OK(NO_CHECKING_JOBS)

    def componentControl(self):
        """Monitor and control component status as defined in the CS.

        Check for running and stopped components and ensure they have the proper status as defined in the CS
        Registry/Hosts/_HOST_/[Running|Stopped] sections

        :returns: :func:`~DIRAC:DIRAC.Core.Utilities.ReturnValues.S_OK`,
           :func:`~DIRAC:DIRAC.Core.Utilities.ReturnValues.S_ERROR`
        """
        # get the current status of the components

        resCurrent = self._getCurrentComponentStatus()
        if not resCurrent["OK"]:
            return resCurrent
        currentStatus = resCurrent["Value"]

        resDefault = self._getDefaultComponentStatus()
        if not resDefault["OK"]:
            return resDefault
        defaultStatus = resDefault["Value"]

        # ensure instances are in the right state
        shouldBe = {}
        shouldBe["Run"] = defaultStatus["Run"].intersection(currentStatus["Down"])
        shouldBe["Down"] = defaultStatus["Down"].intersection(currentStatus["Run"])
        shouldBe["Unknown"] = defaultStatus["All"].symmetric_difference(currentStatus["All"])

        self._ensureComponentRunning(shouldBe["Run"])
        self._ensureComponentDown(shouldBe["Down"])

        for instance in shouldBe["Unknown"]:
            self.logError("Unknown instance", f"{instance!r}, either uninstall or add to config")

        return S_OK()

    def _getCurrentComponentStatus(self):
        """Get current status for components."""
        resOverall = self.sysAdminClient.getOverallStatus()
        if not resOverall["OK"]:
            return resOverall
        currentStatus = {"Down": set(), "Run": set(), "All": set()}
        informationDict = resOverall["Value"]
        for systemsDict in informationDict.values():
            for system, instancesDict in systemsDict.items():
                for instanceName, instanceInfoDict in instancesDict.items():
                    identifier = f"{system}__{instanceName}"
                    runitStatus = instanceInfoDict.get("RunitStatus")
                    if runitStatus in ("Run", "Down"):
                        currentStatus[runitStatus].add(identifier)

        currentStatus["All"] = currentStatus["Run"] | currentStatus["Down"]
        return S_OK(currentStatus)

    def _getDefaultComponentStatus(self):
        """Get the configured status of the components."""
        host = socket.getfqdn()
        defaultStatus = {"Down": set(), "Run": set(), "All": set()}
        resRunning = gConfig.getOptionsDict(Path.cfgPath("/Registry/Hosts/", host, "Running"))
        resStopped = gConfig.getOptionsDict(Path.cfgPath("/Registry/Hosts/", host, "Stopped"))
        if not resRunning["OK"]:
            return resRunning
        if not resStopped["OK"]:
            return resStopped
        defaultStatus["Run"] = set(resRunning["Value"])
        defaultStatus["Down"] = set(resStopped["Value"])
        defaultStatus["All"] = defaultStatus["Run"] | defaultStatus["Down"]

        if defaultStatus["Run"].intersection(defaultStatus["Down"]):
            self.logError("Overlap in configuration", str(defaultStatus["Run"].intersection(defaultStatus["Down"])))
            return S_ERROR("Bad host configuration")

        return S_OK(defaultStatus)

    def _ensureComponentRunning(self, shouldBeRunning):
        """Ensure the correct components are running."""
        for instance in shouldBeRunning:
            self.log.info(f"Starting instance {instance}")
            system, name = instance.split("__")
            if self.controlComponents:
                res = self.sysAdminClient.startComponent(system, name)
                if not res["OK"]:
                    self.logError("Failed to start component:", f"{instance}: {res['Message']}")
                else:
                    self.accounting[instance]["Treatment"] = "Instance was down, started instance"
            else:
                self.accounting[instance]["Treatment"] = "Instance is down, should be started"

    def _ensureComponentDown(self, shouldBeDown):
        """Ensure the correct components are not running."""
        for instance in shouldBeDown:
            self.log.info(f"Stopping instance {instance}")
            system, name = instance.split("__")
            if self.controlComponents:
                res = self.sysAdminClient.stopComponent(system, name)
                if not res["OK"]:
                    self.logError("Failed to stop component:", f"{instance}: {res['Message']}")
                else:
                    self.accounting[instance]["Treatment"] = "Instance was running, stopped instance"
            else:
                self.accounting[instance]["Treatment"] = "Instance is running, should be stopped"

    def checkURLs(self):
        """Ensure that the running services have their URL in the Config."""
        self.log.info("Checking URLs")
        # get services again, in case they were started/stop in controlComponents
        gConfig.forceRefresh(fromMaster=True)

        # get port used for https based services
        try:
            self._tornadoPort = gConfig.getValue(
                Path.cfgPath("/System/Tornado/", "Port"),
                self._tornadoPort,
            )
        except RuntimeError:
            pass

        self.log.debug("Using Tornado Port:", self._tornadoPort)

        res = self.getRunningInstances(instanceType="Services", runitStatus="All")
        if not res["OK"]:
            return S_ERROR("Failure to get running services")
        self.services = res["Value"]
        for service, options in sorted(self.services.items()):
            self.log.debug(f"Checking URL for {service} with options {options}")
            # ignore SystemAdministrator, does not have URLs
            if "SystemAdministrator" in service:
                continue
            self._checkServiceURL(service, options)

        if self.csAPI.csModified and self.commitURLs:
            self.log.info("Commiting changes to the CS")
            result = self.csAPI.commit()
            if not result["OK"]:
                self.logError("Commit to CS failed", result["Message"])
                return S_ERROR("Failed to commit to CS")
        return S_OK()

    def _checkServiceURL(self, serviceName, options):
        """Ensure service URL is properly configured in the CS."""
        url = self._getURL(serviceName, options)
        system = options["System"]
        module = options["Module"]
        self.log.info(f"Checking URLs for {system}/{module}")
        urlsConfigPath = Path.cfgPath(PathFinder.getSystemURLSection(system=system), module)
        urls = gConfig.getValue(urlsConfigPath, [])
        self.log.debug(f"Found configured URLs for {module}: {urls}")
        self.log.debug(f"This URL is {url}")
        runitStatus = options["RunitStatus"]
        wouldHave = "Would have " if not self.commitURLs else ""
        if runitStatus == "Run" and url not in urls:
            urls.append(url)
            message = f"{wouldHave}Added URL {url} to URLs for {system}/{module}"
            self.log.info(message)
            self.accounting[serviceName + "/URL"]["Treatment"] = message
            self.csAPI.modifyValue(urlsConfigPath, ",".join(urls))
        if runitStatus == "Down" and url in urls:
            urls.remove(url)
            message = f"{wouldHave}Removed URL {url} from URLs for {system}/{module}"
            self.log.info(message)
            self.accounting[serviceName + "/URL"]["Treatment"] = message
            self.csAPI.modifyValue(urlsConfigPath, ",".join(urls))

    def _getURL(self, serviceName, options):
        """Return URL for the service."""
        serviceName = serviceName.rsplit("__")[-1]
        system = options["System"]
        port = options.get("Port", self._tornadoPort)
        host = socket.getfqdn()
        protocol = options.get("Protocol", "dips")
        url = f"{protocol}://{host}:{port}/{system}/{serviceName}"
        return url
