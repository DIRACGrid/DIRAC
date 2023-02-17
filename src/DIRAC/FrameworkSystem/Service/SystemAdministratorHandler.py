""" SystemAdministrator service is a tool to control and monitor the DIRAC services and agents
"""
import socket
import os
import re
import time
import getpass
import importlib
import shutil
import platform
import psutil
import tempfile
import subprocess
import requests

from packaging.version import Version, InvalidVersion

import subprocess as commands

from datetime import datetime, timedelta

from diraccfg import CFG

from DIRAC import S_OK, S_ERROR, gConfig, rootPath, gLogger, convertToPy3VersionNumber
from DIRAC.Core.DISET.RequestHandler import RequestHandler
from DIRAC.Core.Utilities import Os
from DIRAC.Core.Utilities.Extensions import extensionsByPriority, getExtensionMetadata
from DIRAC.Core.Utilities.File import mkLink
from DIRAC.Core.Utilities.TimeUtilities import fromString, hour, day
from DIRAC.Core.Utilities.Subprocess import shellCall
from DIRAC.Core.Utilities.ThreadScheduler import gThreadScheduler
from DIRAC.Core.Security.Locations import getHostCertificateAndKeyLocation
from DIRAC.Core.Security.X509Chain import X509Chain  # pylint: disable=import-error
from DIRAC.ConfigurationSystem.Client import PathFinder
from DIRAC.FrameworkSystem.Client.ComponentInstaller import gComponentInstaller
from DIRAC.FrameworkSystem.Client.ComponentMonitoringClient import ComponentMonitoringClient

# pylint: disable=no-self-use


def loadDIRACCFG():
    installPath = gConfig.getValue("/LocalInstallation/TargetPath", "")
    if not installPath:
        installPath = rootPath
    cfgPath = os.path.join(installPath, "etc", "dirac.cfg")
    try:
        diracCFG = CFG().loadFromFile(cfgPath)
    except Exception as excp:
        return S_ERROR(f"Could not load dirac.cfg: {repr(excp)}")

    return S_OK((cfgPath, diracCFG))


class SystemAdministratorHandler(RequestHandler):
    @classmethod
    def initializeHandler(cls, serviceInfo):
        """
        Handler class initialization
        """

        # Check the flag for monitoring of the state of the host
        hostMonitoring = cls.srv_getCSOption("HostMonitoring", True)

        if hostMonitoring:
            gThreadScheduler.addPeriodicTask(60, cls.__storeHostInfo)
            # the SystemAdministrator service does not has to use the client to report data about the host.

        keepSoftwareVersions = cls.srv_getCSOption("KeepSoftwareVersions", 0)
        if keepSoftwareVersions > 0:
            gLogger.info(f"The last {keepSoftwareVersions} software version will be kept and the rest will be deleted!")
            gThreadScheduler.addPeriodicTask(
                600, cls.__deleteOldSoftware, (keepSoftwareVersions,), executions=2
            )  # it is enough to try 2 times

        return S_OK("Initialization went well")

    types_getInfo = []

    def export_getInfo(self):
        """Get versions of the installed DIRAC software and extensions, setup of the
        local installation
        """
        return gComponentInstaller.getInfo()

    types_getSoftwareComponents = []

    def export_getSoftwareComponents(self):
        """Get the list of all the components ( services and agents ) for which the software
        is installed on the system
        """
        return gComponentInstaller.getSoftwareComponents(extensionsByPriority())

    types_getInstalledComponents = []

    def export_getInstalledComponents(self):
        """Get the list of all the components ( services and agents )
        installed on the system in the runit directory
        """
        return gComponentInstaller.getInstalledComponents()

    types_getSetupComponents = []

    def export_getSetupComponents(self):
        """Get the list of all the components ( services and agents )
        set up for running with runsvdir in /opt/dirac/startup directory
        """
        return gComponentInstaller.getSetupComponents()

    types_getOverallStatus = []

    def export_getOverallStatus(self):
        """Get the complete status information of all components."""
        result = gComponentInstaller.getOverallStatus(extensionsByPriority())
        if not result["OK"]:
            return result
        statusDict = result["Value"]
        for compType in statusDict:
            for system in statusDict[compType]:
                for component in statusDict[compType][system]:
                    result = gComponentInstaller.getComponentModule(system, component, compType)
                    if not result["OK"]:
                        statusDict[compType][system][component]["Module"] = "Unknown"
                    else:
                        statusDict[compType][system][component]["Module"] = result["Value"]
        return S_OK(statusDict)

    types_getStartupComponentStatus = [list]

    def export_getStartupComponentStatus(self, componentTupleList):
        """Get the list of all the components ( services and agents )
        set up for running with runsvdir in startup directory
        """
        return gComponentInstaller.getStartupComponentStatus(componentTupleList)

    types_installComponent = [str, str, str]

    def export_installComponent(self, componentType, system, component, componentModule=""):
        """Install runit directory for the specified component"""
        return gComponentInstaller.installComponent(
            componentType, system, component, extensionsByPriority(), componentModule
        )

    types_setupComponent = [str, str, str]

    def export_setupComponent(self, componentType, system, component, componentModule=""):
        """Setup the specified component for running with the runsvdir daemon
        It implies installComponent
        """
        result = gComponentInstaller.setupComponent(
            componentType, system, component, extensionsByPriority(), componentModule
        )
        gConfig.forceRefresh()
        return result

    types_addDefaultOptionsToComponentCfg = [str, str]

    def export_addDefaultOptionsToComponentCfg(self, componentType, system, component):
        """Add default component options local component cfg"""
        return gComponentInstaller.addDefaultOptionsToComponentCfg(
            componentType, system, component, extensionsByPriority()
        )

    types_unsetupComponent = [str, str]

    def export_unsetupComponent(self, system, component):
        """Removed the specified component from running with the runsvdir daemon"""
        return gComponentInstaller.unsetupComponent(system, component)

    types_uninstallComponent = [str, str, bool]

    def export_uninstallComponent(self, system, component, removeLogs):
        """Remove runit directory for the specified component
        It implies unsetupComponent
        """
        return gComponentInstaller.uninstallComponent(system, component, removeLogs)

    types_startComponent = [str, str]

    def export_startComponent(self, system, component):
        """Start the specified component, running with the runsv daemon"""
        return gComponentInstaller.runsvctrlComponent(system, component, "u")

    types_restartComponent = [str, str]

    def export_restartComponent(self, system, component):
        """Restart the specified component, running with the runsv daemon"""
        return gComponentInstaller.runsvctrlComponent(system, component, "t")

    types_stopComponent = [str, str]

    def export_stopComponent(self, system, component):
        """Stop the specified component, running with the runsv daemon"""
        return gComponentInstaller.runsvctrlComponent(system, component, "d")

    types_getLogTail = [str, str]

    def export_getLogTail(self, system, component, length=100):
        """Get the tail of the component log file"""
        return gComponentInstaller.getLogTail(system, component, length)

    ######################################################################################
    #  Database related methods
    #
    types_getMySQLStatus = []

    def export_getMySQLStatus(self):
        """Get the status of the MySQL database installation"""
        return gComponentInstaller.getMySQLStatus()

    types_getDatabases = []

    def export_getDatabases(self, mysqlPassword=None):
        """Get the list of installed databases"""
        if mysqlPassword:
            gComponentInstaller.setMySQLPasswords(mysqlPassword)
        return gComponentInstaller.getDatabases()

    types_getAvailableDatabases = []

    def export_getAvailableDatabases(self):
        """Get the list of databases which software is installed in the system"""
        return gComponentInstaller.getAvailableDatabases()

    types_installDatabase = [str]

    def export_installDatabase(self, dbName, mysqlPassword=None):
        """Install a DIRAC database named dbName"""
        if mysqlPassword:
            gComponentInstaller.setMySQLPasswords(mysqlPassword)
        return gComponentInstaller.installDatabase(dbName)

    types_uninstallDatabase = [str]

    def export_uninstallDatabase(self, dbName, mysqlPassword=None):
        """Uninstall a DIRAC database named dbName"""
        if mysqlPassword:
            gComponentInstaller.setMySQLPasswords(mysqlPassword)
        return gComponentInstaller.uninstallDatabase(gConfig, dbName)

    types_addDatabaseOptionsToCS = [str, str]

    def export_addDatabaseOptionsToCS(self, system, database, overwrite=False):
        """Add the section with the database options to the CS"""
        return gComponentInstaller.addDatabaseOptionsToCS(gConfig, system, database, overwrite=overwrite)

    types_addDefaultOptionsToCS = [str, str, str]

    def export_addDefaultOptionsToCS(self, componentType, system, component, overwrite=False):
        """Add default component options to the global CS or to the local options"""
        return gComponentInstaller.addDefaultOptionsToCS(
            gConfig, componentType, system, component, extensionsByPriority(), overwrite=overwrite
        )

    #######################################################################################
    # General purpose methods
    #
    types_updateSoftware = [str]

    def export_updateSoftware(self, version):
        # Validate and normalise the requested version
        primaryExtension = None
        if "==" in version:
            primaryExtension, version = version.split("==")
        try:
            version = Version(version)
        except InvalidVersion:
            self.log.exception("Invalid version passed", version)
            return S_ERROR(f"Invalid version passed {version!r}")
        isPrerelease = version.is_prerelease
        version = f"v{version}"

        # Find what to install
        otherExtensions = []
        for extension in extensionsByPriority():
            if primaryExtension is None and getExtensionMetadata(extension).get("primary_extension", False):
                primaryExtension = extension
            else:
                otherExtensions.append(extension)
        self.log.info("Installing Python 3 based", f"{primaryExtension} {version}")
        self.log.info("Will also install", repr(otherExtensions))

        # Install DIRACOS
        installer_url = (
            f"https://github.com/DIRACGrid/DIRACOS2/releases/latest/download/DIRACOS-Linux-{platform.machine()}.sh"
        )
        self.log.info("Downloading DIRACOS2 installer from", installer_url)
        with tempfile.NamedTemporaryFile(suffix=".sh", mode="wb") as installer:
            with requests.get(installer_url, stream=True) as r:
                if not r.ok:
                    return S_ERROR(f"Failed to download {installer_url}")
                for chunk in r.iter_content(chunk_size=1024**2):
                    installer.write(chunk)
            installer.flush()
            self.log.info("Downloaded DIRACOS installer to", installer.name)

            newProPrefix = os.path.join(
                rootPath,
                "versions",
                f"{version}-{datetime.utcnow().strftime('%s')}",
            )
            installPrefix = os.path.join(newProPrefix, f"{platform.system()}-{platform.machine()}")
            self.log.info("Running DIRACOS installer for prefix", installPrefix)
            r = subprocess.run(
                ["bash", installer.name, "-p", installPrefix],
                stderr=subprocess.PIPE,
                text=True,
                check=False,
                timeout=600,
            )
            if r.returncode != 0:
                stderr = [x for x in r.stderr.split("\n") if not x.startswith("Extracting : ")]
                self.log.error("Installing DIRACOS2 failed with returncode", f"{r.returncode} and stdout: {stderr}")
                return S_ERROR(f"Failed to install DIRACOS2 {stderr}")

        # Install DIRAC
        cmd = [f"{installPrefix}/bin/pip", "install", "--no-color", "-v"]
        if isPrerelease:
            cmd += ["--pre"]
        cmd += [f"{primaryExtension}[server]=={version}"]
        cmd += [f"{e}[server]" for e in otherExtensions]
        r = subprocess.run(
            cmd,
            stderr=subprocess.PIPE,
            text=True,
            check=False,
            timeout=600,
        )
        if r.returncode != 0:
            self.log.error("Installing DIRACOS2 failed with returncode", f"{r.returncode} and stdout: {r.stderr}")
            return S_ERROR(f"Failed to install DIRACOS2 with message {r.stderr}")

        # Update the pro link
        oldLink = os.path.join(gComponentInstaller.instancePath, "old")
        proLink = os.path.join(gComponentInstaller.instancePath, "pro")
        if os.path.exists(oldLink):
            os.remove(oldLink)
        os.rename(proLink, oldLink)
        mkLink(newProPrefix, proLink)

        return S_OK()

    types_revertSoftware = []

    def export_revertSoftware(self):
        """Revert the last installed version of software to the previous one"""
        oldLink = os.path.join(gComponentInstaller.instancePath, "old")
        oldPath = os.readlink(oldLink)
        proLink = os.path.join(gComponentInstaller.instancePath, "pro")
        os.remove(proLink)
        mkLink(oldPath, proLink)

        return S_OK(oldPath)

    types_setProject = [str]

    def export_setProject(self, projectName):
        result = loadDIRACCFG()
        if not result["OK"]:
            return result
        cfgPath, diracCFG = result["Value"]
        gLogger.notice(f"Setting project to {projectName}")
        diracCFG.setOption("/LocalInstallation/Project", projectName, "Project to install")
        try:
            with open(cfgPath, "w") as fd:
                fd.write(str(diracCFG))
        except OSError as excp:
            return S_ERROR(f"Could not write dirac.cfg: {str(excp)}")
        return S_OK()

    types_getProject = []

    def export_getProject(self):
        result = loadDIRACCFG()
        if not result["OK"]:
            return result
        _cfgPath, diracCFG = result["Value"]
        return S_OK(diracCFG.getOption("/LocalInstallation/Project", "DIRAC"))

    types_addOptionToDiracCfg = [str, str]

    def export_addOptionToDiracCfg(self, option, value):
        """Set option in the local configuration file"""
        return gComponentInstaller.addOptionToDiracCfg(option, value)

    types_executeCommand = [str]

    def export_executeCommand(self, command):
        """Execute a command locally and return its output"""
        result = shellCall(60, command)
        return result

    types_checkComponentLog = [[str, list]]

    def export_checkComponentLog(self, component):
        """Check component log for errors"""
        componentList = []
        if "*" in component:
            if component == "*":
                result = gComponentInstaller.getSetupComponents()
                if result["OK"]:
                    for ctype in ["Services", "Agents", "Executors"]:
                        if ctype in result["Value"]:
                            for sname in result["Value"][ctype]:
                                for cname in result["Value"][ctype][sname]:
                                    componentList.append("/".join([sname, cname]))
        elif isinstance(component, str):
            componentList = [component]
        else:
            componentList = component

        resultDict = {}
        for comp in componentList:
            if "/" not in comp:
                continue
            system, cname = comp.split("/")

            startDir = gComponentInstaller.startDir
            currentLog = startDir + "/" + system + "_" + cname + "/log/current"
            try:
                with open(currentLog) as logFile:
                    logLines = logFile.readlines()
            except OSError as err:
                gLogger.error("File does not exists:", currentLog)
                resultDict[comp] = {"ErrorsHour": -1, "ErrorsDay": -1, "LastError": currentLog + "::" + repr(err)}
                continue

            errors_1 = 0
            errors_24 = 0
            now = datetime.utcnow()
            lastError = ""
            for line in logLines:
                if "ERROR:" in line:
                    fields = line.split()
                    recent = False
                    if len(fields) < 2:  # if the line contains only one word
                        lastError = line.split("ERROR:")[-1].strip()
                        continue
                    timeStamp = fromString(fields[0] + " " + fields[1])
                    if not timeStamp:  # if the timestamp is missing in the log
                        lastError = line.split("ERROR:")[-1].strip()
                        continue
                    if (now - timeStamp) < hour:
                        errors_1 += 1
                        recent = True
                    if (now - timeStamp) < day:
                        errors_24 += 1
                        recent = True
                    if recent:
                        lastError = line.split("ERROR:")[-1].strip()

            resultDict[comp] = {"ErrorsHour": errors_1, "ErrorsDay": errors_24, "LastError": lastError}

        return S_OK(resultDict)

    @staticmethod
    def __readHostInfo():
        """Get host current loads, memory, etc"""

        result = dict()
        # Memory info
        re_parser = re.compile(r"^(?P<key>\S*):\s*(?P<value>\d*)\s*kB")
        for line in open("/proc/meminfo"):
            match = re_parser.match(line)
            if not match:
                continue
            key, value = match.groups(["key", "value"])
            result[key] = int(value)

        for mtype in ["Mem", "Swap"]:
            memory = int(result.get(mtype + "Total"))
            mfree = int(result.get(mtype + "Free"))
            if memory > 0:
                percentage = float(memory - mfree) / float(memory) * 100.0
            else:
                percentage = 0
            name = "Memory"
            if mtype == "Swap":
                name = "Swap"
            result[name] = f"{percentage:.1f}%/{memory / 1024.0:.1f}MB"

        # Loads
        l1, l5, l15 = (str(lx) for lx in os.getloadavg())
        result["Load1"] = l1
        result["Load5"] = l5
        result["Load15"] = l15
        result["Load"] = "/".join([l1, l5, l15])

        # CPU info
        with open("/proc/cpuinfo") as fd:
            lines = fd.readlines()
            processors = 0
            physCores = {}
            for line in lines:
                if line.strip():
                    parameter, value = line.split(":")
                    parameter = parameter.strip()
                    value = value.strip()
                    if parameter.startswith("processor"):
                        processors += 1
                    if parameter.startswith("physical id"):
                        physCores[value] = parameter
                    if parameter.startswith("model name"):
                        result["CPUModel"] = value
                    if parameter.startswith("cpu MHz"):
                        result["CPUClock"] = value
            result["Cores"] = processors
            result["PhysicalCores"] = len(physCores)

        # Disk occupancy
        summary = ""
        _status, output = commands.getstatusoutput("df")
        lines = output.split("\n")
        for i in range(len(lines)):
            if lines[i].startswith("/dev"):
                fields = lines[i].split()
                if len(fields) == 1:
                    fields += lines[i + 1].split()
                partition = fields[5]
                occupancy = fields[4]
                summary += f",{partition}:{occupancy}"
        result["DiskOccupancy"] = summary[1:]
        result["RootDiskSpace"] = Os.getDiskSpace(rootPath)

        # Open files
        puser = getpass.getuser()
        _status, output = commands.getstatusoutput("lsof")
        pipes = 0
        files = 0
        sockets = 0
        lines = output.split("\n")
        for line in lines:
            fType = line.split()[4]
            user = line.split()[2]
            if user == puser:
                if fType in ["REG"]:
                    files += 1
                elif fType in ["unix", "IPv4"]:
                    sockets += 1
                elif fType in ["FIFO"]:
                    pipes += 1
        result["OpenSockets"] = sockets
        result["OpenFiles"] = files
        result["OpenPipes"] = pipes

        infoResult = gComponentInstaller.getInfo()
        if infoResult["OK"]:
            result.update(infoResult["Value"])
            # the infoResult value is {"Extensions":{'a1':'v1',a2:'v2'}; we convert to a string
            result.update(
                {
                    "Extensions": ";".join(
                        [f"{key}:{value}" for (key, value) in infoResult["Value"].get("Extensions").items()]
                    )
                }
            )

        # Host certificate properties
        certFile, _keyFile = getHostCertificateAndKeyLocation()
        chain = X509Chain()
        chain.loadChainFromFile(certFile)
        resultCert = chain.getCredentials()
        if resultCert["OK"]:
            result["SecondsLeft"] = resultCert["Value"]["secondsLeft"]
            result["CertificateValidity"] = str(timedelta(seconds=resultCert["Value"]["secondsLeft"]))
            result["CertificateDN"] = resultCert["Value"]["subject"]
            result["CertificateIssuer"] = resultCert["Value"]["issuer"]

        # Host uptime
        result["Uptime"] = str(timedelta(seconds=(time.time() - psutil.boot_time())))

        return S_OK(result)

    types_getHostInfo = []

    def export_getHostInfo(self):
        """
        Retrieve host parameters
        """
        result = ComponentMonitoringClient().getLog(socket.getfqdn())
        if result["OK"]:
            return S_OK(result["Value"][0])
        return self.__readHostInfo()

    types_getUsedPorts = []

    def export_getUsedPorts(self):
        """
        Retrieve the ports in use by services on this host

        :return: Returns a dictionary containing, for each system, which port is being used by which service
        """
        result = gComponentInstaller.getSetupComponents()
        if not result["OK"]:
            return result

        services = result["Value"]["Services"]
        ports = {}
        for system in services:
            ports[system] = {}
            for service in services[system]:
                url = PathFinder.getServiceURL(f"{system}/{service}")
                port = re.search(r":(\d{4,5})/", url)
                if port:
                    ports[system][service] = port.group(1)
                else:
                    ports[system][service] = "None"

        return S_OK(ports)

    types_getComponentDocumentation = [str, str, str]

    def export_getComponentDocumentation(self, cType, system, module):
        if cType == "service":
            module = f"{module}Handler"
        # Look for the component in extensions
        for extension in extensionsByPriority():
            moduleName = [extension, system + "System", cType.capitalize(), module]
            try:
                importedModule = importlib.import_module(moduleName)
                return S_OK(importedModule.__doc__)
            except Exception:
                pass
        return S_ERROR("No documentation was found")

    @staticmethod
    def __storeHostInfo():
        """
        Retrieves and stores into a MySQL database information about the host
        """
        result = SystemAdministratorHandler.__readHostInfo()
        if not result["OK"]:
            gLogger.error(result["Message"])
            return result

        fields = result["Value"]
        fields["Timestamp"] = datetime.utcnow()
        fields["Extension"] = fields["Extensions"]
        result = ComponentMonitoringClient().updateLog(socket.getfqdn(), fields)
        if not result["OK"]:
            gLogger.error(result["Message"])
            return result

        return S_OK("Profiling information logged correctly")

    @staticmethod
    def __deleteOldSoftware(keepLast):
        """
        It removes all versions except the last x

        :param int keepLast: the number of the software version, what we keep
        """
        versionsDirectory = os.path.join(rootPath, "versions")
        if not os.path.isdir(versionsDirectory):
            gLogger.error("The DIRAC.rootPath is not correct:", versionsDirectory)
            return

        softwareDirs = {}
        for dirName in os.listdir(versionsDirectory):
            try:
                # Python 3 uses dashes while Python 2 uses underscores so replace and split
                # v10.3.1-1637142594, v10r2p10_1629962176
                # TODO: This can be simplified eventually but it's better to leave it for now
                version, timestamp = dirName.replace("_", "-").split("-")
                version = Version(convertToPy3VersionNumber(version))
                timestamp = int(timestamp)
            except Exception:
                gLogger.exception("Failed to extract version info from", f"{dirName!r} in {versionsDirectory!r}")
                continue
            softwareDirs[dirName] = (version, timestamp)
        softwareDirs = sorted(softwareDirs, key=softwareDirs.__getitem__, reverse=False)

        try:
            for directoryName in softwareDirs[: -1 * int(keepLast)]:
                fullPath = os.path.join(versionsDirectory, directoryName)
                gLogger.info(f"Removing {fullPath} directory.")
                shutil.rmtree(fullPath)
        except Exception as e:
            gLogger.error("Can not delete old DIRAC versions from the file system", repr(e))
