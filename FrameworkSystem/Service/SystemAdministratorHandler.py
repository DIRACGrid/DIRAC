""" SystemAdministrator service is a tool to control and monitor the DIRAC services and agents
"""

__RCSID__ = "$Id$"

import socket
import os
import re
import time
import commands
import getpass
import importlib
import shutil
from datetime import datetime, timedelta
from distutils.version import LooseVersion  # pylint: disable=no-name-in-module,import-error

import psutil

from DIRAC import S_OK, S_ERROR, gConfig, rootPath, gLogger
from DIRAC.Core.DISET.RequestHandler import RequestHandler
from DIRAC.Core.Utilities import CFG, Os
from DIRAC.Core.Utilities.File import mkLink
from DIRAC.Core.Utilities.Time import dateTime, fromString, hour, day
from DIRAC.Core.Utilities.Subprocess import shellCall, systemCall
from DIRAC.Core.Utilities.ThreadScheduler import gThreadScheduler
from DIRAC.Core.Utilities import Profiler
from DIRAC.Core.Security.Locations import getHostCertificateAndKeyLocation
from DIRAC.Core.Security.X509Chain import X509Chain  # pylint: disable=import-error
from DIRAC.ConfigurationSystem.Client import PathFinder
from DIRAC.ConfigurationSystem.Client.Helpers.CSGlobals import getCSExtensions
from DIRAC.FrameworkSystem.Client.ComponentInstaller import gComponentInstaller
from DIRAC.FrameworkSystem.Client.ComponentMonitoringClient import ComponentMonitoringClient
from DIRAC.MonitoringSystem.Client.MonitoringReporter import MonitoringReporter

gMonitoringReporter = None

gProfilers = {}

# pylint: disable=no-self-use


def loadDIRACCFG():
  installPath = gConfig.getValue('/LocalInstallation/TargetPath',
                                 gConfig.getValue('/LocalInstallation/RootPath', ''))
  if not installPath:
    installPath = rootPath
  cfgPath = os.path.join(installPath, 'etc', 'dirac.cfg')
  try:
    diracCFG = CFG.CFG().loadFromFile(cfgPath)
  except BaseException as excp:
    return S_ERROR("Could not load dirac.cfg: %s" % repr(excp))

  return S_OK((cfgPath, diracCFG))


class SystemAdministratorHandler(RequestHandler):

  @classmethod
  def initializeHandler(cls, serviceInfo):
    """
    Handler class initialization
    """

    # Check the flag for monitoring of the state of the host
    hostMonitoring = cls.srv_getCSOption('HostMonitoring', True)

    if hostMonitoring:
      gThreadScheduler.addPeriodicTask(60, cls.__storeHostInfo)
      # the SystemAdministrator service does not has to use the client to report data about the host.

    # Check the flag for dynamic monitoring
    dynamicMonitoring = cls.srv_getCSOption('DynamicMonitoring', False)
    messageQueue = cls.srv_getCSOption('MessageQueue', 'dirac.componentmonitoring')

    if dynamicMonitoring:
      global gMonitoringReporter
      gMonitoringReporter = MonitoringReporter(
          monitoringType="ComponentMonitoring", failoverQueueName=messageQueue)
      gThreadScheduler.addPeriodicTask(120, cls.__storeProfiling)

    keepSoftwareVersions = cls.srv_getCSOption('KeepSoftwareVersions', 0)
    if keepSoftwareVersions > 0:
      gLogger.info("The last %s software version will be kept and the rest will be deleted!" % keepSoftwareVersions)
      gThreadScheduler.addPeriodicTask(600,
                                       cls.__deleteOldSoftware,
                                       (keepSoftwareVersions, ),
                                       executions=2)  # it is enough to try 2 times

    return S_OK('Initialization went well')

  types_getInfo = []

  def export_getInfo(self):
    """  Get versions of the installed DIRAC software and extensions, setup of the
         local installation
    """
    return gComponentInstaller.getInfo()

  types_getSoftwareComponents = []

  def export_getSoftwareComponents(self):
    """  Get the list of all the components ( services and agents ) for which the software
         is installed on the system
    """
    return gComponentInstaller.getSoftwareComponents(getCSExtensions())

  types_getInstalledComponents = []

  def export_getInstalledComponents(self):
    """  Get the list of all the components ( services and agents )
         installed on the system in the runit directory
    """
    return gComponentInstaller.getInstalledComponents()

  types_getSetupComponents = []

  def export_getSetupComponents(self):
    """  Get the list of all the components ( services and agents )
         set up for running with runsvdir in /opt/dirac/startup directory
    """
    return gComponentInstaller.getSetupComponents()

  types_getOverallStatus = []

  def export_getOverallStatus(self):
    """  Get the complete status information for the components in the
         given list
    """
    result = gComponentInstaller.getOverallStatus(getCSExtensions())
    if not result['OK']:
      return result
    statusDict = result['Value']
    for compType in statusDict:
      for system in statusDict[compType]:
        for component in statusDict[compType][system]:
          result = gComponentInstaller.getComponentModule(system, component, compType)
          if not result['OK']:
            statusDict[compType][system][component]['Module'] = "Unknown"
          else:
            statusDict[compType][system][component]['Module'] = result['Value']
    return S_OK(statusDict)

  types_getStartupComponentStatus = [list]

  def export_getStartupComponentStatus(self, componentTupleList):
    """  Get the list of all the components ( services and agents )
         set up for running with runsvdir in startup directory
    """
    return gComponentInstaller.getStartupComponentStatus(componentTupleList)

  types_installComponent = [basestring, basestring, basestring]

  def export_installComponent(self, componentType, system, component, componentModule=''):
    """ Install runit directory for the specified component
    """
    return gComponentInstaller.installComponent(componentType, system, component, getCSExtensions(), componentModule)

  types_setupComponent = [basestring, basestring, basestring]

  def export_setupComponent(self, componentType, system, component, componentModule=''):
    """ Setup the specified component for running with the runsvdir daemon
        It implies installComponent
    """
    result = gComponentInstaller.setupComponent(componentType, system, component, getCSExtensions(), componentModule)
    gConfig.forceRefresh()
    return result

  types_addDefaultOptionsToComponentCfg = [basestring, basestring]

  def export_addDefaultOptionsToComponentCfg(self, componentType, system, component):
    """ Add default component options local component cfg
    """
    return gComponentInstaller.addDefaultOptionsToComponentCfg(componentType, system, component, getCSExtensions())

  types_unsetupComponent = [basestring, basestring]

  def export_unsetupComponent(self, system, component):
    """ Removed the specified component from running with the runsvdir daemon
    """
    return gComponentInstaller.unsetupComponent(system, component)

  types_uninstallComponent = [basestring, basestring, bool]

  def export_uninstallComponent(self, system, component, removeLogs):
    """ Remove runit directory for the specified component
        It implies unsetupComponent
    """
    return gComponentInstaller.uninstallComponent(system, component, removeLogs)

  types_startComponent = [basestring, basestring]

  def export_startComponent(self, system, component):
    """ Start the specified component, running with the runsv daemon
    """
    return gComponentInstaller.runsvctrlComponent(system, component, 'u')

  types_restartComponent = [basestring, basestring]

  def export_restartComponent(self, system, component):
    """ Restart the specified component, running with the runsv daemon
    """
    return gComponentInstaller.runsvctrlComponent(system, component, 't')

  types_stopComponent = [basestring, basestring]

  def export_stopComponent(self, system, component):
    """ Stop the specified component, running with the runsv daemon
    """
    return gComponentInstaller.runsvctrlComponent(system, component, 'd')

  types_getLogTail = [basestring, basestring]

  def export_getLogTail(self, system, component, length=100):
    """ Get the tail of the component log file
    """
    return gComponentInstaller.getLogTail(system, component, length)

######################################################################################
#  Database related methods
#
  types_getMySQLStatus = []

  def export_getMySQLStatus(self):
    """ Get the status of the MySQL database installation
    """
    return gComponentInstaller.getMySQLStatus()

  types_getDatabases = []

  def export_getDatabases(self, mysqlPassword=None):
    """ Get the list of installed databases
    """
    if mysqlPassword:
      gComponentInstaller.setMySQLPasswords(mysqlPassword)
    return gComponentInstaller.getDatabases()

  types_getAvailableDatabases = []

  def export_getAvailableDatabases(self):
    """ Get the list of databases which software is installed in the system
    """
    return gComponentInstaller.getAvailableDatabases(getCSExtensions())

  types_installMySQL = []

  def export_installMySQL(self, mysqlPassword=None, diracPassword=None):
    """ Install MySQL database server
    """

    if mysqlPassword or diracPassword:
      gComponentInstaller.setMySQLPasswords(mysqlPassword, diracPassword)
    if gComponentInstaller.mysqlInstalled()['OK']:
      return S_OK('Already installed')

    result = gComponentInstaller.installMySQL()
    if not result['OK']:
      return result

    return S_OK('Successfully installed')

  types_installDatabase = [basestring]

  def export_installDatabase(self, dbName, mysqlPassword=None):
    """ Install a DIRAC database named dbName
    """
    if mysqlPassword:
      gComponentInstaller.setMySQLPasswords(mysqlPassword)
    return gComponentInstaller.installDatabase(dbName)

  types_uninstallDatabase = [basestring]

  def export_uninstallDatabase(self, dbName, mysqlPassword=None):
    """ Uninstall a DIRAC database named dbName
    """
    if mysqlPassword:
      gComponentInstaller.setMySQLPasswords(mysqlPassword)
    return gComponentInstaller.uninstallDatabase(gConfig, dbName)

  types_addDatabaseOptionsToCS = [basestring, basestring]

  def export_addDatabaseOptionsToCS(self, system, database, overwrite=False):
    """ Add the section with the database options to the CS
    """
    return gComponentInstaller.addDatabaseOptionsToCS(gConfig, system, database, overwrite=overwrite)

  types_addDefaultOptionsToCS = [basestring, basestring, basestring]

  def export_addDefaultOptionsToCS(self, componentType, system, component, overwrite=False):
    """ Add default component options to the global CS or to the local options
    """
    return gComponentInstaller.addDefaultOptionsToCS(gConfig, componentType, system, component,
                                                     getCSExtensions(),
                                                     overwrite=overwrite)

#######################################################################################
# General purpose methods
#
  types_updateSoftware = [basestring]

  def export_updateSoftware(self, version, rootPath="", gridVersion=""):
    """ Update the local DIRAC software installation to version
    """

    # Check that we have a sane local configuration
    result = gConfig.getOptionsDict('/LocalInstallation')
    if not result['OK']:
      return S_ERROR('Invalid installation - missing /LocalInstallation section in the configuration')
    elif not result['Value']:
      return S_ERROR('Invalid installation - empty /LocalInstallation section in the configuration')

    if rootPath and not os.path.exists(rootPath):
      return S_ERROR('Path "%s" does not exists' % rootPath)
    # For LHCb we need to check Oracle client
    installOracleClient = False
    oracleFlag = gConfig.getValue('/LocalInstallation/InstallOracleClient', 'unknown')
    if oracleFlag.lower() in ['yes', 'true', '1']:
      installOracleClient = True
    elif oracleFlag.lower() == "unknown":
      result = systemCall(30, ['python', '-c', 'import cx_Oracle'])
      if result['OK'] and result['Value'][0] == 0:
        installOracleClient = True

    cmdList = ['dirac-install', '-r', version, '-t', 'server']
    if rootPath:
      cmdList.extend(['-P', rootPath])

    # Check if there are extensions
    extensionList = getCSExtensions()
    if extensionList:
      # by default we do not install WebApp
      if "WebApp" in extensionList:
        extensionList.remove("WebApp")

    webPortal = gConfig.getValue('/LocalInstallation/WebApp', False)  # this is the new portal
    if webPortal:
      if "WebAppDIRAC" not in extensionList:
        extensionList.append("WebAppDIRAC")

    cmdList += ['-e', ','.join(extensionList)]

    project = gConfig.getValue('/LocalInstallation/Project')
    if project:
      cmdList += ['-l', project]

    # Are grid middleware bindings required ?
    if gridVersion:
      cmdList.extend(['-g', gridVersion])

    targetPath = gConfig.getValue('/LocalInstallation/TargetPath',
                                  gConfig.getValue('/LocalInstallation/RootPath', ''))
    if targetPath and os.path.exists(targetPath + '/etc/dirac.cfg'):
      cmdList.append(targetPath + '/etc/dirac.cfg')
    else:
      return S_ERROR('Local configuration not found')

    result = systemCall(240, cmdList)
    if not result['OK']:
      return result
    status = result['Value'][0]
    if status != 0:
      # Get error messages
      error = []
      output = result['Value'][1].split('\n')
      for line in output:
        line = line.strip()
        if 'error' in line.lower():
          error.append(line)
      if error:
        message = '\n'.join(error)
      else:
        message = "Failed to update software to %s" % version
      return S_ERROR(message)

    # Check if there is a MySQL installation and fix the server scripts if necessary
    if os.path.exists(gComponentInstaller.mysqlDir):
      startupScript = os.path.join(gComponentInstaller.instancePath,
                                   'mysql', 'share', 'mysql', 'mysql.server')
      if not os.path.exists(startupScript):
        startupScript = os.path.join(gComponentInstaller.instancePath, 'pro',
                                     'mysql', 'share', 'mysql', 'mysql.server')
      if os.path.exists(startupScript):
        gComponentInstaller.fixMySQLScripts(startupScript)

    # For LHCb we need to check Oracle client
    if installOracleClient:
      result = systemCall(30, 'install_oracle-client.sh')
      if not result['OK']:
        return result
      status = result['Value'][0]
      if status != 0:
        # Get error messages
        error = result['Value'][1].split('\n')
        error.extend(result['Value'][2].split('\n'))
        error.append('Failed to install Oracle client module')
        return S_ERROR('\n'.join(error))

    if webPortal:
      # we have a to compile the new web portal...
      webappCompileScript = os.path.join(
          gComponentInstaller.instancePath, 'pro', "WebAppDIRAC/scripts", "dirac-webapp-compile.py")
      outfile = "%s.out" % webappCompileScript
      err = "%s.err" % webappCompileScript
      result = systemCall(False, ['dirac-webapp-compile', ' > ', outfile, ' 2> ', err])
      if not result['OK']:
        return result
      if result['Value'][0] != 0:
        error = result['Value'][1].split('\n')
        error.extend(result['Value'][2].split('\n'))
        error.append('Failed to compile the java script!')
        return S_ERROR('\n'.join(error))

    return S_OK()

  types_revertSoftware = []

  def export_revertSoftware(self):
    """ Revert the last installed version of software to the previous one
    """
    oldLink = os.path.join(gComponentInstaller.instancePath, 'old')
    oldPath = os.readlink(oldLink)
    proLink = os.path.join(gComponentInstaller.instancePath, 'pro')
    os.remove(proLink)
    mkLink(oldPath, proLink)

    return S_OK(oldPath)

  types_setProject = [basestring]

  def export_setProject(self, projectName):
    result = loadDIRACCFG()
    if not result['OK']:
      return result
    cfgPath, diracCFG = result['Value']
    gLogger.notice("Setting project to %s" % projectName)
    diracCFG.setOption("/LocalInstallation/Project", projectName, "Project to install")
    try:
      with open(cfgPath, "w") as fd:
        fd.write(str(diracCFG))
    except IOError as excp:
      return S_ERROR("Could not write dirac.cfg: %s" % str(excp))
    return S_OK()

  types_getProject = []

  def export_getProject(self):
    result = loadDIRACCFG()
    if not result['OK']:
      return result
    _cfgPath, diracCFG = result['Value']
    return S_OK(diracCFG.getOption("/LocalInstallation/Project", "DIRAC"))

  types_addOptionToDiracCfg = [basestring, basestring]

  def export_addOptionToDiracCfg(self, option, value):
    """ Set option in the local configuration file
    """
    return gComponentInstaller.addOptionToDiracCfg(option, value)

  types_executeCommand = [basestring]

  def export_executeCommand(self, command):
    """ Execute a command locally and return its output
    """
    result = shellCall(60, command)
    return result

  types_checkComponentLog = [[basestring, list]]

  def export_checkComponentLog(self, component):
    """ Check component log for errors
    """
    componentList = []
    if '*' in component:
      if component == '*':
        result = gComponentInstaller.getSetupComponents()
        if result['OK']:
          for ctype in ['Services', 'Agents', 'Executors']:
            if ctype in result['Value']:
              for sname in result['Value'][ctype]:
                for cname in result['Value'][ctype][sname]:
                  componentList.append('/'.join([sname, cname]))
    elif isinstance(component, basestring):
      componentList = [component]
    else:
      componentList = component

    resultDict = {}
    for comp in componentList:
      if '/' not in comp:
        continue
      system, cname = comp.split('/')

      startDir = gComponentInstaller.startDir
      currentLog = startDir + '/' + system + '_' + cname + '/log/current'
      try:
        logFile = file(currentLog, 'r')
      except IOError as err:
        gLogger.error("File does not exists:", currentLog)
        resultDict[comp] = {'ErrorsHour': -1, 'ErrorsDay': -1, 'LastError': currentLog + '::' + repr(err)}
        continue

      logLines = logFile.readlines()
      logFile.close()

      errors_1 = 0
      errors_24 = 0
      now = dateTime()
      lastError = ''
      for line in logLines:
        if "ERROR:" in line:
          fields = line.split()
          recent = False
          if len(fields) < 2:  # if the line contains only one word
            lastError = line.split('ERROR:')[-1].strip()
            continue
          timeStamp = fromString(fields[0] + ' ' + fields[1])
          if not timeStamp:  # if the timestamp is missing in the log
            lastError = line.split('ERROR:')[-1].strip()
            continue
          if (now - timeStamp) < hour:
            errors_1 += 1
            recent = True
          if (now - timeStamp) < day:
            errors_24 += 1
            recent = True
          if recent:
            lastError = line.split('ERROR:')[-1].strip()

      resultDict[comp] = {'ErrorsHour': errors_1, 'ErrorsDay': errors_24, 'LastError': lastError}

    return S_OK(resultDict)

  @staticmethod
  def __readHostInfo():
    """ Get host current loads, memory, etc
    """

    result = dict()
    # Memory info
    re_parser = re.compile(r'^(?P<key>\S*):\s*(?P<value>\d*)\s*kB')
    for line in open('/proc/meminfo'):
      match = re_parser.match(line)
      if not match:
        continue
      key, value = match.groups(['key', 'value'])
      result[key] = int(value)

    for mtype in ['Mem', 'Swap']:
      memory = int(result.get(mtype + 'Total'))
      mfree = int(result.get(mtype + 'Free'))
      if memory > 0:
        percentage = float(memory - mfree) / float(memory) * 100.
      else:
        percentage = 0
      name = 'Memory'
      if mtype == "Swap":
        name = 'Swap'
      result[name] = '%.1f%%/%.1fMB' % (percentage, memory / 1024.)

    # Loads
    l1, l5, l15 = (str(lx) for lx in os.getloadavg())
    result['Load1'] = l1
    result['Load5'] = l5
    result['Load15'] = l15
    result['Load'] = '/'.join([l1, l5, l15])

    # CPU info
    with open('/proc/cpuinfo', 'r') as fd:
      lines = fd.readlines()
      processors = 0
      physCores = {}
      for line in lines:
        if line.strip():
          parameter, value = line.split(':')
          parameter = parameter.strip()
          value = value.strip()
          if parameter.startswith('processor'):
            processors += 1
          if parameter.startswith('physical id'):
            physCores[value] = parameter
          if parameter.startswith('model name'):
            result['CPUModel'] = value
          if parameter.startswith('cpu MHz'):
            result['CPUClock'] = value
      result['Cores'] = processors
      result['PhysicalCores'] = len(physCores)

    # Disk occupancy
    summary = ''
    _status, output = commands.getstatusoutput('df')
    lines = output.split('\n')
    for i in xrange(len(lines)):
      if lines[i].startswith('/dev'):
        fields = lines[i].split()
        if len(fields) == 1:
          fields += lines[i + 1].split()
        _disk = fields[0].replace('/dev/sd', '')
        partition = fields[5]
        occupancy = fields[4]
        summary += ",%s:%s" % (partition, occupancy)
    result['DiskOccupancy'] = summary[1:]
    result['RootDiskSpace'] = Os.getDiskSpace(rootPath)

    # Open files
    puser = getpass.getuser()
    _status, output = commands.getstatusoutput('lsof')
    pipes = 0
    files = 0
    sockets = 0
    lines = output.split('\n')
    for line in lines:
      fType = line.split()[4]
      user = line.split()[2]
      if user == puser:
        if fType in ['REG']:
          files += 1
        elif fType in ['unix', 'IPv4']:
          sockets += 1
        elif fType in ['FIFO']:
          pipes += 1
    result['OpenSockets'] = sockets
    result['OpenFiles'] = files
    result['OpenPipes'] = pipes

    infoResult = gComponentInstaller.getInfo()
    if infoResult['OK']:
      result.update(infoResult['Value'])
      # the infoResult value is {"Extensions":{'a1':'v1',a2:'v2'}; we convert to a string
      result.update({"Extensions": ";".join(["%s:%s" % (key, value)
                                             for (key, value) in infoResult["Value"].get('Extensions').iteritems()])})

    # Host certificate properties
    certFile, _keyFile = getHostCertificateAndKeyLocation()
    chain = X509Chain()
    chain.loadChainFromFile(certFile)
    resultCert = chain.getCredentials()
    if resultCert['OK']:
      result['SecondsLeft'] = resultCert['Value']['secondsLeft']
      result['CertificateValidity'] = str(timedelta(seconds=resultCert['Value']['secondsLeft']))
      result['CertificateDN'] = resultCert['Value']['subject']
      result['HostProperties'] = resultCert['Value']['groupProperties']
      result['CertificateIssuer'] = resultCert['Value']['issuer']

    # Host uptime
    result['Uptime'] = str(timedelta(seconds=(time.time() - psutil.boot_time())))

    return S_OK(result)

  types_getHostInfo = []

  def export_getHostInfo(self):
    """
    Retrieve host parameters
    """
    client = ComponentMonitoringClient()
    result = client.getLog(socket.getfqdn())

    if result['OK']:
      return S_OK(result['Value'][0])
    return self.__readHostInfo()

  types_getUsedPorts = []

  def export_getUsedPorts(self):
    """
    Retrieve the ports in use by services on this host

    :return: Returns a dictionary containing, for each system, which port is being used by which service
    """
    result = gComponentInstaller.getSetupComponents()
    if not result['OK']:
      return result

    services = result['Value']['Services']
    ports = {}
    for system in services:
      ports[system] = {}
      for service in services[system]:
        url = PathFinder.getServiceURL('%s/%s' % (system, service))
        port = re.search(r':(\d{4,5})/', url)
        if port:
          ports[system][service] = port.group(1)
        else:
          ports[system][service] = 'None'

    return S_OK(ports)

  types_getComponentDocumentation = [basestring, basestring, basestring]

  def export_getComponentDocumentation(self, cType, system, module):
    if cType == 'service':
      module = '%sHandler' % module

    result = gComponentInstaller.getExtensions()
    extensions = result['Value']
    # Look for the component in extensions
    for extension in extensions:
      try:
        importedModule = importlib.import_module('%s.%sSystem.%s.%s' % (extension, system,
                                                                        cType.capitalize(), module))
        return S_OK(importedModule.__doc__)
      except Exception as _e:
        pass

    # If not in an extension, try in base DIRAC
    try:
      importedModule = importlib.import_module('DIRAC.%sSystem.%s.%s' % (system, cType.capitalize(), module))
      return S_OK(importedModule.__doc__)
    except Exception as _e:
      return S_ERROR('No documentation was found')

  @staticmethod
  def __storeHostInfo():
    """
    Retrieves and stores into a MySQL database information about the host
    """
    result = SystemAdministratorHandler.__readHostInfo()
    if not result['OK']:
      gLogger.error(result['Message'])
      return result

    fields = result['Value']
    fields['Timestamp'] = datetime.utcnow()
    fields['Extension'] = fields['Extensions']
    client = ComponentMonitoringClient()
    result = client.updateLog(socket.getfqdn(), fields)
    if not result['OK']:
      gLogger.error(result['Message'])
      return result

    return S_OK('Profiling information logged correctly')

  @staticmethod
  def __storeProfiling():
    """
    Retrieves and stores into ElasticSearch profiling information about the components on the host
    """
    # TODO: if we have a component which is not running, we will not profile the running processes
    result = gComponentInstaller.getStartupComponentStatus([])
    if not result['OK']:
      gLogger.error(result['Message'])
      return S_ERROR(result['Message'])
    startupComps = result['Value']

    result = gComponentInstaller.getSetupComponents()
    if not result['OK']:
      gLogger.error(result['Message'])
      return S_ERROR(result['Message'])
    setupComps = result['Value']

    # Get the profiling information for every running component and send it to MonitoringSystem
    for cType in setupComps:
      for system in setupComps[cType]:
        for comp in setupComps[cType][system]:
          instance = "%s_%s" % (system, comp)
          if instance not in startupComps:
            gLogger.error("Wrongly configured component: %s" % instance)
            continue
          pid = startupComps[instance]['PID']
          if pid not in gProfilers:
            gProfilers[pid] = Profiler.Profiler(pid)
          profiler = gProfilers[pid]
          result = profiler.getAllProcessData()
          if result['OK']:
            log = result['Value']['stats']
            log['host'] = socket.getfqdn()
            log['component'] = instance
            log['timestamp'] = result['Value']['datetime']
            gMonitoringReporter.addRecord(log)
          else:
            gLogger.error(result['Message'])
            return result
    gMonitoringReporter.commit()
    return S_OK('Profiling information logged correctly')

  @staticmethod
  def __deleteOldSoftware(keepLast):
    """
    It removes all versions except the last x

    :param int keepLast: the number of the software version, what we keep
    """

    versionsDirectory = os.path.split(rootPath)[0]
    if versionsDirectory.endswith('versions'):  # make sure we are not deleting from a wrong directory.
      softwareDirs = os.listdir(versionsDirectory)
      softwareDirs.sort(key=LooseVersion, reverse=False)
      try:
        for directoryName in softwareDirs[:-1 * int(keepLast)]:
          fullPath = os.path.join(versionsDirectory, directoryName)
          gLogger.info("Removing %s directory." % fullPath)
          shutil.rmtree(fullPath)
      except Exception as e:
        gLogger.error("Can not delete old DIRAC versions from the file system", repr(e))
    else:
      gLogger.error("The DIRAC.rootPath is not correct: %s" % versionsDirectory)
