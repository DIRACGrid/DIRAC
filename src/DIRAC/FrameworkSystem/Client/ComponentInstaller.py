"""
Module for managing the installation of DIRAC components:
MySQL, DB's, NoSQL DBs, Services, Agents, Executors

It only makes use of defaults in LocalInstallation Section in dirac.cfg

The Following Options are used::

  /DIRAC/Setup:             Setup to be used for any operation
  /LocalInstallation/InstanceName:    Name of the Instance for the current Setup (default /DIRAC/Setup)
  /LocalInstallation/LogLevel:        LogLevel set in "run" script for all components installed
  /LocalInstallation/RootPath:        Python 2 only! Used instead of rootPath in "run" script
                                      if defined (if links are used to named versions)
  /LocalInstallation/InstancePath:    Python 2 only! Location where runit and
                                      startup directories are created (default rootPath)
  /LocalInstallation/UseVersionsDir:  Python 2 only! DIRAC is installed under
                                      versions/<Versioned Directory> with a link from pro
                                      (This option overwrites RootPath and InstancePath)
  /LocalInstallation/Host:            Used when build the URL to be published for the installed
                                      service (default: socket.getfqdn())
  /LocalInstallation/RunitDir:        Location where runit directory is created (default InstancePath/runit)
  /LocalInstallation/StartupDir:      Location where startup directory is created (default InstancePath/startup)
  /LocalInstallation/Database/User:                 (default Dirac)
  /LocalInstallation/Database/Password:             (must be set for SystemAdministrator Service to work)
  /LocalInstallation/Database/RootUser:             (default root)
  /LocalInstallation/Database/RootPwd:              (must be set for SystemAdministrator Service to work)
  /LocalInstallation/Database/Host:                 (must be set for SystemAdministrator Service to work)
  /LocalInstallation/Database/Port:                 (default 3306)
  /LocalInstallation/NoSQLDatabase/Host:            (must be set for SystemAdministrator Service to work)
  /LocalInstallation/NoSQLDatabase/Port:            (default 9200)
  /LocalInstallation/NoSQLDatabase/User:            (default '')
  /LocalInstallation/NoSQLDatabase/Password:        (not strictly necessary)
  /LocalInstallation/NoSQLDatabase/SSL:             (default True)

The setupSite method (used by the dirac-setup-site command) will use the following info::

  /LocalInstallation/Systems:       List of Systems to be defined for this instance
                                    in the CS (default: Configuration, Framework)
  /LocalInstallation/Databases:     List of MySQL Databases to be installed and configured
  /LocalInstallation/Services:      List of System/ServiceName to be setup
  /LocalInstallation/Agents:        List of System/AgentName to be setup
  /LocalInstallation/WebPortal:     Boolean to setup the Web Portal (default no)
  /LocalInstallation/ConfigurationMaster: Boolean, requires Configuration/Server to be given
                                          in the list of Services (default: no)
  /LocalInstallation/PrivateConfiguration: Boolean, requires Configuration/Server to be given
                                           in the list of Services (default: no)

If a Master Configuration Server is being installed the following Options can be used::

  /LocalInstallation/ConfigurationName: Name of the Configuration (default: Setup )
  /LocalInstallation/AdminUserName:  Name of the Admin user (default: None )
  /LocalInstallation/AdminUserDN:    DN of the Admin user certificate (default: None )
  /LocalInstallation/AdminUserEmail: Email of the Admin user (default: None )
  /LocalInstallation/AdminGroupName: Name of the Admin group (default: dirac_admin )
  /LocalInstallation/HostDN: DN of the host certificate (default: None )
  /LocalInstallation/VirtualOrganization: Name of the main Virtual Organization (default: None)

"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

__RCSID__ = "$Id$"

import getpass
import glob
import importlib
import inspect
import io
import os
import pkgutil
import re
import shutil
import stat
import time
from collections import defaultdict

import importlib_resources
import six
import subprocess32 as subprocess
from diraccfg import CFG
from prompt_toolkit import prompt

import DIRAC
from DIRAC import rootPath
from DIRAC import gConfig
from DIRAC import gLogger
from DIRAC.Core.Utilities.Subprocess import systemCall
from DIRAC.Core.Utilities.ReturnValues import S_OK, S_ERROR

from DIRAC.Core.Utilities.Version import getVersion
from DIRAC.Core.Utilities.File import mkDir, mkLink
from DIRAC.ConfigurationSystem.Client.CSAPI import CSAPI
from DIRAC.ConfigurationSystem.Client.Helpers import cfgPath, cfgPathToList, cfgInstallPath, \
    cfgInstallSection, CSGlobals
from DIRAC.Core.Security.Properties import ALARMS_MANAGEMENT, SERVICE_ADMINISTRATOR, \
    CS_ADMINISTRATOR, JOB_ADMINISTRATOR, \
    FULL_DELEGATION, PROXY_MANAGEMENT, OPERATOR, \
    NORMAL_USER, TRUSTED_HOST

from DIRAC.ConfigurationSystem.Client import PathFinder
from DIRAC.FrameworkSystem.Client.ComponentMonitoringClient import ComponentMonitoringClient
from DIRAC.Core.Base.private.ModuleLoader import ModuleLoader
from DIRAC.Core.Base.AgentModule import AgentModule
from DIRAC.Core.Base.ExecutorModule import ExecutorModule
from DIRAC.Core.DISET.RequestHandler import RequestHandler
from DIRAC.Core.Utilities.Decorators import deprecated
from DIRAC.Core.Utilities.PrettyPrint import printTable
from DIRAC.Core.Utilities.Extensions import (
    extensionsByPriority, findDatabases, findModules, findAgents, findServices,
    findExecutors, findSystems,
)

__RCSID__ = "$Id$"


def _safeFloat(value):
  try:
    return float(value)
  except ValueError:
    return -1


def _safeInt(value):
  try:
    return int(value)
  except ValueError:
    return -1


def _makeComponentDict(component, setupDict, installedDict, compType, system, runitDict):
  componentDict = {
      'Setup': component in setupDict.get(compType, {}).get(system, {}),
      'Installed': component in installedDict.get(compType, {}).get(system, {}),
      'RunitStatus': 'Unknown',
      'Timeup': 0,
      'PID': 0,
  }
  compDir = system + '_' + component
  if compDir in runitDict:
    componentDict['RunitStatus'] = runitDict[compDir]['RunitStatus']
    componentDict['Timeup'] = runitDict[compDir]['Timeup']
    componentDict['PID'] = _safeInt(runitDict[compDir].get('PID', -1))
    componentDict['CPU'] = _safeFloat(runitDict[compDir].get('CPU', -1))
    componentDict['MEM'] = _safeFloat(runitDict[compDir].get('MEM', -1))
    componentDict['RSS'] = _safeFloat(runitDict[compDir].get('RSS', -1))
    componentDict['VSZ'] = _safeFloat(runitDict[compDir].get('VSZ', -1))
  return componentDict


class ComponentInstaller(object):

  def __init__(self):
    self.gDefaultPerms = stat.S_IWUSR | stat.S_IRUSR | stat.S_IXUSR | stat.S_IRGRP | stat.S_IXGRP | \
        stat.S_IROTH | stat.S_IXOTH

    # On command line tools this can be set to True to abort after the first error.
    self.exitOnError = False

    # First some global defaults
    gLogger.debug('DIRAC Root Path =', rootPath)

    self.mysqlMode = ''
    self.localCfg = None
    self.cfgFile = ''
    self.setup = ''
    self.instance = ''
    self.logLevel = ''
    self.linkedRootPath = ''
    self.host = ''
    self.basePath = ''
    self.instancePath = ''
    self.runitDir = ''
    self.startDir = ''
    self.db = {}
    self.mysqlUser = ''
    self.mysqlPassword = ''
    self.mysqlRootUser = ''
    self.mysqlRootPwd = ''
    self.mysqlHost = ''
    self.mysqlPort = ''
    self.noSQLHost = ''
    self.noSQLPort = ''
    self.noSQLUser = ''
    self.noSQLPassword = ''
    self.noSQLSSL = ''
    self.controlDir = ''
    self.componentTypes = ['service', 'agent', 'executor']
    self.monitoringClient = None

    self.loadDiracCfg()

  def resultIndexes(self, componentTypes):
    resultIndexes = {}
    for cType in componentTypes:
      result = self._getSectionName(cType)
      if not result['OK']:
        return result
      resultIndexes[cType] = result['Value']
    return S_OK(resultIndexes)

  def loadDiracCfg(self):
    """ Read again defaults from dirac.cfg
    """
    from DIRAC.Core.Utilities.Network import getFQDN

    self.localCfg = CFG()
    self.cfgFile = os.path.join(rootPath, 'etc', 'dirac.cfg')
    try:
      self.localCfg.loadFromFile(self.cfgFile)
    except Exception:
      gLogger.always("Can't load ", self.cfgFile)
      gLogger.always("Might be OK if setting up the site")

    self.setup = self.localCfg.getOption(cfgPath('DIRAC', 'Setup'), '')
    self.instance = self.localCfg.getOption(cfgInstallPath('InstanceName'), self.setup)
    self.logLevel = self.localCfg.getOption(cfgInstallPath('LogLevel'), 'INFO')
    if six.PY2:
      self.linkedRootPath = self.localCfg.getOption(cfgInstallPath('RootPath'), rootPath)
    else:
      self.linkedRootPath = rootPath

    self.host = self.localCfg.getOption(cfgInstallPath('Host'), getFQDN())

    self.basePath = os.path.dirname(rootPath)
    if six.PY2:
      self.instancePath = self.localCfg.getOption(cfgInstallPath('InstancePath'), rootPath)
      useVersionsDir = self.localCfg.getOption(cfgInstallPath('UseVersionsDir'), False)
      if useVersionsDir:
        # This option takes precedence and has no effect for Python 3 installations
        self.instancePath = os.path.dirname(os.path.dirname(rootPath))
        self.linkedRootPath = os.path.join(self.instancePath, 'pro')
      gLogger.verbose('Using Instance Base Dir at', self.instancePath)
    else:
      self.instancePath = rootPath

    self.runitDir = os.path.join(self.instancePath, 'runit')
    self.runitDir = self.localCfg.getOption(cfgInstallPath('RunitDir'), self.runitDir)
    gLogger.verbose('Using Runit Dir at', self.runitDir)

    self.startDir = os.path.join(self.instancePath, 'startup')
    self.startDir = self.localCfg.getOption(cfgInstallPath('StartupDir'), self.startDir)
    gLogger.verbose('Using Startup Dir at', self.startDir)

    self.controlDir = os.path.join(self.instancePath, 'control')
    self.controlDir = self.localCfg.getOption(cfgInstallPath('ControlDir'), self.controlDir)
    gLogger.verbose('Using Control Dir at', self.controlDir)

    # Now some MySQL default values
    self.mysqlRootPwd = self.localCfg.getOption(cfgInstallPath('Database', 'RootPwd'), self.mysqlRootPwd)
    if self.mysqlRootPwd:
      gLogger.verbose('Reading Root MySQL Password from local configuration')
    else:
      gLogger.warn('MySQL root password not found')

    self.mysqlUser = self.localCfg.getOption(cfgInstallPath('Database', 'User'), self.mysqlUser)
    if self.mysqlUser:
      gLogger.verbose('Reading MySQL User from local configuration')
    else:
      gLogger.warn("Using 'Dirac' as MySQL user name")
      self.mysqlUser = 'Dirac'

    self.mysqlPassword = self.localCfg.getOption(cfgInstallPath('Database', 'Password'), self.mysqlPassword)
    if self.mysqlPassword:
      gLogger.verbose('Reading %s MySQL Password from local configuration ' % self.mysqlUser)
    else:
      gLogger.warn('MySQL password not found')

    self.mysqlHost = self.localCfg.getOption(cfgInstallPath('Database', 'Host'), '')
    if self.mysqlHost:
      gLogger.verbose('Using MySQL Host from local configuration', self.mysqlHost)
    else:
      gLogger.warn('Using the same host for MySQL as dirac services')
      self.mysqlHost = self.host

    self.mysqlPort = self.localCfg.getOption(cfgInstallPath('Database', 'Port'), 0)
    if self.mysqlPort:
      gLogger.verbose('Using MySQL Port from local configuration ', self.mysqlPort)
    else:
      gLogger.warn("Using port '3306' as MySQL port")
      self.mysqlPort = 3306

    self.mysqlRootUser = self.localCfg.getOption(cfgInstallPath('Database', 'RootUser'), '')
    if self.mysqlRootUser:
      gLogger.verbose('Using MySQL root user from local configuration ', self.mysqlRootUser)
    else:
      gLogger.warn("Using 'root' as root MySQL user")
      self.mysqlRootUser = 'root'

    self.mysqlMode = self.localCfg.getOption(cfgInstallPath('Database', 'MySQLMode'), '')
    if self.mysqlMode:
      gLogger.verbose('Configuring MySQL server as %s' % self.mysqlMode)

    # Now some noSQL defaults
    self.noSQLHost = self.localCfg.getOption(cfgInstallPath('NoSQLDatabase', 'Host'), '')
    if self.noSQLHost:
      gLogger.verbose('Using NoSQL Host from local configuration', self.noSQLHost)
    else:
      gLogger.warn('Using the same host for NoSQL as dirac services')
      self.noSQLHost = self.host

    self.noSQLPort = self.localCfg.getOption(cfgInstallPath('NoSQLDatabase', 'Port'), 0)
    if self.noSQLPort:
      gLogger.verbose('Using NoSQL Port from local configuration ', self.noSQLPort)
    else:
      gLogger.warn('Using the default port 9200')
      self.noSQLPort = 9200

    self.noSQLUser = self.localCfg.getOption(cfgInstallPath('NoSQLDatabase', 'User'), self.noSQLUser)
    if self.noSQLUser:
      gLogger.verbose('Reading NoSQL User from local configuration')
    else:
      gLogger.warn('NoSQL user not found')

    self.noSQLPassword = self.localCfg.getOption(cfgInstallPath('NoSQLDatabase', 'Password'), self.noSQLPassword)
    if self.noSQLPassword:
      gLogger.verbose('Reading %s NoSQL Password from local configuration ' % self.noSQLUser)
    else:
      gLogger.warn('NoSQL password not found')

    self.noSQLSSL = self.localCfg.getOption(cfgInstallPath('NoSQLDatabase', 'SSL'), self.noSQLSSL)
    if self.noSQLSSL:
      gLogger.verbose("Reading NoSQL SSL choice from local configuration")
    else:
      gLogger.warn("NoSQL SSL choice not found")

    # Now ready to insert components in the Component Monitoring DB
    self.monitoringClient = ComponentMonitoringClient()
    gLogger.verbose('Client configured for Component Monitoring')

  def getInfo(self):
    result = getVersion()
    if not result['OK']:
      return result
    rDict = result['Value']
    rDict['Setup'] = self.setup or 'Unknown'
    return S_OK(rDict)

  @deprecated("Use DIRAC.Core.Utilities.Extensions.extensionsByPriority instead")
  def getExtensions(self):
    """Get the list of installed extensions"""
    extensions = extensionsByPriority()
    try:
      extensions.remove('DIRAC')
    except Exception:
      error = 'DIRAC is not properly installed'
      gLogger.exception(error)
      if self.exitOnError:
        DIRAC.exit(-1)
      return S_ERROR(error)
    return S_OK(extensions)

  def _addCfgToDiracCfg(self, cfg):
    """
    Merge cfg into existing dirac.cfg file
    """
    newCfg = self.localCfg.mergeWith(cfg) if str(self.localCfg) else cfg
    result = newCfg.writeToFile(self.cfgFile)
    if not result:
      return result
    self.loadDiracCfg()
    return result

  def _addCfgToCS(self, cfg):
    """
    Merge cfg into central CS
    """
    gLogger.debug("Adding CFG to CS:")
    gLogger.debug(cfg)

    cfgClient = CSAPI()
    result = cfgClient.downloadCSData()
    if not result['OK']:
      return result
    result = cfgClient.mergeFromCFG(cfg)
    if not result['OK']:
      return result
    result = cfgClient.commit()
    return result

  def _addCfgToLocalCS(self, cfg):
    """
    Merge cfg into local CS
    """
    csName = self.localCfg.getOption(cfgPath('DIRAC', 'Configuration', 'Name'), '')
    if not csName:
      error = 'Missing %s' % cfgPath('DIRAC', 'Configuration', 'Name')
      if self.exitOnError:
        gLogger.error(error)
        DIRAC.exit(-1)
      return S_ERROR(error)

    csCfg = CFG()
    csFile = os.path.join(rootPath, 'etc', '%s.cfg' % csName)
    if os.path.exists(csFile):
      csCfg.loadFromFile(csFile)
    newCfg = csCfg.mergeWith(cfg) if str(csCfg) else cfg
    return newCfg.writeToFile(csFile)

  def _removeOptionFromCS(self, path):
    """
    Delete options from central CS
    """
    cfgClient = CSAPI()
    result = cfgClient.downloadCSData()
    if not result['OK']:
      return result
    result = cfgClient.delOption(path)
    if not result['OK']:
      return result
    result = cfgClient.commit()
    return result

  def _removeSectionFromCS(self, path):
    """
    Delete setions from central CS
    """
    cfgClient = CSAPI()
    result = cfgClient.downloadCSData()
    if not result['OK']:
      return result
    result = cfgClient.delSection(path)
    if not result['OK']:
      return result
    result = cfgClient.commit()
    return result

  def _getCentralCfg(self, installCfg):
    """
    Create the skeleton of central Cfg for an initial Master CS
    """
    # First copy over from installation cfg
    centralCfg = CFG()

    # DIRAC/Extensions
    extensions = self.localCfg.getOption(cfgInstallPath('Extensions'), [])
    centralCfg.createNewSection('DIRAC', '')
    if extensions:
      centralCfg['DIRAC'].addKey('Extensions', ','.join(extensions), '')  # pylint: disable=no-member

    vo = self.localCfg.getOption(cfgInstallPath('VirtualOrganization'), '')
    if vo:
      centralCfg['DIRAC'].addKey('VirtualOrganization', vo, '')  # pylint: disable=no-member

    for section in ['Systems', 'Resources', 'Resources/Sites', 'Operations', 'Registry']:
      if installCfg.isSection(section):
        centralCfg.createNewSection(section, contents=installCfg[section])

    # Now try to add things from the Installation section
    # Registry
    adminUserName = self.localCfg.getOption(cfgInstallPath('AdminUserName'), '')
    adminUserDN = self.localCfg.getOption(cfgInstallPath('AdminUserDN'), '')
    adminUserEmail = self.localCfg.getOption(cfgInstallPath('AdminUserEmail'), '')
    adminGroupName = self.localCfg.getOption(cfgInstallPath('AdminGroupName'), 'dirac_admin')
    hostDN = self.localCfg.getOption(cfgInstallPath('HostDN'), '')
    defaultGroupName = self.localCfg.getOption(cfgInstallPath('DefaultGroupName'), 'dirac_user')
    adminGroupProperties = [ALARMS_MANAGEMENT, SERVICE_ADMINISTRATOR,
                            CS_ADMINISTRATOR, JOB_ADMINISTRATOR,
                            FULL_DELEGATION, PROXY_MANAGEMENT, OPERATOR]
    defaultGroupProperties = [NORMAL_USER]
    defaultHostProperties = [TRUSTED_HOST, CS_ADMINISTRATOR,
                             JOB_ADMINISTRATOR, FULL_DELEGATION,
                             PROXY_MANAGEMENT, OPERATOR]

    for section in (cfgPath('Registry'),
                    cfgPath('Registry', 'Users'),
                    cfgPath('Registry', 'Groups'),
                    cfgPath('Registry', 'Hosts')):
      if not centralCfg.isSection(section):
        centralCfg.createNewSection(section)

    if adminUserName:
      if not (adminUserDN and adminUserEmail):
        gLogger.error('AdminUserName is given but DN or Mail is missing it will not be configured')
      else:
        for section in [cfgPath('Registry', 'Users', adminUserName),
                        cfgPath('Registry', 'Groups', defaultGroupName),
                        cfgPath('Registry', 'Groups', adminGroupName)]:
          if not centralCfg.isSection(section):
            centralCfg.createNewSection(section)

        if centralCfg['Registry'].existsKey('DefaultGroup'):  # pylint: disable=unsubscriptable-object,no-member
          centralCfg['Registry'].deleteKey('DefaultGroup')  # pylint: disable=unsubscriptable-object,no-member
        centralCfg['Registry'].addKey(  # pylint: disable=unsubscriptable-object,no-member
            'DefaultGroup',
            defaultGroupName,
            '')

        if centralCfg['Registry']['Users'][adminUserName].existsKey('DN'):  # pylint: disable=unsubscriptable-object
          centralCfg['Registry']['Users'][adminUserName].deleteKey('DN')  # pylint: disable=unsubscriptable-object
        centralCfg['Registry']['Users'][adminUserName].addKey(  # pylint: disable=unsubscriptable-object
            'DN', adminUserDN, '')

        if centralCfg['Registry']['Users'][adminUserName].existsKey('Email'):  # pylint: disable=unsubscriptable-object
          centralCfg['Registry']['Users'][adminUserName].deleteKey('Email')  # pylint: disable=unsubscriptable-object
        centralCfg['Registry']['Users'][adminUserName].addKey(  # pylint: disable=unsubscriptable-object
            'Email', adminUserEmail, '')

        # Add Admin User to Admin Group and default group
        for group in [adminGroupName, defaultGroupName]:
          if not centralCfg['Registry']['Groups'][group].isOption('Users'):  # pylint: disable=unsubscriptable-object
            centralCfg['Registry']['Groups'][group].addKey('Users', '', '')  # pylint: disable=unsubscriptable-object
          users = centralCfg['Registry']['Groups'][group].getOption(  # pylint: disable=unsubscriptable-object
              'Users', [])
          if adminUserName not in users:
            centralCfg['Registry']['Groups'][group].appendToOption(  # pylint: disable=unsubscriptable-object
                'Users', ', %s' % adminUserName)
          if not centralCfg['Registry']['Groups'][group].isOption(  # pylint: disable=unsubscriptable-object
                  'Properties'):
            centralCfg['Registry']['Groups'][group].addKey(  # pylint: disable=unsubscriptable-object
                'Properties', '', '')

        properties = centralCfg['Registry']['Groups'][adminGroupName].getOption(  # noqa # pylint: disable=unsubscriptable-object
            'Properties', [])
        for prop in adminGroupProperties:
          if prop not in properties:
            properties.append(prop)
            centralCfg['Registry']['Groups'][adminGroupName].appendToOption(  # pylint: disable=unsubscriptable-object
                'Properties', ', %s' % prop)

        properties = centralCfg['Registry']['Groups'][defaultGroupName].getOption(  # noqa # pylint: disable=unsubscriptable-object
            'Properties', [])
        for prop in defaultGroupProperties:
          if prop not in properties:
            properties.append(prop)
            centralCfg['Registry']['Groups'][defaultGroupName].appendToOption(  # pylint: disable=unsubscriptable-object
                'Properties', ', %s' % prop)

    # Add the master Host description
    if hostDN:
      hostSection = cfgPath('Registry', 'Hosts', self.host)
      if not centralCfg.isSection(hostSection):
        centralCfg.createNewSection(hostSection)
      if centralCfg['Registry']['Hosts'][self.host].existsKey('DN'):  # pylint: disable=unsubscriptable-object
        centralCfg['Registry']['Hosts'][self.host].deleteKey('DN')  # pylint: disable=unsubscriptable-object
      centralCfg['Registry']['Hosts'][self.host].addKey('DN', hostDN, '')  # pylint: disable=unsubscriptable-object
      if not centralCfg['Registry']['Hosts'][self.host].isOption(  # pylint: disable=unsubscriptable-object
              'Properties'):
        centralCfg['Registry']['Hosts'][self.host].addKey(  # pylint: disable=unsubscriptable-object
            'Properties', '', '')
      properties = centralCfg['Registry']['Hosts'][self.host].getOption(  # pylint: disable=unsubscriptable-object
          'Properties', [])
      for prop in defaultHostProperties:
        if prop not in properties:
          properties.append(prop)
          centralCfg['Registry']['Hosts'][self.host].appendToOption(  # pylint: disable=unsubscriptable-object
              'Properties', ', %s' % prop)

    # Operations
    if adminUserEmail:
      operationsCfg = self.__getCfg(cfgPath('Operations', 'Defaults', 'EMail'), 'Production', adminUserEmail)
      centralCfg = centralCfg.mergeWith(operationsCfg)
      operationsCfg = self.__getCfg(cfgPath('Operations', 'Defaults', 'EMail'), 'Logging', adminUserEmail)
      centralCfg = centralCfg.mergeWith(operationsCfg)

    # Website
    websiteCfg = self.__getCfg(cfgPath('WebApp', 'Access'), 'upload', 'TrustedHost')
    centralCfg = centralCfg.mergeWith(websiteCfg)

    return centralCfg

  def __getCfg(self, section, option='', value=''):
    """
    Create a new Cfg with given info
    """
    if not section:
      return None
    cfg = CFG()
    sectionList = []
    for sect in cfgPathToList(section):
      if not sect:
        continue
      sectionList.append(sect)
      cfg.createNewSection(cfgPath(*sectionList))
    if not sectionList:
      return None

    if option and value:
      sectionList.append(option)
      cfg.setOption(cfgPath(*sectionList), value)

    return cfg

  def addOptionToDiracCfg(self, option, value):
    """
    Add Option to dirac.cfg
    """
    optionList = cfgPathToList(option)
    optionName = optionList[-1]
    section = cfgPath(*optionList[:-1])
    cfg = self.__getCfg(section, optionName, value)

    if not cfg:
      return S_ERROR('Wrong option: %s = %s' % (option, value))

    if self._addCfgToDiracCfg(cfg):
      return S_OK()

    return S_ERROR('Could not merge %s=%s with local configuration' % (option, value))

  def removeComponentOptionsFromCS(self, system, component, mySetup=None):
    """
    Remove the section with Component options from the CS, if possible
    """
    if mySetup is None:
      mySetup = self.setup

    result = self.monitoringClient.getInstallations(
        {'UnInstallationTime': None, 'Instance': component},
        {'DIRACSystem': system},
        {}, True)
    if not result['OK']:
      return result
    installations = result['Value']

    instanceOption = cfgPath('DIRAC', 'Setups', mySetup, system)
    if gConfig:
      compInstance = gConfig.getValue(instanceOption, '')
    else:
      compInstance = self.localCfg.getOption(instanceOption, '')

    if len(installations) == 1:
      remove = True
      removeMain = False
      installation = installations[0]
      cType = installation['Component']['Type']

      # Is the component a rename of another module?
      isRenamed = installation['Instance'] != installation['Component']['DIRACModule']

      result = self.monitoringClient.getInstallations(
          {'UnInstallationTime': None},
          {'DIRACSystem': system, 'DIRACModule': installation['Component']['DIRACModule']},
          {},
          True)
      if not result['OK']:
        return result
      installations = result['Value']

      # If the component is not renamed we keep it in the CS if there are any renamed ones
      if not isRenamed:
        if len(installations) > 1:
          remove = False
      # If the component is renamed and is the last one, we remove the entry for the main module as well
      else:
        if len(installations) == 1:
          removeMain = True

      if remove:
        result = self._removeSectionFromCS(cfgPath('Systems', system,
                                                   compInstance,
                                                   installation['Component']['Type'].title() + 's', component))
        if not result['OK']:
          return result

        if not isRenamed and cType == 'service':
          result = self._removeOptionFromCS(cfgPath('Systems', system, compInstance, 'URLs', component))
          if not result['OK']:
            # It is maybe in the FailoverURLs ?
            result = self._removeOptionFromCS(cfgPath('Systems', system, compInstance, 'FailoverURLs', component))
          if not result['OK']:
            return result

      if removeMain:
        result = self._removeSectionFromCS(cfgPath('Systems', system,
                                                   compInstance,
                                                   installation['Component']['Type'].title() + 's',
                                                   installation['Component']['Module']))
        if not result['OK']:
          return result

        if cType == 'service':
          result = self._removeOptionFromCS(
              cfgPath(
                  'Systems',
                  system,
                  compInstance,
                  'URLs',
                  installation['Component']['Module']))
          if not result['OK']:
            # it is maybe in the FailoverURLs ?
            result = self._removeOptionFromCS(
                cfgPath('Systems', system, compInstance, 'FailoverURLs', installation['Component']['Module'])
            )
          if not result['OK']:
            return result

      return S_OK('Successfully removed entries from CS')
    return S_OK('Instances of this component still exist. It won\'t be completely removed')

  def addDefaultOptionsToCS(self, gConfig_o, componentType, systemName,
                            component, extensions, mySetup=None,
                            specialOptions={}, overwrite=False,
                            addDefaultOptions=True):
    """
    Add the section with the component options to the CS
    """
    if mySetup is None:
      mySetup = self.setup

    if gConfig_o:
      gConfig_o.forceRefresh()

    system = systemName.replace('System', '')
    instanceOption = cfgPath('DIRAC', 'Setups', mySetup, system)
    if gConfig_o:
      compInstance = gConfig_o.getValue(instanceOption, '')
    else:
      compInstance = self.localCfg.getOption(instanceOption, '')
    if not compInstance:
      return S_ERROR('%s not defined in %s' % (instanceOption, self.cfgFile))

    result = self._getSectionName(componentType)
    if not result['OK']:
      return result
    sectionName = result['Value']

    # Check if the component CS options exist
    addOptions = True
    componentSection = cfgPath('Systems', system, compInstance, sectionName, component)
    if not overwrite:
      if gConfig_o:
        result = gConfig_o.getOptions(componentSection)
        if result['OK']:
          addOptions = False

    if not addOptions:
      return S_OK('Component options already exist')

    # Add the component options now
    result = self.getComponentCfg(
        componentType,
        system,
        component,
        compInstance,
        extensions,
        specialOptions,
        addDefaultOptions)
    if not result['OK']:
      return result
    compCfg = result['Value']

    gLogger.notice('Adding to CS', '%s %s/%s' % (componentType, system, component))
    resultAddToCFG = self._addCfgToCS(compCfg)
    if componentType == 'executor':
      # Is it a container ?
      execList = compCfg.getOption('%s/Load' % componentSection, [])
      for element in execList:
        result = self.addDefaultOptionsToCS(gConfig_o, componentType, systemName, element, extensions, self.setup,
                                            {}, overwrite)
        if not result['OK']:
          gLogger.warn("Can't add to default CS", result['Message'])
        resultAddToCFG.setdefault('Modules', {})
        resultAddToCFG['Modules'][element] = result['OK']
    return resultAddToCFG

  def addDefaultOptionsToComponentCfg(self, componentType, systemName, component, extensions):
    """
    Add default component options local component cfg
    """
    system = systemName.replace('System', '')
    instanceOption = cfgPath('DIRAC', 'Setups', self.setup, system)
    compInstance = self.localCfg.getOption(instanceOption, '')
    if not compInstance:
      return S_ERROR('%s not defined in %s' % (instanceOption, self.cfgFile))

    # Add the component options now
    result = self.getComponentCfg(componentType, system, component, compInstance, extensions)
    if not result['OK']:
      return result
    compCfg = result['Value']

    compCfgFile = os.path.join(rootPath, 'etc', '%s_%s.cfg' % (system, component))
    if compCfg.writeToFile(compCfgFile):  # this returns a True/False
      return S_OK()
    return S_ERROR()

  def addCfgToComponentCfg(self, componentType, systemName, component, cfg):
    """
    Add some extra configuration to the local component cfg
    """
    result = self._getSectionName(componentType)
    if not result['OK']:
      return result
    sectionName = result['Value']

    if not cfg:
      return S_OK()
    system = systemName.replace('System', '')
    instanceOption = cfgPath('DIRAC', 'Setups', self.setup, system)
    compInstance = self.localCfg.getOption(instanceOption, '')

    if not compInstance:
      return S_ERROR('%s not defined in %s' % (instanceOption, self.cfgFile))
    compCfgFile = os.path.join(rootPath, 'etc', '%s_%s.cfg' % (system, component))
    compCfg = CFG()
    if os.path.exists(compCfgFile):
      compCfg.loadFromFile(compCfgFile)
    sectionPath = cfgPath('Systems', system, compInstance, sectionName)

    newCfg = self.__getCfg(sectionPath)
    newCfg.createNewSection(cfgPath(sectionPath, component), 'Added by ComponentInstaller', cfg)
    if newCfg.writeToFile(compCfgFile):
      return S_OK(compCfgFile)
    error = 'Can not write %s' % compCfgFile
    gLogger.error(error)
    return S_ERROR(error)

  def getComponentCfg(self, componentType, system, component, compInstance, extensions,
                      specialOptions={}, addDefaultOptions=True):
    """
    Get the CFG object of the component configuration
    """
    result = self._getSectionName(componentType)
    if not result['OK']:
      return result
    sectionName = result['Value']

    componentModule = specialOptions.get('Module', component)
    compCfg = CFG()
    if addDefaultOptions:
      for ext in extensions:
        cfgTemplateModule = "%s.%sSystem" % (ext, system)
        try:
          cfgTemplate = importlib_resources.read_text(cfgTemplateModule, "ConfigTemplate.cfg")
        except (ImportError, OSError):
          continue
        gLogger.notice('Loading configuration template from', cfgTemplateModule)
        loadCfg = CFG()
        loadCfg.loadFromBuffer(cfgTemplate)
        compCfg = loadCfg.mergeWith(compCfg)

      compPath = cfgPath(sectionName, componentModule)
      if not compCfg.isSection(compPath):
        error = 'Can not find %s in template' % compPath
        gLogger.error(error)
        if self.exitOnError:
          DIRAC.exit(-1)
        return S_ERROR(error)

      compCfg = compCfg[sectionName][componentModule]  # pylint: disable=unsubscriptable-object

      # Delete Dependencies section if any
      compCfg.deleteKey('Dependencies')

    sectionPath = cfgPath('Systems', system, compInstance, sectionName)
    cfg = self.__getCfg(sectionPath)
    cfg.createNewSection(cfgPath(sectionPath, component), '', compCfg)

    for option, value in specialOptions.items():
      cfg.setOption(cfgPath(sectionPath, component, option), value)

    # Add the service URL
    if componentType == "service":
      port = compCfg.getOption('Port', 0)
      if port and self.host:
        urlsPath = cfgPath('Systems', system, compInstance, 'URLs')
        cfg.createNewSection(urlsPath)
        failoverUrlsPath = cfgPath('Systems', system, compInstance, 'FailoverURLs')
        cfg.createNewSection(failoverUrlsPath)
        cfg.setOption(cfgPath(urlsPath, component),
                      'dips://%s:%d/%s/%s' % (self.host, port, system, component))

    return S_OK(cfg)

  def addDatabaseOptionsToCS(self, gConfig_o, systemName, dbName, mySetup=None, overwrite=False):
    """
    Add the section with the database options to the CS
    """
    if mySetup is None:
      mySetup = self.setup

    if gConfig_o:
      gConfig_o.forceRefresh()

    system = systemName.replace('System', '')
    instanceOption = cfgPath('DIRAC', 'Setups', mySetup, system)
    if gConfig_o:
      compInstance = gConfig_o.getValue(instanceOption, '')
    else:
      compInstance = self.localCfg.getOption(instanceOption, '')
    if not compInstance:
      return S_ERROR('%s not defined in %s' % (instanceOption, self.cfgFile))

    # Check if the component CS options exist
    addOptions = True
    if not overwrite:
      databasePath = cfgPath('Systems', system, compInstance, 'Databases', dbName)
      result = gConfig_o.getOptions(databasePath)
      if result['OK']:
        addOptions = False
    if not addOptions:
      return S_OK('Database options already exist')

    # Add the component options now
    result = self.getDatabaseCfg(system, dbName, compInstance)
    if not result['OK']:
      return result
    databaseCfg = result['Value']
    gLogger.notice('Adding to CS', '%s/%s' % (system, dbName))
    return self._addCfgToCS(databaseCfg)

  def removeDatabaseOptionsFromCS(self, gConfig_o, system, dbName, mySetup=None):
    """
    Remove the section with database options from the CS, if possible
    """
    if mySetup is None:
      mySetup = self.setup

    result = self.monitoringClient.installationExists(
        {'UnInstallationTime': None},
        {'DIRACSystem': system, 'Type': 'DB', 'DIRACModule': dbName},
        {})
    if not result['OK']:
      return result
    exists = result['Value']

    instanceOption = cfgPath('DIRAC', 'Setups', mySetup, system)
    if gConfig_o:
      compInstance = gConfig_o.getValue(instanceOption, '')
    else:
      compInstance = self.localCfg.getOption(instanceOption, '')

    if not exists:
      result = self._removeSectionFromCS(cfgPath('Systems', system, compInstance, 'Databases', dbName))
      if not result['OK']:
        return result

    return S_OK('Successfully removed entries from CS')

  def getDatabaseCfg(self, system, dbName, compInstance):
    """
    Get the CFG object of the database configuration
    """
    databasePath = cfgPath('Systems', system, compInstance, 'Databases', dbName)
    cfg = self.__getCfg(databasePath, 'DBName', dbName)
    cfg.setOption(cfgPath(databasePath, 'Host'), self.mysqlHost)
    cfg.setOption(cfgPath(databasePath, 'Port'), self.mysqlPort)

    return S_OK(cfg)

  def addSystemInstance(self, systemName, compInstance, mySetup=None, myCfg=False):
    """
    Add a new system self.instance to dirac.cfg and CS
    """
    if mySetup is None:
      mySetup = self.setup

    system = systemName.replace('System', '')
    gLogger.notice(
        'Adding %s system as %s self.instance for %s self.setup to dirac.cfg and CS' %
        (system, compInstance, mySetup))

    cfg = self.__getCfg(cfgPath('DIRAC', 'Setups', mySetup), system, compInstance)
    if myCfg:
      if not self._addCfgToDiracCfg(cfg):
        return S_ERROR('Failed to add system self.instance to dirac.cfg')

    return self._addCfgToCS(cfg)

  def printStartupStatus(self, rDict):
    """
    Print in nice format the return dictionary from self.getStartupComponentStatus
    (also returned by self.runsvctrlComponent)
    """
    fields = ['Name', 'Runit', 'Uptime', 'PID']
    records = []
    try:
      for comp in rDict:
        records.append([comp,
                        rDict[comp]['RunitStatus'],
                        rDict[comp]['Timeup'],
                        str(rDict[comp]['PID'])])
      printTable(fields, records)
    except Exception as x:
      print("Exception while gathering data for printing: %s" % str(x))
    return S_OK()

  def printOverallStatus(self, rDict):
    """
    Print in nice format the return dictionary from self.getOverallStatus
    """
    fields = ['System', 'Name', 'Type', 'Setup', 'Installed', 'Runit', 'Uptime', 'PID']
    records = []
    try:
      for compType in rDict:
        for system in rDict[compType]:
          for component in rDict[compType][system]:
            record = [system, component, compType.lower()[:-1]]
            if rDict[compType][system][component]['Setup']:
              record.append('SetUp')
            else:
              record.append('NotSetUp')
            if rDict[compType][system][component]['Installed']:
              record.append('Installed')
            else:
              record.append('NotInstalled')
            record.append(str(rDict[compType][system][component]['RunitStatus']))
            record.append(str(rDict[compType][system][component]['Timeup']))
            record.append(str(rDict[compType][system][component]['PID']))
            records.append(record)
      printTable(fields, records)
    except Exception as x:
      print("Exception while gathering data for printing: %s" % str(x))

    return S_OK()

  @deprecated("Use DIRAC.Core.Utilities.Extensions.findSystems instead")
  def getAvailableSystems(self, extensions):
    """Get the list of all systems (in all given extensions) locally available"""
    return list(findSystems(extensions))

  def getSoftwareComponents(self, extensions):
    """
    Get the list of all the components (services, agents, executors) for which the software
    is installed on the system
    """
    # The Gateway does not need a handler
    services = defaultdict(list, {"Framework": ["Gateway"]})
    agents = defaultdict(list)
    executors = defaultdict(list)
    remainders = defaultdict(lambda: defaultdict(list))

    # Components other than services, agents and executors
    remainingTypes = set(self.componentTypes) - {'service', 'agent', 'executor'}
    result = self.resultIndexes(remainingTypes)
    if not result["OK"]:
      return result
    resultIndexes = result["Value"]

    for extension in extensions:
      for system, agent in findAgents(extension):
        loader = pkgutil.get_loader(".".join([extension, system, "Agent", agent]))
        with io.open(loader.get_filename(), "rt") as fp:
          body = fp.read()
        if "AgentModule" in body or "OptimizerModule" in body:
          agents[system.replace("System", "")].append(agent)

      for system, service in findServices(extension):
        if system == "Configuration" and service == "ConfigurationHandler":
          service = "ServerHandler"
        services[system.replace("System", "")].append(service.replace("Handler", ""))

      for system, executor in findExecutors(extension):
        loader = pkgutil.get_loader(".".join([extension, system, "Executor", executor]))
        with io.open(loader.get_filename(), "rt") as fp:
          body = fp.read()
        if "OptimizerExecutor" in body:
          executors[system.replace("System", "")].append(executor)

      # Rest of component types
      for cType in remainingTypes:
        for system, remainder in findModules(extension, cType.title()):
          remainders[cType][system.replace("System", "")].append(remainder)

    resultDict = {resultIndexes[cType]: dict(remainders[cType]) for cType in remainingTypes}
    resultDict["Services"] = dict(services)
    resultDict["Agents"] = dict(agents)
    resultDict["Executors"] = dict(executors)
    return S_OK(resultDict)

  def getInstalledComponents(self):
    """
    Get the list of all the components (services, agents, executors)
    installed on the system in the runit directory
    """
    result = self.resultIndexes(self.componentTypes)
    if not result["OK"]:
      return result
    resultIndexes = result["Value"]

    resultDict = defaultdict(lambda: defaultdict(list))
    for system in os.listdir(self.runitDir):
      systemDir = os.path.join(self.runitDir, system)
      for component in os.listdir(systemDir):
        runFile = os.path.join(systemDir, component, 'run')
        try:
          with io.open(runFile, 'rt') as rFile:
            body = rFile.read()
        except IOError:
          pass
        else:
          for cType in self.componentTypes:
            if 'dirac-%s' % (cType) in body:
              resultDict[cType][system].append(component)

    return S_OK({
        resultIndexes[cType]: dict(resultDict[cType])
        for cType in self.componentTypes
    })

  def getSetupComponents(self):
    """
    Get the list of all the components (services, agents, executors)
    set up for running with runsvdir in startup directory
    """
    if not os.path.isdir(self.startDir):
      return S_ERROR('Startup Directory does not exit: %s' % self.startDir)

    result = self.resultIndexes(self.componentTypes)
    if not result["OK"]:
      return result
    resultIndexes = result["Value"]

    resultDict = defaultdict(lambda: defaultdict(list))
    for component in os.listdir(self.startDir):
      runFile = os.path.join(self.startDir, component, 'run')
      try:
        with io.open(runFile, 'rt') as rfile:
          body = rfile.read()
      except IOError:
        pass
      else:
        for cType in self.componentTypes:
          if 'dirac-%s' % (cType) in body:
            system, compT = component.split('_', 1)
            resultDict[cType][system].append(compT)

    return S_OK({
        resultIndexes[cType]: dict(resultDict[cType])
        for cType in self.componentTypes
    })

  def getStartupComponentStatus(self, componentTupleList):
    """
    Get the list of all the components (services, agents, executors)
    set up for running with runsvdir in startup directory
    """
    try:
      if componentTupleList:
        cList = []
        for componentTuple in componentTupleList:
          cList.extend(glob.glob(os.path.join(self.startDir, '_'.join(componentTuple))))
      else:
        cList = glob.glob(os.path.join(self.startDir, '*'))
    except Exception:
      error = 'Failed to parse List of Components'
      gLogger.exception(error)
      if self.exitOnError:
        DIRAC.exit(-1)
      return S_ERROR(error)

    result = self.execCommand(0, ['runsvstat'] + cList)
    if not result['OK']:
      return result
    output = result['Value'][1].strip().split('\n')

    componentDict = {}
    for line in output:
      if not line:
        continue
      cname, routput = line.split(':')
      cname = cname.replace('%s/' % self.startDir, '')
      run = False
      reResult = re.search('^ run', routput)
      if reResult:
        run = True
      down = False
      reResult = re.search('^ down', routput)
      if reResult:
        down = True
      reResult = re.search('([0-9]+) seconds', routput)
      timeup = 0
      if reResult:
        timeup = reResult.group(1)
      reResult = re.search('pid ([0-9]+)', routput)
      pid = 0
      if reResult:
        pid = reResult.group(1)
      runsv = "Not running"
      if run or down:
        runsv = "Running"
      reResult = re.search('runsv not running', routput)
      if reResult:
        runsv = "Not running"

      runDict = {}
      runDict['CPU'] = -1
      runDict['MEM'] = -1
      runDict['VSZ'] = -1
      runDict['RSS'] = -1
      if pid:  # check the process CPU usage and memory
        # PID %CPU %MEM VSZ
        result = self.execCommand(0, ['ps', '-p', pid, 'u'])
        if result['OK'] and result['Value']:
          stats = result['Value'][1]
          values = re.findall(r"\d*\.\d+|\d+", stats)
          if values:
            runDict['CPU'] = values[1]
            runDict['MEM'] = values[2]
            runDict['VSZ'] = values[3]
            runDict['RSS'] = values[4]

      runDict['Timeup'] = timeup
      runDict['PID'] = pid
      runDict['RunitStatus'] = "Unknown"
      if run:
        runDict['RunitStatus'] = "Run"
      if down:
        runDict['RunitStatus'] = "Down"
      if runsv == "Not running":
        runDict['RunitStatus'] = "NoRunitControl"
      componentDict[cname] = runDict

    return S_OK(componentDict)

  def getComponentModule(self, system, component, compType):
    """
    Get the component software module
    """
    self.setup = CSGlobals.getSetup()
    self.instance = gConfig.getValue(cfgPath('DIRAC', 'Setups', self.setup, system), '')
    if not self.instance:
      return S_OK(component)
    module = gConfig.getValue(cfgPath('Systems', system, self.instance, compType, component, 'Module'), '')
    if not module:
      module = component
    return S_OK(module)

  def getOverallStatus(self, extensions):
    """
    Get the list of all the components (services and agents)
    set up for running with runsvdir in startup directory
    """
    result = self.getSoftwareComponents(extensions)
    if not result['OK']:
      return result
    softDict = result['Value']

    result = self.getSetupComponents()
    if not result['OK']:
      return result
    setupDict = result['Value']

    result = self.getInstalledComponents()
    if not result['OK']:
      return result
    installedDict = result['Value']

    result = self.getStartupComponentStatus([])
    if not result['OK']:
      return result
    runitDict = result['Value']

    # Collect the info now
    result = self.resultIndexes(self.componentTypes)
    if not result["OK"]:
      return result
    resultIndexes = result["Value"]

    resultDict = defaultdict(lambda: defaultdict(dict))
    for cType in resultIndexes.values():
      if 'Services' in softDict:
        for system in softDict[cType]:
          for component in softDict[cType][system]:
            if system == component == 'Configuration':
              # Fix to avoid missing CS due to different between Service name and Handler name
              component = 'Server'
            resultDict[cType][system][component] = _makeComponentDict(
                component, setupDict, installedDict, cType, system, runitDict
            )
      # Installed components can be not the same as in the software list
      if 'Services' in installedDict:
        for system in installedDict[cType]:
          for component in installedDict[cType][system]:
            if component in resultDict.get(cType, {}).get(system, {}):
              continue
            resultDict[cType][system][component] = _makeComponentDict(
                component, setupDict, installedDict, cType, system, runitDict
            )

    return S_OK({k: dict(v) for k, v in resultDict.items()})

  def checkComponentModule(self, componentType, system, module):
    """
    Check existence of the given module
    and if it inherits from the proper class
    """
    if componentType == 'agent':
      loader = ModuleLoader("Agent", PathFinder.getAgentSection, AgentModule)
    elif componentType == 'service':
      loader = ModuleLoader("Service", PathFinder.getServiceSection,
                            RequestHandler, moduleSuffix="Handler")
    elif componentType == 'executor':
      loader = ModuleLoader("Executor", PathFinder.getExecutorSection, ExecutorModule)
    else:
      return S_ERROR('Unknown component type %s' % componentType)

    return loader.loadModule("%s/%s" % (system, module))

  def checkComponentSoftware(self, componentType, system, component, extensions):
    """
    Check the component software
    """
    result = self.getSoftwareComponents(extensions)
    if not result['OK']:
      return result
    softComp = result['Value']

    result = self._getSectionName(componentType)
    if not result['OK']:
      return result

    try:
      softDict = softComp[result['Value']]
    except KeyError:
      return S_ERROR('Unknown component type %s' % componentType)

    if system in softDict and component in softDict[system]:
      return S_OK()

    return S_ERROR('Unknown Component %s/%s' % (system, component))

  def runsvctrlComponent(self, system, component, mode):
    """
    Execute runsvctrl and check status of the specified component
    """
    if mode not in ['u', 'd', 'o', 'p', 'c', 'h', 'a', 'i', 'q', '1', '2', 't', 'k', 'x', 'e']:
      return S_ERROR('Unknown runsvctrl mode "%s"' % mode)

    startCompDirs = glob.glob(os.path.join(self.startDir, '%s_%s' % (system, component)))
    # Make sure that the Configuration server restarts first and the SystemAdmin restarts last
    tmpList = list(startCompDirs)
    for comp in tmpList:
      if "Framework_SystemAdministrator" in comp:
        startCompDirs.append(startCompDirs.pop(startCompDirs.index(comp)))
      if "Configuration_Server" in comp:
        startCompDirs.insert(0, startCompDirs.pop(startCompDirs.index(comp)))
    startCompList = [[k] for k in startCompDirs]
    for startComp in startCompList:
      result = self.execCommand(0, ['runsvctrl', mode] + startComp)
      if not result['OK']:
        return result
      time.sleep(2)

    # Check the runsv status
    if system == '*' or component == '*':
      time.sleep(10)

    # Final check
    result = self.getStartupComponentStatus([(system, component)])
    if not result['OK']:
      gLogger.error('Failed to start the component %s %s' % (system, component))
      return S_ERROR('Failed to start the component')

    return result

  def getLogTail(self, system, component, length=100):
    """
    Get the tail of the component log file
    """
    retDict = {}
    for startCompDir in glob.glob(os.path.join(self.startDir, '%s_%s' % (system, component))):
      compName = os.path.basename(startCompDir)
      logFileName = os.path.join(startCompDir, 'log', 'current')
      if not os.path.exists(logFileName):
        retDict[compName] = 'No log file found'
      else:
        with io.open(logFileName, 'rt') as logFile:
          lines = [line.strip() for line in logFile.readlines()]
        retDict[compName] = '\n'.join(lines[-length:])

    return S_OK(retDict)

  def setupSite(self, scriptCfg, cfg=None):
    """
    Setup a new site using the options defined
    """
    # First we need to find out what needs to be installed
    # by default use dirac.cfg, but if a cfg is given use it and
    # merge it into the dirac.cfg
    diracCfg = CFG()
    installCfg = None
    if cfg:
      try:
        installCfg = CFG()
        installCfg.loadFromFile(cfg)

        for section in ['DIRAC', 'LocalSite', cfgInstallSection]:
          if installCfg.isSection(section):
            diracCfg.createNewSection(section, contents=installCfg[section])

        if self.instancePath != self.basePath:
          if not diracCfg.isSection('LocalSite'):
            diracCfg.createNewSection('LocalSite')
          diracCfg.setOption(cfgPath('LocalSite', 'InstancePath'), self.instancePath)

        self._addCfgToDiracCfg(diracCfg)
      except Exception:  # pylint: disable=broad-except
        error = 'Failed to load %s' % cfg
        gLogger.exception(error)
        if self.exitOnError:
          DIRAC.exit(-1)
        return S_ERROR(error)

    # Now get the necessary info from self.localCfg
    setupSystems = self.localCfg.getOption(cfgInstallPath('Systems'), ['Configuration', 'Framework'])
    setupDatabases = self.localCfg.getOption(cfgInstallPath('Databases'), [])
    setupServices = [k.split('/') for k in self.localCfg.getOption(cfgInstallPath('Services'), [])]
    setupAgents = [k.split('/') for k in self.localCfg.getOption(cfgInstallPath('Agents'), [])]
    setupExecutors = [k.split('/') for k in self.localCfg.getOption(cfgInstallPath('Executors'), [])]
    setupWeb = self.localCfg.getOption(cfgInstallPath('WebPortal'), False)
    setupConfigurationMaster = self.localCfg.getOption(cfgInstallPath('ConfigurationMaster'), False)
    setupPrivateConfiguration = self.localCfg.getOption(cfgInstallPath('PrivateConfiguration'), False)
    setupConfigurationName = self.localCfg.getOption(cfgInstallPath('ConfigurationName'), self.setup)
    setupAddConfiguration = self.localCfg.getOption(cfgInstallPath('AddConfiguration'), True)

    for serviceTuple in setupServices:
      if len(serviceTuple) != 2:
        error = 'Wrong service specification: system/service'
        if self.exitOnError:
          gLogger.error(error)
          DIRAC.exit(-1)
        return S_ERROR(error)
      serviceSysInstance = serviceTuple[0]
      if serviceSysInstance not in setupSystems:
        setupSystems.append(serviceSysInstance)

    for agentTuple in setupAgents:
      if len(agentTuple) != 2:
        error = 'Wrong agent specification: system/agent'
        if self.exitOnError:
          gLogger.error(error)
          DIRAC.exit(-1)
        return S_ERROR(error)
      agentSysInstance = agentTuple[0]
      if agentSysInstance not in setupSystems:
        setupSystems.append(agentSysInstance)

    for executorTuple in setupExecutors:
      if len(executorTuple) != 2:
        error = 'Wrong executor specification: system/executor'
        if self.exitOnError:
          gLogger.error(error)
          DIRAC.exit(-1)
        return S_ERROR(error)
      executorSysInstance = executorTuple[0]
      if executorSysInstance not in setupSystems:
        setupSystems.append(executorSysInstance)

    # And to find out the available extensions
    extensions = extensionsByPriority()

    # Make sure the necessary directories are there
    if self.basePath != self.instancePath:
      mkDir(self.instancePath)

      instanceEtcDir = os.path.join(self.instancePath, 'etc')
      etcDir = os.path.dirname(self.cfgFile)
      if not os.path.exists(instanceEtcDir):
        mkLink(etcDir, instanceEtcDir)

      if os.path.realpath(instanceEtcDir) != os.path.realpath(etcDir):
        error = 'Instance etc (%s) is not the same as DIRAC etc (%s)' % (instanceEtcDir, etcDir)
        if self.exitOnError:
          gLogger.error(error)
          DIRAC.exit(-1)
        return S_ERROR(error)

    # if any server or agent needs to be install we need the startup directory and runsvdir running
    if setupServices or setupAgents or setupExecutors or setupWeb:
      if not os.path.exists(self.startDir):
        mkDir(self.startDir)
      # And need to make sure runsvdir is running
      result = self.execCommand(0, ['ps', '-ef'])
      if not result['OK']:
        if self.exitOnError:
          gLogger.error('Failed to verify runsvdir running', result['Message'])
          DIRAC.exit(-1)
        return S_ERROR(result['Message'])
      processList = result['Value'][1].split('\n')

      # it is pointless to look for more detailed command.
      # Nobody uses runsvdir.... so if it is there, it is us.
      if all('runsvdir' not in process for process in processList):
        gLogger.notice('Starting runsvdir ...')
        with io.open(os.devnull, 'w') as devnull:
          subprocess.Popen(['nohup', 'runsvdir', self.startDir, 'log:  DIRAC runsv'],
                           stdout=devnull, stderr=devnull, universal_newlines=True)

    if ['Configuration', 'Server'] in setupServices and setupConfigurationMaster:
      # This server hosts the Master of the CS
      from DIRAC.ConfigurationSystem.Client.ConfigurationData import gConfigurationData
      gLogger.notice('Installing Master Configuration Server')

      cfg = self.__getCfg(cfgPath('DIRAC', 'Setups', self.setup), 'Configuration', self.instance)
      self._addCfgToDiracCfg(cfg)
      cfg = self.__getCfg(cfgPath('DIRAC', 'Configuration'), 'Master', 'yes')
      cfg.setOption(cfgPath('DIRAC', 'Configuration', 'Name'), setupConfigurationName)

      serversCfgPath = cfgPath('DIRAC', 'Configuration', 'Servers')
      if not self.localCfg.getOption(serversCfgPath, []):
        serverUrl = 'dips://%s:9135/Configuration/Server' % self.host
        cfg.setOption(serversCfgPath, serverUrl)
        gConfigurationData.setOptionInCFG(serversCfgPath, serverUrl)
      instanceOptionPath = cfgPath('DIRAC', 'Setups', self.setup)
      instanceCfg = self.__getCfg(instanceOptionPath, 'Configuration', self.instance)
      cfg = cfg.mergeWith(instanceCfg)
      self._addCfgToDiracCfg(cfg)

      result = self.getComponentCfg(
          'service',
          'Configuration',
          'Server',
          self.instance,
          extensions,
          addDefaultOptions=True)
      if not result['OK']:
        if self.exitOnError:
          DIRAC.exit(-1)
        return result
      compCfg = result['Value']
      cfg = cfg.mergeWith(compCfg)
      gConfigurationData.mergeWithLocal(cfg)

      self.addDefaultOptionsToComponentCfg('service', 'Configuration', 'Server', ["DIRAC"])
      centralCfg = self._getCentralCfg(installCfg or self.localCfg)
      self._addCfgToLocalCS(centralCfg)
      self.setupComponent('service', 'Configuration', 'Server', ["DIRAC"], checkModule=False)
      self.runsvctrlComponent('Configuration', 'Server', 't')

      while ['Configuration', 'Server'] in setupServices:
        setupServices.remove(['Configuration', 'Server'])

    time.sleep(5)

    # Now need to check if there is valid CS to register the info
    result = scriptCfg.enableCS()
    if not result['OK']:
      if self.exitOnError:
        DIRAC.exit(-1)
      return result

    cfgClient = CSAPI()
    if not cfgClient.initialize():
      error = 'Configuration Server not defined'
      if self.exitOnError:
        gLogger.error(error)
        DIRAC.exit(-1)
      return S_ERROR(error)

    # We need to make sure components are connecting to the Master CS, that is the only one being update
    localServers = self.localCfg.getOption(cfgPath('DIRAC', 'Configuration', 'Servers'))
    masterServer = gConfig.getValue(cfgPath('DIRAC', 'Configuration', 'MasterServer'), '')
    initialCfg = self.__getCfg(cfgPath('DIRAC', 'Configuration'), 'Servers', localServers)
    masterCfg = self.__getCfg(cfgPath('DIRAC', 'Configuration'), 'Servers', masterServer)
    self._addCfgToDiracCfg(masterCfg)

    # 1.- Setup the instances in the CS
    # If the Configuration Server used is not the Master, it can take some time for this
    # info to be propagated, this may cause the later self.setup to fail
    if setupAddConfiguration:
      gLogger.notice('Registering System instances')
      for system in setupSystems:
        self.addSystemInstance(system, self.instance, self.setup, True)
      for system, service in setupServices:
        if not self.addDefaultOptionsToCS(None, 'service', system, service, extensions, overwrite=True)['OK']:
          # If we are not allowed to write to the central CS, add the configuration to the local file
          gLogger.warn("Can't write to central CS, so adding to the specific component CFG",
                       "for %s : %s" % (system, service))
          res = self.addDefaultOptionsToComponentCfg('service', system, service, extensions)
          if not res['OK']:
            gLogger.warn("Can't write to the specific component CFG")
      for system, agent in setupAgents:
        if not self.addDefaultOptionsToCS(None, 'agent', system, agent, extensions, overwrite=True)['OK']:
          # If we are not allowed to write to the central CS, add the configuration to the local file
          gLogger.warn("Can't write to central CS, so adding to the specific component CFG")
          res = self.addDefaultOptionsToComponentCfg('agent', system, agent, extensions)
          if not res['OK']:
            gLogger.warn("Can't write to the specific component CFG",
                         "for %s : %s" % (system, agent))
      for system, executor in setupExecutors:
        if not self.addDefaultOptionsToCS(None, 'executor', system, executor, extensions, overwrite=True)['OK']:
          # If we are not allowed to write to the central CS, add the configuration to the local file
          gLogger.warn("Can't write to central CS, so adding to the specific component CFG")
          res = self.addDefaultOptionsToComponentCfg('executor', system, executor, extensions)
          if not res['OK']:
            gLogger.warn("Can't write to the specific component CFG",
                         "for %s : %s" % (system, executor))
    else:
      gLogger.warn('Configuration parameters definition is not requested')

    if ['Configuration', 'Server'] in setupServices and setupPrivateConfiguration:
      cfg = self.__getCfg(cfgPath('DIRAC', 'Configuration'), 'AutoPublish', 'no')
      self._addCfgToDiracCfg(cfg)

    # 2.- Install requested Databases
    # if MySQL is not installed locally, we assume a host is given
    if setupDatabases:
      result = self.getDatabases()
      if not result['OK']:
        if self.exitOnError:
          gLogger.error('Failed to get databases', result['Message'])
          DIRAC.exit(-1)
        return result
      installedDatabases = result['Value']
      result = self.getAvailableDatabases()
      gLogger.debug("Available databases", result)
      if not result['OK']:
        return result
      dbDict = result['Value']

      for dbName in setupDatabases:
        gLogger.verbose("Setting up database", dbName)
        if dbName not in installedDatabases:
          result = self.installDatabase(dbName)
          if not result['OK']:
            gLogger.error(result['Message'])
            DIRAC.exit(-1)
          extension, system = result['Value']
          gLogger.notice('Database %s from %s/%s installed' % (dbName, extension, system))
        else:
          gLogger.notice('Database %s already installed' % dbName)

        dbSystem = dbDict[dbName]['System']
        result = self.addDatabaseOptionsToCS(None, dbSystem, dbName, overwrite=True)
        if not result['OK']:
          gLogger.error('Database %s CS registration failed: %s' % (dbName, result['Message']))

    if self.mysqlPassword and not self._addMySQLToDiracCfg():
      error = 'Failed to add MySQL user/password to local configuration'
      if self.exitOnError:
        gLogger.error(error)
        DIRAC.exit(-1)
      return S_ERROR(error)

    if self.noSQLHost and not self._addNoSQLToDiracCfg():
      error = 'Failed to add NoSQL connection details to local configuration'
      if self.exitOnError:
        gLogger.error(error)
        DIRAC.exit(-1)
      return S_ERROR(error)

    # 3.- Then installed requested services
    for system, service in setupServices:
      result = self.setupComponent('service', system, service, extensions)
      if not result['OK']:
        gLogger.error(result['Message'])
        continue

    # 4.- Now the agents
    for system, agent in setupAgents:
      result = self.setupComponent('agent', system, agent, extensions)
      if not result['OK']:
        gLogger.error(result['Message'])
        continue

    # 5.- Now the executors
    for system, executor in setupExecutors:
      result = self.setupComponent('executor', system, executor, extensions)
      if not result['OK']:
        gLogger.error(result['Message'])
        continue

    # 6.- And finally the Portal
    if setupWeb:
      self.setupPortal()

    if localServers != masterServer:
      self._addCfgToDiracCfg(initialCfg)
      for system, service in setupServices:
        self.runsvctrlComponent(system, service, 't')
      for system, agent in setupAgents:
        self.runsvctrlComponent(system, agent, 't')
      for system, executor in setupExecutors:
        self.runsvctrlComponent(system, executor, 't')

    return S_OK()

  def _getSectionName(self, compType):
    """
    Returns the section name for a component in the CS
    For self.instance, the section for service is Services,
    whereas the section for agent is Agents
    """
    return S_OK('%ss' % (compType.title()))

  def _createRunitLog(self, runitCompDir):
    self.controlDir = os.path.join(runitCompDir, 'control')
    mkDir(self.controlDir)

    logDir = os.path.join(runitCompDir, 'log')
    mkDir(logDir)

    logConfigFile = os.path.join(logDir, 'config')
    with io.open(logConfigFile, 'w') as fd:
      fd.write(
          u"""s10000000
  n20
  """)

    logRunFile = os.path.join(logDir, 'run')
    with io.open(logRunFile, 'w') as fd:
      fd.write(
          u"""#!/bin/bash

rcfile=%(bashrc)s
[[ -e $rcfile ]] && source ${rcfile}
#
exec svlogd .
  """ % {'bashrc': os.path.join(self.instancePath, 'bashrc')})

    os.chmod(logRunFile, self.gDefaultPerms)

  def installComponent(self, componentType, system, component, extensions, componentModule='', checkModule=True):
    """
    Install runit directory for the specified component
    """
    # Check if the component is already installed
    runitCompDir = os.path.join(self.runitDir, system, component)
    if os.path.exists(runitCompDir):
      msg = "%s %s_%s already installed" % (componentType, system, component)
      gLogger.notice(msg)
      return S_OK(runitCompDir)

    # Check that the software for the component is installed
    # Any "Load" or "Module" option in the configuration defining what modules the given "component"
    # needs to load will be taken care of by self.checkComponentModule.
    if checkModule:
      cModule = componentModule or component
      result = self.checkComponentModule(componentType, system, cModule)
      if (
          not result['OK']
          and not self.checkComponentSoftware(componentType, system, cModule, extensions)['OK']
          and componentType != 'executor'
      ):
        error = 'Software for %s %s/%s is not installed' % (componentType, system, component)
        if self.exitOnError:
          gLogger.error(error)
          DIRAC.exit(-1)
        return S_ERROR(error)

    gLogger.notice('Installing %s %s/%s' % (componentType, system, component))

    # Retrieve bash variables to be set
    result = gConfig.getOption('DIRAC/Setups/%s/%s' % (CSGlobals.getSetup(), system))
    if not result['OK']:
      return result
    self.instance = result['Value']

    specialOptions = {}
    if componentModule:
      specialOptions['Module'] = componentModule
    result = self.getComponentCfg(componentType, system, component, self.instance, extensions,
                                  specialOptions=specialOptions)
    if not result['OK']:
      return result
    compCfg = result['Value']

    result = self._getSectionName(componentType)
    if not result['OK']:
      return result
    section = result['Value']

    bashVars = ''
    if compCfg.isSection('Systems/%s/%s/%s/%s/Environment' % (system, self.instance, section, component)):
      dictionary = compCfg.getAsDict()
      bashSection = dictionary['Systems'][system][self.instance][section][component]['BashVariables']
      for var in bashSection:
        bashVars = '%s\nexport %s=%s' % (bashVars, var, bashSection[var])

    # Now do the actual installation
    try:
      componentCfg = os.path.join(self.linkedRootPath, 'etc', '%s_%s.cfg' % (system, component))
      if not os.path.exists(componentCfg):
        io.open(componentCfg, 'w').close()

      self._createRunitLog(runitCompDir)

      runFile = os.path.join(runitCompDir, 'run')
      with io.open(runFile, 'w') as fd:
        fd.write(
            u"""#!/bin/bash

rcfile=%(bashrc)s
[[ -e $rcfile ]] && source ${rcfile}
#
exec 2>&1
#
[[ "%(componentType)s" = "agent" ]] && renice 20 -p $$
#%(bashVariables)s
#
exec dirac-%(componentType)s %(system)s/%(component)s --cfg %(componentCfg)s < /dev/null
    """ % {'bashrc': os.path.join(self.instancePath, 'bashrc'),
                'bashVariables': bashVars,
                'componentType': componentType.replace("-", "_"),
                'system': system,
                'component': component,
                'componentCfg': componentCfg})

      os.chmod(runFile, self.gDefaultPerms)

      cTypeLower = componentType.lower()
      if cTypeLower == 'agent':
        # This is, e.g., /opt/dirac/runit/WorkfloadManagementSystem/Matcher/control/t
        stopFile = os.path.join(runitCompDir, 'control', 't')
        # This is, e.g., /opt/dirac/control/WorkfloadManagementSystem/Matcher/
        controlDir = self.runitDir.replace('runit', 'control')
        with io.open(stopFile, 'w') as fd:
          fd.write(u"""#!/bin/bash

echo %(controlDir)s/%(system)s/%(component)s/stop_%(type)s
touch %(controlDir)s/%(system)s/%(component)s/stop_%(type)s
""" % {'controlDir': controlDir,
              'system': system,
              'component': component,
              'type': cTypeLower})

        os.chmod(stopFile, self.gDefaultPerms)

    except Exception:
      error = 'Failed to prepare self.setup for %s %s/%s' % (componentType, system, component)
      gLogger.exception(error)
      if self.exitOnError:
        DIRAC.exit(-1)
      return S_ERROR(error)

    result = self.execCommand(5, [runFile])

    gLogger.notice(result['Value'][1])

    return S_OK(runitCompDir)

  def setupComponent(self, componentType, system, component, extensions,
                     componentModule='', checkModule=True):
    """
    Install and create link in startup
    """
    result = self.installComponent(componentType, system, component, extensions, componentModule, checkModule)
    if not result['OK']:
      return result

    # Create the startup entry now
    runitCompDir = result['Value']
    startCompDir = os.path.join(self.startDir, '%s_%s' % (system, component))
    mkDir(self.startDir)
    if not os.path.lexists(startCompDir):
      gLogger.notice('Creating startup link at', startCompDir)
      mkLink(runitCompDir, startCompDir)

      # Wait for the service to be recognised (can't use isfile as supervise/ok is a device)
      start = time.time()
      while (time.time() - 10) < start:
        time.sleep(1)
        if os.path.exists(os.path.join(startCompDir, 'supervise', 'ok')):
          break
      else:
        return S_ERROR('Failed to find supervise/ok for component %s_%s' % (system, component))

    # Check the runsv status
    start = time.time()
    while (time.time() - 20) < start:
      result = self.getStartupComponentStatus([(system, component)])
      if not result['OK']:
        continue
      if result['Value'] and result['Value']['%s_%s' % (system, component)]['RunitStatus'] == "Run":
        break
      time.sleep(1)
    else:
      return S_ERROR('Failed to start the component %s_%s' % (system, component))

    resDict = {}
    resDict['ComponentType'] = componentType
    resDict['RunitStatus'] = result['Value']['%s_%s' % (system, component)]['RunitStatus']

    return S_OK(resDict)

  def unsetupComponent(self, system, component):
    """
    Remove link from startup
    """
    for startCompDir in glob.glob(os.path.join(self.startDir, '%s_%s' % (system, component))):
      try:
        os.unlink(startCompDir)
      except Exception:
        gLogger.exception()

    return S_OK()

  def uninstallComponent(self, system, component, removeLogs):
    """
    Remove startup and runit directories
    """
    self.runsvctrlComponent(system, component, 'd')

    self.unsetupComponent(system, component)

    if removeLogs:
      for runitCompDir in glob.glob(os.path.join(self.runitDir, system, component)):
        try:
          shutil.rmtree(runitCompDir)
        except Exception:
          gLogger.exception()

    result = self.removeComponentOptionsFromCS(system, component)
    if not result['OK']:
      return result

    return S_OK()

  def setupPortal(self):
    """
    Install and create link in startup
    """
    result = self.installPortal()
    if not result['OK']:
      return result

    # Create the startup entries now
    runitCompDir = result['Value']
    startCompDir = os.path.join(self.startDir, 'Web_WebApp')

    mkDir(self.startDir)

    mkLink(runitCompDir, startCompDir)

    time.sleep(5)

    # Check the runsv status
    start = time.time()
    while (time.time() - 10) < start:
      result = self.getStartupComponentStatus([('Web', 'WebApp')])
      if not result['OK']:
        return S_ERROR('Failed to start the Portal')
      if result['Value'] and \
         result['Value']['%s_%s' % ('Web', 'WebApp')]['RunitStatus'] == "Run":
        break
      time.sleep(1)

    # Final check
    return self.getStartupComponentStatus([('Web', 'WebApp')])

  def installPortal(self):
    """
    Install runit directories for the Web Portal
    """
    # Check that the software for the Web Portal is installed
    if six.PY2:
      webDir = os.path.join(self.linkedRootPath, 'WebAppDIRAC')
      webappInstalled = os.path.exists(webDir)
    else:
      from importlib import metadata  # pylint: disable=no-name-in-module

      try:
        metadata.version("WebAppDIRAC")
      except metadata.PackageNotFoundError:
        webappInstalled = False
      else:
        webappInstalled = True

    if not webappInstalled:
      error = 'WebApp extension not installed'
      gLogger.error(error)
      if self.exitOnError:
        gLogger.error(error)
        DIRAC.exit(-1)
      return S_ERROR(error)

    # Check if the component is already installed
    runitWebAppDir = os.path.join(self.runitDir, 'Web', 'WebApp')

    # Check if the component is already installed
    if os.path.exists(runitWebAppDir):
      msg = "Web Portal already installed"
      gLogger.notice(msg)
    else:
      gLogger.notice('Installing Web Portal')
      # Now do the actual installation
      try:
        self._createRunitLog(runitWebAppDir)
        runFile = os.path.join(runitWebAppDir, 'run')
        with io.open(runFile, 'w') as fd:
          fd.write(
              u"""#!/bin/bash

rcfile=%(bashrc)s
[[ -e $rcfile ]] && source $rcfile
#
exec 2>&1
#
exec dirac-webapp-run -p < /dev/null
  """ % {'bashrc': os.path.join(self.instancePath, 'bashrc'),
                  'DIRAC': self.linkedRootPath})

        os.chmod(runFile, self.gDefaultPerms)
      except Exception:
        error = 'Failed to prepare self.setup for Web Portal'
        gLogger.exception(error)
        if self.exitOnError:
          DIRAC.exit(-1)
        return S_ERROR(error)

      result = self.execCommand(5, [runFile])
      gLogger.notice(result['Value'][1])

    return S_OK(runitWebAppDir)

  def getMySQLPasswords(self):
    """
    Get MySQL passwords from local configuration or prompt
    """
    if not self.mysqlRootPwd:
      self.mysqlRootPwd = prompt(u"MySQL root password: ", is_password=True)

    if not self.mysqlPassword:
      # Take it if it is already defined
      self.mysqlPassword = self.localCfg.getOption('/Systems/Databases/Password', '')
    if not self.mysqlPassword:
      self.mysqlPassword = prompt(u"MySQL Dirac password: ", is_password=True)

    return S_OK()

  def setMySQLPasswords(self, root='', dirac=''):
    """
    Set MySQL passwords
    """
    if root:
      self.mysqlRootPwd = root
    if dirac:
      self.mysqlPassword = dirac

    return S_OK()

  def getMySQLStatus(self):
    """
    Get the status of the MySQL database installation
    """
    result = self.execCommand(0, ['mysqladmin', 'status'])
    if not result['OK']:
      return result

    keys = [
        None, "UpTime", "NumberOfThreads", "NumberOfQuestions",
        "NumberOfSlowQueries", "NumberOfOpens", "FlushTables", "OpenTables",
        "QueriesPerSecond"
    ]
    return S_OK({
        key: output.strip().split()[0]
        for key, output in zip(keys, result['Value'][1].split(":")) if key
    })

  def getAvailableDatabases(self, extensions=None):
    """Find all databases defined"""
    if not extensions:
      extensions = extensionsByPriority()

    res = self.getAvailableSQLDatabases(extensions)
    gLogger.debug("Available SQL databases", res)
    if not res['OK']:
      return res
    sqlDBs = res['Value']

    res = self.getAvailableESDatabases(extensions)
    gLogger.debug("Available ES databases", res)
    if not res['OK']:
      return res
    esDBs = res['Value']

    allDBs = sqlDBs.copy()
    allDBs.update(esDBs)

    return S_OK(allDBs)

  def getAvailableSQLDatabases(self, extensions):
    """
    Find the sql files

    :param list extensions: list of DIRAC extensions
    :return: dict of MySQL DBs
    """
    dbDict = {}
    for extension in extensions:
      databases = findDatabases(extensions)
      for systemName, dbSql in databases:
        dbName = dbSql.replace('.sql', '')
        dbDict[dbName] = {}
        dbDict[dbName]['Type'] = 'MySQL'
        # TODO: Does this need to be replaced
        dbDict[dbName]['Extension'] = extension.replace("DIRAC", "")
        dbDict[dbName]['System'] = systemName.replace('System', '')
    return S_OK(dbDict)

  def getAvailableESDatabases(self, extensions):
    """
    Find the ES DBs definitions, by introspection.

    This method makes a few assumptions:
    - the files defining modules interacting with ES DBs are found in the xyzSystem/DB/ directories
    - the files defining modules interacting with ES DBs are named xyzDB.py (e.g. MonitoringDB.py)
    - the modules define ES DBs classes with the same name of the module (e.g. class MonitoringDB())
    - the classes are inheriting from the ElasticDB module (e.g. class MonitoringDB(ElasticDB))

    Result should be something like::

       {'MonitoringDB': {'Type': 'ES', 'System': 'Monitoring', 'Extension': ''},
        'ElasticJobParametersDB': {'Type': 'ES', 'System': 'WorkloadManagement', 'Extension': ''}}

    :param list extensions: list of DIRAC extensions
    :return: dict of ES DBs
    """
    dbDict = {}
    for extension in extensions:
      sqlDatabases = findDatabases(extension)
      for systemName, dbName in findModules(extension, "DB", "*DB"):
        if (systemName, dbName + ".sql") in sqlDatabases:
          continue

        # Introspect all possible ones for a ElasticDB attribute
        try:
          module = importlib.import_module(".".join([extension, systemName, "DB", dbName]))
          dbClass = getattr(module, dbName)
        except (AttributeError, ImportError):
          continue
        if 'ElasticDB' not in str(inspect.getmro(dbClass)):
          continue

        dbDict[dbName] = {}
        dbDict[dbName]['Type'] = 'ES'
        # TODO: Does this need to be replaced
        dbDict[dbName]['Extension'] = extension.replace("DIRAC", "")
        dbDict[dbName]['System'] = systemName

    return S_OK(dbDict)

  def getDatabases(self):
    """
    Get the list of installed databases
    """
    result = self.execMySQL('SHOW DATABASES')
    if not result['OK']:
      return result
    dbList = []
    for dbName in result['Value']:
      if not dbName[0] in ['Database', 'information_schema', 'mysql', 'test']:
        dbList.append(dbName[0])

    return S_OK(dbList)

  def installDatabase(self, dbName):
    """
    Install requested DB in MySQL server
    """
    if not self.mysqlRootPwd:
      rootPwdPath = cfgInstallPath('Database', 'RootPwd')
      return S_ERROR('Missing %s in %s' % (rootPwdPath, self.cfgFile))

    if not self.mysqlPassword:
      self.mysqlPassword = self.localCfg.getOption(cfgPath('Systems', 'Databases', 'Password'), self.mysqlPassword)
      if not self.mysqlPassword:
        mysqlPwdPath = cfgPath('Systems', 'Databases', 'Password')
        return S_ERROR('Missing %s in %s' % (mysqlPwdPath, self.cfgFile))

    gLogger.notice('Installing', dbName)

    # is there by chance an extension of it?
    for extension in extensionsByPriority():
      databases = {k: v for v, k in findDatabases(extension)}
      filename = dbName + ".sql"
      if filename in databases:
        break
    else:
      error = 'Database %s not found' % dbName
      gLogger.error(error)
      if self.exitOnError:
        DIRAC.exit(-1)
      return S_ERROR(error)
    systemName = databases[filename]
    moduleName = ".".join([extension, systemName, "DB"])
    gLogger.debug("Installing %s from %s" % (filename, moduleName))
    dbSql = importlib_resources.read_text(moduleName, filename)

    # just check
    result = self.execMySQL('SHOW STATUS')
    if not result['OK']:
      error = 'Could not connect to MySQL server'
      gLogger.error(error)
      if self.exitOnError:
        DIRAC.exit(-1)
      return S_ERROR(error)

    # now creating the Database
    result = self.execMySQL('CREATE DATABASE `%s`' % dbName)
    if not result['OK'] and 'database exists' not in result['Message']:
      gLogger.error('Failed to create databases', result['Message'])
      if self.exitOnError:
        DIRAC.exit(-1)
      return result

    perms = "SELECT,INSERT,LOCK TABLES,UPDATE,DELETE,CREATE,DROP,ALTER,REFERENCES," \
            "CREATE VIEW,SHOW VIEW,INDEX,TRIGGER,ALTER ROUTINE,CREATE ROUTINE"
    for cmd in ["GRANT %s ON `%s`.* TO '%s'@'localhost'" % (perms, dbName, self.mysqlUser),
                "GRANT %s ON `%s`.* TO '%s'@'%s'" % (perms, dbName, self.mysqlUser, self.mysqlHost),
                "GRANT %s ON `%s`.* TO '%s'@'%%'" % (perms, dbName, self.mysqlUser)]:
      result = self.execMySQL(cmd)
      if not result['OK']:
        error = "Error executing '%s'" % cmd
        gLogger.error(error, result['Message'])
        if self.exitOnError:
          DIRAC.exit(-1)
        return S_ERROR(error)
    result = self.execMySQL('FLUSH PRIVILEGES')
    if not result['OK']:
      gLogger.error('Failed to flush privileges', result['Message'])
      if self.exitOnError:
        exit(-1)
      return result

    # first getting the lines to be executed, and then execute them
    try:
      cmdLines = self._createMySQLCMDLines(dbSql)

      # We need to run one SQL cmd at once, mysql is much happier that way.
      # Create a string of commands, ignoring comment lines
      sqlString = '\n'.join(x for x in cmdLines if not x.startswith("--"))

      # Now run each command (They are seperated by ;)
      # Ignore any empty ones
      cmds = [x.strip() for x in sqlString.split(";") if x.strip()]
      for cmd in cmds:
        result = self.execMySQL(cmd, dbName)
        if not result['OK']:
          error = 'Failed to initialize Database'
          gLogger.notice(cmd)
          gLogger.error(error, result['Message'])
          if self.exitOnError:
            DIRAC.exit(-1)
          return S_ERROR(error)

    except Exception as e:
      gLogger.error(str(e))
      if self.exitOnError:
        DIRAC.exit(-1)
      return S_ERROR(error)

    return S_OK([extension, systemName])

  def uninstallDatabase(self, gConfig_o, dbName):
    """
    Remove a database from DIRAC
    """
    result = self.getAvailableDatabases()
    if not result['OK']:
      return result

    dbSystem = result['Value'][dbName]['System']

    result = self.removeDatabaseOptionsFromCS(gConfig_o, dbSystem, dbName)
    if not result['OK']:
      return result

    return S_OK('DB successfully uninstalled')

  def _createMySQLCMDLines(self, dbSql):
    """Creates a list of MYSQL commands to be executed, inspecting the SQL

    :param str dbSql: The SQL to parse
    :returns: list of str corresponding to executable SQL statements
    """
    cmdLines = []
    for line in dbSql.split("\n"):
      # Should we first source an SQL file (is this sql file an extension)?
      if line.lower().startswith('source'):
        sourcedDBbFileName = line.split(' ')[1].replace('\n', '')
        gLogger.info("Found file to source: %s" % sourcedDBbFileName)
        module, filename = sourcedDBbFileName.rsplit("/", 1)
        dbSourced = importlib_resources.read_text(module.replace("/", "."), filename)
        for lineSourced in dbSourced.split("\n"):
          if lineSourced.strip():
            cmdLines.append(lineSourced.strip())

      # Creating/adding cmdLines
      else:
        if line.strip():
          cmdLines.append(line.strip())

    return cmdLines

  def execMySQL(self, cmd, dbName='mysql', localhost=False):
    """
    Execute MySQL Command
    """
    from DIRAC.Core.Utilities.MySQL import MySQL
    if not self.mysqlRootPwd:
      return S_ERROR('MySQL root password is not defined')
    if dbName not in self.db:
      dbHost = self.mysqlHost
      if localhost:
        dbHost = 'localhost'
      self.db[dbName] = MySQL(dbHost, self.mysqlRootUser, self.mysqlRootPwd, dbName, self.mysqlPort)
    if not self.db[dbName]._connected:
      error = 'Could not connect to MySQL server'
      gLogger.error(error)
      if self.exitOnError:
        DIRAC.exit(-1)
      return S_ERROR(error)
    return self.db[dbName]._query(cmd)

  def _addMySQLToDiracCfg(self):
    """
    Add the database access info to the local configuration
    """
    if not self.mysqlPassword:
      return S_ERROR('Missing %s in %s' % (cfgInstallPath('Database', 'Password'), self.cfgFile))

    sectionPath = cfgPath('Systems', 'Databases')
    cfg = self.__getCfg(sectionPath, 'User', self.mysqlUser)
    cfg.setOption(cfgPath(sectionPath, 'Password'), self.mysqlPassword)
    cfg.setOption(cfgPath(sectionPath, 'Host'), self.mysqlHost)
    cfg.setOption(cfgPath(sectionPath, 'Port'), self.mysqlPort)

    return self._addCfgToDiracCfg(cfg)

  def _addNoSQLToDiracCfg(self):
    """
    Add the NoSQL database access info to the local configuration
    """
    sectionPath = cfgPath('Systems', 'NoSQLDatabases')
    cfg = self.__getCfg(sectionPath, 'Host', self.noSQLHost)
    cfg.setOption(cfgPath(sectionPath, 'Port'), self.noSQLPort)
    if self.noSQLUser:
      cfg.setOption(cfgPath(sectionPath, 'User'), self.noSQLUser)
    if self.noSQLPassword:
      cfg.setOption(cfgPath(sectionPath, 'Password'), self.noSQLPassword)
    if self.noSQLSSL:
      cfg.setOption(cfgPath(sectionPath, 'SSL'), self.noSQLSSL)

    return self._addCfgToDiracCfg(cfg)

  def execCommand(self, timeout, cmd):
    """
    Execute command tuple and handle Error cases
    """
    gLogger.debug("Executing command %s with timeout %d" % (cmd, timeout))
    result = systemCall(timeout, cmd)
    if not result['OK']:
      if timeout and result['Message'].find('Timeout') == 0:
        return result
      gLogger.error('Failed to execute', '%s: %s' % (cmd[0], result['Message']))
      if self.exitOnError:
        DIRAC.exit(-1)
      return result

    if result['Value'][0]:
      error = 'Failed to execute'
      gLogger.error(error, cmd[0])
      gLogger.error('Exit code:', ('%s\n' % result['Value'][0]) + '\n'.join(result['Value'][1:]))
      if self.exitOnError:
        DIRAC.exit(-1)
      error = S_ERROR(error)
      error['Value'] = result['Value']
      return error

    gLogger.verbose(result['Value'][1])

    return result

  def installTornado(self):
    """
    Install runit directory for the tornado, and add the configuration of the required service
    """
    # Check if the Tornado itself is already installed
    runitCompDir = os.path.join(self.runitDir, 'Tornado', 'Tornado')
    if os.path.exists(runitCompDir):
      msg = "Tornado_Tornado already installed"
      gLogger.notice(msg)
      return S_OK(runitCompDir)

    # Check the setup for the given system
    result = gConfig.getOption('DIRAC/Setups/%s/Tornado' % (CSGlobals.getSetup()))
    if not result['OK']:
      return result
    self.instance = result['Value']

    # Now do the actual installation
    try:

      self._createRunitLog(runitCompDir)

      runFile = os.path.join(runitCompDir, 'run')
      with io.open(runFile, 'wt') as fd:
        fd.write(
            u"""#!/bin/bash
rcfile=%(bashrc)s
[ -e $rcfile ] && source $rcfile
#
export DIRAC_USE_TORNADO_IOLOOP=Yes
exec 2>&1
#
#
exec tornado-start-all
""" % {'bashrc': os.path.join(self.instancePath, 'bashrc')})

      os.chmod(runFile, self.gDefaultPerms)

    except Exception:
      error = 'Failed to prepare self.setup forTornado'
      gLogger.exception(error)
      if self.exitOnError:
        DIRAC.exit(-1)
      return S_ERROR(error)

    result = self.execCommand(5, [runFile])

    gLogger.notice(result['Value'][1])

    return S_OK(runitCompDir)

  def addTornadoOptionsToCS(self, gConfig_o):
    """
    Add the section with the component options to the CS
    """
    if gConfig_o:
      gConfig_o.forceRefresh()

    instanceOption = cfgPath('DIRAC', 'Setups', self.setup, 'Tornado')

    if gConfig_o:
      compInstance = gConfig_o.getValue(instanceOption, '')
    else:
      compInstance = self.localCfg.getOption(instanceOption, '')
    if not compInstance:
      return S_ERROR('%s not defined in %s' % (instanceOption, self.cfgFile))
    tornadoSection = cfgPath('Systems', 'Tornado', compInstance)

    cfg = self.__getCfg(tornadoSection, 'Port', 8443)
    # cfg.setOption(cfgPath(tornadoSection, 'Password'), self.mysqlPassword)
    return self._addCfgToCS(cfg)

  def setupTornadoService(self, system, component, extensions,
                          componentModule='', checkModule=True):
    """
    Install and create link in startup
    """
    # Create the startup entry now
    # Force the system and component to be 'Tornado' but preserve the interface and the code
    # just to allow for easier refactoring maybe later
    system = 'Tornado'
    component = 'Tornado'
    componentType = 'Tornado'
    runitCompDir = os.path.join(self.runitDir, 'Tornado', 'Tornado')
    startCompDir = os.path.join(self.startDir, '%s_%s' % (system, component))
    mkDir(self.startDir)
    if not os.path.lexists(startCompDir):
      gLogger.notice('Creating startup link at', startCompDir)
      mkLink(runitCompDir, startCompDir)

      # Wait for the service to be recognised (can't use isfile as supervise/ok is a device)
      start = time.time()
      while (time.time() - 10) < start:
        time.sleep(1)
        if os.path.exists(os.path.join(startCompDir, 'supervise', 'ok')):
          break
      else:
        return S_ERROR('Failed to find supervise/ok for component %s_%s' % (system, component))

    # Check the runsv status
    start = time.time()
    while (time.time() - 20) < start:
      result = self.getStartupComponentStatus([(system, component)])
      if not result['OK']:
        continue
      if result['Value'] and result['Value']['%s_%s' % (system, component)]['RunitStatus'] == "Run":
        break
      time.sleep(1)
    else:
      return S_ERROR('Failed to start the component %s_%s' % (system, component))

    resDict = {}
    resDict['ComponentType'] = componentType
    resDict['RunitStatus'] = result['Value']['%s_%s' % (system, component)]['RunitStatus']

    return S_OK(resDict)

    # port = compCfg.getOption('Port', 0)
    # if port and self.host:
    #   urlsPath = cfgPath('Systems', system, compInstance, 'URLs')
    #   cfg.createNewSection(urlsPath)
    #   failoverUrlsPath = cfgPath('Systems', system, compInstance, 'FailoverURLs')
    #   cfg.createNewSection(failoverUrlsPath)
    #   cfg.setOption(cfgPath(urlsPath, component),
    #                 'dips://%s:%d/%s/%s' % (self.host, port, system, component))


gComponentInstaller = ComponentInstaller()
