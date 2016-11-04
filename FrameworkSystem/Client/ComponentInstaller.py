"""
Class for managing the installation of DIRAC components:
MySQL, DB's, Services's, Agents, Executors and Consumers

It only makes use of defaults in LocalInstallation Section in dirac.cfg

The Following Options are used::

  /DIRAC/Setup:             Setup to be used for any operation
  /LocalInstallation/InstanceName:    Name of the Instance for the current Setup (default /DIRAC/Setup)
  /LocalInstallation/LogLevel:        LogLevel set in "run" script for all components installed
  /LocalInstallation/RootPath:        Used instead of rootPath in "run" script if defined (if links are used to named versions)
  /LocalInstallation/InstancePath:    Location where runit and startup directories are created (default rootPath)
  /LocalInstallation/UseVersionsDir:  DIRAC is installed under versions/<Versioned Directory> with a link from pro
                                      (This option overwrites RootPath and InstancePath)
  /LocalInstallation/Host:            Used when build the URL to be published for the installed service (default: socket.getfqdn())
  /LocalInstallation/RunitDir:        Location where runit directory is created (default InstancePath/runit)
  /LocalInstallation/StartupDir:      Location where startup directory is created (default InstancePath/startup)
  /LocalInstallation/MySQLDir:        Location where mysql databases are created (default InstancePath/mysql)
  /LocalInstallation/Database/User:                 (default Dirac)
  /LocalInstallation/Database/Password:             (must be set for SystemAdministrator Service to work)
  /LocalInstallation/Database/RootPwd:              (must be set for SystemAdministrator Service to work)
  /LocalInstallation/Database/Host:                 (must be set for SystemAdministrator Service to work)
  /LocalInstallation/Database/MySQLSmallMem:        Configure a MySQL with small memory requirements for testing purposes innodb_buffer_pool_size=200MB
  /LocalInstallation/Database/MySQLLargeMem:        Configure a MySQL with high memory requirements for production purposes innodb_buffer_pool_size=10000MB

The setupSite method (used by the dirac-setup-site command) will use the following info::

  /LocalInstallation/Systems:       List of Systems to be defined for this instance in the CS (default: Configuration, Framework)
  /LocalInstallation/Databases:     List of Databases to be installed and configured
  /LocalInstallation/Services:      List of System/ServiceName to be setup
  /LocalInstallation/Agents:        List of System/AgentName to be setup
  /LocalInstallation/WebPortal:     Boolean to setup the Web Portal (default no)
  /LocalInstallation/ConfigurationMaster: Boolean, requires Configuration/Server to be given in the list of Services (default: no)
  /LocalInstallation/PrivateConfiguration: Boolean, requires Configuration/Server to be given in the list of Services (default: no)

If a Master Configuration Server is being installed the following Options can be used::

  /LocalInstallation/ConfigurationName: Name of the Configuration (default: Setup )
  /LocalInstallation/AdminUserName:  Name of the Admin user (default: None )
  /LocalInstallation/AdminUserDN:    DN of the Admin user certificate (default: None )
  /LocalInstallation/AdminUserEmail: Email of the Admin user (default: None )
  /LocalInstallation/AdminGroupName: Name of the Admin group (default: dirac_admin )
  /LocalInstallation/HostDN: DN of the host certificate (default: None )
  /LocalInstallation/VirtualOrganization: Name of the main Virtual Organization (default: None)

"""

import os
import re
import glob
import stat
import time
import shutil
import socket

import DIRAC
from DIRAC import rootPath
from DIRAC import gConfig
from DIRAC import gLogger
from DIRAC.Core.Utilities.Subprocess import systemCall
from DIRAC.Core.Utilities.ReturnValues import S_OK, S_ERROR

from DIRAC.Core.Utilities.CFG import CFG
from DIRAC.Core.Utilities.Version import getVersion
from DIRAC.Core.Utilities.File import mkDir, mkLink
from DIRAC.ConfigurationSystem.Client.CSAPI import CSAPI
from DIRAC.ConfigurationSystem.Client.Helpers import cfgPath, cfgPathToList, cfgInstallPath, \
                                                     cfgInstallSection, ResourcesDefaults, CSGlobals
from DIRAC.Core.Security.Properties import ALARMS_MANAGEMENT, SERVICE_ADMINISTRATOR, \
                                           CS_ADMINISTRATOR, JOB_ADMINISTRATOR, \
                                           FULL_DELEGATION, PROXY_MANAGEMENT, OPERATOR, \
                                           NORMAL_USER, TRUSTED_HOST

from DIRAC.ConfigurationSystem.Client import PathFinder
from DIRAC.FrameworkSystem.Client.ComponentMonitoringClient import ComponentMonitoringClient
from DIRAC.FrameworkSystem.Utilities import MonitoringUtilities
from DIRAC.Core.Base.private.ModuleLoader import ModuleLoader
from DIRAC.Core.Base.AgentModule import AgentModule
from DIRAC.Core.Base.ExecutorModule import ExecutorModule
from DIRAC.Core.DISET.RequestHandler import RequestHandler
from DIRAC.Core.Utilities.PrettyPrint import printTable
from DIRAC.Core.Utilities.Platform import getPlatformString

__RCSID__ = "$Id$"


class ComponentInstaller( object ):

  def __init__( self ):
    self.gDefaultPerms = stat.S_IWUSR | stat.S_IRUSR | stat.S_IXUSR | stat.S_IRGRP | stat.S_IXGRP | stat.S_IROTH | stat.S_IXOTH

    # On command line tools this can be set to True to abort after the first error.
    self.exitOnError = False

    # First some global defaults
    gLogger.debug( 'DIRAC Root Path =', rootPath )

    # FIXME: we probably need a better way to do this
    self.mysqlRootPwd = ''
    self.mysqlPassword = ''
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
    self.mysqlDir = ''
    self.mysqlDbDir = ''
    self.mysqlLogDir = ''
    self.mysqlMyOrg = ''
    self.mysqlMyCnf = ''
    self.mysqlStartupScript = ''
    self.mysqlUser = ''
    self.mysqlHost = ''
    self.mysqlPort = ''
    self.mysqlRootUser = ''
    self.mysqlSmallMem = ''
    self.mysqlLargeMem = ''
    self.controlDir = ''
    self.componentTypes = [ 'service', 'agent', 'executor', 'consumer' ]
    self.monitoringClient = None

    self.loadDiracCfg()

  def loadDiracCfg( self, verbose = False ):
    """
    Read again defaults from dirac.cfg
    """

    from DIRAC.Core.Utilities.Network import getFQDN

    self.localCfg = CFG()
    self.cfgFile = os.path.join( rootPath, 'etc', 'dirac.cfg' )
    try:
      self.localCfg.loadFromFile( self.cfgFile )
    except Exception:
      gLogger.always( "Can't load ", self.cfgFile )
      gLogger.always( "Might be OK if setting up the site" )

    self.setup = self.localCfg.getOption( cfgPath( 'DIRAC', 'Setup' ), '' )
    self.instance = self.localCfg.getOption( cfgInstallPath( 'InstanceName' ), self.setup )
    self.logLevel = self.localCfg.getOption( cfgInstallPath( 'LogLevel' ), 'INFO' )
    self.linkedRootPath = self.localCfg.getOption( cfgInstallPath( 'RootPath' ), rootPath )
    useVersionsDir = self.localCfg.getOption( cfgInstallPath( 'UseVersionsDir' ), False )

    self.host = self.localCfg.getOption( cfgInstallPath( 'Host' ), getFQDN() )

    self.basePath = os.path.dirname( rootPath )
    self.instancePath = self.localCfg.getOption( cfgInstallPath( 'InstancePath' ), rootPath )
    if useVersionsDir:
      # This option takes precedence
      self.instancePath = os.path.dirname( os.path.dirname( rootPath ) )
      self.linkedRootPath = os.path.join( self.instancePath, 'pro' )
    if verbose:
      gLogger.notice( 'Using Instance Base Dir at', self.instancePath )

    self.runitDir = os.path.join( self.instancePath, 'runit' )
    self.runitDir = self.localCfg.getOption( cfgInstallPath( 'RunitDir' ), self.runitDir )
    if verbose:
      gLogger.notice( 'Using Runit Dir at', self.runitDir )

    self.startDir = os.path.join( self.instancePath, 'startup' )
    self.startDir = self.localCfg.getOption( cfgInstallPath( 'StartupDir' ), self.startDir )
    if verbose:
      gLogger.notice( 'Using Startup Dir at', self.startDir )

    self.controlDir = os.path.join( self.instancePath, 'control' )
    self.controlDir = self.localCfg.getOption( cfgInstallPath( 'ControlDir' ), self.controlDir )
    if verbose:
      gLogger.notice( 'Using Control Dir at', self.controlDir )

    # Now some MySQL default values
    self.db = {}

    self.mysqlDir = os.path.join( self.instancePath, 'mysql' )
    self.mysqlDir = self.localCfg.getOption( cfgInstallPath( 'MySQLDir' ), self.mysqlDir )
    if verbose:
      gLogger.notice( 'Using MySQL Dir at', self.mysqlDir )

    self.mysqlDbDir = os.path.join( self.mysqlDir, 'db' )
    self.mysqlLogDir = os.path.join( self.mysqlDir, 'log' )
    self.mysqlMyOrg = os.path.join( rootPath, 'mysql', 'etc', 'my.cnf' )
    self.mysqlMyCnf = os.path.join( self.mysqlDir, '.my.cnf' )

    self.mysqlStartupScript = os.path.join( rootPath, 'mysql', 'share', 'mysql', 'mysql.server' )

    self.mysqlRootPwd = self.localCfg.getOption( cfgInstallPath( 'Database', 'RootPwd' ), self.mysqlRootPwd )
    if verbose and self.mysqlRootPwd:
      gLogger.notice( 'Reading Root MySQL Password from local configuration' )

    self.mysqlUser = self.localCfg.getOption( cfgInstallPath( 'Database', 'User' ), '' )
    if self.mysqlUser:
      if verbose:
        gLogger.notice( 'Reading MySQL User from local configuration' )
    else:
      self.mysqlUser = 'Dirac'

    self.mysqlPassword = self.localCfg.getOption( cfgInstallPath( 'Database', 'Password' ), self.mysqlPassword )
    if verbose and self.mysqlPassword:
      gLogger.notice( 'Reading %s MySQL Password from local configuration ' % self.mysqlUser )

    self.mysqlHost = self.localCfg.getOption( cfgInstallPath( 'Database', 'Host' ), '' )
    if self.mysqlHost:
      if verbose:
        gLogger.notice( 'Using MySQL Host from local configuration', self.mysqlHost )
    else:
      # if it is not defined use the same as for dirac services
      self.mysqlHost = self.host

    self.mysqlPort = self.localCfg.getOption( cfgInstallPath( 'Database', 'Port' ), 0 )
    if self.mysqlPort:
      if verbose:
        gLogger.notice( 'Using MySQL Port from local configuration ', self.mysqlPort )
    else:
      # if it is not defined use the same as for dirac services
      self.mysqlPort = 3306

    self.mysqlRootUser = self.localCfg.getOption( cfgInstallPath( 'Database', 'RootUser' ), '' )
    if self.mysqlRootUser:
      if verbose:
        gLogger.notice( 'Using MySQL root user from local configuration ', self.mysqlRootUser )
    else:
      # if it is not defined use root
      self.mysqlRootUser = 'root'

    self.mysqlMode = self.localCfg.getOption( cfgInstallPath( 'Database', 'MySQLMode' ), '' )
    if verbose and self.mysqlMode:
      gLogger.notice( 'Configuring MySQL server as %s' % self.mysqlMode )

    self.mysqlSmallMem = self.localCfg.getOption( cfgInstallPath( 'Database', 'MySQLSmallMem' ), False )
    if verbose and self.mysqlSmallMem:
      gLogger.notice( 'Configuring MySQL server for Low Memory usage' )

    self.mysqlLargeMem = self.localCfg.getOption( cfgInstallPath( 'Database', 'MySQLLargeMem' ), False )
    if verbose and self.mysqlLargeMem:
      gLogger.notice( 'Configuring MySQL server for Large Memory usage' )

    self.monitoringClient = ComponentMonitoringClient()
    if verbose and self.monitoringClient:
      gLogger.notice( 'Client configured for Component Monitoring' )

  def getInfo( self, extensions ):
    result = getVersion()
    if not result['OK']:
      return result
    rDict = result['Value']
    if self.setup:
      rDict['Setup'] = self.setup
    else:
      rDict['Setup'] = 'Unknown'
    return S_OK( rDict )

  def getExtensions( self ):
    """
    Get the list of installed extensions
    """
    initList = glob.glob( os.path.join( rootPath, '*DIRAC', '__init__.py' ) )
    extensions = [ os.path.basename( os.path.dirname( k ) ) for k in initList]
    try:
      extensions.remove( 'DIRAC' )
    except Exception:
      error = 'DIRAC is not properly installed'
      gLogger.exception( error )
      if self.exitOnError:
        DIRAC.exit( -1 )
      return S_ERROR( error )

    return S_OK( extensions )

  def _addCfgToDiracCfg( self, cfg, verbose = False ):
    """
    Merge cfg into existing dirac.cfg file
    """
    if str( self.localCfg ):
      newCfg = self.localCfg.mergeWith( cfg )
    else:
      newCfg = cfg
    result = newCfg.writeToFile( self.cfgFile )
    if not result:
      return result
    self.loadDiracCfg( verbose )
    return result

  def _addCfgToCS( self, cfg ):
    """
    Merge cfg into central CS
    """

    cfgClient = CSAPI()
    result = cfgClient.downloadCSData()
    if not result['OK']:
      return result
    result = cfgClient.mergeFromCFG( cfg )
    if not result['OK']:
      return result
    result = cfgClient.commit()
    return result

  def _addCfgToLocalCS( self, cfg ):
    """
    Merge cfg into local CS
    """
    csName = self.localCfg.getOption( cfgPath( 'DIRAC', 'Configuration', 'Name' ) , '' )
    if not csName:
      error = 'Missing %s' % cfgPath( 'DIRAC', 'Configuration', 'Name' )
      if self.exitOnError:
        gLogger.error( error )
        DIRAC.exit( -1 )
      return S_ERROR( error )

    csCfg = CFG()
    csFile = os.path.join( rootPath, 'etc', '%s.cfg' % csName )
    if os.path.exists( csFile ):
      csCfg.loadFromFile( csFile )
    if str( csCfg ):
      newCfg = csCfg.mergeWith( cfg )
    else:
      newCfg = cfg
    return newCfg.writeToFile( csFile )

  def _removeOptionFromCS( self, path ):
    """
    Delete options from central CS
    """
    cfgClient = CSAPI()
    result = cfgClient.downloadCSData()
    if not result['OK']:
      return result
    result = cfgClient.delOption( path )
    if not result['OK']:
      return result
    result = cfgClient.commit()
    return result

  def _removeSectionFromCS( self, path ):
    """
    Delete setions from central CS
    """
    cfgClient = CSAPI()
    result = cfgClient.downloadCSData()
    if not result['OK']:
      return result
    result = cfgClient.delSection( path )
    if not result['OK']:
      return result
    result = cfgClient.commit()
    return result

  def _getCentralCfg( self, installCfg ):
    """
    Create the skeleton of central Cfg for an initial Master CS
    """
    # First copy over from installation cfg
    centralCfg = CFG()

    # DIRAC/Extensions
    extensions = self.localCfg.getOption( cfgInstallPath( 'Extensions' ), [] )
    while 'Web' in list( extensions ):
      extensions.remove( 'Web' )
    centralCfg.createNewSection( 'DIRAC', '' )
    if extensions:
      centralCfg['DIRAC'].addKey( 'Extensions', ','.join( extensions ), '' ) #pylint: disable=no-member

    vo = self.localCfg.getOption( cfgInstallPath( 'VirtualOrganization' ), '' )
    if vo:
      centralCfg['DIRAC'].addKey( 'VirtualOrganization', vo, '' ) #pylint: disable=no-member

    for section in [ 'Systems', 'Resources',
                     'Resources/Sites', 'Resources/Sites/DIRAC',
                     'Resources/Sites/LCG', 'Operations',
                     'Website', 'Registry' ]:
      if installCfg.isSection( section ):
        centralCfg.createNewSection( section, contents = installCfg[section] )

    # Now try to add things from the Installation section
    # Registry
    adminUserName = self.localCfg.getOption( cfgInstallPath( 'AdminUserName' ), '' )
    adminUserDN = self.localCfg.getOption( cfgInstallPath( 'AdminUserDN' ), '' )
    adminUserEmail = self.localCfg.getOption( cfgInstallPath( 'AdminUserEmail' ), '' )
    adminGroupName = self.localCfg.getOption( cfgInstallPath( 'AdminGroupName' ), 'dirac_admin' )
    hostDN = self.localCfg.getOption( cfgInstallPath( 'HostDN' ), '' )
    defaultGroupName = 'user'
    adminGroupProperties = [ ALARMS_MANAGEMENT, SERVICE_ADMINISTRATOR,
                             CS_ADMINISTRATOR, JOB_ADMINISTRATOR,
                             FULL_DELEGATION, PROXY_MANAGEMENT, OPERATOR ]
    defaultGroupProperties = [ NORMAL_USER ]
    defaultHostProperties = [ TRUSTED_HOST, CS_ADMINISTRATOR,
                              JOB_ADMINISTRATOR, FULL_DELEGATION,
                              PROXY_MANAGEMENT, OPERATOR ]

    for section in ( cfgPath( 'Registry' ),
                     cfgPath( 'Registry', 'Users' ),
                     cfgPath( 'Registry', 'Groups' ),
                     cfgPath( 'Registry', 'Hosts' ) ):
      if not centralCfg.isSection( section ):
        centralCfg.createNewSection( section )

    if adminUserName:
      if not ( adminUserDN and adminUserEmail ):
        gLogger.error( 'AdminUserName is given but DN or Mail is missing it will not be configured' )
      else:
        for section in [ cfgPath( 'Registry', 'Users', adminUserName ),
                         cfgPath( 'Registry', 'Groups', defaultGroupName ),
                         cfgPath( 'Registry', 'Groups', adminGroupName ) ]:
          if not centralCfg.isSection( section ):
            centralCfg.createNewSection( section )

        if centralCfg['Registry'].existsKey( 'DefaultGroup' ): #pylint: disable=unsubscriptable-object,no-member
          centralCfg['Registry'].deleteKey( 'DefaultGroup' ) #pylint: disable=unsubscriptable-object,no-member
        centralCfg['Registry'].addKey( 'DefaultGroup', defaultGroupName, '' ) #pylint: disable=unsubscriptable-object,no-member

        if centralCfg['Registry']['Users'][adminUserName].existsKey( 'DN' ): #pylint: disable=unsubscriptable-object
          centralCfg['Registry']['Users'][adminUserName].deleteKey( 'DN' ) #pylint: disable=unsubscriptable-object
        centralCfg['Registry']['Users'][adminUserName].addKey( 'DN', adminUserDN, '' ) #pylint: disable=unsubscriptable-object

        if centralCfg['Registry']['Users'][adminUserName].existsKey( 'Email' ): #pylint: disable=unsubscriptable-object
          centralCfg['Registry']['Users'][adminUserName].deleteKey( 'Email' ) #pylint: disable=unsubscriptable-object
        centralCfg['Registry']['Users'][adminUserName].addKey( 'Email' , adminUserEmail, '' ) #pylint: disable=unsubscriptable-object

        # Add Admin User to Admin Group and default group
        for group in [adminGroupName, defaultGroupName]:
          if not centralCfg['Registry']['Groups'][group].isOption( 'Users' ): #pylint: disable=unsubscriptable-object
            centralCfg['Registry']['Groups'][group].addKey( 'Users', '', '' ) #pylint: disable=unsubscriptable-object
          users = centralCfg['Registry']['Groups'][group].getOption( 'Users', [] ) #pylint: disable=unsubscriptable-object
          if adminUserName not in users:
            centralCfg['Registry']['Groups'][group].appendToOption( 'Users', ', %s' % adminUserName ) #pylint: disable=unsubscriptable-object
          if not centralCfg['Registry']['Groups'][group].isOption( 'Properties' ): #pylint: disable=unsubscriptable-object
            centralCfg['Registry']['Groups'][group].addKey( 'Properties', '', '' ) #pylint: disable=unsubscriptable-object

        properties = centralCfg['Registry']['Groups'][adminGroupName].getOption( 'Properties', [] ) #pylint: disable=unsubscriptable-object
        for prop in adminGroupProperties:
          if prop not in properties:
            properties.append( prop )
            centralCfg['Registry']['Groups'][adminGroupName].appendToOption( 'Properties', ', %s' % prop ) #pylint: disable=unsubscriptable-object

        properties = centralCfg['Registry']['Groups'][defaultGroupName].getOption( 'Properties', [] ) #pylint: disable=unsubscriptable-object
        for prop in defaultGroupProperties:
          if prop not in properties:
            properties.append( prop )
            centralCfg['Registry']['Groups'][defaultGroupName].appendToOption( 'Properties', ', %s' % prop ) #pylint: disable=unsubscriptable-object

    # Add the master Host description
    if hostDN:
      hostSection = cfgPath( 'Registry', 'Hosts', self.host )
      if not centralCfg.isSection( hostSection ):
        centralCfg.createNewSection( hostSection )
      if centralCfg['Registry']['Hosts'][self.host].existsKey( 'DN' ): #pylint: disable=unsubscriptable-object
        centralCfg['Registry']['Hosts'][self.host].deleteKey( 'DN' ) #pylint: disable=unsubscriptable-object
      centralCfg['Registry']['Hosts'][self.host].addKey( 'DN', hostDN, '' ) #pylint: disable=unsubscriptable-object
      if not centralCfg['Registry']['Hosts'][self.host].isOption( 'Properties' ): #pylint: disable=unsubscriptable-object
        centralCfg['Registry']['Hosts'][self.host].addKey( 'Properties', '', '' ) #pylint: disable=unsubscriptable-object
      properties = centralCfg['Registry']['Hosts'][self.host].getOption( 'Properties', [] ) #pylint: disable=unsubscriptable-object
      for prop in defaultHostProperties:
        if prop not in properties:
          properties.append( prop )
          centralCfg['Registry']['Hosts'][self.host].appendToOption( 'Properties', ', %s' % prop ) #pylint: disable=unsubscriptable-object

    # Operations
    if adminUserEmail:
      operationsCfg = self.__getCfg( cfgPath( 'Operations', 'Defaults', 'EMail' ), 'Production', adminUserEmail )
      centralCfg = centralCfg.mergeWith( operationsCfg )
      operationsCfg = self.__getCfg( cfgPath( 'Operations', 'Defaults', 'EMail' ), 'Logging', adminUserEmail )
      centralCfg = centralCfg.mergeWith( operationsCfg )

    # Website
    websiteCfg = self.__getCfg( cfgPath( 'Website', 'Authorization',
                                         'systems', 'configuration' ), 'Default', 'all' )
    websiteCfg['Website'].addKey( 'DefaultGroups',
                                  ', '.join( ['visitor', defaultGroupName, adminGroupName] ), '' )
    websiteCfg['Website'].addKey( 'DefaultSetup', self.setup, '' )
    websiteCfg['Website']['Authorization']['systems']['configuration'].addKey( 'showHistory' ,
                                                                               'CSAdministrator' , '' )
    websiteCfg['Website']['Authorization']['systems']['configuration'].addKey( 'commitConfiguration' ,
                                                                               'CSAdministrator' , '' )
    websiteCfg['Website']['Authorization']['systems']['configuration'].addKey( 'showCurrentDiff' ,
                                                                               'CSAdministrator' , '' )
    websiteCfg['Website']['Authorization']['systems']['configuration'].addKey( 'showDiff' ,
                                                                               'CSAdministrator' , '' )
    websiteCfg['Website']['Authorization']['systems']['configuration'].addKey( 'rollbackToVersion' ,
                                                                               'CSAdministrator' , '' )
    websiteCfg['Website']['Authorization']['systems']['configuration'].addKey( 'manageRemoteConfig' ,
                                                                               'CSAdministrator' , '' )
    websiteCfg['Website']['Authorization']['systems']['configuration'].appendToOption( 'manageRemoteConfig' ,
                                                                                       ', ServiceAdministrator' )

    centralCfg = centralCfg.mergeWith( websiteCfg )

    return centralCfg

  def __getCfg( self, section, option = '', value = '' ):
    """
    Create a new Cfg with given info
    """
    if not section:
      return None
    cfg = CFG()
    sectionList = []
    for sect in cfgPathToList( section ):
      if not sect:
        continue
      sectionList.append( sect )
      cfg.createNewSection( cfgPath( *sectionList ) )
    if not sectionList:
      return None

    if option and value:
      sectionList.append( option )
      cfg.setOption( cfgPath( *sectionList ), value )

    return cfg

  def addOptionToDiracCfg( self, option, value ):
    """
    Add Option to dirac.cfg
    """
    optionList = cfgPathToList( option )
    optionName = optionList[-1]
    section = cfgPath( *optionList[:-1] )
    cfg = self.__getCfg( section, optionName, value )

    if not cfg:
      return S_ERROR( 'Wrong option: %s = %s' % ( option, value ) )

    if self._addCfgToDiracCfg( cfg ):
      return S_OK()

    return S_ERROR( 'Could not merge %s=%s with local configuration' % ( option, value ) )

  def removeComponentOptionsFromCS( self, system, component, mySetup = None ):
    """
    Remove the section with Component options from the CS, if possible
    """
    if mySetup is None:
      mySetup = self.setup

    result = self.monitoringClient.getInstallations( { 'UnInstallationTime': None, 'Instance': component },
                                                     { 'System': system },
                                                     {}, True )
    if not result[ 'OK' ]:
      return result
    installations = result[ 'Value' ]

    instanceOption = cfgPath( 'DIRAC', 'Setups', mySetup, system )
    if gConfig:
      compInstance = gConfig.getValue( instanceOption, '' )
    else:
      compInstance = self.localCfg.getOption( instanceOption, '' )

    if len( installations ) == 1:
      remove = True
      removeMain = False
      installation = installations[0]
      cType = installation[ 'Component' ][ 'Type' ]

      # Is the component a rename of another module?
      if installation[ 'Instance' ] == installation[ 'Component' ][ 'Module' ]:
        isRenamed = False
      else:
        isRenamed = True

      result = self.monitoringClient.getInstallations( { 'UnInstallationTime': None },
                                                       { 'System': system, 'Module': installation[ 'Component' ][ 'Module' ] },
                                                       {}, True )
      if not result[ 'OK' ]:
        return result
      installations = result[ 'Value' ]

      # If the component is not renamed we keep it in the CS if there are any renamed ones
      if not isRenamed:
        if len( installations ) > 1:
          remove = False
      # If the component is renamed and is the last one, we remove the entry for the main module as well
      else:
        if len( installations ) == 1:
          removeMain = True

      if remove:
        result = self._removeSectionFromCS( cfgPath( 'Systems', system,
                                                     compInstance,
                                                     installation[ 'Component' ][ 'Type' ].title() + 's', component ) )
        if not result[ 'OK' ]:
          return result

        if not isRenamed and cType == 'service':
          result = self._removeOptionFromCS( cfgPath( 'Systems', system, compInstance, 'URLs', component ) )
          if not result[ 'OK' ]:
            return result

      if removeMain:
        result = self._removeSectionFromCS( cfgPath( 'Systems', system,
                                                     compInstance,
                                                     installation[ 'Component' ][ 'Type' ].title() + 's',
                                                     installation[ 'Component' ][ 'Module' ] ) )
        if not result[ 'OK' ]:
          return result

        if cType == 'service':
          result = self._removeOptionFromCS( cfgPath( 'Systems', system, compInstance, 'URLs', installation[ 'Component' ][ 'Module' ] ) )
          if not result[ 'OK' ]:
            return result

      return S_OK( 'Successfully removed entries from CS' )
    return S_OK( 'Instances of this component still exist. It won\'t be completely removed' )

  def addDefaultOptionsToCS( self, gConfig, componentType, systemName,
                             component, extensions, mySetup = None,
                             specialOptions = {}, overwrite = False,
                             addDefaultOptions = True ):
    """
    Add the section with the component options to the CS
    """
    if mySetup is None:
      mySetup = self.setup

    if gConfig:
      gConfig.forceRefresh()

    system = systemName.replace( 'System', '' )
    instanceOption = cfgPath( 'DIRAC', 'Setups', mySetup, system )
    if gConfig:
      compInstance = gConfig.getValue( instanceOption, '' )
    else:
      compInstance = self.localCfg.getOption( instanceOption, '' )
    if not compInstance:
      return S_ERROR( '%s not defined in %s' % ( instanceOption, self.cfgFile ) )

    result = self._getSectionName( componentType )
    if not result[ 'OK' ]:
      return result
    sectionName = result[ 'Value' ]

    # Check if the component CS options exist
    addOptions = True
    componentSection = cfgPath( 'Systems', system, compInstance, sectionName, component )
    if not overwrite:
      if gConfig:
        result = gConfig.getOptions( componentSection )
        if result['OK']:
          addOptions = False

    if not addOptions:
      return S_OK( 'Component options already exist' )

    # Add the component options now
    result = self.getComponentCfg( componentType, system, component, compInstance, extensions, specialOptions, addDefaultOptions )
    if not result['OK']:
      return result
    compCfg = result['Value']

    gLogger.notice( 'Adding to CS', '%s %s/%s' % ( componentType, system, component ) )
    resultAddToCFG = self._addCfgToCS( compCfg )
    if componentType == 'executor':
      # Is it a container ?
      execList = compCfg.getOption( '%s/Load' % componentSection, [] )
      for element in execList:
        result = self.addDefaultOptionsToCS( gConfig, componentType, systemName, element, extensions, self.setup,
                                             {}, overwrite )
        resultAddToCFG.setdefault( 'Modules', {} )
        resultAddToCFG['Modules'][element] = result['OK']
    return resultAddToCFG

  def addDefaultOptionsToComponentCfg( self, componentType, systemName, component, extensions ):
    """
    Add default component options local component cfg
    """
    system = systemName.replace( 'System', '' )
    instanceOption = cfgPath( 'DIRAC', 'Setups', self.setup, system )
    compInstance = self.localCfg.getOption( instanceOption, '' )
    if not compInstance:
      return S_ERROR( '%s not defined in %s' % ( instanceOption, self.cfgFile ) )

    # Add the component options now
    result = self.getComponentCfg( componentType, system, component, compInstance, extensions )
    if not result['OK']:
      return result
    compCfg = result['Value']

    compCfgFile = os.path.join( rootPath, 'etc', '%s_%s.cfg' % ( system, component ) )
    return compCfg.writeToFile( compCfgFile )

  def addCfgToComponentCfg( self, componentType, systemName, component, cfg ):
    """
    Add some extra configuration to the local component cfg
    """
    result = self._getSectionName( componentType )
    if not result[ 'OK' ]:
      return result
    sectionName = result[ 'Value' ]

    if not cfg:
      return S_OK()
    system = systemName.replace( 'System', '' )
    instanceOption = cfgPath( 'DIRAC', 'Setups', self.setup, system )
    compInstance = self.localCfg.getOption( instanceOption, '' )

    if not compInstance:
      return S_ERROR( '%s not defined in %s' % ( instanceOption, self.cfgFile ) )
    compCfgFile = os.path.join( rootPath, 'etc', '%s_%s.cfg' % ( system, component ) )
    compCfg = CFG()
    if os.path.exists( compCfgFile ):
      compCfg.loadFromFile( compCfgFile )
    sectionPath = cfgPath( 'Systems', system, compInstance, sectionName )

    newCfg = self.__getCfg( sectionPath )
    newCfg.createNewSection( cfgPath( sectionPath, component ), 'Added by ComponentInstaller', cfg )
    if newCfg.writeToFile( compCfgFile ):
      return S_OK( compCfgFile )
    error = 'Can not write %s' % compCfgFile
    gLogger.error( error )
    return S_ERROR( error )

  def getComponentCfg( self, componentType, system, component, compInstance, extensions,
                       specialOptions = {}, addDefaultOptions = True ):
    """
    Get the CFG object of the component configuration
    """
    result = self._getSectionName( componentType )
    if not result[ 'OK' ]:
      return result
    sectionName = result[ 'Value' ]

    componentModule = component
    if "Module" in specialOptions and specialOptions[ 'Module' ]:
      componentModule = specialOptions['Module']

    compCfg = CFG()

    if addDefaultOptions:
      extensionsDIRAC = [ x + 'DIRAC' for x in extensions ] + extensions
      for ext in extensionsDIRAC + ['DIRAC']:
        cfgTemplatePath = os.path.join( rootPath, ext, '%sSystem' % system, 'ConfigTemplate.cfg' )
        if os.path.exists( cfgTemplatePath ):
          gLogger.notice( 'Loading configuration template', cfgTemplatePath )
          # Look up the component in this template
          loadCfg = CFG()
          loadCfg.loadFromFile( cfgTemplatePath )
          compCfg = loadCfg.mergeWith( compCfg )


      compPath = cfgPath( sectionName, componentModule )
      if not compCfg.isSection( compPath ):
        error = 'Can not find %s in template' % compPath
        gLogger.error( error )
        if self.exitOnError:
          DIRAC.exit( -1 )
        return S_ERROR( error )

      compCfg = compCfg[sectionName][componentModule] #pylint: disable=unsubscriptable-object

      # Delete Dependencies section if any
      compCfg.deleteKey( 'Dependencies' )

    sectionPath = cfgPath( 'Systems', system, compInstance, sectionName )
    cfg = self.__getCfg( sectionPath )
    cfg.createNewSection( cfgPath( sectionPath, component ), '', compCfg )

    for option, value in specialOptions.items():
      cfg.setOption( cfgPath( sectionPath, component, option ), value )

    # Add the service URL
    if componentType == "service":
      port = compCfg.getOption( 'Port' , 0 )
      if port and self.host:
        urlsPath = cfgPath( 'Systems', system, compInstance, 'URLs' )
        cfg.createNewSection( urlsPath )
        cfg.setOption( cfgPath( urlsPath, component ),
                       'dips://%s:%d/%s/%s' % ( self.host, port, system, component ) )

    return S_OK( cfg )

  def addDatabaseOptionsToCS( self, gConfig, systemName, dbName, mySetup = None, overwrite = False ):
    """
    Add the section with the database options to the CS
    """
    if mySetup is None:
      mySetup = self.setup

    if gConfig:
      gConfig.forceRefresh()

    system = systemName.replace( 'System', '' )
    instanceOption = cfgPath( 'DIRAC', 'Setups', mySetup, system )
    if gConfig:
      compInstance = gConfig.getValue( instanceOption, '' )
    else:
      compInstance = self.localCfg.getOption( instanceOption, '' )
    if not compInstance:
      return S_ERROR( '%s not defined in %s' % ( instanceOption, self.cfgFile ) )

    # Check if the component CS options exist
    addOptions = True
    if not overwrite:
      databasePath = cfgPath( 'Systems', system, compInstance, 'Databases', dbName )
      result = gConfig.getOptions( databasePath )
      if result['OK']:
        addOptions = False
    if not addOptions:
      return S_OK( 'Database options already exist' )

    # Add the component options now
    result = self.getDatabaseCfg( system, dbName, compInstance )
    if not result['OK']:
      return result
    databaseCfg = result['Value']
    gLogger.notice( 'Adding to CS', '%s/%s' % ( system, dbName ) )
    return self._addCfgToCS( databaseCfg )

  def removeDatabaseOptionsFromCS( self, gConfig_o, system, dbName, mySetup = None ):
    """
    Remove the section with database options from the CS, if possible
    """
    if mySetup is None:
      mySetup = self.setup

    result = self.monitoringClient.installationExists( { 'UnInstallationTime': None },
                                                       { 'System': system, 'Type': 'DB', 'Module': dbName },
                                                       {} )
    if not result[ 'OK' ]:
      return result
    exists = result[ 'Value' ]

    instanceOption = cfgPath( 'DIRAC', 'Setups', mySetup, system )
    if gConfig_o:
      compInstance = gConfig_o.getValue( instanceOption, '' )
    else:
      compInstance = self.localCfg.getOption( instanceOption, '' )

    if not exists:
      result = self._removeSectionFromCS( cfgPath( 'Systems', system, compInstance, 'Databases', dbName ) )
      if not result[ 'OK' ]:
        return result

    return S_OK( 'Successfully removed entries from CS' )

  def getDatabaseCfg( self, system, dbName, compInstance ):
    """
    Get the CFG object of the database configuration
    """
    databasePath = cfgPath( 'Systems', system, compInstance, 'Databases', dbName )
    cfg = self.__getCfg( databasePath, 'DBName', dbName )
    cfg.setOption( cfgPath( databasePath, 'Host' ), self.mysqlHost )
    cfg.setOption( cfgPath( databasePath, 'Port' ), self.mysqlPort )

    return S_OK( cfg )

  def addSystemInstance( self, systemName, compInstance, mySetup = None, myCfg = False ):
    """
    Add a new system self.instance to dirac.cfg and CS
    """
    if mySetup is None:
      mySetup = self.setup

    system = systemName.replace( 'System', '' )
    gLogger.notice( 'Adding %s system as %s self.instance for %s self.setup to dirac.cfg and CS' % ( system, compInstance, mySetup ) )

    cfg = self.__getCfg( cfgPath( 'DIRAC', 'Setups', mySetup ), system, compInstance )
    if myCfg:
      if not self._addCfgToDiracCfg( cfg ):
        return S_ERROR( 'Failed to add system self.instance to dirac.cfg' )

    return self._addCfgToCS( cfg )

  def printStartupStatus( self, rDict ):
    """
    Print in nice format the return dictionary from self.getStartupComponentStatus
    (also returned by self.runsvctrlComponent)
    """
    fields = ['Name', 'Runit', 'Uptime', 'PID']
    records = []
    try:
      for comp in rDict:
        records.append( [comp,
                         rDict[comp]['RunitStatus'],
                         rDict[comp]['Timeup'],
                         str( rDict[comp]['PID'] ) ] )
      printTable( fields, records )
    except Exception as x:
      print "Exception while gathering data for printing: %s" % str( x )
    return S_OK()

  def printOverallStatus( self, rDict ):
    """
    Print in nice format the return dictionary from self.getOverallStatus
    """
    fields = ['System', 'Name', 'Type', 'Setup', 'Installed', 'Runit', 'Uptime', 'PID']
    records = []
    try:
      for compType in rDict:
        for system in rDict[compType]:
          for component in rDict[compType][system]:
            record = [ system, component, compType.lower()[:-1] ]
            if rDict[compType][system][component]['Setup']:
              record.append( 'SetUp' )
            else:
              record.append( 'NotSetUp' )
            if rDict[compType][system][component]['Installed']:
              record.append( 'Installed' )
            else:
              record.append( 'NotInstalled' )
            record.append( str( rDict[compType][system][component]['RunitStatus'] ) )
            record.append( str( rDict[compType][system][component]['Timeup'] ) )
            record.append( str( rDict[compType][system][component]['PID'] ) )
            records.append( record )
      printTable( fields, records )
    except Exception as x:
      print "Exception while gathering data for printing: %s" % str( x )

    return S_OK()

  def getAvailableSystems( self, extensions ):
    """
    Get the list of all systems (in all given extensions) locally available
    """
    systems = []

    for extension in extensions:
      extensionPath = os.path.join( DIRAC.rootPath, extension, '*System' )
      for system in [ os.path.basename( k ).split( 'System' )[0] for k in glob.glob( extensionPath ) ]:
        if system not in systems:
          systems.append( system )

    return systems

  def getSoftwareComponents( self, extensions ):
    """
    Get the list of all the components ( services and agents ) for which the software
    is installed on the system
    """
    # The Gateway does not need a handler
    services = { 'Framework' : ['Gateway'] }
    agents = {}
    executors = {}
    remainders = {}

    resultDict = {}

    remainingTypes = [ cType for cType in self.componentTypes if cType not in [ 'service', 'agent', 'executor' ] ]
    resultIndexes = {}
    # Components other than services, agents and executors
    for cType in remainingTypes:
      result = self._getSectionName( cType )
      if not result[ 'OK' ]:
        return result
      resultIndexes[ cType ] = result[ 'Value' ]
      resultDict[ resultIndexes[ cType ] ] = {}
      remainders[ cType ] = {}

    for extension in ['DIRAC'] + [ x + 'DIRAC' for x in extensions]:
      if not os.path.exists( os.path.join( rootPath, extension ) ):
        # Not all the extensions are necessarily installed in this self.instance
        continue
      systemList = os.listdir( os.path.join( rootPath, extension ) )
      for sys in systemList:
        system = sys.replace( 'System', '' )
        try:
          agentDir = os.path.join( rootPath, extension, sys, 'Agent' )
          agentList = os.listdir( agentDir )
          for agent in agentList:
            if os.path.splitext( agent )[1] == ".py":
              agentFile = os.path.join( agentDir, agent )
              with open( agentFile, 'r' ) as afile:
                body = afile.read()
              if body.find( 'AgentModule' ) != -1 or body.find( 'OptimizerModule' ) != -1:
                if not agents.has_key( system ):
                  agents[system] = []
                agents[system].append( agent.replace( '.py', '' ) )
        except OSError:
          pass
        try:
          serviceDir = os.path.join( rootPath, extension, sys, 'Service' )
          serviceList = os.listdir( serviceDir )
          for service in serviceList:
            if service.find( 'Handler' ) != -1 and os.path.splitext( service )[1] == '.py':
              if not services.has_key( system ):
                services[system] = []
              if system == 'Configuration' and service == 'ConfigurationHandler.py':
                service = 'ServerHandler.py'
              services[system].append( service.replace( '.py', '' ).replace( 'Handler', '' ) )
        except OSError:
          pass
        try:
          executorDir = os.path.join( rootPath, extension, sys, 'Executor' )
          executorList = os.listdir( executorDir )
          for executor in executorList:
            if os.path.splitext( executor )[1] == ".py":
              executorFile = os.path.join( executorDir, executor )
              with open( executorFile, 'r' ) as afile:
                body = afile.read()
              if body.find( 'OptimizerExecutor' ) != -1:
                if not executors.has_key( system ):
                  executors[system] = []
                executors[system].append( executor.replace( '.py', '' ) )
        except OSError:
          pass

        # Rest of component types
        for cType in remainingTypes:
          try:
            remainDir = os.path.join( rootPath, extension, sys, cType.title() )
            remainList = os.listdir( remainDir )
            for remainder in remainList:
              if os.path.splitext( remainder )[1] == ".py":
                if not remainders[ cType ].has_key( system ):
                  remainders[ cType ][system] = []
                remainders[ cType ][system].append( remainder.replace( '.py', '' ) )
          except OSError:
            pass

    resultDict['Services'] = services
    resultDict['Agents'] = agents
    resultDict['Executors'] = executors
    for cType in remainingTypes:
      resultDict[ resultIndexes[ cType ] ] = remainders[ cType ]
    return S_OK( resultDict )

  def getInstalledComponents( self ):
    """
    Get the list of all the components ( services and agents )
    installed on the system in the runit directory
    """

    resultDict = {}
    resultIndexes = {}
    for cType in self.componentTypes:
      result = self._getSectionName( cType )
      if not result[ 'OK' ]:
        return result
      resultIndexes[ cType ] = result[ 'Value' ]
      resultDict[ resultIndexes[ cType ] ] = {}

    systemList = os.listdir( self.runitDir )
    for system in systemList:
      systemDir = os.path.join( self.runitDir, system )
      components = os.listdir( systemDir )
      for component in components:
        try:
          runFile = os.path.join( systemDir, component, 'run' )
          rfile = open( runFile, 'r' )
          body = rfile.read()
          rfile.close()

          for cType in self.componentTypes:
            if body.find( 'dirac-%s' % ( cType ) ) != -1:
              if not resultDict[ resultIndexes[ cType ] ].has_key( system ):
                resultDict[ resultIndexes[ cType ] ][system] = []
              resultDict[ resultIndexes[ cType ] ][system].append( component )
        except IOError:
          pass

    return S_OK( resultDict )

  def getSetupComponents( self ):
    """
    Get the list of all the components ( services and agents )
    set up for running with runsvdir in startup directory
    """

    resultDict = {}
    resultIndexes = {}
    for cType in self.componentTypes:
      result = self._getSectionName( cType )
      if not result[ 'OK' ]:
        return result
      resultIndexes[ cType ] = result[ 'Value' ]
      resultDict[ resultIndexes[ cType ] ] = {}

    if not os.path.isdir( self.startDir ):
      return S_ERROR( 'Startup Directory does not exit: %s' % self.startDir )
    componentList = os.listdir( self.startDir )
    for component in componentList:
      try:
        runFile = os.path.join( self.startDir, component, 'run' )
        rfile = open( runFile, 'r' )
        body = rfile.read()
        rfile.close()

        for cType in self.componentTypes:
          if body.find( 'dirac-%s' % ( cType ) ) != -1:
            system, compT = component.split( '_' )[0:2]
            if not resultDict[ resultIndexes[ cType ] ].has_key( system ):
              resultDict[ resultIndexes[ cType ] ][system] = []
            resultDict[ resultIndexes[ cType ] ][system].append( compT )
      except IOError:
        pass

    return S_OK( resultDict )

  def getStartupComponentStatus( self, componentTupleList ):
    """
    Get the list of all the components ( services and agents )
    set up for running with runsvdir in startup directory
    """
    try:
      if componentTupleList:
        cList = []
        for componentTuple in componentTupleList:
          cList.extend( glob.glob( os.path.join( self.startDir, '_'.join( componentTuple ) ) ) )
      else:
        cList = glob.glob( os.path.join( self.startDir, '*' ) )
    except Exception:
      error = 'Failed to parse List of Components'
      gLogger.exception( error )
      if self.exitOnError:
        DIRAC.exit( -1 )
      return S_ERROR( error )

    result = self.execCommand( 0, ['runsvstat'] + cList )
    if not result['OK']:
      return result
    output = result['Value'][1].strip().split( '\n' )

    componentDict = {}
    for line in output:
      if not line:
        continue
      cname, routput = line.split( ':' )
      cname = cname.replace( '%s/' % self.startDir, '' )
      run = False
      reResult = re.search( '^ run', routput )
      if reResult:
        run = True
      down = False
      reResult = re.search( '^ down', routput )
      if reResult:
        down = True
      reResult = re.search( '([0-9]+) seconds', routput )
      timeup = 0
      if reResult:
        timeup = reResult.group( 1 )
      reResult = re.search( 'pid ([0-9]+)', routput )
      pid = 0
      if reResult:
        pid = reResult.group( 1 )
      runsv = "Not running"
      if run or down:
        runsv = "Running"
      reResult = re.search( 'runsv not running', routput )
      if reResult:
        runsv = "Not running"

      runDict = {}
      runDict['CPU'] = -1
      runDict['MEM'] = -1
      runDict['VSZ'] = -1
      runDict['RSS'] = -1
      if pid: # check the process CPU usage and memory
        # PID %CPU %MEM VSZ
        result = self.execCommand( 0, ['ps', '-p', pid, 'u'] )
        if result['OK'] and len( result['Value'] ) > 0:
          stats = result['Value'][1]
          values = re.findall( r"\d*\.\d+|\d+", stats )
          if len( values ) > 0:
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

    return S_OK( componentDict )

  def getComponentModule( self, gConfig, system, component, compType ):
    """
    Get the component software module
    """
    self.setup = CSGlobals.getSetup()
    self.instance = gConfig.getValue( cfgPath( 'DIRAC', 'Setups', self.setup, system ), '' )
    if not self.instance:
      return S_OK( component )
    module = gConfig.getValue( cfgPath( 'Systems', system, self.instance, compType, component, 'Module' ), '' )
    if not module:
      module = component
    return S_OK( module )

  def getOverallStatus( self, extensions ):
    """
    Get the list of all the components ( services and agents )
    set up for running with runsvdir in startup directory
    """

    result = self.getSoftwareComponents( extensions )
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

    result = self.getStartupComponentStatus( [] )
    if not result['OK']:
      return result
    runitDict = result['Value']

    # Collect the info now
    resultDict = {}
    resultIndexes = {}
    for cType in self.componentTypes:
      result = self._getSectionName( cType )
      if not result[ 'OK' ]:
        return result
      resultIndexes[ cType ] = result[ 'Value' ]
      resultDict[ resultIndexes[ cType ] ] = {}

    for compType in resultIndexes.values():
      if softDict.has_key( 'Services' ):
        for system in softDict[compType]:
          resultDict[compType][system] = {}
          for component in softDict[compType][system]:
            if system == 'Configuration' and component == 'Configuration':
              # Fix to avoid missing CS due to different between Service name and Handler name
              component = 'Server'
            resultDict[compType][system][component] = {}
            resultDict[compType][system][component]['Setup'] = False
            resultDict[compType][system][component]['Installed'] = False
            resultDict[compType][system][component]['RunitStatus'] = 'Unknown'
            resultDict[compType][system][component]['Timeup'] = 0
            resultDict[compType][system][component]['PID'] = 0
            # TODO: why do we need a try here?
            try:
              if component in setupDict[compType][system]:
                resultDict[compType][system][component]['Setup'] = True
            except Exception:
              pass
            try:
              if component in installedDict[compType][system]:
                resultDict[compType][system][component]['Installed'] = True
            except Exception:
              pass
            try:
              compDir = system + '_' + component
              if runitDict.has_key( compDir ):
                resultDict[compType][system][component]['RunitStatus'] = runitDict[compDir]['RunitStatus']
                resultDict[compType][system][component]['Timeup'] = runitDict[compDir]['Timeup']
                resultDict[compType][system][component]['PID'] = runitDict[compDir]['PID']
                resultDict[compType][system][component]['CPU'] = runitDict[compDir]['CPU']
                resultDict[compType][system][component]['MEM'] = runitDict[compDir]['MEM']
                resultDict[compType][system][component]['RSS'] = runitDict[compDir]['RSS']
                resultDict[compType][system][component]['VSZ'] = runitDict[compDir]['VSZ']
            except Exception:
              # print str(x)
              pass

      # Installed components can be not the same as in the software list
      if installedDict.has_key( 'Services' ):
        for system in installedDict[compType]:
          for component in installedDict[compType][system]:
            if compType in resultDict:
              if system in resultDict[compType]:
                if component in resultDict[compType][system]:
                  continue
            resultDict[compType][system][component] = {}
            resultDict[compType][system][component]['Setup'] = False
            resultDict[compType][system][component]['Installed'] = True
            resultDict[compType][system][component]['RunitStatus'] = 'Unknown'
            resultDict[compType][system][component]['Timeup'] = 0
            resultDict[compType][system][component]['PID'] = 0
            # TODO: why do we need a try here?
            try:
              if component in setupDict[compType][system]:
                resultDict[compType][system][component]['Setup'] = True
            except Exception:
              pass
            try:
              compDir = system + '_' + component
              if runitDict.has_key( compDir ):
                resultDict[compType][system][component]['RunitStatus'] = runitDict[compDir]['RunitStatus']
                resultDict[compType][system][component]['Timeup'] = runitDict[compDir]['Timeup']
                resultDict[compType][system][component]['PID'] = runitDict[compDir]['PID']
                resultDict[compType][system][component]['CPU'] = runitDict[compDir]['CPU']
                resultDict[compType][system][component]['MEM'] = runitDict[compDir]['MEM']
                resultDict[compType][system][component]['RSS'] = runitDict[compDir]['RSS']
                resultDict[compType][system][component]['VSZ'] = runitDict[compDir]['VSZ']
            except Exception:
              # print str(x)
              pass

    return S_OK( resultDict )

  def checkComponentModule( self, componentType, system, module ):
    """
    Check existence of the given module
    and if it inherits from the proper class
    """
    if componentType == 'agent':
      loader = ModuleLoader( "Agent", PathFinder.getAgentSection, AgentModule )
    elif componentType == 'service':
      loader = ModuleLoader( "Service", PathFinder.getServiceSection,
                             RequestHandler, moduleSuffix = "Handler" )
    elif componentType == 'executor':
      loader = ModuleLoader( "Executor", PathFinder.getExecutorSection, ExecutorModule )
    else:
      return S_ERROR( 'Unknown component type %s' % componentType )

    return loader.loadModule( "%s/%s" % ( system, module ) )

  def checkComponentSoftware( self, componentType, system, component, extensions ):
    """
    Check the component software
    """
    result = self.getSoftwareComponents( extensions )
    if not result['OK']:
      return result
    softComp = result[ 'Value' ]

    result = self._getSectionName( componentType )
    if not result[ 'OK' ]:
      return result

    try:
      softDict = softComp[ result[ 'Value' ] ]
    except KeyError, e:
      return S_ERROR( 'Unknown component type %s' % componentType )

    if system in softDict and component in softDict[system]:
      return S_OK()

    return S_ERROR( 'Unknown Component %s/%s' % ( system, component ) )

  def runsvctrlComponent( self, system, component, mode ):
    """
    Execute runsvctrl and check status of the specified component
    """
    if not mode in ['u', 'd', 'o', 'p', 'c', 'h', 'a', 'i', 'q', '1', '2', 't', 'k', 'x', 'e']:
      return S_ERROR( 'Unknown runsvctrl mode "%s"' % mode )

    startCompDirs = glob.glob( os.path.join( self.startDir, '%s_%s' % ( system, component ) ) )
    # Make sure that the Configuration server restarts first and the SystemAdmin restarts last
    tmpList = list( startCompDirs )
    for comp in tmpList:
      if "Framework_SystemAdministrator" in comp:
        startCompDirs.append( startCompDirs.pop( startCompDirs.index( comp ) ) )
      if "Configuration_Server" in comp:
        startCompDirs.insert( 0, startCompDirs.pop( startCompDirs.index( comp ) ) )
    startCompList = [ [k] for k in startCompDirs]
    for startComp in startCompList:
      result = self.execCommand( 0, ['runsvctrl', mode] + startComp )
      if not result['OK']:
        return result
      time.sleep( 2 )

    # Check the runsv status
    if system == '*' or component == '*':
      time.sleep( 10 )

    # Final check
    result = self.getStartupComponentStatus( [( system, component )] )
    if not result['OK']:
      gLogger.error( 'Failed to start the component %s %s' %(system, component) )
      return S_ERROR( 'Failed to start the component' )

    return result

  def getLogTail( self, system, component, length = 100 ):
    """
    Get the tail of the component log file
    """
    retDict = {}
    for startCompDir in glob.glob( os.path.join( self.startDir, '%s_%s' % ( system, component ) ) ):
      compName = os.path.basename( startCompDir )
      logFileName = os.path.join( startCompDir, 'log', 'current' )
      if not os.path.exists( logFileName ):
        retDict[compName] = 'No log file found'
      else:
        logFile = open( logFileName, 'r' )
        lines = [ line.strip() for line in logFile.readlines() ]
        logFile.close()

        if len( lines ) < length:
          retDict[compName] = '\n'.join( lines )
        else:
          retDict[compName] = '\n'.join( lines[-length:] )

    return S_OK( retDict )

  def setupSite( self, scriptCfg, cfg = None ):
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
        installCfg.loadFromFile( cfg )

        for section in ['DIRAC', 'LocalSite', cfgInstallSection]:
          if installCfg.isSection( section ):
            diracCfg.createNewSection( section, contents = installCfg[section] )

        if self.instancePath != self.basePath:
          if not diracCfg.isSection( 'LocalSite' ):
            diracCfg.createNewSection( 'LocalSite' )
          diracCfg.setOption( cfgPath( 'LocalSite', 'InstancePath' ), self.instancePath )

        self._addCfgToDiracCfg( diracCfg, verbose = True )
      except Exception:
        error = 'Failed to load %s' % cfg
        gLogger.exception( error )
        if self.exitOnError:
          DIRAC.exit( -1 )
        return S_ERROR( error )

    # Now get the necessary info from self.localCfg
    setupSystems = self.localCfg.getOption( cfgInstallPath( 'Systems' ), ['Configuration', 'Framework'] )
    installMySQLFlag = self.localCfg.getOption( cfgInstallPath( 'InstallMySQL' ), False )
    setupDatabases = self.localCfg.getOption( cfgInstallPath( 'Databases' ), [] )
    setupServices = [ k.split( '/' ) for k in self.localCfg.getOption( cfgInstallPath( 'Services' ), [] ) ]
    setupAgents = [ k.split( '/' ) for k in self.localCfg.getOption( cfgInstallPath( 'Agents' ), [] ) ]
    setupExecutors = [ k.split( '/' ) for k in self.localCfg.getOption( cfgInstallPath( 'Executors' ), [] ) ]
    setupWeb = self.localCfg.getOption( cfgInstallPath( 'WebPortal' ), False )
    setupWebApp = self.localCfg.getOption( cfgInstallPath( 'WebApp' ), False )
    setupConfigurationMaster = self.localCfg.getOption( cfgInstallPath( 'ConfigurationMaster' ), False )
    setupPrivateConfiguration = self.localCfg.getOption( cfgInstallPath( 'PrivateConfiguration' ), False )
    setupConfigurationName = self.localCfg.getOption( cfgInstallPath( 'ConfigurationName' ), self.setup )
    setupAddConfiguration = self.localCfg.getOption( cfgInstallPath( 'AddConfiguration' ), True )

    for serviceTuple in setupServices:
      error = ''
      if len( serviceTuple ) != 2:
        error = 'Wrong service specification: system/service'
      # elif serviceTuple[0] not in setupSystems:
      #   error = 'System %s not available' % serviceTuple[0]
      if error:
        if self.exitOnError:
          gLogger.error( error )
          DIRAC.exit( -1 )
        return S_ERROR( error )
      serviceSysInstance = serviceTuple[0]
      if not serviceSysInstance in setupSystems:
        setupSystems.append( serviceSysInstance )

    for agentTuple in setupAgents:
      error = ''
      if len( agentTuple ) != 2:
        error = 'Wrong agent specification: system/agent'
      # elif agentTuple[0] not in setupSystems:
      #   error = 'System %s not available' % agentTuple[0]
      if error:
        if self.exitOnError:
          gLogger.error( error )
          DIRAC.exit( -1 )
        return S_ERROR( error )
      agentSysInstance = agentTuple[0]
      if not agentSysInstance in setupSystems:
        setupSystems.append( agentSysInstance )

    for executorTuple in setupExecutors:
      error = ''
      if len( executorTuple ) != 2:
        error = 'Wrong executor specification: system/executor'
      if error:
        if self.exitOnError:
          gLogger.error( error )
          DIRAC.exit( -1 )
        return S_ERROR( error )
      executorSysInstance = executorTuple[0]
      if not executorSysInstance in setupSystems:
        setupSystems.append( executorSysInstance )

    # And to find out the available extensions
    result = self.getExtensions()
    if not result['OK']:
      return result
    extensions = [ k.replace( 'DIRAC', '' ) for k in result['Value']]

    # Make sure the necessary directories are there
    if self.basePath != self.instancePath:
      mkDir(self.instancePath)

      instanceEtcDir = os.path.join( self.instancePath, 'etc' )
      etcDir = os.path.dirname( self.cfgFile )
      if not os.path.exists( instanceEtcDir ):
        mkLink( etcDir, instanceEtcDir )

      if os.path.realpath( instanceEtcDir ) != os.path.realpath( etcDir ):
        error = 'Instance etc (%s) is not the same as DIRAC etc (%s)' % ( instanceEtcDir, etcDir )
        if self.exitOnError:
          gLogger.error( error )
          DIRAC.exit( -1 )
        return S_ERROR( error )

    # if any server or agent needs to be install we need the startup directory and runsvdir running
    if setupServices or setupAgents or setupExecutors or setupWeb:
      if not os.path.exists( self.startDir ):
        mkDir(self.startDir)
      # And need to make sure runsvdir is running
      result = self.execCommand( 0, ['ps', '-ef'] )
      if not result['OK']:
        if self.exitOnError:
          gLogger.error( 'Failed to verify runsvdir running', result['Message'] )
          DIRAC.exit( -1 )
        return S_ERROR( result['Message'] )
      processList = result['Value'][1].split( '\n' )
      cmd = 'runsvdir %s' % self.startDir
      cmdFound = False
      for process in processList:
        if process.find( cmd ) != -1:
          cmdFound = True
      if not cmdFound:
        gLogger.notice( 'Starting runsvdir ...' )
        os.system( "runsvdir %s 'log:  DIRAC runsv' &" % self.startDir )

    if ['Configuration', 'Server'] in setupServices and setupConfigurationMaster:
      # This server hosts the Master of the CS
      from DIRAC.ConfigurationSystem.Client.ConfigurationData import gConfigurationData
      gLogger.notice( 'Installing Master Configuration Server' )
      cfg = self.__getCfg( cfgPath( 'DIRAC', 'Setups', self.setup ), 'Configuration', self.instance )
      self._addCfgToDiracCfg( cfg )
      cfg = self.__getCfg( cfgPath( 'DIRAC', 'Configuration' ), 'Master' , 'yes' )
      cfg.setOption( cfgPath( 'DIRAC', 'Configuration', 'Name' ) , setupConfigurationName )

      serversCfgPath = cfgPath( 'DIRAC', 'Configuration', 'Servers' )
      if not self.localCfg.getOption( serversCfgPath , [] ):
        serverUrl = 'dips://%s:9135/Configuration/Server' % self.host
        cfg.setOption( serversCfgPath, serverUrl )
        gConfigurationData.setOptionInCFG( serversCfgPath, serverUrl )
      instanceOptionPath = cfgPath( 'DIRAC', 'Setups', self.setup )
      instanceCfg = self.__getCfg( instanceOptionPath, 'Configuration', self.instance )
      cfg = cfg.mergeWith( instanceCfg )
      self._addCfgToDiracCfg( cfg )

      result = self.getComponentCfg( 'service', 'Configuration', 'Server', self.instance, extensions, addDefaultOptions = True )
      if not result['OK']:
        if self.exitOnError:
          DIRAC.exit( -1 )
        else:
          return result
      compCfg = result['Value']
      cfg = cfg.mergeWith( compCfg )
      gConfigurationData.mergeWithLocal( cfg )

      self.addDefaultOptionsToComponentCfg( 'service', 'Configuration', 'Server', [] )
      if installCfg:
        centralCfg = self._getCentralCfg( installCfg )
      else:
        centralCfg = self._getCentralCfg( self.localCfg )
      self._addCfgToLocalCS( centralCfg )
      self.setupComponent( 'service', 'Configuration', 'Server', [], checkModule = False )
      MonitoringUtilities.monitorInstallation( 'service', 'Configuration', 'Server' )
      self.runsvctrlComponent( 'Configuration', 'Server', 't' )

      while ['Configuration', 'Server'] in setupServices:
        setupServices.remove( ['Configuration', 'Server'] )

    time.sleep( 5 )

    # Now need to check if there is valid CS to register the info
    result = scriptCfg.enableCS()
    if not result['OK']:
      if self.exitOnError:
        DIRAC.exit( -1 )
      return result

    cfgClient = CSAPI()
    if not cfgClient.initialize():
      error = 'Configuration Server not defined'
      if self.exitOnError:
        gLogger.error( error )
        DIRAC.exit( -1 )
      return S_ERROR( error )

    # We need to make sure components are connecting to the Master CS, that is the only one being update
    from DIRAC import gConfig
    localServers = self.localCfg.getOption( cfgPath( 'DIRAC', 'Configuration', 'Servers' ) )
    masterServer = gConfig.getValue( cfgPath( 'DIRAC', 'Configuration', 'MasterServer' ), '' )
    initialCfg = self.__getCfg( cfgPath( 'DIRAC', 'Configuration' ), 'Servers' , localServers )
    masterCfg = self.__getCfg( cfgPath( 'DIRAC', 'Configuration' ), 'Servers' , masterServer )
    self._addCfgToDiracCfg( masterCfg )

    # 1.- Setup the instances in the CS
    # If the Configuration Server used is not the Master, it can take some time for this
    # info to be propagated, this may cause the later self.setup to fail
    if setupAddConfiguration:
      gLogger.notice( 'Registering System instances' )
      for system in setupSystems:
        self.addSystemInstance( system, self.instance, self.setup, True )
      for system, service in setupServices:
        if not self.addDefaultOptionsToCS( None, 'service', system, service, extensions, overwrite = True )['OK']:
          # If we are not allowed to write to the central CS, add the configuration to the local file
          self.addDefaultOptionsToComponentCfg( 'service', system, service, extensions )
      for system, agent in setupAgents:
        if not self.addDefaultOptionsToCS( None, 'agent', system, agent, extensions, overwrite = True )['OK']:
          # If we are not allowed to write to the central CS, add the configuration to the local file
          self.addDefaultOptionsToComponentCfg( 'agent', system, agent, extensions )
      for system, executor in setupExecutors:
        if not self.addDefaultOptionsToCS( None, 'executor', system, executor, extensions, overwrite = True )['OK']:
          # If we are not allowed to write to the central CS, add the configuration to the local file
          self.addDefaultOptionsToComponentCfg( 'executor', system, executor, extensions )
    else:
      gLogger.warn( 'Configuration parameters definition is not requested' )

    if ['Configuration', 'Server'] in setupServices and setupPrivateConfiguration:
      cfg = self.__getCfg( cfgPath( 'DIRAC', 'Configuration' ), 'AutoPublish' , 'no' )
      self._addCfgToDiracCfg( cfg )

    # 2.- Check if MySQL is to be installed
    if installMySQLFlag:
      gLogger.notice( 'Installing MySQL' )
      self.getMySQLPasswords()
      self.installMySQL()

    # 3.- Install requested Databases
    # if MySQL is not installed locally, we assume a host is given
    if setupDatabases:
      result = self.getDatabases()
      if not result['OK']:
        if self.exitOnError:
          gLogger.error( 'Failed to get databases', result['Message'] )
          DIRAC.exit( -1 )
        return result
      installedDatabases = result['Value']
      result = self.getAvailableDatabases( CSGlobals.getCSExtensions() )
      if not result[ 'OK' ]:
        return result
      dbDict = result['Value']

      for dbName in setupDatabases:
        if dbName not in installedDatabases:
          result = self.installDatabase( dbName )
          if not result['OK']:
            gLogger.error( result['Message'] )
            DIRAC.exit( -1 )
          extension, system = result['Value']
          gLogger.notice( 'Database %s from %s/%s installed' % ( dbName, extension, system ) )
        else:
          gLogger.notice( 'Database %s already installed' % dbName )

        dbSystem = dbDict[dbName]['System']
        result = self.addDatabaseOptionsToCS( None, dbSystem, dbName, overwrite = True )
        if not result['OK']:
          gLogger.error( 'Database %s CS registration failed: %s' % ( dbName, result['Message'] ) )


    if self.mysqlPassword:
      if not self._addMySQLToDiracCfg():
        error = 'Failed to add MySQL user password to local configuration'
        if self.exitOnError:
          gLogger.error( error )
          DIRAC.exit( -1 )
        return S_ERROR( error )

    # 4.- Then installed requested services
    for system, service in setupServices:
      result = self.setupComponent( 'service', system, service, extensions, monitorFlag = False )
      if not result['OK']:
        gLogger.error( result['Message'] )
        continue
      result = MonitoringUtilities.monitorInstallation( 'service', system, service )
      if not result['OK']:
        gLogger.error( 'Error registering installation into database: %s' % result[ 'Message' ] )

    # 5.- Now the agents
    for system, agent in setupAgents:
      result = self.setupComponent( 'agent', system, agent, extensions, monitorFlag = False )
      if not result['OK']:
        gLogger.error( result['Message'] )
        continue
      result = MonitoringUtilities.monitorInstallation( 'agent', system, agent )
      if not result['OK']:
        gLogger.error( 'Error registering installation into database: %s' % result[ 'Message' ] )

    # 6.- Now the executors
    for system, executor in setupExecutors:
      result = self.setupComponent( 'executor', system, executor, extensions, monitorFlag = False )
      if not result['OK']:
        gLogger.error( result['Message'] )
        continue
      result = MonitoringUtilities.monitorInstallation( 'executor', system, executor )
      if not result['OK']:
        gLogger.error( 'Error registering installation into database: %s' % result[ 'Message' ] )

    # 7.- And finally the Portal
    if setupWeb:
      if setupWebApp:
        self.setupNewPortal()
      else:
        self.setupPortal()

    if localServers != masterServer:
      self._addCfgToDiracCfg( initialCfg )
      for system, service in  setupServices:
        self.runsvctrlComponent( system, service, 't' )
      for system, agent in setupAgents:
        self.runsvctrlComponent( system, agent, 't' )
      for system, executor in setupExecutors:
        self.runsvctrlComponent( system, executor, 't' )

    return S_OK()

  def _getSectionName( self, compType ):
    """
    Returns the section name for a component in the CS
    For self.instance, the section for service is Services,
    whereas the section for agent is Agents
    """
    return S_OK( '%ss' % ( compType.title() ) )

  def _createRunitLog( self, runitCompDir ):
    self.controlDir = os.path.join( runitCompDir, 'control' )
    mkDir( self.controlDir )

    logDir = os.path.join( runitCompDir, 'log' )
    mkDir( logDir )

    logConfigFile = os.path.join( logDir, 'config' )
    with open( logConfigFile, 'w' ) as fd:
      fd.write(
  """s10000000
  n20
  """ )

    logRunFile = os.path.join( logDir, 'run' )
    with open( logRunFile, 'w' ) as fd:
      fd.write(
  """#!/bin/bash
  #
  rcfile=%(bashrc)s
  [ -e $rcfile ] && source $rcfile
  #
  exec svlogd .

  """ % { 'bashrc' : os.path.join( self.instancePath, 'bashrc' ) } )

    os.chmod( logRunFile, self.gDefaultPerms )

  def installComponent( self, componentType, system, component, extensions, componentModule = '', checkModule = True ):
    """
    Install runit directory for the specified component
    """
    # Check if the component is already installed
    runitCompDir = os.path.join( self.runitDir, system, component )
    if os.path.exists( runitCompDir ):
      msg = "%s %s_%s already installed" % ( componentType, system, component )
      gLogger.notice( msg )
      return S_OK( runitCompDir )

    # Check that the software for the component is installed
    # Any "Load" or "Module" option in the configuration defining what modules the given "component"
    # needs to load will be taken care of by self.checkComponentModule.
    if checkModule:
      cModule = componentModule
      if not cModule:
        cModule = component
      result = self.checkComponentModule( componentType, system, cModule )
      if not result['OK']:
        if not self.checkComponentSoftware( componentType, system, cModule, extensions )['OK'] and componentType != 'executor':
          error = 'Software for %s %s/%s is not installed' % ( componentType, system, component )
          if self.exitOnError:
            gLogger.error( error )
            DIRAC.exit( -1 )
          return S_ERROR( error )

    gLogger.notice( 'Installing %s %s/%s' % ( componentType, system, component ) )

    # Retrieve bash variables to be set
    result = gConfig.getOption( 'DIRAC/Setups/%s/%s' % ( CSGlobals.getSetup(), system ) )
    if not result[ 'OK' ]:
      return result
    self.instance = result[ 'Value' ]

    specialOptions = {}
    if componentModule:
      specialOptions['Module'] = componentModule
    result = self.getComponentCfg( componentType, system, component, self.instance, extensions,
                                   specialOptions = specialOptions )
    if not result[ 'OK' ]:
      return result
    compCfg = result[ 'Value' ]

    result = self._getSectionName( componentType )
    if not result[ 'OK' ]:
      return result
    section = result[ 'Value' ]

    bashVars = ''
    if compCfg.isSection( 'Systems/%s/%s/%s/%s/Environment' % ( system, self.instance, section, component ) ):
      dictionary = compCfg.getAsDict()
      bashSection = dictionary[ 'Systems' ][ system ][ self.instance ][ section ][ component ][ 'BashVariables' ]
      for var in bashSection:
        bashVars = '%s\nexport %s=%s' % ( bashVars, var, bashSection[ var ] )

    # Now do the actual installation
    try:
      componentCfg = os.path.join( self.linkedRootPath, 'etc', '%s_%s.cfg' % ( system, component ) )
      if not os.path.exists( componentCfg ):
        fd = open( componentCfg, 'w' )
        fd.close()

      self._createRunitLog( runitCompDir )

      runFile = os.path.join( runitCompDir, 'run' )
      fd = open( runFile, 'w' )
      fd.write(
  """#!/bin/bash
  rcfile=%(bashrc)s
  [ -e $rcfile ] && source $rcfile
  #
  exec 2>&1
  #
  [ "%(componentType)s" = "agent" ] && renice 20 -p $$
  #%(bashVariables)s
  #
  exec python $DIRAC/DIRAC/Core/scripts/dirac-%(componentType)s.py %(system)s/%(component)s %(componentCfg)s < /dev/null
  """ % {'bashrc': os.path.join( self.instancePath, 'bashrc' ),
         'bashVariables': bashVars,
         'componentType': componentType,
         'system' : system,
         'component': component,
         'componentCfg': componentCfg } )
      fd.close()

      os.chmod( runFile, self.gDefaultPerms )

      cTypeLower = componentType.lower()
      if cTypeLower == 'agent' or cTypeLower == 'consumer':
        stopFile = os.path.join( runitCompDir, 'control', 't' ) # This is, e.g., /opt/dirac/runit/WorkfloadManagementSystem/Matcher/control/t
        controlDir = self.runitDir.replace('runit', 'control') # This is, e.g., /opt/dirac/control/WorkfloadManagementSystem/Matcher/
        with open( stopFile, 'w' ) as fd:
          fd.write( """#!/bin/bash
echo %(controlDir)s/%(system)s/%(component)s/stop_%(type)s
touch %(controlDir)s/%(system)s/%(component)s/stop_%(type)s
""" % { 'controlDir': controlDir,
        'system' : system,
        'component': component,
        'type': cTypeLower } )

        os.chmod( stopFile, self.gDefaultPerms )

    except Exception:
      error = 'Failed to prepare self.setup for %s %s/%s' % ( componentType, system, component )
      gLogger.exception( error )
      if self.exitOnError:
        DIRAC.exit( -1 )
      return S_ERROR( error )

    result = self.execCommand( 5, [runFile] )

    gLogger.notice( result['Value'][1] )

    return S_OK( runitCompDir )

  def setupComponent( self, componentType, system, component, extensions,
                      componentModule = '', checkModule = True, monitorFlag = True ):
    """
    Install and create link in startup
    """
    result = self.installComponent( componentType, system, component, extensions, componentModule, checkModule )
    if not result['OK']:
      return result

    # Create the startup entry now
    runitCompDir = result['Value']
    startCompDir = os.path.join( self.startDir, '%s_%s' % ( system, component ) )
    mkDir(self.startDir)
    if not os.path.lexists( startCompDir ):
      gLogger.notice( 'Creating startup link at', startCompDir )
      mkLink( runitCompDir, startCompDir )
      time.sleep( 10 )

    # Check the runsv status
    start = time.time()
    while ( time.time() - 20 ) < start:
      result = self.getStartupComponentStatus( [ ( system, component )] )
      if not result['OK']:
        continue
      if result['Value'] and result['Value']['%s_%s' % ( system, component )]['RunitStatus'] == "Run":
        break
      time.sleep( 1 )

    # Final check
    result = self.getStartupComponentStatus( [( system, component )] )
    if not result['OK']:
      return S_ERROR( 'Failed to start the component %s_%s' % ( system, component ) )

    resDict = {}
    resDict['ComponentType'] = componentType
    resDict['RunitStatus'] = result['Value']['%s_%s' % ( system, component )]['RunitStatus']

    return S_OK( resDict )

  def unsetupComponent( self, system, component ):
    """
    Remove link from startup
    """
    for startCompDir in glob.glob( os.path.join( self.startDir, '%s_%s' % ( system, component ) ) ):
      try:
        os.unlink( startCompDir )
      except Exception:
        gLogger.exception()

    return S_OK()

  def uninstallComponent( self, system, component, removeLogs ):
    """
    Remove startup and runit directories
    """
    result = self.runsvctrlComponent( system, component, 'd' )
    if not result['OK']:
      pass

    result = self.unsetupComponent( system, component )

    if removeLogs:
      for runitCompDir in glob.glob( os.path.join( self.runitDir, system, component ) ):
        try:
          shutil.rmtree( runitCompDir )
        except Exception:
          gLogger.exception()

    result = self.removeComponentOptionsFromCS( system, component )
    if not result [ 'OK' ]:
      return result

    return S_OK()

  def installPortal( self ):
    """
    Install runit directories for the Web Portal
    """
    # Check that the software for the Web Portal is installed
    error = ''
    webDir = os.path.join( self.linkedRootPath, 'Web' )
    if not os.path.exists( webDir ):
      error = 'Web extension not installed at %s' % webDir
      if self.exitOnError:
        gLogger.error( error )
        DIRAC.exit( -1 )
      return S_ERROR( error )

    # First the lighthttpd server

    # Check if the component is already installed
    runitHttpdDir = os.path.join( self.runitDir, 'Web', 'httpd' )
    runitPasterDir = os.path.join( self.runitDir, 'Web', 'paster' )

    if os.path.exists( runitHttpdDir ):
      msg = "lighthttpd already installed"
      gLogger.notice( msg )
    else:
      gLogger.notice( 'Installing Lighttpd' )
      # Now do the actual installation
      try:
        self._createRunitLog( runitHttpdDir )
        runFile = os.path.join( runitHttpdDir, 'run' )
        fd = open( runFile, 'w' )
        fd.write(
  """#!/bin/bash
  rcfile=%(bashrc)s
  [ -e $rcfile ] && source $rcfile
  #
  exec 2>&1
  #
  exec lighttpdSvc.sh < /dev/null
  """ % {'bashrc': os.path.join( self.instancePath, 'bashrc' ), } )
        fd.close()

        os.chmod( runFile, self.gDefaultPerms )
      except Exception:
        error = 'Failed to prepare self.setup for lighttpd'
        gLogger.exception( error )
        if self.exitOnError:
          DIRAC.exit( -1 )
        return S_ERROR( error )

      result = self.execCommand( 5, [runFile] )
      gLogger.notice( result['Value'][1] )

    # Second the Web portal

    # Check if the component is already installed
    if os.path.exists( runitPasterDir ):
      msg = "Web Portal already installed"
      gLogger.notice( msg )
    else:
      gLogger.notice( 'Installing Web Portal' )
      # Now do the actual installation
      try:
        self._createRunitLog( runitPasterDir )
        runFile = os.path.join( runitPasterDir, 'run' )
        fd = open( runFile, 'w' )
        fd.write(
  """#!/bin/bash
  rcfile=%(bashrc)s
  [ -e $rcfile ] && source $rcfile
  #
  exec 2>&1
  #
  cd %(DIRAC)s/Web
  exec paster serve --reload production.ini < /dev/null
  """ % {'bashrc': os.path.join( self.instancePath, 'bashrc' ),
         'DIRAC': self.linkedRootPath} )
        fd.close()

        os.chmod( runFile, self.gDefaultPerms )
      except Exception:
        error = 'Failed to prepare self.setup for Web Portal'
        gLogger.exception( error )
        if self.exitOnError:
          DIRAC.exit( -1 )
        return S_ERROR( error )

      result = self.execCommand( 5, [runFile] )
      gLogger.notice( result['Value'][1] )

    return S_OK( [runitHttpdDir, runitPasterDir] )

  def setupPortal( self ):
    """
    Install and create link in startup
    """
    result = self.installPortal()
    if not result['OK']:
      return result

    # Create the startup entries now
    runitCompDir = result['Value']
    startCompDir = [ os.path.join( self.startDir, 'Web_httpd' ),
                     os.path.join( self.startDir, 'Web_paster' ) ]

    mkDir( self.startDir )

    for i in range( 2 ):
      if not os.path.lexists( startCompDir[i] ):
        gLogger.notice( 'Creating startup link at', startCompDir[i] )
        mkLink( runitCompDir[i], startCompDir[i] )
        time.sleep( 1 )
    time.sleep( 5 )

    # Check the runsv status
    start = time.time()
    while ( time.time() - 10 ) < start:
      result = self.getStartupComponentStatus( [ ( 'Web', 'httpd' ), ( 'Web', 'paster' ) ] )
      if not result['OK']:
        return S_ERROR( 'Failed to start the Portal' )
      if result['Value'] and \
         result['Value']['%s_%s' % ( 'Web', 'httpd' )]['RunitStatus'] == "Run" and \
         result['Value']['%s_%s' % ( 'Web', 'paster' )]['RunitStatus'] == "Run" :
        break
      time.sleep( 1 )

    # Final check
    return self.getStartupComponentStatus( [ ( 'Web', 'httpd' ), ( 'Web', 'paster' ) ] )

  def setupNewPortal( self ):
    """
    Install and create link in startup
    """
    result = self.installNewPortal()
    if not result['OK']:
      return result

    # Create the startup entries now
    runitCompDir = result['Value']
    startCompDir = os.path.join( self.startDir, 'Web_WebApp' )


    mkDir( self.startDir )

    mkLink( runitCompDir, startCompDir )

    time.sleep( 5 )

    # Check the runsv status
    start = time.time()
    while ( time.time() - 10 ) < start:
      result = self.getStartupComponentStatus( [( 'Web', 'WebApp' )] )
      if not result['OK']:
        return S_ERROR( 'Failed to start the Portal' )
      if result['Value'] and \
         result['Value']['%s_%s' % ( 'Web', 'WebApp' )]['RunitStatus'] == "Run":
        break
      time.sleep( 1 )

    # Final check
    return self.getStartupComponentStatus( [ ( 'Web', 'WebApp' ) ] )

  def installNewPortal( self ):
    """
    Install runit directories for the Web Portal
    """

    result = self.execCommand( False, ["pip", "install", "tornado"] )
    if not result['OK']:
      error = "Tornado can not be installed:%s" % result['Value']
      gLogger.error( error )
      DIRAC.exit( -1 )
      return error
    else:
      gLogger.notice( "Tornado is installed successfully!" )

    # Check that the software for the Web Portal is installed
    error = ''
    webDir = os.path.join( self.linkedRootPath, 'WebAppDIRAC' )
    if not os.path.exists( webDir ):
      error = 'WebApp extension not installed at %s' % webDir
      if self.exitOnError:
        gLogger.error( error )
        DIRAC.exit( -1 )
      return S_ERROR( error )

    # compile the JS code
    prodMode = ""
    webappCompileScript = os.path.join( self.linkedRootPath, "WebAppDIRAC/scripts", "dirac-webapp-compile.py" )
    if os.path.isfile( webappCompileScript ):
      os.chmod( webappCompileScript , self.gDefaultPerms )
      gLogger.notice( "Executing %s..." % webappCompileScript )
      if os.system( "python '%s' > '%s.out' 2> '%s.err'" % ( webappCompileScript,
                                                             webappCompileScript,
                                                             webappCompileScript ) ):
        gLogger.error( "Compile script %s failed. Check %s.err" % ( webappCompileScript,
                                                                    webappCompileScript ) )
      else:
        prodMode = "-p"

    # Check if the component is already installed
    runitWebAppDir = os.path.join( self.runitDir, 'Web', 'WebApp' )

    # Check if the component is already installed
    if os.path.exists( runitWebAppDir ):
      msg = "Web Portal already installed"
      gLogger.notice( msg )
    else:
      gLogger.notice( 'Installing Web Portal' )
      # Now do the actual installation
      try:
        self._createRunitLog( runitWebAppDir )
        runFile = os.path.join( runitWebAppDir, 'run' )
        with open( runFile, 'w' ) as fd:
          fd.write(
  """#!/bin/bash
  rcfile=%(bashrc)s
  [ -e $rcfile ] && source $rcfile
  #
  exec 2>&1
  #
  exec python %(DIRAC)s/WebAppDIRAC/scripts/dirac-webapp-run.py %(prodMode)s < /dev/null
  """ % {'bashrc': os.path.join( self.instancePath, 'bashrc' ),
         'DIRAC': self.linkedRootPath,
         'prodMode':prodMode} )

        os.chmod( runFile, self.gDefaultPerms )
      except Exception:
        error = 'Failed to prepare self.setup for Web Portal'
        gLogger.exception( error )
        if self.exitOnError:
          DIRAC.exit( -1 )
        return S_ERROR( error )

      result = self.execCommand( 5, [runFile] )
      gLogger.notice( result['Value'][1] )

    return S_OK( runitWebAppDir )

  def fixMySQLScripts( self, startupScript = None ):
    """
    Edit MySQL scripts to point to desired locations for db and my.cnf
    """
    if startupScript is None:
      startupScript = self.mysqlStartupScript

    gLogger.verbose( 'Updating:', startupScript )
    try:
      fd = open( startupScript, 'r' )
      orgLines = fd.readlines()
      fd.close()
      fd = open( startupScript, 'w' )
      for line in orgLines:
        if line.find( 'export HOME' ) == 0:
          continue
        if line.find( 'datadir=' ) == 0:
          line = 'datadir=%s\n' % self.mysqlDbDir
          gLogger.debug( line )
          line += 'export HOME=%s\n' % self.mysqlDir
        if line.find( 'basedir=' ) == 0:
          platform = getPlatformString()
          line = 'basedir=%s\n' % os.path.join( rootPath, platform )
        if line.find( 'extra_args=' ) == 0:
          line = 'extra_args="-n"\n'
        if line.find( '$bindir/mysqld_safe --' ) >= 0 and not ' --defaults-file' in line:
          line = line.replace( 'mysqld_safe', 'mysqld_safe --defaults-file=$HOME/.my.cnf' )
        fd.write( line )
      fd.close()
    except Exception:
      error = 'Failed to Update MySQL startup script'
      gLogger.exception( error )
      if self.exitOnError:
        DIRAC.exit( -1 )
      return S_ERROR( error )

    return S_OK()


  def mysqlInstalled( self, doNotExit = False ):
    """
    Check if MySQL is already installed
    """

    if os.path.exists( self.mysqlDbDir ) or os.path.exists( self.mysqlLogDir ):
      return S_OK()
    if doNotExit:
      return S_ERROR()

    error = 'MySQL not properly Installed'
    gLogger.error( error )
    if self.exitOnError:
      DIRAC.exit( -1 )
    return S_ERROR( error )

  def getMySQLPasswords( self ):
    """
    Get MySQL passwords from local configuration or prompt
    """
    import getpass
    if not self.mysqlRootPwd:
      self.mysqlRootPwd = getpass.getpass( 'MySQL root password: ' )

    if not self.mysqlPassword:
      # Take it if it is already defined
      self.mysqlPassword = self.localCfg.getOption( '/Systems/Databases/Password', '' )
      if not self.mysqlPassword:
        self.mysqlPassword = getpass.getpass( 'MySQL Dirac password: ' )

    return S_OK()

  def setMySQLPasswords( self, root = '', dirac = '' ):
    """
    Set MySQL passwords
    """
    if root:
      self.mysqlRootPwd = root
    if dirac:
      self.mysqlPassword = dirac

    return S_OK()

  def startMySQL( self ):
    """
    Start MySQL server
    """
    result = self.mysqlInstalled()
    if not result['OK']:
      return result
    return self.execCommand( 0, [self.mysqlStartupScript, 'start'] )

  def stopMySQL( self ):
    """
    Stop MySQL server
    """
    result = self.mysqlInstalled()
    if not result['OK']:
      return result
    return self.execCommand( 0, [self.mysqlStartupScript, 'stop'] )

  def installMySQL( self ):
    """
    Attempt an installation of MySQL
    mode:

      -Master
      -Slave
      -None

    """
    self.fixMySQLScripts()

    if self.mysqlInstalled( doNotExit = True )['OK']:
      gLogger.notice( 'MySQL already installed' )
      return S_OK()

    if self.mysqlMode.lower() not in [ '', 'master', 'slave' ]:
      error = 'Unknown MySQL server Mode'
      if self.exitOnError:
        gLogger.fatal( error, self.mysqlMode )
        DIRAC.exit( -1 )
      gLogger.error( error, self.mysqlMode )
      return S_ERROR( error )

    if self.mysqlHost:
      gLogger.notice( 'Installing MySQL server at', self.mysqlHost )

    if self.mysqlMode:
      gLogger.notice( 'This is a MySQl %s server' % self.mysqlMode )

    mkDir( self.mysqlDbDir )
    mkDir( self.mysqlLogDir )

    try:
      with open( self.mysqlMyOrg, 'r' ) as fd:
        myOrg = fd.readlines()

      with open( self.mysqlMyCnf, 'w' ) as fd:
        for line in myOrg:
          if line.find( '[mysqld]' ) == 0:
            line += '\n'.join( [ 'innodb_file_per_table', '' ] )
          elif line.find( 'innodb_log_arch_dir' ) == 0:
            line = ''
          elif line.find( 'innodb_data_file_path' ) == 0:
            line = line.replace( '2000M', '200M' )
          elif line.find( 'server-id' ) == 0 and self.mysqlMode.lower() == 'master':
            # MySQL Configuration for Master Server
            line = '\n'.join( ['server-id = 1',
                               '# DIRAC Master-Server',
                               'sync-binlog = 1',
                               'replicate-ignore-table = mysql.MonitorData',
                               '# replicate-ignore-db=db_name',
                               'log-bin = mysql-bin',
                               'log-slave-updates', '' ] )
          elif line.find( 'server-id' ) == 0 and self.mysqlMode.lower() == 'slave':
            # MySQL Configuration for Slave Server
            line = '\n'.join( ['server-id = %s' % int( time.time() ),
                               '# DIRAC Slave-Server',
                               'sync-binlog = 1',
                               'replicate-ignore-table = mysql.MonitorData',
                               '# replicate-ignore-db=db_name',
                               'log-bin = mysql-bin',
                               'log-slave-updates', '' ] )
          elif line.find( '/opt/dirac/mysql' ) > -1:
            line = line.replace( '/opt/dirac/mysql', self.mysqlDir )

          if self.mysqlSmallMem:
            if line.find( 'innodb_buffer_pool_size' ) == 0:
              line = 'innodb_buffer_pool_size = 200M\n'
          elif self.mysqlLargeMem:
            if line.find( 'innodb_buffer_pool_size' ) == 0:
              line = 'innodb_buffer_pool_size = 10G\n'

          fd.write( line )
    except Exception:
      error = 'Can not create my.cnf'
      gLogger.exception( error )
      if self.exitOnError:
        DIRAC.exit( -1 )
      return S_ERROR( error )

    gLogger.notice( 'Initializing MySQL...' )
    platform = getPlatformString()
    baseDir = os.path.join( rootPath, platform )
    result = self.execCommand( 0, ['mysql_install_db',
                                   '--defaults-file=%s' % self.mysqlMyCnf,
                                   '--baseDir=%s' % baseDir,
                                   '--datadir=%s' % self.mysqlDbDir ] )
    if not result['OK']:
      return result

    gLogger.notice( 'Starting MySQL...' )
    result = self.startMySQL()
    if not result['OK']:
      return result

    gLogger.notice( 'Setting MySQL root password' )
    result = self.execCommand( 0, ['mysqladmin', '-u', self.mysqlRootUser, 'password', self.mysqlRootPwd] )
    if not result['OK']:
      return result

    # MySQL tends to define root@host user rather than root@host.domain
    hostName = self.mysqlHost.split( '.' )[0]
    result = self.execMySQL( "UPDATE user SET Host='%s' WHERE Host='%s'" % ( self.mysqlHost, hostName ),
                             localhost = True )
    if not result['OK']:
      return result
    result = self.execMySQL( "FLUSH PRIVILEGES" )
    if not result['OK']:
      return result

    if self.mysqlHost and socket.gethostbyname( self.mysqlHost ) != '127.0.0.1' :
      result = self.execCommand( 0, ['mysqladmin', '-u', self.mysqlRootUser, '-h', self.mysqlHost, 'password', self.mysqlRootPwd] )
      if not result['OK']:
        return result

    result = self.execMySQL( "DELETE from user WHERE Password=''", localhost = True )

    if not self._addMySQLToDiracCfg():
      return S_ERROR( 'Failed to add MySQL user password to local configuration' )

    return S_OK()

  def getMySQLStatus( self ):
    """
    Get the status of the MySQL database installation
    """
    result = self.execCommand( 0, ['mysqladmin', 'status' ] )
    if not result['OK']:
      return result
    output = result['Value'][1]
    _d1, uptime, nthreads, nquestions, nslow, nopens, nflash, nopen, nqpersec = output.split( ':' )
    resDict = {}
    resDict['UpTime'] = uptime.strip().split()[0]
    resDict['NumberOfThreads'] = nthreads.strip().split()[0]
    resDict['NumberOfQuestions'] = nquestions.strip().split()[0]
    resDict['NumberOfSlowQueries'] = nslow.strip().split()[0]
    resDict['NumberOfOpens'] = nopens.strip().split()[0]
    resDict['OpenTables'] = nopen.strip().split()[0]
    resDict['FlushTables'] = nflash.strip().split()[0]
    resDict['QueriesPerSecond'] = nqpersec.strip().split()[0]
    return S_OK( resDict )

  def getAvailableDatabases( self, extensions ):

    dbDict = {}
    for extension in extensions + ['']:
      databases = glob.glob( os.path.join( rootPath,
                                           ( '%sDIRAC' % extension ).replace( 'DIRACDIRAC', 'DIRAC' ),
                                           '*', 'DB', '*.sql' ) )
      for dbPath in databases:
        dbName = os.path.basename( dbPath ).replace( '.sql', '' )
        dbDict[dbName] = {}
        dbDict[dbName]['Extension'] = extension
        dbDict[dbName]['System'] = dbPath.split( '/' )[-3].replace( 'System', '' )

    return S_OK( dbDict )


  def getDatabases( self ):
    """
    Get the list of installed databases
    """
    result = self.execMySQL( 'SHOW DATABASES' )
    if not result['OK']:
      return result
    dbList = []
    for dbName in result['Value']:
      if not dbName[0] in ['Database', 'information_schema', 'mysql', 'test']:
        dbList.append( dbName[0] )

    return S_OK( dbList )


  def installDatabase( self, dbName ):
    """
    Install requested DB in MySQL server
    """

    if not self.mysqlRootPwd:
      rootPwdPath = cfgInstallPath( 'Database', 'RootPwd' )
      return S_ERROR( 'Missing %s in %s' % ( rootPwdPath, self.cfgFile ) )

    if not self.mysqlPassword:
      self.mysqlPassword = self.localCfg.getOption( cfgPath( 'Systems', 'Databases', 'Password' ), self.mysqlPassword )
      if not self.mysqlPassword:
        mysqlPwdPath = cfgPath( 'Systems', 'Databases', 'Password' )
        return S_ERROR( 'Missing %s in %s' % ( mysqlPwdPath, self.cfgFile ) )

    gLogger.notice( 'Installing', dbName )

    dbFile = glob.glob( os.path.join( rootPath, '*', '*', 'DB', '%s.sql' % dbName ) )

    if not dbFile:
      error = 'Database %s not found' % dbName
      gLogger.error( error )
      if self.exitOnError:
        DIRAC.exit( -1 )
      return S_ERROR( error )

    dbFile = dbFile[0]

    # just check
    result = self.execMySQL( 'SHOW STATUS' )
    if not result['OK']:
      error = 'Could not connect to MySQL server'
      gLogger.error( error )
      if self.exitOnError:
        DIRAC.exit( -1 )
      return S_ERROR( error )

    # now creating the Database
    result = self.execMySQL( 'CREATE DATABASE `%s`' % dbName )
    if not result['OK'] and not 'database exists' in result[ 'Message' ]:
      gLogger.error( 'Failed to create databases', result['Message'] )
      if self.exitOnError:
        DIRAC.exit( -1 )
      return result

    perms = "SELECT,INSERT,LOCK TABLES,UPDATE,DELETE,CREATE,DROP,ALTER," \
            "CREATE VIEW,SHOW VIEW,INDEX,TRIGGER,ALTER ROUTINE,CREATE ROUTINE"
    for cmd in ["GRANT %s ON `%s`.* TO '%s'@'localhost' IDENTIFIED BY '%s'" % ( perms, dbName, self.mysqlUser,
                                                                                self.mysqlPassword ),
                "GRANT %s ON `%s`.* TO '%s'@'%s' IDENTIFIED BY '%s'" % ( perms, dbName, self.mysqlUser,
                                                                         self.mysqlHost, self.mysqlPassword ),
                "GRANT %s ON `%s`.* TO '%s'@'%%' IDENTIFIED BY '%s'" % ( perms, dbName, self.mysqlUser,
                                                                         self.mysqlPassword ) ]:
      result = self.execMySQL( cmd )
      if not result['OK']:
        error = "Error executing '%s'" % cmd
        gLogger.error( error, result['Message'] )
        if self.exitOnError:
          DIRAC.exit( -1 )
        return S_ERROR( error )
    result = self.execMySQL( 'FLUSH PRIVILEGES' )
    if not result['OK']:
      gLogger.error( 'Failed to flush provileges', result['Message'] )
      if self.exitOnError:
        exit( -1 )
      return result

    # first getting the lines to be executed, and then execute them
    try:
      cmdLines = self._createMySQLCMDLines( dbFile )

      # We need to run one SQL cmd at once, mysql is much happier that way.
      # Create a string of commands, ignoring comment lines
      sqlString = '\n'.join( x for x in cmdLines if not x.startswith( "--" ) )

      # Now run each command (They are seperated by ;)
      # Ignore any empty ones
      cmds = [ x.strip() for x in sqlString.split( ";" ) if x.strip() ]
      for cmd in cmds:
        result = self.execMySQL( cmd, dbName )
        if not result['OK']:
          error = 'Failed to initialize Database'
          gLogger.notice( cmd )
          gLogger.error( error, result['Message'] )
          if self.exitOnError:
            DIRAC.exit( -1 )
          return S_ERROR( error )

    except Exception as e:
      gLogger.error( str( e ) )
      if self.exitOnError:
        DIRAC.exit( -1 )
      return S_ERROR( error )

    return S_OK( dbFile.split( '/' )[-4:-2] )

  def uninstallDatabase( self, gConfig_o, dbName ):
    """
    Remove a database from DIRAC
    """
    result = self.getAvailableDatabases( CSGlobals.getCSExtensions() )
    if not result[ 'OK' ]:
      return result

    dbSystem = result[ 'Value' ][ dbName ][ 'System' ]

    result = self.removeDatabaseOptionsFromCS( gConfig_o, dbSystem, dbName )
    if not result [ 'OK' ]:
      return result

    return S_OK( 'DB successfully uninstalled' )

  def _createMySQLCMDLines( self, dbFile ):
    """ Creates a list of MYSQL commands to be executed, inspecting the dbFile(s)
    """

    cmdLines = []

    fd = open( dbFile )
    dbLines = fd.readlines()
    fd.close()

    for line in dbLines:
      # Should we first source an SQL file (is this sql file an extension)?
      if line.lower().startswith( 'source' ):
        sourcedDBbFileName = line.split( ' ' )[1].replace( '\n', '' )
        gLogger.info( "Found file to source: %s" % sourcedDBbFileName )
        sourcedDBbFile = os.path.join( rootPath, sourcedDBbFileName )
        fdSourced = open( sourcedDBbFile )
        dbLinesSourced = fdSourced.readlines()
        fdSourced.close()
        for lineSourced in dbLinesSourced:
          if lineSourced.strip():
            cmdLines.append( lineSourced.strip() )

      # Creating/adding cmdLines
      else:
        if line.strip():
          cmdLines.append( line.strip() )

    return cmdLines

  def execMySQL( self, cmd, dbName = 'mysql', localhost = False ):
    """
    Execute MySQL Command
    """
    from DIRAC.Core.Utilities.MySQL import MySQL
    if not self.mysqlRootPwd:
      return S_ERROR( 'MySQL root password is not defined' )
    if dbName not in self.db:
      dbHost = self.mysqlHost
      if localhost:
        dbHost = 'localhost'
      self.db[dbName] = MySQL( dbHost, self.mysqlRootUser, self.mysqlRootPwd, dbName, self.mysqlPort )
    if not self.db[dbName]._connected:
      error = 'Could not connect to MySQL server'
      gLogger.error( error )
      if self.exitOnError:
        DIRAC.exit( -1 )
      return S_ERROR( error )
    return self.db[dbName]._query( cmd )

  def _addMySQLToDiracCfg( self ):
    """
    Add the database access info to the local configuration
    """
    if not self.mysqlPassword:
      return S_ERROR( 'Missing %s in %s' % ( cfgInstallPath( 'Database', 'Password' ), self.cfgFile ) )

    sectionPath = cfgPath( 'Systems', 'Databases' )
    cfg = self.__getCfg( sectionPath, 'User', self.mysqlUser )
    cfg.setOption( cfgPath( sectionPath, 'Password' ), self.mysqlPassword )

    return self._addCfgToDiracCfg( cfg )

  def configureCE( self, ceName = '', ceType = '', cfg = None, currentSectionPath = '' ):
    """
    Produce new dirac.cfg including configuration for new CE
    """
    from DIRAC.Resources.Computing.ComputingElementFactory    import ComputingElementFactory
    cesCfg = ResourcesDefaults.getComputingElementDefaults( ceName, ceType, cfg, currentSectionPath )
    ceNameList = cesCfg.listSections()
    if not ceNameList:
      error = 'No CE Name provided'
      gLogger.error( error )
      if self.exitOnError:
        DIRAC.exit( -1 )
      return S_ERROR( error )

    for ceName in ceNameList:
      if 'CEType' not in cesCfg[ceName]:
        error = 'Missing Type for CE "%s"' % ceName
        gLogger.error( error )
        if self.exitOnError:
          DIRAC.exit( -1 )
        return S_ERROR( error )

    localsiteCfg = self.localCfg['LocalSite']
    # Replace Configuration under LocalSite with new Configuration
    for ceName in ceNameList:
      if localsiteCfg.existsKey( ceName ):
        gLogger.notice( ' Removing existing CE:', ceName )
        localsiteCfg.deleteKey( ceName )
      gLogger.notice( 'Configuring CE:', ceName )
      localsiteCfg.createNewSection( ceName, contents = cesCfg[ceName] )

    # Apply configuration and try to instantiate the CEs
    gConfig.loadCFG( self.localCfg )

    for ceName in ceNameList:
      ceFactory = ComputingElementFactory()
      try:
        ceInstance = ceFactory.getCE( ceType, ceName )
      except Exception:
        error = 'Fail to instantiate CE'
        gLogger.exception( error )
        if self.exitOnError:
          DIRAC.exit( -1 )
        return S_ERROR( error )
      if not ceInstance['OK']:
        error = 'Fail to instantiate CE: %s' % ceInstance['Message']
        gLogger.error( error )
        if self.exitOnError:
          DIRAC.exit( -1 )
        return S_ERROR( error )

    # Everything is OK, we can save the new cfg
    self.localCfg.writeToFile( self.cfgFile )
    gLogger.always( 'LocalSite section in %s has been uptdated with new configuration:' % os.path.basename( self.cfgFile ) )
    gLogger.always( str( self.localCfg['LocalSite'] ) )

    return S_OK( ceNameList )

  def execCommand( self, timeout, cmd ):
    """
    Execute command tuple and handle Error cases
    """
    result = systemCall( timeout, cmd )
    if not result['OK']:
      if timeout and result['Message'].find( 'Timeout' ) == 0:
        return result
      gLogger.error( 'Failed to execute', '%s: %s' % ( cmd[0], result['Message'] ) )
      if self.exitOnError:
        DIRAC.exit( -1 )
      return result

    if result['Value'][0]:
      error = 'Failed to execute'
      gLogger.error( error, cmd[0] )
      gLogger.error( 'Exit code:' , ( '%s\n' % result['Value'][0] ) + '\n'.join( result['Value'][1:] ) )
      if self.exitOnError:
        DIRAC.exit( -1 )
      error = S_ERROR( error )
      error['Value'] = result['Value']
      return error

    gLogger.verbose( result['Value'][1] )

    return result

gComponentInstaller = ComponentInstaller()
