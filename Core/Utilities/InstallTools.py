########################################################################
# $HeadURL: svn+ssh://svn.cern.ch/reps/dirac/DIRAC/trunk/DIRAC/WorkloadManagementSystem/Agent/TaskQueueDirector.py $
# File :   InstallTools.py
# Author : Ricardo Graciani
########################################################################

"""
Collection of Tools for installation of DIRAC components: MySQL, DB's, Services's, Agents

It only makes use of defaults in LocalInstallation Section in dirac.cfg

The Following Options are used:

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

The setupSite method (used by the dirac-setup-site command) will use the following info:

/LocalInstallation/Systems:       List of Systems to be defined for this instance in the CS (default: Configuration, Framework)
/LocalInstallation/Databases:     List of Databases to be installed and configured
/LocalInstallation/Services:      List of System/ServiceName to be setup
/LocalInstallation/Agents:        List of System/AgentName to be setup
/LocalInstallation/WebPortal:     Boolean to setup the Web Portal (default no)
/LocalInstallation/ConfigurationMaster: Boolean, requires Configuration/Server to be given in the list of Services (default: no)

If a Master Configuration Server is being installed the following Options can be used:

/LocalInstallation/ConfigurationName: Name of the Configuration (default: Setup )

/LocalInstallation/AdminUserName:  Name of the Admin user (default: None )
/LocalInstallation/AdminUserDN:    DN of the Admin user certificate (default: None )
/LocalInstallation/AdminUserEmail: Email of the Admin user (default: None )
/LocalInstallation/AdminGroupName: Name of the Admin group (default: dirac_admin )

/LocalInstallation/HostDN: DN of the host certificate (default: None )

/LocalInstallation/VirtualOrganization: Name of the main Virtual Organization (default: None)
"""

__RCSID__ = "$Id: TaskQueueDirector.py 23253 2010-03-18 08:34:57Z rgracian $"

#
import os, re, glob, stat, time, shutil

defaultPerms = stat.S_IWUSR | stat.S_IRUSR | stat.S_IXUSR | stat.S_IRGRP | stat.S_IXGRP | stat.S_IROTH | stat.S_IXOTH

from DIRAC import rootPath
from DIRAC import gLogger
from DIRAC import exit
from DIRAC import systemCall
from DIRAC import S_OK, S_ERROR

from DIRAC.Core.Utilities.CFG import CFG
from DIRAC.Core.Utilities.Version import getVersion
from DIRAC.ConfigurationSystem.Client.CSAPI import CSAPI
from DIRAC.ConfigurationSystem.Client.Helpers import cfgPath, cfgInstallPath, cfgInstallSection

# On command line tools this can be set to True to abort after the first error.
exitOnError = False

# First some global defaults
gLogger.info( 'DIRAC Root Path =', rootPath )

def loadDiracCfg( verbose = False ):
  """
  Read again defaults from dirac.cfg
  """
  global localCfg, cfgFile, setup, instance, logLevel, linkedRootPath, host
  global basePath, instancePath, runitDir, startDir
  global db, mysqlDir, mysqlDbDir, mysqlLogDir, mysqlMyOrg, mysqlMyCnf, mysqlStartupScript
  global mysqlRootPwd, mysqlUser, mysqlPassword, mysqlHost, mysqlMode, mysqlSmallMem, mysqlLargeMem

  from DIRAC.Core.Utilities.Network import getFQDN

  localCfg = CFG()
  cfgFile = os.path.join( rootPath, 'etc', 'dirac.cfg' )
  try:
    localCfg.loadFromFile( cfgFile )
  except:
    gLogger.always( "Can't load ", cfgFile )
    gLogger.always( "Might be OK if setting up the site" )

  setup = localCfg.getOption( cfgPath( 'DIRAC', 'Setup' ), '' )
  instance = localCfg.getOption( cfgInstallPath( 'InstanceName' ), setup )
  logLevel = localCfg.getOption( cfgInstallPath( 'LogLevel' ), 'INFO' )
  linkedRootPath = localCfg.getOption( cfgInstallPath( 'RootPath' ), rootPath )
  useVersionsDir = localCfg.getOption( cfgInstallPath( 'UseVersionsDir' ), False )

  host = localCfg.getOption( cfgInstallPath( 'Host' ), getFQDN() )

  basePath = os.path.dirname( rootPath )
  instancePath = localCfg.getOption( cfgInstallPath( 'InstancePath' ), rootPath )
  if useVersionsDir:
    # This option takes precedence
    instancePath = os.path.dirname( os.path.dirname( rootPath ) )
    linkedRootPath = os.path.join( instancePath, 'pro' )
  if verbose:
    gLogger.info( 'Using Instance Base Dir at', instancePath )

  runitDir = os.path.join( instancePath, 'runit' )
  runitDir = localCfg.getOption( cfgInstallPath( 'RunitDir' ), runitDir )
  if verbose:
    gLogger.info( 'Using Runit Dir at', runitDir )

  startDir = os.path.join( instancePath, 'startup' )
  startDir = localCfg.getOption( cfgInstallPath( 'StartupDir' ), startDir )
  if verbose:
    gLogger.info( 'Using Startup Dir at', startDir )

  # Now some MySQL default values
  db = {}

  mysqlDir = os.path.join( instancePath, 'mysql' )
  mysqlDir = localCfg.getOption( cfgInstallPath( 'MySQLDir' ), mysqlDir )
  if verbose:
    gLogger.info( 'Using MySQL Dir at', mysqlDir )

  mysqlDbDir = os.path.join( mysqlDir, 'db' )
  mysqlLogDir = os.path.join( mysqlDir, 'log' )
  mysqlMyOrg = os.path.join( rootPath, 'mysql', 'etc', 'my.cnf' )
  mysqlMyCnf = os.path.join( mysqlDir, '.my.cnf' )

  mysqlStartupScript = os.path.join( rootPath, 'mysql', 'share', 'mysql', 'mysql.server' )

  mysqlRootPwd = localCfg.getOption( cfgInstallPath( 'Database', 'RootPwd' ), mysqlRootPwd )
  if verbose and mysqlRootPwd:
    gLogger.info( 'Reading Root MySQL Password from local configuration' )

  mysqlUser = localCfg.getOption( cfgInstallPath( 'Database', 'User' ), '' )
  if verbose and mysqlUser:
    gLogger.info( 'Reading MySQL User from local configuration' )
  else:
    mysqlUser = 'Dirac'

  mysqlPassword = localCfg.getOption( cfgInstallPath( 'Database', 'Password' ), mysqlPassword )
  if verbose and mysqlPassword:
    gLogger.info( 'Reading %s MySQL Password from local configuration' % mysqlUser )

  mysqlHost = localCfg.getOption( cfgInstallPath( 'Database', 'Host' ), '' )
  if verbose and mysqlHost:
    gLogger.info( 'Using MySQL Host from local configuration', mysqlHost )

  mysqlMode = localCfg.getOption( cfgInstallPath( 'Database', 'MySQLMode' ), '' )
  if verbose and mysqlMode:
    gLogger.info( 'Configuring MySQL server as %s' % mysqlMode )

  mysqlSmallMem = localCfg.getOption( cfgInstallPath( 'Database', 'MySQLSmallMem' ), False )
  if verbose and mysqlSmallMem:
    gLogger.info( 'Configuring MySQL server for Low Memory uasge' )

  mysqlLargeMem = localCfg.getOption( cfgInstallPath( 'Database', 'MySQLLargeMem' ), False )
  if verbose and mysqlLargeMem:
    gLogger.info( 'Configuring MySQL server for Large Memory uasge' )

mysqlRootPwd = ''
mysqlPassword = ''
loadDiracCfg()

def getInfo( extensions ):
  result = getVersion()
  if not result['OK']:
    return result
  rDict = result['Value']
  if setup:
    rDict['Setup'] = setup
  else:
    rDict['Setup'] = 'Unknown'
  return S_OK( rDict )

def getExtensions():
  """
  Get the list of installed extensions
  """
  initList = glob.glob( os.path.join( rootPath, '*DIRAC', '__init__.py' ) )
  extensions = [ os.path.basename( os.path.dirname( k ) ) for k in initList]
  try:
    extensions.remove( 'DIRAC' )
  except:
    error = 'DIRAC is not properly installed'
    gLogger.exception( error )
    if exitOnError:
      exit( -1 )
    return S_ERROR( error )

  return S_OK( extensions )

def _addCfgToDiracCfg( cfg, verbose = False ):
  """
  Merge cfg into existing dirac.cfg file
  """
  global localCfg
  if str( localCfg ):
    newCfg = localCfg.mergeWith( cfg )
  else:
    newCfg = cfg
  result = newCfg.writeToFile( cfgFile )
  if not result:
    return result
  loadDiracCfg( verbose )
  return result

def _addCfgToCS( cfg ):
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
  return cfgClient.commit()

def _addCfgToLocalCS( cfg ):
  """
  Merge cfg into local CS
  """
  csName = localCfg.getOption( cfgPath( 'DIRAC', 'Configuration', 'Name' ) , '' )
  if not csName:
    error = 'Missing %s' % cfgPath( 'DIRAC', 'Configuration', 'Name' )
    if exitOnError:
      gLogger.error( error )
      exit( -1 )
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

def _getCentralCfg( installCfg ):
  """
  Create the esqueleton of central Cfg for an initial Master CS
  """
  # First copy over from installation cfg
  centralCfg = CFG()

  # DIRAC/Extensions
  extensions = localCfg.getOption( cfgInstallPath( 'Extensions' ), [] )
  while 'Web' in list( extensions ):
    extensions.remove( 'Web' )
  centralCfg.createNewSection( 'DIRAC', '' )
  if extensions:
    centralCfg['DIRAC'].addKey( 'Extensions', ','.join( extensions ), '' )

  vo = localCfg.getOption( cfgInstallPath( 'VirtualOrganization' ), '' )
  if vo:
    centralCfg['DIRAC'].addKey( 'VirtualOrganization', vo, '' )

  for section in [ 'Systems', 'Resources', 'Operations', 'Website', 'Registry' ]:
    if installCfg.isSection( section ):
      centralCfg.createNewSection( section, contents = installCfg[section] )

  # Now try to add things from the Installation section
  # Registry
  adminUserName = localCfg.getOption( cfgInstallPath( 'AdminUserName' ), '' )
  adminUserDN = localCfg.getOption( cfgInstallPath( 'AdminUserDN' ), '' )
  adminUserEmail = localCfg.getOption( cfgInstallPath( 'AdminUserEmail' ), '' )
  adminGroupName = localCfg.getOption( cfgInstallPath( 'AdminGroupName' ), 'dirac_admin' )
  hostDN = localCfg.getOption( cfgInstallPath( 'HostDN' ), '' )
  defaultGroupName = 'user'
  adminGroupProperties = ['CSAdministrator', 'ServiceAdministrator', 'JobAdministrator', 'Operator', 'FullDelegation', 'ProxyManagment', 'AlarmsManagement']
  defaultGroupProperties = ['NormalUser']
  defaultHostProperties = [ 'JobAdministrator', 'FullDelegation', 'Operator', 'CSAdministrator', 'ProxyManagement', 'TrustedHost']

  if adminUserName:
    if not ( adminUserDN and adminUserEmail ):
      gLogger.error( 'AdminUserName is given but DN or Mail is missing it will not be configured' )
    else:
      for section in [cfgPath( 'Registry' ),
                      cfgPath( 'Registry', 'Users' ),
                      cfgPath( 'Registry', 'Users', adminUserName ),
                      cfgPath( 'Registry', 'Groups' ),
                      cfgPath( 'Registry', 'Groups', defaultGroupName ),
                      cfgPath( 'Registry', 'Groups', adminGroupName ),
                      cfgPath( 'Registry', 'Hosts' ),
                      cfgPath( 'Registry', 'Hosts', host ) ]:
        section, centralCfg.isSection( section )
        if not centralCfg.isSection( section ):
          centralCfg.createNewSection( section )

      if centralCfg['Registry'].existsKey( 'DefaultGroup' ):
        centralCfg['Registry'].deleteKey( 'DefaultGroup' )
      centralCfg['Registry'].addKey( 'DefaultGroup', defaultGroupName, '' )

      if centralCfg['Registry']['Users'][adminUserName].existsKey( 'DN' ):
        centralCfg['Registry']['Users'][adminUserName].deleteKey( 'DN' )
      centralCfg['Registry']['Users'][adminUserName].addKey( 'DN', adminUserDN, '' )

      if centralCfg['Registry']['Users'][adminUserName].existsKey( 'Email' ):
        centralCfg['Registry']['Users'][adminUserName].deleteKey( 'Email' )
      centralCfg['Registry']['Users'][adminUserName].addKey( 'Email' , adminUserEmail, '' )

      # Add Admin User to Admin Group and default group
      for group in [adminGroupName, defaultGroupName]:
        if not centralCfg['Registry']['Groups'][group].isOption( 'Users' ):
          centralCfg['Registry']['Groups'][group].addKey( 'Users', '', '' )
        users = centralCfg['Registry']['Groups'][group].getOption( 'Users', [] )
        if adminUserName not in users:
          centralCfg['Registry']['Groups'][group].appendToOption( 'Users', ', %s' % adminUserName )
        if not centralCfg['Registry']['Groups'][group].isOption( 'Properties' ):
          centralCfg['Registry']['Groups'][group].addKey( 'Properties', '', '' )

      properties = centralCfg['Registry']['Groups'][adminGroupName].getOption( 'Properties', [] )
      for property in adminGroupProperties:
        if property not in properties:
          properties.append( property )
          centralCfg['Registry']['Groups'][adminGroupName].appendToOption( 'Properties', ', %s' % property )

      properties = centralCfg['Registry']['Groups'][defaultGroupName].getOption( 'Properties', [] )
      for property in defaultGroupProperties:
        if property not in properties:
          properties.append( property )
          centralCfg['Registry']['Groups'][defaultGroupName].appendToOption( 'Properties', ', %s' % property )

      # Add the master Host description
      if hostDN:
        if centralCfg['Registry']['Hosts'][host].existsKey( 'DN' ):
          centralCfg['Registry']['Hosts'][host].deleteKey( 'DN' )
        centralCfg['Registry']['Hosts'][host].addKey( 'DN', hostDN, '' )
        if not centralCfg['Registry']['Hosts'][host].isOption( 'Properties' ):
          centralCfg['Registry']['Hosts'][host].addKey( 'Properties', '', '' )
        properties = centralCfg['Registry']['Hosts'][host].getOption( 'Properties', [] )
        for property in defaultHostProperties:
          if property not in properties:
            properties.append( property )
            centralCfg['Registry']['Hosts'][host].appendToOption( 'Properties', ', %s' % property )

  # Operations
  if adminUserEmail:
    operationsCfg = __getCfg( cfgPath( 'Operations', 'EMail' ), 'Production', adminUserEmail )
    centralCfg = centralCfg.mergeWith( operationsCfg )

  # Website
  websiteCfg = __getCfg( cfgPath( 'Website', 'Authorization', 'systems', 'configuration' ), 'Default', 'all' )
  websiteCfg['Website'].addKey( 'DefaultGroups', ', '.join( ['visitor', defaultGroupName, adminGroupName] ), '' )
  websiteCfg['Website'].addKey( 'DefaultSetup', setup, '' )
  websiteCfg['Website']['Authorization']['systems']['configuration'].addKey( 'showHistory' , 'CSAdministrator' , '' )
  websiteCfg['Website']['Authorization']['systems']['configuration'].addKey( 'commitConfiguration' , 'CSAdministrator' , '' )
  websiteCfg['Website']['Authorization']['systems']['configuration'].addKey( 'showCurrentDiff' , 'CSAdministrator' , '' )
  websiteCfg['Website']['Authorization']['systems']['configuration'].addKey( 'showDiff' , 'CSAdministrator' , '' )
  websiteCfg['Website']['Authorization']['systems']['configuration'].addKey( 'rollbackToVersion' , 'CSAdministrator' , '' )
  websiteCfg['Website']['Authorization']['systems']['configuration'].addKey( 'manageRemoteConfig' , 'CSAdministrator' , '' )
  websiteCfg['Website']['Authorization']['systems']['configuration'].appendToOption( 'manageRemoteConfig' , ', ServiceAdministrator' )

  centralCfg = centralCfg.mergeWith( websiteCfg )

  return centralCfg

def __getCfg( section, option = '', value = '' ):
  """
  Create a new Cfg with given info
  """
  if not section:
    return None
  cfg = CFG()
  sectionList = []
  for section in section.split( '/' ):
    if not section:
      continue
    sectionList.append( section )
    cfg.createNewSection( cfgPath( *sectionList ) )
  if not sectionList:
    return None

  if option and value:
    sectionList.append( option )
    cfg.setOption( '/'.join( sectionList ), value )

  return cfg

def addOptionToDiracCfg( option, value ):
  """
  Add Option to dirac.cfg
  """
  optionList = option.split( '/' )
  optionName = optionList[-1]
  section = cfgPath( *optionList[:-1] )
  cfg = __getCfg( section, optionName, value )

  if not cfg:
    return S_ERROR( 'Wrong option: %s = %s' % ( option, value ) )

  if _addCfgToDiracCfg( cfg ):
    return S_OK()

  return S_ERROR( 'Could not merge %s=%s with local configuration' % ( option, value ) )

def addDefaultOptionsToCS( gConfig, componentType, systemName, component, extensions, overwrite = False ):
  """ Add the section with the component options to the CS
  """
  system = systemName.replace( 'System', '' )
  instanceOption = cfgPath( 'DIRAC', 'Setups', setup, system )
  if gConfig:
    instance = gConfig.getValue( instanceOption, '' )
  else:  
    instance = localCfg.getOption( instanceOption, '' )
  if not instance:
    return S_ERROR( '%s not defined in %s' % ( instanceOption, cfgFile ) )

  sectionName = "Agents"
  if componentType == 'service':
    sectionName = "Services"

  # Check if the component CS options exist
  addOptions = True
  if not overwrite:
    componentSection = cfgPath( 'Systems', system, instance, sectionName, component )
    result = gConfig.getOptions( componentSection )
    if result['OK']:
      addOptions = False
  if not addOptions:
    return S_OK( 'Component options already exist' )

  # Add the component options now
  result = getComponentCfg( componentType, system, component, instance, extensions )
  if not result['OK']:
    return result
  compCfg = result['Value']

  gLogger.info( 'Adding to CS', '%s/%s' % ( system, component ) )
  return _addCfgToCS( compCfg )

def addDefaultOptionsToComponentCfg( componentType, systemName, component, extensions ):
  """
  Add default component options local component cfg
  """
  system = systemName.replace( 'System', '' )
  instanceOption = cfgPath( 'DIRAC', 'Setups', setup, system )
  instance = localCfg.getOption( instanceOption, '' )
  if not instance:
    return S_ERROR( '%s not defined in %s' % ( instanceOption, cfgFile ) )

  # Add the component options now
  result = getComponentCfg( componentType, system, component, instance, extensions )
  if not result['OK']:
    return result
  compCfg = result['Value']

  compCfgFile = os.path.join( rootPath, 'etc', '%s_%s.cfg' % ( system, component ) )
  return compCfg.writeToFile( compCfgFile )


def getComponentCfg( componentType, system, component, instance, extensions ):
  """
  Get the CFG object of the component configuration
  """
  sectionName = 'Services'
  if componentType == 'agent':
    sectionName = 'Agents'

  compCfg = ''
  for ext in extensions + ['DIRAC']:
    cfgTemplatePath = os.path.join( rootPath, ext, '%sSystem' % system, 'ConfigTemplate.cfg' )
    if os.path.exists( cfgTemplatePath ):
      gLogger.info( 'Loading configuration template', cfgTemplatePath )
      # Look up the component in this template
      loadCfg = CFG()
      loadCfg.loadFromFile( cfgTemplatePath )
      try:
        compCfg = loadCfg[sectionName][component]
      except:
        error = 'Can not find %s in template' % cfgPath( sectionName, component )
        gLogger.error( error )
        if exitOnError:
          exit( -1 )
        return S_ERROR( error )

  if not compCfg:
    error = 'Configuration template not found'
    gLogger.error( error )
    if exitOnError:
      exit( -1 )
    return S_ERROR( error )

  sectionPath = cfgPath( 'Systems', system, instance, sectionName )
  cfg = __getCfg( sectionPath )
  cfg.createNewSection( cfgPath( sectionPath, component ), '', compCfg )

  # Add the service URL
  if componentType == "service":
    port = compCfg.getOption( 'Port' , 0 )
    if port and host:
      urlsPath = cfgPath( 'Systems', system, instance, 'URLs' )
      cfg.createNewSection( urlsPath )
      cfg.setOption( cfgPath( urlsPath, component ),
                    'dips://%s:%d/%s/%s' % ( host, port, system, component ) )

  return S_OK( cfg )

def addDatabaseOptionsToCS( gConfig, systemName, dbName, overwrite = False ):
  """
  Add the section with the database options to the CS
  """
  system = systemName.replace( 'System', '' )
  instanceOption = cfgPath( 'DIRAC', 'Setups', setup, system )
  if gConfig:
    instance = gConfig.getValue( instanceOption, '' )
  else:  
    instance = localCfg.getOption( instanceOption, '' )
  if not instance:
    return S_ERROR( '%s not defined in %s' % ( instanceOption, cfgFile ) )

  # Check if the component CS options exist
  addOptions = True
  if not overwrite:
    databasePath = cfgPath( 'Systems', system, instance, 'Databases', dbName )
    result = gConfig.getOptions( databasePath )
    if result['OK']:
      addOptions = False
  if not addOptions:
    return S_OK( 'Database options already exist' )

  # Add the component options now
  result = getDatabaseCfg( system, dbName, instance )
  if not result['OK']:
    return result
  databaseCfg = result['Value']
  gLogger.info( 'Adding to CS', '%s/%s' % ( system, dbName ) )
  return _addCfgToCS( databaseCfg )

def getDatabaseCfg( system, dbName, instance ):
  """ 
  Get the CFG object of the database configuration
  """
  databasePath = cfgPath( 'Systems', system, instance, 'Databases', dbName )
  cfg = __getCfg( databasePath, 'DBName', dbName )
  cfg.setOption( cfgPath( databasePath, 'Host' ), mysqlHost )

  return S_OK( cfg )

def addSystemInstance( systemName, instance ):
  """ 
  Add a new system instance to dirac.cfg and CS
  """
  system = systemName.replace( 'System', '' )
  gLogger.info( 'Adding %s system as %s instance for %s setup to dirac.cfg and CS' % ( system, instance, setup ) )

  cfg = __getCfg( cfgPath( 'DIRAC', 'Setups', setup ), system, instance )
  if not _addCfgToDiracCfg( cfg ):
    return S_ERROR( 'Failed to add system instance to dirac.cfg' )

  return _addCfgToCS( cfg )

def printStartupStatus( rDict ):
  """
  Print in nice format the return dictionary from getStartupComponentStatus (also returned by runsvctrlComponent)
  """
  try:
    print 'Name'.rjust( 32 ), ':', 'Runit    Uptime    PID'
    for comp in rDict:
      print comp.rjust( 32 ), ':', rDict[comp]['RunitStatus'].ljust( 7 ), rDict[comp]['Timeup'].rjust( 7 ), rDict[comp]['PID'].rjust( 8 )
  except:
    pass
  return S_OK()

def printOverallStatus( rDict ):
  """
  Print in nice format the return dictionary from getOverallStatus
  """
  try:
    print
    print "   System", ' '*20, 'Name', ' '*5, 'Type', ' '*23, 'Setup    Installed   Runit    Uptime    PID'
    print '-' * 116
    for compType in rDict:
      for system in rDict[compType]:
        for component in rDict[compType][system]:
          print  system.ljust( 28 ), component.ljust( 28 ), compType.lower()[:-1].ljust( 7 ),
          if rDict[compType][system][component]['Setup']:
            print 'SetUp'.rjust( 12 ),
          else:
            print 'NotSetup'.rjust( 12 ),
          if rDict[compType][system][component]['Installed']:
            print 'Installed'.rjust( 12 ),
          else:
            print 'NotInstalled'.rjust( 12 ),
          print str( rDict[compType][system][component]['RunitStatus'] ).ljust( 7 ),
          print str( rDict[compType][system][component]['Timeup'] ).rjust( 7 ),
          print str( rDict[compType][system][component]['PID'] ).rjust( 8 ),
          print
  except:
    pass

  return S_OK()

def getSoftwareComponents( extensions ):
  """  Get the list of all the components ( services and agents ) for which the software
       is installed on the system
  """

  services = {}
  agents = {}

  for extension in ['DIRAC'] + [ x + 'DIRAC' for x in extensions]:
    if not os.path.exists( os.path.join( rootPath, extension ) ):
      # Not all the extensions are necessarily installed in this instance
      continue
    systemList = os.listdir( os.path.join( rootPath, extension ) )
    for sys in systemList:
      system = sys.replace( 'System', '' )
      try:
        agentDir = os.path.join( rootPath, extension, sys, 'Agent' )
        agentList = os.listdir( agentDir )
        for agent in agentList:
          if agent[-3:] == ".py":
            agentFile = os.path.join( agentDir, agent )
            afile = open( agentFile, 'r' )
            body = afile.read()
            afile.close()
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
          if service.find( 'Handler' ) != -1 and service[-3:] == '.py':
            if not services.has_key( system ):
              services[system] = []
            if system == 'Configuration' and service == 'ConfigurationHandler.py':
              service = 'ServerHandler.py'
            services[system].append( service.replace( '.py', '' ).replace( 'Handler', '' ) )
      except OSError:
        pass

  resultDict = {}
  resultDict['Services'] = services
  resultDict['Agents'] = agents
  return S_OK( resultDict )

def getInstalledComponents():
  """
  Get the list of all the components ( services and agents ) 
  installed on the system in the runit directory
  """

  services = {}
  agents = {}
  systemList = os.listdir( runitDir )
  for system in systemList:
    systemDir = os.path.join( runitDir, system )
    components = os.listdir( systemDir )
    for component in components:
      try:
        runFile = os.path.join( systemDir, component, 'run' )
        rfile = open( runFile, 'r' )
        body = rfile.read()
        rfile.close()
        if body.find( 'dirac-service' ) != -1:
          if not services.has_key( system ):
            services[system] = []
          services[system].append( component )
        elif body.find( 'dirac-agent' ) != -1:
          if not agents.has_key( system ):
            agents[system] = []
          agents[system].append( component )
      except IOError:
        pass

  resultDict = {}
  resultDict['Services'] = services
  resultDict['Agents'] = agents
  return S_OK( resultDict )

def getSetupComponents():
  """  Get the list of all the components ( services and agents ) 
       set up for running with runsvdir in startup directory 
  """

  services = {}
  agents = {}
  if not os.path.isdir( startDir ):
    return S_ERROR( 'Startup Directory does not exit: %s' % startDir )
  componentList = os.listdir( startDir )
  for component in componentList:
    try:
      runFile = os.path.join( startDir, component, 'run' )
      rfile = open( runFile, 'r' )
      body = rfile.read()
      rfile.close()
      if body.find( 'dirac-service' ) != -1:
        system, service = component.split( '_' )
        if not services.has_key( system ):
          services[system] = []
        services[system].append( service )
      elif body.find( 'dirac-agent' ) != -1:
        system, agent = component.split( '_' )
        if not agents.has_key( system ):
          agents[system] = []
        agents[system].append( agent )
    except IOError:
      pass

  resultDict = {}
  resultDict['Services'] = services
  resultDict['Agents'] = agents
  return S_OK( resultDict )

def getStartupComponentStatus( componentTupleList ):
  """  Get the list of all the components ( services and agents ) 
       set up for running with runsvdir in startup directory 
  """
  try:
    if componentTupleList:
      cList = []
      for componentTuple in componentTupleList:
        cList.extend( glob.glob( os.path.join( startDir, '_'.join( componentTuple ) ) ) )
    else:
      cList = glob.glob( os.path.join( startDir, '*' ) )
  except:
    error = 'Failed to parse List of Components'
    gLogger.exception( error )
    if exitOnError:
      exit( -1 )
    return S_ERROR( error )

  result = execCommand( 0, ['runsvstat'] + cList )
  if not result['OK']:
    return result
  output = result['Value'][1].strip().split( '\n' )

  componentDict = {}
  for line in output:
    if not line:
      continue
    cname, routput = line.split( ':' )
    cname = cname.replace( '%s/' % startDir, '' )
    run = False
    result = re.search( '^ run', routput )
    if result:
      run = True
    down = False
    result = re.search( '^ down', routput )
    if result:
      down = True
    result = re.search( '([0-9]+) seconds', routput )
    timeup = 0
    if result:
      timeup = result.group( 1 )
    result = re.search( 'pid ([0-9]+)', routput )
    pid = 0
    if result:
      pid = result.group( 1 )
    runsv = "Not running"
    if run or down:
      runsv = "Running"
    result = re.search( 'runsv not running', routput )
    if result:
      runsv = "Not running"

    runDict = {}
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

def getOverallStatus( extensions ):
  """  Get the list of all the components ( services and agents ) 
       set up for running with runsvdir in startup directory 
  """

  result = getSoftwareComponents( extensions )
  if not result['OK']:
    return result
  softDict = result['Value']

  result = getSetupComponents()
  if not result['OK']:
    return result
  setupDict = result['Value']

  result = getInstalledComponents()
  if not result['OK']:
    return result
  installedDict = result['Value']

  result = getStartupComponentStatus( [] )
  if not result['OK']:
    return result
  runitDict = result['Value']

  # Collect the info now
  resultDict = {'Services':{}, 'Agents':{}}
  for compType in ['Services', 'Agents']:
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
          except Exception, x:
            #print str(x)
            pass
          try:
            if component in installedDict[compType][system]:
              resultDict[compType][system][component]['Installed'] = True
          except Exception, x:
            #print str(x) 
            pass
          try:
            compDir = system + '_' + component
            if runitDict.has_key( compDir ):
              resultDict[compType][system][component]['RunitStatus'] = runitDict[compDir]['RunitStatus']
              resultDict[compType][system][component]['Timeup'] = runitDict[compDir]['Timeup']
              resultDict[compType][system][component]['PID'] = runitDict[compDir]['PID']
          except Exception, x:
            #print str(x)
            pass

  return S_OK( resultDict )

def checkComponentSoftware( componentType, system, component, extensions ):
  """ Check the component software
  """
  result = getSoftwareComponents( extensions )
  if not result['OK']:
    return result

  if componentType == 'service':
    softDict = result['Value']['Services']
  elif componentType == 'agent':
    softDict = result['Value']['Agents']
  else:
    return S_ERROR( 'Unknown component type %s' % componentType )

  if system in softDict and component in softDict[system]:
    return S_OK()

  return S_ERROR( 'Unknown Component %s/%s' % ( system, component ) )

def runsvctrlComponent( system, component, mode ):
  """
  Execute runsvctrl and check status of the specified component
  """
  if not mode in ['u', 'd', 'o', 'p', 'c', 'h', 'a', 'i', 'q', '1', '2', 't', 'k', 'x', 'e']:
    return S_ERROR( 'Unknown runsvctrl mode', mode )

  for startCompDir in glob.glob( os.path.join( startDir, '%s_%s' % ( system, component ) ) ):
    result = execCommand( 0, ['runsvctrl', mode, startCompDir] )
    if not result['OK']:
      return result
    time.sleep( 1 )

  # Check the runsv status
  if system == '*' or component == '*':
    time.sleep( 5 )

  # Final check
  result = getStartupComponentStatus( [( system, component )] )
  if not result['OK']:
    return S_ERROR( 'Failed to start the component' )

  return result

def getLogTail( system, component, length = 100 ):
  """
  Get the tail of the component log file
  """
  retDict = {}
  for startCompDir in glob.glob( os.path.join( startDir, '%s_%s' % ( system, component ) ) ):
    compName = os.path.basename( startCompDir )
    logFileName = os.path.join( startCompDir, 'log', 'current' )
    if not os.path.exists( logFileName ):
      retDict[compName] = 'No log file found'
    else:
      logFile = open( logFileName, 'r' )
      lines = [ l.strip() for l in logFile.readlines() ]
      logFile.close()

      if len( lines ) < length:
        retDict[compName] = '\n'.join( lines )
      else:
        retDict[compName] = '\n'.join( lines[-length:] )

  return S_OK( retDict )

def setupSite( scriptCfg, cfg = None ):
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

      if instancePath != basePath:
        if not diracCfg.isSection( 'LocalSite' ):
          diracCfg.createNewSection( 'LocalSite' )
        diracCfg.setOption( cfgPath( 'LocalSite', 'InstancePath' ), instancePath )

      _addCfgToDiracCfg( diracCfg, verbose = True )
    except:
      error = 'Failed to load %s' % cfg
      gLogger.exception( error )
      if exitOnError:
        exit( -1 )
      return S_ERROR( error )

  # Now get the necessary info from localCfg
  setupSystems = localCfg.getOption( cfgInstallPath( 'Systems' ), ['Configuration', 'Framework'] )
  setupDatabases = localCfg.getOption( cfgInstallPath( 'Databases' ), [] )
  setupServices = [ k.split( '/' ) for k in localCfg.getOption( cfgInstallPath( 'Services' ), [] ) ]
  setupAgents = [ k.split( '/' ) for k in localCfg.getOption( cfgInstallPath( 'Agents' ), [] ) ]
  setupWeb = localCfg.getOption( cfgInstallPath( 'WebPortal' ), False )
  setupConfigurationMaster = localCfg.getOption( cfgInstallPath( 'ConfigurationMaster' ), False )
  setupConfigurationName = localCfg.getOption( cfgInstallPath( 'ConfigurationName' ), setup )

  for serviceTuple in setupServices:
    error = ''
    if len( serviceTuple ) != 2:
      error = 'Wrong service specification: system/service'
    elif serviceTuple[0] not in setupSystems:
      error = 'System %s not available' % serviceTuple[0]
    if error:
      if exitOnError:
        gLogger.error( error )
        exit( -1 )
      return S_ERROR( error )

  for agentTuple in setupAgents:
    error = ''
    if len( agentTuple ) != 2:
      error = 'Wrong agent specification: system/service'
    elif agentTuple[0] not in setupSystems:
      error = 'System %s not available' % agentTuple[0]
    if error:
      if exitOnError:
        gLogger.error( error )
        exit( -1 )
      return S_ERROR( error )

  # And to find out the available extensions
  result = getExtensions()
  if not result['OK']:
    return result
  extensions = [ k.replace( 'DIRAC', '' ) for k in result['Value']]

  # Make sure the necessary directories are there
  if basePath != instancePath:
    if not os.path.exists( instancePath ):
      try:
        os.makedirs( instancePath )
      except:
        error = 'Can not create directory for instance %s' % instancePath
        if exitOnError:
          gLogger.exception( error )
          exit( -1 )
        return S_ERROR( error )
    if not os.path.isdir( instancePath ):
      error = 'Instance directory %s is not valid' % instancePath
      if exitOnError:
        gLogger.error( error )
        exit( -1 )
      return S_ERROR( error )

    instanceEtcDir = os.path.join( instancePath, 'etc' )
    etcDir = os.path.dirname( cfgFile )
    if not os.path.exists( instanceEtcDir ):
      try:
        os.symlink( etcDir, instanceEtcDir )
      except:
        error = 'Can not create link to configuration %s' % instanceEtcDir
        if exitOnError:
          gLogger.exception( error )
          exit( -1 )
        return S_ERROR( error )

    if os.path.realpath( instanceEtcDir ) != os.path.realpath( etcDir ):
      error = 'Instance etc (%s) is not the same as DIRAC etc (%s)' % ( instanceEtcDir, etcDir )
      if exitOnError:
        gLogger.error( error )
        exit( -1 )
      return S_ERROR( error )

  # if any server or agent needs to be install we need the startup directory and runsvdir running
  if setupServices or setupAgents or setupWeb:
    if not os.path.exists( startDir ):
      try:
        os.makedirs( startDir )
      except:
        error = 'Can not create %s' % startDir
        if exitOnError:
          gLogger.exception( error )
          exit( -1 )
        return S_ERROR( error )
    # And need to make sure runsvdir is running
    result = execCommand( 0, ['ps', '-ef'] )
    if not result['OK']:
      if exitOnError:
        gLogger.error( result['Message'] )
        exit( -1 )
      return S_ERROR( result['Message'] )
    processList = result['Value'][1].split( '\n' )
    cmd = 'runsvdir %s' % startDir
    cmdFound = False
    for process in processList:
      if process.find( cmd ) != -1:
        cmdFound = True
    if not cmdFound:
      gLogger.info( 'Starting runsvdir ...' )
      os.system( "runsvdir %s 'log:  DIRAC runsv' &" % startDir )

  if ['Configuration', 'Server'] in setupServices and setupConfigurationMaster:
    # This server hosts the Master of the CS
    gLogger.info( 'Installing Master Configuration Server' )
    cfg = __getCfg( cfgPath( 'DIRAC', 'Setups', setup ), 'Configuration', instance )
    _addCfgToDiracCfg( cfg )
    cfg = __getCfg( cfgPath( 'DIRAC', 'Configuration' ), 'Master' , 'yes' )
    cfg.setOption( cfgPath( 'DIRAC', 'Configuration', 'Name' ) , setupConfigurationName )
    _addCfgToDiracCfg( cfg )
    addDefaultOptionsToComponentCfg( 'service', 'Configuration', 'Server', [] )
    if installCfg:
      centralCfg = _getCentralCfg( installCfg )
    else:
      centralCfg = _getCentralCfg( localCfg )
    _addCfgToLocalCS( centralCfg )
    setupComponent( 'service', 'Configuration', 'Server', [] )
    runsvctrlComponent( 'Configuration', 'Server', 't' )

    while ['Configuration', 'Server'] in setupServices:
      setupServices.remove( ['Configuration', 'Server'] )

  time.sleep( 5 )

  # Now need to check if there is valid CS to register the info
  result = scriptCfg.enableCS()
  if not result['OK']:
    if exitOnError:
      exit( -1 )
    return result

  cfgClient = CSAPI()
  if not cfgClient.initialize():
    error = 'Configuration Server not defined'
    if exitOnError:
      gLogger.error( error )
      exit( -1 )
    return S_ERROR( error )

  # We need to make sure components are connecting to the Master CS, that is the only one being update
  from DIRAC import gConfig
  localServers = localCfg.getOption( cfgPath( 'DIRAC', 'Configuration', 'Servers' ) )
  masterServer = gConfig.getValue( cfgPath( 'DIRAC', 'Configuration', 'MasterServer' ), '' )
  initialCfg = __getCfg( cfgPath( 'DIRAC', 'Configuration' ), 'Servers' , localServers )
  masterCfg = __getCfg( cfgPath( 'DIRAC', 'Configuration' ), 'Servers' , masterServer )
  _addCfgToDiracCfg( masterCfg )

  # 1.- Setup the instances in the CS
  # If the Configuration Server used is not the Master, it can take some time for this
  # info to be propagated, this my cause the later setup to fail
  gLogger.info( 'Registering System instances' )
  for system in setupSystems:
    addSystemInstance( system, instance )
  for system, service in setupServices:
    addDefaultOptionsToCS( None, 'service', system, service, extensions, True )
  for system, agent in setupAgents:
    addDefaultOptionsToCS( None, 'agent', system, agent, extensions, True )

  # 2.- Check if MySQL is required
  if setupDatabases:
    gLogger.info( 'Installing MySQL' )
    getMySQLPasswords()
    installMySQL()

    # 3.- And install requested Databases
    result = getDatabases()
    if not result['OK']:
      if exitOnError:
        gLogger.error( result['Message'] )
        exit( -1 )
      return result
    installedDatabases = result['Value']
    for db in setupDatabases:
      if db not in installedDatabases:
        extension, system = installDatabase( db )['Value']
        gLogger.info( 'Database %s from %s/%s installed' % ( db, extension, system ) )
        result = addDatabaseOptionsToCS( None, system, db, True )
        if not result['OK']:
          gLogger.error('Database %s CS registration failed: %s' % (db,result['Message']) )
      gLogger.info( 'Database %s already installed' % db )

  # 4.- Then installed requested services
  for system, service in setupServices:
    setupComponent( 'service', system, service, extensions )

  # 5.- Now the agents
  for system, agent in setupAgents:
    setupComponent( 'agent', system, agent, extensions )

  # 6.- And finally the Portal
  if setupWeb:
    setupPortal()

  if localServers != masterServer:
    _addCfgToDiracCfg( initialCfg )
    for system, service in  setupServices:
      runsvctrlComponent( system, service, 't' )
    for system, agent in setupAgents:
      runsvctrlComponent( system, agent, 't' )

  return S_OK()

def _createRunitLog( runitCompDir ):
    logDir = os.path.join( runitCompDir, 'log' )
    os.makedirs( logDir )

    logConfigFile = os.path.join( logDir, 'config' )
    f = open( logConfigFile, 'w' )
    f.write( 
"""s10000000
n20
""" )
    f.close()

    logRunFile = os.path.join( logDir, 'run' )
    f = open( logRunFile, 'w' )
    f.write( 
"""#!/bin/bash
#
rcfile=%(bashrc)s
[ -e $rcfile ] && source $rcfile
#
exec svlogd .

""" % { 'bashrc' : os.path.join( instancePath, 'bashrc' ) } )
    f.close()

    os.chmod( logRunFile, defaultPerms )


def installComponent( componentType, system, component, extensions ):
  """ Install runit directory for the specified component
  """
  # Check that the software for the component is installed
  if not checkComponentSoftware( componentType, system, component, extensions )['OK']:
    error = 'Software for %s %s/%s is not installed' % ( componentType, system, component )
    if exitOnError:
      gLogger.error( error )
      exit( -1 )
    return S_ERROR( error )

  # Check if the component is already installed
  runitCompDir = os.path.join( runitDir, system, component )
  if os.path.exists( runitCompDir ):
    msg = "%s %s_%s already installed" % ( componentType, system, component )
    gLogger.info( msg )
    return S_OK( runitCompDir )

  gLogger.info( 'Installing %s %s/%s' % ( componentType, system, component ) )

  # Now do the actual installation
  try:
    componentCfg = os.path.join( linkedRootPath, 'etc', '%s_%s.cfg' % ( system, component ) )
    if not os.path.exists( componentCfg ):
      f = open( componentCfg, 'w' )
      f.close()

    _createRunitLog( runitCompDir )

    runFile = os.path.join( runitCompDir, 'run' )
    f = open( runFile, 'w' )
    f.write( 
"""#!/bin/bash
rcfile=%(bashrc)s
[ -e $rcfile ] && source $rcfile
#
exec 2>&1
#
[ "%(componentType)s" = "agent" ] && renice 20 -p $$
#
exec python %(DIRAC)s/DIRAC/Core/scripts/dirac-%(componentType)s.py %(system)s/%(component)s %(componentCfg)s -o LogLevel=%(logLevel)s < /dev/null
""" % {'bashrc': os.path.join( instancePath, 'bashrc' ),
       'DIRAC': linkedRootPath,
       'componentType': componentType,
       'system' : system,
       'component': component,
       'componentCfg': componentCfg,
       'logLevel': logLevel } )
    f.close()

    os.chmod( runFile, defaultPerms )

  except:
    error = 'Failed to prepare setup for %s %s/%s' % ( componentType, system, component )
    gLogger.exception( error )
    if exitOnError:
      exit( -1 )
    return S_ERROR( error )

  result = execCommand( 5, [runFile] )

  gLogger.info( result['Value'][1] )

  return S_OK( runitCompDir )

def setupComponent( componentType, system, component, extensions ):
  """
  Install and create link in startup
  """
  result = installComponent( componentType, system, component, extensions )
  if not result['OK']:
    return result

  # Create the startup entry now
  runitCompDir = result['Value']
  startCompDir = os.path.join( startDir, '%s_%s' % ( system, component ) )
  if not os.path.exists( startDir ):
    os.makedirs( startDir )
  if not os.path.lexists( startCompDir ):
    gLogger.info( 'Creating startup link at', startCompDir )
    os.symlink( runitCompDir, startCompDir )
    time.sleep( 5 )

  # Check the runsv status
  start = time.time()
  while ( time.time() - 20 ) < start:
    result = getStartupComponentStatus( [ ( system, component )] )
    if not result['OK']:
      return S_ERROR( 'Failed to start the component %s_%s' % ( system, component ) )
    if result['Value'] and result['Value']['%s_%s' % ( system, component )]['RunitStatus'] == "Run":
      break
    time.sleep( 1 )

  # Final check
  result = getStartupComponentStatus( [( system, component )] )
  if not result['OK']:
    return S_ERROR( 'Failed to start the component %s_%s' % ( system, component ) )

  resDict = {}
  resDict['ComponentType'] = componentType
  resDict['RunitStatus'] = result['Value']['%s_%s' % ( system, component )]['RunitStatus']
  return S_OK( resDict )

def unsetupComponent( system, component ):
  """
  Remove link from startup
  """
  for startCompDir in glob.glob( os.path.join( startDir, '%s_%s' % ( system, component ) ) ):
    try:
      os.unlink( startCompDir )
    except:
      gLogger.exception()

  return S_OK()

def uninstallComponent( system, component ):
  """
  Remove startup and runit directories
  """
  unsetupComponent( system, component )

  for runitCompDir in glob.glob( os.path.join( runitDir, system, component ) ):
    try:
      shutil.rmtree( runitCompDir )
    except:
      gLogger.exception()

  return S_OK()

def installPortal():
  """
  Install runit directories for the Web Portal
  """
  # Check that the software for the Web Portal is installed
  error = ''
  webDir = os.path.join( linkedRootPath, 'Web' )
  if not os.path.exists( webDir ):
    error = 'Web extension not installed at %s' % webDir
    if exitOnError:
      gLogger.error( error )
      exit( -1 )
    return S_ERROR( error )

  # First the lighthttpd server

  # Check if the component is already installed
  runitHttpdDir = os.path.join( runitDir, 'Web', 'httpd' )
  runitPasterDir = os.path.join( runitDir, 'Web', 'paster' )

  if os.path.exists( runitHttpdDir ):
    msg = "lighthttpd already installed"
    gLogger.info( msg )
  else:
    gLogger.info( 'Installing Light httpd' )
    # Now do the actual installation
    try:
      _createRunitLog( runitHttpdDir )
      runFile = os.path.join( runitHttpdDir, 'run' )
      f = open( runFile, 'w' )
      f.write( 
"""#!/bin/bash
rcfile=%(bashrc)s
[ -e $rcfile ] && source $rcfile
#
exec 2>&1
#
exec lighttpdSvc.sh < /dev/null
""" % {'bashrc': os.path.join( instancePath, 'bashrc' ), } )
      f.close()

      os.chmod( runFile, defaultPerms )
    except:
      error = 'Failed to prepare setup for light httpd'
      gLogger.exception( error )
      if exitOnError:
        exit( -1 )
      return S_ERROR( error )

    result = execCommand( 5, [runFile] )
    gLogger.info( result['Value'][1] )

  # Second the Web portal

  # Check if the component is already installed
  if os.path.exists( runitPasterDir ):
    msg = "Web Portal already installed"
    gLogger.info( msg )
  else:
    gLogger.info( 'Installing Web Portal' )
    # Now do the actual installation
    try:
      _createRunitLog( runitPasterDir )
      runFile = os.path.join( runitPasterDir, 'run' )
      f = open( runFile, 'w' )
      f.write( 
"""#!/bin/bash
rcfile=%(bashrc)s
[ -e $rcfile ] && source $rcfile
#
exec 2>&1
#
cd %(DIRAC)s/Web
exec paster serve --reload production.ini < /dev/null
""" % {'bashrc': os.path.join( instancePath, 'bashrc' ),
       'DIRAC': linkedRootPath} )
      f.close()

      os.chmod( runFile, defaultPerms )
    except:
      error = 'Failed to prepare setup for Web Portal'
      gLogger.exception( error )
      if exitOnError:
        exit( -1 )
      return S_ERROR( error )

    result = execCommand( 5, [runFile] )
    gLogger.info( result['Value'][1] )

  return S_OK( [runitHttpdDir, runitPasterDir] )

def setupPortal():
  """
  Install and create link in startup
  """
  result = installPortal()
  if not result['OK']:
    return result

  # Create the startup entries now
  runitCompDir = result['Value']
  startCompDir = [ os.path.join( startDir, 'Web_httpd' ),
                   os.path.join( startDir, 'Web_paster' ) ]

  if not os.path.exists( startDir ):
    os.makedirs( startDir )

  for i in range( 2 ):
    if not os.path.lexists( startCompDir[i] ):
      gLogger.info( 'Creating startup link at', startCompDir[i] )
      os.symlink( runitCompDir[i], startCompDir[i] )
      time.sleep( 1 )
  time.sleep( 5 )

  # Check the runsv status
  start = time.time()
  while ( time.time() - 10 ) < start:
    result = getStartupComponentStatus( [ ( 'Web', 'httpd' ), ( 'Web', 'paster' ) ] )
    if not result['OK']:
      return S_ERROR( 'Failed to start the Portal' )
    if result['Value'] and \
       result['Value']['%s_%s' % ( 'Web', 'httpd' )]['RunitStatus'] == "Run" and \
       result['Value']['%s_%s' % ( 'Web', 'paster' )]['RunitStatus'] == "Run" :
      break
    time.sleep( 1 )

  # Final check
  return getStartupComponentStatus( [ ( 'Web', 'httpd' ), ( 'Web', 'paster' ) ] )

def fixMySQLScripts():
  """
  Edit MySQL scripts to point to desired locations for db and my.cnf
  """
  gLogger.verbose( 'Updating:', mysqlStartupScript )
  try:
    f = open( mysqlStartupScript, 'r' )
    orgLines = f.readlines()
    f.close()

    f = open( mysqlStartupScript, 'w' )
    for line in orgLines:
      if line.find( 'export HOME' ) == 0:
        continue
      if line.find( 'datadir=' ) == 0:
        line = 'datadir=%s\n' % mysqlDbDir
        gLogger.debug( line )
        line += 'export HOME=%s\n' % mysqlDir
      f.write( line )
    f.close()
  except:
    error = 'Failed to Update MySQL startup script'
    gLogger.exception( error )
    if exitOnError:
      exit( -1 )
    return S_ERROR( error )

  return S_OK()


def mysqlInstalled( doNotExit = False ):
  """
  Check if MySQL is already installed
  """

  if os.path.exists( mysqlDbDir ) or os.path.exists( mysqlLogDir ):
    return S_OK()
  if doNotExit:
    return S_ERROR()

  error = 'MySQL not properly Installed'
  gLogger.error( error )
  if exitOnError:
    exit( -1 )
  return S_ERROR( error )

def getMySQLPasswords():
  """
  Get MySQL passwords from local configuration or prompt
  """
  import getpass
  global mysqlRootPwd, mysqlPassword
  if not mysqlRootPwd:
    mysqlRootPwd = getpass.getpass( 'MySQL root password: ' )
  if not mysqlPassword:
    mysqlPassword = getpass.getpass( 'MySQL Dirac password: ' )

  return S_OK()

def setMySQLPasswords( root = '', dirac = '' ):
  """
  Set MySQL passwords
  """
  global mysqlRootPwd, mysqlPassword
  if root:
    mysqlRootPwd = root
  if dirac:
    mysqlPassword = dirac

  return S_OK()

def startMySQL():
  """
  Start MySQL server
  """
  result = mysqlInstalled()
  if not result['OK']:
    return result
  return execCommand( 0, [mysqlStartupScript, 'start'] )

def stopMySQL():
  """
  Stop MySQL server
  """
  result = mysqlInstalled()
  if not result['OK']:
    return result
  return execCommand( 0, [mysqlStartupScript, 'stop'] )

def installMySQL():
  """
  Attempt an installation of MySQL
  mode:
    Master
    Slave
    None
  """
  if mysqlInstalled( doNotExit = True )['OK']:
    gLogger.info( 'MySQL already installed' )
    return S_OK()

  if mysqlMode.lower() not in [ '', 'master', 'slave' ]:
    error = 'Unknown MySQL server Mode'
    if exitOnError:
      gLogger.fatal( error, mysqlMode )
      exit( -1 )
    gLogger.error( error, mysqlMode )
    return S_ERROR( error )

  if mysqlHost:
    gLogger.info( 'Installing MySQL server at', mysqlHost )

  if mysqlMode:
    gLogger.info( 'This is a MySQl %s server' % mysqlMode )

  fixMySQLScripts()

  try:
    os.makedirs( mysqlDbDir )
    os.makedirs( mysqlLogDir )
  except:
    error = 'Can not create MySQL dirs'
    gLogger.exception( error )
    if exitOnError:
      exit( -1 )
    return S_ERROR( error )

  try:
    f = open( mysqlMyOrg, 'r' )
    myOrg = f.readlines()
    f.close()

    f = open( mysqlMyCnf, 'w' )
    for line in myOrg:
      if line.find( '[mysqld]' ) == 0:
        line += '\n'.join( [ 'innodb_file_per_table', '' ] )
      elif line.find( 'innodb_log_arch_dir' ) == 0:
        line = ''
      elif line.find( 'innodb_data_file_path' ) == 0:
        line = line.replace( '2000M', '200M' )
      elif line.find( 'server-id' ) == 0 and mysqlMode.lower() == 'master':
        # MySQL Configuration for Master Server
        line = '\n'.join( ['server-id = 1',
                           '# DIRAC Master-Server',
                           'sync-binlog = 1',
                           'replicate-ignore-table = mysql.MonitorData',
                           '# replicate-ignore-db=db_name',
                           'log-bin = mysql-bin',
                           'log-slave-updates', '' ] )
      elif line.find( 'server-id' ) == 0 and mysqlMode.lower() == 'slave':
        # MySQL Configuration for Slave Server
        import time
        line = '\n'.join( ['server-id = %s' % int( time.time() ),
                           '# DIRAC Slave-Server',
                           'sync-binlog = 1',
                           'replicate-ignore-table = mysql.MonitorData',
                           '# replicate-ignore-db=db_name',
                           'log-bin = mysql-bin',
                           'log-slave-updates', '' ] )
      elif line.find( '/opt/dirac/mysql' ) > -1:
        line = line.replace( '/opt/dirac/mysql', mysqlDir )

      if mysqlSmallMem:
        if line.find( 'innodb_buffer_pool_size' ) == 0:
          line = 'innodb_buffer_pool_size = 200M\n'
      elif mysqlLargeMem:
        if line.find( 'innodb_buffer_pool_size' ) == 0:
          line = 'innodb_buffer_pool_size = 10G\n'

      f.write( line )
    f.close()
  except:
    error = 'Can not create my.cnf'
    gLogger.exception( error )
    if exitOnError:
      exit( -1 )
    return S_ERROR( error )

  gLogger.info( 'Initializing MySQL...' )
  result = execCommand( 0, ['mysql_install_db',
                            '--defaults-file=%s' % mysqlMyCnf,
                            '--datadir=%s' % mysqlDbDir ] )
  if not result['OK']:
    return result

  gLogger.info( 'Starting MySQL...' )
  result = startMySQL()
  if not result['OK']:
    return result

  gLogger.info( 'Setting MySQL root password' )
  result = execCommand( 0, ['mysqladmin', '-u', 'root', 'password', mysqlRootPwd] )
  if not result['OK']:
    return result
  if mysqlHost:
    result = execCommand( 0, ['mysqladmin', '-u', 'root',
                              '-h', '%s' % mysqlHost, 'password', mysqlRootPwd] )
    if not result['OK']:
      return result

  if not _addMySQLToDiracCfg():
    return S_ERROR( 'Failed to add MySQL user password to local configuration' )

  return S_OK()

def getMySQLStatus():
  """
  Get the status of the MySQL database installation
  """
  result = execCommand( 0, ['mysqladmin', 'status' ] )
  if not result['OK']:
    return result
  output = result['Value'][1]
  d1, uptime, nthreads, nquestions, nslow, nopens, nflash, nopen, nqpersec = output.split( ':' )
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

def getAvailableDatabases( extensions ):

  dbDict = {}
  for extension in extensions + ['']:
    databases = glob.glob( os.path.join( rootPath, '%sDIRAC' % extension, '*', 'DB', '*.sql' ) )
    for db in databases:
      dbName = os.path.basename( db ).replace( '.sql', '' )
      dbDict[dbName] = {}
      dbDict[dbName]['Extension'] = extension
      dbDict[dbName]['System'] = db.split('/')[-3].replace('System','')

  return S_OK( dbDict )


def getDatabases():
  """
  Get the list of installed databases
  """
  result = execMySQL( 'SHOW DATABASES' )
  if not result['OK']:
    return result
  dbList = []
  for db in result['Value']:
    if not db[0] in ['Database', 'information_schema', 'mysql', 'test']:
      dbList.append( db[0] )

  return S_OK( dbList )


def installDatabase( dbName ):
  """
  Install requested DB in MySQL server
  """
  
  global mysqlRootPwd, mysqlPassword
  
  result = mysqlInstalled()
  if not result['OK']:
    return result

  if not mysqlRootPwd:
    rootPwdPath = cfgInstallPath( 'Database', 'RootPwd' )
    return S_ERROR( 'Missing %s in %s' % ( rootPwdPath, cfgFile ) )

  if not mysqlPassword:
    mysqlPassword = localCfg.getOption(cfgPath( 'Systems', 'Databases', 'Password' ),mysqlPassword)
    if not mysqlPassword:
      mysqlPwdPath = cfgPath( 'Systems', 'Databases', 'Password' )
      return S_ERROR( 'Missing %s in %s' % ( mysqlPwdPath, cfgFile ) )

  gLogger.info( 'Installing', dbName )

  dbFile = glob.glob( os.path.join( rootPath, '*', '*', 'DB', '%s.sql' % dbName ) )

  if not dbFile:
    error = 'Database %s not found' % dbName
    gLogger.error( error )
    if exitOnError:
      exit( -1 )
    return S_ERROR( error )

  dbFile = dbFile[0]

  try:
    f = open( dbFile )
    dbLines = f.readlines()
    f.close()
    dbAdded = False
    cmdLines = []
    for l in dbLines:
      if l.lower().find( ( 'use %s;' % dbName ).lower() ) > -1:
        result = execMySQL( 'CREATE DATABASE `%s`' % dbName )
        if not result['OK']:
          gLogger.error( result['Message'] )
          if exitOnError:
            exit( -1 )
          return result

        result = execMySQL( 'SHOW STATUS' )
        if not result['OK']:
          error = 'Could not connect to MySQL server'
          gLogger.error( error )
          if exitOnError:
            exit( -1 )
          return S_ERROR( error )
        for cmd in ["GRANT SELECT,INSERT,LOCK TABLES,UPDATE,DELETE,CREATE,DROP,ALTER ON `%s`.* TO 'Dirac'@'localhost' IDENTIFIED BY '%s'" % ( dbName, mysqlPassword ),
                    "GRANT SELECT,INSERT,LOCK TABLES,UPDATE,DELETE,CREATE,DROP,ALTER ON `%s`.* TO 'Dirac'@'%s' IDENTIFIED BY '%s'" % ( dbName, mysqlHost, mysqlPassword ),
                    "GRANT SELECT,INSERT,LOCK TABLES,UPDATE,DELETE,CREATE,DROP,ALTER ON `%s`.* TO 'Dirac'@'%%' IDENTIFIED BY '%s'" % ( dbName, mysqlPassword ),
                    ]:
          result = execMySQL( cmd )
          if not result['OK']:
            error = 'Error setting MySQL permissions'
            gLogger.error( error, result['Message'] )
            if exitOnError:
              exit( -1 )
            return S_ERROR( error )
          dbAdded = True
          result = execMySQL( 'FLUSH PRIVILEGES' )
          if not result['OK']:
            gLogger.error( result['Message'] )
            if exitOnError:
              exit( -1 )
            return result

      elif dbAdded:
        if l.strip():
          cmdLines.append( l.strip() )
        if l.strip() and l.strip()[-1] == ';':
          result = execMySQL( '\n'.join( cmdLines ), dbName )
          if not result['OK']:
            error = 'Failed to initialize Database'
            gLogger.info( '\n'.join( cmdLines ) )
            gLogger.error( error, result['Message'] )
            if exitOnError:
              exit( -1 )
            return S_ERROR( error )
          cmdLines = []

    # last line might not have the last ";"
    if cmdLines:
      cmd = '\n'.join( cmdLines )
      if cmd.lower().find( 'source' ) == 0:
        try:
          dbFile = cmd.split()[1]
          f = open( dbFile )
          dbLines = f.readlines()
          f.close()
          cmdLines = []
          for l in dbLines:
            if l.strip():
              cmdLines.append( l.strip() )
            if l.strip() and l.strip()[-1] == ';':
              result = execMySQL( '\n'.join( cmdLines ), dbName )
              if not result['OK']:
                error = 'Failed to initialize Database'
                gLogger.info( '\n'.join( cmdLines ) )
                gLogger.error( error, result['Message'] )
                if exitOnError:
                  exit( -1 )
                return S_ERROR( error )
              cmdLines = []
        except:
          error = 'Failed to %s' % cmd
          gLogger.exception( error )
          if exitOnError:
            exit( -1 )
          return S_ERROR( error )

    if not dbAdded:
      error = 'Missing "use %s;"' % dbName
      gLogger.error( error )
      if exitOnError:
        exit( -1 )
      return S_ERROR( error )

  except:
    error = 'Failed to create Database'
    gLogger.exception( error )
    if exitOnError:
      exit( -1 )
    return S_ERROR( error )

  return S_OK( dbFile.split( '/' )[-4:-2] )

def execMySQL( cmd, dbName = 'mysql' ):
  """
  Execute MySQL Command
  """
  global db
  from DIRAC.Core.Utilities.MySQL import MySQL
  if not mysqlRootPwd:
    return S_ERROR( 'MySQL root password is not defined' )
  if dbName not in db:
    db[dbName] = MySQL( mysqlHost, 'root', mysqlRootPwd, dbName )
  if not db[dbName]._connected:
    error = 'Could not connect to MySQL server'
    gLogger.error( error )
    if exitOnError:
      exit( -1 )
    return S_ERROR( error )
  return db[dbName]._query( cmd )

def _addMySQLToDiracCfg():
  """
  Add the database access info to the local configuration
  """
  if not mysqlPassword:
    return S_ERROR( 'Missing %s in %s' % ( cfgInstallPath( 'Database', 'Password' ), cfgFile ) )

  sectionPath = cfgPath( 'Systems', 'Databases' )
  cfg = __getCfg( sectionPath, 'User', mysqlUser )
  cfg.setOption( cfgPath( sectionPath, 'Password' ), mysqlPassword )

  return _addCfgToDiracCfg( cfg )

def execCommand( timeout, cmd ):
  """
  Execute command tuple and handle Error cases
  """
  result = systemCall( timeout, cmd )
  if not result['OK']:
    if timeout and result['Message'].find( 'Timeout' ) == 0:
      return result
    gLogger.error( 'Failed to execute', cmd[0] )
    gLogger.error( result['Message'] )
    if exitOnError:
      exit( -1 )
    return result

  if result['Value'][0]:
    error = 'Failed to execute'
    gLogger.error( error, cmd[0] )
    gLogger.error( 'Exit code:' , ( '%s\n' % result['Value'][0] ) + '\n'.join( result['Value'][1:] ) )
    if exitOnError:
      exit( -1 )
    error = S_ERROR( error )
    error['Value'] = result['Value']
    return error

  gLogger.verbose( result['Value'][1] )

  return result
