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

/LocalInstallation/LogLevel:      LogLevel set in "run" script for all components installed
/LocalInstallation/RootPath:      Used instead of rootPath in "run" script if defined (if links are used to named versions)
/LocalInstallation/Host:          Used when build the URL to be published for the installed service
/LocalInstallation/InstancePath:  Location where runit and startup directories are created (default rootPath)
/LocalInstallation/RunitDir:      Location where runit directory is created (default InstancePath/runit)
/LocalInstallation/StartupDir:    Location where startup directory is created (default InstancePath/startup)
/LocalInstallation/MySQLDir:      Location where mysql databases are created (default InstancePath/mysql)

/LocalInstallation/Databases/User:                 (default Dirac)
/LocalInstallation/Databases/Password:             (must be set for SystemAdministrator Service to work)
/LocalInstallation/Databases/RootPwd:              (must be set for SystemAdministrator Service to work)
/LocalInstallation/Databases/Host:                 (must be set for SystemAdministrator Service to work)

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

exitOnError = False

gLogger.info( 'DIRAC Root Path =', rootPath )

localCfg = CFG()
cfgFile = os.path.join( rootPath, 'etc', 'dirac.cfg' )
localCfg.loadFromFile( cfgFile )

setup = localCfg.getOption( '/DIRAC/Setup', '' )
logLevel = localCfg.getOption( '/LocalInstallation/LogLevel', 'INFO' )
linkedRootPath = localCfg.getOption( '/LocalInstallation/RootPath', rootPath )
host = localCfg.getOption( '/LocalInstallation/Host', '' )


instancePath = os.path.dirname( rootPath )
instancePath = localCfg.getOption( '/LocalInstallation/InstancePath', instancePath )
gLogger.info( 'Using Instance Base Dir at', instancePath )

runitDir = os.path.join( instancePath, 'runit' )
runitDir = localCfg.getOption( '/LocalInstallation/RunitDir', runitDir )
gLogger.info( 'Using Runit Dir at', runitDir )

startDir = os.path.join( instancePath, 'startup' )
startDir = localCfg.getOption( '/LocalInstallation/StartupDir', startDir )
gLogger.info( 'Using Startup Dir at', startDir )

db = False

mysqlDir = os.path.join( instancePath, 'mysql' )
mysqlDir = localCfg.getOption( '/LocalInstallation/MySQLDir', mysqlDir )
gLogger.info( 'Using MySQL Dir at', mysqlDir )

mysqlDbDir = os.path.join( mysqlDir, 'db' )
mysqlLogDir = os.path.join( mysqlDir, 'log' )
mysqlMyOrg = os.path.join( rootPath, 'mysql', 'etc', 'my.cnf' )
mysqlMyCnf = os.path.join( mysqlDir, '.my.cnf' )


mysqlStartupScript = os.path.join( rootPath, 'mysql', 'share', 'mysql', 'mysql.server' )

mysqlRootPwd = localCfg.getOption( '/LocalInstallation/Databases/RootPwd', '' )
if mysqlRootPwd:
  gLogger.info( 'Reading Root MySQL Password from local configuration' )

mysqlUser = localCfg.getOption( '/LocalInstallation/Databases/User', '' )
if mysqlUser:
  gLogger.info( 'Reading MySQL User from local configuration' )
else:
  mysqlUser = 'Dirac'

mysqlPassword = localCfg.getOption( '/LocalInstallation/Databases/Password', '' )
if mysqlPassword:
  gLogger.info( 'Reading %s MySQL Password from local configuration' % mysqlUser )

mysqlHost = localCfg.getOption( '/LocalInstallation/Databases/Host', '' )
if mysqlHost:
  gLogger.info( 'Using MySQL Host from local configuration', mysqlHost )

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

def _addCfgToDiracCfg( cfg ):
  """
  Merge cfg into existing dirac.cfg file
  """
  global localCfg
  newCfg = localCfg.mergeWith( cfg )
  result = newCfg.writeToFile( cfgFile )
  if not result:
    return result
  localCfg.loadFromFile( cfgFile )
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
    cfg.createNewSection( '/'.join( sectionList ) )
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
  optionName = option.split( '/' )[-1]
  section = '/'.join( option.split( '/' )[:-1] )
  cfg = __getCfg( section, optionName, value )

  if not cfg:
    return S_ERROR( 'Wrong option: %s = %s' % ( option, value ) )

  if _addCfgToDiracCfg( cfg ):
    return S_OK()

  return S_ERROR( 'Could not merge %s=%s with local configuration' % ( option, value ) )

def addDefaultOptionsToCS( gConfig, componentType, system, component, extensions, overwrite = False ):
  """ Add the section with the component options to the CS
  """
  instance = localCfg.getOption( '/DIRAC/Setups/%s/%s' % ( setup, system ), '' )
  if not instance:
    return S_ERROR( '/DIRAC/Setups/%s/%s not defined in %s' % ( setup, system, cfgFile ) )

  sectionName = "Agents"
  if componentType == 'service':
    sectionName = "Services"

  # Check if the component CS options exist
  addOptions = True
  if not overwrite:
    result = gConfig.getOptions( '/Systems/%s/%s/%s/%s' % ( system, instance, sectionName, component ) )
    if result['OK']:
      addOptions = False
  if not addOptions:
    return S_OK( 'Component options already exist' )

  # Add the component options now
  result = getComponentCfg( componentType, system, component, instance, extensions )
  if not result['OK']:
    return result
  compCfg = result['Value']

  return _addCfgToCS( compCfg )

def addDefaultOptionsToComponentCfg( componentType, system, component, extensions ):
  """
  Add default component options local component cfg
  """
  instance = localCfg.getOption( '/DIRAC/Setups/%s/%s' % ( setup, system ), '' )
  if not instance:
    return S_ERROR( '/DIRAC/Setups/%s/%s not defined in %s' % ( setup, system, cfgFile ) )

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
    cfgPath = os.path.join( rootPath, ext, '%sSystem' % system, 'ConfigTemplate.cfg' )
    if os.path.exists( cfgPath ):
      gLogger.info( 'Loading configuration template', cfgPath )
      # Look up the component in this template
      loadCfg = CFG()
      loadCfg.loadFromFile( cfgPath )
      try:
        compCfg = loadCfg[sectionName][component]
      except:
        error = 'Can not find %s/%s in template' % ( sectionName, component )
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

  section = '/'.join( ['Systems', system, instance, sectionName] )
  cfg = __getCfg( section )
  cfg.createNewSection( '/'.join( [section, component] ), '', compCfg )

  # Add the service URL
  if componentType == "service":
    port = compCfg.getOption( '/Port', 0 )
    if port and host:
      cfg.createNewSection( '/'.join( ['Systems', system, instance, 'URLs'] ) )
      cfg.setOption( '/'.join( ['Systems', system, instance, 'URLs', component] ),
                    'dips://%s:%d/%s/%s' % ( host, port, system, component ) )

  return S_OK( cfg )

def addDatabaseOptionsToCS( gConfig, systemName, dbName, overwrite = False ):
  """
  Add the section with the database options to the CS
  """
  system = systemName.replace( 'System', '' )
  instance = localCfg.getOption( '/DIRAC/Setups/%s/%s' % ( setup, system ), '' )
  if not instance:
    return S_ERROR( '/DIRAC/Setups/%s/%s not defined in %s' % ( setup, system, cfgFile ) )

  # Check if the component CS options exist
  addOptions = True
  if not overwrite:
    result = gConfig.getOptions( '/Systems/%s/%s/Databases/%s' % ( system, instance, dbName ) )
    if result['OK']:
      addOptions = False
  if not addOptions:
    return S_OK( 'Database options already exist' )

  # Add the component options now
  result = getDatabaseCfg( system, dbName, instance )
  if not result['OK']:
    return result
  databaseCfg = result['Value']

  return _addCfgToCS( databaseCfg )

def getDatabaseCfg( system, dbName, instance ):
  """ 
  Get the CFG object of the database configuration
  """
  section = '/'.join( ['Systems', system, instance, 'Databases', dbName] )
  print section
  cfg = __getCfg( section, 'DBName', dbName )
  cfg.setOption( '/'.join( [section, 'Host'] ), mysqlHost )

  return S_OK( cfg )

def addSystemInstance( system, instance ):
  """ 
  Add a new system instance to dirac.cfg and CS
  """

  cfg = __getCfg( '/'.join( ['DIRAC', 'Setups', setup] ), system, instance )
  if not _addCfgToDiracCfg( cfg ):
    return S_ERROR( 'Failed to add system instance to dirac.cfg' )

  return _addCfgToCS( cfg )

def getSoftwareComponents( extensions ):
  """  Get the list of all the components ( services and agents ) for which the software
       is installed on the system
  """

  services = {}
  agents = {}

  for extension in ['DIRAC'] + [ x + 'DIRAC' for x in extensions]:
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
        cList.append( os.path.join( startDir, '_'.join( componentTuple ) ) )
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

def setupSite():
  """
  Setup a new site using the options defined
  """
  return S_OK()

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
    f = open( componentCfg, 'w' )
    f.close()

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

    os.chmod( logRunFile, defaultPerms )
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
  if not os.path.lexists( startCompDir ):
    gLogger.info( 'Creating startup link at', startCompDir )
    os.symlink( runitCompDir, startCompDir )
    time.sleep( 5 )

  # Check the runsv status
  start = time.time()
  while ( time.time() - 10 ) < start:
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


def mysqlInstalled():
  """
  Check if MySQL is already installed
  """

  if os.path.exists( mysqlDbDir ) or os.path.exists( mysqlLogDir ):
    return S_OK()

  return S_ERROR()

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
  if not mysqlInstalled()['OK']:
    return S_ERROR( 'MySQL not properly Installed' )

  return execCommand( 0, [mysqlStartupScript, 'start'] )

def stopMySQL():
  """
  Stop MySQL server
  """
  if not mysqlInstalled()['OK']:
    return S_ERROR( 'MySQL not properly Installed' )

  return execCommand( 0, [mysqlStartupScript, 'stop'] )

def installMySQL():
  """
  Attempt an installation of MySQL
  mode:
    Master
    Slave
    None
  """
  if mysqlInstalled()['OK']:
    gLogger.info( 'MySQL already installed' )
    return S_OK()

  mysqlMode = localCfg.getOption( '/LocalInstallation/Databases/MySQLMode', '' )

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

  mysqlSmallMem = localCfg.getOption( '/LocalInstallation/Databases/MySQLSmallMem', False )

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
      elif line.find( 'server-id' ) == 0 and mysqlMode.lower() == 'master':
        # MySQL Configuration for Master Server
        line = '\n'.join( ['server-id = 1',
                           '# DIRAC Master-Server',
                           'sync-binlog = 1',
                           'replicate-ignore-table = mysql.MonitorData',
                           '# replicate-ignore-db=db_name',
                           'log-bin = mysql-bin'
                           'log-slave-updates' ] )
      elif line.find( 'server-id' ) == 0 and mysqlMode.lower() == 'slave':
        # MySQL Configuration for Slave Server
        import time
        line = '\n'.join( ['server-id = %s' % int( time.time() ),
                           '# DIRAC Slave-Server',
                           'sync-binlog = 1',
                           'replicate-ignore-table = mysql.MonitorData',
                           '# replicate-ignore-db=db_name',
                           'log-bin = mysql-bin'
                           'log-slave-updates' ] )
      elif line.find( '/opt/dirac/mysql' ) > -1:
        line = line.replace( '/opt/dirac/mysql', mysqlDir )

      # TODO: if mysqlSmallMem need to fix the size of some buffers

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
    result = execCommand( 0, ['mysqladmin', '-u', 'root', '-p%s' % mysqlRootPwd,
                              '-h', '%s' % mysqlHost, 'password', mysqlRootPwd] )
    if not result['OK']:
      return result
  result = execCommand( 0, ['mysqladmin', '-u', 'root', '-p%s' % mysqlRootPwd,
                            'flush-privileges'] )
  if not result['OK']:
    return result

  if not _addMySQLToDiracCfg():
    return S_ERROR( 'Failed to add MySQL logging info to local configuration' )

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

  dbList = []
  for extension in extensions + ['']:
    databases = glob.glob( os.path.join( rootPath, 'DIRAC%s' % extension, '*', 'DB', '*.sql' ) )
    for db in databases:
      dbName = os.path.basename( db )
      if not dbName in dbList:
        dbList.append( dbName.replace( '.sql', '' ) )

  return S_OK( dbList )


def getDatabases():
  """
  Get the list of installed databases
  """
  result = execMySQL( 'SHOW DATABASES', 'mysql' )
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
  print os.getcwd()
  if not mysqlInstalled()['OK']:
    error = 'MySQL not installed'
    gLogger.error( error )
    if exitOnError:
      exit( -1 )
    return S_ERROR( error )

  if not mysqlRootPwd:
    return S_ERROR( 'Missing /LocalInstallation/Databases/RootPwd in %s' % cfgFile )

  if not mysqlPassword:
    return S_ERROR( 'Missing /LocalInstallation/Databases/Password in %s' % cfgFile )

  dbFile = glob.glob( os.path.join( rootPath, '*', '*', 'DB', '%s.sql' % dbName ) )

  if not dbFile:
    error = 'Database %s not found' % dbName
    gLogger.error( error )
    if exitOnError:
      exit( -1 )
    return S_ERROR( error )

  dbFile = dbFile[0]

  try:
    from DIRAC.Core.Utilities.MySQL import MySQL
    f = open( dbFile )
    dbLines = f.readlines()
    f.close()
    dbAdded = False
    cmdLines = []
    for l in dbLines:
      if l.lower().find( ( 'use %s;' % dbName ).lower() ) > -1:
        result = execCommand( 0, [ 'mysqladmin', '-u', 'root', '-p%s' % mysqlRootPwd,
                                  'create', dbName] )
        if not result['OK']:
          gLogger.error( result['Message'] )
          if exitOnError:
            exit( -1 )
          return result

        db = MySQL( mysqlHost, 'root', mysqlRootPwd, dbName )
        if not db._connected:
          error = 'Could not connect to MySQL server'
          gLogger.error( error )
          if exitOnError:
            exit( -1 )
          return S_ERROR( error )
        for cmd in ["GRANT SELECT,INSERT,LOCK TABLES,UPDATE,DELETE,CREATE,DROP,ALTER ON `%s`.* TO 'Dirac'@'localhost' IDENTIFIED BY '%s'" % ( dbName, mysqlPassword ),
                    "GRANT SELECT,INSERT,LOCK TABLES,UPDATE,DELETE,CREATE,DROP,ALTER ON `%s`.* TO 'Dirac'@'%s' IDENTIFIED BY '%s'" % ( dbName, mysqlHost, mysqlPassword ),
                    "GRANT SELECT,INSERT,LOCK TABLES,UPDATE,DELETE,CREATE,DROP,ALTER ON `%s`.* TO 'Dirac'@'%%' IDENTIFIED BY '%s'" % ( dbName, mysqlPassword ),
                    ]:
          result = db._query( cmd )
          if not result['OK']:
            error = 'Error setting MySQL permissions'
            gLogger.error( error, result['Message'] )
            if exitOnError:
              exit( -1 )
            return S_ERROR( error )
          dbAdded = True
          result = execCommand( 0, ['mysqladmin', '-u', 'root', '-p%s' % mysqlRootPwd,
                                    'flush-privileges'] )
          if not result['OK']:
            gLogger.error( result['Message'] )
            if exitOnError:
              exit( -1 )
            return result

      elif dbAdded:
        if l.strip():
          cmdLines.append( l.strip() )
        if l.strip() and l.strip()[-1] == ';':
          result = db._query( '\n'.join( cmdLines ) )
          if not result['OK']:
            error = 'Failed to initialize Database'
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
              result = db._query( '\n'.join( cmdLines ) )
              if not result['OK']:
                error = 'Failed to initialize Database'
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

    del db
  except:
    error = 'Failed to create Database'
    gLogger.exception( error )
    if exitOnError:
      exit( -1 )
    return S_ERROR( error )

  return S_OK( dbFile.split( '/' )[-4:-2] )

def execMySQL( cmd, dbName ):
  """
  Execute MySQL Command
  """
  global db
  from DIRAC.Core.Utilities.MySQL import MySQL
  if not db:
    db = MySQL( mysqlHost, 'root', mysqlRootPwd, dbName )
  if not db._connected:
    error = 'Could not connect to MySQL server'
    gLogger.error( error )
    if exitOnError:
      exit( -1 )
    return S_ERROR( error )
  return db._query( cmd )

def _addMySQLToDiracCfg():
  """
  Add the database access info to the local configuration
  """
  if not mysqlPassword:
    return S_ERROR( 'Missing /LocalInstallation/Databases/Password in %s' % cfgFile )

  section = '/'.join( ['Systems', 'Databases'] )
  cfg = __getCfg( section, 'User', mysqlUser )
  cfg.setOption( '/'.join( [section, 'Password'] ), mysqlPassword )

  return _addCfgToDiracCfg( cfg )

def installService():
  pass

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

