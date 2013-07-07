# $HeadURL$

""" SystemAdministrator service is a tool to control and monitor the DIRAC services and agents
"""

__RCSID__ = "$Id$"

from types import *
import os, re, commands, getpass
from datetime import timedelta
from DIRAC import S_OK, S_ERROR, gConfig, rootPath, gLogger
from DIRAC.Core.DISET.RequestHandler import RequestHandler
from DIRAC.ConfigurationSystem.Client.Helpers.CSGlobals import getCSExtensions
from DIRAC.Core.Utilities import InstallTools, CFG, Os
from DIRAC.Core.Utilities.Time import dateTime, fromString, hour, day
from DIRAC.Core.Utilities.Subprocess import shellCall, systemCall
from DIRAC.Core.Security.Locations import getHostCertificateAndKeyLocation
from DIRAC.Core.Security.X509Chain import X509Chain
import DIRAC

class SystemAdministratorHandler( RequestHandler ):

  types_getInfo = [ ]
  def export_getInfo( self ):
    """  Get versions of the installed DIRAC software and extensions, setup of the
         local installation
    """
    return InstallTools.getInfo( getCSExtensions() )

  types_getSoftwareComponents = [ ]
  def export_getSoftwareComponents( self ):
    """  Get the list of all the components ( services and agents ) for which the software
         is installed on the system
    """
    return InstallTools.getSoftwareComponents( getCSExtensions() )

  types_getInstalledComponents = [ ]
  def export_getInstalledComponents( self ):
    """  Get the list of all the components ( services and agents )
         installed on the system in the runit directory
    """
    return InstallTools.getInstalledComponents()

  types_getSetupComponents = [ ]
  def export_getSetupComponents( self ):
    """  Get the list of all the components ( services and agents )
         set up for running with runsvdir in /opt/dirac/startup directory
    """
    return InstallTools.getSetupComponents()

  types_getOverallStatus = []
  def export_getOverallStatus( self ):
    """  Get the complete status information for the components in the
         given list
    """
    result = InstallTools.getOverallStatus( getCSExtensions() )
    if not result['OK']:
      return result
    statusDict = result['Value']
    for compType in statusDict:
      for system in statusDict[compType]:
        for component in statusDict[compType][system]:
          result = InstallTools.getComponentModule( gConfig,system,component,compType )
          if not result['OK']:
            statusDict[compType][system][component]['Module'] = "Unknown"
          else:
            statusDict[compType][system][component]['Module'] = result['Value']
    return S_OK(statusDict)   

  types_getStartupComponentStatus = [ ListType ]
  def export_getStartupComponentStatus( self, componentTupleList ):
    """  Get the list of all the components ( services and agents )
         set up for running with runsvdir in startup directory
    """
    return InstallTools.getStartupComponentStatus( componentTupleList )

  types_installComponent = [ StringTypes, StringTypes, StringTypes ]
  def export_installComponent( self, componentType, system, component, componentModule='' ):
    """ Install runit directory for the specified component
    """
    return InstallTools.installComponent( componentType, system, component, getCSExtensions(), componentModule )

  types_setupComponent = [ StringTypes, StringTypes, StringTypes ]
  def export_setupComponent( self, componentType, system, component, componentModule='' ):
    """ Setup the specified component for running with the runsvdir daemon
        It implies installComponent
    """
    result = InstallTools.setupComponent( componentType, system, component, getCSExtensions(), componentModule )
    gConfig.forceRefresh()
    return result

  types_addDefaultOptionsToComponentCfg = [ StringTypes, StringTypes ]
  def export_addDefaultOptionsToComponentCfg( self, componentType, system, component ):
    """ Add default component options local component cfg
    """
    return InstallTools.addDefaultOptionsToComponentCfg( componentType, system, component, getCSExtensions() )

  types_unsetupComponent = [ StringTypes, StringTypes ]
  def export_unsetupComponent( self, system, component ):
    """ Removed the specified component from running with the runsvdir daemon
    """
    return InstallTools.unsetupComponent( system, component )

  types_uninstallComponent = [ StringTypes, StringTypes ]
  def export_uninstallComponent( self, system, component ):
    """ Remove runit directory for the specified component
        It implies unsetupComponent
    """
    return InstallTools.uninstallComponent( system, component )

  types_startComponent = [ StringTypes, StringTypes ]
  def export_startComponent( self, system, component ):
    """ Start the specified component, running with the runsv daemon
    """
    return InstallTools.runsvctrlComponent( system, component, 'u' )

  types_restartComponent = [ StringTypes, StringTypes ]
  def export_restartComponent( self, system, component ):
    """ Restart the specified component, running with the runsv daemon
    """
    return InstallTools.runsvctrlComponent( system, component, 't' )

  types_stopComponent = [ StringTypes, StringTypes ]
  def export_stopComponent( self, system, component ):
    """ Stop the specified component, running with the runsv daemon
    """
    return InstallTools.runsvctrlComponent( system, component, 'd' )

  types_getLogTail = [ StringTypes, StringTypes ]
  def export_getLogTail( self, system, component, length = 100 ):
    """ Get the tail of the component log file
    """
    return InstallTools.getLogTail( system, component, length )

######################################################################################
#  Database related methods
#
  types_getMySQLStatus = [ ]
  def export_getMySQLStatus( self ):
    """ Get the status of the MySQL database installation
    """
    return InstallTools.getMySQLStatus()

  types_getDatabases = [ ]
  def export_getDatabases( self, mysqlPassword = None ):
    """ Get the list of installed databases
    """
    if mysqlPassword :
      InstallTools.setMySQLPasswords( mysqlPassword )
    return InstallTools.getDatabases()

  types_getAvailableDatabases = [ ]
  def export_getAvailableDatabases( self ):
    """ Get the list of databases which software is installed in the system
    """
    return InstallTools.getAvailableDatabases( getCSExtensions() )

  types_installMySQL = []
  def export_installMySQL( self, mysqlPassword = None, diracPassword = None ):
    """ Install MySQL database server
    """

    if mysqlPassword or diracPassword:
      InstallTools.setMySQLPasswords( mysqlPassword, diracPassword )
    if InstallTools.mysqlInstalled()['OK']:
      return S_OK( 'Already installed' )

    result = InstallTools.installMySQL()
    if not result['OK']:
      return result

    return S_OK( 'Successfully installed' )

  types_installDatabase = [ StringTypes ]
  def export_installDatabase( self, dbName, mysqlPassword = None ):
    """ Install a DIRAC database named dbName
    """
    if mysqlPassword :
      InstallTools.setMySQLPasswords( mysqlPassword )
    return InstallTools.installDatabase( dbName )

  types_addDatabaseOptionsToCS = [ StringTypes, StringTypes ]
  def export_addDatabaseOptionsToCS( self, system, database, overwrite = False ):
    """ Add the section with the database options to the CS
    """
    return InstallTools.addDatabaseOptionsToCS( gConfig, system, database, overwrite = overwrite )

  types_addDefaultOptionsToCS = [StringTypes, StringTypes, StringTypes]
  def export_addDefaultOptionsToCS( self, componentType, system, component, overwrite = False ):
    """ Add default component options to the global CS or to the local options
    """
    return InstallTools.addDefaultOptionsToCS( gConfig, componentType, system, component,
                                               getCSExtensions(),
                                               overwrite = overwrite )

#######################################################################################
# General purpose methods
#
  types_updateSoftware = [ StringTypes ]
  def export_updateSoftware( self, version, rootPath = "", gridVersion = "" ):
    """ Update the local DIRAC software installation to version
    """

    # Check that we have a sane local configuration
    result = gConfig.getOptionsDict( '/LocalInstallation' )
    if not result['OK']:
      return S_ERROR( 'Invalid installation - missing /LocalInstallation section in the configuration' )
    elif not result['Value']:
      return S_ERROR( 'Invalid installation - empty /LocalInstallation section in the configuration' )

    if rootPath and not os.path.exists( rootPath ):
      return S_ERROR( 'Path "%s" does not exists' % rootPath )
    # For LHCb we need to check Oracle client
    installOracleClient = False
    oracleFlag = gConfig.getValue( '/LocalInstallation/InstallOracleClient', 'unknown' )
    if oracleFlag.lower() in ['yes', 'true', '1']:
      installOracleClient = True
    elif oracleFlag.lower() == "unknown":
      result = systemCall( 30, ['python', '-c', 'import cx_Oracle'] )
      if result['OK'] and result['Value'][0] == 0:
        installOracleClient = True

    cmdList = ['dirac-install', '-r', version, '-t', 'server']
    if rootPath:
      cmdList.extend( ['-P', rootPath] )

    # Check if there are extensions
    extensionList = getCSExtensions()
    webFlag = gConfig.getValue( '/LocalInstallation/WebPortal', False )
    if webFlag:
      extensionList.append( 'Web' )
    if extensionList:
      cmdList += ['-e', ','.join( extensionList )]

    # Are grid middleware bindings required ?
    if gridVersion:
      cmdList.extend( ['-g', gridVersion] )

    targetPath = gConfig.getValue( '/LocalInstallation/TargetPath',
                                  gConfig.getValue( '/LocalInstallation/RootPath', '' ) )
    if targetPath and os.path.exists( targetPath + '/etc/dirac.cfg' ):
      cmdList.append( targetPath + '/etc/dirac.cfg' )
    else:
      return S_ERROR( 'Local configuration not found' )

    result = systemCall( 240, cmdList )
    if not result['OK']:
      return result
    status = result['Value'][0]
    if status != 0:
      # Get error messages
      error = []
      output = result['Value'][1].split( '\n' )
      for line in output:
        line = line.strip()
        if 'error' in line.lower():
          error.append( line )
      if error:
        message = '\n'.join( error )
      else:
        message = "Failed to update software to %s" % version
      return S_ERROR( message )

    # Check if there is a MySQL installation and fix the server scripts if necessary
    if os.path.exists( InstallTools.mysqlDir ):
      startupScript = os.path.join( InstallTools.instancePath,
                                    'mysql', 'share', 'mysql', 'mysql.server' )
      if not os.path.exists( startupScript ):
        startupScript = os.path.join( InstallTools.instancePath, 'pro',
                                     'mysql', 'share', 'mysql', 'mysql.server' )
      if os.path.exists( startupScript ):
        InstallTools.fixMySQLScripts( startupScript )

    # For LHCb we need to check Oracle client
    if installOracleClient:
      result = systemCall( 30, 'install_oracle-client.sh' )
      if not result['OK']:
        return result
      status = result['Value'][0]
      if status != 0:
        # Get error messages
        error = result['Value'][1].split( '\n' )
        error.extend( result['Value'][2].split( '\n' ) )
        error.append( 'Failed to install Oracle client module' )
        return S_ERROR( '\n'.join( error ) )
    return S_OK()
  
  types_revertSoftware = [ ]
  def export_revertSoftware( self ):
    """ Revert the last installed version of software to the previous one
    """
    oldLink = os.path.join( InstallTools.instancePath, 'old' )
    oldPath = os.readlink( oldLink )
    proLink = os.path.join( InstallTools.instancePath, 'pro' )
    os.remove(proLink)
    os.symlink( oldPath, proLink )
    
    return S_OK( oldPath )

  def __loadDIRACCFG( self ):
    installPath = gConfig.getValue( '/LocalInstallation/TargetPath',
                                    gConfig.getValue( '/LocalInstallation/RootPath', '' ) )
    if not installPath:
      installPath = rootPath
    cfgPath = os.path.join( installPath, 'etc', 'dirac.cfg' )
    try:
      diracCFG = CFG.CFG().loadFromFile( cfgPath )
    except Exception, excp:
      return S_ERROR( "Could not load dirac.cfg: %s" % str( excp ) )
    #HACK: Remove me once the v6 migration hell is over
    if diracCFG.isOption( "/LocalInstallation/ExtraPackages" ):
      diracCFG[ "LocalInstallation" ].renameKey( "ExtraPackages", "ExtraModules" )
      try:
        fd = open( cfgPath, "w" )
        fd.write( str( diracCFG ) )
        fd.close()
      except IOError, excp :
        gLogger.warn( "Could not write dirac.cfg: %s" % str( excp ) )
        pass
    #EOH (End Of Hack)
    return S_OK( ( cfgPath, diracCFG ) )

  types_setProject = [ StringTypes ]
  def export_setProject( self, projectName ):
    result = self.__loadDIRACCFG()
    if not result[ 'OK' ]:
      return result
    cfgPath, diracCFG = result[ 'Value' ]
    gLogger.notice( "Setting project to %s" % projectName )
    diracCFG.setOption( "/LocalInstallation/Project", projectName, "Project to install" )
    try:
      fd = open( cfgPath, "w" )
      fd.write( str( diracCFG ) )
      fd.close()
    except IOError, excp :
      return S_ERROR( "Could not write dirac.cfg: %s" % str( excp ) )
    return S_OK()

  types_getProject = []
  def export_getProject( self ):
    result = self.__loadDIRACCFG()
    if not result[ 'OK' ]:
      return result
    cfgPath, diracCFG = result[ 'Value' ]
    return S_OK( diracCFG.getOption( "/LocalInstallation/Project", "DIRAC" ) )

  types_addOptionToDiracCfg = [ StringTypes, StringTypes ]
  def export_addOptionToDiracCfg( self, option, value ):
    """ Set option in the local configuration file
    """
    return InstallTools.addOptionToDiracCfg( option, value )

  types_executeCommand = [ StringTypes ]
  def export_executeCommand( self, command ):
    """ Execute a command locally and return its output
    """
    result = shellCall( 60, command )
    return result

  types_checkComponentLog = [ list( StringTypes ) + [ListType] ]
  def export_checkComponentLog( self, component ):
    """ Check component log for errors
    """
    componentList = []
    if '*' in component:
      if component == '*':
        result = InstallTools.getSetupComponents()
        if result['OK']:
          for ctype in ['Services', 'Agents']:
            if ctype in result['Value']:
              for sname in result['Value'][ctype]:
                for cname in result['Value'][ctype][sname]:
                  componentList.append( '/'.join( [sname, cname] ) )
    elif type( component ) in StringTypes:
      componentList = [component]
    else:
      componentList = component

    resultDict = {}
    for c in componentList:
      if not '/' in c:
        continue
      system, cname = c.split( '/' )

      startDir = InstallTools.startDir
      currentLog = startDir + '/' + system + '_' + cname + '/log/current'
      logFile = file( currentLog, 'r' )
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
          timeStamp = fromString( fields[0] + ' ' + fields[1] )
          if ( now - timeStamp ) < hour:
            errors_1 += 1
            recent = True
          if ( now - timeStamp ) < day:
            errors_24 += 1
            recent = True
          if recent:
            lastError = line.split( 'ERROR:' )[-1].strip()

      resultDict[c] = {'ErrorsHour':errors_1, 'ErrorsDay':errors_24, 'LastError':lastError}

    return S_OK( resultDict )

  types_getHostInfo = []
  def export_getHostInfo(self):
    """ Get host current loads, memory, etc
    """

    result = dict()
    # Memory info
    re_parser = re.compile(r'^(?P<key>\S*):\s*(?P<value>\d*)\s*kB' )
    for line in open('/proc/meminfo'):
      match = re_parser.match(line)
      if not match:
        continue
      key, value = match.groups(['key', 'value'])
      result[key] = int(value)

    for mtype in ['Mem','Swap']:
      memory = int(result.get(mtype+'Total'))
      mfree = int(result.get(mtype+'Free'))
      if memory > 0:
        percentage = float(memory-mfree)/float(memory)*100.
      else:
        percentage = 0
      name = 'Memory'
      if mtype == "Swap":
        name = 'Swap'
      result[name] = '%.1f%%/%.1fMB' % (percentage,memory/1024.)

    # Loads
    line = open('/proc/loadavg').read()
    l1,l5,l15,d1,d2 = line.split()
    result['Load1'] = l1
    result['Load5'] = l5
    result['Load15'] = l15
    result['Load'] = '/'.join([l1,l5,l15])
    
    # CPU info
    lines = open( '/proc/cpuinfo', 'r' ).readlines()
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
    status,output = commands.getstatusoutput('df')
    lines = output.split('\n')
    for i in range( len( lines ) ):
      if lines[i].startswith('/dev'):
        fields = lines[i].split()
        if len( fields ) == 1:
          fields += lines[i+1].split()
        disk = fields[0].replace('/dev/sd','')
        partition = fields[5]
        occupancy = fields[4]
        summary += ",%s:%s" % (partition,occupancy)
    result['DiskOccupancy'] = summary[1:]
    result['RootDiskSpace'] = Os.getDiskSpace( DIRAC.rootPath )
    
    # Open files
    puser= getpass.getuser()
    status,output = commands.getstatusoutput('lsof')
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
        elif fType in ['unix','IPv4']:
          sockets += 1
        elif fType in ['FIFO']:
          pipes += 1
    result['OpenSockets'] = sockets
    result['OpenFiles'] = files
    result['OpenPipes'] = pipes
    
    infoResult = InstallTools.getInfo( getCSExtensions() )
    if infoResult['OK']:
      result.update( infoResult['Value'] )

    # Host certificate properties
    certFile,keyFile = getHostCertificateAndKeyLocation()
    chain = X509Chain()
    chain.loadChainFromFile( certFile )
    resultCert = chain.getCredentials()
    if resultCert['OK']:
      result['SecondsLeft'] = resultCert['Value']['secondsLeft']
      result['CertificateValidity'] = str( timedelta( seconds = resultCert['Value']['secondsLeft'] ) )
      result['CertificateDN'] = resultCert['Value']['subject']
      result['HostProperties'] = ','.join( resultCert['Value']['groupProperties'] )
      result['CertificateIssuer'] = resultCert['Value']['issuer']

    # Host uptime
    try:
      upFile = open('/proc/uptime', 'r')
      uptime_seconds = float(upFile.readline().split()[0])
      upFile.close()
      result['Uptime'] = str(timedelta(seconds = uptime_seconds))
    except:
      pass

    return S_OK(result)