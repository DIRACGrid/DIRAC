#!/usr/bin/env python
########################################################################
# $HeadURL$
""" File Catalog Client Command Line Interface. """

__RCSID__ = "$Id$"

import cmd
import sys
import pprint
import os
import atexit
import readline
import datetime
from DIRAC.Core.Utilities.ColorCLI import colorize
from DIRAC.FrameworkSystem.Client.SystemAdministratorClient import SystemAdministratorClient
from DIRAC.FrameworkSystem.Client.SystemAdministratorIntegrator import SystemAdministratorIntegrator
from DIRAC.FrameworkSystem.Client.ComponentMonitoringClient import ComponentMonitoringClient
from DIRAC.FrameworkSystem.Utilities import MonitoringUtilities
import DIRAC.Core.Utilities.InstallTools as InstallTools
from DIRAC.ConfigurationSystem.Client.Helpers import getCSExtensions
from DIRAC.Core.Utilities import List
from DIRAC.Core.Utilities.PromptUser import promptUser
from DIRAC import gConfig
from DIRAC import gLogger
from DIRAC.Core.Utilities.PrettyPrint import printTable
from DIRAC.Core.Security.ProxyInfo import getProxyInfo

class SystemAdministratorClientCLI( cmd.Cmd ):
  """
  """
  def __errMsg( self, errMsg ):
    gLogger.error( "%s %s" % ( colorize( "[ERROR]", "red" ), errMsg ) )

  def __init__( self, host = None ):
    cmd.Cmd.__init__( self )
    # Check if Port is given
    self.host = None
    self.port = None
    self.prompt = '[%s]> ' % colorize( "no host", "yellow" )
    if host:
      self.__setHost( host )
    self.cwd = ''  
    self.previous_cwd = ''
    self.homeDir = ''
    self.runitComponents = [ "service", "agent", "executor", "consumer" ]

    # store history
    histfilename = os.path.basename(sys.argv[0])
    historyFile = os.path.expanduser( "~/.dirac/%s.history" % histfilename[0:-3])
    if not os.path.exists( os.path.dirname(historyFile) ):
      os.makedirs( os.path.dirname(historyFile) )
    if os.path.isfile( historyFile ):
      readline.read_history_file( historyFile )
    readline.set_history_length(1000)
    atexit.register( readline.write_history_file, historyFile )


  def __setHost( self, host ):
    hostList = host.split( ':' )
    self.host = hostList[0]
    if len( hostList ) == 2:
      self.port = hostList[1]
    else:
      self.port = None
    gLogger.notice( "Pinging %s..." % self.host )
    result = self.__getClient().ping()
    if result[ 'OK' ]:
      colorHost = colorize( host, "green" )
    else:
      self.__errMsg( "Could not connect to %s: %s" % ( self.host, result[ 'Message' ] ) )
      colorHost = colorize( host, "red" )
    self.prompt = '[%s]> ' % colorHost

  def __getClient( self ):
    return SystemAdministratorClient( self.host, self.port )

  def do_set( self, args ):
    """
        Set options

        usage:

          set host <hostname>     - Set the hostname to work with
          set project <project>   - Set the project to install/upgrade in the host
    """
    if not args:
      gLogger.notice( self.do_set.__doc__ )
      return

    cmds = { 'host' : ( 1, self.__do_set_host ),
             'project' : ( 1, self.__do_set_project ) }

    args = List.fromChar( args, " " )

    for cmd in cmds:
      if cmd == args[0]:
        if len( args ) != 1 + cmds[ cmd ][0]:
          self.__errMsg( "Missing arguments" )
          gLogger.notice( self.do_set.__doc__ )
          return
        return cmds[ cmd ][1]( args[1:] )
    self.__errMsg( "Invalid command" )
    gLogger.notice( self.do_set.__doc__ )
    return

  def __do_set_host( self, args ):
    host = args[0]
    if host.find( '.' ) == -1 and host != "localhost":
      self.__errMsg( "Provide the full host name including its domain" )
      return
    self.__setHost( host )

  def __do_set_project( self, args ):
    project = args[0]
    result = self.__getClient().setProject( project )
    if not result[ 'OK' ]:
      self.__errMsg( "Cannot set project: %s" % result[ 'Message' ] )
    else:
      gLogger.notice( "Project set to %s" % project )


  def do_show( self, args ):
    """
        Show list of components with various related information

        usage:

          show software      - show components for which software is available
          show installed     - show components installed in the host with runit system
          show setup         - show components set up for automatic running in the host
          show project       - show project to install or upgrade
          show status        - show status of the installed components
          show database      - show status of the databases
          show mysql         - show status of the MySQL server
          show log  <system> <service|agent> [nlines]
                             - show last <nlines> lines in the component log file
          show info          - show version of software and setup
          show doc <type> <system> <name>
                             - show documentation for a given service or agent
          show host          - show host related parameters
          show hosts         - show all available hosts
          show installations [ list | current | -n <Name> | -h <Host> | -s <System> | -m <Module> | -t <Type> | -itb <InstallationTime before>
                              | -ita <InstallationTime after> | -utb <UnInstallationTime before> | -uta <UnInstallationTime after> ]*
                             - show all the installations of components that match the given parameters
          show errors [*|<system> <service|agent>]
                             - show error count for the given component or all the components
                               in the last hour and day
    """

    argss = args.split()
    if not argss:
      gLogger.notice( self.do_show.__doc__ )
      return

    option = argss[0]
    del argss[0]

    if option == 'software':
      client = SystemAdministratorClient( self.host, self.port )
      result = client.getSoftwareComponents()
      if not result['OK']:
        self.__errMsg( result['Message'] )
      else:
        gLogger.notice( '' )
        pprint.pprint( result['Value'] )
    elif option == 'installed':
      client = SystemAdministratorClient( self.host, self.port )
      result = client.getInstalledComponents()
      if not result['OK']:
        self.__errMsg( result['Message'] )
      else:
        gLogger.notice( '' )
        pprint.pprint( result['Value'] )
    elif option == 'setup':
      client = SystemAdministratorClient( self.host, self.port )
      result = client.getSetupComponents()
      if not result['OK']:
        self.__errMsg( result['Message'] )
      else:
        gLogger.notice( '' )
        pprint.pprint( result['Value'] )
    elif option == 'project':
      result = SystemAdministratorClient( self.host, self.port ).getProject()
      if not result['OK']:
        self.__errMsg( result['Message'] )
      else:
        gLogger.notice( "Current project is %s" % result[ 'Value' ] )
    elif option == 'status':
      client = SystemAdministratorClient( self.host, self.port )
      result = client.getOverallStatus()
      if not result['OK']:
        self.__errMsg( result['Message'] )
      else:
        fields = ["System",'Name','Module','Type','Setup','Installed','Runit','Uptime','PID']
        records = []
        rDict = result['Value']
        for compType in rDict:
          for system in rDict[compType]:
            components = rDict[compType][system].keys()
            components.sort()
            for component in components:
              record = []
              if rDict[compType][system][component]['Installed']:
                module = str( rDict[compType][system][component]['Module'] )
                record += [ system,component,module,compType.lower()[:-1]]
                if rDict[compType][system][component]['Setup']:
                  record += ['Setup']
                else:
                  record += ['NotSetup']
                if rDict[compType][system][component]['Installed']:
                  record += ['Installed']
                else:
                  record += ['NotInstalled']
                record += [str( rDict[compType][system][component]['RunitStatus'] )]
                record += [str( rDict[compType][system][component]['Timeup'] )]
                record += [str( rDict[compType][system][component]['PID'] )]
                records.append(record)  
        printTable(fields,records)        
    elif option == 'database' or option == 'databases':
      client = SystemAdministratorClient( self.host, self.port )
      if not InstallTools.mysqlPassword:
        InstallTools.mysqlPassword = "LocalConfig"
      InstallTools.getMySQLPasswords()
      result = client.getDatabases( InstallTools.mysqlRootPwd )
      if not result['OK']:
        self.__errMsg( result['Message'] )
        return
      resultSW = client.getAvailableDatabases()
      if not resultSW['OK']:
        self.__errMsg( resultSW['Message'] )
        return

      sw = resultSW['Value']
      installed = result['Value']
      gLogger.notice( '' )
      for db in sw:
        if db in installed:
          gLogger.notice( db.rjust( 25 ), ': Installed' )
        else:
          gLogger.notice( db.rjust( 25 ), ': Not installed' )
      if not sw:
        gLogger.notice( "No database found" )
    elif option == 'mysql':
      client = SystemAdministratorClient( self.host, self.port )
      result = client.getMySQLStatus()
      if not result['OK']:
        self.__errMsg( result['Message'] )
      elif result['Value']:
        gLogger.notice( '' )
        for par, value in result['Value'].items():
          gLogger.notice( ( par.rjust( 28 ), ':', value ) )
      else:
        gLogger.notice( "No MySQL database found" )
    elif option == "log":
      self.getLog( argss )
    elif option == "info":
      client = SystemAdministratorClient( self.host, self.port )
      result = client.getInfo()
      if not result['OK']:
        self.__errMsg( result['Message'] )
      else:
        gLogger.notice( '' )
        gLogger.notice( "Setup:", result['Value']['Setup'] )
        gLogger.notice( "DIRAC version:", result['Value']['DIRAC'] )
        if result['Value']['Extensions']:
          for e, v in result['Value']['Extensions'].items():
            gLogger.notice( "%s version" % e, v )
        gLogger.notice( '' )
    elif option == "host":
      client = SystemAdministratorClient( self.host, self.port )
      result = client.getHostInfo()
      if not result['OK']:
        self.__errMsg( result['Message'] )
      else:   
        gLogger.notice( '' )
        gLogger.notice( "Host info:" )
        gLogger.notice( '' )
        
        fields = ['Parameter','Value']
        records = []
        for key, value in result['Value'].items():
          records.append( [key, str( value ) ] )
          
        printTable( fields, records )  
    elif option == "hosts":
      client = ComponentMonitoringClient()
      result = client.getHosts( {}, False, False )
      if not result[ 'OK' ]:
        self.__errMsg( 'Error retrieving the list of hosts: %s' % ( result[ 'Message' ] ) )
      else:
        hostList = result[ 'Value' ]
        gLogger.notice( '' )
        gLogger.notice( ' ' + 'Host'.center( 32 ) + ' ' + 'CPU'.center( 34 ) + ' ' )
        gLogger.notice( ( '-' * 69 ) )
        for element in hostList:
          gLogger.notice( '|' + element[ 'HostName' ].center( 32 ) + '|' + element[ 'CPU' ].center( 34 ) + '|' )
        gLogger.notice( ( '-' * 69 ) )
        gLogger.notice( '' )
    elif option == "errors":
      self.getErrors( argss )
    elif option == "installations":
      self.getInstallations( argss )
    elif option == "doc":
      if len( argss ) > 2:
        if argss[0] in [ 'service', 'agent' ]:
          compType = argss[0]
          compSystem = argss[1]
          compModule = argss[2]
          client = SystemAdministratorClient( self.host, self.port )
          result = client.getComponentDocumentation( compType, compSystem, compModule )
          if result[ 'OK' ]:
            gLogger.notice( result[ 'Value' ] )
          else:
            self.__errMsg( result[ 'Message' ] )
        else:
          gLogger.notice( self.do_show.__doc__ )
      else:
        gLogger.notice( self.do_show.__doc__ )
    else:
      gLogger.notice( "Unknown option:", option )

  def getErrors( self, argss ):
    """ Get and gLogger.notice( out errors from the logs of specified components )
    """
    component = ''
    if len( argss ) < 1:
      component = '*'
    else:
      system = argss[0]
      if system == "*":
        component = '*'
      else:
        if len( argss ) < 2:
          gLogger.notice( '' )
          gLogger.notice( self.do_show.__doc__ )
          return
        comp = argss[1]
        component = '/'.join( [system, comp] )

    client = SystemAdministratorClient( self.host, self.port )
    result = client.checkComponentLog( component )
    if not result['OK']:
      self.__errMsg( result['Message'] )
    else:
      fields = ['System', 'Component', 'Last hour', 'Last day', 'Last error']
      records = []
      for cname in result['Value']:
        system, component = cname.split( '/' )
        errors_1 = result['Value'][cname]['ErrorsHour']
        errors_24 = result['Value'][cname]['ErrorsDay']
        lastError = result['Value'][cname]['LastError']
        lastError.strip()
        if len( lastError ) > 80:
          lastError = lastError[:80] + '...'
        records.append( [system, component, str( errors_1 ), str( errors_24 ), lastError] )
      records.sort()
      printTable( fields, records )

  def getInstallations( self, argss ):
    """ Get data from the component monitoring database
    """
    display = 'table'
    installationFilter = {}
    componentFilter = {}
    hostFilter = {}

    key = None
    for arg in argss:
      if not key:
        if arg == 'list':
          display = 'list'
        elif arg == 'current':
          installationFilter[ 'UnInstallationTime' ] = None
        elif arg == '-t':
          key = 'Component.Type'
        elif arg == '-m':
          key = 'Component.Module'
        elif arg == '-s':
          key = 'Component.System'
        elif arg == '-h':
          key = 'Host.HostName'
        elif arg == '-n':
          key = 'Instance'
        elif arg == '-itb':
          key = 'InstallationTime.smaller'
        elif arg == '-ita':
          key = 'InstallationTime.bigger'
        elif arg == '-utb':
          key = 'UnInstallationTime.smaller'
        elif arg == '-uta':
          key = 'UnInstallationTime.bigger'
      else:
        if 'Component.' in key:
          componentFilter[ key.replace( 'Component.', '' ) ] = arg
        elif 'Host.' in key:
          hostFilter[ key.replace( 'Host.', '' ) ] = arg
        else:
          if 'Time.' in key:
            arg = datetime.datetime.strptime( arg, '%d-%m-%Y' )
          installationFilter[ key ] = arg
        key = None

    client = ComponentMonitoringClient()
    result = client.getInstallations( installationFilter, componentFilter, hostFilter, True )
    if not result[ 'OK' ]:
      self.__errMsg( 'Could not retrieve the installations: %s' % ( result[ 'Message' ] ) )
      installations = None
    else:
      installations = result[ 'Value' ]

    if installations:
      if display == 'table':
        gLogger.notice( '' )
        gLogger.notice( ' ' + 'Num'.center( 5 ) + ' ' \
                        + 'Host'.center( 20 ) + ' ' \
                        + 'Name'.center( 20 ) + ' ' \
                        + 'Module'.center( 20 ) + ' ' \
                        + 'System'.center( 16 ) + ' ' \
                        + 'Type'.center( 12 ) + ' ' \
                        + 'Installed on'.center( 18 ) + ' ' \
                        + 'Install by'.center( 12 ) + ' ' \
                        + 'Uninstalled on'.center( 18 ) + ' ' \
                        + 'Uninstall by'.center( 12 ) )
        gLogger.notice( ( '-' ) * 164 )
      for i, installation in enumerate( installations ):
        if not installation[ 'InstalledBy' ]:
          installedBy = ''
        else:
          installedBy = installation[ 'InstalledBy' ]

        if not installation[ 'UnInstalledBy' ]:
          uninstalledBy = ''
        else:
          uninstalledBy = installation[ 'UnInstalledBy' ]

        if installation[ 'UnInstallationTime' ]:
          uninstalledOn = installation[ 'UnInstallationTime' ].strftime( "%d-%m-%Y %H:%M" )
          isInstalled = 'No'
        else:
          uninstalledOn = ''
          isInstalled = 'Yes'

        if display == 'table':
          gLogger.notice( '|' + str( i + 1 ).center( 5 ) + '|' \
                          + installation[ 'Host' ][ 'HostName' ].center( 20 ) + '|' \
                          + installation[ 'Instance' ].center( 20 ) + '|' \
                          + installation[ 'Component' ][ 'Module' ].center( 20 ) + '|' \
                          + installation[ 'Component' ][ 'System' ].center( 16 ) + '|' \
                          + installation[ 'Component' ][ 'Type' ].center( 12 ) + '|' \
                          + installation[ 'InstallationTime' ].strftime( "%d-%m-%Y %H:%M" ).center( 18 ) + '|' \
                          + installedBy.center( 12 ) + '|' \
                          + uninstalledOn.center( 18 ) + '|' \
                          + uninstalledBy.center( 12 ) + '|' )
          gLogger.notice( ( '-' ) * 164 )
        elif display == 'list':
          gLogger.notice( '' )
          gLogger.notice( 'Installation: '.rjust( 20 ) + str ( i + 1 ) )
          gLogger.notice( 'Installed: '.rjust( 20 ) + isInstalled )
          gLogger.notice( 'Host: '.rjust( 20 ) + installation[ 'Host' ][ 'HostName' ] )
          gLogger.notice( 'Name: '.rjust( 20 ) + installation[ 'Instance' ] )
          gLogger.notice( 'Module: '.rjust( 20 ) + installation[ 'Component' ][ 'Module' ] )
          gLogger.notice( 'System: '.rjust( 20 ) + installation[ 'Component' ][ 'System' ] )
          gLogger.notice( 'Type: '.rjust( 20 ) + installation[ 'Component' ][ 'Type' ] )
          gLogger.notice( 'Installed on: '.rjust( 20 ) + installation[ 'InstallationTime' ].strftime( "%d-%m-%Y %H:%M" ) )
          if installedBy != '':
            gLogger.notice( 'Installed by: '.rjust( 20 ) + installedBy )
          if uninstalledOn != '':
            gLogger.notice( 'Uninstalled on: '.rjust( 20 ) + uninstalledOn )
            gLogger.notice( 'Uninstalled by: '.rjust( 20 ) + uninstalledBy )
        else:
          self.__errMsg( 'No display mode was selected' )
      gLogger.notice( '' )

  def getLog( self, argss ):
    """ Get the tail of the log file of the given component
    """
    if len( argss ) < 2:
      gLogger.notice( '' )
      gLogger.notice( self.do_show.__doc__ )
      return

    system = argss[0]
    component = argss[1]
    nLines = 40
    if len( argss ) > 2:
      nLines = int( argss[2] )
    client = SystemAdministratorClient( self.host, self.port )
    result = client.getLogTail( system, component, nLines )
    if not result['OK']:
      self.__errMsg( result['Message'] )
    elif result['Value']:
      for line in result['Value']['_'.join( [system, component] )].split( '\n' ):
        gLogger.notice( '   ', line )

    else:
      gLogger.notice( "No logs found" )

  def do_install( self, args ):
    """
        Install various DIRAC components

        usage:

          install mysql
          install db <database>
          install service <system> <service> [-m <ModuleName>] [-p <Option>=<Value>] [-p <Option>=<Value>] ...
          install agent <system> <agent> [-m <ModuleName>] [-p <Option>=<Value>] [-p <Option>=<Value>] ...
          install executor <system> <executor> [-m <ModuleName>] [-p <Option>=<Value>] [-p <Option>=<Value>] ...
    """
    argss = args.split()
    if not argss:
      gLogger.notice( self.do_install.__doc__ )
      return

    # Retrieve user installing the component
    result = getProxyInfo()
    if not result[ 'OK' ]:
      self.__errMsg( result[ 'Message'] )
    user = result[ 'Value' ][ 'username' ]

    option = argss[0]
    del argss[0]
    if option == "mysql":
      gLogger.notice( "Installing MySQL database, this can take a while ..." )
      client = SystemAdministratorClient( self.host, self.port )
      if InstallTools.mysqlPassword == 'LocalConfig':
        InstallTools.mysqlPassword = ''
      InstallTools.getMySQLPasswords()
      result = client.installMySQL( InstallTools.mysqlRootPwd, InstallTools.mysqlPassword )
      if not result['OK']:
        self.__errMsg( result['Message'] )
      else:
        gLogger.notice( "MySQL:", result['Value'] )
        gLogger.notice( "You might need to restart SystemAdministrator service to take new settings into account" )
    elif option == "db":
      if not argss:
        gLogger.notice( self.do_install.__doc__ )
        return
      database = argss[0]
      client = SystemAdministratorClient( self.host, self.port )

      result = client.getAvailableDatabases()
      if not result['OK']:
        self.__errMsg( "Can not get database list: %s" % result['Message'] )
        return
      if not result['Value'].has_key( database ):
        self.__errMsg( "Unknown database %s: " % database )
        return
      system = result['Value'][database]['System']
      setup = gConfig.getValue( '/DIRAC/Setup', '' )
      if not setup:
        self.__errMsg( "Unknown current setup" )
        return
      instance = gConfig.getValue( '/DIRAC/Setups/%s/%s' % ( setup, system ), '' )
      if not instance:
        self.__errMsg( "No instance defined for system %s" % system )
        self.__errMsg( "\tAdd new instance with 'add instance %s <instance_name>'" % system )
        return

      if not InstallTools.mysqlPassword:
        InstallTools.mysqlPassword = 'LocalConfig'
      InstallTools.getMySQLPasswords()
      result = client.installDatabase( database, InstallTools.mysqlRootPwd )
      if not result['OK']:
        self.__errMsg( result['Message'] )
        return
      extension, system = result['Value']

      result = client.getHostInfo()
      if not result[ 'OK' ]:
        self.__errMsg( result[ 'Message' ] )
        return
      else:
        cpu = result[ 'Value' ][ 'CPUModel' ]
      hostname = self.host
      if not result[ 'OK' ]:
        self.__errMsg( result[ 'Message' ] )
        return

      if database != 'InstalledComponentsDB':
        result = MonitoringUtilities.monitorInstallation( 'DB', system.replace( 'System', '' ), database, cpu = cpu, hostname = hostname )
        if not result['OK']:
          self.__errMsg( result['Message'] )
          return
      # result = client.addDatabaseOptionsToCS( system, database )
      InstallTools.mysqlHost = self.host
      result = client.getInfo()
      if not result['OK']:
        self.__errMsg( result['Message'] )
      hostSetup = result['Value']['Setup']
      result = InstallTools.addDatabaseOptionsToCS( gConfig, system, database, hostSetup, overwrite = True )
      if not result['OK']:
        self.__errMsg( result['Message'] )
        return
      gLogger.notice( "Database %s from %s/%s installed successfully" % ( database, extension, system ) )
    elif option in self.runitComponents:
      if len( argss ) < 2:
        gLogger.notice( self.do_install.__doc__ )
        return

      system = argss[0]
      del argss[0]
      component = argss[0]
      del argss[0]
      
      specialOptions = {}
      module = ''
      for i in range(len(argss)):
        if argss[i] == "-m":
          specialOptions['Module'] = argss[i+1]
          module = argss[i+1]
        if argss[i] == "-p":
          opt,value = argss[i+1].split('=')
          specialOptions[opt] = value           
      if module == component:
        module = ''
      
      client = SystemAdministratorClient( self.host, self.port )
      # First need to update the CS
      # result = client.addDefaultOptionsToCS( option, system, component )
      InstallTools.host = self.host
      result = client.getInfo()
      if not result['OK']:
        self.__errMsg( result['Message'] )
        return
      hostSetup = result['Value']['Setup']
      
      # Install Module section if not yet there
      if module:
        result = InstallTools.addDefaultOptionsToCS( gConfig, option, system, module, 
                                                     getCSExtensions(), hostSetup )
        # Add component section with specific parameters only
        result = InstallTools.addDefaultOptionsToCS( gConfig, option, system, component, 
                                                     getCSExtensions(), hostSetup, specialOptions, 
                                                     addDefaultOptions = True )
      else:  
        # Install component section
        result = InstallTools.addDefaultOptionsToCS( gConfig, option, system, component, 
                                                     getCSExtensions(), hostSetup, specialOptions )
    
      if not result['OK']:
        self.__errMsg( result['Message'] )
        return
      # Then we can install and start the component
      result = client.setupComponent( option, system, component, module )
      if not result['OK']:
        self.__errMsg( result['Message'] )
        return
      compType = result['Value']['ComponentType']
      runit = result['Value']['RunitStatus']
      gLogger.notice( "%s %s_%s is installed, runit status: %s" % ( compType, system, component, runit ) )

      # And register it in the database
      result = client.getHostInfo()
      if not result[ 'OK' ]:
        self.__errMsg( result[ 'Message' ] )
        return
      else:
        cpu = result[ 'Value' ][ 'CPUModel' ]
      hostname = self.host
      if component == 'ComponentMonitoring':
        result = MonitoringUtilities.monitorInstallation( 'DB', system, 'InstalledComponentsDB', cpu = cpu, hostname = hostname )
        if not result['OK']:
          self.__errMsg( 'Error registering installation into database: %s' % result[ 'Message' ] )
          return
      result = MonitoringUtilities.monitorInstallation( option, system, component, module, cpu = cpu, hostname = hostname )
      if not result['OK']:
        self.__errMsg( 'Error registering installation into database: %s' % result[ 'Message' ] )
        return
    else:
      gLogger.notice( "Unknown option:", option )

  def do_uninstall( self, args ):
    """
        Uninstall DIRAC component

        usage:

          uninstall db <database>
          uninstall <-f ForceLogUninstall> <system> <component>
    """
    argss = args.split()
    if not argss:
      gLogger.notice( self.do_uninstall.__doc__ )
      return

    # Retrieve user uninstalling the component
    result = getProxyInfo()
    if not result[ 'OK' ]:
      self.__errMsg( result[ 'Message'] )
    user = result[ 'Value' ][ 'username' ]

    option = argss[0]
    if option == 'db':
      component = argss[1]
      client = SystemAdministratorClient( self.host, self.port )

      result = client.getHostInfo()
      if not result[ 'OK' ]:
        self.__errMsg( result[ 'Message' ] )
        return
      else:
        cpu = result[ 'Value' ][ 'CPUModel' ]
      hostname = self.host
      result = client.getAvailableDatabases()
      if not result[ 'OK' ]:
        self.__errMsg( result[ 'Message' ] )
        return
      system = result[ 'Value' ][ component ][ 'System' ]
      result = MonitoringUtilities.monitorUninstallation( system , component, hostname = hostname, cpu = cpu )
      if not result[ 'OK' ]:
        self.__errMsg( result[ 'Message' ] )
        return

      result = client.uninstallDatabase( component )
      if not result[ 'OK' ]:
        self.__errMsg( result[ 'Message' ] )
      else:
        gLogger.notice( "Successfully uninstalled %s" % ( component ) )
    else:
      if option == '-f':
        force = True
        del argss[0]
      else:
        force = False

      if len( argss ) != 2:
        gLogger.notice( self.do_uninstall.__doc__ )
        return

      system, component = argss
      client = SystemAdministratorClient( self.host, self.port )

      monitoringClient = ComponentMonitoringClient()
      result = monitoringClient.getInstallations( { 'Instance': component, 'UnInstallationTime': None },
                                                  { 'System': system },
                                                  { 'HostName': self.host }, True )
      if not result[ 'OK' ]:
        self.__errMsg( result[ 'Message' ] )
        return
      if len( result[ 'Value' ] ) < 1:
        self.__errMsg( "Given component does not exist" )
        return
      if len( result[ 'Value' ] ) > 1:
        self.__errMsg( "Too many components match" )
        return

      removeLogs = False
      if force:
        removeLogs = True
      else:
        if result[ 'Value' ][0][ 'Component' ][ 'Type' ] in self.runitComponents:
          result = promptUser( 'Remove logs?', ['y', 'n'], 'n' )
          if result[ 'OK' ]:
            removeLogs = result[ 'Value' ] == 'y'

      result = client.uninstallComponent( system, component, removeLogs )
      if not result[ 'OK' ]:
        self.__errMsg( result[ 'Message' ] )
      else:
        gLogger.notice( "Successfully uninstalled %s/%s" % ( system, component ) )

      result = client.getHostInfo()
      if not result[ 'OK' ]:
        self.__errMsg( result[ 'Message' ] )
        return
      else:
        cpu = result[ 'Value' ][ 'CPUModel' ]
      hostname = self.host
      result = MonitoringUtilities.monitorUninstallation( system, component, hostname = hostname, cpu = cpu )
      if not result[ 'OK' ]:
        return result

  def do_start( self, args ):
    """ Start services or agents or database server

        usage:

          start <system|*> <service|agent|*>
          start mysql
    """
    argss = args.split()
    if len( argss ) < 2:
      gLogger.notice( self.do_start.__doc__ )
      return
    option = argss[0]
    del argss[0]

    if option != 'mysql':
      if len( argss ) < 1:
        gLogger.notice( self.do_start.__doc__ )
        return
      system = option
      if system != '*':
        component = argss[0]
      else:
        component = '*'
      client = SystemAdministratorClient( self.host, self.port )
      result = client.startComponent( system, component )
      if not result['OK']:
        self.__errMsg( result['Message'] )
      else:
        if system != '*' and component != '*':
          gLogger.notice( "\n%s_%s started successfully, runit status:\n" % ( system, component ) )
        else:
          gLogger.notice( "\nComponents started successfully, runit status:\n" )
        for comp in result['Value']:
          gLogger.notice( ( comp.rjust( 32 ), ':', result['Value'][comp]['RunitStatus'] ) )
    else:
      gLogger.notice( "Not yet implemented" )

  def do_restart( self, args ):
    """ Restart services or agents or database server

        usage:

          restart <system|*> <service|agent|*>
          restart mysql
    """
    if not args:
      gLogger.notice( self.do_restart.__doc__ )
      return

    argss = args.split()
    option = argss[0]
    del argss[0]
    if option != 'mysql':
      if option != "*":
        if len( argss ) < 1:
          gLogger.notice( self.do_restart.__doc__ )
          return
      system = option
      if system != '*':
        component = argss[0]
      else:
        component = '*'
      client = SystemAdministratorClient( self.host, self.port )
      result = client.restartComponent( system, component )
      if not result['OK']:
        if system == '*':
          gLogger.notice( "All systems are restarted, connection to SystemAdministrator is lost" )
        else:
          self.__errMsg( result['Message'] )
      else:
        if system != '*' and component != '*':
          gLogger.notice( "\n%s_%s started successfully, runit status:\n" % ( system, component ) )
        else:
          gLogger.notice( "\nComponents started successfully, runit status:\n" )
        for comp in result['Value']:
          gLogger.notice( ( comp.rjust( 32 ), ':', result['Value'][comp]['RunitStatus'] ) )
    else:
      gLogger.notice( "Not yet implemented" )

  def do_stop( self, args ):
    """ Stop services or agents or database server

        usage:

          stop <system|*> <service|agent|*>
          stop mysql
    """
    if not args:
      gLogger.notice( self.do_stop.__doc__ )
      return

    argss = args.split()
    if argss[0] != 'mysql':
      system = argss[0]
      if system != '*':
        component = argss[1]
      else:
        component = '*'
      client = SystemAdministratorClient( self.host, self.port )
      result = client.stopComponent( system, component )
      if not result['OK']:
        self.__errMsg( result['Message'] )
      else:
        if system != '*' and component != '*':
          gLogger.notice( "\n%s_%s stopped successfully, runit status:\n" % ( system, component ) )
        else:
          gLogger.notice( "\nComponents stopped successfully, runit status:\n" )
        for comp in result['Value']:
          gLogger.notice( ( comp.rjust( 32 ), ':', result['Value'][comp]['RunitStatus'] ) )
    else:
      gLogger.notice( "Not yet implemented" )

  def do_update( self, args ):
    """ Update the software on the target host to a given version

        usage:

          update <version> [ -r <rootPath> ] [ -g <lcgVersion> ]
          
              where rootPath - path to the DIRAC installation
                    lcgVersion - version of the LCG bindings to install
    """
    try:
      argss = args.split()
      version = argss[0]
      rootPath = ''
      lcgVersion = ''
      del argss[0]

      while len( argss ) > 0:
        if argss[0] == '-r':
          rootPath = argss[1]
          del argss[0]
          del argss[0]
        elif argss[0] == '-g':
          lcgVersion = argss[1]  
          del argss[0]
          del argss[0]
    except Exception, x:
      gLogger.notice( "ERROR: wrong input:", str( x ) )
      gLogger.notice( self.do_update.__doc__ )
      return  
    
    client = SystemAdministratorClient( self.host, self.port )
    gLogger.notice( "Software update can take a while, please wait ..." )
    result = client.updateSoftware( version, rootPath, lcgVersion, timeout = 300 )
    if not result['OK']:
      self.__errMsg( "Failed to update the software" )
      gLogger.notice( result['Message'] )
    else:
      gLogger.notice( "Software successfully updated." )
      gLogger.notice( "You should restart the services to use the new software version." )
      gLogger.notice( "Think of updating /Operations/<vo>/<setup>/Pilot/Versions section in the CS" )

  def do_revert( self, args ):
    """ Revert the last installed version of software to the previous one
    
        usage:
        
            revert
    """ 
    client = SystemAdministratorClient( self.host, self.port )
    result = client.revertSoftware()
    if not result['OK']:
      gLogger.notice( "Error:", result['Message'] )
    else:
      gLogger.notice( "Software reverted to", result['Value'] )

  def do_add( self, args ):
    """
        Add new entity to the Configuration Service

        usage:

          add system <system> <instance>
    """
    if not args:
      gLogger.notice( self.do_add.__doc__ )
      return

    argss = args.split()
    option = argss[0]
    del argss[0]
    if option == "instance" or option == "system":
      system = argss[0]
      instance = argss[1]
      client = SystemAdministratorClient( self.host, self.port )
      result = client.getInfo()
      if not result['OK']:
        self.__errMsg( result['Message'] )
      hostSetup = result['Value']['Setup']
      instanceName = gConfig.getValue( '/DIRAC/Setups/%s/%s' % ( hostSetup, system ), '' )
      if instanceName:
        if instanceName == instance:
          gLogger.notice( "System %s already has instance %s defined in %s Setup" % ( system, instance, hostSetup ) )
        else:
          self.__errMsg( "System %s already has instance %s defined in %s Setup" % ( system, instance, hostSetup ) )
        return
      result = InstallTools.addSystemInstance( system, instance, hostSetup )
      if not result['OK']:
        self.__errMsg( result['Message'] )
      else:
        gLogger.notice( "%s system instance %s added successfully" % ( system, instance ) )
    else:
      gLogger.notice( "Unknown option:", option )

  def do_exec( self, args ):
    """ Execute a shell command on the remote host and get back the output

        usage:

          exec <cmd> [<arguments>]
    """
    client = SystemAdministratorClient( self.host, self.port )
    command = 'cd %s;' % self.cwd + args
    result = client.executeCommand( command )
    if not result['OK']:
      self.__errMsg( result['Message'] )
      return
    status, output, error = result['Value']
    gLogger.notice( '' )
    for line in output.split( '\n' ):
      gLogger.notice( line )
    if error:
      self.__errMsg( status )
      for line in error.split( '\n' ):
        gLogger.notice( line )

  def do_execfile( self, args ):
    """ Execute a series of administrator CLI commands from a given file

        usage:

          execfile <filename>
    """
    if not args:
      gLogger.notice( self.do_execfile.__doc__ )
      return

    argss = args.split()
    fname = argss[0]
    execfile = open( fname, 'r' )
    lines = execfile.readlines()
    execfile.close()

    for line in lines:
      if line.find( '#' ) != -1 :
        line = line[:line.find( '#' )]
      line = line.strip()
      if not line:
        continue
      gLogger.notice( "\n--> Executing %s\n" % line )
      elements = line.split()
      command = elements[0]
      args = ' '.join( elements[1:] )
      eval( "self.do_%s(args)" % command )
      
  def do_cd( self, args ):    
    """ Change the current working directory on the target host
    
        Usage:
          cd <dirpath>
    """
    argss = args.split()
    
    if len( argss ) == 0:
      # Return to $HOME
      if self.homeDir:
        self.previous_cwd = self.cwd
        self.cwd = self.homeDir
      else:  
        client = SystemAdministratorClient( self.host, self.port )
        command = 'echo $HOME'
        result = client.executeCommand( command )
        if not result['OK']:
          self.__errMsg( result['Message'] )
          return
        status, output, _error = result['Value']
        if not status and output:
          self.homeDir = output.strip()
          self.previous_cwd = self.cwd
          self.cwd = self.homeDir
      self.prompt = '[%s:%s]> ' % ( self.host, self.cwd )  
      return
        
    newPath = argss[0]
    if newPath == '-':
      if self.previous_cwd:
        cwd = self.cwd
        self.cwd = self.previous_cwd
        self.previous_cwd = cwd
    elif newPath.startswith( '/' ):
      self.previous_cwd = self.cwd
      self.cwd = newPath
    else:
      newPath = self.cwd + '/' + newPath
      self.previous_cwd = self.cwd
      self.cwd = os.path.normpath( newPath )  
    self.prompt = '[%s:%s]> ' % ( self.host, self.cwd )  

  def do_showall( self, args ):
    """ Show status of all the components in all the hosts
    
        Usage:
          showall [-snmth] [-ASE] [-N name] [-H host] - show status of components
                              
        Options:
            -d extra debug printout
          Sorting options:                      
            -s system
            -n component name
            -m component module
            -t component type
            -h component host  
          Selection options:
            -A select agents
            -S select services
            -E select executors
            -N <component pattern> select component with the name containing the pattern 
            -H <host name> select the given host  
            -T <setup name> select the given setup
    """
    
    argss = args.split()
    sortOption = ''
    componentType = ''
    componentName = ''
    hostName = ''
    setupName = ''
    debug = False
    while len( argss ) > 0:
      option = argss[0]
      del argss[0]
      sortOption = ''
      if option == '-s':
        sortOption = "System"
      elif option == '-n':
        sortOption = "Name" 
      elif option == '-m':
        sortOption = "Module"
      elif option == '-t':
        sortOption = "Type"
      elif option == '-h':
        sortOption = "Host"
      elif option == "-A":
        componentType = 'Agents'
      elif option == "-S":
        componentType = 'Services'
      elif option == "-E":
        componentType = 'Executors'
      elif option == "-d":
        debug = True  
      elif option == "-N":
        componentName = argss[0]        
        del argss[0]      
      elif option == "-H":
        hostName = argss[0]        
        del argss[0]   
      elif option == "-T":
        setupName = argss[0]        
        del argss[0]     
      else:
        self.__errMsg( 'Invalid option %s' % option )  
        return
    
    client = SystemAdministratorIntegrator()
    silentHosts = client.getSilentHosts()
    respondingHosts = client.getRespondingHosts()
    resultAll = client.getOverallStatus()
    resultInfo = client.getInfo()
    
    if not resultAll['OK']:
      self.__errMsg( resultAll['Message'] )
    else:
      fields = ["System",'Name','Module','Type','Setup','Host','Runit','Uptime']
      records = []
      for host in resultAll['Value']:
        if hostName and not hostName in host:
          continue
        result = resultAll['Value'][host]
        if not result['OK']:
          if debug:
            self.__errMsg( "Host %s: %s" % (host,result['Message']) )
          continue  
        rDict = result['Value']
        for compType in rDict:
          if componentType and componentType != compType:
            continue
          for system in rDict[compType]:
            components = rDict[compType][system].keys()
            components.sort()
            for component in components:
              if componentName and not componentName in component:
                continue
              record = []
              if rDict[compType][system][component]['Installed']:
                module = str( rDict[compType][system][component]['Module'] )
                record += [ system,component,module,compType.lower()[:-1]]
                if resultInfo['OK'] and host in resultInfo['Value'] and resultInfo['Value'][host]['OK']:
                  setup = resultInfo['Value'][host]['Value']['Setup']
                else:
                  setup = 'Unknown'
                if setupName and not setupName in setup:
                  continue  
                record += [setup]    
                record += [host]  
                record += [str( rDict[compType][system][component]['RunitStatus'] )]
                record += [str( rDict[compType][system][component]['Timeup'] )]
                records.append(record)  
      printTable( fields, records, sortOption )        
      if silentHosts:
        print "\n %d out of %d hosts did not respond" % ( len( silentHosts ), len( respondingHosts ) )

  def default( self, args ):

    argss = args.split()
    command = argss[0]
    if command in ['ls','cat','pwd','chown','chmod','chgrp',
                   'id','date','uname','cp','mv','scp']:
      self.do_exec( args )

  def do_exit( self, args ):
    """ Exit the shell.

    usage: exit
    """
    gLogger.notice( '' )
    sys.exit( 0 )

  def do_quit( self, args ):
    """ Exit the shell.

    usage: quit
    """
    gLogger.notice( '' )
    sys.exit( 0 )

  def emptyline( self ):
    pass

