#!/usr/bin/env python
########################################################################
# $HeadURL$
########################################################################
""" File Catalog Client Command Line Interface. """

__RCSID__ = "$Id$"

import cmd
import sys
import pprint
import getpass
from DIRAC.FrameworkSystem.Client.SystemAdministratorClient import SystemAdministratorClient
import DIRAC.Core.Utilities.InstallTools as InstallTools
from DIRAC import gConfig

class SystemAdministratorClientCLI( cmd.Cmd ):
  """ 
  """
  def __init__( self, host = None ):
    cmd.Cmd.__init__( self )
    # Check if Port is given
    self.host = None
    self.port = None
    self.prompt = 'nohost >'
    self.__setHost( host )

  def __setHost( self, host ):
    if host:
      self.prompt = '%s >' % host
      hostList = host.split( ':' )
      self.host = hostList[0]
      if len( hostList ) == 2:
        self.port = hostList[1]

  def do_set( self, args ):
    """
        Set the host to be managed
    
        usage:
        
          set host <hostname>
    """
    argss = args.split()
    if len( argss ) < 2:
      print self.do_set.__doc__
      return

    option = argss[0]
    del argss[0]
    if option == 'host':
      host = argss[0]
      self.__setHost( host )
    else:
      print "Unknown option:", option

  def do_show( self, args ):
    """ 
        Show list of components
        
        usage:
    
          show software      - show components for which software is available
          show installed     - show components installed in the host
          show setup         - show components set up in the host
          show status        - show status of the installed components
          show database      - show the status of the databases
          show mysql         - show the status of the MySQL server
          show log <system> <service|agent>
          show info    - show version of software and setup
    """

    argss = args.split()
    if not argss:
      print self.do_show.__doc__
      return

    option = argss[0]
    del argss[0]
    if option == 'software':
      client = SystemAdministratorClient( self.host, self.port )
      result = client.getSoftwareComponents()
      if not result['OK']:
        print " ERROR:", result['Message']
      else:
        print
        pprint.pprint( result['Value'] )
    elif option == 'installed':
      client = SystemAdministratorClient( self.host, self.port )
      result = client.getInstalledComponents()
      if not result['OK']:
        print " ERROR:", result['Message']
      else:
        print
        pprint.pprint( result['Value'] )
    elif option == 'setup':
      client = SystemAdministratorClient( self.host, self.port )
      result = client.getSetupComponents()
      if not result['OK']:
        print " ERROR:", result['Message']
      else:
        print
        pprint.pprint( result['Value'] )
    elif option == 'status':
      client = SystemAdministratorClient( self.host, self.port )
      result = client.getOverallStatus()
      if not result['OK']:
        print "ERROR:", result['Message']
      else:
        rDict = result['Value']
        print
        print "   System", ' '*20, 'Name', ' '*5, 'Type', ' '*23, 'Setup    Installed   Runit    Uptime    PID'
        print '-' * 116
        for compType in rDict:
          for system in rDict[compType]:
            for component in rDict[compType][system]:
              if rDict[compType][system][component]['Installed']:
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
    elif option == 'database' or option == 'databases':
      client = SystemAdministratorClient( self.host, self.port )
      InstallTools.mysqlPassword = "LocalConfig"
      InstallTools.getMySQLPasswords()
      result = client.getDatabases(InstallTools.mysqlRootPwd)
      if not result['OK']:
        print "ERROR:", result['Message']
        return
      resultSW = client.getAvailableDatabases()
      if not resultSW['OK']:
        print "ERROR:", resultSW['Message']
        return

      sw = resultSW['Value']
      installed = result['Value']
      print
      for db in sw:
        if db in installed:
          print db.rjust( 25 ), ': Installed'
        else:
          print db.rjust( 25 ), ': Not installed'
      if not sw:
        print "No database found"
    elif option == 'mysql':
      client = SystemAdministratorClient( self.host, self.port )
      result = client.getMySQLStatus()
      if not result['OK']:
        print "ERROR:", result['Message']
      elif result['Value']:
        print
        for par, value in result['Value'].items():
          print par.rjust( 28 ), ':', value
      else:
        print "No MySQL database found"
    elif option == "log":
      self.getLog( argss )
    elif option == "info":
      client = SystemAdministratorClient( self.host, self.port )
      result = client.getInfo()
      if not result['OK']:
        print "ERROR:", result['Message']
      else:
        print
        print "Setup:", result['Value']['Setup']
        print "DIRAC version:", result['Value']['DIRAC']
        if result['Value']['Extensions']:
          for e, v in result['Value']['Extensions'].items():
            print "%s version" % e, v
        print    
    else:
      print "Unknown option:", option

  def getLog( self, argss ):
    """ Get the tail of the log file of the given component
    """
    if len( argss ) < 2:
      print
      print self.do_show.__doc__
      return

    system = argss[0]
    component = argss[1]
    nLines = 40
    if len(argss) > 2:
      nLines = int(argss[2])
    client = SystemAdministratorClient( self.host, self.port )
    result = client.getLogTail( system, component, nLines )        
    if not result['OK']:
      print "ERROR:", result['Message']
    elif result['Value']:    
      for line in result['Value']['_'.join([system,component])].split('\n'):
        print '   ',line
    
    else:
      print "No logs found"

  def do_install( self, args ):
    """ 
        Install various DIRAC components 
    
        usage:
        
          install mysql
          install db <database>
          install service <system> <service>
          install agent <system> <agent>
    """
    argss = args.split()
    if not argss:
      print self.do_install.__doc__
      return

    option = argss[0]
    del argss[0]
    if option == "mysql":
      print "Installing MySQL database, this can take a while ..."
      client = SystemAdministratorClient( self.host, self.port )
      if InstallTools.mysqlPassword == "LocalConfig":
        InstallTools.mysqlPassword = ''
      InstallTools.getMySQLPasswords()
      result = client.installMySQL(InstallTools.mysqlRootPwd,InstallTools.mysqlPassword)
      if not result['OK']:
        print "ERROR:", result['Message']
      else:
        print "MySQL:", result['Value']
        print "You might need to restart SystemAdministrator service to take new settings into account"
    elif option == "db":
      if not argss:
        print self.do_install.__doc__
        return

      database = argss[0]
      client = SystemAdministratorClient( self.host, self.port )
      InstallTools.getMySQLPasswords()
      result = client.installDatabase( database, InstallTools.mysqlRootPwd )
      if not result['OK']:
        print "ERROR:", result['Message']
        return
      extension, system = result['Value']
      # result = client.addDatabaseOptionsToCS( system, database )
      result = InstallTools.addDatabaseOptionsToCS(gConfig,system, database)
      if not result['OK']:
        print "ERROR:", result['Message']
        return
      print "Database %s from %s/%s installed successfully" % ( database, extension, system )
    elif option == "service" or option == "agent":
      if len( argss ) < 2:
        print self.do_install.__doc__
        return

      system = argss[0]
      component = argss[1]
      client = SystemAdministratorClient( self.host, self.port )
      # First need to update the CS
      # result = client.addDefaultOptionsToCS( option, system, component )
      extensions = gConfig.getValue('/DIRAC/Extensions',[])
      result = InstallTools.addDefaultOptionsToCS(gConfig, option, system, component, extensions)
      if not result['OK']:
        print "ERROR:", result['Message']
        return
      # Then we can install and start the component
      result = client.setupComponent( option, system, component )
      if not result['OK']:
        print "ERROR:", result['Message']
        return
      compType = result['Value']['ComponentType']
      runit = result['Value']['RunitStatus']
      print "%s %s_%s is installed, runit status: %s" % ( compType, system, component, runit )
    else:
      print "Unknown option:", option

  def do_start( self, args ):
    """ Start services or agents or database server    
      
        usage:
        
          start <system|*> <service|agent|*>
          start mysql
    """
    argss = args.split()
    if len( argss ) < 2:
      print self.do_start.__doc__
      return
    option = argss[0]
    del argss[0]

    if option != 'mysql':
      if len( argss ) < 1:
        print self.do_start.__doc__
        return
      system = option
      if system != '*':
        component = argss[0]
      else:
        component = '*'
      client = SystemAdministratorClient( self.host, self.port )
      result = client.startComponent( system, component )
      if not result['OK']:
        print "ERROR:", result['Message']
      else:
        if system != '*' and component != '*':
          print "\n%s_%s started successfully, runit status:\n" % ( system, component )
        else:
          print "\nComponents started successfully, runit status:\n"
        for comp in result['Value']:
          print comp.rjust( 32 ), ':', result['Value'][comp]['RunitStatus']
    else:
      print "Not yet implemented"

  def do_restart( self, args ):
    """ Restart services or agents or database server    
      
        usage:
        
          restart <system|*> <service|agent|*>
          restart mysql
    """
    argss = args.split()
    option = argss[0]
    del argss[0]
    if option != 'mysql':
      if option != "*":
        if len( argss ) < 1:
          print self.do_restart.__doc__
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
          print "All systems are restarted, connection to SystemAdministrator is lost"
        else:
          print "ERROR:", result['Message']
      else:
        if system != '*' and component != '*':
          print "\n%s_%s started successfully, runit status:\n" % ( system, component )
        else:
          print "\nComponents started successfully, runit status:\n"
        for comp in result['Value']:
          print comp.rjust( 32 ), ':', result['Value'][comp]['RunitStatus']
    else:
      print "Not yet implemented"

  def do_stop( self, args ):
    """ Stop services or agents or database server    
      
        usage:
        
          stop <system|*> <service|agent|*>
          stop mysql
    """
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
        print "ERROR:", result['Message']
      else:
        if system != '*' and component != '*':
          print "\n%s_%s stopped successfully, runit status:\n" % ( system, component )
        else:
          print "\nComponents stopped successfully, runit status:\n"
        for comp in result['Value']:
          print comp.rjust( 32 ), ':', result['Value'][comp]['RunitStatus']
    else:
      print "Not yet implemented"

  def do_update( self, args ):
    """ Update the software on the target host to a given version
    
        usage:
          
          update <version> 
    """
    argss = args.split()
    version = argss[0]
    client = SystemAdministratorClient( self.host, self.port )
    print "Software update can take a while, please wait ..."
    result = client.updateSoftware( version )
    if not result['OK']:
      print "ERROR:", result['Message']
    else:
      print "Software successfully updated."
      print "You should restart the services to use the new software version."

  def do_add( self, args ):
    """ 
        Add new entity to the Configuration Service
    
        usage:
        
          add instance <system> <instance>
    """
    argss = args.split()
    option = argss[0]
    del argss[0]
    if option == "instance":
      system = argss[0]
      instance = argss[1]
      client = SystemAdministratorClient( self.host, self.port )
      result = client.addSystemInstance( system, instance )
      if not result['OK']:
        print "ERROR:", result['Message']
      else:
        print "%s system instance %s added successfully" % ( system, instance )
    else:
      print "Unknown option:", option

  def do_exec( self, args ):
    """ Execute a shell command on the remote host and get back the output
    
        usage:
        
          exec <cmd> [<arguments>]
    """
    client = SystemAdministratorClient( self.host, self.port )
    result = client.executeCommand( args )
    if not result['OK']:
      print "ERROR:", result['Message']
    status, output, error = result['Value']
    print
    for line in output.split( '\n' ):
      print line
    if error:
      print "Error:", status
      for line in error.split( '\n' ):
        print line

  def do_execfile( self, args ):
    """ Execute a series of administrator CLI commands from a given file
    
        usage:
        
          execfile <filename>
    """
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
      print "\n--> Executing %s\n" % line
      elements = line.split()
      command = elements[0]
      args = ' '.join( elements[1:] )
      eval( "self.do_%s(args)" % command )

  def do_exit( self, args ):
    """ Exit the shell.

    usage: exit
    """
    print
    sys.exit( 0 )

  def do_quit( self, args ):
    """ Exit the shell.

    usage: quit
    """
    print
    sys.exit( 0 )

  def emptyline( self ):
    pass

