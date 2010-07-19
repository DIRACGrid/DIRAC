#!/usr/bin/env python
########################################################################
# $HeadURL$
########################################################################
""" File Catalog Client Command Line Interface. """

__RCSID__ = "$Id$"

import stat
import sys
import cmd
import commands
import os.path
from types  import *
from DIRAC  import gConfig, gLogger, S_OK, S_ERROR
from DIRAC.FrameworkSystem.Client.SystemAdministratorClient import SystemAdministratorClient

class SystemAdministratorClientCLI(cmd.Cmd):
  """ 
  """

  def __init__(self,host=None):
    cmd.Cmd.__init__(self)
    self.host = host
    self.prompt = 'nohost >'
    if self.host:
      self.prompt = '%s >' % self.host
    self.rootPwd = ''  
    
  def do_set(self,args):
    """ Set the host to be managed
    
        usage:
        
          set host <hostname>
    """
    argss = args.split()
    option = argss[0]
    if option == 'host':
      host = argss[1]
    
    self.host = host
    self.prompt = '%s >' % self.host
    
  def __getSoftwareComponents(self):
    
    componentDict = {}
    client = SystemAdministratorClient(self.host)
    result = client.getSoftwareComponents()
    if not result['OK']:
      print "ERROR:",result['Message']
      return componentDict
    rDict = result['Value']
    for compType in rDict:
      for system in rDict[compType]:
        for component in rDict[compType][system]:  
          if not componentDict.has_key(component):
            componentDict[component] = [(system,compType[:-1])]
          else:
            componentDict[component].append((system,compType[:-1]))
            
    return componentDict          
      
  
  def do_show(self,args):
    """ 
        Add a record to the File Catalog
    
        usage:
        
          show software      - show components for which software is available
          show installed     - show components installed in the host
          show status        - show status of the installed components
          show database      - show the status of the databases
          show mysql         - show the status of the MySQL server
          show log <system> <service|agent>
          show info    - show version of software and setup
    """
    
    argss = args.split()
    option = argss[0]
    del argss[0]
    if option == 'software':
      client = SystemAdministratorClient(self.host)
      result = client.getSoftwareComponents()
      if not result['OK']:
        print "ERROR:",result['Message']
      else:
        serviceCount = 0
        agentCount = 0  
        for compType in result['Value']:
          for system in result['Value'][compType]:
            for component in result['Value'][compType][system]:
              print compType.ljust(8),system.ljust(28),component.ljust(28)
              if compType == 'Services':
                serviceCount += 1
              if compType == 'Agents':
                agentCount += 1                  
        print "Total: %d services, %d agents" % (serviceCount,agentCount)       
    elif option == 'installed':
      client = SystemAdministratorClient(self.host)
      result = client.getSetupComponents()
      if not result['OK']:
        print "ERROR:",result['Message']
      else:  
        serviceCount = 0
        agentCount = 0  
        for compType in result['Value']:
          for system in result['Value'][compType]:
            for component in result['Value'][compType][system]:
              print compType.ljust(8),system.ljust(28),component.ljust(28) 
              if compType == 'Services':
                serviceCount += 1
              if compType == 'Agents':
                agentCount += 1  
        print "Total: %d services, %d agents" % (serviceCount,agentCount)
    elif option == 'status':
      client = SystemAdministratorClient(self.host)
      result = client.getOverallStatus()
      if not result['OK']:
        print "ERROR:",result['Message']
      else:  
        rDict = result['Value']
        print "   System",' '*20,'Name',' '*5,'Type',' '*23,'Setup    Installed   Runit    Uptime    PID'
        print '-'*116
        for compType in rDict:
          for system in rDict[compType]:
            for component in rDict[compType][system]:
              if rDict[compType][system][component]['Installed']:
                print  system.ljust(28),component.ljust(28),compType.lower()[:-1].ljust(7),
                if rDict[compType][system][component]['Setup']:
                  print 'SetUp'.rjust(12),
                else:
                  print 'NotSetup'.rjust(12),  
                if rDict[compType][system][component]['Installed']:
                  print 'Installed'.rjust(12),
                else:
                  print 'NotInstalled'.rjust(12),
                print str(rDict[compType][system][component]['RunitStatus']).ljust(7),  
                print str(rDict[compType][system][component]['Timeup']).rjust(7),
                print str(rDict[compType][system][component]['PID']).rjust(8),
                print  
    elif option == 'database' or option == 'databases':
      client = SystemAdministratorClient(self.host)
      result = client.getDatabases(self.rootPwd)
      if not result['OK']:
        print "ERROR:",result['Message']
        return
      if result.has_key('MySQLPassword'):
        self.rootPwd = result['MySQLPassword']  
      resultSW = client.getSoftwareDatabases()
      if not resultSW['OK']:
        print "ERROR:",resultSW['Message']
        return
        
      sw = resultSW['Value']
      installed = result['Value']
      print
      for db in sw:
        if db in installed:
          print db.rjust(25),': Installed'
        else:
          print db.rjust(25),': Not installed'    
      if not sw:
        print "No database found"  
    elif option == 'mysql':
      client = SystemAdministratorClient(self.host)
      result = client.getMySQLStatus()
      if not result['OK']:
        print "ERROR:",result['Message']
      elif result['Value']:  
        for par,value in result['Value'].items():
          print par.rjust(28),':',value
      else:
        print "No MySQL database found"        
    elif option == "log":
      self.getLog(argss)         
    elif option == "info":
      client = SystemAdministratorClient(self.host)
      result = client.getInfo()
      if not result['OK']:
        print "ERROR:",result['Message']
      print "Setup:", result['Value']['Setup'] 
      print "DIRAC version:",result['Value']['DIRAC']
      if result['Value']['Extensions']:
        for e,v in result['Value']['Extensions'].items():
          print "%s version" % e,v
    else:
      print "Unknown option:",option          
      
  def getLog(self,argss):
    """ Get the tail of the log file of the given component
    """    
    system = argss[0]
    component = argss[1]
    client = SystemAdministratorClient(self.host)
    result = client.getLogTail(system,component,40)
    if not result['OK']:
      print "ERROR:",result['Message']  
    elif result['Value']:  
      
      print result['Value']
      
      #for line in result['Value'].split('\n'):
      #  print line.strip()
    else:
      print "No logs found"    
      
  def do_install(self,args):
    """ 
        Install various DIRAC components 
    
        usage:
        
          install mysql
          install db <database>
          install service <system> <service>
          install agent <system> <agent>
          install <component>
    """    
    argss = args.split()
    option = argss[0]
    del argss[0]
    if option == "mysql":
      print "Installing MySQL database, this can take a while ..."
      client = SystemAdministratorClient(self.host)
      result = client.installMySQL(self.rootPwd)
      if not result['OK']:
        print "ERROR:",result['Message']
      else:
        print "MySQL:", result['Value']  
        print "You might need to restart SystemAdministrator service to take new settings into account"
        if result.has_key('MySQLPassword'):
          self.rootPwd = result['MySQLPassword']  
    elif option == "db":
      database = argss[0]
      client = SystemAdministratorClient(self.host)
      result = client.installDatabase(database,self.rootPwd)
      if not result['OK']:
        print "ERROR:",result['Message']
        return
      if result.has_key('MySQLPassword'):
        self.rootPwd = result['MySQLPassword'] 
      extension,system = result['Value']
      result = client.addCSDatabaseOptions(system,database,self.host)
      if not result['OK']:
        print "ERROR:",result['Message']
        return
      print "Database %s from %s installed successfully" % (database,extension) 
    elif option == "service" or option == "agent":
      system = argss[0]
      component = argss[1]
      client = SystemAdministratorClient(self.host)
      result = client.setupComponent(system,component)
      if not result['OK']:
        print "ERROR:",result['Message']
        return      
      compType = result['Value']['ComponentType']
      runit = result['Value']['RunitStatus']
      result = client.addCSDefaultOptions(system,component,self.host)
      if not result['OK']:
        print "ERROR:",result['Message']
        return
      print "%s %s_%s is installed, runit status: %s" % (compType,system,component,runit)  
    else:
      componentDict = self.__getSoftwareComponents()
      if componentDict.has_key(option):
        if len(componentDict[option]) == 1:
          system,compTypeSoft = componentDict[option][0]
          client = SystemAdministratorClient(self.host)
          result = client.addSystemInstance(system)
          if not result['OK']:
            print "ERROR:",result['Message']
            return  
          instance = result['Value']
          result = client.setupComponent(system,option)
          if not result['OK']:
            print "ERROR:",result['Message']
            return      
          compType = result['Value']['ComponentType']
          if compType.lower() == 'unknown':
            compType = compTypeSoft.lower()  
          runit = result['Value']['RunitStatus']
          result = client.addCSDefaultOptions(system,option,self.host)
          if not result['OK']:
            print "ERROR:",result['Message']
            return
          print "%s %s_%s is installed in instance %s, runit status: %s" % (compType,system,option,instance,runit)
        elif len(componentDict[option]) > 1:
          print "Ambiguous component choice:"
          i = 0
          for system,compType in componentDict[option]:
            i += 1
            print "%d. %s %s in %s system" % (i,compType,option,system)
          print "Use install %s command instead" % compType
        else:
          print "Component %s not found" % option    
            
      
  def do_start(self,args):
    """ Start services or agents or database server    
      
        usage:
        
          start <system|*> <service|agent|*>
          start mysql
    """  
    argss = args.split()
    if argss[0] != 'mysql':
      system = argss[0]
      if system != '*':
        component = argss[1]
      else:
        component = '*'  
      client = SystemAdministratorClient(self.host)
      result = client.startComponent(system,component)
      if not result['OK']:
        print "ERROR:",result['Message']
      else:
        if system != '*' and component != '*':
          print "\n%s_%s started successfully, runit status:\n" % (system,component)
        else:
          print "\nComponents started successfully, runit status:\n" 
        for comp in result['Value']:
          print comp.rjust(32),':',result['Value'][comp]['RunitStatus']
    else:
      print "Not yet implemented"  
      
  def do_restart(self,args):
    """ Restart services or agents or database server    
      
        usage:
        
          restart <system|*> <service|agent|*>
          restart mysql
    """  
    argss = args.split()
    if argss[0] != 'mysql':
      system = argss[0]
      if system != '*':
        component = argss[1]
      else:
        component = '*'  
      client = SystemAdministratorClient(self.host)
      result = client.restartComponent(system,component)
      if not result['OK']:
        if system == '*':
          print "All systems are restarted, connection to SystemAdministrator is lost"
        else:  
          print "ERROR:",result['Message']
      else:
        if system != '*' and component != '*':
          print "\n%s_%s started successfully, runit status:\n" % (system,component)
        else:
          print "\nComponents started successfully, runit status:\n" 
        for comp in result['Value']:
          print comp.rjust(32),':',result['Value'][comp]['RunitStatus']
    else:
      print "Not yet implemented"      
      
  def do_stop(self,args):
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
      client = SystemAdministratorClient(self.host)
      result = client.stopComponent(system,component)
      if not result['OK']:
        print "ERROR:",result['Message']
      else:
        if system != '*' and component != '*':
          print "\n%s_%s stopped successfully, runit status:\n" % (system,component)
        else:
          print "\nComponents stopped successfully, runit status:\n" 
        for comp in result['Value']:
          print comp.rjust(32),':',result['Value'][comp]['RunitStatus']
    else:
      print "Not yet implemented"          
    
  def do_update(self,args):
    """ Update the software on the target host
    
        usage:
          
          update <version> 
    """
    argss = args.split()
    version = argss[0]
    client = SystemAdministratorClient(self.host)
    print "Software update can take a while, please wait ..."
    result = client.updateSoftware(version)    
    if not result['OK']:
      print "ERROR:",result['Message']
    else:
      print "Software successfully updated." 
      print "You should restart the services to use the new software version."
    
  def do_add(self,args):
    """ 
        Add new entity to the Configuration Service
    
        usage:
        
          add instance <system> [<instance_name>]
          
          System instance name is Production by default
    """           
    argss = args.split()
    option = argss[0]
    del argss[0]
    if option == "instance":
      system = argss[0]
      if len(argss)>1:
        instance = argss[1]
      else:
        instance = "Production"  
      client = SystemAdministratorClient(self.host)
      result = client.addSystemInstance(system,instance)
      if not result['OK']:
        print "ERROR:",result['Message']
      else:
        print "%s system instance %s added successfully" % (system,instance) 
        
  def do_exec(self,args ):
    """ Execute a shell command on the remote host and get back the output
    
        usage:
        
          exec <cmd> [<arguments>]
    """         
    client = SystemAdministratorClient(self.host)
    result = client.executeCommand(args)    
    if not result['OK']:
      print "ERROR:",result['Message']
    status,output,error = result['Value']  
    for line in output.split('\n'):
      print line  
    if error:
      print "Error:", status
      for line in error.split('\n'):
        print line    
        
  def do_execfile(self,args ):
    """ Execute a series of administrator CLI commands from a given file
    
        usage:
        
          execfile <filename>
    """
    argss = args.split()
    fname = argss[0]
    execfile = open(fname,'r')
    lines = execfile.readlines()
    execfile.close()
    
    for line in lines:
      line = line.strip()
      if not line:
        continue
      if line[0] == "#":
        continue
      print "\n--> Executing %s\n" % line
      elements = line.split()
      command = elements[0]
      args = ' '.join(elements[1:])
      result = eval("self.do_%s(args)" % command)          
      
  def do_exit(self, args):
    """ Exit the shell.

    usage: exit
    """
    sys.exit(0)
    
  def do_quit(self, args):
    """ Exit the shell.

    usage: quit
    """
    sys.exit(0)  

  def emptyline(self): 
    pass      
      
