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
  
  def do_show(self,args):
    """ 
        Add a record to the File Catalog
    
        usage:
        
          show software
          show installed 
          show status
          show database
          show mysql
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
        print result['Value']
    elif option == 'installed':
      client = SystemAdministratorClient(self.host)
      result = client.getSetupComponents()
      if not result['OK']:
        print "ERROR:",result['Message']
      else:  
        print result['Value']
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
      result = client.getDatabases()
      if not result['OK']:
        print "ERROR:",result['Message']
      resultSW = client.getSoftwareDatabases()
      if not resultSW['OK']:
        print "ERROR:",resultSW['Message']
        
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
    else:
      print "Unknown option:",option          
      
  def do_install(self,args):
    """ 
        Install various DIRAC components 
    
        usage:
        
          install mysql
          install db <database>
          install service <system> <service>
          install agent <system> <agent>
    """    
    argss = args.split()
    option = argss[0]
    del argss[0]
    if option == "mysql":
      client = SystemAdministratorClient(self.host)
      result = client.installMySQL()
      if not result['OK']:
        print "ERROR:",result['Message']
      else:
        print "MySQL installed successfully"  
    elif option == "db":
      database = argss[0]
      client = SystemAdministratorClient(self.host)
      result = client.installDatabase(database)
      if not result['OK']:
        print "ERROR:",result['Message']
        return
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
      print "Unknown option:",option    
      
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
    result = client.updateSoftware(version)    
    if not result['OK']:
      print "ERROR:",result['Message']
    else:
      print "Software successfully updated"
    
  def do_add(self,args):
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
      client = SystemAdministratorClient(self.host)
      result = client.addSystemInstance(system,instance)
      if not result['OK']:
        print "ERROR:",result['Message']
      else:
        print "%s system instance %s added successfully" % (system,instance) 
      
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
      
