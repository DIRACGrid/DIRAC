########################################################################
# $HeadURL$
########################################################################

""" The SystemAdministratorClient is a class representing the client of the DIRAC
    SystemAdministrator service. It has also methods to update the Configuration
    Service with the DIRAC components options
""" 

__RCSID__ = "$Id$"

import re, time, random, os, types, getpass
from DIRAC.Core.DISET.RPCClient  import RPCClient
from DIRAC import S_OK, S_ERROR, gLogger,gConfig,rootPath
from DIRAC.Core.Base.Client import Client
from DIRAC.Core.Utilities.CFG import CFG
from DIRAC.ConfigurationSystem.Client.CSAPI import CSAPI

class SystemAdministratorClient(Client):

  def __init__(self,host):
    """ Constructor function. Takes a mandatory host parameter 
    """
    self.setServer('dips://%s:9162/Framework/SystemAdministrator' % host)
      
  def __getComponentType(self,system,component):
    """ Get the component type: service or agent
    """    
    compType = "unknown"
    result = self.getSoftwareComponents()   
    if not result['OK']:
      return 'unknown'
    
    services = result['Value']['Services']
    if services.has_key(system):
      if component in services[system]:
        return 'service'
    agents = result['Value']['Agents']
    if agents.has_key(system):
      if component in agents[system]:
        return 'agent'  
    return 'unknown'  
      
  def __getComponentCFG(self,system,component,compType=None,inst=None):
    """ Get the CFG object of the component default configuration
    """
    if not compType:
      componentType = self.__getComponentType(system,component)
    else:
      componentType = compType
    if not componentType or componentType == 'unknown':
      return S_ERROR('Failed to determine the component type')
    
    if not inst:
      instance = self.__getInstance(system)
      if instance == "Unknown":
        return S_ERROR('Unknown setup')
    else:
      instance = inst
     
    sectionName = 'Services'
    if componentType == 'agent':
      sectionName = 'Agents'

    # Find the component type template
    basePath = '%s/DIRAC/Core/Base/BaseTemplate.cfg' % rootPath
    baseCfg = ''
    if os.path.exists(basePath):
      baseCfg = CFG()
      baseCfg.loadFromFile(basePath)
      if componentType == 'agent':
        baseCfg = baseCfg['Agent']
      else:
        baseCfg = baseCfg['Service']  

    # Find the component options template  
    extensions = gConfig.getValue('/DIRAC/Extensions',[])
    compCfg = ''
    for pkg in extensions+['DIRAC']:
      cfgPath = '/%s/%s/%sSystem/ConfigTemplate.cfg'%(rootPath,pkg,system)
      if os.path.exists(cfgPath):
        # Look up the component in this template
        loadCfg = CFG() 
        loadCfg.loadFromFile(cfgPath)
        try:
          compCfg = loadCfg[sectionName][component]
        except KeyError,x:
          gLogger.warn('No %s section found' % componentType)  
          
    if compCfg:
      if baseCfg:
        compCfg = baseCfg.mergeWith(compCfg)
      return S_OK(compCfg)
    else:
      return S_OK(baseCfg)      

  def __addCSOptions(self,system,component,host=None,override=False):
    """ Add the section with the component options to the CS
    """
    componentType = self.__getComponentType(system,component)
    if not componentType or componentType == 'unknown':
      return S_ERROR('Failed to determine the component type')

    instance = self.__getInstance(system)
    if instance == "Unknown":
      return S_ERROR('Unknown setup')
    
    sectionName = "Agents"
    if componentType == 'service':
      sectionName = "Services"

    # Check if the component CS options exist
    addOptions = True
    if not override:
      result = gConfig.getOptions('/Systems/%s/%s/%s/%s' % (system,instance,sectionName,component) )
      if result['OK']:
        addOptions = False
    if not addOptions:
      return S_OK('Component options already exist')
        
    # Add the component options now
    result = self.__getComponentCFG(system,component,componentType,instance)
    if not result['OK']:
      return result
    compCfg = result['Value']

    cfg = CFG() 
    cfg.createNewSection('Systems')
    cfg.createNewSection('Systems/%s' % system)
    cfg.createNewSection('Systems/%s/%s' % (system,instance) )
    cfg.createNewSection('Systems/%s/%s/%s' % (system,instance,sectionName) )
    cfg.createNewSection('Systems/%s/%s/%s/%s' % (system,instance,sectionName,component ),'',compCfg )

    # Add the service URL
    if componentType == "service":
      port = compCfg.getOption('/Port',0)
      if port and host:
        cfg.createNewSection('Systems/%s/%s/URLs' % (system,instance) )
        cfg.setOption('Systems/%s/%s/URLs/%s' % (system,instance,component),
                      'dips://%s:%d/%s/%s' % (host,port,system,component) )
    
    cfgClient = CSAPI()
    result = cfgClient.downloadCSData()
    if not result['OK']:
      return result
    result = cfgClient.mergeFromCFG(cfg)
    if not result['OK']:
      return result
    result = cfgClient.commit()

    return result

  def addCSDefaultOptions(self,system,component,host=None,override=False):
    """ Add default component options to the global CS or to the local options
    """
    return self.__addCSOptions(system,component,host=host,override=override)
      
  def getDatabases(self,password=None):
    """ Get the installed databases
    """
    if not password:
      pword = getpass.getpass('MySQL root password: ')
    else:
      pword = password  
    server = RPCClient(self.serverURL)
    return server.getDatabases(pword)    
  
  def installMySQL(self,rootpwd=None,diracpwd=None):
    """ Install the MySQL database on the server side
    """
    if not rootpwd:
      rpword = getpass.getpass('MySQL root password: ')
    else:
      rpword = rootpwd
    if not diracpwd:
      dpword = getpass.getpass('MySQL Dirac password: ')
    else:
      dpword = diracpwd
      
    server = RPCClient(self.serverURL)
    return server.installMySQL(rpword,dpword)
  
  def installDatabase(self,database,rootpwd=None):
    """ Install the MySQL database on the server side
    """
    if not rootpwd:
      rpword = getpass.getpass('MySQL root password: ')
    else:
      rpword = rootpwd
      
    server = RPCClient(self.serverURL)
    return server.installDatabase(rpword,database)      
  
  def __getInstance(self,system):
    """ Get the name of the local instance of the given system
    """
    setup = gConfig.getValue('/DIRAC/Setup','')
    if not setup:
      return False
    instance = gConfig.getValue('/DIRAC/Setups/%s/%s' % (setup,system),'Unknown')
    return instance
      
  def __getDatabaseCFG(self,system,dbname,host,user=None,password=None):
    """ Get the CFG configuration file for a database
    """
    instance = self.__getInstance(system)
    if instance == "Unknown":
      return S_ERROR('Unknown setup')
    
    cfg = CFG() 
    cfg.createNewSection('Systems')
    cfg.createNewSection('Systems/%s' % system)
    cfg.createNewSection('Systems/%s/%s' % (system,instance) )
    cfg.createNewSection('Systems/%s/%s/Databases' % (system,instance) )
    secName = 'Systems/%s/%s/Databases/%s' % (system,instance,dbname )
    cfg.createNewSection(secName )
    cfg.setOption(secName+'/DBName',dbname)
    cfg.setOption(secName+'/Host',host)
    cfg.setOption(secName+'/MaxQueueSize',10)
    if user:
      cfg.setOption(secName+'/User',user)
    if password:
      cfg.setOption(secName+'/Password',password)    

    return S_OK(cfg) 
  
  def addCSDatabaseOptions(self,systemName,dbname,host,override=False):
    """ Add the section with the database options to the CS
    """
    system = systemName.replace('System','')
    instance = self.__getInstance(system)
    if instance == "Unknown":
      return S_ERROR('Unknown setup')

    # Check if the component CS options exist
    addOptions = True
    if not override:
      result = gConfig.getOptions('/Systems/%s/%s/Databases/%s' % (system,instance,dbname) )
      if result['OK']:
        addOptions = False
    if not addOptions:
      return S_OK('Database options already exist')
        
    # Add the component options now
    result = self.__getDatabaseCFG(system,dbname,host)
    if not result['OK']:
      return result
    cfg = result['Value']

    cfgClient = CSAPI()
    result = cfgClient.downloadCSData()
    if not result['OK']:
      return result
    result = cfgClient.mergeFromCFG(cfg)
    if not result['OK']:
      return result
    result = cfgClient.commit()

    return result        
    
  def addSystemInstance(self,system,instance):
    """ Add a new system instance
    """  
    
    setup = gConfig.getValue('/DIRAC/Setup','')
    if not setup:
      return S_ERROR('Failed to get setup')
    cfg = CFG()
    cfg.createNewSection('DIRAC')
    cfg.createNewSection('DIRAC/Setups')
    cfg.createNewSection('DIRAC/Setups/%s' % setup)
    cfg.setOption('/DIRAC/Setups/%s/%s' % (setup,system),instance)
    
    cfgClient = CSAPI()
    result = cfgClient.downloadCSData()
    if not result['OK']:
      return result
    result = cfgClient.mergeFromCFG(cfg)
    if not result['OK']:
      return result
    result = cfgClient.commit()

    return result