# $HeadURL$

""" SystemAdministrator service is a tool to control and monitor the DIRAC services and agents
"""

__RCSID__ = "$Id$"

import os, re, shutil, time
from types import *
from DIRAC import S_OK, S_ERROR, gConfig, gLogger, shellCall
from DIRAC.Core.DISET.RequestHandler import RequestHandler
from DIRAC.FrameworkSystem.DB.ComponentMonitoringDB import ComponentMonitoringDB

cmDB = None
DIRACROOT = '/opt/dirac'

def initializeSystemAdministratorHandler( serviceInfo ):

  global cmDB
  cmDB = ComponentMonitoringDB()
  return S_OK()


class SystemAdministratorHandler( RequestHandler ):
  
  def __getSoftwareComponents(self):
    """  Get the list of all the components ( services and agents ) for which the software
         is installed on the system
    """
    result = self.getRemoteCredentials()
    user = result['username']
    group = result['group']
    
    extensions = gConfig.getValue('/DIRAC/Extensions',[])
    services = {}
    agents = {}
    
    systemList = os.listdir(DIRACROOT+'/pro/DIRAC')
    for extension in ['DIRAC']+[ x+'DIRAC' for x in extensions]:
      for system in systemList:
        try:
          agentList = os.listdir(DIRACROOT+'/pro/%s/%s/Agent' % (extension,system) )
          for agent in agentList:
            if agent[-3:] == ".py":
              afile = open(DIRACROOT+'/pro/%s/%s/Agent/' % (extension,system)+agent,'r')
              body = afile.read()
              afile.close()
              if body.find('AgentModule') != -1 or body.find('OptimizerModuleModule') != -1:
                if not agents.has_key(system):
                  agents[system] = []
                agents[system].append(agent.replace('.py',''))  
        except OSError:
          pass  
        try:
          serviceList = os.listdir(DIRACROOT+'/pro/%s/%s/Service' % (extension,system) )
          for service in serviceList:
            if service.find('Handler') != -1 and service[-3:] == '.py':
              if not services.has_key(system):
                services[system] = []
              services[system].append(service.replace('.py','').replace('Handler','') )  
        except OSError:
          pass  
        
    resultDict = {}
    resultDict['Services'] = services
    resultDict['Agents'] = agents   
    return S_OK(resultDict)    

  types_getSoftwareComponents = [ ]
  def export_getSoftwareComponents(self):
    """  Get the list of all the components ( services and agents ) for which the software
         is installed on the system
    """
    return self.__getSoftwareComponents()
    
  types_getInstalledComponents = [ ]
  def export_getInstalledComponents(self):
    """  Get the list of all the components ( services and agents ) 
         installed on the system in the runit directory
    """
   
    services = {}
    agents = {}
    systemList = os.listdir(DIRACROOT+'/runit')
    for system in systemList:
      components = os.listdir(DIRACROOT+'/runit/%s' % system)
      for component in components:
        try:
          rfile = open(DIRACROOT+'/runit/%s/%s/run' % (system,component),'r')
          body = rfile.read()
          rfile.close()
          if body.find('dirac-service') != -1:
            if not services.has_key(system):
              services[system] = []
            services[system].append(component)
          elif body.find('dirac-agent') != -1: 
            if not agents.has_key(system):
              agents[system] = []
            agents[system].append(component)
        except IOError:
          pass 

    resultDict = {}
    resultDict['Services'] = services
    resultDict['Agents'] = agents
    return S_OK(resultDict)

  types_getSetupComponents = [ ]
  def export_getSetupComponents(self):
    """  Get the list of all the components ( services and agents ) 
         set up for running with runsvdir in /opt/dirac/startup directory 
    """
    services = {}
    agents = {}
    componentList = os.listdir(DIRACROOT+'/startup')
    for component in componentList:
      try:
        rfile = open(DIRACROOT+'/startup/%s' % component + '/run','r')
        body = rfile.read()
        rfile.close()
        if body.find('dirac-service') != -1:
          system,service = component.split('_')
          if not services.has_key(system):
            services[system] = []
          services[system].append(service)
        elif body.find('dirac-agent') != -1:
          system,agent = component.split('_') 
          if not agents.has_key(system):
            agents[system] = []
          agents[system].append(agent)
      except IOError:
        pass

    resultDict = {}
    resultDict['Services'] = services
    resultDict['Agents'] = agents
    return S_OK(resultDict)

  def __getRunitComponentStatus(self,componentList):
    """  Get the list of all the components ( services and agents ) 
         set up for runnig with runsvdir in /opt/dirac/startup directory 
    """
    if componentList:
      cList = [ DIRACROOT+'/startup/'+c for c in componentList]
      cString = ' '.join(cList)
    else:
      cString = DIRACROOT+'/startup/*'  
    result = shellCall(0,'runsvstat %s' % cString)
    if not result['OK']:
      return S_ERROR('Failed runsvstat shell call')
    output = result['Value'][1].strip().split('\n')
    
    componentDict = {}
    for line in output:
      if not line:
        continue
      cname,routput = line.split(':')
      cname = cname.replace('/opt/dirac/startup/','')
      run = False
      result = re.search('^ run',routput)
      if result:
        run = True
      down = False
      result = re.search('^ down',routput)
      if result:
        down = True
      result = re.search('([0-9]+) seconds',routput)
      timeup = 0
      if result:
        timeup = result.group(1)
      result = re.search('pid ([0-9]+)',routput)
      pid = 0
      if result:
        pid = result.group(1)
      runsv = "Not running"
      if run or down:
        runsv = "Running" 
      result = re.search('runsv not running',routput)
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

    return S_OK(componentDict)
  
  types_getRunitComponentStatus = [ ListType ]
  def export_getRunitComponentStatus(self,componentList):
    """  Get the list of all the components ( services and agents ) 
         set up for runnig with runsvdir in /opt/dirac/startup directory 
    """
    return self.__getRunitComponentStatus(componentList)
  
  def __getComponentType(self,system,component):
    """ Check the component software and get its type
    """

    componentType = 'unknown'
    result = self.__getSoftwareComponents()
    if not result['OK']:
      return componentType
    softDict = result['Value']
    if softDict['Services'].has_key(system+'System'):
      if component in softDict['Services'][system+'System']:
        componentType = 'service'
    if softDict['Agents'].has_key(system+'System'):
      if component in softDict['Agents'][system+'System']:
        componentType = 'agent'
    
    return componentType
  
  def __installComponent(self,system,component):
    """ Install runit directory for the specified component
    """  
    # Check that the software for the component is installed
    componentType = self.__getComponentType(system,component)
    if componentType == 'unknown':
      return S_ERROR('Software for component %s_%s is not installed' % (system,component) )

    runitDir = DIRACROOT+'/runit/%s/%s' % (system,component)
    if os.path.exists(runitDir):
      result = S_OK(componentType)
      result['Message'] = "%s %s_%s already installed" % (componentType,system,component)
      return result  
 
    if componentType == 'service':
      result = shellCall(0,'install_service.sh %s %s' % (system,component) )
    elif componentType == 'agent':
      result = shellCall(0,'install_agent.sh %s %s' % (system,component) )    
    else:
      return S_ERROR('Faulty component type %s' % componentType)
    
    if not result['OK']:
      return S_ERROR(result['Value'][2])
    
    return S_OK(componentType) 
    
  types_installComponent = [ StringTypes, StringTypes ]
  def export_installComponent(self,system,component):
    """ Install runit directory for the specified component
    """   
    return self.__installComponent(system,component)
  
  types_setupComponent = [ StringTypes, StringTypes ]
  def export_setupComponent(self,system,component):
    """ Setup the specified component for running with the runsv daemon
    """  
    
    # Check that the runit directory is there and sain
    runitDir = DIRACROOT+'/runit/%s/%s' % (system,component)
    if not os.path.exists(runitDir):
      # Check that the software for the component is installed              
      result = self.__installComponent(system,component)
      if not result['OK']:
        return result
      
    sainCheck = True
    message = ''
    if not os.path.exists(runitDir+'/run'):
      sainCheck = False
      message += ' No run script;'
    if not os.path.exists(runitDir+'/log'):
      sainCheck = False
      message += ' No log directory;'
    if not os.path.exists(runitDir+'/log/run'):
      sainCheck = False 
      message += ' No log run script'
    if not sainCheck:
      return S_ERROR('No sain installation: %s' % message )       
    
    # Create the startup entry now
    linkName = DIRACROOT+'/startup/%s_%s' % (system,component)
    if not os.path.lexists(linkName):
      os.symlink(DIRACROOT+'/runit/%s/%s' % (system,component), linkName )
    
    # Check the runsv status
    start = time.time()
    while (time.time()-10) < start: 
      result = self.__getRunitComponentStatus(['%s_%s' % (system,component)])
      if not result['OK']:
        return S_ERROR('Failed to start the component %s_%s' % (system,component) )
      if result['Value']['%s_%s' % (system,component)]['RunitStatus'] == "Run":
        return S_OK("Run")
      time.sleep(1)
    
    # Final check
    result = self.__getRunitComponentStatus(['%s_%s' % (system,component)])
    if not result['OK']:
      return S_ERROR('Failed to start the component %s_%s' % (system,component) )
    
    return S_OK(result['Value']['%s_%s' % (system,component)]['RunitStatus'])  
  
  types_unsetupComponent = [ StringTypes, StringTypes ]
  def export_unsetupComponent(self,system,component):
    """ Setup the specified component for running with the runsv daemon
    """  
    startupDir = DIRACROOT+'/startup/%s_%s' % (system,component)
    if os.path.lexists(startupDir):
      os.unlink(startupDir)
    
    return S_OK()
  
  types_uninstallComponent = [ StringTypes, StringTypes ]
  def export_uninstallComponent(self,system,component):
    """ Setup the specified component for running with the runsv daemon
    """  
    startupDir = DIRACROOT+'/startup/%s_%s' % (system,component)
    if os.path.lexists(startupDir):
      os.unlink(startupDir)
    
    runitDir = DIRACROOT+'/runit/%s/%s' % (system,component)
    if os.path.exists(runitDir):
      shutil.rmtree(runitDir)
      
    return S_OK()
    
  def __startComponent(self,system,component,mode):
    """ Start the specified component for running with the runsv daemon
    """
    template = 'runsvctrl %s '+DIRACROOT+'/startup/%s_%s' 
    result = shellCall(0,template % (mode,system,component) )
    # Check the runsv status
    start = time.time()
    while (time.time()-10) < start: 
      result = self.__getRunitComponentStatus(['%s_%s' % (system,component)])
      if not result['OK']:
        return S_ERROR('Failed to start the component %s_%s' % (system,component) )
      if result['Value']['%s_%s' % (system,component)]['RunitStatus'] == "Run":
        return S_OK("Run")
      time.sleep(1)
    
    # Final check
    result = self.__getRunitComponentStatus(['%s_%s' % (system,component)])
    if not result['OK']:
      return S_ERROR('Failed to start the component %s_%s' % (system,component) )
    
    return S_OK(result['Value']['%s_%s' % (system,component)]['RunitStatus'])    
  
  types_startComponent = [ StringTypes, StringTypes ]
  def export_startComponent(self,system,component):
    """ Start the specified component for running with the runsv daemon
    """ 
    return self.__startComponent(system,component,'u')
  
  types_restartComponent = [ StringTypes, StringTypes ]
  def export_restartComponent(self,system,component):
    """ Start the specified component for running with the runsv daemon
    """ 
    return self.__startComponent(system,component,'t')
  
  types_stopComponent = [ StringTypes, StringTypes ]
  def export_stopComponent(self,system,component):
    """ Start the specified component for running with the runsv daemon
    """ 
    result = shellCall(0,'runsvctrl d '+DIRACROOT+'/startup/%s_%s' % (system,component) )
    # Check the runsv status
    start = time.time()
    while (time.time()-10) < start: 
      result = self.__getRunitComponentStatus(['%s_%s' % (system,component)])
      if not result['OK']:
        return S_ERROR('Failed to stop the component %s_%s' % (system,component) )
      if result['Value']['%s_%s' % (system,component)]['RunitStatus'] == "Down":
        return S_OK("Down")
      time.sleep(1)
    
    # Final check
    result = self.__getRunitComponentStatus(['%s_%s' % (system,component)])
    if not result['OK']:
      return S_ERROR('Failed to stop the component %s_%s' % (system,component) )
    
    return S_OK(result['Value']['%s_%s' % (system,component)]['RunitStatus']) 
     
     
  types_getLogTail = [ StringTypes, StringTypes ]
  def export_getLogTail(self,system,component,length=100):
    """ Get the tail of the component log file
    """       
    
    logFileName = DIRACROOT+'/runit/%s/%s/log/current' % (system,component)
    if not os.path.exists(logFileName):
      return S_ERROR('No log file found')
    
    logFile = open(logFileName,'r')
    lines = logFile.readlines()
    logFile.close()
    
    if len(lines) < length:
      return S_OK( '\n'.join(lines) )
    else:
      tail = '\n'.join(lines[-length:])
      return S_OK(tail)
