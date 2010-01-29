# $HeadURL$

""" SystemAdministrator service is a tool to control and monitor the DIRAC services and agents
"""

__RCSID__ = "$Id$"

import os,re
from DIRAC import S_OK, S_ERROR, gConfig, gLogger, shellCall
from DIRAC.Core.DISET.RequestHandler import RequestHandler
from DIRAC.FrameworkSystem.DB.ComponentMonitoringDB import ComponentMonitoringDB

cmDB = None
DIRACROOT = '/opt/dirac/pro'

def initializeSystemAdministratorHandler( serviceInfo ):

  global cmDB
  cmDB = ComponentMonitoringDB()
  return S_OK()


class SystemAdministratorHandler( RequestHandler ):

  types_getSoftwareComponents = [ ]
  def export_getSoftwareComponents(self):
    """  Get the list of all the components ( services and agents ) for which the software
         is installed on the system
    """
    result = self.getRemoteCredentials()
    user = result['username']
    group = result['group']
    
    extensions = gConfig.getValue('/DIRAC/Extensions',[])
    services = {}
    agents = {}
    
    systemList = os.listdir(DIRACROOT+'/DIRAC')
    for extension in ['DIRAC']+[ x+'DIRAC' for x in extensions]:
      for system in systemList:
        try:
          agentList = os.listdir(DIRACROOT+'/%s/%s/Agent' % (extension,system) )
          for agent in agentList:
            if agent[-3:] == ".py":
              afile = open(DIRACROOT+'/%s/%s/Agent/' % (extension,system)+agent,'r')
              body = afile.read()
              afile.close()
              if body.find('AgentModule') != -1 or body.find('OptimizerModuleModule') != -1:
                if not agents.has_key(system):
                  agents[system] = []
                agents[system].append(agent.replace('.py',''))  
        except OSError:
          pass  
        try:
          serviceList = os.listdir(DIRACROOT+'/%s/%s/Service' % (extension,system) )
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
    
  types_getInstalledComponents = [ ]
  def export_getInstalledComponents(self):
    """  Get the list of all the components ( services and agents ) 
         installed on the system in the runit directory
    """
   
    services = {}
    agents = {}
    systemList = os.listdir('/opt/dirac/runit')
    for system in systemList:
      components = os.listdir('/opt/dirac/runit/%s' % system)
      for component in components:
        try:
          rfile = open('/opt/dirac/runit/%s/%s/run' % (system,component),'r')
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
         set up for runnig with runsvdir in /opt/dirac/startup directory 
    """

    services = {}
    agents = {}

    componentList = os.listdir('/opt/dirac/startup')
    for component in componentList:
      try:
        rfile = open('/opt/dirac/startup/%s' % component + '/run','r')
        body = rfile.read()
        rfile.close()
        if body.find('dirac-service') != -1:
          system,service = component.split('_')

          print "AT >>> ss", system,service

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

  types_getRunitComponentStatus = [ ]
  def export_getRunitComponentStatus(self):
    """  Get the list of all the components ( services and agents ) 
         set up for runnig with runsvdir in /opt/dirac/startup directory 
    """
    result = shellCall(0,'runsvstat /opt/dirac/startup/*')
    if not result['OK']:
      return S_ERROR('Failed runsvstat shell call')
    output = result['Value'][1].strip().split('\n')
    componentDict = {}
    for line in output:
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
    
     
