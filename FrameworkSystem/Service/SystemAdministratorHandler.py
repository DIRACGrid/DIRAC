# $HeadURL:  $

""" SystemAdministrator service is a tool to control and monitor the DIRAC services and agents
"""

__RCSID__ = "$Id:  $"

import os
from DIRAC import S_OK, S_ERROR, gConfig, gLogger
from DIRAC.FrameworkSystem.ComponentMonitoringDB import ComponentMonitoringDB

cmDB = None
DIRACROOT = '/opt/dirac/pro'

def initializeSystemAdministratorHandler( serviceInfo ):

  global cmDB
  cmDB = ComponentMonitoringDB()
  return S_OK()


class SystemAdministratorHandler( RequestHandler ):

  types_getSoftwareComponents = [ ]
  def export_getSoftwareComponents(self):
    """  Get the list of all the components ( services and agents ) that can be configured
         on the system
    """
    result = self.getRemoteCredentials()
    user = result['username']
    group = result['group']
    
    extensions = gConfig.getValue('/DIRAC/Extensions',[])
    services = {}
    agents = {}
    
    systemList = os.listdir(DIRACROOT+'/DIRAC')
    for extension in ['DIRAC']+extensions:
      for system in systemList:
        try:
          agentList = os.listdir(DIRACROOT+'/%s/%s/Agent' % (extension,system) )
          for agent in agentList:
            if not agents.has_key(system):
              agents[system] = []
            agents[system].append(agent)  
        except OSError:
          pass  
        try:
          serviceList = os.listdir(DIRACROOT+'/%s/%s/Service' % (extension,system) )
          for service in serviceList:
            if service.find('Handler') != -1:
              if not agents.has_key(system):
                services[system] = []
              services[system].append(service)  
        except OSError:
          pass  
        
    resultDict = {}
    resultDict['Services'] = services
    resultDict['Agents'] = agents   
    return S_OK()        
    
    