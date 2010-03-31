########################################################################
# $HeadURL$
########################################################################
"""  LemonAgent reports the state of all installed and set up services and agents. This output is then 
     used in lemon sensors.                                                                            
"""                                                                                                    
__RCSID__ = "$Id$"

import DIRAC
from DIRAC  import gLogger, gConfig, gMonitor, S_OK, S_ERROR
from DIRAC.Core.Base.AgentModule import AgentModule         
from DIRAC.FrameworkSystem.Client.SystemAdministratorClient import SystemAdministratorClient
from socket import gethostname;

class LemonAgent( AgentModule ):
                                
  def initialize( self ):       
    self.NON_CRITICAL = "NonCritical"
    self.CRITICAL = "Critical"
    self.FAILURE = "FAILURE"
    self.OK = "OK"

    self.setup = gConfig.getValue('/DIRAC/Setup','LHCb-Development')
    self.outputNonCritical = True
    #all components not present here will be treated as non critical

    self.admClient = SystemAdministratorClient('localhost')

    return S_OK()

  def execute( self ):
    """ Main execution method
    """

    monitoredSetups = gConfig.getValue('/Operations/lhcb/Lemon/MonitoredSetups', ['LHCb-Production'])
    self.monitoringEnabled = self.setup in monitoredSetups

    if not self.monitoringEnabled:
      self._log("Framewok/LemonAgent", self.NON_CRITICAL, self.OK, "Monitoring not enabled for this setup: " + self.setup +". Exiting.");
      return S_OK()
    
    hostsInMaintenance = gConfig.getValue('/Operations/lhcb/Lemon/HostsInMaintenance',[]);
    if gethostname() in hostsInMaintenance:
      self._log("Framewok/LemonAgent", self.NON_CRITICAL, self.OK, "I am in maintenance mode, exiting.");
      return S_OK()

    result = self.admClient.getOverallStatus()

    if not result or not result['OK']:
      self._log("Framework/LemonAgent", self.CRITICAL, self.FAILURE, "Can not obtain result!!");
      return S_OK()	

    services = result[ 'Value' ][ 'Services' ]
    agents = result[ 'Value' ][ 'Agents' ]
    self._processResults(services);
    self._processResults(agents);

    return S_OK()

  def _processResults(self, results):
    for system in results:
      for part in results[system]:
        component = results[system][part]
        componentName = system + "/" + part
        if component['Setup'] == True:   #we want to monitor only set up services and agents
          critLevel = self._getCriticality(componentName)
          if critLevel == self.NON_CRITICAL and self.outputNonCritical == False:
            continue
          if component['RunitStatus'] == 'Run':
            self._log(componentName, self._getCriticality(componentName), self.OK, "Service/Agent running fine");
          else:
            self._log(componentName, self._getCriticality(componentName), self.FAILURE, "Service/Agent failure!");
    #    else:
    #      if component['Installed'] == True:
    #        print componentName + " is installed but not set up"

  def _getCriticality(self, component):
    #lets try to retrieve common criticality first
    criticality = gConfig.getValue('/Operations/lhcb/Lemon/Criticalities/' + component, self.NON_CRITICAL)
    #maybe it got redefined in <setup> subtree:
    criticality = gConfig.getValue('/Operations/lhcb/' + self.setup + '/Lemon/Criticalities/' + component, criticality)
    return criticality

  def _log( self, component, criticality, status, string ):
    gLogger.info( "LEMON " + criticality + " " + status + " " + component + ": " +string + "\n")
