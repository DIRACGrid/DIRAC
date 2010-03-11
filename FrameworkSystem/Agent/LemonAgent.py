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

class LemonAgent( AgentModule ):
                                
  def initialize( self ):       
    self.NON_CRITICAL = "INFO_ONLY"
    self.CRITICAL = "CRITICAL"
    self.FAILURE = "FAILURE"
    self.OK = "OK"

    self.outputNonCritical = True
    #all components not present here will be threated as non critical
    self.criticality = { "Stager/Stager" : self.CRITICAL,
                         "WorkloadManagement/PilotMonitor" : self.NON_CRITICAL,
                         "WorkloadManagement/OutputSandbox" : self.CRITICAL,
                         "WorkloadManagement/SandboxStore" : self.CRITICAL }

    self.admClient = SystemAdministratorClient('localhost')

    return S_OK()

  def execute( self ):
    """ Main execution method
    """

    result = self.admClient.getOverallStatus()

    if not result or not result['OK']:
      self._log("None/None", self.CRITICAL, self.FAILURE, "Can not obtain result!!");

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
    if component not in self.criticality.keys():
       return self.NON_CRITICAL
    else:
       return self.criticality[component]

  def _log( self, component, criticality, status, string ):
    gLogger.info( "LEMON " + criticality + " " + status + " " + component + ": " +string + "\n")
