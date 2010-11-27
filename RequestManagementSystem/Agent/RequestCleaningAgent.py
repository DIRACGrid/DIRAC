########################################################################
# $HeadURL:  $
########################################################################

"""  The RequestCleaning agent removes the already executed requests from the database after
     a grace period.
"""

from DIRAC  import gLogger, S_OK, S_ERROR
from DIRAC.Core.Base.AgentModule import AgentModule
from DIRAC.RequestManagementSystem.Client.RequestClient import RequestClient
from DIRAC.Core.Utilities.Time import dateTime,day

__RCSID__ = "$Id: $"

AGENT_NAME = 'RequestManagement/ZuziaAgent'

class RequestCleaningAgent(AgentModule):

  def initialize(self):

    self.graceRemovalPeriod = self.am_getOption('GraceRemovalPeriod',7)
    self.checkAssigned = self.am_getOption('CheckAssigned',True)
    self.ftsCleaning = self.am_getOption('FTSCleaning',True)
    self.requestClient = RequestClient()
    
    return S_OK()

  def execute(self):
    """ Main execution method
    """
    
    toDate = dateTime() - day*self.graceRemovalPeriod
    result = self.requestClient.selectRequests({'Status':'Done','ToDate':str(toDate)})
    if not result['OK']:
      return result
    requestDict = result['Value']
    for rID,rName in requestDict.items():

      gLogger.verbose("Removing request %s" % rName)
      result = self.requestClient.deleteRequest(rName)
      if not result['OK']:
        gLogger.error('Failed to delete request %s' % rName, result['Message'])
      else:
        gLogger.info('Successfully removed request %d/%s' % (rID,rName) )
      
    if self.checkAssigned:
      pass

    if self.ftsCleaning:
      pass
    
    return S_OK()  
