########################################################################
# $HeadURL:  $
########################################################################

"""  The RequestCleaning agent removes the already executed requests from the database after
     a grace period.
"""

from DIRAC  import gLogger, gConfig, gMonitor, S_OK, S_ERROR
from DIRAC.Core.Base.AgentModule import AgentModule
from DIRAC.RequestManagementSystem.Client.RequestClient import RequestClient
from DIRAC.ConfigurationSystem.Client import PathFinder

import time,os,re
from types import *

__RCSID__ = "$Id: $"

AGENT_NAME = 'RequestManagement/ZuziaAgent'

class RequestCleaningAgent(AgentModule):

  def initialize(self):

    self.graceRemovalPeriod = self.am_getValue('GraceRemovalPeriod',7)
    self.checkAssigned = self.am_getValue('CheckAssigned',True)
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
      result = self.requestClient.deleteRequest(rName)
      
    if self.checkAssigned:
      pass
    
    return S_OK()  