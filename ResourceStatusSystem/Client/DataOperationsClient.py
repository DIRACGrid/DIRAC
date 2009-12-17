""" DataOperationsClient class is a client for to get data operations' statistics.
"""

from datetime import datetime, timedelta
#from DIRAC import gLogger
from DIRAC.ResourceStatusSystem.Utilities.Exceptions import *
from DIRAC.ResourceStatusSystem.Utilities.Utils import *
from DIRAC.AccountingSystem.Client.ReportsClient import ReportsClient
#from DIRAC.Core.DISET.RPCClient import RPCClient

class JobsClient:
  
  def __init__(self):
    self.rc = ReportsClient()

#############################################################################

  def getFailedStats(self, name, fromD = None, toD = None):
    """  
    Return failed transfer stats. If fromD and toD are not specified, takes last 24h.
    
    :params:
      :attr:`name`: string - 
    
      :attr:`fromD`: datetime object: date from which take stats - optional 
    
      :attr:`toD`: datetime object: date to which take stats - optional 

    :return:
      {
      }
    """
    pass
  
#############################################################################
