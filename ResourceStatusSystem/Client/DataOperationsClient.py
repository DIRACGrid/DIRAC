""" DataOperationsClient class is a client for to get data operations' statistics.
"""

from datetime import datetime, timedelta
#from DIRAC import gLogger
from DIRAC.ResourceStatusSystem.Utilities.Exceptions import *
from DIRAC.ResourceStatusSystem.Utilities.Utils import *
from DIRAC.AccountingSystem.Client.ReportsClient import ReportsClient
#from DIRAC.Core.DISET.RPCClient import RPCClient

class DataOperationsClient:
  
  def __init__(self):
    self.rc = ReportsClient()

#############################################################################

  def getQualityStats(self, granularity, name, fromD = None, toD = None):
    """  
    Return quality of transfer stats. If fromD and toD are not specified, takes last 2 hours.
    
    :params:
      :attr:`granularity`: string - 'Site', 'Resource' or 'StoragaElement'
      
      :attr:`name`: string - the name of the res
    
      :attr:`fromD`: datetime object: date from which take stats - optional 
    
      :attr:`toD`: datetime object: date to which take stats - optional 

    :return:
      {
        {'TransferQuality':'xxxx'}
      }
    """
    
    if fromD is None:
      fromD = datetime.utcnow()-timedelta(hours = 2)
    if toD is None:
      toD = datetime.utcnow()
     
    if granularity in ('StorageElement', 'StorageElements'):
      pr_quality = self.rc.getReport('DataOperation', 'Quality', fromD, toD, 
                        {'OperationType':'putAndRegister', 'Destination':[name]}, 'Channel')
    else:
      raise InvalidRes, where(self, self.getQualityStats)
      
    if not pr_quality['OK']:
      exceptStr = where(self, self.getQualityStats) + pr_quality['Message']
      gLogger.exception(exceptStr,'',errorMsg)
      return {'TransferQuality': None}
    
    pr_q_d = pr_quality['Value']['data']
    
    if pr_q_d == {}:
      return {'TransferQuality': None}
    else:
      if len(pr_q_d) == 1:
        print pr_q_d
        values = []
        for k in pr_q_d.keys():
          for n in pr_q_d[k].values():
            values.append(n)
        return {'TransferQuality': sum(values)/len(values)}
      else:
        values = []
        for n in pr_q_d['Total'].values():
          values.append(n)
        return {'TransferQuality': sum(values)/len(values)} 
  
#############################################################################
