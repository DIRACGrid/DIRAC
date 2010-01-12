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

  def getQualityStats(self, name, fromD = None, toD = None):
    """  
    Return quialty of transfer stats. If fromD and toD are not specified, takes last 2h.
    
    :params:
      :attr:`name`: string - 
    
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
     
    pr_quality = self.rc.getReport('DataOperation', 'Quality', fromD, toD, 
                      {'OperationType':'putAndRegister', 'Destination':[name]}, 'Channel')
    if not pr_quality['OK']:
      raise RSSException, where(self, self.getQualityStats) + pr_quality['Message']
    
    pr_q_d = pr_quality['Value']['data']
    
    if pr_q_d == {}:
      return {'TransferQuality': None}
    else:
      if len(pr_q_d) == 1:
        values = []
        for k in pr_q_d.keys():
          values.append(pr_q_d[k])
        return {'TransferQuality': sum(values)/len(values)}
      else:
        values = []
        for k in pr_q_d['Total'].keys():
          values.append(pr_q_d['Total'][k])
        return {'TransferQuality': sum(values)/len(values)} 
  
#############################################################################
