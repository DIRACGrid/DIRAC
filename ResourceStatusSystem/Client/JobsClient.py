""" JobResultsClient class is a client for to get jobs' stats.
"""

from datetime import datetime, timedelta
from DIRAC import gLogger
from DIRAC.ResourceStatusSystem.Utilities.Exceptions import *
from DIRAC.ResourceStatusSystem.Utilities.Utils import *
#from DIRAC.AccountingSystem.Client.ReportsClient import ReportsClient
from DIRAC.Core.DISET.RPCClient import RPCClient

class JobsClient:
  
#  def __init__(self):
#    self.rc = ReportsClient()

#############################################################################

  def getJobsStats(self, granularity, name, periods):
    """  
    Return jobs stats of entity in args
    
    :params:
      :attr:`granularity`: string - a ValidRes
    
      :attr:`name`: should be the name of the ValidRes
    
      :attr:`periods`: list - contains the periods to consider in the query

    :return:
      {
        'MeanProcessedJobs': X'

        'LastProcessedJobs': X'
      }
    """
    
    if granularity.capitalize() not in ValidRes:
      raise InvalidRes, where(self, self.getJobStats)
    
    if granularity == 'Site':
      entity = getSiteRealName(name)
      _granularity = 'Site'
    else:
      entity = name
      _granularity = 'GridCE'
    
    #######TODO
#    numberOfJobsLash2Hours = self.rc.getReport('Job', 'NumberOfJobs', 
#                                               datetime.utcnow()-timedelta(hours = 2), 
#                                               datetime.utcnow(), {granularity:[entity]}, 
#                                               'GridStatus')
#    numberOfJobsLash2Hours = self.rc.getReport('Job', 'NumberOfJobs', 
#                                               datetime.utcnow()-timedelta(hours = 2), 
#                                               datetime.utcnow(), {granularity:[entity]}, 
#                                               'GridStatus')
#    
#    for x in numberOfJobs['Value']['data'].itervalues():
#      total = 0
#      for y in x.values():
#        total = total+y
#      
#    print r


#############################################################################

  def getJobsEff(self, granularity, name, periods):
    """
    Return job stats of entity in args for periods
    
    :params:
      :attr:`granularity`: string - should be a ValidRes
    
      :attr:`name` should be the name of the ValidRes
    
      :attr:`periods`: list - contains the periods to consider in the query

    returns:
      {
        'JobsEff': X (0-1)'
      }
    """

    if granularity == 'Site':
      entity = getSiteRealName(name)
      _granularity = 'Site'
    else:
      entity = name
      _granularity = 'GridCE'
    
    #######TODO
#    numberOfJobs = self.rc.getReport('Job', 'NumberOfJobs', 
#                                     datetime.utcnow()-timedelta(hours = 24), 
#                                     datetime.utcnow(), {self._granularity:[self_entity]}, 
#                                     'GridStatus')
    

#############################################################################

  def getSystemCharge(self):
    """ Returns last hour system charge, and the system charge of an hour before

        returns:
          {
            'LastHour': n_lastHour
            'anHourBefore': n_anHourBefore
          }
    """
    
    # qui ho bisogno dei running jobs
    
    #######TODO

#    numberOfJobsLastHour = self.rc.getReport('Job', 'TotalNumberOfJobs', 
#                                             datetime.utcnow()-timedelta(hours = 1), 
#                                             datetime.utcnow(), {}, 'Grid')
    
    
#############################################################################


  def getJobsSimpleEff(self, granularity, name):
    """  
    Return simple jobs efficiency
    
    :params:
      :attr:`granularity`: string - should be a ValidRes
    
      :attr:`name`: string - should be the name of the ValidRes
    
    :return:
      {
        'JobsEff': 'Good'|'Fair'|'Poor'|'Idle'|'Bad'
      }
    """

    if granularity not in ('Site', 'Sites'):
      from DIRAC.ResourceStatusSystem.Client.ResourceStatusClient import ResourceStatusClient 
      rsc = ResourceStatusClient()
      name = rsc.getGeneralName(granularity, name, 'Site')
    
    RPC = RPCClient("WorkloadManagement/WMSAdministrator")
    res = RPC.getSiteSummaryWeb({'Site':name},[],0,500)
    if not res['OK']:
#      raise RSSException, where(self, self.getJobsSimpleEff) + " " + res['Message'] 
      exceptStr = where(self, self.getJobsSimpleEff)
      gLogger.exception(exceptStr,'', res['Message'])
      return {'JobsEff': None}
    
    try:
      eff = res['Value']['Records'][0][16]
    except IndexError:
      return {'JobsEff':'Idle'}
    
    return {'JobsEff':eff}
  
#############################################################################
