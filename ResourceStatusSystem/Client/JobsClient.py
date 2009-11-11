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
    """  return jobs stats of entity in args
        - granularity should be a ValidRes
        - name should be the name of the ValidRes
        - periods contains the periods to consider in the query

        returns:
          {
            'MeanProcessedJobs': X'
            'LastProcessedJobs': X'
          }
    """
    
    if args[0].capitalize() not in ValidRes:
      raise InvalidRes, where(self, self.getJobStats)
    
    if args[0] == 'Site':
      entity = getSiteRealName(args[1])
      _granularity = 'Site'
    else:
      entity = args[1]
      granularity = 'GridCE'
    
    #######TODO
#    numberOfJobsLash2Hours = self.rc.getReport('Job', 'NumberOfJobs', datetime.utcnow()-timedelta(hours = 2), datetime.utcnow(), {granularity:[entity]}, 'GridStatus')
#    numberOfJobsLash2Hours = self.rc.getReport('Job', 'NumberOfJobs', datetime.utcnow()-timedelta(hours = 2), datetime.utcnow(), {granularity:[entity]}, 'GridStatus')
#    
#    for x in numberOfJobs['Value']['data'].itervalues():
#      total = 0
#      for y in x.values():
#        total = total+y
#      
#    print r


#############################################################################

  def getJobsEff(self, granularity, name, periods):
    """  return job stats of entity in args for periods
        - granularity should be a ValidRes
        - name should be the name of the ValidRes
        
        - periods contains the periods to consider in the query

        returns:
          {
            'JobsEff': X (0-1)'
          }
    """

    if args[0] == 'Site':
      entity = getSiteRealName(args[1])
      _granularity = 'Site'
    else:
      entity = args[1]
      granularity = 'GridCE'
    
    #######TODO
#    numberOfJobs = self.rc.getReport('Job', 'NumberOfJobs', datetime.utcnow()-timedelta(hours = 24), datetime.utcnow(), {self._granularity:[self_entity]}, 'GridStatus')
    

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

#    numberOfJobsLastHour = self.rc.getReport('Job', 'TotalNumberOfJobs', datetime.utcnow()-timedelta(hours = 1), datetime.utcnow(), {}, 'Grid')
    
    
#############################################################################


  def getJobsSimpleEff(self, granularity, name):
    """  return jobs simple efficiency of entity in args for periods
        - granularity should be a ValidRes
        - name should be the name of the ValidRes
        
        returns:
          {
            'JobsEff': 'Good'|'Fair'|'Poor'|'Idle'|'Bad'
          }
    """

    
    RPC = RPCClient("WorkloadManagement/WMSAdministrator")
    res = RPC.getSiteSummaryWeb({'Site':name},[],0,500)
    if not res['OK']:
      raise RSSException, where(self, self.getJobsSimpleEff) + " " + res['Message'] 
    
    try:
      eff = res['Value']['Records'][0][16]
    except IndexError:
      return {'JobsEff':'Idle'}
    
    return {'JobsEff':eff}
  
#############################################################################
