""" PilotsClient class is a client for to get pilots stats.
"""

from datetime import datetime, timedelta
from DIRAC import gLogger
from DIRAC.ResourceStatusSystem.Utilities.Exceptions import *
from DIRAC.ResourceStatusSystem.Utilities.Utils import *
#from DIRAC.AccountingSystem.Client.ReportsClient import ReportsClient
from DIRAC.Core.DISET.RPCClient import RPCClient

class PilotsClient:
  
#  def __init__(self):
#    self.rc = ReportsClient()

#############################################################################

  def getPilotStats(self, granularity, name, periods):
    """  return pilot stats of entity in args
        - granularity should be a ValidRes
        - name should be the name of the ValidRes
        - periods contains the periods to consider in the query

        returns:
          {
            'MeanProcessedPilots': X'
            'LastProcessedPilots': X'
          }
    """
    
    if args[0].capitalize() not in ValidRes:
      raise InvalidRes, where(self, self.getPilotStats)
    
    if args[0] == 'Site':
      entity = getSiteRealName(args[1])
      _granularity = 'Site'
    else:
      entity = args[1]
      granularity = 'GridCE'
    
    #######TODO
#    numberOfPilotsLash2Hours = self.rc.getReport('Pilot', 'NumberOfPilots', datetime.utcnow()-timedelta(hours = 2), datetime.utcnow(), {granularity:[entity]}, 'GridStatus')
#    numberOfPilotsLash2Hours = self.rc.getReport('Pilot', 'NumberOfPilots', datetime.utcnow()-timedelta(hours = 2), datetime.utcnow(), {granularity:[entity]}, 'GridStatus')
    
#    for x in numberOfPilots['Value']['data'].itervalues():
#      total = 0
#      for y in x.values():
#        total = total+y
#      
#    print r


#############################################################################

  def getPilotsEff(self, granularity, name, periods):
    """  return pilot stats of entity in args for periods
        - granularity should be a ValidRes
        - name should be the name of the ValidRes
        
        - periods contains the periods to consider in the query

        returns:
          {
            'PilotsEff': X (0-1)'
          }
    """

    if args[0] == 'Site':
      entity = getSiteRealName(args[1])
      _granularity = 'Site'
    else:
      entity = args[1]
      granularity = 'GridCE'
    
    #######TODO
#    numberOfPilots = self.rc.getReport('Pilot', 'NumberOfPilots', datetime.utcnow()-timedelta(hours = 24), datetime.utcnow(), {self._granularity:[self_entity]}, 'GridStatus')
    
    
#############################################################################

  def getPilotSimpleEff(self, granularity, name, periods):
    """  return pilot simple efficiency of entity in args for periods
        - granularity should be a ValidRes
        - name should be the name of the ValidRes
        
        returns:
          {
            'PilotsEff': X (0-1)'
          }
    """

    
#    RPC = RPCClient("WorkloadManagement/WMSAdministrator")

#############################################################################


  def getPilotsSimpleEff(self, granularity, name):
    """  return pilots simple efficiency of entity in args for periods
        - granularity should be a ValidRes
        - name should be the name of the ValidRes
            if granularity is resource, name is a tuple containing
            the resource name along with its site name
        
        returns:
          {
            'PilotsEff': 'Good'|'Fair'|'Poor'|'Idle'|'Bad'
          }
    """
    
    RPC = RPCClient("WorkloadManagement/WMSAdministrator")
    if granularity == 'Site':
      res = RPC.getPilotSummaryWeb({'GridSite':name},[],0,500)
    elif granularity == 'Resource':
      res = RPC.getPilotSummaryWeb({'ExpandSite':name[1]},[],0,500)
    if not res['OK']:
      raise RSSException, where(self, self.getPilotsSimpleEff) + " " + res['Message'] 
    
    try:
      if granularity == 'Site':
        eff = res['Value']['Records'][0][14]
        return {'PilotsEff':eff}
      elif granularity == 'Resource':
        for x in res['Value']['Records']:
          if x[1] == args[0]:
            eff = x[14]
        try:
          eff
          return {'PilotsEff':eff}
        except NameError:
          return {'PilotsEff':'Idle'} 
        
    except IndexError:
      return {'PilotsEff':'Idle'}
    
#############################################################################
