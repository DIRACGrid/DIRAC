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
    """  
    Return pilot stats
    
    :params:
      :attr:`granularity`: string - should be a ValidRes
    
      :attr:`name`: string - should be the name of the ValidRes
    
      :attr:`periods`: list - contains the periods to consider in the query

    :return:
      {
        'MeanProcessedPilots': X'

        'LastProcessedPilots': X'
      }
    """
    
    if granularity.capitalize() not in ValidRes:
      raise InvalidRes, where(self, self.getPilotStats)
    
    if granularity == 'Site':
      entity = getSiteRealName(name)
      _granularity = 'Site'
    else:
      entity = name
      _granularity = 'GridCE'
    
    #######TODO
#    numberOfPilotsLash2Hours = self.rc.getReport('Pilot', 'NumberOfPilots', 
#                                                 datetime.utcnow()-timedelta(hours = 2), 
#                                                 datetime.utcnow(), {granularity:[entity]}, 
#                                                 'GridStatus')
#    numberOfPilotsLash2Hours = self.rc.getReport('Pilot', 'NumberOfPilots', 
#                                                 datetime.utcnow()-timedelta(hours = 2), 
#                                                 datetime.utcnow(), {granularity:[entity]}, 
#                                                 'GridStatus')
#    
#    for x in numberOfPilots['Value']['data'].itervalues():
#      total = 0
#      for y in x.values():
#        total = total+y
#      
#    print r


#############################################################################

  def getPilotsEff(self, granularity, name, periods):
    """  
    Return pilot stats of entity in args for periods
    
    :params:
      :attr:`granularity`: string - should be a ValidRes
    
      :attr:`name`: string - should be the name of the ValidRes
    
      :attr:`name`: list - periods contains the periods to consider in the query

    returns:
      {
        'PilotsEff': X (0-1)'
      }
    """

    if granularity == 'Site':
      entity = getSiteRealName(name)
      _granularity = 'Site'
    else:
      entity = name
      granularity = 'GridCE'
    
    #######TODO
#    numberOfPilots = self.rc.getReport('Pilot', 'NumberOfPilots', 
#                                       datetime.utcnow()-timedelta(hours = 24), 
#                                       datetime.utcnow(), {self._granularity:[self_entity]}, 
#                                       'GridStatus')
    
    
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


  def getPilotsSimpleEff(self, granularity, name, siteName = None):
    """  
    Return pilots simple efficiency of entity in args for periods
    
    :params:
      :attr:`granularity`: string - should be a ValidRes (Site or Resource)
      
      :attr:`name`: string - should be the name of the ValidRes
      
      :attr:`siteName`: string - optional site name, in case 
      granularity is `Resource`
    
    :return:
    {
      'PilotsEff': 'Good'|'Fair'|'Poor'|'Idle'|'Bad'
    }
    """
    
#    try:
    
    RPC = RPCClient("WorkloadManagement/WMSAdministrator")
    if granularity in ('Site', 'Sites'):
      res = RPC.getPilotSummaryWeb({'GridSite':name},[],0,1)
    elif granularity in ('Resource', 'Resources'):
      if siteName is None:
        from DIRAC.ResourceStatusSystem.Client.ResourceStatusClient import ResourceStatusClient
        rsc = ResourceStatusClient()
        siteName = rsc.getGeneralName(granularity, name, 'Site')
        if siteName is None or siteName == []:
          gLogger.info('%s is not a resource in DIRAC' %name)
          return {'PilotsEff':None}
        
      res = RPC.getPilotSummaryWeb({'ExpandSite':siteName},[],0,100)
    else:
      raise InvalidRes, where(self, self.getPilotSimpleEff)
    
    if not res['OK']:
      raise RSSException, where(self, self.getPilotsSimpleEff) + " " + res['Message'] 
    
    try:
      if granularity in ('Site', 'Sites'):
        eff = res['Value']['Records'][0][14]
        return {'PilotsEff':eff}
      elif granularity in ('Resource', 'Resources'):
        for x in res['Value']['Records']:
          if x[1] == name:
            eff = x[14]
        try:
          eff
          return {'PilotsEff':eff}
        except NameError:
          return {'PilotsEff':None} 
        
    except IndexError:
      return {'PilotsEff':None}
    
#    except Exception, x:
#      exceptStr = where(self, self.getPilotsSimpleEff)
#      gLogger.exception(exceptStr,'', x)
#      return {'PilotsEff': None}
    
#############################################################################
