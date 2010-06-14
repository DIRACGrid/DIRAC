""" PilotsClient class is a client for to get pilots stats.
"""

from datetime import datetime, timedelta

from DIRAC.Core.Utilities.SitesDIRACGOCDBmapping import getGOCSiteName
from DIRAC.Core.DISET.RPCClient import RPCClient

from DIRAC.ResourceStatusSystem.Utilities.Exceptions import *
from DIRAC.ResourceStatusSystem.Utilities.Utils import *

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
      entity = getGOCSiteName(name)
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


  def getPilotsSimpleEff(self, granularity, name, siteName = None, 
                         RPCWMSAdmin = None, timeout = None):
    """  
    Return pilots simple efficiency of entity in args for periods
    
    :params:
      :attr:`granularity`: string - should be a ValidRes (Site or Resource)
      
      :attr:`name`: string or list - names of the ValidRes
      
      :attr:`siteName`: string - optional site name, in case 
      granularity is `Resource`

      :attr:`RPCWMSAdmin`: RPCClient to RPCWMSAdmin
    
    :return:
    {
      'PilotsEff': 'Good'|'Fair'|'Poor'|'Idle'|'Bad'
    }
    """
    
    if RPCWMSAdmin is not None: 
      RPC = RPCWMSAdmin
    else:
      from DIRAC.Core.DISET.RPCClient import RPCClient
      RPC = RPCClient("WorkloadManagement/WMSAdministrator", timeout = timeout)

    if granularity in ('Site', 'Sites'):
      res = RPC.getPilotSummaryWeb({'GridSite':name},[],0,300)
    elif granularity in ('Resource', 'Resources'):
      if siteName is None:
        from DIRAC.ResourceStatusSystem.Client.ResourceStatusClient import ResourceStatusClient
        rsc = ResourceStatusClient()
        siteName = rsc.getGeneralName(granularity, name, 'Site')
        if siteName is None or siteName == []:
#          gLogger.info('%s is not a resource in DIRAC' %name)
          return None
        
      res = RPC.getPilotSummaryWeb({'ExpandSite':siteName},[],0,50)
    else:
      raise InvalidRes, where(self, self.getPilotSimpleEff)
    
    if not res['OK']:
      raise RSSException, where(self, self.getPilotsSimpleEff) + " " + res['Message'] 
    else:
      res = res['Value']['Records']
    
    if len(res) == 0:
      return None 

    effRes = {}

    try:
      if granularity in ('Site', 'Sites'):
        for r in res:
          name = r[0]
          try:
            eff = r[14]
          except IndexError:
            eff = 'Idle'
          effRes[name] = eff 
        
      elif granularity in ('Resource', 'Resources'):
        eff = None
        for r in res:
          if r[1] == name:
            try:
              eff = r[14]
            except IndexError:
              eff = 'Idle'
            break
        effRes[name] = eff 

      return effRes
        
    except IndexError:
      return None
    
#############################################################################
