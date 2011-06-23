""" PilotsClient class is a client for to get pilots stats.
"""
from DIRAC.Core.Utilities.SitesDIRACGOCDBmapping import getGOCSiteName

from DIRAC.ResourceStatusSystem.Utilities.Exceptions import InvalidRes, RSSException
from DIRAC.ResourceStatusSystem.Utilities.Utils import where
from DIRAC.ResourceStatusSystem.Policy.Configurations import ValidRes

class PilotsClient:

#############################################################################

  def getPilotStats(self, granularity, name, periods):
    """
    Return pilot stats

    :Parameters:
      `granularity`
        string - should be a ValidRes

      `name`
        string - should be the name of the ValidRes

      `periods`
        list - contains the periods to consider in the query

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
      if not entity['OK']:
        raise RSSException, entity['Message']
      entity = entity['Value']
      _granularity = 'Site'
    else:
      entity = name
      _granularity = 'GridCE'

    #######TODO
#    numberOfPilotsLash2Hours = self.rc.getReport('Pilot', 'NumberOfPilots',
#                                                 datetime.datetime.utcnow()-datetime.timedelta(hours = 2),
#                                                 datetime.datetime.utcnow(), {granularity:[entity]},
#                                                 'GridStatus')
#    numberOfPilotsLash2Hours = self.rc.getReport('Pilot', 'NumberOfPilots',
#                                                 datetime.datetime.utcnow()-datetime.timedelta(hours = 2),
#                                                 datetime.datetime.utcnow(), {granularity:[entity]},
#                                                 'GridStatus')
#
#    for x in numberOfPilots['Value']['data'].itervalues():
#      total = 0
#      for y in x.values():
#        total = total+y
#
#    print r


#############################################################################
#
#  def getPilotsEff(self, granularity, name, periods):
#    """
#    Return pilot stats of entity in args for periods
#
#    :Parameters:
#      `granularity`
#        string - should be a ValidRes
#
#      `name`
#        string - should be the name of the ValidRes
#
#      `name`
#        list - periods contains the periods to consider in the query
#
#    :return:
#      {
#        'PilotsEff': X (0-1)'
#      }
#    """
#
#    if granularity == 'Site':
#      entity = getSiteRealName(name)
#      _granularity = 'Site'
#    else:
##      entity = name
#      granularity = 'GridCE'
#
    #######TODO
#    numberOfPilots = self.rc.getReport('Pilot', 'NumberOfPilots',
#                                       datetime.datetime.utcnow()-datetime.timedelta(hours = 24),
#                                       datetime.datetime.utcnow(), {self._granularity:[self_entity]},
#                                       'GridStatus')


#############################################################################


  def getPilotsSimpleEff(self, granularity, name, siteName = None,
                         RPCWMSAdmin = None, timeout = None):
    """
    Return pilots simple efficiency of entity in args for periods

    :Parameters:
      `granularity`
        string - should be a ValidRes (Site or Resource)

      `name`
        string or list - names of the ValidRes

      `siteName`
        string - optional site name, in case granularity is `Resource`

      `RPCWMSAdmin`
        RPCClient to RPCWMSAdmin

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
        if not siteName[ 'OK' ]:
          raise RSSException, where( self, self.getPilotsSimpleEff ) + " " + res[ 'Message' ]
        if siteName[ 'Value' ] is None or siteName[ 'Value' ] == []:
          return None
        siteName = siteName['Value']

      res = RPC.getPilotSummaryWeb({'ExpandSite':siteName},[],0,50)
    else:
      raise InvalidRes, where(self, self.getPilotsSimpleEff)

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
