# $HeadURL $
''' PilotsClient

  Module to get pilots stats.

'''

__RCSID__  = '$Id: $'

class PilotsClient( object ):
  """ 
  PilotsClient class is a client to get pilots stats.
  """

  def __init__( self ):
    self.gate = PrivatePilotsClient()
    
  def getPilotsSimpleEff( self, granularity, name, siteName = None, 
                          RPCWMSAdmin = None  ):  
    """
    
    Return pilots simple efficiency of entity in args for periods
    
    :Parameters:
      **granularity** - `string`
        should be a ValidRes (Site or Resource)
      **name** - `string || list` 
        name(s) of the ValidRes 
      **siteName** - `[,string]`
        optional site name, in case granularity is `Resource` 
      **RPCWMSAdmin** - `[,RPCClient]`
        RPCClient to WMSAdmin
        
    :return: { PilotsEff : Good | Fair | Poor | Idle | Bad }
      
    """
    return self.gate.getPilotsSimpleEff( granularity, name, siteName = siteName, 
                                         RPCWMSAdmin = RPCWMSAdmin )

################################################################################

class PrivatePilotsClient( object ):

  def getPilotsSimpleEff( self, granularity, name, siteName = None, 
                          RPCWMSAdmin = None ):

    if RPCWMSAdmin is not None:
      RPC = RPCWMSAdmin
    else:
      from DIRAC.Core.DISET.RPCClient import RPCClient
      RPC = RPCClient( "WorkloadManagement/WMSAdministrator" )

    if granularity == 'Site':
      res = RPC.getPilotSummaryWeb( { 'GridSite' : name }, [], 0, 300 )
    elif granularity == 'Resource':
      if siteName is None:
        from DIRAC.ResourceStatusSystem.Client.ResourceStatusClient import ResourceStatusClient
        rsClient = ResourceStatusClient()
        siteName = rsClient.getGeneralName( granularity, name, 'Site' )
        if not siteName[ 'OK' ]:
          print res[ 'Message' ]
          return {}
  
        if siteName[ 'Value' ] is None or siteName[ 'Value' ] == []:
          return {}
        siteName = siteName['Value']

      res = RPC.getPilotSummaryWeb( { 'ExpandSite' : siteName }, [], 0, 50 )
    else:
      return {}

    if not res['OK']:
      print res[ 'Message' ]
      return {}
    
    res = res['Value']['Records']

    if len(res) == 0:
      return {}

    effRes = {}

    try:
      if granularity == 'Site':
        for r in res:
          name = r[0]
          try:
            eff = r[14]
          except IndexError:
            eff = 'Idle'
          effRes[name] = eff

      elif granularity == 'Resource':
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
      return {}
         
################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF