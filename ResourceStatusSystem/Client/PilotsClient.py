# $HeadURL $
''' PilotsClient

  Module to get pilots stats.

'''

from DIRAC                                                  import S_OK, S_ERROR
from DIRAC.Core.DISET.RPCClient                             import RPCClient
from DIRAC.ResourceStatusSystem.Client.ResourceStatusClient import ResourceStatusClient

__RCSID__  = '$Id: $'

class PilotsClient( object ):
  """ 
  PilotsClient class is a client to get pilots stats.
  """

  def __init__( self ):
    self.gate = RPCClient( 'WorkloadManagement/WMSAdministrator' )
    
  def getPilotsSimpleEff( self, element, name, siteName = None, RPCWMSAdmin = None ):  
    """
    
    Return pilots simple efficiency of entity in args for periods
    
    :Parameters:
      **granularity** - `string`
        should be a ValidElement (Site or Resource)
      **name** - `string || list` 
        name(s) of the ValidElement 
      **siteName** - `[,string]`
        optional site name, in case granularity is `Resource` 
      **RPCWMSAdmin** - `[,RPCClient]`
        RPCClient to WMSAdmin
        
    :return: { PilotsEff : Good | Fair | Poor | Idle | Bad }
      
    """

    if element == 'Site':
      results = self.gate.getPilotSummaryWeb( { 'GridSite' : name }, [], 0, 300 )
      
    elif element == 'Resource':
      
      #FIXME: implement it !
      return S_ERROR( 'Not implemented yet' )
 
      
#      if siteName is None:
#        
#        rsClient = ResourceStatusClient()
#        siteName = rsClient.getGeneralName( granularity, name, 'Site' )
#        if not siteName[ 'OK' ]:
#          print res[ 'Message' ]
#          return {}
#  
#        if siteName[ 'Value' ] is None or siteName[ 'Value' ] == []:
#          return {}
#        siteName = siteName['Value']
#
#      res = self.gate.getPilotSummaryWeb( { 'ExpandSite' : siteName }, [], 0, 50 )
    
    else:
      return S_ERROR( 'Not accepted element %s' % element )

    if not results[ 'OK' ]:
      return results
    results = results[ 'Value' ][ 'Records' ]

    if len( results ) == 0:
      return S_ERROR( 'No records found' )

# FIXME: modified logic !! Check if it works.
#
#    try:
#      if granularity == 'Site':
#        for r in res:
#          name = r[0]
#          try:
#            eff = r[14]
#          except IndexError:
#            eff = 'Idle'
#          effRes[name] = eff
#
#      elif granularity == 'Resource':
#        eff = None
#        for r in res:
#          if r[1] == name:
#            try:
#              eff = r[14]
#            except IndexError:
#              eff = 'Idle'
#            break
#        effRes[name] = eff
#
#      return effRes
#
#    except IndexError:
#      return {}
#         
    effRes = {}

    for result in results:
      
      if element == 'Site':
        name = result[ 0 ]
      elif result[ 1 ] != name:
        continue        
          
      try:
        eff = result[ 14 ]
      except IndexError:
        eff = 'Idle'
      effRes[ name ] = eff

    return S_OK( effRes )
             
################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF