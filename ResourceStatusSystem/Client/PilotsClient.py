## $HeadURL $
#''' PilotsClient
#
#  Module to get pilots stats.
#
#'''
#
#from DIRAC                                                  import S_OK, S_ERROR
#from DIRAC.Core.DISET.RPCClient                             import RPCClient
#from DIRAC.ResourceStatusSystem.Client.ResourceStatusClient import ResourceStatusClient
#
#__RCSID__  = '$Id: $'
#
#class PilotsClient( object ):
#  """ 
#  PilotsClient class is a client to get pilots stats.
#  """
#
#  def __init__( self ):
#    self.gate = RPCClient( 'WorkloadManagement/WMSAdministrator' )
#    
#  def getPilotsSiteSimpleEff( self, siteName ):  
#    """
#    
#    Return pilots simple efficiency of entity in args for periods
#    
#    :Parameters:
#      **granularity** - `string`
#        should be a ValidElement (Site or Resource)
#      **name** - `string || list` 
#        name(s) of the ValidElement 
#      **siteName** - `[,string]`
#        optional site name, in case granularity is `Resource` 
#      **RPCWMSAdmin** - `[,RPCClient]`
#        RPCClient to WMSAdmin
#        
#    :return: { PilotsEff : Good | Fair | Poor | Idle | Bad }
#      
#    """
#
#    results = self.gate.getPilotSummaryWeb( { 'GridSite' : siteName }, [], 0, 300 )
#    if not results[ 'OK' ]:
#      return results
#    results = results[ 'Value' ]
#    
#    if not 'ParameterNames' in results:
#      return S_ERROR( 'Malformed result dictionary' )
#    params = results[ 'ParameterNames' ]
#    
#    if not 'Records' in results:
#      return S_ERROR( 'Malformed result dictionary' )
#    records = results[ 'Records' ]
#    
#    pilotResults = [] 
#       
#    for record in records:
#      
#      pilotResults.append( dict( zip( params , record )) )
#    
#    return S_OK( pilotResults )  
#      
#  def getPilotsSiteCEsSimpleEff( self, siteName ):
#    """
#    
#    Return pilots simple efficiency of entity in args for periods
#    
#    :Parameters:
#      **granularity** - `string`
#        should be a ValidElement (Site or Resource)
#      **name** - `string || list` 
#        name(s) of the ValidElement 
#      **siteName** - `[,string]`
#        optional site name, in case granularity is `Resource` 
#      **RPCWMSAdmin** - `[,RPCClient]`
#        RPCClient to WMSAdmin
#        
#    :return: { PilotsEff : Good | Fair | Poor | Idle | Bad }
#      
#    """
#
#    results = self.gate.getPilotSummaryWeb( { 'ExpandSite' : siteName }, [], 0, 300 )
#    if not results[ 'OK' ]:
#      return results
#    results = results[ 'Value' ]
#    
#    if not 'ParameterNames' in results:
#      return S_ERROR( 'Malformed result dictionary' )
#    params = results[ 'ParameterNames' ]
#    
#    if not 'Records' in results:
#      return S_ERROR( 'Malformed result dictionary' )
#    records = results[ 'Records' ]
#      
#    pilotResults = [] 
#       
#    for record in records:
#      
#      pilotResults.append( dict( zip( params , record )) )
#    
#    return S_OK( pilotResults )    
#             
#################################################################################
##EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF