################################################################################
# $HeadURL $
################################################################################
__RCSID__  = "$Id$"

class JobsClient( object ):
  """ 
  JobResultsClient class is a client to get jobs' stats.
  """
  
  def __init__( self ):
    self.gate = PrivateJobsClient()

  def getJobsSimpleEff( self, name, RPCWMSAdmin = None ):
    """  
    Return simple jobs efficiency
    
    :Parameters:
      **name** - `string||list`
        site name(s)
    
      **RPCWMSAdmin** - `[,RPCClient]`
        RPCClient to RPCWMSAdmin

    :return: { SiteName : Good | Fair | Poor | Idle | Bad }
    """
    return self.gate.getJobsSimpleEff( name, RPCWMSAdmin = RPCWMSAdmin )

#  def getJobsStats(self, name, periods = None):
#    pass

#  def getJobsEff( self, granularity, name, periods ):
#    pass

#  def getSystemCharge(self):
#    pass

################################################################################

class PrivateJobsClient( object ):
  
  def getJobsSimpleEff( self, name, RPCWMSAdmin = None ):
  
    if RPCWMSAdmin is not None: 
      RPC = RPCWMSAdmin
    else:
      from DIRAC.Core.DISET.RPCClient import RPCClient
      RPC = RPCClient( "WorkloadManagement/WMSAdministrator" )

    res = RPC.getSiteSummaryWeb( { 'Site' : name }, [] , 0, 500 )
    if not res[ 'OK' ]:
      print res[ 'Message' ]
      return None
    else:
      res = res[ 'Value' ][ 'Records' ]
    
    effRes = {}
    
    if len( res ) == 0:
      return None 
    for r in res:
      name = r[ 0 ]
      try:
        eff = r[ 16 ]
      except IndexError:
        eff = 'Idle'
      effRes[ name ] = eff 
    
    return effRes
  
################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF