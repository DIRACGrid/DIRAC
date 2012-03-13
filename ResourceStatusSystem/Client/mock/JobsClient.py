class JobsClient( object ):
  
  def __init__( self ):
    self.gate = PrivateJobsClient()
    
  def getJobsSimpleEff( self, name, RPCWMSAdmin = None ):
    return self.gate.getJobsSimpleEff( name, RPCWMSAdmin = RPCWMSAdmin )
    
class PrivateJobsClient( object ):
  
  def getJobsSimpleEff( self, name, RPCWMSAdmin = None ):
    return {}      