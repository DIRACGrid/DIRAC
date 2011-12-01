class PilotsClient( object ):
  
  def __init__( self ):
    self.gate = PrivatePilotsClient()
    
  def getPilotsSimpleEff( self, granularity, name, siteName = None, 
                          RPCWMSAdmin = None  ):  
    return self.gate.getPilotsSimpleEff( granularity, name, siteName = None, 
                                         RPCWMSAdmin = None  )
    
class PrivatePilotsClient( object ):
  
  def getPilotsSimpleEff( self, granularity, name, siteName = None, 
                          RPCWMSAdmin = None  ):
    return {}      