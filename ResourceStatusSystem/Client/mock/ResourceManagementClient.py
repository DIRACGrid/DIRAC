from DIRAC.ResourceStatusSystem.DB.mock.ResourceManagementDB import ResourceManagementDB

class ResourceManagementClient( object ):
  
  def __init__( self, serviceIn = None ):
    
    if serviceIn is None:
      self.gate = ResourceManagementDB()
    else:
      self.gate = serviceIn  
    
  def insert( self, *args, **kwargs ):
    return self.gate.insert( args, kwargs )
  
  def update( self, *args, **kwargs ):
    return self.gate.update( args, kwargs )
  
  def get( self, *args, **kwargs ):
    return self.gate.get( args, kwargs )
    
  def delete( self, *args, **kwargs ):
    return self.gate.delete( args, kwargs )   