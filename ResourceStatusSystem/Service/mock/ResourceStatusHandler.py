from DIRAC import S_OK

from DIRAC.ResourceStatusSystem.DB.mock.ResourceStatusDB    import ResourceStatusDB
from DIRAC.ResourceStatusSystem.Service.mock.RequestHandler import RequestHandler

from DIRAC.ResourceStatusSystem.Utilities.Decorators        import HandlerDec2

db = ResourceStatusDB()

class ResourceStatusHandler( RequestHandler ):
  
  def __init__( self ):
    pass
  
  @HandlerDec2
  def insert( self, args, kwargs ):
    return db
  
  @HandlerDec2
  def update( self, args, kwargs ):
    return db
  
  @HandlerDec2
  def get( self, args, kwargs ):
    return db
  
  @HandlerDec2
  def delete( self, args, kwargs ):
    return db