from DIRAC import S_OK

from DIRAC.ResourceStatusSystem.DB.mock.ResourceManagementDB import ResourceManagementDB
from DIRAC.ResourceStatusSystem.Service.mock.RequestHandler  import RequestHandler

from DIRAC.ResourceStatusSystem.Utilities.Decorators         import HandlerDec

db = ResourceManagementDB()

class ResourceManagementHandler( RequestHandler ):
  
  def __init__( self ):
    pass
  
  @HandlerDec
  def insert( self, args, kwargs ):
    return db
  
  @HandlerDec
  def update( self, args, kwargs ):
    return db
  
  @HandlerDec
  def get( self, args, kwargs ):
    return db
  
  @HandlerDec
  def delete( self, args, kwargs ):
    return db