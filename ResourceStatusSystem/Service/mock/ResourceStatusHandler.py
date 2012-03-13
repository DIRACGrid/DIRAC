from DIRAC import S_OK

from DIRAC.ResourceStatusSystem.Service.mock.RequestHandler import RequestHandler

class ResourceStatusHandler( RequestHandler ):
  
  def __init__( self ):
    pass
  
  def insert( self, args, kwargs ):
    return S_OK()
  
  def update( self, args, kwargs ):
    return S_OK()
  
  def get( self, args, kwargs ):
    return S_OK()
  
  def delete( self, args, kwargs ):
    return S_OK()