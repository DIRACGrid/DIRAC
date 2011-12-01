import inspect, types

from DIRAC import S_ERROR 


class HandlerDec( object ):
  
  def __init__( self, f ):
    self.f = f
    
  def __get__( self, obj, objtype = None ):
    return types.MethodType( self, obj, objtype )
    
  def __call__( self, *args, **kwargs ):#f ):  
      
      ins = inspect.getargspec( self.f )

      fname = self.f.__name__.replace( 'export_', '' )
      db    = self.f( *args )

      if ins.args[-1] == 'kwargs':       
        kwargs = list( args )[ -1 ]
        args   = tuple( list( args )[ :-1 ] )

      args  = tuple( list( args )[ 1:] ) 

      try:
        dbFunction = getattr( db, fname )
      except Exception, x:
        return S_ERROR( x )    

      try:
        resQuery = dbFunction( *args, **kwargs )
      except Exception, x:   
        return S_ERROR( x )
    
      return resQuery   
  