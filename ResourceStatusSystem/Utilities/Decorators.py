import types
import inspect

from DIRAC import S_ERROR, gLogger

class CheckExecution( object ):
  
  def __init__( self, f ):
    self.f = f
    
  def __get__( self, obj, objtype = None ):
    return types.MethodType( self, obj, objtype ) 
    
  def __call__( self, *args, **kwargs ):
      try:
        return self.f( *args, **kwargs )
      except Exception, x:
        return S_ERROR( x )

################################################################################

class CheckExecution2( object ):
  
  def __init__( self, f ):
    self.f = f
    
  def __get__( self, obj, objtype = None ):
    return types.MethodType( self, obj, objtype ) 
    
  def __call__( self, *args, **kwargs ):
      
      functionNames = [ 'addOrModify', 'set', 'get', 'update', 'delete' ]         
      tableName = self.f.__name__  
      for fN in functionNames:  
        tableName = tableName.replace( fN, '' ) 
      
      if self.f.startswith( 'addOrModify' ) or self.f.startswith( 'set' ):
        kwargs = {} 
      
      kwargs.update( { 'table' : tableName })
      
      try:
        
        self.f.kwargs = kwargs
        return self.f( *args, **kwargs )
      except Exception, x:
        return S_ERROR( x )

################################################################################

class HandlerExecution( object ):
  
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
        gLogger.exception( 'Unable to find function %s \n %s' % ( fname, x ) )
        return S_ERROR( x )    

      gLogger.info( 'Attempting to %s' % fname )
      if args:
        gLogger.info( '%s.args: %s' % ( fname, args ) )
      if kwargs:  
        gLogger.info( '%s.kwargs: %s' % ( fname, kwargs ) )
        
      try:
        resQuery = dbFunction( *args, **kwargs )
        gLogger.info( 'Done %s' % fname )
      except Exception, x:
        gLogger.exception( 'Something went wrong executing %s \n %s' % ( fname, x ) )    
        return S_ERROR( x )
    
      return resQuery   

################################################################################

class ClientExecution( object ):
  
  def __init__( self, f ):
    self.f = f
    
  def __get__( self, obj, objtype = None ):
    return types.MethodType( self, obj, objtype )
    
  def __call__( self, *args, **kwargs ):  
 
    ins = inspect.getargspec( self.f )
    newArgs = ( 1 and ins.defaults ) or ()

    if newArgs:
      #Keyword arguments on function self.f
      funkwargs = ins.args[ -len( ins.defaults ): ]
      
      kw      = dict(zip( funkwargs, ins.defaults ) )
      kw.update( kwargs ) 
      newArgs = [ kw[k] for k in funkwargs ]
      
      for fk in funkwargs:
        kwargs.pop( fk, None )  
      
    gate    = args[ 0 ].gate  
    booster = args[ 0 ].booster
     
    fname   = self.f.__name__  
    args    = tuple( list(args)[1:] + list( newArgs ))    
       
    try:
      gateFunction = getattr( gate, fname )
    except AttributeError, x:
      gateFunction = getattr( booster, fname )  
    except Exception, x:
      return S_ERROR( x )    

    return gateFunction( *args, **kwargs )

################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF    