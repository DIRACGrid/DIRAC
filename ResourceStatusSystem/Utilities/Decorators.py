import types
import inspect

from DIRAC                                            import S_ERROR

################################################################################

class CheckExecution( object ):
  
  def __init__( self, f ):
    self.f = f
    
  def __get__( self, obj, objtype=None ):
    return types.MethodType( self, obj, objtype ) 
    
  def __call__( self, *args, **kwargs ):
#    try:
      return self.f( *args, **kwargs )   

################################################################################

class ClientExecutor( object ):
  
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
    except AttributeError:
      gateFunction = getattr( booster, fname )  
    except Exception, x:
      return S_ERROR( x )    

    return gateFunction( *args, **kwargs )
    