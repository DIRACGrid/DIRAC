################################################################################
# $HeadURL $
################################################################################
__RCSID__  = "$Id$"

import types
import inspect

from DIRAC import S_ERROR, gLogger

class CheckExecution2( object ):
  
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

class DBDec( object ):
  
  def __init__( self, f ):
    self.f = f
    
  def __get__( self, obj, objtype = None ):
    return types.MethodType( self, obj, objtype ) 
    
  def __call__( self, *args, **kwargs ):
      
    functionNames = [ 'insert', 'update', 'get', 'delete' ]         
    tableName = self.f.__name__  
    for fN in functionNames:  
      tableName = tableName.replace( fN, '' ) 
      
    if self.f.__name__.startswith( 'insert' ) or self.f.__name__.startswith( 'update' ):
      kwargs = {} 
      
    kwargs.update( { 'table' : tableName })
      
    try:
      self.f.kwargs = kwargs
      return self.f( *args, **kwargs )
    except Exception, x:
      return S_ERROR( x )

################################################################################

class HandlerDec2( object ):
  
  def __init__( self, f ):
    self.f = f
    
  def __get__( self, obj, objtype = None ):
    return types.MethodType( self, obj, objtype )
    
  def __call__( self, *args, **kwargs ):  
      
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
    #if args:
    gLogger.info( '%s.args: %s' % ( fname, args ) )
    #if kwargs:  
    gLogger.info( '%s.kwargs: %s' % ( fname, kwargs ) )
        
    try:
      resQuery = dbFunction( *args, **kwargs )
      gLogger.info( 'Done %s' % fname )
    except Exception, x:
      gLogger.exception( 'Something went wrong executing %s \n %s' % ( fname, x ) )    
      return S_ERROR( x )
    
    return resQuery   

################################################################################

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

class ClientDec( object ):
  
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

class ClientDec2( object ):
  
  def __init__( self, f ):
    self.f = f
    
  def __get__( self, obj, objtype = None ):
    return types.MethodType( self, obj, objtype )
    
  def __call__( self, *args, **kwargs ):  
 
    fname   = self.f.__name__  
    gate    = args[ 0 ].gate  
    booster = args[ 0 ].booster
    
    try:
      gateFunction = getattr( gate, fname )
    except AttributeError, x:
      gateFunction = getattr( booster, fname )  
    except Exception, x:
      return S_ERROR( x )   
 
 
    ins = inspect.getargspec( self.f )
    
    defKwargs = ( 1 and ins.defaults ) or () 
    
    kwargsLen = len( defKwargs )
    argsLen   = len( ins.args ) - kwargsLen 
    if ins.varargs is not None:
      argsLen = len( ins.args )
    
    #processArgs
    newArgs = tuple( list( args )[ 1:argsLen ] )  
    if len( args ) > len( ins.args ):
      raise TypeError( '%s arguments received' % len(args) )
    
    #processKwargs
    newKwargs = []
    if kwargsLen: #newArgs:
      #Keyword arguments on function self.f
      funkwargs = ins.args[ -kwargsLen: ]
      kw      = dict(zip( funkwargs, defKwargs ) )
      
      kw.update( kwargs ) 
      
      if list( args )[ argsLen: ]:
        for _i in xrange( argsLen, len(args) ):
          
          funkey = funkwargs[ _i - argsLen ]
          
          if kw[ funkey ] is not None:
            raise TypeError( '%s Got %s twice %s,%s' % ( fname, funkey, kw[ funkey ], args[_i]))
          kw[ funkey ] = args[ _i ]
      
      newKwargs = [ kw[k] for k in funkwargs ]
      
      for fk in funkwargs:
        kwargs.pop( fk, None )    

    args    = tuple( list(newArgs) + newKwargs )         

    return gateFunction( *args, **kwargs )
  
################################################################################

class ClientDec3( object ):
  
  def __init__( self, f ):
    self.f = f
    
  def __get__( self, obj, objtype = None ):
    return types.MethodType( self, obj, objtype )
    
  def __call__( self, *args, **kwargs ):  

    INTERNAL_FUNCTIONS = [ 'insert', 'update', 'get', 'delete' ]
 
    fname   = self.f.__name__  
    gate    = args[ 0 ].gate  
    booster = args[ 0 ].booster
    
    try:
      
      _fname, _table = fname,''
      
      for _if in INTERNAL_FUNCTIONS:
        if _if in fname:
          _fname = _if
          _table = fname.replace( _if, '' )

      if _fname != fname:
        kwargs[ 'table' ] = _table
                   
      gateFunction      = getattr( gate, _fname )

    except AttributeError, x:
      gateFunction = getattr( booster, fname )  
    except Exception, x:
      return S_ERROR( x )   
 
    ins = inspect.getargspec( self.f )
    
    defKwargs = ( 1 and ins.defaults ) or () 
    
    kwargsLen = len( defKwargs )
    argsLen   = len( ins.args ) - kwargsLen 
    if ins.varargs is not None:
      argsLen = len( ins.args )
    
    #processArgs
    newArgs = tuple( list( args )[ 1:argsLen ] )   
    if len( args ) > len( ins.args ):
      raise TypeError( '%s arguments received' % len(args) )
    
    #processKwargs
    newKwargs = []
    if kwargsLen: #newArgs:
      #Keyword arguments on function self.f
      funkwargs = ins.args[ -kwargsLen: ]
      kw      = dict(zip( funkwargs, defKwargs ) )
      
      kw.update( kwargs ) 
      
      if list( args )[ argsLen: ]:
        for _i in xrange( argsLen, len(args) ):
          
          funkey = funkwargs[ _i - argsLen ]
          
          if kw[ funkey ] is not None:
            raise TypeError( '%s Got %s twice %s,%s' % ( fname, funkey, kw[ funkey ], args[_i]))
          kw[ funkey ] = args[ _i ]
      
      newKwargs = [ kw[k] for k in funkwargs ]
      
      for fk in funkwargs:
        kwargs.pop( fk, None )    

    args    = tuple( list(newArgs) + newKwargs )         
     
    if kwargs.has_key( 'table' ):
      return gateFunction( args, kwargs )
    
    print args
    print kwargs
    
    return gateFunction( *args, **kwargs )
    
################################################################################

class ClientDec4( object ):
  
  def __init__( self, f ):
    self.f = f
    
  def __get__( self, obj, objtype = None ):
    return types.MethodType( self, obj, objtype )
    
  def __call__( self, *args, **kwargs ):  

    fname   = self.f.__name__   
    booster = args[ 0 ].booster
    
    try:
      gateFunction = getattr( booster, '_%s' % fname )  
    except Exception, x:
      return S_ERROR( x )   
 
    ins = inspect.getargspec( self.f )
    
    defKwargs = ( 1 and ins.defaults ) or () 
    
    kwargsLen = len( defKwargs )
    argsLen   = len( ins.args ) - kwargsLen 
    if ins.varargs is not None:
      argsLen = len( ins.args )
    
    #processArgs
    newArgs = tuple( list( args )[ 1:argsLen ] )   
    if len( args ) > len( ins.args ):
      raise TypeError( '%s arguments received' % len(args) )
    
    #processKwargs
    newKwargs = []
    if kwargsLen: #newArgs:
      #Keyword arguments on function self.f
      funkwargs = ins.args[ -kwargsLen: ]
      kw      = dict(zip( funkwargs, defKwargs ) )
      
      kw.update( kwargs ) 
      
      if list( args )[ argsLen: ]:
        for _i in xrange( argsLen, len(args) ):
          
          funkey = funkwargs[ _i - argsLen ]
          
          if kw[ funkey ] is not None:
            raise TypeError( '%s Got %s twice %s,%s' % ( fname, funkey, kw[ funkey ], args[_i]))
          kw[ funkey ] = args[ _i ]
      
      newKwargs = [ kw[k] for k in funkwargs ]
      
      for fk in funkwargs:
        kwargs.pop( fk, None )    

    args    = tuple( list(newArgs) + newKwargs )         
     
#    if kwargs.has_key( 'table' ):
    return gateFunction( args, kwargs )
    
#    return gateFunction( *args, **kwargs )

################################################################################

class ClientDec5( object ):
  
  def __init__( self, f ):
    self.f = f
    
  def __get__( self, obj, objtype = None ):
    return types.MethodType( self, obj, objtype )
    
  def __call__( self, *args, **kwargs ):  

    INTERNAL_FUNCTIONS = [ 'insert', 'update', 'get', 'delete' ]

    fname   = self.f.__name__   
    client = args[ 0 ].client     
 
    ins = inspect.getargspec( self.f )
    
    defKwargs = ( 1 and ins.defaults ) or () 
    
    kwargsLen = len( defKwargs )
    argsLen   = len( ins.args ) - kwargsLen 
    if ins.varargs is not None:
      argsLen = len( ins.args )
    
    #processArgs
    newArgs = tuple( list( args )[ 1:argsLen ] )   
    if len( args ) > len( ins.args ):
      raise TypeError( '%s arguments received' % len(args) )
    
    #processKwargs
    newKwargs = []
    if kwargsLen: #newArgs:
      #Keyword arguments on function self.f
      funkwargs = ins.args[ -kwargsLen: ]
      kw      = dict(zip( funkwargs, defKwargs ) )
      
      kw.update( kwargs ) 
      
      if list( args )[ argsLen: ]:
        for _i in xrange( argsLen, len(args) ):
          
          funkey = funkwargs[ _i - argsLen ]
          
          if kw[ funkey ] is not None:
            raise TypeError( '%s Got %s twice %s,%s' % ( fname, funkey, kw[ funkey ], args[_i]))
          kw[ funkey ] = args[ _i ]
      
      newKwargs = [ kw[k] for k in funkwargs ]
      
      for fk in funkwargs:
        kwargs.pop( fk, None )    

    args    = tuple( list(newArgs) + newKwargs )         
    
    try:
      
      _fname, _table = fname,''
      
      for _if in INTERNAL_FUNCTIONS:
        if fname.startswith( _if ):
          _fname = _if
          _table = fname.replace( _if, '' )
          if _table.startswith( 'Element' ):
            _element = args[ 0 ]
            _table   = _table.replace( 'Element', _element )
            args = args[ 1: ]

      kwargs[ 'table' ] = _table
                   
      gateFunction      = getattr( client, _fname )

    except Exception, x:
      return S_ERROR( x )  
     
    return gateFunction( *args, **kwargs )

################################################################################

class APIDecorator( object ):
  
  def __init__( self, f ):
    self.f = f
    
  def __get__( self, obj, objtype = None ):
    return types.MethodType( self, obj, objtype )
    
  def __call__( self, *args, **kwargs ):  

    eBaseAPI = args[ 0 ].eBaseAPI
    try:
      eBaseAPIFunction      = getattr( eBaseAPI, self.f.__name__ )
    except Exception, x:
      return S_ERROR( x )
    
    args = args[1:]
     
    return eBaseAPIFunction( *args, **kwargs )          
          
################################################################################
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #  
################################################################################

'''
  HOW DOES THIS WORK.
    
    will come soon...
'''
            
################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF    