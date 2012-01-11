################################################################################
# $HeadURL $
################################################################################
__RCSID__  = "$Id$"

import inspect, types

from DIRAC import S_ERROR, gLogger

class BaseDec( object ):

  def __init__( self, f ):
    self.f = f
    
  def __get__( self, obj, objtype = None ):
    return types.MethodType( self, obj, objtype ) 

  def __call__( self, *args, **kwargs ):
    # Do it yourself !
    pass

class CheckDBExecution( BaseDec ):
  
  def __call__( self, *args, **kwargs ):
    
    try:
      return self.f( *args, **kwargs )
    except Exception, x:
      return S_ERROR( x )

class ValidateDBTypes( BaseDec ):
  
  def __call__( self, *args, **kwargs ):  
  
    if not isinstance( args[1], dict ):
      return S_ERROR( 'args MUST be a dict, not %s' % type( args[1] ))
    if not isinstance( args[2], dict ):
      return S_ERROR( 'kwargs MUST be a dict, not %s' % type( args[2] ))
    return self.f( *args, **kwargs )

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

    args  = tuple( list( args )[ 1:] )[ 0 ] 

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
      resQuery = dbFunction( args, kwargs )
      gLogger.info( 'Done %s' % fname )
    except Exception, x:
      gLogger.exception( 'Something went wrong executing %s \n %s' % ( fname, x ) )    
      return S_ERROR( x )
    
    return resQuery   

################################################################################

class HandlerDec3( object ):
  
  def __init__( self, f, *args, **kwargs ):
    self.f           = f
    self.db          = None
    self.credentials = None
    
  def processArgs( self, *args ):
    
    _db, _credentials = self.f( *args )
    
    self.db          = _db
    self.credentials = _credentials  
    
  def __get__( self, obj, objtype = None ):
    return types.MethodType( self, obj, objtype )
    
  def __call__( self, *args, **kwargs ):  
      
    fname = self.f.__name__.replace( 'export_', '' )    
    if self.db is None:
      self.processArgs( *args )
    
    ins = inspect.getargspec( self.f )    
    if ins.args[-1] == 'meta':       
      kwargs = list( args )[ -1 ]
      args   = tuple( list( args )[ :-1 ] )

    args = tuple( list( args )[ 1:] )[ 0 ] 

    try:
      dbFunction = getattr( self.db, fname )
    except Exception, x:
      gLogger.exception( 'Unable to find function %s \n %s' % ( fname, x ) )
      return S_ERROR( x )    

    gLogger.info( 'Attempting to %s' % fname )

    gLogger.info( '%s.args: %s'   % ( fname, args )   )  
    gLogger.info( '%s.kwargs: %s' % ( fname, kwargs ) )
        
    try:
      resQuery = dbFunction( args, kwargs )
      gLogger.info( 'Done %s' % fname )
    except Exception, x:
      gLogger.exception( 'Something went wrong executing %s \n %s' % ( fname, x ) )    
      return S_ERROR( x )
    
    return resQuery   

################################################################################

class AdminRequired( BaseDec ):
  
  def __call__( self, *args, **kwargs ):
    
    self.f.processArgs( *args, **kwargs )
    
    credentials = self.f.credentials   
    credGroup   = credentials.get( 'group', '' )
    
    if not credGroup == 'diracAdmin':
      return S_ERROR( 'Not enough permissions to execute this action.' ) 
    
    return self.f( *args, **kwargs )

################################################################################

class HandlerDec( object ):
  
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

    else:     
      if not len( args ) == len ( ins.args ):
        raise TypeError( '%s arguments received' % len( args ))

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
            kwargs.pop( 'element', None ) 

      kwargs[ 'table' ] = _table
                   
      gateFunction      = getattr( client, _fname )

    except Exception, x:
      return S_ERROR( x )  
     
    return gateFunction( *args, **kwargs )

################################################################################

class ClientDec6( BaseDec ):
  
  def __call__( self, *args, **kwargs ):

    INTERNAL_FUNCTIONS = [ 'insert', 'update', 'get', 'delete' ]

    fname   = self.f.__name__   
    client = args[ 0 ].gate     
 
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

    else:     
      if not len( args ) == len ( ins.args ):
        raise TypeError( '%s arguments received' % len( args ))

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
            kwargs.pop( 'element', None ) 

      kwargs[ 'table' ] = _table
                   
      gateFunction      = getattr( client, _fname )

    except Exception, x:
      return S_ERROR( x )  
     
    return gateFunction( args, kwargs )

################################################################################

class ClientDec7( BaseDec ):
  
  def __call__( self, *args, **kwargs ):

    private = args[ 0 ].private
    gate    = args[ 0 ].gate
     
    try:
      eBaseAPIFunction      = getattr( eBaseAPI, self.f.__name__ )
      args = args[1:]
      return eBaseAPIFunction( *args, **kwargs )
    except Exception, x:
      return S_ERROR( x ) 

################################################################################

class ClientFastDec( BaseDec ):
  
  def __call__( self, *args, **kwargs ):

    INTERNAL_FUNCTIONS = [ 'insert', 'update', 'get', 'delete' ]

    fname   = self.f.__name__   
    client = args[ 0 ].gate
    try:
      fKwargs1 = self.f( *args,**kwargs )
    except Exception, x:
      return S_ERROR( x )  
    del fKwargs1[ 'self' ]
    
    import copy
    # Avoids messing pointers while doing the pop of meta
    fKwargs = copy.deepcopy( fKwargs1 )
    meta    = fKwargs.pop( 'meta', {} )
    
    try:    

      _fname, _table = fname,''   
      
      for _if in INTERNAL_FUNCTIONS:
        if fname.startswith( _if ):
          _fname = _if
          _table = fname.replace( _if, '' )
      
          if _table.startswith( 'Element' ):
            _element = fKwargs.pop( 'element')
            _table   = _table.replace( 'Element', _element )
            fKwargs[ '%sName' % _element ] = fKwargs[ 'elementName' ]
            del fKwargs[ 'elementName' ]
          meta[ 'table' ] = _table
          
      gateFunction      = getattr( client, _fname )

    except Exception, x:
      return S_ERROR( x )  

#    try:
    return gateFunction( fKwargs, meta )
#    except Exception, x:
#      return S_ERROR( x )
     
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
      args = args[1:]
      return eBaseAPIFunction( *args, **kwargs )
    except Exception, x:
      return S_ERROR( x )        
          
################################################################################
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #  
################################################################################

'''
  HOW DOES THIS WORK.
    
    will come soon...
'''
            
################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF    