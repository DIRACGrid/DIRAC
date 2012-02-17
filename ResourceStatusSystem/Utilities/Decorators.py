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

################################################################################

class CheckDBExecution( BaseDec ):
  
  def __call__( self, *args, **kwargs ):
    
    try:
      return self.f( *args, **kwargs )
    except Exception, x:
      return S_ERROR( x )

################################################################################

class ValidateDBTypes( BaseDec ):
  
  def __call__( self, *args, **kwargs ):  
  
    if not isinstance( args[1], dict ):
      return S_ERROR( 'args MUST be a dict, not %s' % type( args[1] ))
    if not isinstance( args[2], dict ):
      return S_ERROR( 'kwargs MUST be a dict, not %s' % type( args[2] ))
    return self.f( *args, **kwargs )

################################################################################

class HandlerDec( BaseDec ):
    
  def __call__( self, *args, **kwargs ):  
      
    fname = self.f.__name__.replace( 'export_', '' ) 
    db    = self.f( *args )   
    
    ins = inspect.getargspec( self.f )    
    if ins.args[-1] == 'meta':       
      kwargs = list( args )[ -1 ]
      args   = tuple( list( args )[ :-1 ] )

    args = tuple( list( args )[ 1:] )[ 0 ] 

    try:
      dbFunction = getattr( db, fname )
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

class HandlerDecCredentials( object ):
  
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
  '''
  This decorator for services is not used, but we keep it. It might be useful in
  the future. Goes on top of HandlerDecCredentials.
  '''
  
  def __call__( self, *args, **kwargs ):
    
    self.f.processArgs( *args, **kwargs )
    
    _system,_handler = args[ 0 ].__module__.rsplit( '.', 2 )[-2:]
    
    okProperties   = set( [ 'SiteAdmin' ] )
    
    credentials    = self.f.credentials   
    credProperties = set( credentials.get( 'properties', '' ) )   
    
    if not credProperties & okProperties: 
      return S_ERROR( 'Not right property to execute this action.' ) 
    
    return self.f( *args, **kwargs )

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
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF    