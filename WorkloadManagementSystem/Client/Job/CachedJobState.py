
import types, copy
from DIRAC import S_OK, S_ERROR, gLogger
from DIRAC.WorkloadManagementSystem.Client.Job.JobState import JobState

class CachedJobState( object ):

  class TracedMethod( object ):

    def __init__( self, functor ):
      self.__functor = functor

    #Black magic to map the unbound function received at TracedMethod.__init__ time
    #to a JobState method with a proper self
    def __get__( self, obj, type = None ):
      return self.__class__( self.__functor.__get__( obj, type ) )

    def __call__( self, *args, **kwargs ):
      funcSelf = self.__functor.__self__
      if kwargs:
        trace = ( self.__functor.__name__, args, kwargs )
      else:
        trace = ( self.__functor.__name__, args )
      result = self.__functor( *args, **kwargs )
      if result[ 'OK' ]:
        funcSelf._addTrace( trace )
      return result

  log = gLogger.getSubLogger( "CachedJobState" )

  def __init__( self, jid ):
    self.dOnlyCache = False
    self.__jid = jid
    self.__jobState = JobState( jid )
    self.__cache = {}
    self.__trace = []

  @property
  def jid( self ):
    return self.__jid

  def _addTrace( self, actionTuple ):
    self.__trace.append( actionTuple )

  def getTrace( self ):
    return copy.copy( self.__trace )

  def commitChanges( self ):
    if not self.__trace:
      return S_OK()
    trace = self.__trace
    result = self.__jobState.executeTrace( trace )
    done = 0
    try:
      result.pop( 'rpcStub' )
    except KeyError:
      pass
    if not result[ 'OK' ]:
      if 'nProc' in result:
        done = result[ 'nProc' ]
    else:
      done = len( trace )
    for iP in range( done ):
      self.__trace.pop( 0 )
    return result

  def __cacheExists( self, keyList ):
    if type( keyList ) in types.StringTypes:
      keyList = [ keyList ]
    for key in keyList:
      if key not in self.__cache:
        return False
    return True

  def __cacheResult( self, cKey, functor, fArgs = None ):
    keyType = type( cKey )
    #If it's a string
    if keyType in types.StringTypes:
      if cKey not in self.__cache:
        if self.dOnlyCache:
          return S_ERROR( "%s is not cached" )
        if not fArgs:
          fArgs = tuple()
        result = functor( *fArgs )
        if not result[ 'OK' ]:
          return result
        data = result[ 'Value' ]
        self.__cache[ cKey ] = data
      return S_OK( self.__cache[ cKey ] )
    #Tuple/List
    elif keyType in ( types.ListType, types.TupleType ):
      if not self.__cacheExists( cKey ):
        if self.dOnlyCache:
          return S_ERROR( "%s is not cached" )
        if not fArgs:
          fArgs = tuple()
        result = functor( *fArgs )
        if not result[ 'OK' ]:
          return result
        data = result[ 'Value' ]
        if len( cKey ) != len( data ):
          gLogger.warn( "CachedJobState.__memorize( %s, %s = %s ) doesn't receive the same amount of values as keys" % ( cKey,
                                                                                                           functor, data ) )
          return data
        for i in range( len( cKey ) ):
          self.__cache[ cKey[ i ] ] = data[i]
      #Prepare result
      return S_OK( tuple( [ self.__cache[ cK ]  for cK in cKey ] ) )
    else:
      raise RuntimeError( "Cache key %s does not have a valid type" % cKey )

  def __cacheDict( self, prefix, functor, keyList = None ):
    if not keyList or not self.__cacheExists( [ "%s.%s" % ( prefix, key ) for key in keyList ] ):
      result = functor( keyList )
      if not result[ 'OK' ]:
        return result
      data = result [ 'Value' ]
      for key in data:
        cKey = "%s.%s" % ( prefix, key )
        #If the key is already in the cache. DO NOT TOUCH. User may have already modified it. 
        #We update the coming data with the cached data
        if cKey in self.__cache:
          data[ key ] = self.__cache[ cKey ]
        else:
          self.__cache[ cKey ] = data[ key ]
      return S_OK( data )
    return S_OK( dict( [ ( key, self.__cache[ "%s.%s" % ( prefix, key ) ] ) for key in keyList ] ) )

  def _inspectCache( self ):
    return copy.deepcopy( self.__cache )

  def clearCache( self ):
    self.__cache = {}


# Attributes
# 

  @TracedMethod
  def setStatus( self, majorStatus, minorStatus ):
    self.__cache[ 'att.Status' ] = majorStatus
    self.__cache[ 'att.MinorStatus' ] = minorStatus
    return S_OK()

  @TracedMethod
  def setMinorStatus( self, minorStatus ):
    self.__cache[ 'att.MinorStatus' ] = minorStatus
    return S_OK()

  def getStatus( self ):
    return self.__cacheResult( ( 'att.Status', 'att.MinorStatus' ), self.__jobState.getStatus )

  @TracedMethod
  def setAppStatus( self, appStatus ):
    self.__cache[ 'att.ApplicationStatus' ] = appStatus
    return S_OK()

  def getAppStatus( self ):
    return self.__cacheResult( 'att.ApplicationStatus', self.__jobState.getAppStatus )
#
# Attribs
#

  @TracedMethod
  def setAttribute( self, name, value ):
    if type( name ) not in types.StringTypes:
      return S_ERROR( "Attribute name has to be a string" )
    self.__cache[ "att.%s" % name ] = value
    return S_OK()

  @TracedMethod
  def setAttributes( self, attDict ):
    if type( attDict ) != types.DictType:
      return S_ERROR( "Attributes has to be a dictionary and it's %s" % str( type( attDict ) ) )
    for key in attDict:
      self.__cache[ 'att.%s' % key ] = attDict[ key ]
    return S_OK()

  def getAttribute( self, name ):
    return self.__cacheResult( 'att.%s' % name, self.__jobState.getAttribute, ( name, ) )

  def getAttributes( self, nameList = None ):
    return self.__cacheDict( 'att', self.__jobState.getAttributes, nameList )

#Optimizer params


  @TracedMethod
  def setOptParameter( self, name, value ):
    if type( name ) not in types.StringTypes:
      return S_ERROR( "Attribute name has to be a string" )
    self.__cache[ 'optp.%s' % name ] = value
    return S_OK()

  @TracedMethod
  def setOptParameters( self, pDict ):
    if type( pDict ) != types.DictType:
      return S_ERROR( "Optimizer parameters has to be a dictionary" )
    for key in pDict:
      self.__cache[ 'optp.%s' % key ] = pDict[ key ]
    return S_OK()

  @TracedMethod
  def removeOptParameters( self, nameList ):
    if type( nameList ) in types.StringTypes:
      nameList = [ nameList ]
    elif type( nameList ) not in ( types.ListType, types.TupleType ):
      return S_ERROR( "A list of parameters is expected as an argument to removeOptParameters" )
    for name in nameList:
      try:
        self.__cache.pop( "optp.%s" % name )
      except KeyError:
        pass
    return S_OK()

  def getOptParameter( self, name ):
    return self.__cacheResult( "optp.%s" % name, self.__jobState.getOptParameter, ( name, ) )

  def getOptParameters( self, nameList = None ):
    return self.__cacheDict( 'optp', self.__jobState.getOptParameters, nameList )
