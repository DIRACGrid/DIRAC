
import types, copy, time
from DIRAC.Core.Utilities import Time, DEncode
from DIRAC import S_OK, S_ERROR, gLogger
from DIRAC.WorkloadManagementSystem.Client.JobState.JobState import JobState
from DIRAC.WorkloadManagementSystem.Client.JobState.JobManifest import JobManifest

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
      funcName = self.__functor.__name__
      if not funcSelf.valid:
        return S_ERROR( "CachedJobState( %d ) is not valid" % funcSelf.jid )
      result = self.__functor( *args, **kwargs )
      if result[ 'OK' ]:
        if funcName in ( "setStatus", "setMinorStatus", "setAppStatus" ):
          kwargs[ 'updateTime' ] = Time.dateTime()
        if kwargs:
          trace = ( funcName, args, kwargs )
        else:
          trace = ( funcName, args )
        funcSelf._addTrace( trace )
      return result

  log = gLogger.getSubLogger( "CachedJobState" )

  def __init__( self, jid, skipInitState = False ):
    self.dOnlyCache = False
    self.__jid = jid
    self.__jobState = JobState( jid )
    self.__cache = {}
    self.__trace = []
    self.__manifest = False
    self.__initState = None
    self.__lastValidState = time.time()
    if not skipInitState:
      result = self.getAttributes( [ "Status", "MinorStatus", "LastUpdateTime" ] )
      if result[ 'OK' ]:
        self.__initState = result[ 'Value' ]
      else:
        self.__initState = None

  def recheckValidity( self, graceTime = 600 ):
    now = time.time()
    if graceTime <= 0 or now - self.__lastValidState > graceTime:
      self.__lastValidState = now
      result =  self.__jobState.getAttributes( [ "Status", "MinorStatus", "LastUpdateTime" ] )
      if not result[ 'OK' ]:
        return result
      currentState = result[ 'Value' ]
      if not currentState == self.__initState:
        return S_OK( False )
      return S_OK( True )
    return S_OK( self.valid )

  @property
  def valid( self ):
    return self.__initState != None

  @property
  def jid( self ):
    return self.__jid

  def _addTrace( self, actionTuple ):
    self.__trace.append( actionTuple )

  def getTrace( self ):
    return copy.copy( self.__trace )

  def commitChanges( self ):
    if self.__initState == None:
      return S_ERROR( "CachedJobState( %d ) is not valid" % self.__jid )
    if not self.__trace:
      return S_OK()
    trace = self.__trace
    result = self.__jobState.executeTrace( self.__initState, trace )
    try:
      result.pop( 'rpcStub' )
    except KeyError:
      pass
    trLen = len( self.__trace )
    self.__trace = []
    if not result[ 'OK' ]:
      self.__initState = None
      self.__cache = {}
      return result
    self.__initState = result[ 'Value' ]
    self.__lastValidState = time.time()
    return S_OK( trLen )

  def serialize( self ):
    if self.__manifest:
      manifest = ( self.__manifest.dumpAsCFG(), self.__manifest.isDirty() )
    else:
      manifest = None
    return DEncode.encode( ( self.__jid, self.__cache, self.__trace, manifest, self.__initState ) )

  @staticmethod
  def deserialize( stub ):
    dataTuple, slen = DEncode.decode( stub )
    if len( dataTuple ) != 5:
      return S_ERROR( "Invalid stub" )
    #jid
    if type( dataTuple[0] ) not in ( types.IntType, types.LongType ):
      return S_ERROR( "Invalid stub" )
    #cache
    if type( dataTuple[1] ) != types.DictType:
      return S_ERROR( "Invalid stub" )
    #trace
    if type( dataTuple[2] ) != types.ListType:
      return S_ERROR( "Invalid stub" )
    #manifest
    tdt3 = type( dataTuple[3] )
    if tdt3 != types.NoneType and ( tdt3 != types.TupleType and len( dataTuple[3] ) != 2 ):
      return S_ERROR( "Invalid stub" )
    #initstate
    if type( dataTuple[4] ) != types.DictType:
      return S_ERROR( "Invalid stub" )
    cjs = CachedJobState( dataTuple[0], skipInitState = True )
    cjs.__cache = dataTuple[1]
    cjs.__trace = dataTuple[2]
    dt3 = dataTuple[3]
    if dataTuple[3]:
      manifest = JobManifest()
      result = manifest.loadCFG( dt3[0] )
      if not result[ 'OK' ]:
        return result
      if dt3[1]:
        manifest.setDirty()
      else:
        manifest.clearDirty()
      cjs.__manifest = manifest
    cjs.__initState = dataTuple[4]
    return S_OK( cjs )

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

  def _clearCache( self ):
    self.__cache = {}

  @property
  def _internals( self ):
    if self.__manifest:
      manifest = ( self.__manifest.dumpAsCFG(), self.__manifest.isDirty() )
    else:
      manifest = None
    return ( self.__jid, self.dOnlyCache, dict( self.__cache ),
             tuple( self.__trace ), manifest, dict( self.__initState ) )

#
# Manifest
#

  def getManifest( self ):
    if not self.__manifest:
      result = self.__jobState.getManifest()
      if not result[ 'OK' ]:
        return result
      self.__manifest = result[ 'Value' ]
    return S_OK( self.__manifest )

  def setManifest( self, manifest ):
    if not isinstance( manifest, JobManifest ):
      result = manifest.load( result[ 'Value' ] )
      if not result[ 'OK' ]:
        return result
      manifest = result[ 'Value ']
    manCFG = manifest.dumpAsCFG()
    if self.__manifest and ( self.__manifest.dumpAsCFG() == manCFG and not manifest.isDirty() ):
      return S_OK()
    self._addTrace( ( "setManifest", ( manCFG, ) ) )
    self.__manifest = manifest
    self.__manifest.clearDirty()
    return S_OK()

# Attributes
#

  @TracedMethod
  def setStatus( self, majorStatus, minorStatus = None ):
    self.__cache[ 'att.Status' ] = majorStatus
    if minorStatus:
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

#Job params

  @TracedMethod
  def setParameter( self, name, value ):
    if type( name ) not in types.StringTypes:
      return S_ERROR( "Job parameter name has to be a string" )
    self.__cache[ 'jobp.%s' % name ] = value
    return S_OK()

  @TracedMethod
  def setParameters( self, pDict ):
    if type( pDict ) != types.DictType:
      return S_ERROR( "Job parameters has to be a dictionary" )
    for key in pDict:
      self.__cache[ 'jobp.%s' % key ] = pDict[ key ]
    return S_OK()

  def getParameter( self, name ):
    return self.__cacheResult( "jobp.%s" % name, self.__jobState.getParameter, ( name, ) )

  def getParameters( self, nameList = None ):
    return self.__cacheDict( 'jobp', self.__jobState.getParameters, nameList )

#Optimizer params

  @TracedMethod
  def setOptParameter( self, name, value ):
    if type( name ) not in types.StringTypes:
      return S_ERROR( "Optimizer parameter name has to be a string" )
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

#Other

  def getInputData( self ):
    return self.__cacheResult( "inputData" , self.__jobState.getInputData )

  @TracedMethod
  def insertIntoTQ( self ):
    if self.valid:
      return S_OK()
    return S_ERROR( "Cached state is invalid" )
