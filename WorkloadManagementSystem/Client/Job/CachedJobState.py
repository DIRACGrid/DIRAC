
import types
from DIRAC import S_OK, S_ERROR, gLogger
from DIRAC.WorkloadManagementSystem.Client.Job.JobState import JobState

class CachedJobState( object ):

  class TracedMethod( object ):

    def __init__( self, functor ):
      self.__functor = functor

    #Black magic to map the unbound function received at TracedMethod.__init__ time
    #to a JobState method with a proper self
    def __get__( self, obj, type = None ):
      return self.__class__( self.__func.__get__( obj, type ) )

    def __call__( self, *args, **kwargs ):
      funcSelf = self.__functor.__self__
      trace = args
      print "trace is %s" % str( args )
      funcSelf._addTrace( trace )
      return self.__functor( *args, **kwargs )

  def __init__( self, jid ):
    self.__jid = jid
    self.__jobState = JobState( jid )
    self.__cache = {}
    self.__trace = []

  @property
  def jid( self ):
    return self.__jid

  @property
  def traceActions( self ):
    return self.__keepTrace

  def _addTrace( self, actionTuple ):
    self.__trace.append( actionTuple )

  def __cacheExists( self, keyList ):
    if type( keyList ) in types.StringTypes:
      keyList = [ keyList ]
    for key in keyList:
      if key not in self.__cache:
        return False
    return True

  def __cacheResult( self, cKey, functor ):
    keyType = type( cKey )
    #If it's a string
    if keyType in types.StringTypes:
      if cKey not in self.__cache:
        result = functor()
        if not result[ 'OK' ]:
          return result
        data = result[ 'Value' ]
        self.__cache[ cKey ] = data
      return S_OK( self.__cache[ cKey ] )
    #Tuple/List
    elif keyType in ( types.ListType, types.TupleType ):
      if not self.__cacheExists( cKeys ):
        result = functor()
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

  def __cacheDict( self, prefix, keyList, functor ):
    cKeys = [ "%s.%s" % ( prefix, key ) for key in keyList ]
    if not self.__cacheExists( cKeys ):
      result = functor( keyList )
      if not result[ 'OK' ]:
        return result
      data = result [ 'Value' ]
      for key in keyList:
        self.__cache[ "%s.%s" % ( prefix, key ) ] = data[ key ]
    return S_OK( dict( [ ( "%s.%s" % ( prefix, key ), data[ key ] ) for key in keyList ] ) )


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
    self.__cacheResult( ( 'att.Status', 'att.MinorStatus' ), self.__jobState.getStatus )

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
    if type( name ) != types.StringTypes:
      return S_ERROR( "Attribute name has to be a string" )
    self.__cache[ "att.%s" % name ] = value
    return S_OK()

  @TracedMethod
  def setAttributes( self, attDict ):
    if type( attrDict ) != types.DictType:
      return S_ERROR( "Attributes has to be a dictionary" )
    for key in attDict:
      self.__cache[ 'att.%s' % key ] = attDict[ key ]
    return S_OK()

  def getAttribute( self, name ):
    return self.__cacheResult( 'att.%s' % name, self.__jobState.getAttribute )

  def getAttributeList( self, nameList ):
    return self.__cacheDict( 'att', nameList, self.__jobState.getAttributeList )
