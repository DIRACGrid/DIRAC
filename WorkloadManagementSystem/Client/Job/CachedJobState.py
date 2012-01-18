
import types
from DIRAC import S_OK, S_ERROR, gConfig
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

  def __memoizedMethod( self, cKey, functor ):
    if cKey not in self.__cache:
      result = functor()
      if not result[ 'OK' ]:
        return result
      self.__cache[ cKey ] = result[ 'Value' ]
    return S_OK( self.__cache[ cKey ] )

#
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
    cKeys = ( 'att.Status', 'att.MinorStatus' )
    if not self.__cacheExists( cKeys ):
      result = self.__jobState.getStatus()
      if not result[ 'OK' ]:
        return result
      data = result[ 'Value' ]
      for iP in range( len( cKeys ) ):
        self.__cache[ cKeys[ iP ] ] = data[ iP ]
    return S_OK( ( self.__cache[ 'att.Status' ], self.__cache[ 'att.MinorStatus' ] ) )

  def setAppStatus( self, appStatus ):
    self.__cache[ 'att.ApplicationStatus' ] = appStatus
    return S_OK()

  def getAppStatus( self ):
    return self.__memoizedMethod( 'att.ApplicationStatus', self.__jobState.getAppStatus )
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
    return self.__memoizedMethod( 'att.%s' % name, self.__jobState.getAttribute )

  def getAttributeList( self, nameList ):
    #TODO: cache list of attributes
    return self.__memoizedMethod( 'att.%s' % name, self.__jobState.getAttribute )
