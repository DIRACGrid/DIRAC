
import types
from DIRAC import S_OK, S_ERROR, gConfig
from DIRAC.WorkloadManagementSystem.Client.Job.JobManifest import JobManifest
from DIRAC.Core.DISET.RPCClient import RPCClient

class JobState( object ):

  __jobDB = None
  _sDisableLocal = False

  class RemoteMethod( object ):

    def __init__( self, functor ):
      self.__functor = functor

    def __get__( self, obj, type = None ):
      return self.__class__( self.__functor.__get__( obj, type ) )

    def __call__( self, *args, **kwargs ):
      funcSelf = self.__functor.__self__
      if kwargs:
        raise Exception( "JobState.%s does not support keyword arguments" % ( self.__functor.__name ) )
      if not funcSelf.hasLocalAccess:
        rpc = funcSelf._getStoreClient()
        return getattr( rpc, self.__functor.__name__ )( funcSelf.jid, *args )
      return self.__functor( *args )

  def __init__( self, jid, forceLocal = False, getRPCFunctor = False ):
    self.__jid = jid
    self.__forceLocal = forceLocal
    if getRPCFunctor:
      self.__getRPCFunctor = getRPCFunctor
    else:
      self.__getRPCFunctor = RPCClient
    #Init DB if there
    if JobState.__jobDB == None:
      try:
        from DIRAC.WorkloadManagementSystem.DB.JobDB import JobDB
        JobState.__jobDB = JobDB()
        result = JobState.__jobDB._getConnection()
        if not result[ 'OK' ]:
          gLogger.warn( "Could not connect to JobDB (%s). Resorting to RPC" % result[ 'Message' ] )
          JobState.__jobDB = False
        else:
          result[ 'Value' ].close()
      except ImportError:
        JobState.__jobDB = False

  @property
  def jid( self ):
    return self.__jid

  @property
  def hasLocalAccess( self ):
    if JobState._sDisableLocal:
      return False
    if JobState.__jobDB or self.__forceLocal:
      return True
    return False

  def __getDB( self ):
    return JobState.__jobDB

  def _getStoreClient( self ):
    return self.__getRPCFunctor( "WorkloadManagement/JobStore" )

  def getManifest( self, rawData = False ):
    if self.hasLocalAccess:
      result = self.__getDB().getJobJDL( self.__jid )
    else:
      result = self._getStoreClient().getManifest( self.__jid )
    if not result[ 'OK' ] or rawData:
      return result
    jobManifest = JobManifest()
    result = jobManifest.loadJDL( result[ 'Value' ] )
    if not result[ 'OK' ]:
      return result
    return S_OK( jobManifest )

  def setManifest( self, jobManifest ):
    if isinstance( jobManifest, JobManifest ):
      manifest = jobManifest.dumpAsJDL()
    else:
      manifest = str( jobManifest )
    if self.hasLocalAccess:
      return self.__getDB().setJobJDL( self.__jid, manifest )
    return self._getStoreClient().setManifest( self.__jid, manifest )

#
# Status
# 

  def __checkType( self, value, tList ):
    if type( value ) not in tList:
      raise TypeError( "%s has wrong type. Has to be one of %s" % ( value, tList ) )

  @RemoteMethod
  def setStatus( self, majorStatus, minorStatus ):
    try:
      self.__checkType( majorStatus, types.StringType )
      self.__checkType( minorStatus, types.StringType )
    except TypeError, excp:
      return S_ERROR( str( excp ) )
    return JobState.__jobDB.setJobStatus( self.__jid, majorStatus, minorStatus )

  @RemoteMethod
  def setMinorStatus( self, minorStatus ):
    try:
      self.__checkType( minorStatus, types.StringType )
    except TypeError, excp:
      return S_ERROR( str( excp ) )
    return JobState.__jobDB.setJobStatus( self.__jid, minor = minorStatus )

  @RemoteMethod
  def getStatus( self ):
    result = JobState.__jobDB.getJobAttributes( self.__jid, [ 'Status', 'MinorStatus' ] )
    if not result[ 'OK' ]:
      return result
    data = result[ 'Value' ]
    return S_OK( ( data[ 'Status' ], data[ 'MinorStatus' ] ) )


  @RemoteMethod
  def setAppStatus( self, appStatus ):
    try:
      self.__checkType( appStatus, types.StringType )
    except TypeError, excp:
      return S_ERROR( str( excp ) )
    return JobState.__jobDB.setJobStatus( self.__jid, application = appStatus )

  @RemoteMethod
  def getAppStatus( self ):
    result = JobState.__jobDB.getJobAttributes( self.__jid, [ 'ApplicationStatus' ] )
    if result[ 'OK' ]:
      result[ 'Value' ] = result[ 'Value' ][ 'ApplicationStatus' ]
    return result

#Attributes

  @RemoteMethod
  def setAttribute( self, name, value ):
    try:
      self.__checkType( name, types.StringType )
      self.__checkType( value, types.StringType )
    except TypeError, excp:
      return S_ERROR( str( excp ) )
    return JobState.__jobDB.setJobAttribute( self.__jid, name, value )

  @RemoteMethod
  def setAttributes( self, attDict ):
    try:
      self.__checkType( attDict, types.DictType )
    except TypeError, excp:
      return S_ERROR( str( excp ) )
    keys = [ key for key in attDict ]
    values = [ attDict[ key ] for key in keys ]
    return JobState.__jobDB.setJobAttributes( self.__jid, keys, values )

  @RemoteMethod
  def getAttribute( self, name ):
    try:
      self.__checkType( name , types.StringTypes )
    except TypeError, excp:
      return S_ERROR( str( excp ) )
    return JobState.__jobDB.getJobAttribute( self.__jid, name )

  @RemoteMethod
  def getAttributes( self, nameList = None ):
    try:
      self.__checkType( nameList , ( types.ListType, types.TupleType,
                                     types.NoneType ) )
    except TypeError, excp:
      return S_ERROR( str( excp ) )
    return JobState.__jobDB.getJobAttributes( self.__jid, nameList )

#Optimizer parameters

  @RemoteMethod
  def setOptParameter( self, name, value ):
    try:
      self.__checkType( name, types.StringType )
      self.__checkType( value, types.StringType )
    except TypeError, excp:
      return S_ERROR( str( excp ) )
    return JobState.__jobDB.setJobOptParameter( self.__jid, name, value )

  @RemoteMethod
  def setOptParameters( self, pDict ):
    try:
      self.__checkType( pDict, types.DictType )
    except TypeError, excp:
      return S_ERROR( str( excp ) )
    for name in pDict:
      result = JobState.__jobDB.setJobOptParameter( self.__jid, name, pDict[ name ] )
      if not result[ 'OK' ]:
        return result
    return S_OK()

  @RemoteMethod
  def removeOptParameters( self, nameList ):
    if type( nameList ) in types.StringTypes:
      nameList = [ nameList ]
    try:
      self.__checkType( nameList, ( types.ListType, types.TupleType ) )
    except TypeError, excp:
      return S_ERROR( str( excp ) )
    for name in nameList:
      result = JobState.__jobDB.removeJobOptParameter( self.__jid, name )
      if not result[ 'OK' ]:
        return result
    return S_OK()

  @RemoteMethod
  def getOptParameter( self, name ):
    try:
      self.__checkType( name, types.StringType )
    except TypeError, excp:
      return S_ERROR( str( excp ) )
    return JobState.__jobDB.getJobOptParameter( self.__jid, name )

  @RemoteMethod
  def getOptParameters( self, nameList = None ):
    try:
      self.__checkType( nameList, ( types.ListType, types.TupleType,
                                     types.NoneType ) )
    except TypeError, excp:
      return S_ERROR( str( excp ) )
    return JobState.__jobDB.getJobOptParameters( self.__jid, nameList )
