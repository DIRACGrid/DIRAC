
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
# Attributes
# 

  def __checkType( self, value, tList ):
    if type( value ) not in tList:
      raise TypeException( "%s has wrong type. Has to be one of %s" % ( value, tList ) )

  @RemoteMethod
  def setStatus( self, majorStatus, minorStatus ):
    try:
      self.__checkType( majorStatus, types.StringType )
      self.__checkType( minorStatus, types.StringType )
    except TypeException, excp:
      return S_ERROR( str( excp ) )
    return JobState.__jobDB.setJobStatus( self.__jid, majorStatus, minorStatus )

  @RemoteMethod
  def setMinorStatus( self, minorStatus ):
    try:
      self.__checkType( minorStatus, types.StringType )
    except TypeException, excp:
      return S_ERROR( str( excp ) )
    return JobState.__jobDB.setJobStatus( self.__jid, minor = minorStatus )

  @RemoteMethod
  def setAppStatus( self, appStatus ):
    try:
      self.__checkType( appStatus, types.StringType )
    except TypeException, excp:
      return S_ERROR( str( excp ) )
    return JobState.__jobDB.setJobStatus( self.__jid, application = appStatus )

  @RemoteMethod
  def getStatus( self ):
    return JobState.__jobDB.getAttributesForJobList( jobIDs, [ 'Status', 'MinorStatus', 'ApplicationStatus'] )



