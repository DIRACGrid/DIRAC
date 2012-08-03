
import types
from DIRAC import S_OK, S_ERROR, gLogger
from DIRAC.Core.Utilities import Time
from DIRAC.WorkloadManagementSystem.Client.JobState.JobManifest import JobManifest
from DIRAC.Core.DISET.RPCClient import RPCClient
from DIRAC.WorkloadManagementSystem.Service.JobPolicy import *

class JobState( object ):

  class DBHold:

    def __init__( self ):
      self.checked = False
      self.reset()

    def reset( self ):
      self.job = False
      self.log = False
      self.tq = False

  __db = DBHold()

  _sDisableLocal = False

  class RemoteMethod( object ):

    def __init__( self, functor ):
      self.__functor = functor

    def __get__( self, obj, type = None ):
      return self.__class__( self.__functor.__get__( obj, type ) )

    def __call__( self, *args, **kwargs ):
      funcSelf = self.__functor.__self__
      if not funcSelf.localAccess:
        rpc = funcSelf._getStoreClient()
        if kwargs:
          fArgs = ( args, kwargs )
        else:
          fArgs = ( args, )
        return getattr( rpc, self.__functor.__name__ )( funcSelf.jid, fArgs )
      return self.__functor( *args, **kwargs )

  def __init__( self, jid, forceLocal = False, getRPCFunctor = False, source = "Unknown" ):
    self.__jid = jid
    self.__source = str( source )
    self.__forceLocal = forceLocal
    if getRPCFunctor:
      self.__getRPCFunctor = getRPCFunctor
    else:
      self.__getRPCFunctor = RPCClient
    #Init DB if there
    if not JobState.__db.checked:
      JobState.__db.checked = True
      for varName, dbName in ( ( 'job', 'JobDB' ), ( 'log', 'JobLoggingDB' ),
                               ( 'tq', 'TaskQueueDB' ) ):
        try:
          dbImp = "DIRAC.WorkloadManagementSystem.DB.%s" % dbName
          dbMod = __import__( dbImp, fromlist = [ dbImp ] )
          dbClass = getattr( dbMod, dbName )
          dbInstance = dbClass()
          setattr( JobState.__db, varName, dbInstance )
          result = dbInstance._getConnection()
          if not result[ 'OK' ]:
            gLogger.warn( "Could not connect to %s (%s). Resorting to RPC" % ( dbName, result[ 'Message' ] ) )
            JobState.__db.reset()
            break
          else:
            result[ 'Value' ].close()
        except RuntimeError:
          JobState.__db.reset()
          break
        except ImportError:
          JobState.__db.reset()
          break

  @property
  def jid( self ):
    return self.__jid

  def setSource( self, source ):
    self.__source = source

  @property
  def localAccess( self ):
    if JobState._sDisableLocal:
      return False
    if JobState.__db.job or self.__forceLocal:
      return True
    return False

  def __getDB( self ):
    return JobState.__db.job

  def _getStoreClient( self ):
    return self.__getRPCFunctor( "WorkloadManagement/JobStateSync" )

  def getManifest( self, rawData = False ):
    if self.localAccess:
      result = self.__getDB().getJobJDL( self.__jid )
    else:
      result = self._getStoreClient().getManifest( self.__jid )
    if not result[ 'OK' ] or rawData:
      return result
    manifest = JobManifest()
    result = manifest.loadJDL( result[ 'Value' ] )
    if not result[ 'OK' ]:
      return result
    return S_OK( manifest )

  def setManifest( self, manifest ):
    if not isinstance( manifest, JobManifest ):
      manifestStr = manifest
      manifest = JobManifest()
      result = manifest.load( manifestStr )
      if not result[ 'OK' ]:
        return result
    manifestJDL = manifest.dumpAsJDL()
    if self.localAccess:
      return self.__getDB().setJobJDL( self.__jid, manifestJDL )
    return self._getStoreClient().setManifest( self.__jid, manifestJDL )

#Execute traces

  @RemoteMethod
  def executeTrace( self, initialState, trace ):
    try:
      self.__checkType( initialState , types.DictType )
      self.__checkType( trace , ( types.ListType, types.TupleType ) )
    except TypeError, excp:
      return S_ERROR( str( excp ) )
    result = self.getAttributes( initialState.keys() )
    if not result[ 'OK' ]:
      return result
    if not result[ 'Value' ] == initialState:
      return S_ERROR( "Initial state was different. Expected %s Received %s" % ( initialState, result[ 'Value' ] ) )
    gLogger.verbose( "Job %s: About to execute trace. Current state %s" % ( self.__jid, initialState ) )

    for step in trace:
      if type( step ) != types.TupleType or len( step ) < 2:
        return S_ERROR( "Step %s is not properly formatted" % str( step ) )
      if type( step[1] ) != types.TupleType:
        return S_ERROR( "Step %s is not properly formatted" % str( step ) )
      if len( step ) > 2 and type( step[2] ) != types.DictType:
        return S_ERROR( "Step %s is not properly formatted" % str( step ) )
      try:
        fT = getattr( self, step[0] )
      except AttributeError:
        return S_ERROR( "Step %s has invalid method name" % str( step ) )
      try:
        gLogger.verbose( " Job %s: Trace step %s" % ( self.__jid, step ) )
        args = step[1]
        if len( step ) > 2:
          kwargs = step[2]
          fRes = fT( *args, **kwargs )
        else:
          fRes = fT( *args )
      except Exception, excp:
        gLogger.exception( "JobState cannot have exceptions like this!" )
        return S_ERROR( "Step %s has had an exception! %s" % ( step , excp ) )
      if not fRes[ 'OK' ]:
        return S_ERROR( "Step %s has gotten error: %s" % ( step, fRes[ 'Message' ] ) )

    gLogger.info( "Job %s: Ended trace execution" % self.__jid )
    #We return a new initial state
    return self.getAttributes( initialState.keys() )
#
# Status
#

  def __checkType( self, value, tList ):
    if type( tList ) not in ( types.ListType, types.TupleType ):
      tList = [ tList ]
    if type( value ) not in tList:
      raise TypeError( "%s has wrong type. Has to be one of %s" % ( value, tList ) )

  @RemoteMethod
  def setStatus( self, majorStatus, minorStatus = None, appStatus = None, source = None, updateTime = None ):
    try:
      self.__checkType( majorStatus, types.StringType )
      self.__checkType( minorStatus, ( types.StringType, types.NoneType ) )
      self.__checkType( appStatus, ( types.StringType, types.NoneType ) )
      self.__checkType( source, ( types.StringType, types.NoneType ) )
      self.__checkType( updateTime, ( types.NoneType, Time._dateTimeType ) )
    except TypeError, excp:
      return S_ERROR( str( excp ) )
    result = JobState.__db.job.setJobStatus( self.__jid, majorStatus, minorStatus, appStatus )
    if not result[ 'OK' ]:
      return result
    #HACK: Cause joblogging is crappy
    if not minorStatus:
      minorStatus = 'idem'
    if not source:
      source = self.__source
    return JobState.__db.log.addLoggingRecord( self.__jid, majorStatus, minorStatus, appStatus,
                                                 date = updateTime, source = source )

  @RemoteMethod
  def setMinorStatus( self, minorStatus, source = None, updateTime = None ):
    try:
      self.__checkType( minorStatus, types.StringType )
      self.__checkType( source, ( types.StringType, types.NoneType ) )
    except TypeError, excp:
      return S_ERROR( str( excp ) )
    result = JobState.__db.job.setJobStatus( self.__jid, minor = minorStatus )
    if not result[ 'OK' ]:
      return result
    if not source:
      source = self.__source
    return JobState.__db.log.addLoggingRecord( self.__jid, minor = minorStatus,
                                                 date = updateTime, source = source )


  @RemoteMethod
  def getStatus( self ):
    result = JobState.__db.job.getJobAttributes( self.__jid, [ 'Status', 'MinorStatus' ] )
    if not result[ 'OK' ]:
      return result
    data = result[ 'Value' ]
    return S_OK( ( data[ 'Status' ], data[ 'MinorStatus' ] ) )


  @RemoteMethod
  def setAppStatus( self, appStatus, source = None, updateTime = None ):
    try:
      self.__checkType( appStatus, types.StringType )
      self.__checkType( source, ( types.StringType, types.NoneType ) )
    except TypeError, excp:
      return S_ERROR( str( excp ) )
    result = JobState.__db.job.setJobStatus( self.__jid, application = appStatus )
    if not result[ 'OK' ]:
      return result
    if not source:
      source = self.__source
    return JobState.__db.log.addLoggingRecord( self.__jid, application = appStatus,
                                                 date = updateTime, source = source )

  @RemoteMethod
  def getAppStatus( self ):
    result = JobState.__db.job.getJobAttributes( self.__jid, [ 'ApplicationStatus' ] )
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
    return JobState.__db.job.setJobAttribute( self.__jid, name, value )

  @RemoteMethod
  def setAttributes( self, attDict ):
    try:
      self.__checkType( attDict, types.DictType )
    except TypeError, excp:
      return S_ERROR( str( excp ) )
    keys = [ key for key in attDict ]
    values = [ attDict[ key ] for key in keys ]
    return JobState.__db.job.setJobAttributes( self.__jid, keys, values )

  @RemoteMethod
  def getAttribute( self, name ):
    try:
      self.__checkType( name , types.StringTypes )
    except TypeError, excp:
      return S_ERROR( str( excp ) )
    return JobState.__db.job.getJobAttribute( self.__jid, name )

  @RemoteMethod
  def getAttributes( self, nameList = None ):
    try:
      self.__checkType( nameList , ( types.ListType, types.TupleType,
                                     types.NoneType ) )
    except TypeError, excp:
      return S_ERROR( str( excp ) )
    return JobState.__db.job.getJobAttributes( self.__jid, nameList )

#Job parameters

  @RemoteMethod
  def setParameter( self, name, value ):
    try:
      self.__checkType( name, types.StringType )
      self.__checkType( value, types.StringType )
    except TypeError, excp:
      return S_ERROR( str( excp ) )
    return JobState.__db.job.setJobParameter( self.__jid, name, value )

  @RemoteMethod
  def setParameters( self, pDict ):
    try:
      self.__checkType( pDict, types.DictType )
    except TypeError, excp:
      return S_ERROR( str( excp ) )
    pList = []
    for name in pDict:
      pList.append( ( name, pDict[ name ] ) )
    return JobState.__db.job.setJobParameters( self.__jid, pList )

  @RemoteMethod
  def getParameter( self, name ):
    try:
      self.__checkType( name, types.StringType )
    except TypeError, excp:
      return S_ERROR( str( excp ) )
    return JobState.__db.job.getJobParameter( self.__jid, name )

  @RemoteMethod
  def getParameters( self, nameList = None ):
    try:
      self.__checkType( nameList, ( types.ListType, types.TupleType,
                                     types.NoneType ) )
    except TypeError, excp:
      return S_ERROR( str( excp ) )
    return JobState.__db.job.getJobParameters( self.__jid, nameList )


#Optimizer parameters

  @RemoteMethod
  def setOptParameter( self, name, value ):
    try:
      self.__checkType( name, types.StringType )
      self.__checkType( value, types.StringType )
    except TypeError, excp:
      return S_ERROR( str( excp ) )
    return JobState.__db.job.setJobOptParameter( self.__jid, name, value )

  @RemoteMethod
  def setOptParameters( self, pDict ):
    try:
      self.__checkType( pDict, types.DictType )
    except TypeError, excp:
      return S_ERROR( str( excp ) )
    for name in pDict:
      result = JobState.__db.job.setJobOptParameter( self.__jid, name, pDict[ name ] )
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
      result = JobState.__db.job.removeJobOptParameter( self.__jid, name )
      if not result[ 'OK' ]:
        return result
    return S_OK()

  @RemoteMethod
  def getOptParameter( self, name ):
    try:
      self.__checkType( name, types.StringType )
    except TypeError, excp:
      return S_ERROR( str( excp ) )
    return JobState.__db.job.getJobOptParameter( self.__jid, name )

  @RemoteMethod
  def getOptParameters( self, nameList = None ):
    try:
      self.__checkType( nameList, ( types.ListType, types.TupleType,
                                     types.NoneType ) )
    except TypeError, excp:
      return S_ERROR( str( excp ) )
    return JobState.__db.job.getJobOptParameters( self.__jid, nameList )

#Other

  right_resetJob = RIGHT_RESET
  @RemoteMethod
  def resetJob( self, source = "" ):
    result = JobState.__db.job.setJobAttribute( self.__jid, "RescheduleCounter", -1 )
    if not result[ 'OK' ]:
      return S_ERROR( "Cannot set the RescheduleCounter for job %s: %s" % ( self.__jid, result[ 'Message' ] ) )
    result = JobState.__db.tq.deleteJob( self.__jid )
    if not result[ 'OK' ]:
      return S_ERROR( "Cannot delete from TQ job %s: %s" % ( self.__jid, result[ 'Message' ] ) )
    result = JobState.__db.job.rescheduleJob( self.__jid )
    if not result[ 'OK' ]:
      return S_ERROR( "Cannot reschedule in JobDB job %s: %s" % ( self.__jid, result[ 'Message' ] ) )
    JobState.__db.log.addLoggingRecord( self.__jid, "Received", "", "", source = source )
    return S_OK()

  right_getInputData = RIGHT_GET_INFO
  @RemoteMethod
  def getInputData( self ):
    return JobState.__db.job.getInputData( self.__jid )

  right_insertIntoTQ = RIGHT_CHANGE_STATUS
  @RemoteMethod
  def insertIntoTQ( self ):
    result = self.getManifest()
    if not result[ 'OK' ]:
      return result
    manifest = result[ 'Value' ]

    reqSection = "JobRequirements"

    result = manifest.getSection( reqSection )
    if not result[ 'OK' ]:
      return S_ERROR( "No %s section in the job manifest" % reqSection )
    reqCfg = result[ 'Value' ]

    jobReqDict = {}
    for name in JobState.__db.tq.getSingleValueTQDefFields():
      if name in reqCfg:
        if name == 'CPUTime':
          jobReqDict[ name ] = int( reqCfg[ name ] )
        else:
          jobReqDict[ name ] = reqCfg[ name ]

    for name in JobState.__db.tq.getMultiValueTQDefFields():
      if name in reqCfg:
        jobReqDict[ name ] = reqCfg.getOption( name, [] )

    jobPriority = reqCfg.getOption( 'UserPriority', 1 )

    result = JobState.__db.tq.insertJob( self.__jid, jobReqDict, jobPriority )
    if not result[ 'OK' ]:
      errMsg = result[ 'Message' ]
      # Force removing the job from the TQ if it was actually inserted
      result = JobState.__db.tq.deleteJob( self.__jid )
      if result['OK']:
        if result['Value']:
          self.log.info( "Job %s removed from the TQ" % self.__jid )
      return S_ERROR( "Cannot insert in task queue: %s" % errMsg )
    return S_OK()



