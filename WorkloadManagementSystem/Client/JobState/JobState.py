
import types
from DIRAC import gLogger, S_OK, S_ERROR
from DIRAC.Core.Utilities import Time
from DIRAC.WorkloadManagementSystem.Client.JobState.JobManifest import JobManifest
from DIRAC.Core.DISET.RPCClient import RPCClient
from DIRAC.WorkloadManagementSystem.Service.JobPolicy import RIGHT_GET_INFO, RIGHT_RESCHEDULE
from DIRAC.WorkloadManagementSystem.Service.JobPolicy import RIGHT_RESET, RIGHT_CHANGE_STATUS

__RCSID__ = "$Id"

class JobState( object ):

  class DBHold:

    def __init__( self ):
      self.checked = False
      self.reset()

    def reset( self ):
      self.job = None
      self.log = None
      self.tq = None

  __db = DBHold()

  _sDisableLocal = False

  class RemoteMethod( object ):

    def __init__( self, functor ):
      self.__functor = functor

    def __get__( self, obj, oType = None ):
      return self.__class__( self.__functor.__get__( obj, oType ) )

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
    self.checkDBAccess()

  @classmethod
  def checkDBAccess( cls ):
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
    if not result[ 'Value' ]:
      return S_ERROR( "No manifest for job %s" % self.__jid )
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
      return self.__retryFunction( 5, self.__getDB().setJobJDL, ( self.__jid, manifestJDL ) )
    return self._getStoreClient().setManifest( self.__jid, manifestJDL )

#Execute traces

  def __retryFunction( self, retries, functor, args = False, kwargs = False ):
    retries = max( 1, retries )
    if not args:
      args = tuple()
    if not kwargs:
      kwargs = {}
    while retries:
      retries -= 1
      result = functor( *args, **kwargs )
      if result[ 'OK' ]:
        return result
      if retries == 0:
        return result
    return S_ERROR( "No more retries" )

  right_commitCache = RIGHT_GET_INFO
  @RemoteMethod
  def commitCache( self, initialState, cache, jobLog ):
    try:
      self.__checkType( initialState , types.DictType )
      self.__checkType( cache , types.DictType )
      self.__checkType( jobLog , ( types.ListType, types.TupleType ) )
    except TypeError, excp:
      return S_ERROR( str( excp ) )
    result = self.getAttributes( initialState.keys() )
    if not result[ 'OK' ]:
      return result
    if not result[ 'Value' ] == initialState:
      return S_OK( False )
    gLogger.verbose( "Job %s: About to execute trace. Current state %s" % ( self.__jid, initialState ) )

    data = { 'att': [], 'jobp': [], 'optp': [] }
    for key in cache:
      for dk in data:
        if key.find( "%s." % dk ) == 0:
          data[ dk ].append( ( key[ len( dk ) + 1:], cache[ key ] ) )

    jobDB = JobState.__db.job
    if data[ 'att' ]:
      attN = [ t[0] for t in data[ 'att' ] ]
      attV = [ t[1] for t in data[ 'att' ] ]
      result = self.__retryFunction( 5, jobDB.setJobAttributes,
                                     ( self.__jid, attN, attV ), { 'update' : True } )
      if not result[ 'OK' ]:
        return result

    if data[ 'jobp' ]:
      result = self.__retryFunction( 5, jobDB.setJobParameters, ( self.__jid, data[ 'jobp' ] ) )
      if not result[ 'OK' ]:
        return result

    for k,v in  data[ 'optp' ]:
      result = self.__retryFunction( 5, jobDB.setJobOptParameter, ( self.__jid, k, v ) )
      if not result[ 'OK' ]:
        return result

    if 'inputData' in cache:
      result = self.__retryFunction( 5, jobDB.setInputData, ( self.__jid, cache[ 'inputData' ] ) )
      if not result[ 'OK' ]:
        return result

    logDB = JobState.__db.log
    gLogger.verbose( "Adding logging records for %s" % self.__jid )
    for record, updateTime, source in jobLog:
      gLogger.verbose( "Logging records for %s: %s %s %s" % ( self.__jid, record, updateTime, source ) )
      record[ 'date' ] = updateTime
      record[ 'source' ] = source
      result = self.__retryFunction( 5, logDB.addLoggingRecord, ( self.__jid, ), record )
      if not result[ 'OK' ]:
        return result

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

  right_setStatus = RIGHT_GET_INFO
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

  right_getMinorStatus = RIGHT_GET_INFO
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

  right_setAppStatus = RIGHT_GET_INFO
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

  right_getAppStatus = RIGHT_GET_INFO
  @RemoteMethod
  def getAppStatus( self ):
    result = JobState.__db.job.getJobAttributes( self.__jid, [ 'ApplicationStatus' ] )
    if result[ 'OK' ]:
      result[ 'Value' ] = result[ 'Value' ][ 'ApplicationStatus' ]
    return result

#Attributes

  right_setAttribute = RIGHT_GET_INFO
  @RemoteMethod
  def setAttribute( self, name, value ):
    try:
      self.__checkType( name, types.StringType )
      self.__checkType( value, types.StringType )
    except TypeError, excp:
      return S_ERROR( str( excp ) )
    return JobState.__db.job.setJobAttribute( self.__jid, name, value )

  right_setAttributes = RIGHT_GET_INFO
  @RemoteMethod
  def setAttributes( self, attDict ):
    try:
      self.__checkType( attDict, types.DictType )
    except TypeError, excp:
      return S_ERROR( str( excp ) )
    keys = [ key for key in attDict ]
    values = [ attDict[ key ] for key in keys ]
    return JobState.__db.job.setJobAttributes( self.__jid, keys, values )

  right_getAttribute = RIGHT_GET_INFO
  @RemoteMethod
  def getAttribute( self, name ):
    try:
      self.__checkType( name , types.StringTypes )
    except TypeError, excp:
      return S_ERROR( str( excp ) )
    return JobState.__db.job.getJobAttribute( self.__jid, name )

  right_getAttributes = RIGHT_GET_INFO
  @RemoteMethod
  def getAttributes( self, nameList = None ):
    try:
      self.__checkType( nameList , ( types.ListType, types.TupleType,
                                     types.NoneType ) )
    except TypeError, excp:
      return S_ERROR( str( excp ) )
    return JobState.__db.job.getJobAttributes( self.__jid, nameList )

#Job parameters

  right_setParameter = RIGHT_GET_INFO
  @RemoteMethod
  def setParameter( self, name, value ):
    try:
      self.__checkType( name, types.StringType )
      self.__checkType( value, types.StringType )
    except TypeError, excp:
      return S_ERROR( str( excp ) )
    return JobState.__db.job.setJobParameter( self.__jid, name, value )

  right_setParameters = RIGHT_GET_INFO
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

  right_getParameter = RIGHT_GET_INFO
  @RemoteMethod
  def getParameter( self, name ):
    try:
      self.__checkType( name, types.StringType )
    except TypeError, excp:
      return S_ERROR( str( excp ) )
    return JobState.__db.job.getJobParameter( self.__jid, name )

  right_getParameters = RIGHT_GET_INFO
  @RemoteMethod
  def getParameters( self, nameList = None ):
    try:
      self.__checkType( nameList, ( types.ListType, types.TupleType,
                                     types.NoneType ) )
    except TypeError, excp:
      return S_ERROR( str( excp ) )
    return JobState.__db.job.getJobParameters( self.__jid, nameList )


#Optimizer parameters

  right_setOptParameter = RIGHT_GET_INFO
  @RemoteMethod
  def setOptParameter( self, name, value ):
    try:
      self.__checkType( name, types.StringType )
      self.__checkType( value, types.StringType )
    except TypeError, excp:
      return S_ERROR( str( excp ) )
    return JobState.__db.job.setJobOptParameter( self.__jid, name, value )

  right_setOptParameters = RIGHT_GET_INFO
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

  right_removeOptParameters = RIGHT_GET_INFO
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

  right_getOptParameter = RIGHT_GET_INFO
  @RemoteMethod
  def getOptParameter( self, name ):
    try:
      self.__checkType( name, types.StringType )
    except TypeError, excp:
      return S_ERROR( str( excp ) )
    return JobState.__db.job.getJobOptParameter( self.__jid, name )

  right_getOptParameters = RIGHT_GET_INFO
  @RemoteMethod
  def getOptParameters( self, nameList = None ):
    try:
      self.__checkType( nameList, ( types.ListType, types.TupleType,
                                     types.NoneType ) )
    except TypeError, excp:
      return S_ERROR( str( excp ) )
    return JobState.__db.job.getJobOptParameters( self.__jid, nameList )

#Other

  @classmethod
  def cleanTaskQueues( cls, source = '' ):
    result = JobState.__db.tq.enableAllTaskQueues()
    if not result[ 'OK' ]:
      return result
    result = JobState.__db.tq.findOrphanJobs()
    if not result[ 'OK' ]:
      return result
    for jid in result[ 'Value' ]:
      result = JobState.__db.tq.deleteJob( jid )
      if not result[ 'OK' ]:
        gLogger.error( "Cannot delete from TQ job %s: %s" % ( jid, result[ 'Message' ] ) )
        continue
      result = JobState.__db.job.rescheduleJob( jid )
      if not result[ 'OK' ]:
        gLogger.error( "Cannot reschedule in JobDB job %s: %s" % ( jid, result[ 'Message' ] ) )
        continue
      JobState.__db.log.addLoggingRecord( jid, "Received", "", "", source = "JobState" )
    return S_OK()


  right_resetJob = RIGHT_RESCHEDULE
  @RemoteMethod
  def rescheduleJob( self, source = "" ):
    result = JobState.__db.tq.deleteJob( self.__jid )
    if not result[ 'OK' ]:
      return S_ERROR( "Cannot delete from TQ job %s: %s" % ( self.__jid, result[ 'Message' ] ) )
    result = JobState.__db.job.rescheduleJob( self.__jid )
    if not result[ 'OK' ]:
      return S_ERROR( "Cannot reschedule in JobDB job %s: %s" % ( self.__jid, result[ 'Message' ] ) )
    JobState.__db.log.addLoggingRecord( self.__jid, "Received", "", "", source = source )
    return S_OK()

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

  @classmethod
  def checkInputDataStructure( self, pDict ):
    if type( pDict ) != types.DictType:
      return S_ERROR( "Input data has to be a dictionary" )
    for lfn in pDict:
      if 'Replicas' not in pDict[ lfn ]:
        return S_ERROR( "Missing replicas for lfn %s" % lfn )
        replicas = pDict[ lfn ][ 'Replicas' ]
        for seName in replicas:
          if 'SURL' not in replicas or 'Disk' not in replicas:
            return S_ERROR( "Missing SURL or Disk for %s:%s replica" % ( seName, lfn ) )
    return S_OK()

  right_setInputData = RIGHT_GET_INFO
  @RemoteMethod
  def set_InputData( self, lfnData ):
    result = self.checkInputDataStructure( lfnData )
    if not result[ 'OK' ]:
      return result
    return self.__db.job.setInputData( self.__jid, lfnData )

  right_insertIntoTQ = RIGHT_CHANGE_STATUS
  @RemoteMethod
  def insertIntoTQ( self, manifest = None ):
    if not manifest:
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

    result = self.__retryFunction( 2, JobState.__db.tq.insertJob, ( self.__jid, jobReqDict, jobPriority ) )
    if not result[ 'OK' ]:
      errMsg = result[ 'Message' ]
      # Force removing the job from the TQ if it was actually inserted
      result = JobState.__db.tq.deleteJob( self.__jid )
      if result['OK']:
        if result['Value']:
          gLogger.info( "Job %s removed from the TQ" % self.__jid )
      return S_ERROR( "Cannot insert in task queue: %s" % errMsg )
    return S_OK()



