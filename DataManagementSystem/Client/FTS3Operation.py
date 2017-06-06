import datetime
import json
from sqlalchemy import orm

from DIRAC.DataManagementSystem.Client.FTS3Job import FTS3Job
from DIRAC.DataManagementSystem.private import FTS3Utilities

from DIRAC.DataManagementSystem.Client.DataManager import DataManager
from DIRAC.FrameworkSystem.Client.Logger import gLogger

from DIRAC.Resources.Storage.StorageElement import StorageElement
from DIRAC.Core.Utilities.ReturnValues import returnSingleResult


from DIRAC import S_OK, S_ERROR

from DIRAC.Core.Utilities.List import breakListIntoChunks
from DIRAC.ResourceStatusSystem.Client.ResourceStatus import ResourceStatus
from DIRAC.DataManagementSystem.Client.FTS3File import FTS3File
from DIRAC.DataManagementSystem.private.FTS3Utilities import FTS3Serializable

from DIRAC.RequestManagementSystem.Client.ReqClient import ReqClient
from DIRAC.RequestManagementSystem.Client.Operation import Operation as rmsOperation
from DIRAC.RequestManagementSystem.Client.File import File as rmsFile


class FTS3Operation( FTS3Serializable ):
  """ Abstract class to represent an operation to be executed by FTS. It is a
      container for FTSFiles, as well as for FTSJobs.

      There can be a mapping between one FTS3Operation and one RMS Operation.

      The FTS3Operation takes care of generating the appropriate FTSJobs,
      and to perform a callback when the work with FTS is over. The actual
      generation and callback depends on the subclass.

      This class should not be instantiated directly, but rather one of its
      subclass
  """

  ALL_STATES = ['Active',  # Default state until FTS has done everything
                'Processed',  # Interactions with FTS done, but callback not done
                'Finished',  # Everything was done
                'Canceled',  # Canceled by the user
                'Failed',  # I don't know yet
               ]
  FINAL_STATES = ['Finished', 'Canceled', 'Failed' ]
  INIT_STATE = 'Active'

  _attrToSerialize = ['operationID', 'username', 'userGroup', 'rmsReqID', 'rmsOpID',
                      'sourceSEs','ftsFiles','activity','priority',
                      'ftsJobs', 'creationTime', 'lastUpdate', 'error', 'status']

  def __init__( self, ftsFiles = None, username = None, userGroup = None, rmsReqID = -1,
                rmsOpID = 0, sourceSEs = None, activity = None, priority = None ):

    """
        :param ftsFiles: list of FTS3Files object that belongs to the operation
        :param username: username whose proxy should be used
        :param userGroup: group that should be used with username
        :param rmsReqID: ID of the Request in the RMS system
        :param rmsOpID: ID of the Operation in the RMS system
        :param sourceSEs: list of SE to be used as source (if applicable)
        :param activity: FTS activity to use
        :param priority: FTS priority to use

    """
    ############################
    # persistent attributes

    self.username = username
    self.userGroup = userGroup

    self.rmsReqID = rmsReqID
    self.rmsOpID = rmsOpID

    if isinstance(sourceSEs, list):
      sourceSEs = ','.join( sourceSEs )

    self.sourceSEs = sourceSEs

    self.ftsFiles = ftsFiles if ftsFiles else []

    self.activity = activity
    self.priority = priority

    self.ftsJobs = []

    now = datetime.datetime.utcnow().replace( microsecond = 0 )

    self.creationTime = now
    self.lastUpdate = now
    self.error = None
    self.status = FTS3Operation.INIT_STATE



    ########################

    self.reqClient = None
    self.dManager = None
    self._log = None
    self.init_on_load()



  @orm.reconstructor
  def init_on_load( self ):
    """ This method initializes some attributes.
        It is called by sqlalchemy (which does not call __init__)
    """
    self.dManager = DataManager()

    self.rssClient = ResourceStatus()

    opID = getattr( self, 'operationID', None )
    loggerName = '%s/' % opID if opID else ''
    loggerName += 'req_%s/op_%s' % (self.rmsReqID, self.rmsOpID )

    self._log = gLogger.getSubLogger( loggerName , True )


  def isTotallyProcessed( self ):
    """ Returns True if and only if there is nothing
        else to be done by FTS for this operation.
        All files are successful or definitely failed
    """

    if self.status == 'Processed':
      return True

    fileStatuses = set( [f.status for f in self.ftsFiles] )

    # If all the files are in a final state
    if fileStatuses <= set( FTS3File.FINAL_STATES ):
      self.status = 'Processed'
      return True

    return False


  def _getFilesToSubmit( self, maxAttemptsPerFile = 10 ):
    """ Return the list of FTS3files that can be submitted
        Either because they never were submitted, or because
        we can make more attempts

        :param maxAttemptsPerFile: the maximum number of attempts to be tried for a file

        :return List of FTS3File to submit
    """

    toSubmit = []

    for ftsFile in self.ftsFiles:
      if ftsFile.attempt >= maxAttemptsPerFile:
        ftsFile.status = 'Defunct'
      # The file was never submitted or
      # The file failed from the point of view of FTS
      # but no more than the maxAttemptsPerFile
      elif ftsFile.status in ('New', 'Failed'):
        toSubmit.append( ftsFile )

    return toSubmit



  @staticmethod
  def _checkSEAccess( seName, accessType ):
    """Check the Status of a storage element
        :param seName: name of the StorageElement
        :param accessType ReadAccess, WriteAccess,CheckAccess,RemoveAccess

        :return S_ERROR if not allowed or error, S_OK() otherwise
    """
    # Check that the target is writable
#     access = self.rssClient.getStorageElementStatus( seName, accessType )
#     if not access["OK"]:
#       return access
#     if access["Value"][seName][accessType] not in ( "Active", "Degraded" ):
#       return S_ERROR( "%s does not have %s in Active or Degraded" % ( seName, accessType ) )


    status = StorageElement( seName ).getStatus()
    if not status['OK']:
      return status

    status = status['Value']

    accessType = accessType.replace( 'Access', '' )
    if not status[accessType]:
      return S_ERROR( "%s does not have %s in Active or Degraded" % ( seName, accessType ) )


    return S_OK()

  def _createNewJob( self, jobType, ftsFiles, targetSE, sourceSE = None ):
    """ Create a new FTS3Job object
        :param jobType: type of job to create (Transfer, Staging, Removal)
        :param ftsFiles: list of FTS3File objects the job has to work on
        :param targetSE: SE on which to operate
        :param sourceSE: source SE, only useful for Transfer jobs

        :return FTS3Job object
     """

    newJob = FTS3Job()
    newJob.type = jobType
    newJob.sourceSE = sourceSE
    newJob.targetSE = targetSE
    newJob.activity = self.activity
    newJob.priority = self.priority
    newJob.username = self.username
    newJob.userGroup = self.userGroup
    newJob.filesToSubmit = ftsFiles
    newJob.operationID = getattr( self, 'operationID' )

    return newJob


  def _callback( self ):
    """Actually performs the callback
    """
    raise NotImplementedError( "You should not be using the base class" )

  def callback( self ):
    """ Trigger the callback once all the FTS interactions are done
        and update the status of the Operation to 'Finished' if successful
    """
    self.reqClient = ReqClient()

    res = self._callback()

    if res['OK']:
      self.status = 'Finished'

    return res

  def prepareNewJobs( self, maxFilesPerJob = 100, maxAttemptsPerFile = 10 ):
    """ Prepare the new jobs that have to be submitted

        :param maxFilesPerJob: maximum number of files assigned to a job
        :param maxAttemptsPerFile: maximum number of retry after an fts failure

        :return list of jobs
    """
    raise NotImplementedError( "You should not be using the base class" )


  def _updateRmsOperationStatus( self ):
    """ Update the status of the Files in the rms operation
          :return: S_OK with a dict:
                        * request: rms Request object
                        * operation: rms Operation object
                        * ftsFilesByTarget: dict where keys are the SE, and values the list of ftsFiles that were successful
    """

    log = self._log.getSubLogger( "_updateRmsOperationStatus/%s/%s" % ( getattr( self, 'operationID' ), self.rmsReqID ), child = True )



    res = self.reqClient.getRequest( self.rmsReqID )
    if not res['OK']:
      return res

    request = res['Value']

    res = request.getWaiting()

    if not res["OK"]:
      log.error( "Unable to find 'Scheduled' operation in request" )
      res = self.reqClient.putRequest( request, useFailoverProxy = False, retryMainService = 3 )
      if not res['OK']:
        log.error( "Could not put back the request !", res['Message'] )
      return S_ERROR( "Could not find scheduled operation" )

    operation = res['Value']

    # We index the files of the operation by their IDs
    rmsFileIDs = {}

    for opFile in operation:
      rmsFileIDs[opFile.FileID] = opFile


    # Files that failed to transfer
    defunctRmsFileIDs = set()

    # { SE : [FTS3Files] }
    ftsFilesByTarget = {}
    for ftsFile in self.ftsFiles:

      if ftsFile.status == 'Defunct':
        log.info( "File failed to transfer, setting it to failed in RMS", "%s %s" % ( ftsFile.lfn, ftsFile.targetSE ) )
        defunctRmsFileIDs.add( ftsFile.rmsFileID )
        continue

      if ftsFile.status == 'Canceled':
        log.info( "File canceled, setting it Failed in RMS", "%s %s" % ( ftsFile.lfn, ftsFile.targetSE ) )
        defunctRmsFileIDs.add( ftsFile.rmsFileID )
        continue

      # SHOULD NEVER HAPPEN !
      if ftsFile.status != 'Finished':
        log.error( "Callback called with file in non terminal state", "%s %s" % ( ftsFile.lfn, ftsFile.targetSE ) )
        res = self.reqClient.putRequest( request, useFailoverProxy = False, retryMainService = 3 )
        if not res['OK']:
          log.error( "Could not put back the request !", res['Message'] )
        return S_ERROR( "Callback called with file in non terminal state" )


      ftsFilesByTarget.setdefault( ftsFile.targetSE, [] ).append( ftsFile )


    # Now, we set the rmsFile as done in the operation, providing
    # that they are not in the defunctFiles.
    # We cannot do this in the previous list because in the FTS system,
    # each destination is a separate line in the DB but not in the RMS

    for ftsFile in self.ftsFiles:
      opFile = rmsFileIDs[ftsFile.rmsFileID]

      opFile.Status = 'Failed' if ftsFile.rmsFileID in defunctRmsFileIDs else 'Done'


    return S_OK( { 'request' : request, 'operation' : operation, 'ftsFilesByTarget' : ftsFilesByTarget} )


  @classmethod
  def fromRMSObjects( cls, rmsReq, rmsOp, username ):
    """ Construct an FTS3Operation object from the RMS Request and Operation corresponding.
        The attributes taken are the OwnerGroup, Request and Operation IDS, sourceSE,
        and activity and priority if they are defined in the Argument field of the operation
        :param rmsReq: RMS Request object
        :param rmsOp: RMS Operation object
        :param username: username to which associate the FTS3Operation (normally comes from the Req OwnerDN)

        :returns: FTS3Operation object
    """

    ftsOp = cls()
    ftsOp.username = username
    ftsOp.userGroup = rmsReq.OwnerGroup

    ftsOp.rmsReqID = rmsReq.RequestID
    ftsOp.rmsOpID = rmsOp.OperationID


    ftsOp.sourceSEs = rmsOp.SourceSE

    try:
      argumentDic = json.loads( rmsOp.Arguments )

      ftsOp.activity = argumentDic['activity']
      ftsOp.priority = argumentDic['priority']
    except Exception as _e:
      pass

    return ftsOp



class FTS3TransferOperation(FTS3Operation):
  """ Class to be used for a Replication operation
  """



  def prepareNewJobs( self, maxFilesPerJob = 100, maxAttemptsPerFile = 10 ):

    log = self._log.getSubLogger( "_prepareNewJobs", child = True )

    filesToSubmit = self._getFilesToSubmit( maxAttemptsPerFile = maxAttemptsPerFile )
    log.debug( "%s ftsFiles to submit" % len( filesToSubmit ) )


    newJobs = []

    # {targetSE : [FTS3Files] }
    res = FTS3Utilities.groupFilesByTarget( filesToSubmit )
    if not res['OK']:
      return res
    filesGroupedByTarget = res['Value']


    for targetSE, ftsFiles in filesGroupedByTarget.iteritems():

      res = self._checkSEAccess( targetSE, 'WriteAccess' )

      if not res['OK']:
        log.error( res )
        for ftsFile in ftsFiles:
          ftsFile.attempt += 1
        continue


      sourceSEs = self.sourceSEs.split( ',' ) if self.sourceSEs is not None else []
      # { sourceSE : [FTSFiles] }
      res = FTS3Utilities.generatePossibleTransfersBySources( ftsFiles, allowedSources = sourceSEs )

      if not res['OK']:
        return res

      possibleTransfersBySource = res['Value']

      # Pick a unique source for each transfer
      res = FTS3Utilities.selectUniqueSourceforTransfers( possibleTransfersBySource )
      if not res['OK']:
        return res

      uniqueTransfersBySource = res['Value']

      # We don't need to check the source, since it is already filtered by the DataManager
      for sourceSE, ftsFiles in uniqueTransfersBySource.iteritems():


        for ftsFilesChunk in breakListIntoChunks( ftsFiles, maxFilesPerJob ):

          newJob = self._createNewJob( 'Transfer', ftsFilesChunk, targetSE, sourceSE = sourceSE )

          newJobs.append( newJob )


    return S_OK( newJobs )


  def _callback( self ):
    """" After a Transfer operation, we have to update the matching Request in the
        RMS, and add the registration operation just before the ReplicateAndRegister one

        NOTE: we don't use ReqProxy when putting the request back to avoid operational hell
    """

    log = self._log.getSubLogger( "callback", child = True )

    # In case there is no Request associated to the Transfer
    # we do not do the callback. Not really advised, but there is a feature
    # request to use the FTS3 system without RMS
    if self.rmsReqID == -1 :
      return S_OK()

    # Now we check the status of the Request.
    # If it is not scheduled, we do not perform the callback

    res = self.reqClient.getRequestStatus( self.rmsReqID )
    if not res['OK']:
      log.error("Could not get request status", res)
      return res
    status = res['Value']
    if status != 'Scheduled':
      return S_ERROR( "Request with id %s is not Scheduled:%s" % ( self.rmsReqID, status ) )

    res = self._updateRmsOperationStatus()

    if not res['OK']:
      return res

    ftsFilesByTarget = res['Value']['ftsFilesByTarget']
    request = res['Value']['request']
    operation = res['Value']['operation']


    log.info( "will create %s 'RegisterReplica' operations" % len( ftsFilesByTarget ) )

    for target, ftsFileList in ftsFilesByTarget.iteritems():
      log.info( "creating 'RegisterReplica' operation for targetSE %s with %s files..." % ( target,
                                                                                            len( ftsFileList ) ) )
      registerOperation = rmsOperation()
      registerOperation.Type = "RegisterReplica"
      registerOperation.Status = "Waiting"
      registerOperation.TargetSE = target
      if operation.Catalog:
        registerOperation.Catalog = operation.Catalog

      targetSE = StorageElement( target )
      for ftsFile in ftsFileList:
        opFile = rmsFile()
        opFile.LFN = ftsFile.lfn
        opFile.Checksum = ftsFile.checksum
        # TODO: are we really ever going to change type... ?
        opFile.ChecksumType = 'ADLER32'
        opFile.Size = ftsFile.size
        res = returnSingleResult( targetSE.getURL( ftsFile.lfn, protocol = 'srm' ) )

        # This should never happen !
        if not res["OK"]:
          log.error( "Could not get url", res['Message'] )
          continue
        opFile.PFN = res["Value"]
        registerOperation.addFile( opFile )

      request.insertBefore( registerOperation, operation )


    return self.reqClient.putRequest( request, useFailoverProxy = False, retryMainService = 3 )



class FTS3StagingOperation( FTS3Operation ):
  """ Class to be used for a Staging operation
  """

  def prepareNewJobs( self, maxFilesPerJob = 100, maxAttemptsPerFile = 10 ):

    log = gLogger.getSubLogger( "_prepareNewJobs", child = True )

    filesToSubmit = self._getFilesToSubmit( maxAttemptsPerFile = maxAttemptsPerFile )
    log.debug( "%s ftsFiles to submit" % len( filesToSubmit ) )


    newJobs = []


    # {targetSE : [FTS3Files] }
    filesGroupedByTarget = FTS3Utilities.groupFilesByTarget( filesToSubmit )

    for targetSE, ftsFiles in filesGroupedByTarget.iteritems():

      res = self._checkSEAccess( targetSE, 'ReadAccess' )

      if not res['OK']:
        log.error( res )
        continue


      for ftsFilesChunk in breakListIntoChunks( ftsFiles, maxFilesPerJob ):

        newJob = self._createNewJob( 'Staging', ftsFilesChunk, targetSE, sourceSE = targetSE )

        newJobs.append( newJob )


    return S_OK( newJobs )


  def _callback( self ):
    """" After a Staging operation, we have to update the matching Request in the
        RMS, and nothing more. If a callback is to be performed, it will be the next
        operation in the request, and put by the caller

        NOTE: we don't use ReqProxy when putting the request back to avoid operational hell
    """

    res = self._updateRmsOperationStatus()

    if not res['OK']:
      return res

    request = res['Value']['request']


    return self.reqClient.putRequest( request, useFailoverProxy = False, retryMainService = 3 )
