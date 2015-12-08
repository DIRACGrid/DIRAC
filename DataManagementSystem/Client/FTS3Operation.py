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

from sqlalchemy import orm

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

  def __init__( self, ftsFiles = None, username = None, userGroup = None, rmsReqID = 0,
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

    self.creationTime = None
    self.lastUpdate = None
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

    opID = getattr( self, 'operationID' )
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
      # The file was never submitted
      if ftsFile.status == 'New':
        toSubmit.append( ftsFile )
      # The file failed from the point of view of FTS
      elif ftsFile.status == 'Failed':
        # If we don't make more attempts, put it in Defunct state
        if ftsFile.attempt >= maxAttemptsPerFile:
          ftsFile.status = 'Defunct'
        else:
          toSubmit.append( ftsFile )

    return toSubmit

      


  def _checkSEAccess( self, seName, accessType ):
    """Check the Status of a storage element
        :param seName: name of the StorageElement
        :param accessType ReadAccess, WriteAccess,CheckAccess,RemoveAccess

        :return S_ERROR if not allowed or error, S_OK() otherwise
    """
    # Check that the target is writable
    access = self.rssClient.getStorageElementStatus( seName, accessType )
    if not access["OK"]:
      return access

    if access["Value"][seName][accessType] not in ( "Active", "Degraded" ):
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
  

  def _upadteRmsOperationStatus( self ):
    """ Update the status of the Files in the rms operation
          :return: S_OK with a dict:
                        * request: rms Request object
                        * operation: rms Operation object
                        * ftsFilesByTarget: dict where keys are the SE, and values the list of ftsFiles that were successful
    """

    log = self._log.getSubLogger( "_upadteRmsOperationStatus/%s/%s" % ( getattr( self, 'operationID' ), self.rmsReqID ), child = True )



    res = self.reqClient.getRequest( self.rmsReqID )
    if not res['OK']:
      return res

    request = res['Value']

    operation = request.getWaiting()
    if not operation["OK"]:
      log.error( "Unable to find 'Scheduled' ReplicateAndRegister operation in request" )
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
        log.error( "File failed to transfer, setting it to failed in RMS", "%s %s" % ( ftsFile.lfn, ftsFile.targetSE ) )
        defunctRmsFileIDs.add( ftsFile.rmsFileID )
        continue
      
      if ftsFile.status == 'Canceled':
        log.info( "File canceled, setting it Failed in RMS", "%s %s" % ( ftsFile.lfn, ftsFile.targetSE ) )
        defunctRmsFileIDs.add( ftsFile.rmsFileID )
        continue

      # SHOULD NEVER HAPPEN !
      if ftsFile.status != 'Done':
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

    res = self._upadteRmsOperationStatus()

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
      registerOperation.Catalog = operation.Catalog

      targetSE = StorageElement( target )
      for ftsFile in ftsFileList:
        opFile = rmsFile()
        opFile.LFN = ftsFile.lfn
        opFile.LFN = ftsFile.checksum
        opFile.Size = ftsFile.size
        res = returnSingleResult( targetSE.getURL( ftsFile.LFN, protocol = 'srm' ) )

        # This should never happen !
        if not res["OK"]:
          log.error( "Could not get url", res['Message'] )
          continue
        opFile.PFN = res["Value"]
        registerOperation.addFile( opFile )

      request.insertBefore( registerOperation, operation )


    return self.reqClient.putRequest( request, useFailoverProxy = False, retryMainService = 3 )



class FTS3RemovalOperation( FTS3Operation ):
  """ Class to be used for a Removal operation
  """

  def prepareNewJobs( self, maxFilesPerJob = 100, maxAttemptsPerFile = 10 ):

    log = self._log.getSubLogger( "_prepareNewJobs", child = True )

    filesToSubmit = self._getFilesToSubmit( maxAttemptsPerFile = maxAttemptsPerFile )
    log.debug( "%s ftsFiles to submit" % len( filesToSubmit ) )


    newJobs = []


    # {targetSE : [FTS3Files] }
    filesGroupedByTarget = FTS3Utilities.groupFilesByTarget( filesToSubmit )

    for targetSE, ftsFiles in filesGroupedByTarget.iteritems():

      res = self._checkSEAccess( targetSE, 'RemoveAccess' )

      if not res['OK']:
        log.error( res )
        continue



      for ftsFilesChunk in breakListIntoChunks( ftsFiles, maxFilesPerJob ):

        newJob = self._createNewJob( 'Removal', ftsFilesChunk, targetSE )

        newJobs.append( newJob )


    return S_OK( newJobs )


  def _callback( self ):
    """" After a Removal operation, we have to update the matching Request in the
        RMS, and add the removal operation just before the RemoveReplica/RemoveFile one

        NOTE: we don't use ReqProxy when putting the request back to avoid operational hell
    """

    log = self._log.getSubLogger( "callback", child = True )

    res = self._upadteRmsOperationStatus()

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

      targetSE = StorageElement( target )
      for ftsFile in ftsFileList:
        opFile = rmsFile()
        opFile.LFN = ftsFile.lfn
        res = returnSingleResult( targetSE.getURL( ftsFile.LFN, protocol = 'srm' ) )

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

       



