########################################################################
# $HeadURL $
# File: ReplicateAndRegister.py
# Author: Krzysztof.Ciba@NOSPAMgmail.com
# Date: 2013/03/13 18:49:12
########################################################################
""" :mod: ReplicateAndRegister
    ==========================

    .. module: ReplicateAndRegister
    :synopsis: ReplicateAndRegister operation handler
    .. moduleauthor:: Krzysztof.Ciba@NOSPAMgmail.com

    ReplicateAndRegister operation handler
"""
__RCSID__ = "$Id $"
# #
# @file ReplicateAndRegister.py
# @author Krzysztof.Ciba@NOSPAMgmail.com
# @date 2013/03/13 18:49:28
# @brief Definition of ReplicateAndRegister class.

# # imports
import re
# # from DIRAC
from DIRAC import S_OK, S_ERROR, gMonitor, gLogger

from DIRAC.DataManagementSystem.Client.FTSClient                                  import FTSClient
from DIRAC.DataManagementSystem.Client.DataManager                                import DataManager
from DIRAC.DataManagementSystem.Agent.RequestOperations.DMSRequestOperationsBase  import DMSRequestOperationsBase

from DIRAC.Resources.Storage.StorageElement                                       import StorageElement
from DIRAC.Resources.Catalog.FileCatalog                                          import FileCatalog

def filterReplicas( opFile, logger = None, dataManager = None, seCache = None ):
  """ filter out banned/invalid source SEs """
  from DIRAC.Core.Utilities.Adler import compareAdler

  if not logger:
    logger = gLogger
  if not dataManager:
    dataManager = DataManager()
  if not seCache:
    seCache = {}

  log = logger.getSubLogger( "filterReplicas" )
  ret = { "Valid" : [], "Banned" : [], "Bad" : [], 'NoReplicas':[], 'NoPFN':[] }

  replicas = dataManager.getReplicas( opFile.LFN, allStatus = False )
  if not replicas["OK"]:
    log.error( replicas["Message"] )
    return replicas
  reNotExists = re.compile( "not such file or directory" )
  replicas = replicas["Value"]
  failed = replicas["Failed"].get( opFile.LFN , "" )
  if reNotExists.match( failed.lower() ):
    opFile.Status = "Failed"
    opFile.Error = failed
    return S_ERROR( failed )

  replicas = replicas["Successful"].get( opFile.LFN, {} )

  for repSEName in replicas:

    repSE = seCache[repSEName] if repSEName in seCache else \
            seCache.setdefault( repSEName, StorageElement( repSEName, "SRM2" ) )

    pfn = repSE.getPfnForLfn( opFile.LFN )
    if not pfn["OK"] or opFile.LFN not in pfn['Value']['Successful']:
      log.warn( "unable to create pfn for %s lfn at %s: %s" % ( opFile.LFN, repSEName, pfn.get( 'Message', pfn.get( 'Value', {} ).get( 'Failed', {} ).get( opFile.LFN ) ) ) )
      ret["NoPFN"].append( repSEName )
    else:
      pfn = pfn["Value"]['Successful'][ opFile.LFN ]

      repSEMetadata = repSE.getFileMetadata( pfn )
      error = repSEMetadata.get( 'Message', repSEMetadata.get( 'Value', {} ).get( 'Failed', {} ).get( pfn ) )
      if error:
        log.warn( 'unable to get metadata at %s for %s' % ( repSEName, opFile.LFN ), error )
        if 'File does not exist' in error:
          ret['NoReplicas'].append( repSEName )
        else:
          log.verbose( "StorageElement '%s' is banned for reading" % ( repSEName ) )
          ret["Banned"].append( repSEName )
      else:
        repSEMetadata = repSEMetadata['Value']['Successful'][pfn]

        seChecksum = repSEMetadata.get( "Checksum" )
        if opFile.Checksum and seChecksum and not compareAdler( seChecksum, opFile.Checksum ) :
          log.warn( " %s checksum mismatch: %s %s:%s" % ( opFile.LFN,
                                                               opFile.Checksum,
                                                               repSE,
                                                               seChecksum ) )
          ret["Bad"].append( repSEName )
        else:
          # # if we're here repSE is OK
          ret["Valid"].append( repSEName )

  return S_OK( ret )


########################################################################
class ReplicateAndRegister( DMSRequestOperationsBase ):
  """
  .. class:: ReplicateAndRegister

  ReplicateAndRegister operation handler
  """

  def __init__( self, operation = None, csPath = None ):
    """c'tor

    :param self: self reference
    :param Operation operation: Operation instance
    :param str csPath: CS path for this handler
    """
    super( ReplicateAndRegister, self ).__init__( operation, csPath )
    # # own gMonitor stuff for files
    gMonitor.registerActivity( "ReplicateAndRegisterAtt", "Replicate and register attempted",
                               "RequestExecutingAgent", "Files/min", gMonitor.OP_SUM )
    gMonitor.registerActivity( "ReplicateOK", "Replications successful",
                               "RequestExecutingAgent", "Files/min", gMonitor.OP_SUM )
    gMonitor.registerActivity( "ReplicateFail", "Replications failed",
                               "RequestExecutingAgent", "Files/min", gMonitor.OP_SUM )
    gMonitor.registerActivity( "RegisterOK", "Registrations successful",
                               "RequestExecutingAgent", "Files/min", gMonitor.OP_SUM )
    gMonitor.registerActivity( "RegisterFail", "Registrations failed",
                               "RequestExecutingAgent", "Files/min", gMonitor.OP_SUM )
    # # for FTS
    gMonitor.registerActivity( "FTSScheduleAtt", "Files schedule attempted",
                               "RequestExecutingAgent", "Files/min", gMonitor.OP_SUM )
    gMonitor.registerActivity( "FTSScheduleOK", "File schedule successful",
                               "RequestExecutingAgent", "Files/min", gMonitor.OP_SUM )
    gMonitor.registerActivity( "FTSScheduleFail", "File schedule failed",
                               "RequestExecutingAgent", "Files/min", gMonitor.OP_SUM )
    # # SE cache
    self.seCache = {}

    # Clients
    self.fc = FileCatalog()
    self.ftsClient = FTSClient()

  def __call__( self ):
    """ call me maybe """
    # # check replicas first
    checkReplicas = self.__checkReplicas()
    if not checkReplicas["OK"]:
      self.log.error( checkReplicas["Message"] )
    if hasattr( self, "FTSMode" ) and getattr( self, "FTSMode" ):
      bannedGroups = getattr( self, "FTSBannedGroups" ) if hasattr( self, "FTSBannedGroups" ) else ()
      if self.request.OwnerGroup in bannedGroups:
        self.log.info( "usage of FTS system is banned for request's owner" )
        return self.rmTransfer()
      return self.ftsTransfer()
    return self.rmTransfer()

  def __checkReplicas( self ):
    """ check done replicas and update file states  """
    waitingFiles = dict( [ ( opFile.LFN, opFile ) for opFile in self.operation
                          if opFile.Status in ( "Waiting", "Scheduled" ) ] )
    targetSESet = set( self.operation.targetSEList )

    replicas = self.fc.getReplicas( waitingFiles.keys() )
    if not replicas["OK"]:
      self.log.error( replicas["Message"] )
      return replicas

    reMissing = re.compile( "no such file or directory" )
    for failedLFN, errStr in replicas["Value"]["Failed"].items():
      waitingFiles[failedLFN].Error = errStr
      if reMissing.search( errStr.lower() ):
        self.log.error( "file %s does not exists" % failedLFN )
        gMonitor.addMark( "ReplicateFail", len( targetSESet ) )
        waitingFiles[failedLFN].Status = "Failed"

    for successfulLFN, reps in replicas["Value"]["Successful"].items():
      if targetSESet.issubset( set( reps ) ):
        self.log.info( "file %s has been replicated to all targets" % successfulLFN )
        waitingFiles[successfulLFN].Status = "Done"

    return S_OK()

  def _addMetadataToFiles( self, toSchedule ):
    """ Add metadata to those files that need to be scheduled through FTS

        toSchedule is a dictionary:
        {'lfn1': [opFile, validReplicas, validTargets], 'lfn2': [opFile, validReplicas, validTargets]}
    """
    if toSchedule:
      self.log.info( "found %s files to schedule, getting metadata from FC" % len( toSchedule ) )
      lfns = toSchedule.keys()
    else:
      self.log.info( "No files to schedule" )
      return S_OK()

    res = self.fc.getFileMetadata( lfns )
    if not res['OK']:
      return res
    else:
      if res['Value']['Failed']:
        self.log.warn( "Can't schedule %d files: problems getting the metadata: %s" % ( len( res['Value']['Failed'] ),
                                                                                ', '.join( res['Value']['Failed'] ) ) )
      metadata = res['Value']['Successful']

    filesToScheduleList = []

    for lfnsToSchedule, lfnMetadata in metadata.items():
      opFileToSchedule = toSchedule[lfnsToSchedule][0]
      opFileToSchedule.GUID = lfnMetadata['GUID']
      opFileToSchedule.Checksum = metadata[lfnsToSchedule]['Checksum']
      opFileToSchedule.ChecksumType = metadata[lfnsToSchedule]['CheckSumType']
      opFileToSchedule.Size = metadata[lfnsToSchedule]['Size']

      filesToScheduleList.append( ( opFileToSchedule.toJSON()['Value'],
                                    toSchedule[lfnsToSchedule][1],
                                    toSchedule[lfnsToSchedule][2] ) )

    return S_OK( filesToScheduleList )



  def _filterReplicas( self, opFile ):
    """ filter out banned/invalid source SEs """
    return filterReplicas( opFile, logger = self.log, dataManager = self.dm, seCache = self.seCache )

  def ftsTransfer( self ):
    """ replicate and register using FTS """

    self.log.info( "scheduling files in FTS..." )

    bannedTargets = self.checkSEsRSS()
    if not bannedTargets['OK']:
      gMonitor.addMark( "FTSScheduleAtt" )
      gMonitor.addMark( "FTSScheduleFail" )
      return bannedTargets

    if bannedTargets['Value']:
      return S_OK( "%s targets are banned for writing" % ",".join( bannedTargets['Value'] ) )

    # Can continue now
    self.log.verbose( "No targets banned for writing" )

    toSchedule = {}

    for opFile in self.getWaitingFilesList():
      opFile.Error = ''
      gMonitor.addMark( "FTSScheduleAtt" )
      # # check replicas
      replicas = self._filterReplicas( opFile )
      if not replicas["OK"]:
        continue
      replicas = replicas["Value"]

      validReplicas = replicas["Valid"]
      bannedReplicas = replicas["Banned"]
      noReplicas = replicas['NoReplicas']
      badReplicas = replicas['Bad']
      noPFN = replicas['NoPFN']

      if not validReplicas:
        gMonitor.addMark( "FTSScheduleFail" )
        if bannedReplicas:
          self.log.warn( "unable to schedule '%s', replicas only at banned SEs" % opFile.LFN )
        elif noReplicas:
          self.log.error( "unable to schedule %s, file doesn't exist" % opFile.LFN )
          opFile.Error = 'No replicas found'
          opFile.Status = 'Failed'
        elif badReplicas:
          self.log.error( "unable to schedule %s, all replicas have a bad checksum" % opFile.LFN )
          opFile.Error = 'All replicas have a bad checksum'
          opFile.Status = 'Failed'
        elif noPFN:
          self.log.warn( "unable to schedule %s, could not get a PFN" % opFile.LFN )

      else:
        validTargets = list( set( self.operation.targetSEList ) - set( validReplicas ) )
        if not validTargets:
          self.log.info( "file %s is already present at all targets" % opFile.LFN )
          opFile.Status = "Done"
        else:
          toSchedule[opFile.LFN] = [ opFile, validReplicas, validTargets ]

    res = self._addMetadataToFiles( toSchedule )
    if not res['OK']:
      return res
    else:
      filesToScheduleList = res['Value']


    if filesToScheduleList:

      ftsSchedule = self.ftsClient.ftsSchedule( self.request.RequestID,
                                                self.operation.OperationID,
                                                filesToScheduleList )
      if not ftsSchedule["OK"]:
        self.log.error( ftsSchedule["Message"] )
        return ftsSchedule

      # might have nothing to schedule
      ftsSchedule = ftsSchedule["Value"]
      if not ftsSchedule:
        return S_OK()

      for fileID in ftsSchedule["Successful"]:
        gMonitor.addMark( "FTSScheduleOK", 1 )
        for opFile in self.operation:
          if fileID == opFile.FileID:
            opFile.Status = "Scheduled"
            self.log.always( "%s has been scheduled for FTS" % opFile.LFN )

      for fileID in ftsSchedule["Failed"]:
        gMonitor.addMark( "FTSScheduleFail", 1 )
        for opFile in self.operation:
          if fileID == opFile.FileID:
            opFile.Error = ftsSchedule["Failed"][fileID]
            if 'sourceSURL equals to targetSURL' in opFile.Error:
              # In this case there is no need to continue
              opFile.Status = 'Failed'
            self.log.warn( "unable to schedule %s for FTS: %s" % ( opFile.LFN, opFile.Error ) )
    else:
      self.log.info( "No files to schedule after metadata checks" )

    # Just in case some transfers could not be scheduled, try them with RM
    return self.rmTransfer( fromFTS = True )

  def rmTransfer( self, fromFTS = False ):
    """ replicate and register using dataManager  """
    # # get waiting files. If none just return
    waitingFiles = self.getWaitingFilesList()
    if not waitingFiles:
      return S_OK()
    if fromFTS:
      self.log.info( "Trying transfer using replica manager as FTS failed" )
    else:
      self.log.info( "Transferring files using Data manager..." )
    # # source SE
    sourceSE = self.operation.SourceSE if self.operation.SourceSE else None
    if sourceSE:
      # # check source se for read
      sourceRead = self.rssSEStatus( sourceSE, "ReadAccess" )
      if not sourceRead["OK"]:
        self.log.info( sourceRead["Message"] )
        for opFile in self.operation:
          opFile.Error = sourceRead["Message"]
        self.operation.Error = sourceRead["Message"]
        gMonitor.addMark( "ReplicateAndRegisterAtt", len( self.operation ) )
        gMonitor.addMark( "ReplicateFail", len( self.operation ) )
        return sourceRead

      if not sourceRead["Value"]:
        self.operation.Error = "SourceSE %s is banned for reading" % sourceSE
        self.log.info( self.operation.Error )
        return S_OK( self.operation.Error )

    # # check targetSEs for write
    bannedTargets = self.checkSEsRSS()
    if not bannedTargets['OK']:
      gMonitor.addMark( "ReplicateAndRegisterAtt", len( self.operation ) )
      gMonitor.addMark( "ReplicateFail", len( self.operation ) )
      return bannedTargets

    if bannedTargets['Value']:
      return S_OK( "%s targets are banned for writing" % ",".join( bannedTargets['Value'] ) )

    # Can continue now
    self.log.verbose( "No targets banned for writing" )

    # # loop over files
    for opFile in waitingFiles:

      gMonitor.addMark( "ReplicateAndRegisterAtt", 1 )
      opFile.Error = ''
      lfn = opFile.LFN

      # Check if replica is at the specified source
      replicas = self._filterReplicas( opFile )
      if not replicas["OK"]:
        self.log.error( replicas["Message"] )
        continue
      replicas = replicas["Value"]
      if not replicas["Valid"]:
        self.log.warn( "unable to find valid replicas for %s" % lfn )
        continue
      # # get the first one in the list
      if sourceSE not in replicas['Valid']:
        if sourceSE:
          self.log.warn( "%s is not at specified sourceSE %s, changed to %s" % ( lfn, sourceSE, replicas["Valid"][0] ) )
        sourceSE = replicas["Valid"][0]

      # # loop over targetSE
      for targetSE in self.operation.targetSEList:

        # # call DataManager
        if targetSE == sourceSE:
          self.log.warn( "Request to replicate %s to the source SE: %s" % ( lfn, sourceSE ) )
          continue
        res = self.dm.replicateAndRegister( lfn, targetSE, sourceSE = sourceSE )
        if res["OK"]:

          if lfn in res["Value"]["Successful"]:

            if "replicate" in res["Value"]["Successful"][lfn]:

              repTime = res["Value"]["Successful"][lfn]["replicate"]
              prString = "file %s replicated at %s in %s s." % ( lfn, targetSE, repTime )

              gMonitor.addMark( "ReplicateOK", 1 )

              if "register" in res["Value"]["Successful"][lfn]:

                gMonitor.addMark( "RegisterOK", 1 )
                regTime = res["Value"]["Successful"][lfn]["register"]
                prString += ' and registered in %s s.' % regTime
                self.log.info( prString )
              else:

                gMonitor.addMark( "RegisterFail", 1 )
                prString += " but failed to register"
                self.log.warn( prString )

                opFile.Error = "Failed to register"
                # # add register replica operation
                registerOperation = self.getRegisterOperation( opFile, targetSE )
                self.request.insertAfter( registerOperation, self.operation )

            else:

              self.log.error( "failed to replicate %s to %s." % ( lfn, targetSE ) )
              gMonitor.addMark( "ReplicateFail", 1 )
              opFile.Error = "Failed to replicate"

          else:

            gMonitor.addMark( "ReplicateFail", 1 )
            reason = res["Value"]["Failed"][lfn]
            self.log.error( "failed to replicate and register file %s at %s: %s" % ( lfn, targetSE, reason ) )
            opFile.Error = reason

        else:

          gMonitor.addMark( "ReplicateFail", 1 )
          opFile.Error = "DataManager error: %s" % res["Message"]
          self.log.error( opFile.Error )

      if not opFile.Error:
        if len( self.operation.targetSEList ) > 1:
          self.log.info( "file %s has been replicated to all targetSEs" % lfn )
        opFile.Status = "Done"


    return S_OK()
