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
from DIRAC.Core.Utilities.Adler import compareAdler

from DIRAC.DataManagementSystem.Client.DataManager                                import DataManager
from DIRAC.DataManagementSystem.Agent.RequestOperations.DMSRequestOperationsBase  import DMSRequestOperationsBase

from DIRAC.Resources.Storage.StorageElement                                       import StorageElement
from DIRAC.Resources.Catalog.FileCatalog                                          import FileCatalog

def filterReplicas( opFile, logger = None, dataManager = None ):
  """ filter out banned/invalid source SEs """

  if logger is None:
    logger = gLogger
  if dataManager is None:
    dataManager = DataManager()

  log = logger.getSubLogger( "filterReplicas" )
  ret = { "Valid" : [], "NoMetadata" : [], "Bad" : [], 'NoReplicas':[], 'NoPFN':[] }

  replicas = dataManager.getActiveReplicas( opFile.LFN )
  if not replicas["OK"]:
    log.error( 'Failed to get active replicas', replicas["Message"] )
    return replicas
  reNotExists = re.compile( r".*such file.*" )
  replicas = replicas["Value"]
  failed = replicas["Failed"].get( opFile.LFN , "" )
  if reNotExists.match( failed.lower() ):
    opFile.Status = "Failed"
    opFile.Error = failed
    return S_ERROR( failed )

  replicas = replicas["Successful"].get( opFile.LFN, {} )

  if not opFile.Checksum:
    # Set Checksum to FC checksum if not set in the request
    fcMetadata = FileCatalog().getFileMetadata( opFile.LFN )
    fcChecksum = fcMetadata.get( 'Value', {} ).get( 'Successful', {} ).get( opFile.LFN, {} ).get( 'Checksum', '' )
    # Replace opFile.Checksum if it doesn't match a valid FC checksum
    if fcChecksum:
      opFile.Checksum = fcChecksum
      opFile.ChecksumType = fcMetadata['Value']['Successful'][opFile.LFN].get( 'ChecksumType', 'Adler32' )

  for repSEName in replicas:

    repSE = StorageElement( repSEName )


    repSEMetadata = repSE.getFileMetadata( opFile.LFN )
    error = repSEMetadata.get( 'Message', repSEMetadata.get( 'Value', {} ).get( 'Failed', {} ).get( opFile.LFN ) )
    if error:
      log.warn( 'unable to get metadata at %s for %s' % ( repSEName, opFile.LFN ), error.replace( '\n', '' ) )
      if 'File does not exist' in error:
        ret['NoReplicas'].append( repSEName )
      else:
        ret["NoMetadata"].append( repSEName )
    else:
      repSEMetadata = repSEMetadata['Value']['Successful'][opFile.LFN]

      seChecksum = repSEMetadata.get( "Checksum" )
      if ( opFile.Checksum and seChecksum and compareAdler( seChecksum, opFile.Checksum ) ) or\
         ( not opFile.Checksum and not seChecksum ):
        # # All checksums are OK
        ret["Valid"].append( repSEName )
      else:
        log.warn( " %s checksum mismatch, FC: '%s' @%s: '%s'" % ( opFile.LFN,
                                                              opFile.Checksum,
                                                              repSEName,
                                                              seChecksum ) )
        ret["Bad"].append( repSEName )

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

    # Clients
    self.fc = FileCatalog()
    if hasattr( self, "FTSMode" ) and getattr( self, "FTSMode" ):
      from DIRAC.DataManagementSystem.Client.FTSClient import FTSClient
      self.ftsClient = FTSClient()

  def __call__( self ):
    """ call me maybe """
    # # check replicas first
    checkReplicas = self.__checkReplicas()
    if not checkReplicas["OK"]:
      self.log.error( 'Failed to check replicas', checkReplicas["Message"] )
    if hasattr( self, "FTSMode" ) and getattr( self, "FTSMode" ):
      bannedGroups = getattr( self, "FTSBannedGroups" ) if hasattr( self, "FTSBannedGroups" ) else ()
      if self.request.OwnerGroup in bannedGroups:
        self.log.verbose( "usage of FTS system is banned for request's owner" )
        return self.dmTransfer()
      return self.ftsTransfer()
    return self.dmTransfer()

  def __checkReplicas( self ):
    """ check done replicas and update file states  """
    waitingFiles = dict( [ ( opFile.LFN, opFile ) for opFile in self.operation
                          if opFile.Status in ( "Waiting", "Scheduled" ) ] )
    targetSESet = set( self.operation.targetSEList )

    replicas = self.fc.getReplicas( waitingFiles.keys() )
    if not replicas["OK"]:
      self.log.error( 'Failed to get replicas', replicas["Message"] )
      return replicas

    reMissing = re.compile( r".*such file.*" )
    for failedLFN, errStr in replicas["Value"]["Failed"].items():
      waitingFiles[failedLFN].Error = errStr
      if reMissing.search( errStr.lower() ):
        self.log.error( "File does not exists", failedLFN )
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
      opFileToSchedule.ChecksumType = metadata[lfnsToSchedule]['ChecksumType']
      opFileToSchedule.Size = metadata[lfnsToSchedule]['Size']

      filesToScheduleList.append( ( opFileToSchedule.toJSON()['Value'],
                                    toSchedule[lfnsToSchedule][1],
                                    toSchedule[lfnsToSchedule][2] ) )

    return S_OK( filesToScheduleList )



  def _filterReplicas( self, opFile ):
    """ filter out banned/invalid source SEs """
    return filterReplicas( opFile, logger = self.log, dataManager = self.dm )

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
      noMetaReplicas = replicas["NoMetadata"]
      noReplicas = replicas['NoReplicas']
      badReplicas = replicas['Bad']
      noPFN = replicas['NoPFN']

      if validReplicas:
        validTargets = list( set( self.operation.targetSEList ) - set( validReplicas ) )
        if not validTargets:
          self.log.info( "file %s is already present at all targets" % opFile.LFN )
          opFile.Status = "Done"
        else:
          toSchedule[opFile.LFN] = [ opFile, validReplicas, validTargets ]
      else:
        gMonitor.addMark( "FTSScheduleFail" )
        if noMetaReplicas:
          self.log.warn( "unable to schedule '%s', couldn't get metadata at %s" % ( opFile.LFN, ','.join( noMetaReplicas ) ) )
          opFile.Error = "Couldn't get metadata"
        elif noReplicas:
          self.log.error( "Unable to schedule transfer",
                          "File %s doesn't exist at %s" % ( opFile.LFN, ','.join( noReplicas ) ) )
          opFile.Error = 'No replicas found'
          opFile.Status = 'Failed'
        elif badReplicas:
          self.log.error( "Unable to schedule transfer",
                          "File %s, all replicas have a bad checksum at %s" % ( opFile.LFN, ','.join( badReplicas ) ) )
          opFile.Error = 'All replicas have a bad checksum'
          opFile.Status = 'Failed'
        elif noPFN:
          self.log.warn( "unable to schedule %s, could not get a PFN at %s" % ( opFile.LFN, ','.join( noPFN ) ) )

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
        self.log.error( "Completely failed to schedule to FTS:", ftsSchedule["Message"] )
        return ftsSchedule

      # might have nothing to schedule
      ftsSchedule = ftsSchedule["Value"]
      if not ftsSchedule:
        return S_OK()

      self.log.info( "%d files have been scheduled to FTS" % len( ftsSchedule['Successful'] ) )
      for opFile in self.operation:
        fileID = opFile.FileID
        if fileID in ftsSchedule["Successful"]:
          gMonitor.addMark( "FTSScheduleOK", 1 )
          opFile.Status = "Scheduled"
          self.log.debug( "%s has been scheduled for FTS" % opFile.LFN )
        elif fileID in ftsSchedule["Failed"]:
          gMonitor.addMark( "FTSScheduleFail", 1 )
          opFile.Error = ftsSchedule["Failed"][fileID]
          if 'sourceSURL equals to targetSURL' in opFile.Error:
            # In this case there is no need to continue
            opFile.Status = 'Failed'
          self.log.warn( "unable to schedule %s for FTS: %s" % ( opFile.LFN, opFile.Error ) )
    else:
      self.log.info( "No files to schedule after metadata checks" )

    # Just in case some transfers could not be scheduled, try them with RM
    return self.dmTransfer( fromFTS = True )

  def dmTransfer( self, fromFTS = False ):
    """ replicate and register using dataManager  """
    # # get waiting files. If none just return
    # # source SE
    sourceSE = self.operation.SourceSE if self.operation.SourceSE else None
    if sourceSE:
      # # check source se for read
      bannedSource = self.checkSEsRSS( sourceSE, 'ReadAccess' )
      if not bannedSource["OK"]:
        gMonitor.addMark( "ReplicateAndRegisterAtt", len( self.operation ) )
        gMonitor.addMark( "ReplicateFail", len( self.operation ) )
        return bannedSource

      if bannedSource["Value"]:
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
      self.operation.Error = "%s targets are banned for writing" % ",".join( bannedTargets['Value'] )
      return S_OK( self.operation.Error )

    # Can continue now
    self.log.verbose( "No targets banned for writing" )

    waitingFiles = self.getWaitingFilesList()
    if not waitingFiles:
      return S_OK()
    # # loop over files
    if fromFTS:
      self.log.info( "Trying transfer using replica manager as FTS failed" )
    else:
      self.log.info( "Transferring files using Data manager..." )
    for opFile in waitingFiles:

      gMonitor.addMark( "ReplicateAndRegisterAtt", 1 )
      opFile.Error = ''
      lfn = opFile.LFN

      # Check if replica is at the specified source
      replicas = self._filterReplicas( opFile )
      if not replicas["OK"]:
        self.log.error( 'Failed to check replicas', replicas["Message"] )
        continue
      replicas = replicas["Value"]
      validReplicas = replicas["Valid"]
      noMetaReplicas = replicas["NoMetadata"]
      noReplicas = replicas['NoReplicas']
      badReplicas = replicas['Bad']
      noPFN = replicas['NoPFN']

      if not validReplicas:
        gMonitor.addMark( "ReplicateFail" )
        if noMetaReplicas:
          self.log.warn( "unable to replicate '%s', couldn't get metadata at %s" % ( opFile.LFN, ','.join( noMetaReplicas ) ) )
          opFile.Error = "Couldn't get metadata"
        elif noReplicas:
          self.log.error( "Unable to replicate", "File %s doesn't exist at %s" % ( opFile.LFN, ','.join( noReplicas ) ) )
          opFile.Error = 'No replicas found'
          opFile.Status = 'Failed'
        elif badReplicas:
          self.log.error( "Unable to replicate", "%s, all replicas have a bad checksum at %s" % ( opFile.LFN, ','.join( badReplicas ) ) )
          opFile.Error = 'All replicas have a bad checksum'
          opFile.Status = 'Failed'
        elif noPFN:
          self.log.warn( "unable to replicate %s, could not get a PFN" % opFile.LFN )
        continue
      # # get the first one in the list
      if sourceSE not in validReplicas:
        if sourceSE:
          self.log.warn( "%s is not at specified sourceSE %s, changed to %s" % ( lfn, sourceSE, validReplicas[0] ) )
        sourceSE = validReplicas[0]

      # # loop over targetSE
      catalogs = self.operation.Catalog
      if catalogs:
        catalogs = [ cat.strip() for cat in catalogs.split( ',' ) ]

      for targetSE in self.operation.targetSEList:

        # # call DataManager
        if targetSE in validReplicas:
          self.log.warn( "Request to replicate %s to an existing location: %s" % ( lfn, targetSE ) )
          opFile.Status = 'Done'
          continue
        res = self.dm.replicateAndRegister( lfn, targetSE, sourceSE = sourceSE, catalog = catalogs )
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
                registerOperation = self.getRegisterOperation( opFile, targetSE, type = 'RegisterReplica' )
                self.request.insertAfter( registerOperation, self.operation )

            else:

              self.log.error( "Failed to replicate", "%s to %s" % ( lfn, targetSE ) )
              gMonitor.addMark( "ReplicateFail", 1 )
              opFile.Error = "Failed to replicate"

          else:

            gMonitor.addMark( "ReplicateFail", 1 )
            reason = res["Value"]["Failed"][lfn]
            self.log.error( "Failed to replicate and register", "File %s at %s:" % ( lfn, targetSE ), reason )
            opFile.Error = reason

        else:

          gMonitor.addMark( "ReplicateFail", 1 )
          opFile.Error = "DataManager error: %s" % res["Message"]
          self.log.error( "DataManager error", res["Message"] )

      if not opFile.Error:
        if len( self.operation.targetSEList ) > 1:
          self.log.info( "file %s has been replicated to all targetSEs" % lfn )
        opFile.Status = "Done"


    return S_OK()
