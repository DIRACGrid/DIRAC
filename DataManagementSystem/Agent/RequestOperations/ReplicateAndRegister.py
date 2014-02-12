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
from DIRAC import S_OK, S_ERROR, gMonitor

from DIRAC.RequestManagementSystem.private.OperationHandlerBase                   import OperationHandlerBase
from DIRAC.DataManagementSystem.Client.FTSClient                                  import FTSClient
from DIRAC.Resources.Storage.StorageElement                                       import StorageElement
from DIRAC.DataManagementSystem.Agent.RequestOperations.DMSRequestOperationsBase  import DMSRequestOperationsBase
from DIRAC.Resources.Catalog.FileCatalog                        import FileCatalog
from DIRAC.Resources.Utilities                                  import Utils



########################################################################
class ReplicateAndRegister( OperationHandlerBase, DMSRequestOperationsBase ):
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

    replicas = self.fc.getReplicas( waitingFiles )
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

    ret = { "Valid" : [], "Banned" : [], "Bad" : [] }

    replicas = self.dm.getActiveReplicas( opFile.LFN )
    if not replicas["OK"]:
      self.log.error( replicas["Message"] )
    reNotExists = re.compile( "not such file or directory" )
    replicas = replicas["Value"]
    failed = replicas["Failed"].get( opFile.LFN , "" )
    if reNotExists.match( failed.lower() ):
      opFile.Status = "Failed"
      opFile.Error = failed
      return S_ERROR( failed )

    replicas = replicas["Successful"][opFile.LFN] if opFile.LFN in replicas["Successful"] else {}

    for repSEName in replicas:

      seRead = self.rssSEStatus( repSEName, "ReadAccess" )
      if not seRead["OK"]:
        self.log.info( seRead["Message"] )
        ret["Banned"].append( repSEName )
        continue
      if not seRead["Value"]:
        self.log.info( "StorageElement '%s' is banned for reading" % ( repSEName ) )

      repSE = self.seCache.get( repSEName, None )
      if not repSE:
        repSE = StorageElement( repSEName, "SRM2" )
        self.seCache[repSE] = repSE

      pfn = Utils.executeSingleFileOrDirWrapper( repSE.getPfnForLfn( opFile.LFN ) )
      if not pfn["OK"]:
        self.log.warn( "unable to create pfn for %s lfn: %s" % ( opFile.LFN, pfn["Message"] ) )
        ret["Banned"].append( repSEName )
        continue
      pfn = pfn["Value"]

      repSEMetadata = repSE.getFileMetadata( pfn, singleFile = True )
      if not repSEMetadata["OK"]:
        self.log.warn( repSEMetadata["Message"] )
        ret["Banned"].append( repSEName )
        continue
      repSEMetadata = repSEMetadata["Value"]

      seChecksum = repSEMetadata["Checksum"].replace( "x", "0" ).zfill( 8 ) if "Checksum" in repSEMetadata else None
      if opFile.Checksum and opFile.Checksum != seChecksum:
        self.log.warn( " %s checksum mismatch: %s %s:%s" % ( opFile.LFN,
                                                             opFile.Checksum,
                                                             repSE,
                                                             seChecksum ) )
        ret["Bad"].append( repSEName )
        continue
      # # if we're here repSE is OK
      ret["Valid"].append( repSEName )

    return S_OK( ret )

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
      gMonitor.addMark( "FTSScheduleAtt" )
      # # check replicas
      replicas = self._filterReplicas( opFile )
      if not replicas["OK"]:
        continue
      replicas = replicas["Value"]

      if not replicas["Valid"] and replicas["Banned"]:
        self.log.warn( "unable to schedule '%s', replicas only at banned SEs" % opFile.LFN )
        gMonitor.addMark( "FTSScheduleFail" )
        continue

      validReplicas = replicas["Valid"]
      bannedReplicas = replicas["Banned"]

      if not validReplicas and bannedReplicas:
        self.log.warn( "unable to schedule '%s', replicas only at banned SEs" % opFile.LFN )
        gMonitor.addMark( "FTSScheduleFail" )
        continue

      if validReplicas:
        validTargets = list( set( self.operation.targetSEList ) - set( validReplicas ) )
        if not validTargets:
          self.log.info( "file %s is already present at all targets" % opFile.LFN )
          opFile.Status = "Done"
          continue

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

      for fileID, reason in ftsSchedule["Failed"]:
        gMonitor.addMark( "FTSScheduleFail", 1 )
        for opFile in self.operation:
          if fileID == opFile.FileID:
            opFile.Error = reason
            self.log.error( "unable to schedule %s for FTS: %s" % ( opFile.LFN, opFile.Error ) )
    else:
      self.log.info( "No files to schedule after metadata checks" )

    return S_OK()

  def rmTransfer( self ):
    """ replicate and register using DataManager  """
    self.log.info( "transferring files using Data manager..." )
    # # source SE
    sourceSE = self.operation.SourceSE if self.operation.SourceSE else None
    if sourceSE:
      # # check source se for read
      sourceRead = self.rssSEStatus( sourceSE, "ReadAccess" )
      if not sourceRead["OK"]:
        self.log.info( sourceRead["Message"] )
        for opFile in self.operation:
          opFile.Error = sourceRead["Message"]
          opFile.Status = "Failed"
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

    # # get waiting files
    waitingFiles = self.getWaitingFilesList()
    # # loop over files
    for opFile in waitingFiles:

      gMonitor.addMark( "ReplicateAndRegisterAtt", 1 )
      lfn = opFile.LFN

      if not sourceSE:
        replicas = self._filterReplicas( opFile )
        if not replicas["OK"]:
          self.log.error( replicas["Message"] )
          continue
        replicas = replicas["Value"]
        if not replicas["Valid"]:
          self.log.warn( "unable to find valid replicas for %s" % lfn )
          continue
        # # get the first one in the list
        sourceSE = replicas["Valid"][0]

      # # loop over targetSE
      for targetSE in self.operation.targetSEList:

        # # call DataManager
        res = self.dm.replicateAndRegister( lfn, targetSE, sourceSE = sourceSE )

        if res["OK"]:

          if lfn in res["Value"]["Successful"]:

            if "replicate" in res["Value"]["Successful"][lfn]:

              repTime = res["Value"]["Successful"][lfn]["replicate"]
              self.log.info( "file %s replicated at %s in %s s." % ( lfn, targetSE, repTime ) )

              gMonitor.addMark( "ReplicateOK", 1 )

              if "register" in res["Value"]["Successful"][lfn]:

                gMonitor.addMark( "RegisterOK", 1 )
                regTime = res["Value"]["Successful"][lfn]["register"]
                self.log.info( "file %s registered at %s in %s s." % ( lfn, targetSE, regTime ) )

              else:

                gMonitor.addMark( "RegisterFail", 1 )
                self.log.info( "failed to register %s at %s." % ( lfn, targetSE ) )

                opFile.Error = "Failed to register"
                opFile.Status = "Failed"
                # # add register replica operation
                registerOperation = self.getRegisterOperation( opFile, targetSE )
                self.request.insertAfter( registerOperation, self.operation )

            else:

              self.log.info( "failed to replicate %s to %s." % ( lfn, targetSE ) )
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

        self.log.info( "file %s has been replicated to SE %s" % ( lfn, targetSE ) )

      if not opFile.Error:
        self.log.info( "file %s has been replicated to all targetSEs" % lfn )
        opFile.Status = "Done"


    return S_OK()
