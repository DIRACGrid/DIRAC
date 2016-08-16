""" MoveReplica operation handler

    This handler moves replicas from source SEs to target SEs. Replicas are first replicated to target SEs and then removed from the source SEs
"""

__RCSID__ = "$Id $"

# # imports
import re, os, string
# # from DIRAC
from DIRAC import S_OK, S_ERROR, gLogger
from DIRAC.FrameworkSystem.Client.MonitoringClient import gMonitor
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
  noReplicas = False
  if not replicas:
    allReplicas = dataManager.getReplicas( opFile.LFN )
    if allReplicas['OK']:
      allReplicas = allReplicas['Value']['Successful'].get( opFile.LFN, {} )
      if not allReplicas:
        ret['NoReplicas'].append( None )
        noReplicas = True
      else:
        # We try inactive replicas to see if maybe the file doesn't exist at all
        replicas = allReplicas
      log.warn( "File has no%s replica in File Catalog" % ( '' if noReplicas else ' active' ), opFile.LFN )
    else:
      return allReplicas

  if not opFile.Checksum:
    # Set Checksum to FC checksum if not set in the request
    fcMetadata = FileCatalog().getFileMetadata( opFile.LFN )
    fcChecksum = fcMetadata.get( 'Value', {} ).get( 'Successful', {} ).get( opFile.LFN, {} ).get( 'Checksum' )
    # Replace opFile.Checksum if it doesn't match a valid FC checksum
    if fcChecksum:
      opFile.Checksum = fcChecksum
      opFile.ChecksumType = fcMetadata['Value']['Successful'][opFile.LFN].get( 'ChecksumType', 'Adler32' )

  for repSEName in replicas:
    repSEMetadata = StorageElement( repSEName ).getFileMetadata( opFile.LFN )
    error = repSEMetadata.get( 'Message', repSEMetadata.get( 'Value', {} ).get( 'Failed', {} ).get( opFile.LFN ) )
    if error:
      log.warn( 'unable to get metadata at %s for %s' % ( repSEName, opFile.LFN ), error.replace( '\n', '' ) )
      if 'File does not exist' in error:
        ret['NoReplicas'].append( repSEName )
      else:
        ret["NoMetadata"].append( repSEName )
    elif not noReplicas:
      repSEMetadata = repSEMetadata['Value']['Successful'][opFile.LFN]

      seChecksum = repSEMetadata.get( "Checksum" )
      if not seChecksum and opFile.Checksum:
        opFile.Checksum = None
        opFile.ChecksumType = None
      elif seChecksum and not opFile.Checksum:
        opFile.Checksum = seChecksum
      if not opFile.Checksum or not seChecksum or compareAdler( seChecksum, opFile.Checksum ):
        # # All checksums are OK
        ret["Valid"].append( repSEName )
      else:
        log.warn( " %s checksum mismatch, FC: '%s' @%s: '%s'" % ( opFile.LFN,
                                                              opFile.Checksum,
                                                              repSEName,
                                                              seChecksum ) )
        ret["Bad"].append( repSEName )
    else:
      # If a replica was found somewhere, don't set the file as no replicas
      ret['NoReplicas'] = []

  return S_OK( ret )

####
class MoveReplica( DMSRequestOperationsBase ):
  """
  .. class:: MoveReplica

  MoveReplica operation handler
  """

  def __init__( self, operation = None, csPath = None ):
    """c'tor

    :param self: self reference
    :param Operation operation: Operation instance
    :param str csPath: CS path for this handler
    """
    super( MoveReplica, self ).__init__( operation, csPath )
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
    gMonitor.registerActivity( "RemoveReplicaAtt", "Replica removals attempted",
                               "RequestExecutingAgent", "Files/min", gMonitor.OP_SUM )
    gMonitor.registerActivity( "RemoveReplicaOK", "Successful replica removals",
                               "RequestExecutingAgent", "Files/min", gMonitor.OP_SUM )
    gMonitor.registerActivity( "RemoveReplicaFail", "Failed replica removals",
                               "RequestExecutingAgent", "Files/min", gMonitor.OP_SUM )

    # Clients
    self.fc = FileCatalog()

  def __call__( self ):
    """ call me maybe """
    # # check replicas first
    checkReplicas = self.__checkReplicas()
    if not checkReplicas["OK"]:
      self.log.error( 'Failed to check replicas', checkReplicas["Message"] )

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

    # # check sourceSEs for removal
    # # for removal the targetSEs are the sourceSEs of the replication
    targetSEs = self.operation.sourceSEList
    bannedTargets = self.checkSEsRSS( targetSEs, access = 'RemoveAccess' )
    if not bannedTargets['OK']:
      gMonitor.addMark( "RemoveReplicaAtt" )
      gMonitor.addMark( "RemoveReplicaFail" )
      return bannedTargets

    if bannedTargets['Value']:
      return S_OK( "%s targets are banned for removal" % ",".join( bannedTargets['Value'] ) )

    # Can continue now
    self.log.verbose( "No targets banned for removal" )

    ## Do the transfer
    # # get waiting files. If none just return
    waitingFiles = self.getWaitingFilesList()
    if not waitingFiles:
      return S_OK()

    # # loop over files
    self.log.info( "Transferring files using Data manager..." )
    for opFile in waitingFiles:
      res = self.dmTransfer(opFile)
      if not res["OK"]:
        continue
      else:
        ## Do the replica removal
        self.log.info( "Removing files using Data manager..." )
        toRemoveDict = dict( [ ( opFile.LFN, opFile ) for opFile in waitingFiles ] )
        self.log.info( "todo: %s replicas to delete from %s sites" % ( len( toRemoveDict ), len( targetSEs ) ) )
        self.dmRemoval(toRemoveDict,targetSEs)

    return S_OK()

  def dmRemoval(self, toRemoveDict, targetSEs ):

    gMonitor.addMark( "RemoveReplicaAtt", len( toRemoveDict ) * len( targetSEs ) )
    # # keep status for each targetSE
    removalStatus = dict.fromkeys( toRemoveDict.keys(), None )
    for lfn in removalStatus:
      removalStatus[lfn] = dict.fromkeys( targetSEs, None )

    # # loop over targetSEs
    for targetSE in targetSEs:
      self.log.info( "removing replicas at %s" % targetSE )

      # # 1st step - bulk removal
      bulkRemoval = self.bulkRemoval( toRemoveDict, targetSE )
      if not bulkRemoval["OK"]:
        self.log.error( 'Bulk replica removal failed', bulkRemoval["Message"] )
        return bulkRemoval
      bulkRemoval = bulkRemoval["Value"]

      # # update removal status for successful files
      removalOK = [ opFile for opFile in bulkRemoval.values() if not opFile.Error ]

      for opFile in removalOK:
        removalStatus[opFile.LFN][targetSE] = ""
      gMonitor.addMark( "RemoveReplicaOK", len( removalOK ) )

      # # 2nd step - process the rest again
      toRetry = dict( [ ( lfn, opFile ) for lfn, opFile in bulkRemoval.items() if opFile.Error ] )
      for lfn, opFile in toRetry.items():
        self.singleRemoval( opFile, targetSE )
        if not opFile.Error:
          gMonitor.addMark( "RemoveReplicaOK", 1 )
          removalStatus[lfn][targetSE] = ""
        else:
          gMonitor.addMark( "RemoveReplicaFail", 1 )
          removalStatus[lfn][targetSE] = opFile.Error

    # # update file status for waiting files
    failed = 0
    for opFile in self.operation:
      if opFile.Status == "Waiting":
        errors = list( set( [ error for error in removalStatus[lfn].values() if error ] ) )
        if errors:
          opFile.Error = ",".join( errors )
          # This seems to be the only offending error
          if "Write access not permitted for this credential" in opFile.Error:
            failed += 1
            continue
        opFile.Status = "Done"

    if failed:
      self.operation.Error = "failed to remove %s replicas" % failed

    return S_OK(removalStatus)

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

    return S_OK()

  def _filterReplicas( self, opFile ):
    """ filter out banned/invalid source SEs """
    return filterReplicas( opFile, logger = self.log, dataManager = self.dm )

  def dmTransfer( self, opFile ):
    """ replicate and register using dataManager  """
    # # get waiting files. If none just return
    # # source SE
    sourceSE = self.operation.SourceSE if self.operation.SourceSE else None

    gMonitor.addMark( "ReplicateAndRegisterAtt", 1 )
    opFile.Error = ''
    lfn = opFile.LFN

    # Check if replica is at the specified source
    replicas = self._filterReplicas( opFile )
    if not replicas["OK"]:
      self.log.error( 'Failed to check replicas', replicas["Message"] )
      return S_ERROR()
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
      return S_ERROR()
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
          self.log.error( "Failed to replicate and register", "File %s at %s: %s" % ( lfn, targetSE , reason ))
          opFile.Error = reason
      else:
        gMonitor.addMark( "ReplicateFail", 1 )
        opFile.Error = "DataManager error: %s" % res["Message"]
        self.log.error( "DataManager error", res["Message"] )

    if not opFile.Error:
      if len( self.operation.targetSEList ) > 1:
        self.log.info( "file %s has been replicated to all targetSEs" % lfn )
    else:
      return S_ERROR("dmTransfer failed")

    return S_OK()

  def bulkRemoval( self, toRemoveDict, targetSE ):
    """ remove replicas :toRemoveDict: at :targetSE:

    :param dict toRemoveDict: { lfn: opFile, ... }
    :param str targetSE: target SE name
    :return: toRemoveDict with updated errors
    """
    removeReplicas = self.dm.removeReplica( targetSE, toRemoveDict.keys() )

    if not removeReplicas["OK"]:
      for opFile in toRemoveDict.values():
        opFile.Error = removeReplicas["Message"]
      return S_ERROR( removeReplicas["Message"] )
    removeReplicas = removeReplicas["Value"]
    # # filter out failed
    for lfn, opFile in toRemoveDict.items():
      if lfn in removeReplicas["Failed"]:
        opFile.Error = str( removeReplicas["Failed"][lfn] )
    return S_OK( toRemoveDict )

  def singleRemoval( self, opFile, targetSE ):
    """ remove opFile replica from targetSE

    :param File opFile: File instance
    :param str targetSE: target SE name
    """
    proxyFile = None
    if "Write access not permitted for this credential" in opFile.Error:
      # # not a DataManger? set status to failed and return
      if "DataManager" in self.shifter:
        # #  you're a data manager - save current proxy and get a new one for LFN and retry
        saveProxy = os.environ["X509_USER_PROXY"]
        try:
          fileProxy = self.getProxyForLFN( opFile.LFN )
          if not fileProxy["OK"]:
            opFile.Error = fileProxy["Message"]
          else:
            proxyFile = fileProxy["Value"]
            removeReplica = self.dm.removeReplica( targetSE, opFile.LFN )
            if not removeReplica["OK"]:
              opFile.Error = removeReplica["Message"]
            else:
              removeReplica = removeReplica["Value"]
              if opFile.LFN in removeReplica["Failed"]:
                opFile.Error = removeReplica["Failed"][opFile.LFN]
              else:
                # # reset error - replica has been removed this time
                opFile.Error = ""
        finally:
          if proxyFile:
            os.unlink( proxyFile )
          # # put back request owner proxy to env
          os.environ["X509_USER_PROXY"] = saveProxy
    return S_OK( opFile )
