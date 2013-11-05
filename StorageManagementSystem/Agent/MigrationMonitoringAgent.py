"""  MigrationMonitoringAgent monitors the migration status of newly uploaded files to ensure they are migrated correctly and timely.
"""

__RCSID__ = "$Id$"

from DIRAC                                                  import gLogger, gConfig, gMonitor, S_OK, S_ERROR, rootPath, siteName
from DIRAC.Core.Base.AgentModule                            import AgentModule
from DIRAC.DataManagementSystem.Client.ReplicaManager       import ReplicaManager
from DIRAC.DataManagementSystem.Client.DataIntegrityClient  import DataIntegrityClient
from DIRAC.DataManagementSystem.Client.DataLoggingClient    import DataLoggingClient
from DIRAC.AccountingSystem.Client.Types.DataOperation      import DataOperation
from DIRAC.AccountingSystem.Client.DataStoreClient          import gDataStoreClient
from DIRAC.Core.Utilities.List                              import sortList
from DIRAC.Core.Utilities.Adler                             import compareAdler
import time, os, datetime, re
from types import *

AGENT_NAME = 'StorageManagement/MigrationMonitoringAgent'

class MigrationMonitoringAgent( AgentModule ):

  def initialize( self ):
    self.ReplicaManager = ReplicaManager()
    self.DataLog = DataLoggingClient()
    self.DataIntegrityClient = DataIntegrityClient()
    if self.am_getOption( 'DirectDB', False ):
      from DIRAC.StorageManagementSystem.DB.MigrationMonitoringDB import MigrationMonitoringDB
      self.MigrationMonitoringDB = MigrationMonitoringDB()
    else:
      from DIRAC.StorageManagementSystem.Client.MigrationMonitoringClient import MigrationMonitoringClient
      self.MigrationMonitoringDB = MigrationMonitoringClient()

    # This sets the Default Proxy to used as that defined under 
    # /Operations/Shifter/DataManager
    # the shifterProxy option in the Configuration can be used to change this default.
    self.am_setOption( 'shifterProxy', 'DataManager' )

    self.userName = 'acsmith'
    self.storageElements = self.am_getOption( 'StorageElements', ['CERN-RAW'] )
    self.lastMonitors = {}

    gMonitor.registerActivity( "Iteration", "Agent Loops/min", "MigrationMonitoringAgent", "Loops", gMonitor.OP_SUM )
    if self.storageElements:
      gLogger.info( "Agent will be initialised to monitor the following SEs:" )
      for se in self.storageElements:
        gLogger.info( se )
        self.lastMonitors[se] = datetime.datetime.utcfromtimestamp( 0.0 )
        gMonitor.registerActivity( "Iteration%s" % se, "Agent Loops/min", "MigrationMonitoringAgent", "Loops", gMonitor.OP_SUM )
        gMonitor.registerActivity( "MigratingFiles%s" % se, "Files waiting for migration", "MigrationMonitoringAgent", "Files", gMonitor.OP_MEAN )
        gMonitor.registerActivity( "MigratedFiles%s" % se, "Newly migrated files", "MigrationMonitoringAgent", "Files", gMonitor.OP_SUM )
        gMonitor.registerActivity( "TotalMigratedFiles%s" % se, "Total migrated files", "MigrationMonitoringAgent", "Files", gMonitor.OP_ACUM )
        gMonitor.registerActivity( "TotalMigratedSize%s" % se, "Total migrated file size", "MigrationMonitoringAgent", "GB", gMonitor.OP_ACUM )
        gMonitor.registerActivity( "ChecksumMatches%s" % se, "Successfully migrated files", "MigrationMonitoringAgent", "Files", gMonitor.OP_SUM )
        gMonitor.registerActivity( "TotalChecksumMatches%s" % se, "Total successfully migrated files", "MigrationMonitoringAgent", "Files", gMonitor.OP_ACUM )
        gMonitor.registerActivity( "ChecksumMismatches%s" % se, "Erroneously migrated files", "MigrationMonitoringAgent", "Files", gMonitor.OP_SUM )
        gMonitor.registerActivity( "TotalChecksumMismatches%s" % se, "Total erroneously migrated files", "MigrationMonitoringAgent", "Files", gMonitor.OP_ACUM )
        gMonitor.registerActivity( "MigrationTime%s" % se, "Average migration time", "MigrationMonitoringAgent", "Seconds", gMonitor.OP_MEAN )
    return S_OK()

  def execute( self ):
    self.enableFlag = self.am_getOption( 'EnableFlag', 'True' )
    if not self.enableFlag == 'True':
      self.log.info( 'MigrationMonitoringAgent is disabled by configuration option EnableFlag' )
      return S_OK( 'Disabled via CS flag' )
    gMonitor.addMark( "Iteration", 1 )
    self.NewToMigrating()
    for se in self.storageElements:
      gMonitor.addMark( "Iteration%s" % se, 1 )
      self.MigratingToMigrated( se )
    return S_OK()

  #########################################################################################################
  #
  # Includes the file size and checksum information for replicas which do not have it
  #

  def NewToMigrating( self ):
    """ Obtain the new files from the migration monitoring db and (where necessary) add the size and checksum information
    """
    # First get the new files from the database
    gLogger.info( "NewToMigrating: Attempting to obtain 'New' files." )
    res = self.__getFiles( '', 'New' )
    if not res['OK']:
      gLogger.error( "NewToMigrating: Failed to get 'New' files.", res['Message'] )
      return res
    newFiles = res['Value']['Files']
    if not newFiles:
      gLogger.info( "NewToMigrating: Found no 'New' files." )
      return S_OK()
    # Get the metadata from the catalog for which do not have size or checksum
    res = self.__getCatalogFileMetadata( newFiles )
    if not res['OK']:
      gLogger.error( "NewToMigrating: Failed to get metadata for files", res['Message'] )
      return res
    metadata = res['Value']
    # Add the metadata to the migration monitoring DB.
    res = self.__updateNewMigrating( metadata )
    return S_OK()

  def __updateNewMigrating( self, fileMetadata ):
    gLogger.info( "__updateNewMigrating: Updating metadata for %s files" % len( fileMetadata ) )
    gLogger.info( "PUT THE CODE HERE TO UPDATE THE METDATA" )

    #self.__setMigratingReplicaStatus(fileMetadata.keys(),'Migrating')
    return S_OK()

  #########################################################################################################
  #
  # Monitors the migration of files
  #

  def MigratingToMigrated( self, se ):
    """ Obtain the active files from the migration monitoring db and check their status
    """
    # First get the migrating files from the database
    gLogger.info( "[%s] MigratingToMigrated: Attempting to obtain 'Migrating' files." % se )
    res = self.__getFiles( se, 'Migrating' )
    if not res['OK']:
      gLogger.error( "[%s] MigratingToMigrated: Failed to get 'Migrating' files." % se, res['Message'] )
      return res
    pfnIDs = res['Value']['PFNIDs']
    if not pfnIDs:
      gLogger.info( "[%s] MigratingToMigrated: Found no 'Migrating' files." % se )
      return S_OK()
    migratingFiles = res['Value']['Files']
    gLogger.info( "[%s] MigratingToMigrated: Found %d 'Migrating' files." % ( se, len( pfnIDs ) ) )
    gMonitor.addMark( "MigratingFiles%s" % se, len( pfnIDs ) )
    gLogger.info( "[%s] MigratingToMigrated: Obtaining physical file metadata for 'Migrating' files." % se )
    startTime = datetime.datetime.utcnow()
    res = self.__getMigratedFiles( se, pfnIDs.keys() )
    if not res['OK']:
      gLogger.error( "[%s] MigratingToMigrated: Failed to get 'Migrating' file metadata." % se, res['Message'] )
      return res
    assumedEndTime = datetime.datetime.utcnow() - ( ( datetime.datetime.utcnow() - startTime ) / 2 ) # Assumed that the files are found migrated midway through obtaining the metadata
    previousMonitorTime = self.lastMonitors[se]
    self.lastMonitors[se] = datetime.datetime.utcnow()
    terminal = res['Value']['Terminal']
    migrated = res['Value']['Migrated']

    # Update the problematic files in the integrity DB and update the MigrationMonitoringDB
    gLogger.info( "[%s] MigratingToMigrated: Found %d terminally failed files." % ( se, len( terminal ) ) )
    if terminal:
      replicaTuples = []
      terminalFileIDs = []
      for pfn, prognosis in terminal.items():
        fileID = pfnIDs[pfn]
        terminalFileIDs.append( fileID )
        lfn = migratingFiles[fileID]['LFN']
        se = migratingFiles[fileID]['SE']
        replicaTuples.append( ( lfn, pfn, se, prognosis ) )
      self.__reportProblematicReplicas( replicaTuples )
      res = self.MigrationMonitoringDB.setMigratingReplicaStatus( terminalFileIDs, 'Failed' )
      if not res['OK']:
        gLogger.error( "[%s] MigratingToMigrated: Failed to update terminal files." % se, res['Message'] )

    # Update the migrated files and send accounting
    gLogger.info( "[%s] MigratingToMigrated: Found %d migrated files." % ( se, len( migrated ) ) )
    if migrated:
      migratedFileIDs = {}
      for pfn, checksum in migrated.items():
        migratedFileIDs[pfnIDs[pfn]] = checksum
      #res = self.MigrationMonitoringDB.setMigratingReplicaStatus(migratedFileIDs.keys(),'Migrated')
      #if not res['OK']:
      #  gLogger.error("[%s] MigratingToMigrated: Failed to update migrated files." % se, res['Message'])
      # Check the checksums of the migrated files
      res = self.__validateChecksums( se, migratedFileIDs, migratingFiles )
      if not res['OK']:
        gLogger.error( "[%s] MigratingToMigrated: Failed to perform checksum matching." % se, res['Message'] )
        matchingFiles = []
        mismatchingFiles = []
      else:
        matchingFiles = res['Value']['MatchingFiles']
        mismatchingFiles = res['Value']['MismatchFiles']
      # Create and send the accounting messages
      res = self.__updateMigrationAccounting( se, migratingFiles, matchingFiles, mismatchingFiles, assumedEndTime, previousMonitorTime )
      if not res['OK']:
        gLogger.error( "[%s] MigratingToMigrated: Failed to send accounting for migrated files." % se, res['Message'] )
    return S_OK()

  def __getMigratedFiles( self, se, pfns ):
    # Get the active files from the database
    migrated = {}
    terminal = {}
    res = self.ReplicaManager.getStorageFileMetadata( pfns, se )
    if not res['OK']:
      return res
    for pfn, error in res['Value']['Failed'].items():
      if re.search( "File does not exist", error ):
        gLogger.error( "[%s] __getStorageMetadata: PFN does not exist at StorageElement." % se, "%s %s" % ( pfn, error ) )
        terminal[pfn] = 'PFNMissing'
      else:
        gLogger.warn( "[%s] __getMigratedFiles: Failed to obtain physical file metadata." % se, "%s %s" % ( pfn, error ) )
    storageMetadata = res['Value']['Successful']
    for pfn, metadata in storageMetadata.items():
      if metadata['Migrated']:
        checksum = ''
        if metadata.has_key( 'Checksum' ):
          checksum = metadata['Checksum']
        migrated[pfn] = checksum
      elif metadata['Lost']:
        gLogger.error( "[%s] __getMigratedFiles: PFN has been Lost by the StorageElement." % se, "%s" % ( pfn ) )
        terminal[pfn] = 'PFNLost'
      elif metadata['Unavailable']:
        gLogger.error( "[%s] __getMigratedFiles: PFN declared Unavailable by StorageElement." % se, "%s" % ( pfn ) )
        terminal[pfn] = 'PFNUnavailable'
    resDict = {'Terminal':terminal, 'Migrated':migrated}
    return S_OK( resDict )

  def __validateChecksums( self, se, migratedFileIDs, migratingFiles ):
    """ Obtain the checksums in the catalog if not present and check against the checksum from the storage
    """
    lfnFileID = {}
    checksumToObtain = []
    for fileID in migratedFileIDs.keys():
      if not migratingFiles[fileID]['Checksum']:
        lfn = migratingFiles[fileID]['LFN']
        checksumToObtain.append( lfn )
        lfnFileID[lfn] = fileID
    if checksumToObtain:
      res = self.ReplicaManager.getCatalogFileMetadata( checksumToObtain )
      if not res['OK']:
        gLogger.error( "[%s] __validateChecksums: Failed to obtain file checksums" % se )
        return res
      for lfn, error in res['Value']['Failed'].items():
        gLogger.error( "[%s] __validateChecksums: Failed to get file checksum" % se, "%s %s" % ( lfn, error ) )
      for lfn, metadata in res['Value']['Successful'].items():
        migratingFiles[lfnFileID[lfn]]['Checksum'] = metadata['CheckSumValue']
    mismatchFiles = []
    matchFiles = []
    checksumMismatches = []
    fileRecords = []
    for fileID, seChecksum in migratedFileIDs.items():
      lfn = migratingFiles[fileID]['LFN']
      catalogChecksum = migratingFiles[fileID]['Checksum']
      if not seChecksum:
        gLogger.error( "[%s] __validateChecksums: Storage checksum not available" % se, migratingFiles[fileID]['PFN'] )
      elif not compareAdler( seChecksum, catalogChecksum ):
        gLogger.error( "[%s] __validateChecksums: Storage and catalog checksum mismatch" % se, "%s '%s' '%s'" % ( migratingFiles[fileID]['PFN'], seChecksum, catalogChecksum ) )
        mismatchFiles.append( fileID )
        pfn = migratingFiles[fileID]['PFN']
        se = migratingFiles[fileID]['SE']
        checksumMismatches.append( ( lfn, pfn, se, 'CatalogPFNChecksumMismatch' ) )
        fileRecords.append( ( lfn, 'Checksum match', '%s@%s' % ( seChecksum, se ), '', 'MigrationMonitoringAgent' ) )
      else:
        fileRecords.append( ( lfn, 'Checksum mismatch', '%s@%s' % ( seChecksum, se ), '', 'MigrationMonitoringAgent' ) )
        matchFiles.append( fileID )
    # Add the data logging records
    self.DataLog.addFileRecords( fileRecords )
    if checksumMismatches:
      # Update the (mis)matching checksums (in the integrityDB and) in the migration monitoring db
      self.__reportProblematicReplicas( checksumMismatches )
      res = self.MigrationMonitoringDB.setMigratingReplicaStatus( mismatchFiles, 'ChecksumFail' )
      if not res['OK']:
        gLogger.error( "[%s] __validateChecksums: Failed to update checksum mismatching files." % se, res['Message'] )
    if matchFiles:
      res = self.MigrationMonitoringDB.setMigratingReplicaStatus( matchFiles, 'ChecksumMatch' )
      if not res['OK']:
        gLogger.error( "[%s] __validateChecksums: Failed to update checksum mismatching files." % se, res['Message'] )
    resDict = {'MatchingFiles':matchFiles, 'MismatchFiles':mismatchFiles}
    return S_OK( resDict )

  def __updateMigrationAccounting( self, se, migratingFiles, matchingFiles, mismatchingFiles, assumedEndTime, previousMonitorTime ):
    """ Create accounting messages for the overall throughput observed and the total migration time for the files
    """
    allMigrated = matchingFiles + mismatchingFiles
    gMonitor.addMark( "MigratedFiles%s" % se, len( allMigrated ) )
    gMonitor.addMark( "TotalMigratedFiles%s" % se, len( allMigrated ) )
    lfnFileID = {}
    sizesToObtain = []
    for fileID in allMigrated:
      if not migratingFiles[fileID]['Size']:
        lfn = migratingFiles[fileID]['LFN']
        sizesToObtain.append( lfn )
        lfnFileID[lfn] = fileID
    if sizesToObtain:
      res = self.ReplicaManager.getCatalogFileSize( sizesToObtain )
      if not res['OK']:
        gLogger.error( "[%s] __updateMigrationAccounting: Failed to obtain file sizes" % se )
        return res
      for lfn, error in res['Value']['Failed'].items():
        gLogger.error( "[%s] __updateAccounting: Failed to get file size" % se, "%s %s" % ( lfn, error ) )
        migratingFiles[lfnFileID[lfn]]['Size'] = 0
      for lfn, size in res['Value']['Successful'].items():
        migratingFiles[lfnFileID[lfn]]['Size'] = size
    totalSize = 0
    for fileID in allMigrated:
      size = migratingFiles[fileID]['Size']
      totalSize += size
      submitTime = migratingFiles[fileID]['SubmitTime']
      timeDiff = submitTime - assumedEndTime
      migrationTime = ( timeDiff.days * 86400 ) + ( timeDiff.seconds ) + ( timeDiff.microseconds / 1000000.0 )
      gMonitor.addMark( "MigrationTime%s" % se, migrationTime )
      gDataStoreClient.addRegister( self.__initialiseAccountingObject( 'MigrationTime', se, submitTime, assumedEndTime, size ) )
      gDataStoreClient.addRegister( self.__initialiseAccountingObject( 'MigrationThroughput', se, previousMonitorTime, assumedEndTime, size ) )
      oDataOperation = self.__initialiseAccountingObject( 'MigrationSuccess', se, submitTime, assumedEndTime, size )
      if fileID in mismatchingFiles:
        oDataOperation.setValueByKey( 'TransferOK', 0 )
        oDataOperation.setValueByKey( 'FinalStatus', 'Failed' )
      gDataStoreClient.addRegister( oDataOperation )
    gMonitor.addMark( "TotalMigratedSize%s" % se, totalSize )
    gMonitor.addMark( "ChecksumMismatches%s" % se, len( mismatchingFiles ) )
    gMonitor.addMark( "TotalChecksumMismatches%s" % se, len( mismatchingFiles ) )
    gMonitor.addMark( "ChecksumMatches%s" % se, len( matchingFiles ) )
    gMonitor.addMark( "TotalChecksumMatches%s" % se, len( matchingFiles ) )
    if allMigrated:
      gLogger.info( '[%s] __updateMigrationAccounting: Attempting to send accounting message...' % se )
      return gDataStoreClient.commit()
    return S_OK()

  #########################################################################################################
  #
  # Utility methods used by all methods
  #

  def __getFiles( self, se, status ):
    # Get files with the given status and se from the database
    res = self.MigrationMonitoringDB.getMigratingReplicas( se, status )
    if not res['OK']:
      return res
    files = res['Value']
    pfnIDs = {}
    if len( files.keys() ) > 0:
      for fileID, metadataDict in files.items():
        pfn = metadataDict['PFN']
        pfnIDs[pfn] = fileID
    return S_OK( {'PFNIDs':pfnIDs, 'Files':files} )

  def __getCatalogFileMetadata( self, files ):
    lfnFileID = {}
    metadataToObtain = []
    for fileID, metadata in files.items():
      if not ( metadata['Size'] and metadata['Checksum'] ):
        lfn = metadata['LFN']
        metadataToObtain.append( lfn )
        lfnFileID[lfn] = fileID
    if not metadataToObtain:
      return S_OK()
    res = self.ReplicaManager.getCatalogFileMetadata( metadataToObtain )
    if not res['OK']:
      gLogger.error( "__getCatalogFileMetadata: Failed to obtain file metadata", res['Message'] )
      return res
    successful = res['Value']['Successful']
    failed = res['Value']['Failed']
    terminalIDs = []
    problematicFiles = []
    for lfn, error in failed.items():
      gLogger.error( "__getCatalogFileMetadata: Failed to get file metadata", "%s %s" % ( lfn, error ) )
      if re.search( "No such file or directory", error ):
        fileID = lfnFileID[lfn]
        lfn = files[fileID]['LFN']
        pfn = files[fileID]['PFN']
        se = files[fileID]['SE']
        problematicFiles.append( lfn )
        terminalIDs.append( fileID )
    if terminalIDs:
      self.__reportProblematicFiles( problematicFiles, 'LFNCatalogMissing' )
      self.__setMigratingReplicaStatus( terminalIDs, 'Failed' )
    fileMetadata = {}
    for lfn, metadata in successful.items():
      size = metadata['Size']
      checksum = metadata['CheckSumValue']
      fileMetadata[lfnFileID[lfn]] = {'Size':size, 'Checksum':checksum}
    return S_OK( fileMetadata )

  def __setMigratingReplicaStatus( self, fileIDs, status ):
    gLogger.info( "__setMigratingReplicaStatus: Attempting to update %s files to '%s'" % ( len( fileIDs ), status ) )
    res = self.MigrationMonitoringDB.setMigratingReplicaStatus( fileIDs, status )
    if not res['OK']:
      gLogger.info( "__setMigratingReplicaStatus: Failed to update status of files", res['Message'] )
    else:
      gLogger.info( "__setMigratingReplicaStatus: Successfully updated status of files" )

  def __reportProblematicFiles( self, lfns, reason ):
    gLogger.info( '__reportProblematicFiles: The following %s files were found with %s' % ( len( lfns ), reason ) )
    for lfn in sortList( lfns ):
      gLogger.info( lfn )
    res = self.DataIntegrityClient.setFileProblematic( lfns, reason, sourceComponent = 'MigrationMonitoringAgent' )
    if not res['OK']:
      gLogger.info( '__reportProblematicFiles: Failed to update integrity DB with files', res['Message'] )
    else:
      gLogger.info( '__reportProblematicFiles: Successfully updated integrity DB with files' )

  def __reportProblematicReplicas( self, replicaTuples ):
    gLogger.info( '__reportProblematicReplicas: The following %s files being reported to integrity DB:' % ( len( replicaTuples ) ) )
    for lfn, pfn, se, reason in sortList( replicaTuples ):
      if lfn:
        gLogger.info( lfn )
      else:
        gLogger.info( pfn )
    res = self.DataIntegrityClient.setReplicaProblematic( replicaTuples, sourceComponent = 'MigrationMonitoringAgent' )
    if not res['OK']:
      gLogger.info( '__reportProblematicReplicas: Failed to update integrity DB with replicas', res['Message'] )
    else:
      gLogger.info( '__reportProblematicReplicas: Successfully updated integrity DB with replicas' )

  def __initialiseAccountingObject( self, operation, se, startTime, endTime, size ):
    accountingDict = {}
    accountingDict['OperationType'] = operation
    accountingDict['User'] = self.userName
    accountingDict['Protocol'] = 'SRM'
    accountingDict['RegistrationTime'] = 0.0
    accountingDict['RegistrationOK'] = 0
    accountingDict['RegistrationTotal'] = 0
    accountingDict['TransferTotal'] = 1
    accountingDict['TransferOK'] = 1
    accountingDict['TransferSize'] = size
    timeDiff = endTime - startTime
    transferTime = ( timeDiff.days * 86400 ) + ( timeDiff.seconds ) + ( timeDiff.microseconds / 1000000.0 )
    accountingDict['TransferTime'] = transferTime
    accountingDict['FinalStatus'] = 'Successful'
    accountingDict['Source'] = siteName()
    accountingDict['Destination'] = se
    oDataOperation = DataOperation()
    oDataOperation.setEndTime( endTime )
    oDataOperation.setStartTime( startTime )
    oDataOperation.setValuesFromDict( accountingDict )
    return oDataOperation
