########################################################################
# $HeadURL$
########################################################################
__RCSID__ = "$Id$"

from DIRAC                                                          import S_OK, S_ERROR, gConfig, gMonitor, gLogger, rootPath
from DIRAC.Core.Base.AgentModule                                    import AgentModule
from DIRAC.Core.Utilities.List                                      import sortList, breakListIntoChunks
from DIRAC.DataManagementSystem.Client.ReplicaManager               import ReplicaManager
from DIRAC.RequestManagementSystem.Client.RequestClient             import RequestClient
from DIRAC.DataManagementSystem.Client.StorageUsageClient           import StorageUsageClient
from DIRAC.Resources.Catalog.FileCatalogClient                      import FileCatalogClient
from DIRAC.TransformationSystem.Client.TransformationClient         import TransformationClient
from DIRAC.WorkloadManagementSystem.Client.WMSClient                import WMSClient
from datetime                                                       import datetime, timedelta
import re, os

AGENT_NAME = 'Transformation/TransformationCleaningAgent'

class TransformationCleaningAgent( AgentModule ):

  #############################################################################
  def initialize( self ):
    """Sets defaults """
    self.replicaManager = ReplicaManager()
    self.transClient = TransformationClient()
    self.wmsClient = WMSClient()
    self.requestClient = RequestClient()
    self.metadataClient = FileCatalogClient()
    self.storageUsageClient = StorageUsageClient()

    # This sets the Default Proxy to used as that defined under 
    # /Operations/Shifter/DataManager
    # the shifterProxy option in the Configuration can be used to change this default.
    self.am_setOption( 'shifterProxy', 'DataManager' )

    self.transformationTypes = sortList( self.am_getOption( 'TransformationTypes', ['MCSimulation', 'DataReconstruction', 'DataStripping', 'MCStripping', 'Merge', 'Replication'] ) )
    gLogger.info( "Will consider the following transformation types: %s" % str( self.transformationTypes ) )
    self.directoryLocations = sortList( self.am_getOption( 'DirectoryLocations', ['TransformationDB', 'StorageUsage', 'MetadataCatalog'] ) )
    gLogger.info( "Will search for directories in the following locations: %s" % str( self.directoryLocations ) )
    self.transfidmeta = self.am_getOption( 'TransfIDMeta', "TransformationID" )
    gLogger.info( "Will use %s as metadata tag name for TransformationID" % self.transfidmeta )
    self.archiveAfter = self.am_getOption( 'ArchiveAfter', 7 ) # days
    gLogger.info( "Will archive Completed transformations after %d days" % self.archiveAfter )
    self.activeStorages = sortList( self.am_getOption( 'ActiveSEs', [] ) )
    gLogger.info( "Will check the following storage elements: %s" % str( self.activeStorages ) )
    self.logSE = self.am_getOption( 'TransformationLogSE', 'LogSE' )
    gLogger.info( "Will remove logs found on storage element: %s" % self.logSE )
    return S_OK()

  #############################################################################
  def execute( self ):
    """ The TransformationCleaningAgent execution method.
    """
    self.enableFlag = self.am_getOption( 'EnableFlag', 'True' )
    if not self.enableFlag == 'True':
      self.log.info( 'TransformationCleaningAgent is disabled by configuration option %s/EnableFlag' % ( self.section ) )
      return S_OK( 'Disabled via CS flag' )

    # Obtain the transformations in Cleaning status and remove any mention of the jobs/files
    res = self.transClient.getTransformations( {'Status':'Cleaning', 'Type':self.transformationTypes} )
    if res['OK']:
      for transDict in res['Value']:
        # If transformation is of type `Replication` or `Removal`, there is nothing to clean.
        # We just archive
        if transDict[ 'Type' ] in [ 'Replication', 'Removal' ]:
          self.archiveTransformation( transDict['TransformationID'] )
        else:      
          self.cleanTransformation( transDict['TransformationID'] )

    # Obtain the transformations in RemovingFiles status and (wait for it) removes the output files
    res = self.transClient.getTransformations( {'Status':'RemovingFiles', 'Type':self.transformationTypes} )
    if res['OK']:
      for transDict in res['Value']:
        self.removeTransformationOutput( transDict['TransformationID'] )

    # Obtain the transformations in Completed status and archive if inactive for X days
    olderThanTime = datetime.utcnow() - timedelta( days = self.archiveAfter )
    res = self.transClient.getTransformations( {'Status':'Completed', 'Type':self.transformationTypes}, older = olderThanTime )
    if res['OK']:
      for transDict in res['Value']:
        self.archiveTransformation( transDict['TransformationID'] )

    return S_OK()

  #############################################################################
  #
  # Get the transformation directories for checking
  #

  def getTransformationDirectories( self, transID ):
    """ Get the directories for the supplied transformation from the transformation system """
    directories = []
    if 'TransformationDB' in self.directoryLocations:
      res = self.transClient.getTransformationParameters( transID, ['OutputDirectories'] )
      if not res['OK']:
        gLogger.error( "Failed to obtain transformation directories", res['Message'] )
        return res
      transDirectories = res['Value'].splitlines()
      directories = self.__addDirs( transID, transDirectories, directories )

    if 'StorageUsage' in self.directoryLocations:
      res = self.storageUsageClient.getStorageDirectories( '', '', transID, [] )
      if not res['OK']:
        gLogger.error( "Failed to obtain storage usage directories", res['Message'] )
        return res
      transDirectories = res['Value']
      directories = self.__addDirs( transID, transDirectories, directories )

    if 'MetadataCatalog' in self.directoryLocations:
      res = self.metadataClient.findDirectoriesByMetadata( {self.transfidmeta:transID} )
      if not res['OK']:
        gLogger.error( "Failed to obtain metadata catalog directories", res['Message'] )
        return res
      transDirectories = res['Value']
      directories = self.__addDirs( transID, transDirectories, directories )
    if not directories:
      gLogger.info( "No output directories found" )
    directories = sortList( directories )
    return S_OK( directories )

  def __addDirs( self, transID, newDirs, existingDirs ):
    for dir in newDirs:
      transStr = str( transID ).zfill( 8 )
      if re.search( transStr, dir ):
        if not dir in existingDirs:
          existingDirs.append( dir )
    return existingDirs

  #############################################################################
  #
  # These are the methods for performing the cleaning of catalogs and storage
  #

  def cleanStorageContents( self, directory ):
    for storageElement in self.activeStorages:
      res = self.__removeStorageDirectory( directory, storageElement )
      if not res['OK']:
        return res
    return S_OK()

  def __removeStorageDirectory( self, directory, storageElement ):
    gLogger.info( 'Removing the contents of %s at %s' % ( directory, storageElement ) )
    res = self.replicaManager.getPfnForLfn( [directory], storageElement )
    if not res['OK']:
      gLogger.error( "Failed to get PFN for directory", res['Message'] )
      return res
    for directory, error in res['Value']['Failed'].items():
      gLogger.error( 'Failed to obtain directory PFN from LFN', '%s %s' % ( directory, error ) )
    if res['Value']['Failed']:
      return S_ERROR( 'Failed to obtain directory PFN from LFNs' )
    storageDirectory = res['Value']['Successful'].values()[0]
    res = self.replicaManager.getStorageFileExists( storageDirectory, storageElement, singleFile = True )
    if not res['OK']:
      gLogger.error( "Failed to obtain existance of directory", res['Message'] )
      return res
    exists = res['Value']
    if not exists:
      gLogger.info( "The directory %s does not exist at %s " % ( directory, storageElement ) )
      return S_OK()
    res = self.replicaManager.removeStorageDirectory( storageDirectory, storageElement, recursive = True, singleDirectory = True )
    if not res['OK']:
      gLogger.error( "Failed to remove storage directory", res['Message'] )
      return res
    gLogger.info( "Successfully removed %d files from %s at %s" % ( res['Value']['FilesRemoved'], directory, storageElement ) )
    return S_OK()

  def cleanCatalogContents( self, directory ):
    res = self.__getCatalogDirectoryContents( [directory] )
    if not res['OK']:
      return res
    filesFound = res['Value']
    if not filesFound:
      return S_OK()
    gLogger.info( "Attempting to remove %d possible remnants from the catalog and storage" % len( filesFound ) )
    res = self.replicaManager.removeFile( filesFound )
    if not res['OK']:
      return res
    for lfn, reason in res['Value']['Failed'].items():
      gLogger.error( "Failed to remove file found in the catalog", "%s %s" % ( lfn, reason ) )
    if res['Value']['Failed']:
      return S_ERROR( "Failed to remove all files found in the catalog" )
    return S_OK()

  def __getCatalogDirectoryContents( self, directories ):
    gLogger.info( 'Obtaining the catalog contents for %d directories:' % len( directories ) )
    for directory in directories:
      gLogger.info( directory )
    activeDirs = directories
    allFiles = {}
    while len( activeDirs ) > 0:
      currentDir = activeDirs[0]
      res = self.replicaManager.getCatalogListDirectory( currentDir, singleFile = True )
      activeDirs.remove( currentDir )
      if not res['OK'] and res['Message'].endswith( 'The supplied path does not exist' ):
        gLogger.info( "The supplied directory %s does not exist" % currentDir )
      elif not res['OK']:
        gLogger.error( 'Failed to get directory contents', '%s %s' % ( currentDir, res['Message'] ) )
      else:
        dirContents = res['Value']
        activeDirs.extend( dirContents['SubDirs'] )
        allFiles.update( dirContents['Files'] )
    gLogger.info( "Found %d files" % len( allFiles ) )
    return S_OK( allFiles.keys() )

  def cleanTransformationLogFiles( self, directory ):
    gLogger.info( "Removing log files found in the directory %s" % directory )
    res = self.replicaManager.removeStorageDirectory( directory, self.logSE, singleDirectory = True )
    if not res['OK']:
      gLogger.error( "Failed to remove log files", res['Message'] )
      return res
    gLogger.info( "Successfully removed transformation log directory" )
    return S_OK()

  #############################################################################
  #
  # These are the functional methods for archiving and cleaning transformations
  #

  def removeTransformationOutput( self, transID ):
    """ This just removes any mention of the output data from the catalog and storage """
    gLogger.info( "Removing output data for transformation %s" % transID )
    res = self.getTransformationDirectories( transID )
    if not res['OK']:
      gLogger.error( 'Problem obtaining directories for transformation %s with result "%s"' % ( transID, res ) )
      return S_OK()
    directories = res['Value']
    for directory in directories:
      if not re.search( '/LOG/', directory ):
        res = self.cleanCatalogContents( directory )
        if not res['OK']:
          return res
        res = self.cleanStorageContents( directory )
        if not res['OK']:
          return res
    gLogger.info( "Removed directories in the catalog and storage for transformation" )
    # Clean ALL the possible remnants found in the metadata catalog
    res = self.cleanMetadataCatalogFiles( transID, directories )
    if not res['OK']:
      return res
    gLogger.info( "Successfully removed output of transformation %d" % transID )
    # Change the status of the transformation to RemovedFiles
    res = self.transClient.setTransformationParameter( transID, 'Status', 'RemovedFiles' )
    if not res['OK']:
      gLogger.error( "Failed to update status of transformation %s to RemovedFiles" % ( transID ), res['Message'] )
      return res
    gLogger.info( "Updated status of transformation %s to RemovedFiles" % ( transID ) )
    return S_OK()

  def archiveTransformation( self, transID ):
    """ This just removes job from the jobDB and the transformation DB """
    gLogger.info( "Archiving transformation %s" % transID )
    # Clean the jobs in the WMS and any failover requests found
    res = self.cleanTransformationTasks( transID )
    if not res['OK']:
      return res
    # Clean the transformation DB of the files and job information
    res = self.transClient.cleanTransformation( transID )
    if not res['OK']:
      return res
    gLogger.info( "Successfully archived transformation %d" % transID )
    # Change the status of the transformation to archived
    res = self.transClient.setTransformationParameter( transID, 'Status', 'Archived' )
    if not res['OK']:
      gLogger.error( "Failed to update status of transformation %s to Archived" % ( transID ), res['Message'] )
      return res
    gLogger.info( "Updated status of transformation %s to Archived" % ( transID ) )
    return S_OK()

  def cleanTransformation( self, transID ):
    """ This removes any mention of the supplied transformation 
    """
    gLogger.info( "Cleaning transformation %s" % transID )
    res = self.getTransformationDirectories( transID )
    if not res['OK']:
      gLogger.error( 'Problem obtaining directories for transformation %s with result "%s"' % ( transID, res ) )
      return S_OK()
    directories = res['Value']
    # Clean the jobs in the WMS and any failover requests found
    res = self.cleanTransformationTasks( transID )
    if not res['OK']:
      return res
    # Clean the log files for the jobs
    for directory in directories:
      if re.search( '/LOG/', directory ):
        res = self.cleanTransformationLogFiles( directory )
        if not res['OK']:
          return res
      res = self.cleanCatalogContents( directory )
      if not res['OK']:
        return res
      res = self.cleanStorageContents( directory )
      if not res['OK']:
        return res
    # Clean ALL the possible remnants found in the BK
    res = self.cleanMetadataCatalogFiles( transID, directories )
    if not res['OK']:
      return res
    # Clean the transformation DB of the files and job information
    res = self.transClient.cleanTransformation( transID )
    if not res['OK']:
      return res
    gLogger.info( "Successfully cleaned transformation %d" % transID )
    # Change the status of the transformation to deleted
    res = self.transClient.setTransformationParameter( transID, 'Status', 'Deleted' )
    if not res['OK']:
      gLogger.error( "Failed to update status of transformation %s to Deleted" % ( transID ), res['Message'] )
      return res
    gLogger.info( "Updated status of transformation %s to Deleted" % ( transID ) )
    return S_OK()

  def cleanMetadataCatalogFiles( self, transID, directories ):
    res = self.metadataClient.findFilesByMetadata( {self.transfidmeta:transID} )
    if not res['OK']:
      return res
    fileToRemove = res['Value']
    if not len(fileToRemove):
      gLogger.info('No files found for transID %s'%transID)
      return S_OK()
    res = self.replicaManager.removeFile( fileToRemove )
    if not res['OK']:
      return res
    for lfn, reason in res['Value']['Failed'].items():
      gLogger.error( "Failed to remove file found in metadata catalog", "%s %s" % ( lfn, reason ) )
    if res['Value']['Failed']:
      return S_ERROR( "Failed to remove all files found in the metadata catalog" )
    gLogger.info( "Successfully removed all files found in the BK" )
    return S_OK()

  #############################################################################
  #
  # These are the methods for removing the jobs from the WMS and transformation DB
  #

  def cleanTransformationTasks( self, transID ):
    res = self.__getTransformationExternalIDs( transID )
    if not res['OK']:
      return res
    externalIDs = res['Value']
    if externalIDs:
      res = self.transClient.getTransformationParameters( transID, ['Type'] )
      if not res['OK']:
        gLogger.error( "Failed to determine transformation type" )
        return res
      transType = res['Value']
      if transType == 'Replication':
        res = self.__removeRequests( externalIDs )
      else:
        res = self.__removeWMSTasks( externalIDs )
      if not res['OK']:
        return res
    return S_OK()

  def __getTransformationExternalIDs( self, transID ):
    res = self.transClient.getTransformationTasks( condDict = {'TransformationID':transID} )
    if not res['OK']:
      gLogger.error( "Failed to get externalIDs for transformation %d" % transID, res['Message'] )
      return res
    externalIDs = []
    for taskDict in res['Value']:
      externalIDs.append( taskDict['ExternalID'] )
    gLogger.info( "Found %d tasks for transformation" % len( externalIDs ) )
    return S_OK( externalIDs )

  def __removeRequests( self, requestIDs ):
    gLogger.error( "Not removing requests but should do" )
    return S_OK()

  def __removeWMSTasks( self, jobIDs ):
    allRemove = True
    for jobList in breakListIntoChunks( jobIDs, 500 ):
      res = self.wmsClient.deleteJob( jobList )
      if res['OK']:
        gLogger.info( "Successfully removed %d jobs from WMS" % len( jobList ) )
      elif ( res.has_key( 'InvalidJobIDs' ) ) and ( not res.has_key( 'NonauthorizedJobIDs' ) ) and ( not res.has_key( 'FailedJobIDs' ) ):
        gLogger.info( "Found %s jobs which did not exist in the WMS" % len( res['InvalidJobIDs'] ) )
      elif res.has_key( 'NonauthorizedJobIDs' ):
        gLogger.error( "Failed to remove %s jobs because not authorized" % len( res['NonauthorizedJobIDs'] ) )
        allRemove = False
      elif res.has_key( 'FailedJobIDs' ):
        gLogger.error( "Failed to remove %s jobs" % len( res['FailedJobIDs'] ) )
        allRemove = False
    if not allRemove:
      return S_ERROR( "Failed to remove all remnants from WMS" )
    gLogger.info( "Successfully removed all tasks from the WMS" )
    res = self.requestClient.getRequestForJobs( jobIDs )
    if not res['OK']:
      gLogger.error( "Failed to get requestID for jobs.", res['Message'] )
      return res
    failoverRequests = res['Value']
    gLogger.info( "Found %d jobs with associated failover requests" % len( failoverRequests ) )
    if not failoverRequests:
      return S_OK()
    failed = 0
    for jobID, requestName in failoverRequests.items():
      res = self.requestClient.deleteRequest( requestName )
      if not res['OK']:
        gLogger.error( "Failed to remove request from RequestDB", res['Message'] )
        failed += 1
      else:
        gLogger.verbose( "Removed request %s associated to job %d." % ( requestName, jobID ) )
    if failed:
      gLogger.info( "Successfully removed %s requests" % ( len( failoverRequests ) - failed ) )
      gLogger.info( "Failed to remove %s requests" % failed )
      return S_ERROR( "Failed to remove all the request from RequestDB" )
    gLogger.info( "Successfully removed all the associated failover requests" )
    return S_OK()
