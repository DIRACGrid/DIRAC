########################################################################
# $HeadURL$
########################################################################

""" :mod: TransformationCleaningAgent 
    =================================
 
    .. module: TransformationCleaningAgent
    :synopsis: clean up of finalised transformations
"""

__RCSID__ = "$Id$"

## imports
import re
from datetime import datetime, timedelta
## from DIRAC
from DIRAC import S_OK, S_ERROR, gConfig, gMonitor, gLogger, rootPath
from DIRAC.Core.Base.AgentModule import AgentModule
from DIRAC.Core.Utilities.List import sortList, breakListIntoChunks
from DIRAC.DataManagementSystem.Client.ReplicaManager import ReplicaManager
from DIRAC.RequestManagementSystem.Client.RequestClient import RequestClient
from DIRAC.DataManagementSystem.Client.StorageUsageClient import StorageUsageClient
from DIRAC.Resources.Catalog.FileCatalogClient import FileCatalogClient
from DIRAC.TransformationSystem.Client.TransformationClient import TransformationClient
from DIRAC.WorkloadManagementSystem.Client.WMSClient import WMSClient

## agent's name
AGENT_NAME = 'Transformation/TransformationCleaningAgent'

class TransformationCleaningAgent( AgentModule ):
  """
  .. class:: TransformationCleaningAgent

  :param ReplicaManger replicaManager: ReplicaManager instance
  :param TransfromationClient transClient: TransfromationClient instance
  :param RequestClient requestClient: RequestClient instance
  :param FileCatalogClient metadataClient: FileCatalogClient instance
  :param StorageUsageClient storageUsageClient: StorageUsageClient instance

  """

  def __init__( self, agentName, loadName, baseAgentName = False,	properties = dict() ):
    """ c'tor

    :param self: self reference
    :param str agentName: name of agent
    :param str loadName: name of module
    :param bool baseAgentName: whatever  
    :param dict properties: whatever else
    """
    AgentModule.__init__( self, agentName, loadName, baseAgentName, properties )
    ## replica manager
    self.replicaManager = ReplicaManager()
    ## transformation client
    self.transClient = TransformationClient()
    ## wms client
    self.wmsClient = WMSClient()
    ## request client
    self.requestClient = RequestClient()
    ## file catalog clinet
    self.metadataClient = FileCatalogClient()
    ## storage usage agent
    self.storageUsageClient = StorageUsageClient()

    ## placeholders for CS options

    ## transformations types
    self.transformationTypes = None 
    ## directory locations
    self.directoryLocations = None 
    ## transformation metadata
    self.transfidmeta = None 
    ## archive periof in days
    self.archiveAfter = None 
    ## active SEs
    self.activeStorages = None 
    ## transformation log SEs
    self.logSE = None 
    ## enable/disable execution
    self.enableFlag = None 

  def initialize( self ):
    """ agent initialisation 
    
    reading and setting confing opts

    :param self: self reference
    """
    ## shifter proxy
    self.am_setOption( 'shifterProxy', 'DataManager' )
    ## transformations types
    self.transformationTypes = sortList( self.am_getOption( 'TransformationTypes', [ 'MCSimulation', 
                                                                                     'DataReconstruction', 
                                                                                     'DataStripping', 
                                                                                     'MCStripping', 
                                                                                     'Merge', 
                                                                                     'Replication' ] ) )
    self.log.info( "Will consider the following transformation types: %s" % str( self.transformationTypes ) )
    ## directory locations
    self.directoryLocations = sortList( self.am_getOption( 'DirectoryLocations', [ 'TransformationDB', 
                                                                                   'StorageUsage', 
                                                                                   'MetadataCatalog' ] ) )
    self.log.info( "Will search for directories in the following locations: %s" % str( self.directoryLocations ) )
    ## transformation metadata
    self.transfidmeta = self.am_getOption( 'TransfIDMeta', "TransformationID" )
    self.log.info( "Will use %s as metadata tag name for TransformationID" % self.transfidmeta )
    ## archive periof in days
    self.archiveAfter = self.am_getOption( 'ArchiveAfter', 7 ) # days
    self.log.info( "Will archive Completed transformations after %d days" % self.archiveAfter )
    ## active SEs
    self.activeStorages = sortList( self.am_getOption( 'ActiveSEs', [] ) )
    self.log.info( "Will check the following storage elements: %s" % str( self.activeStorages ) )
    ## transformation log SEs
    self.logSE = self.am_getOption( 'TransformationLogSE', 'LogSE' )
    self.log.info( "Will remove logs found on storage element: %s" % self.logSE )
    ## enable/disable execution, should be using CS option Status?? with default value as 'Active'?? 
    self.enableFlag = self.am_getOption( 'EnableFlag', 'True' )
    return S_OK()
    
  #############################################################################
  def execute( self ):
    """ execution in one agent's cycle

    :param self: self reference
    """

    self.enableFlag = self.am_getOption( 'EnableFlag', 'True' )
    if not self.enableFlag == 'True':
      self.log.info( 'MCExtensionAgent is disabled by configuration option EnableFlag' )
      return S_OK( 'Disabled via CS flag' )

    ## Obtain the transformations in Cleaning status and remove any mention of the jobs/files
    res = self.transClient.getTransformations( { 'Status' : 'Cleaning', 
                                                 'Type' : self.transformationTypes } )
    if res['OK']:
      for transDict in res['Value']:
        ## if transformation is of type `Replication` or `Removal`, there is nothing to clean.
        ## We just archive
        if transDict[ 'Type' ] in [ 'Replication', 'Removal' ]:
          self.archiveTransformation( transDict['TransformationID'] )
        else:      
          self.cleanTransformation( transDict['TransformationID'] )

    ## Obtain the transformations in RemovingFiles status and (wait for it) removes the output files
    res = self.transClient.getTransformations( { 'Status' : 'RemovingFiles', 
                                                 'Type' : self.transformationTypes} )
    if res['OK']:
      for transDict in res['Value']:
        self.removeTransformationOutput( transDict['TransformationID'] )

    ## Obtain the transformations in Completed status and archive if inactive for X days
    olderThanTime = datetime.utcnow() - timedelta( days = self.archiveAfter )
    res = self.transClient.getTransformations( { 'Status' : 'Completed', 
                                                 'Type' : self.transformationTypes },
                                               older = olderThanTime )
    if res['OK']:
      for transDict in res['Value']:
        self.archiveTransformation( transDict['TransformationID'] )

    return S_OK()

  #############################################################################
  #
  # Get the transformation directories for checking
  #

  def getTransformationDirectories( self, transID ):
    """ get the directories for the supplied transformation from the transformation system 

    :param self: self reference
    :param int transID: transformation ID
    """
    directories = []
    if 'TransformationDB' in self.directoryLocations:
      res = self.transClient.getTransformationParameters( transID, ['OutputDirectories'] )
      if not res['OK']:
        self.log.error( "Failed to obtain transformation directories", res['Message'] )
        return res
      transDirectories = res['Value'].splitlines()
      directories = self.__addDirs( transID, transDirectories, directories )

    if 'StorageUsage' in self.directoryLocations:
      res = self.storageUsageClient.getStorageDirectories( '', '', transID, [] )
      if not res['OK']:
        self.log.error( "Failed to obtain storage usage directories", res['Message'] )
        return res
      transDirectories = res['Value']
      directories = self.__addDirs( transID, transDirectories, directories )

    if 'MetadataCatalog' in self.directoryLocations:
      res = self.metadataClient.findDirectoriesByMetadata( {self.transfidmeta:transID} )
      if not res['OK']:
        self.log.error( "Failed to obtain metadata catalog directories", res['Message'] )
        return res
      transDirectories = res['Value']
      directories = self.__addDirs( transID, transDirectories, directories )
    if not directories:
      self.log.info( "No output directories found" )
    directories = sortList( directories )
    return S_OK( directories )

  def __addDirs( self, transID, newDirs, existingDirs ):
    """ append uniqe :newDirs: list to :existingDirs: list

    :param self: self reference 
    :param int transID: transformationID
    :param list newDirs: src list of paths
    :param list existingDirs: dest list of paths
    """
    for folder in newDirs:
      transStr = str( transID ).zfill( 8 )
      if re.search( transStr, str(folder) ):
        if not folder in existingDirs:
          existingDirs.append( folder )
    return existingDirs

  #############################################################################
  #
  # These are the methods for performing the cleaning of catalogs and storage
  #

  def cleanStorageContents( self, directory ):
    """ delete lfn dir from all active SE

    :param self: self reference
    :param sre directory: folder name
    """
    for storageElement in self.activeStorages:
      res = self.__removeStorageDirectory( directory, storageElement )
      if not res['OK']:
        return res
    return S_OK()

  def __removeStorageDirectory( self, directory, storageElement ):
    """ wipe out all contents from :directory: at :storageElement:

    :param self: self reference
    :param str directory: path
    :param str storageElement: SE name
    """
    self.log.info( 'Removing the contents of %s at %s' % ( directory, storageElement ) )
    res = self.replicaManager.getPfnForLfn( [directory], storageElement )
    if not res['OK']:
      self.log.error( "Failed to get PFN for directory", res['Message'] )
      return res
    for directory, error in res['Value']['Failed'].items():
      self.log.error( 'Failed to obtain directory PFN from LFN', '%s %s' % ( directory, error ) )
    if res['Value']['Failed']:
      return S_ERROR( 'Failed to obtain directory PFN from LFNs' )
    storageDirectory = res['Value']['Successful'].values()[0]
    res = self.replicaManager.getStorageFileExists( storageDirectory, storageElement, singleFile = True )
    if not res['OK']:
      self.log.error( "Failed to obtain existance of directory", res['Message'] )
      return res
    exists = res['Value']
    if not exists:
      self.log.info( "The directory %s does not exist at %s " % ( directory, storageElement ) )
      return S_OK()
    res = self.replicaManager.removeStorageDirectory( storageDirectory, 
                                                      storageElement, 
                                                      recursive = True, 
                                                      singleDirectory = True )
    if not res['OK']:
      self.log.error( "Failed to remove storage directory", res['Message'] )
      return res
    self.log.info( "Successfully removed %d files from %s at %s" % ( res['Value']['FilesRemoved'], 
                                                                     directory, 
                                                                     storageElement ) )
    return S_OK()

  def cleanCatalogContents( self, directory ):
    """ wipe out everything from catalog under folder :directory:

    :param self: self reference
    :params str directory: folder name
    """
    res = self.__getCatalogDirectoryContents( [directory] )
    if not res['OK']:
      return res
    filesFound = res['Value']
    if not filesFound:
      return S_OK()
    self.log.info( "Attempting to remove %d possible remnants from the catalog and storage" % len( filesFound ) )
    res = self.replicaManager.removeFile( filesFound )
    if not res['OK']:
      return res
    for lfn, reason in res['Value']['Failed'].items():
      self.log.error( "Failed to remove file found in the catalog", "%s %s" % ( lfn, reason ) )
    if res['Value']['Failed']:
      return S_ERROR( "Failed to remove all files found in the catalog" )
    return S_OK()

  def __getCatalogDirectoryContents( self, directories ):
    """ get catalog contents under paths :directories: 

    :param self: self reference 
    :param list directories: list of paths in catalog
    """
    self.log.info( 'Obtaining the catalog contents for %d directories:' % len( directories ) )
    for directory in directories:
      self.log.info( directory )
    activeDirs = directories
    allFiles = {}
    while len( activeDirs ) > 0:
      currentDir = activeDirs[0]
      res = self.replicaManager.getCatalogListDirectory( currentDir, singleFile = True )
      activeDirs.remove( currentDir )
      if not res['OK'] and res['Message'].endswith( 'The supplied path does not exist' ):
        self.log.info( "The supplied directory %s does not exist" % currentDir )
      elif not res['OK']:
        self.log.error( 'Failed to get directory contents', '%s %s' % ( currentDir, res['Message'] ) )
      else:
        dirContents = res['Value']
        activeDirs.extend( dirContents['SubDirs'] )
        allFiles.update( dirContents['Files'] )
    self.log.info( "Found %d files" % len( allFiles ) )
    return S_OK( allFiles.keys() )

  def cleanTransformationLogFiles( self, directory ):
    """ clean up transformation logs from directory :directory:

    :param self: self reference 
    :param str directory: folder name
    """
    self.log.info( "Removing log files found in the directory %s" % directory )
    res = self.replicaManager.removeStorageDirectory( directory, self.logSE, singleDirectory = True )
    if not res['OK']:
      self.log.error( "Failed to remove log files", res['Message'] )
      return res
    self.log.info( "Successfully removed transformation log directory" )
    return S_OK()

  #############################################################################
  #
  # These are the functional methods for archiving and cleaning transformations
  #

  def removeTransformationOutput( self, transID ):
    """ This just removes any mention of the output data from the catalog and storage """
    self.log.info( "Removing output data for transformation %s" % transID )
    res = self.getTransformationDirectories( transID )
    if not res['OK']:
      self.log.error( 'Problem obtaining directories for transformation %s with result "%s"' % ( transID, res ) )
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
    self.log.info( "Removed directories in the catalog and storage for transformation" )
    # Clean ALL the possible remnants found in the metadata catalog
    res = self.cleanMetadataCatalogFiles( transID, directories )
    if not res['OK']:
      return res
    self.log.info( "Successfully removed output of transformation %d" % transID )
    # Change the status of the transformation to RemovedFiles
    res = self.transClient.setTransformationParameter( transID, 'Status', 'RemovedFiles' )
    if not res['OK']:
      self.log.error( "Failed to update status of transformation %s to RemovedFiles" % ( transID ), res['Message'] )
      return res
    self.log.info( "Updated status of transformation %s to RemovedFiles" % ( transID ) )
    return S_OK()

  def archiveTransformation( self, transID ):
    """ This just removes job from the jobDB and the transformation DB 

    :param self: self reference
    :param int transID: transformation ID
    """
    self.log.info( "Archiving transformation %s" % transID )
    # Clean the jobs in the WMS and any failover requests found
    res = self.cleanTransformationTasks( transID )
    if not res['OK']:
      return res
    # Clean the transformation DB of the files and job information
    res = self.transClient.cleanTransformation( transID )
    if not res['OK']:
      return res
    self.log.info( "Successfully archived transformation %d" % transID )
    # Change the status of the transformation to archived
    res = self.transClient.setTransformationParameter( transID, 'Status', 'Archived' )
    if not res['OK']:
      self.log.error( "Failed to update status of transformation %s to Archived" % ( transID ), res['Message'] )
      return res
    self.log.info( "Updated status of transformation %s to Archived" % ( transID ) )
    return S_OK()

  def cleanTransformation( self, transID ):
    """ This removes any mention of the supplied transformation 
    """
    self.log.info( "Cleaning transformation %s" % transID )
    res = self.getTransformationDirectories( transID )
    if not res['OK']:
      self.log.error( 'Problem obtaining directories for transformation %s with result "%s"' % ( transID, res ) )
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
    self.log.info( "Successfully cleaned transformation %d" % transID )
    # Change the status of the transformation to deleted
    res = self.transClient.setTransformationParameter( transID, 'Status', 'Deleted' )
    if not res['OK']:
      self.log.error( "Failed to update status of transformation %s to Deleted" % ( transID ), res['Message'] )
      return res
    self.log.info( "Updated status of transformation %s to Deleted" % ( transID ) )
    return S_OK()

  def cleanMetadataCatalogFiles( self, transID, directories=None ):
    """ wipe out files from catalog """
    res = self.metadataClient.findFilesByMetadata( { self.transfidmeta : transID } )
    if not res['OK']:
      return res
    fileToRemove = res['Value']
    if not fileToRemove:
      self.log.info( 'No files found for transID %s' % transID )
      return S_OK()
    res = self.replicaManager.removeFile( fileToRemove )
    if not res['OK']:
      return res
    for lfn, reason in res['Value']['Failed'].items():
      self.log.error( "Failed to remove file found in metadata catalog", "%s %s" % ( lfn, reason ) )
    if res['Value']['Failed']:
      return S_ERROR( "Failed to remove all files found in the metadata catalog" )
    self.log.info( "Successfully removed all files found in the BK" )
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
        self.log.error( "Failed to determine transformation type" )
        return res
      transType = res['Value']
      if transType in [ 'Replication', 'Removal' ]:
        res = self.__removeRequests( externalIDs )
      else:
        res = self.__removeWMSTasks( externalIDs )
      if not res['OK']:
        return res
    return S_OK()

  def __getTransformationExternalIDs( self, transID ):
    """ collect all ExternalIDs for transformation :transID: 
    
    :param self: self reference
    :param int transID: transforamtion ID
    """
    res = self.transClient.getTransformationTasks( condDict = { 'TransformationID' : transID } )
    if not res['OK']:
      self.log.error( "Failed to get externalIDs for transformation %d" % transID, res['Message'] )
      return res
    externalIDs = [ taskDict['ExternalID'] for taskDict in res["Value"] ]
    self.log.info( "Found %d tasks for transformation" % len( externalIDs ) )
    return S_OK( externalIDs )

  def __removeRequests( self, requestIDs ):
    """ dummy method """
    self.log.error( "Not removing requests but should do" )
    return S_OK()

  def __removeWMSTasks( self, transJobIDs ):
    """ wipe out jobs and their requests from the system 
    
    TODO: should check request status, maybe FTS files as well ???

    :param self: self reference
    :param list trasnJobIDs: job IDs
    """
    # Prevent 0 job IDs
    jobIDs = [ int(j) for j in transJobIDs if int(j) ]
    allRemove = True
    for jobList in breakListIntoChunks( jobIDs, 500 ):

      res = self.wmsClient.killJob( jobList )
      if res['OK']:
        self.log.info( "Successfully killed %d jobs from WMS" % len( jobList ) )
      elif ( "InvalidJobIDs" in res ) and ( "NonauthorizedJobIDs" not in res ) and ( "FailedJobIDs" not in res ):
        self.log.info( "Found %s jobs which did not exist in the WMS" % len( res['InvalidJobIDs'] ) )
      elif "NonauthorizedJobIDs" in res:
        self.log.error( "Failed to kill %s jobs because not authorized" % len( res['NonauthorizedJobIDs'] ) )
        allRemove = False
      elif "FailedJobIDs" in res:
        self.log.error( "Failed to kill %s jobs" % len( res['FailedJobIDs'] ) )
        allRemove = False

      res = self.wmsClient.deleteJob( jobList )
      if res['OK']:
        self.log.info( "Successfully removed %d jobs from WMS" % len( jobList ) )
      elif ( "InvalidJobIDs" in res ) and ( "NonauthorizedJobIDs" not in res ) and ( "FailedJobIDs" not in res ):
        self.log.info( "Found %s jobs which did not exist in the WMS" % len( res['InvalidJobIDs'] ) )
      elif "NonauthorizedJobIDs" in res:
        self.log.error( "Failed to remove %s jobs because not authorized" % len( res['NonauthorizedJobIDs'] ) )
        allRemove = False
      elif "FailedJobIDs" in res:
        self.log.error( "Failed to remove %s jobs" % len( res['FailedJobIDs'] ) )
        allRemove = False

    if not allRemove:
      return S_ERROR( "Failed to remove all remnants from WMS" )
    self.log.info( "Successfully removed all tasks from the WMS" )

    if not jobIDs:
      self.log.info("JobIDs not present, unable to remove asociated requests.")
      return S_OK()

    res = self.requestClient.getRequestForJobs( jobIDs )
    if not res['OK']:
      self.log.error( "Failed to get requestID for jobs.", res['Message'] )
      return res
    failoverRequests = res['Value']
    self.log.info( "Found %d jobs with associated failover requests" % len( failoverRequests ) )
    if not failoverRequests:
      return S_OK()
    failed = 0
    for jobID, requestName in failoverRequests.items():
      # Put this check just in case, tasks must have associated jobs
      if jobID == 0 or jobID == '0':
        continue
      res = self.requestClient.deleteRequest( requestName )
      if not res['OK']:
        self.log.error( "Failed to remove request from RequestDB", res['Message'] )
        failed += 1
      else:
        self.log.verbose( "Removed request %s associated to job %d." % ( requestName, jobID ) )
    if failed:
      self.log.info( "Successfully removed %s requests" % ( len( failoverRequests ) - failed ) )
      self.log.info( "Failed to remove %s requests" % failed )
      return S_ERROR( "Failed to remove all the request from RequestDB" )
    self.log.info( "Successfully removed all the associated failover requests" )
    return S_OK()
