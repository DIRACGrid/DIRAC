"""
This is the Data Integrity Client which allows the simple reporting of
problematic file and replicas to the IntegrityDB and their status
correctly updated in the FileCatalog.
"""

__RCSID__ = "$Id$"

from DIRAC                                                import S_OK, S_ERROR, gLogger, gConfig
from DIRAC.DataManagementSystem.Client.DataManager        import DataManager
from DIRAC.Resources.Storage.StorageElement               import StorageElement
from DIRAC.Resources.Catalog.FileCatalog                  import FileCatalog
from DIRAC.Core.Utilities.ReturnValues                    import returnSingleResult
from DIRAC.Core.Utilities.List                            import sortList
from DIRAC.Core.Base.Client                               import Client
import re, os, types

class DataIntegrityClient( Client ):

  """
  The following methods are supported in the service but are not mentioned explicitly here:

          getProblematic()
             Obtains a problematic file from the IntegrityDB based on the LastUpdate time

          getPrognosisProblematics(prognosis)
            Obtains all the problematics of a particular prognosis from the integrityDB

          getProblematicsSummary()
            Obtains a count of the number of problematics for each prognosis found

          getDistinctPrognosis()
            Obtains the distinct prognosis found in the integrityDB

          getTransformationProblematics(prodID)
            Obtains the problematics for a given production

          incrementProblematicRetry(fileID)
            Increments the retry count for the supplied file ID

          changeProblematicPrognosis(fileID,newPrognosis)
            Changes the prognosis of the supplied file to the new prognosis

          setProblematicStatus(fileID,status)
            Updates the status of a problematic in the integrityDB

          removeProblematic(self,fileID)
            This removes the specified file ID from the integrity DB

          insertProblematic(sourceComponent,fileMetadata)
            Inserts file with supplied metadata into the integrity DB

  """

  def __init__( self, **kwargs ):

    Client.__init__( self, **kwargs )
    self.setServer( 'DataManagement/DataIntegrity' )
    self.dm = DataManager()
    self.fc = FileCatalog()

  ##########################################################################
  #
  # This section contains the specific methods for LFC->SE checks
  #

  def catalogDirectoryToSE( self, lfnDir ):
    """ This obtains the replica and metadata information from the catalog for the supplied directory and checks against the storage elements.
    """
    gLogger.info( "-" * 40 )
    gLogger.info( "Performing the LFC->SE check" )
    gLogger.info( "-" * 40 )
    if type( lfnDir ) in types.StringTypes:
      lfnDir = [lfnDir]
    res = self.__getCatalogDirectoryContents( lfnDir )
    if not res['OK']:
      return res
    replicas = res['Value']['Replicas']
    catalogMetadata = res['Value']['Metadata']
    res = self.__checkPhysicalFiles( replicas, catalogMetadata )
    if not res['OK']:
      return res
    resDict = {'CatalogMetadata':catalogMetadata, 'CatalogReplicas':replicas}
    return S_OK( resDict )

  def catalogFileToSE( self, lfns ):
    """ This obtains the replica and metadata information from the catalog and checks against the storage elements.
    """
    gLogger.info( "-" * 40 )
    gLogger.info( "Performing the LFC->SE check" )
    gLogger.info( "-" * 40 )
    if type( lfns ) in types.StringTypes:
      lfns = [lfns]
    res = self.__getCatalogMetadata( lfns )
    if not res['OK']:
      return res
    catalogMetadata = res['Value']
    res = self.__getCatalogReplicas( catalogMetadata.keys() )
    if not res['OK']:
      return res
    replicas = res['Value']
    res = self.__checkPhysicalFiles( replicas, catalogMetadata )
    if not res['OK']:
      return res
    resDict = {'CatalogMetadata':catalogMetadata, 'CatalogReplicas':replicas}
    return S_OK( resDict )

  def checkPhysicalFiles( self, replicas, catalogMetadata, ses = [] ):
    """ This obtains takes the supplied replica and metadata information obtained from the catalog and checks against the storage elements.
    """
    gLogger.info( "-" * 40 )
    gLogger.info( "Performing the LFC->SE check" )
    gLogger.info( "-" * 40 )
    return self.__checkPhysicalFiles( replicas, catalogMetadata, ses = ses )

  def __checkPhysicalFiles( self, replicas, catalogMetadata, ses = [] ):
    """ This obtains the physical file metadata and checks the metadata against the catalog entries
    """
    seLfns = {}
    for lfn, replicaDict in replicas.items():
      for se, _url in replicaDict.items():
        if ( ses ) and ( se not in ses ):
          continue
        seLfns.setdefault( se, [] ).append( lfn )
    gLogger.info( '%s %s' % ( 'Storage Element'.ljust( 20 ), 'Replicas'.rjust( 20 ) ) )



    for se in sortList( seLfns ):
      files = len( seLfns[se] )
      gLogger.info( '%s %s' % ( se.ljust( 20 ), str( files ).rjust( 20 ) ) )

      lfns = seLfns[se]
      sizeMismatch = []
      res = self.__checkPhysicalFileMetadata( lfns, se )
      if not res['OK']:
        gLogger.error( 'Failed to get physical file metadata.', res['Message'] )
        return res
      for lfn, metadata in res['Value'].items():
        if lfn in catalogMetadata:
          if ( metadata['Size'] != catalogMetadata[lfn]['Size'] ) and ( metadata['Size'] != 0 ):
            sizeMismatch.append( ( lfn, 'deprecatedUrl', se, 'CatalogPFNSizeMismatch' ) )
      if sizeMismatch:
        self.__reportProblematicReplicas( sizeMismatch, se, 'CatalogPFNSizeMismatch' )
    return S_OK()

  def __checkPhysicalFileMetadata( self, lfns, se ):
    """ Check obtain the physical file metadata and check the files are available
    """
    gLogger.info( 'Checking the integrity of %s physical files at %s' % ( len( lfns ), se ) )


    res = StorageElement( se ).getFileMetadata( lfns )

    if not res['OK']:
      gLogger.error( 'Failed to get metadata for lfns.', res['Message'] )
      return res
    lfnMetadataDict = res['Value']['Successful']
    # If the replicas are completely missing
    missingReplicas = []
    for lfn, reason in res['Value']['Failed'].items():
      if re.search( 'File does not exist', reason ):
        missingReplicas.append( ( lfn, 'deprecatedUrl', se, 'PFNMissing' ) )
    if missingReplicas:
      self.__reportProblematicReplicas( missingReplicas, se, 'PFNMissing' )
    lostReplicas = []
    unavailableReplicas = []
    zeroSizeReplicas = []
    # If the files are not accessible
    for lfn, lfnMetadata in lfnMetadataDict.items():
      if lfnMetadata['Lost']:
        lostReplicas.append( ( lfn, 'deprecatedUrl', se, 'PFNLost' ) )
      if lfnMetadata['Unavailable']:
        unavailableReplicas.append( ( lfn, 'deprecatedUrl', se, 'PFNUnavailable' ) )
      if lfnMetadata['Size'] == 0:
        zeroSizeReplicas.append( ( lfn, 'deprecatedUrl', se, 'PFNZeroSize' ) )
    if lostReplicas:
      self.__reportProblematicReplicas( lostReplicas, se, 'PFNLost' )
    if unavailableReplicas:
      self.__reportProblematicReplicas( unavailableReplicas, se, 'PFNUnavailable' )
    if zeroSizeReplicas:
      self.__reportProblematicReplicas( zeroSizeReplicas, se, 'PFNZeroSize' )
    gLogger.info( 'Checking the integrity of physical files at %s complete' % se )
    return S_OK( lfnMetadataDict )

  ##########################################################################
  #
  # This section contains the specific methods for SE->LFC checks
  #

  def storageDirectoryToCatalog( self, lfnDir, storageElement ):
    """ This obtains the file found on the storage element in the supplied directories and determines whether they exist in the catalog and checks their metadata elements
    """
    gLogger.info( "-" * 40 )
    gLogger.info( "Performing the SE->LFC check at %s" % storageElement )
    gLogger.info( "-" * 40 )
    if type( lfnDir ) in types.StringTypes:
      lfnDir = [lfnDir]
    res = self.__getStorageDirectoryContents( lfnDir, storageElement )
    if not res['OK']:
      return res
    storageFileMetadata = res['Value']
    if storageFileMetadata:
      return self.__checkCatalogForSEFiles( storageFileMetadata, storageElement )
    return S_OK( {'CatalogMetadata':{}, 'StorageMetadata':{}} )

  def __checkCatalogForSEFiles( self, storageMetadata, storageElement ):
    gLogger.info( 'Checking %s storage files exist in the catalog' % len( storageMetadata ) )


    res = self.fc.getReplicas( storageMetadata )
    if not res['OK']:
      gLogger.error( "Failed to get replicas for LFN", res['Message'] )
      return res
    failedLfns = res['Value']['Failed']
    successfulLfns = res['Value']['Successful']
    notRegisteredLfns = []
    
    for lfn in storageMetadata:
      if lfn in failedLfns: 
        if 'No such file or directory' in failedLfns[lfn]:
          notRegisteredLfns.append( ( lfn, 'deprecatedUrl', storageElement, 'LFNNotRegistered' ) )
          failedLfns.pop( lfn )
      elif storageElement not in successfulLfns[lfn]:
        notRegisteredLfns.append( ( lfn, 'deprecatedUrl', storageElement, 'LFNNotRegistered' ) )

    if notRegisteredLfns:
      self.__reportProblematicReplicas( notRegisteredLfns, storageElement, 'LFNNotRegistered' )
    if failedLfns:
      return S_ERROR( 'Failed to obtain replicas' )


    # For the LFNs found to be registered obtain the file metadata from the catalog and verify against the storage metadata
    res = self.__getCatalogMetadata( storageMetadata )
    if not res['OK']:
      return res
    catalogMetadata = res['Value']
    sizeMismatch = []
    for lfn, lfnCatalogMetadata in catalogMetadata.items():
      lfnStorageMetadata = storageMetadata[lfn]
      if ( lfnStorageMetadata['Size'] != lfnCatalogMetadata['Size'] ) and ( lfnStorageMetadata['Size'] != 0 ):
        sizeMismatch.append( ( lfn, 'deprecatedUrl', storageElement, 'CatalogPFNSizeMismatch' ) )
    if sizeMismatch:
      self.__reportProblematicReplicas( sizeMismatch, storageElement, 'CatalogPFNSizeMismatch' )
    gLogger.info( 'Checking storage files exist in the catalog complete' )
    resDict = {'CatalogMetadata':catalogMetadata, 'StorageMetadata':storageMetadata}
    return S_OK( resDict )

  def getStorageDirectoryContents( self, lfnDir, storageElement ):
    """ This obtains takes the supplied lfn directories and recursively obtains the files in the supplied storage element
    """
    return self.__getStorageDirectoryContents( lfnDir, storageElement )

  def __getStorageDirectoryContents( self, lfnDir, storageElement ):
    """ Obtians the contents of the supplied directory on the storage
    """
    gLogger.info( 'Obtaining the contents for %s directories at %s' % ( len( lfnDir ), storageElement ) )

    se = StorageElement( storageElement )

    res = se.exists( lfnDir )
    if not res['OK']:
      gLogger.error( "Failed to obtain existance of directories", res['Message'] )
      return res
    for directory, error in res['Value']['Failed'].items():
      gLogger.error( 'Failed to determine existance of directory', '%s %s' % ( directory, error ) )
    if res['Value']['Failed']:
      return S_ERROR( 'Failed to determine existance of directory' )
    directoryExists = res['Value']['Successful']
    activeDirs = []
    for directory in sorted( directoryExists ):
      exists = directoryExists[directory]
      if exists:
        activeDirs.append( directory )
    allFiles = {}
    while len( activeDirs ) > 0:
      currentDir = activeDirs[0]
      res = se.listDirectory( currentDir )
      activeDirs.remove( currentDir )
      if not res['OK']:
        gLogger.error( 'Failed to get directory contents', res['Message'] )
        return res
      elif currentDir in res['Value']['Failed']:
        gLogger.error( 'Failed to get directory contents', '%s %s' % ( currentDir, res['Value']['Failed'][currentDir] ) )
        return S_ERROR( res['Value']['Failed'][currentDir] )
      else:
        dirContents = res['Value']['Successful'][currentDir]
        activeDirs.extend( se.getLFNFromURL( dirContents['SubDirs'] ).get( 'Value', {} ).get( 'Successful', [] ) )
        fileURLMetadata = dirContents['Files']
        fileMetadata = {}
        res = se.getLFNFromURL( fileURLMetadata )
        if not res['OK']:
          gLogger.error( 'Failed to get directory content LFNs', res['Message'] )
          return res

        for url, error in res['Value']['Failed'].items():
          gLogger.error( "Failed to get LFN for URL", "%s %s" % ( url, error ) )
        if res['Value']['Failed']:
          return S_ERROR( "Failed to get LFNs for PFNs" )
        urlLfns = res['Value']['Successful']
        for urlLfn, lfn in urlLfns.items():
          fileMetadata[lfn] = fileURLMetadata[urlLfn]
        allFiles.update( fileMetadata )

    zeroSizeFiles = []

    for lfn in sorted( allFiles ):
      if os.path.basename( lfn ) == 'dirac_directory':
        allFiles.pop( lfn )
      else:
        metadata = allFiles[lfn]
        if metadata['Size'] == 0:
          zeroSizeFiles.append( ( lfn, 'deprecatedUrl', storageElement, 'PFNZeroSize' ) )
    if zeroSizeFiles:
      self.__reportProblematicReplicas( zeroSizeFiles, storageElement, 'PFNZeroSize' )

    gLogger.info( 'Obtained at total of %s files for directories at %s' % ( len( allFiles ), storageElement ) )
    return S_OK( allFiles )

  def __getStoragePathExists( self, lfnPaths, storageElement ):
    gLogger.info( 'Determining the existance of %d files at %s' % ( len( lfnPaths ), storageElement ) )

    se = StorageElement( storageElement )

    res = se.exists( lfnPaths )
    if not res['OK']:
      gLogger.error( "Failed to obtain existance of paths", res['Message'] )
      return res
    for lfnPath, error in res['Value']['Failed'].items():
      gLogger.error( 'Failed to determine existance of path', '%s %s' % ( lfnPath, error ) )
    if res['Value']['Failed']:
      return S_ERROR( 'Failed to determine existance of paths' )
    pathExists = res['Value']['Successful']
    resDict = {}
    for lfn, exists in pathExists.items():
      if exists:
        resDict[lfn] = True
    return S_OK( resDict )

  ##########################################################################
  #
  # This section contains the specific methods for obtaining replica and metadata information from the catalog
  #

  def __getCatalogDirectoryContents( self, lfnDir ):
    """ Obtain the contents of the supplied directory
    """
    gLogger.info( 'Obtaining the catalog contents for %s directories' % len( lfnDir ) )

    activeDirs = lfnDir
    allFiles = {}
    while len( activeDirs ) > 0:
      currentDir = activeDirs[0]
      res = self.fc.listDirectory( currentDir )
      activeDirs.remove( currentDir )
      if not res['OK']:
        gLogger.error( 'Failed to get directory contents', res['Message'] )
        return res
      elif res['Value']['Failed'].has_key( currentDir ):
        gLogger.error( 'Failed to get directory contents', '%s %s' % ( currentDir, res['Value']['Failed'][currentDir] ) )
      else:
        dirContents = res['Value']['Successful'][currentDir]
        activeDirs.extend( dirContents['SubDirs'] )
        allFiles.update( dirContents['Files'] )

    zeroReplicaFiles = []
    zeroSizeFiles = []
    allReplicaDict = {}
    allMetadataDict = {}
    for lfn, lfnDict in allFiles.items():
      lfnReplicas = {}
      for se, replicaDict in lfnDict['Replicas'].items():
        lfnReplicas[se] = replicaDict['PFN']
      if not lfnReplicas:
        zeroReplicaFiles.append( lfn )
      allReplicaDict[lfn] = lfnReplicas
      allMetadataDict[lfn] = lfnDict['MetaData']
      if lfnDict['MetaData']['Size'] == 0:
        zeroSizeFiles.append( lfn )
    if zeroReplicaFiles:
      self.__reportProblematicFiles( zeroReplicaFiles, 'LFNZeroReplicas' )
    if zeroSizeFiles:
      self.__reportProblematicFiles( zeroSizeFiles, 'LFNZeroSize' )
    gLogger.info( 'Obtained at total of %s files for the supplied directories' % len( allMetadataDict ) )
    resDict = {'Metadata':allMetadataDict, 'Replicas':allReplicaDict}
    return S_OK( resDict )

  def __getCatalogReplicas( self, lfns ):
    """ Obtain the file replicas from the catalog while checking that there are replicas
    """
    gLogger.info( 'Obtaining the replicas for %s files' % len( lfns ) )

    zeroReplicaFiles = []
    res = self.fc.getReplicas( lfns, allStatus = True )
    if not res['OK']:
      gLogger.error( 'Failed to get catalog replicas', res['Message'] )
      return res
    allReplicas = res['Value']['Successful']
    for lfn, error in res['Value']['Failed'].items():
      if re.search( 'File has zero replicas', error ):
        zeroReplicaFiles.append( lfn )
    if zeroReplicaFiles:
      self.__reportProblematicFiles( zeroReplicaFiles, 'LFNZeroReplicas' )
    gLogger.info( 'Obtaining the replicas for files complete' )
    return S_OK( allReplicas )

  def __getCatalogMetadata( self, lfns ):
    """ Obtain the file metadata from the catalog while checking they exist
    """
    if not lfns:
      return S_OK( {} )
    gLogger.info( 'Obtaining the catalog metadata for %s files' % len( lfns ) )

    missingCatalogFiles = []
    zeroSizeFiles = []
    res = self.fc.getFileMetadata( lfns )
    if not res['OK']:
      gLogger.error( 'Failed to get catalog metadata', res['Message'] )
      return res
    allMetadata = res['Value']['Successful']
    for lfn, error in res['Value']['Failed'].items():
      if re.search( 'No such file or directory', error ):
        missingCatalogFiles.append( lfn )
    if missingCatalogFiles:
      self.__reportProblematicFiles( missingCatalogFiles, 'LFNCatalogMissing' )
    for lfn, metadata in allMetadata.items():
      if metadata['Size'] == 0:
        zeroSizeFiles.append( lfn )
    if zeroSizeFiles:
      self.__reportProblematicFiles( zeroSizeFiles, 'LFNZeroSize' )
    gLogger.info( 'Obtaining the catalog metadata complete' )
    return S_OK( allMetadata )

  ##########################################################################
  #
  # This section contains the methods for inserting problematic files into the integrity DB
  #

  def __reportProblematicFiles( self, lfns, reason ):
    """ Simple wrapper function around setFileProblematic """
    gLogger.info( 'The following %s files were found with %s' % ( len( lfns ), reason ) )
    for lfn in sortList( lfns ):
      gLogger.info( lfn )
    res = self.setFileProblematic( lfns, reason, sourceComponent = 'DataIntegrityClient' )
    if not res['OK']:
      gLogger.info( 'Failed to update integrity DB with files', res['Message'] )
    else:
      gLogger.info( 'Successfully updated integrity DB with files' )

  def setFileProblematic( self, lfn, reason, sourceComponent = '' ):
    """ This method updates the status of the file in the FileCatalog and the IntegrityDB

        lfn - the lfn of the file
        reason - this is given to the integrity DB and should reflect the problem observed with the file

        sourceComponent is the component issuing the request.
    """
    if type( lfn ) == types.ListType:
      lfns = lfn
    elif type( lfn ) == types.StringType:
      lfns = [lfn]
    else:
      errStr = "DataIntegrityClient.setFileProblematic: Supplied file info must be list or a single LFN."
      gLogger.error( errStr )
      return S_ERROR( errStr )
    gLogger.info( "DataIntegrityClient.setFileProblematic: Attempting to update %s files." % len( lfns ) )
    fileMetadata = {}
    for lfn in lfns:
      fileMetadata[lfn] = {'Prognosis':reason, 'LFN':lfn, 'PFN':'', 'SE':''}
    res = self.insertProblematic( sourceComponent, fileMetadata )
    if not res['OK']:
      gLogger.error( "DataIntegrityClient.setReplicaProblematic: Failed to insert problematics to integrity DB" )
    return res

  def __reportProblematicReplicas( self, replicaTuple, se, reason ):
    """ Simple wrapper function around setReplicaProblematic """
    gLogger.info( 'The following %s files had %s at %s' % ( len( replicaTuple ), reason, se ) )
    for lfn, _pfn, se, reason in sortList( replicaTuple ):
      if lfn:
        gLogger.info( lfn )
    res = self.setReplicaProblematic( replicaTuple, sourceComponent = 'DataIntegrityClient' )
    if not res['OK']:
      gLogger.info( 'Failed to update integrity DB with replicas', res['Message'] )
    else:
      gLogger.info( 'Successfully updated integrity DB with replicas' )

  def setReplicaProblematic( self, replicaTuple, sourceComponent = '' ):
    """ This method updates the status of the replica in the FileCatalog and the IntegrityDB
        The supplied replicaDict should be of the form {lfn :{'PFN':pfn,'SE':se,'Prognosis':prognosis}

        lfn - the lfn of the file
        pfn - the pfn if available (otherwise '')
        se - the storage element of the problematic replica (otherwise '')
        prognosis - this is given to the integrity DB and should reflect the problem observed with the file

        sourceComponent is the component issuing the request.
    """
    if type( replicaTuple ) == types.TupleType:
      replicaTuple = [replicaTuple]
    elif type( replicaTuple ) == types.ListType:
      pass
    else:
      errStr = "DataIntegrityClient.setReplicaProblematic: Supplied replica info must be a tuple or list of tuples."
      gLogger.error( errStr )
      return S_ERROR( errStr )
    gLogger.info( "DataIntegrityClient.setReplicaProblematic: Attempting to update %s replicas." % len( replicaTuple ) )
    replicaDict = {}
    for lfn, pfn, se, reason in replicaTuple:
      replicaDict[lfn] = {'Prognosis':reason, 'LFN':lfn, 'PFN':pfn, 'SE':se}
    res = self.insertProblematic( sourceComponent, replicaDict )
    if not res['OK']:
      gLogger.error( "DataIntegrityClient.setReplicaProblematic: Failed to insert problematic to integrity DB" )
      return res
    for lfn in replicaDict.keys():
      replicaDict[lfn]['Status'] = 'Problematic'

    res = self.fc.setReplicaStatus( replicaDict )
    if not res['OK']:
      errStr = "DataIntegrityClient.setReplicaProblematic: Completely failed to update replicas."
      gLogger.error( errStr, res['Message'] )
      return res
    failed = res['Value']['Failed']
    successful = res['Value']['Successful']
    resDict = {'Successful':successful, 'Failed':failed}
    return S_OK( resDict )

  ##########################################################################
  #
  # This section contains the resolution methods for various prognoses
  #

  def __updateCompletedFiles( self, prognosis, fileID ):
    gLogger.info( "%s file (%d) is resolved" % ( prognosis, fileID ) )
    return self.setProblematicStatus( fileID, 'Resolved' )

  def __returnProblematicError( self, fileID, res ):
    self.incrementProblematicRetry( fileID )
    gLogger.error( 'DataIntegrityClient failure', res['Message'] )
    return res

#   def __getRegisteredPFNLFN( self, pfn, storageElement ):
#
#     res = StorageElement( storageElement ).getURL( pfn )
#     if not res['OK']:
#       gLogger.error( "Failed to get registered PFN for physical files", res['Message'] )
#       return res
#     for pfn, error in res['Value']['Failed'].items():
#       gLogger.error( 'Failed to obtain registered PFN for physical file', '%s %s' % ( pfn, error ) )
#       return S_ERROR( 'Failed to obtain registered PFNs from physical file' )
#     registeredPFN = res['Value']['Successful'][pfn]
#     res = returnSingleResult( self.fc.getLFNForPFN( registeredPFN ) )
#     if ( not res['OK'] ) and re.search( 'No such file or directory', res['Message'] ):
#       return S_OK( False )
#     return S_OK( res['Value'] )

  def __updateReplicaToChecked( self, problematicDict ):
    lfn = problematicDict['LFN']
    fileID = problematicDict['FileID']
    prognosis = problematicDict['Prognosis']
    problematicDict['Status'] = 'Checked'

    res = returnSingleResult( self.fc.setReplicaStatus( {lfn:problematicDict} ) )

    if not res['OK']:
      return self.__returnProblematicError( fileID, res )
    gLogger.info( "%s replica (%d) is updated to Checked status" % ( prognosis, fileID ) )
    return self.__updateCompletedFiles( prognosis, fileID )

  def resolveCatalogPFNSizeMismatch( self, problematicDict ):
    """ This takes the problematic dictionary returned by the integrity DB and resolved the CatalogPFNSizeMismatch prognosis
    """
    lfn = problematicDict['LFN']
    se = problematicDict['SE']
    fileID = problematicDict['FileID']


    res = returnSingleResult( self.fc.getFileSize( lfn ) )
    if not res['OK']:
      return self.__returnProblematicError( fileID, res )
    catalogSize = res['Value']
    res = returnSingleResult( StorageElement( se ).getFileSize( lfn ) )
    if not res['OK']:
      return self.__returnProblematicError( fileID, res )
    storageSize = res['Value']
    bkKCatalog = FileCatalog( ['BookkeepingDB'] )
    res = returnSingleResult( bkKCatalog.getFileSize( lfn ) )
    if not res['OK']:
      return self.__returnProblematicError( fileID, res )
    bookkeepingSize = res['Value']
    if bookkeepingSize == catalogSize == storageSize:
      gLogger.info( "CatalogPFNSizeMismatch replica (%d) matched all registered sizes." % fileID )
      return self.__updateReplicaToChecked( problematicDict )
    if ( catalogSize == bookkeepingSize ):
      gLogger.info( "CatalogPFNSizeMismatch replica (%d) found to mismatch the bookkeeping also" % fileID )
      res = returnSingleResult( self.fc.getReplicas( lfn ) )
      if not res['OK']:
        return self.__returnProblematicError( fileID, res )
      if len( res['Value'] ) <= 1:
        gLogger.info( "CatalogPFNSizeMismatch replica (%d) has no other replicas." % fileID )
        return S_ERROR( "Not removing catalog file mismatch since the only replica" )
      else:
        gLogger.info( "CatalogPFNSizeMismatch replica (%d) has other replicas. Removing..." % fileID )
        res = self.dm.removeReplica( se, lfn )
        if not res['OK']:
          return self.__returnProblematicError( fileID, res )
        return self.__updateCompletedFiles( 'CatalogPFNSizeMismatch', fileID )
    if ( catalogSize != bookkeepingSize ) and ( bookkeepingSize == storageSize ):
      gLogger.info( "CatalogPFNSizeMismatch replica (%d) found to match the bookkeeping size" % fileID )
      res = self.__updateReplicaToChecked( problematicDict )
      if not res['OK']:
        return self.__returnProblematicError( fileID, res )
      return self.changeProblematicPrognosis( fileID, 'BKCatalogSizeMismatch' )
    gLogger.info( "CatalogPFNSizeMismatch replica (%d) all sizes found mismatch. Updating retry count" % fileID )
    return self.incrementProblematicRetry( fileID )

  def resolvePFNNotRegistered( self, problematicDict ):
    """ This takes the problematic dictionary returned by the integrity DB and resolved the PFNNotRegistered prognosis
    """
    lfn = problematicDict['LFN']
    seName = problematicDict['SE']
    fileID = problematicDict['FileID']

    se = StorageElement( seName )
    res = returnSingleResult( self.fc.exists( lfn ) )
    if not res['OK']:
      return self.__returnProblematicError( fileID, res )
    if not res['Value']:
      # The file does not exist in the catalog
      res = returnSingleResult( se.removeFile( lfn ) )
      if not res['OK']:
        return self.__returnProblematicError( fileID, res )
      return self.__updateCompletedFiles( 'PFNNotRegistered', fileID )
    res = returnSingleResult( se.getFileMetadata( lfn ) )
    if ( not res['OK'] ) and ( re.search( 'File does not exist', res['Message'] ) ):
      gLogger.info( "PFNNotRegistered replica (%d) found to be missing." % fileID )
      return self.__updateCompletedFiles( 'PFNNotRegistered', fileID )
    elif not res['OK']:
      return self.__returnProblematicError( fileID, res )
    storageMetadata = res['Value']
    if storageMetadata['Lost']:
      gLogger.info( "PFNNotRegistered replica (%d) found to be Lost. Updating prognosis" % fileID )
      return self.changeProblematicPrognosis( fileID, 'PFNLost' )
    if storageMetadata['Unavailable']:
      gLogger.info( "PFNNotRegistered replica (%d) found to be Unavailable. Updating retry count" % fileID )
      return self.incrementProblematicRetry( fileID )

    # HACK until we can obtain the space token descriptions through GFAL
    site = seName.split( '_' )[0].split( '-' )[0]
    if not storageMetadata['Cached']:
      if lfn.endswith( '.raw' ):
        seName = '%s-RAW' % site
      else:
        seName = '%s-RDST' % site
    elif storageMetadata['Migrated']:
      if lfn.startswith( '/lhcb/data' ):
        seName = '%s_M-DST' % site
      else:
        seName = '%s_MC_M-DST' % site
    else:
      if lfn.startswith( '/lhcb/data' ):
        seName = '%s-DST' % site
      else:
        seName = '%s_MC-DST' % site

    problematicDict['SE'] = seName
    res = returnSingleResult( se.getURL( lfn ) )
    if not res['OK']:
      return self.__returnProblematicError( fileID, res )

    problematicDict['PFN'] = res['Value']

    res = returnSingleResult( self.fc.addReplica( {lfn:problematicDict} ) )
    if not res['OK']:
      return self.__returnProblematicError( fileID, res )
    res = returnSingleResult( self.fc.getFileMetadata( lfn ) )
    if not res['OK']:
      return self.__returnProblematicError( fileID, res )
    if res['Value']['Size'] != storageMetadata['Size']:
      gLogger.info( "PFNNotRegistered replica (%d) found with catalog size mismatch. Updating prognosis" % fileID )
      return self.changeProblematicPrognosis( fileID, 'CatalogPFNSizeMismatch' )
    return self.__updateCompletedFiles( 'PFNNotRegistered', fileID )

  def resolveLFNCatalogMissing( self, problematicDict ):
    """ This takes the problematic dictionary returned by the integrity DB and resolved the LFNCatalogMissing prognosis
    """
    lfn = problematicDict['LFN']
    fileID = problematicDict['FileID']

    res = returnSingleResult( self.fc.exists( lfn ) )
    if not res['OK']:
      return self.__returnProblematicError( fileID, res )
    if res['Value']:
      return self.__updateCompletedFiles( 'LFNCatalogMissing', fileID )
    # Remove the file from all catalogs
    # RF_NOTE : here I can do it because it's a single file, but otherwise I would need to sort the path
    res = returnSingleResult( self.fc.removeFile( lfn ) )
    if not res['OK']:
      return self.__returnProblematicError( fileID, res )
    return self.__updateCompletedFiles( 'LFNCatalogMissing', fileID )

  def resolvePFNMissing( self, problematicDict ):
    """ This takes the problematic dictionary returned by the integrity DB and resolved the PFNMissing prognosis
    """
    se = problematicDict['SE']
    lfn = problematicDict['LFN']
    fileID = problematicDict['FileID']

    res = returnSingleResult( self.fc.exists( lfn ) )
    if not res['OK']:
      return self.__returnProblematicError( fileID, res )
    if not res['Value']:
      gLogger.info( "PFNMissing file (%d) no longer exists in catalog" % fileID )
      return self.__updateCompletedFiles( 'PFNMissing', fileID )

    res = returnSingleResult( StorageElement( se ).exists( lfn ) )
    if not res['OK']:
      return self.__returnProblematicError( fileID, res )
    if res['Value']:
      gLogger.info( "PFNMissing replica (%d) is no longer missing" % fileID )
      return self.__updateReplicaToChecked( problematicDict )
    gLogger.info( "PFNMissing replica (%d) does not exist" % fileID )
    res = returnSingleResult( self.fc.getReplicas( lfn, allStatus = True ) )
    if not res['OK']:
      return self.__returnProblematicError( fileID, res )
    replicas = res['Value']
    seSite = se.split( '_' )[0].split( '-' )[0]
    found = False
    print replicas
    for replicaSE in replicas.keys():
      if re.search( seSite, replicaSE ):
        found = True
        problematicDict['SE'] = replicaSE
        se = replicaSE
    if not found:
      gLogger.info( "PFNMissing replica (%d) is no longer registered at SE. Resolved." % fileID )
      return self.__updateCompletedFiles( 'PFNMissing', fileID )
    gLogger.info( "PFNMissing replica (%d) does not exist. Removing from catalog..." % fileID )
    res = returnSingleResult( self.fc.removeReplica( {lfn:problematicDict} ) )
    if not res['OK']:
      return self.__returnProblematicError( fileID, res )
    if len( replicas ) == 1:
      gLogger.info( "PFNMissing replica (%d) had a single replica. Updating prognosis" % fileID )
      return self.changeProblematicPrognosis( fileID, 'LFNZeroReplicas' )
    res = self.dm.replicateAndRegister( problematicDict['LFN'], se )
    if not res['OK']:
      return self.__returnProblematicError( fileID, res )
    # If we get here the problem is solved so we can update the integrityDB
    return self.__updateCompletedFiles( 'PFNMissing', fileID )

  def resolvePFNUnavailable( self, problematicDict ):
    """ This takes the problematic dictionary returned by the integrity DB and resolved the PFNUnavailable prognosis
    """
    lfn = problematicDict['LFN']
    se = problematicDict['SE']
    fileID = problematicDict['FileID']

    res = returnSingleResult( StorageElement( se ).getFileMetadata( lfn ) )
    if ( not res['OK'] ) and ( re.search( 'File does not exist', res['Message'] ) ):
      # The file is no longer Unavailable but has now dissapeared completely
      gLogger.info( "PFNUnavailable replica (%d) found to be missing. Updating prognosis" % fileID )
      return self.changeProblematicPrognosis( fileID, 'PFNMissing' )
    if ( not res['OK'] ) or res['Value']['Unavailable']:
      gLogger.info( "PFNUnavailable replica (%d) found to still be Unavailable" % fileID )
      return self.incrementProblematicRetry( fileID )
    if res['Value']['Lost']:
      gLogger.info( "PFNUnavailable replica (%d) is now found to be Lost. Updating prognosis" % fileID )
      return self.changeProblematicPrognosis( fileID, 'PFNLost' )
    gLogger.info( "PFNUnavailable replica (%d) is no longer Unavailable" % fileID )
    # Need to make the replica okay in the Catalog
    return self.__updateReplicaToChecked( problematicDict )

  def resolvePFNZeroSize( self, problematicDict ):
    """ This takes the problematic dictionary returned by the integrity DB and resolves the PFNZeroSize prognosis
    """
    lfn = problematicDict['LFN']
    seName = problematicDict['SE']
    fileID = problematicDict['FileID']

    se = StorageElement( seName )

    res = returnSingleResult( se.getFileSize( lfn ) )
    if ( not res['OK'] ) and ( re.search( 'File does not exist', res['Message'] ) ):
      gLogger.info( "PFNZeroSize replica (%d) found to be missing. Updating prognosis" % problematicDict['FileID'] )
      return self.changeProblematicPrognosis( fileID, 'PFNMissing' )
    storageSize = res['Value']
    if storageSize == 0:
      res = returnSingleResult( se.removeFile( lfn ) )

      if not res['OK']:
        return self.__returnProblematicError( fileID, res )
      gLogger.info( "PFNZeroSize replica (%d) removed. Updating prognosis" % problematicDict['FileID'] )
      return self.changeProblematicPrognosis( fileID, 'PFNMissing' )


    res = returnSingleResult( self.fc.getReplicas( lfn ) )
    if not res['OK']:
      return self.__returnProblematicError( fileID, res )
    if seName not in res['Value']:
      gLogger.info( "PFNZeroSize replica (%d) not registered in catalog. Updating prognosis" % problematicDict['FileID'] )
      return self.changeProblematicPrognosis( fileID, 'PFNNotRegistered' )
    res = returnSingleResult( self.fc.getFileMetadata( lfn ) )

    if not res['OK']:
      return self.__returnProblematicError( fileID, res )
    catalogSize = res['Value']['Size']
    if catalogSize != storageSize:
      gLogger.info( "PFNZeroSize replica (%d) size found to differ from registered metadata. Updating prognosis" % problematicDict['FileID'] )
      return self.changeProblematicPrognosis( fileID, 'CatalogPFNSizeMismatch' )
    return self.__updateCompletedFiles( 'PFNZeroSize', fileID )

  ############################################################################################

  def resolveLFNZeroReplicas( self, problematicDict ):
    """ This takes the problematic dictionary returned by the integrity DB and resolves the LFNZeroReplicas prognosis
    """
    lfn = problematicDict['LFN']
    fileID = problematicDict['FileID']

    res = returnSingleResult( self.fc.getReplicas( lfn, allStatus = True ) )
    if res['OK'] and res['Value']:
      gLogger.info( "LFNZeroReplicas file (%d) found to have replicas" % fileID )
    else:
      gLogger.info( "LFNZeroReplicas file (%d) does not have replicas. Checking storage..." % fileID )
      pfnsFound = False
      for storageElementName in sorted( gConfig.getValue( 'Resources/StorageElementGroups/Tier1_MC_M-DST', [] ) ):
        res = self.__getStoragePathExists( [lfn], storageElementName )
        if lfn in res['Value']:
          gLogger.info( "LFNZeroReplicas file (%d) found storage file at %s" % ( fileID, storageElementName ) )
          self.__reportProblematicReplicas( [( lfn, 'deprecatedUrl', storageElementName, 'PFNNotRegistered' )], storageElementName, 'PFNNotRegistered' )
          pfnsFound = True
      if not pfnsFound:
        gLogger.info( "LFNZeroReplicas file (%d) did not have storage files. Removing..." % fileID )
        res = returnSingleResult( self.fc.removeFile( lfn ) )
        if not res['OK']:
          gLogger.error( 'DataIntegrityClient: failed to remove file', res['Message'] )
          # Increment the number of retries for this file
          self.server.incrementProblematicRetry( fileID )
          return res
        gLogger.info( "LFNZeroReplicas file (%d) removed from catalog" % fileID )
    # If we get here the problem is solved so we can update the integrityDB
    return self.__updateCompletedFiles( 'LFNZeroReplicas', fileID )
