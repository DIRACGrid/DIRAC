""" 
:mod: DataManager

.. module: DataManager

:synopsis: DataManager links the functionalities of StorageElement and FileCatalog.

This module consists DataManager and related classes.

"""


# # RSCID
__RCSID__ = "$Id$"
# # imports
from datetime import datetime, timedelta
import fnmatch
import os
import time
from types import StringTypes, ListType, DictType, StringType, TupleType
# # from DIRAC
import DIRAC
from DIRAC import S_OK, S_ERROR, gLogger, gConfig
from DIRAC.ConfigurationSystem.Client.Helpers.Operations import Operations
from DIRAC.AccountingSystem.Client.DataStoreClient import gDataStoreClient
from DIRAC.AccountingSystem.Client.Types.DataOperation import DataOperation
from DIRAC.Core.Utilities.Adler import fileAdler, compareAdler
from DIRAC.Core.Utilities.File import makeGuid, getSize
from DIRAC.Core.Utilities.List import randomize
from DIRAC.Core.Utilities.SiteSEMapping import getSEsForSite, isSameSiteSE, getSEsForCountry
from DIRAC.Resources.Catalog.FileCatalog import FileCatalog
from DIRAC.Resources.Storage.StorageElement import StorageElement
from DIRAC.Resources.Storage.StorageFactory import StorageFactory
from DIRAC.ResourceStatusSystem.Client.ResourceStatus import ResourceStatus
from DIRAC.Core.Security.ProxyInfo import getProxyInfo
from DIRAC.Resources.Utilities import Utils

class DataManager( object ):
  """
  .. class:: DataManager

  A DataManager is taking all the actions that impact or require the FileCatalog and the StorageElement together
  """
  def __init__( self, catalogs = [] ):
    """ c'tor

    :param self: self reference
    """
    self.log = gLogger.getSubLogger( self.__class__.__name__, True )
    self.fc = FileCatalog( catalogs )
    self.accountingClient = None
    self.registrationProtocol = ['SRM2', 'DIP']
    self.thirdPartyProtocols = ['SRM2', 'DIP']
    self.resourceStatus = ResourceStatus()
    self.ignoreMissingInFC = Operations().getValue( 'DataManagement/IgnoreMissingInFC', False )
    self.useCatalogPFN = Operations().getValue( 'DataManagement/UseCatalogPFN', True )

  def setAccountingClient( self, client ):
    """ Set Accounting Client instance
    """
    self.accountingClient = client

  def __verifyOperationWritePermission( self, path ):
    """  Check if we have write permission to the given directory
    """
    if type( path ) in StringTypes:
      paths = [ path ]
    else:
      paths = path

    res = self.fc.getPathPermissions( paths )
    if not res['OK']:
      return res
    for path in paths:
      if not res['Value']['Successful'].get( path, {} ).get( 'Write', False ):
        return S_OK( False )
    return S_OK( True )

  ##########################################################################
  #
  # These are the bulk removal methods
  #

  def cleanLogicalDirectory( self, lfnDir ):
    """ Clean the logical directory from the catalog and storage
    """
    if type( lfnDir ) in StringTypes:
      lfnDir = [ lfnDir ]
    retDict = { "Successful" : {}, "Failed" : {} }
    for folder in lfnDir:
      res = self.__cleanDirectory( folder )
      if not res['OK']:
        self.log.debug( "Failed to clean directory.", "%s %s" % ( folder, res['Message'] ) )
        retDict["Failed"][folder] = res['Message']
      else:
        self.log.debug( "Successfully removed directory.", folder )
        retDict["Successful"][folder] = res['Value']
    return S_OK( retDict )

  def __cleanDirectory( self, folder ):
    """ delete all files from directory :folder: in FileCatalog and StorageElement

    :param self: self reference
    :param str folder: directory name
    """
    res = self.__verifyOperationWritePermission( folder )
    if not res['OK']:
      return res
    if not res['Value']:
      errStr = "__cleanDirectory: Write access not permitted for this credential."
      self.log.debug( errStr, folder )
      return S_ERROR( errStr )
    res = self.__getCatalogDirectoryContents( [ folder ] )
    if not res['OK']:
      return res
    res = self.removeFile( res['Value'].keys() + [ '%s/dirac_directory' % folder ] )
    if not res['OK']:
      return res
    for lfn, reason in res['Value']['Failed'].items():
      gLogger.error( "Failed to remove file found in the catalog", "%s %s" % ( lfn, reason ) )

    storageElements = gConfig.getValue( 'Resources/StorageElementGroups/SE_Cleaning_List', [] )
    failed = False
    for storageElement in sorted( storageElements ):
      res = self.__removeStorageDirectory( folder, storageElement )
      if not res['OK']:
        failed = True
    if failed:
      return S_ERROR( "Failed to clean storage directory at all SEs" )
    res = Utils.executeSingleFileOrDirWrapper( self.fc.removeDirectory( folder, recursive = True ) )
    if not res['OK']:
      return res
    return S_OK()

  def __removeStorageDirectory( self, directory, storageElement ):
    """ delete SE directory

    :param self: self reference
    :param str directory: folder to be removed
    :param str storageElement: DIRAC SE name
    """

    se = StorageElement( storageElement )
    res = Utils.executeSingleFileOrDirWrapper( se.exists( directory ) )

    if not res['OK']:
      self.log.debug( "Failed to obtain existance of directory", res['Message'] )
      return res

    exists = res['Value']
    if not exists:
      self.log.debug( "The directory %s does not exist at %s " % ( directory, storageElement ) )
      return S_OK()

    res = Utils.executeSingleFileOrDirWrapper( se.removeDirectory( directory, recursive = True ) )
    if not res['OK']:
      self.log.debug( "Failed to remove storage directory", res['Message'] )
      return res

    self.log.debug( "Successfully removed %d files from %s at %s" % ( res['Value']['FilesRemoved'],
                                                                    directory,
                                                                    storageElement ) )
    return S_OK()

  def __getCatalogDirectoryContents( self, directories ):
    """ ls recursively all files in directories

    :param self: self reference
    :param list directories: folder names
    """
    self.log.debug( 'Obtaining the catalog contents for %d directories:' % len( directories ) )
    activeDirs = directories
    allFiles = {}
    while len( activeDirs ) > 0:
      currentDir = activeDirs[0]
      res = Utils.executeSingleFileOrDirWrapper( self.fc.listDirectory( currentDir ) )
      activeDirs.remove( currentDir )

      if not res['OK']:
        self.log.debug( "Problem getting the %s directory content" % currentDir, res['Message'] )
      else:
        dirContents = res['Value']
        activeDirs.extend( dirContents['SubDirs'] )
        allFiles.update( dirContents['Files'] )

    self.log.debug( "Found %d files" % len( allFiles ) )
    return S_OK( allFiles )


  def getReplicasFromDirectory( self, directory ):
    """ get all replicas from a given directory

    :param self: self reference
    :param mixed directory: list of directories or one directory
    """
    if type( directory ) in StringTypes:
      directories = [directory]
    else:
      directories = directory
    res = self.__getCatalogDirectoryContents( directories )
    if not res['OK']:
      return res
    allReplicas = {}
    for lfn, metadata in res['Value'].items():
      allReplicas[lfn] = metadata['Replicas']
    return S_OK( allReplicas )

  def getFilesFromDirectory( self, directory, days = 0, wildcard = '*' ):
    """ get all files from :directory: older than :days: days matching to :wildcard:

    :param self: self reference
    :param mixed directory: list of directories or directory name
    :param int days: ctime days
    :param str wildcard: pattern to match
    """
    if type( directory ) in StringTypes:
      directories = [directory]
    else:
      directories = directory
    self.log.debug( "Obtaining the files older than %d days in %d directories:" % ( days, len( directories ) ) )
    for folder in directories:
      self.log.debug( folder )
    activeDirs = directories
    allFiles = []
    while len( activeDirs ) > 0:
      currentDir = activeDirs[0]
      # We only need the metadata (verbose) if a limit date is given
      res = Utils.executeSingleFileOrDirWrapper( self.fc.listDirectory( currentDir, verbose = ( days != 0 ) ) )
      activeDirs.remove( currentDir )
      if not res['OK']:
        self.log.debug( "Error retrieving directory contents", "%s %s" % ( currentDir, res['Message'] ) )
      else:
        dirContents = res['Value']
        subdirs = dirContents['SubDirs']
        files = dirContents['Files']
        self.log.debug( "%s: %d files, %d sub-directories" % ( currentDir, len( files ), len( subdirs ) ) )
        for subdir in subdirs:
          if ( not days ) or self.__isOlderThan( subdirs[subdir]['CreationDate'], days ):
            if subdir[0] != '/':
              subdir = currentDir + '/' + subdir
            activeDirs.append( subdir )
        for fileName in files:
          fileInfo = files[fileName]
          fileInfo = fileInfo.get( 'Metadata', fileInfo )
          if ( not days ) or not fileInfo.get( 'CreationDate' ) or self.__isOlderThan( fileInfo['CreationDate'], days ):
            if wildcard == '*' or fnmatch.fnmatch( fileName, wildcard ):
              fileName = fileInfo.get( 'LFN', fileName )
              allFiles.append( fileName )
    return S_OK( allFiles )

  def __isOlderThan( self, stringTime, days ):
    timeDelta = timedelta( days = days )
    maxCTime = datetime.utcnow() - timeDelta
    # st = time.strptime( stringTime, "%a %b %d %H:%M:%S %Y" )
    # cTimeStruct = datetime( st[0], st[1], st[2], st[3], st[4], st[5], st[6], None )
    cTimeStruct = stringTime
    if cTimeStruct < maxCTime:
      return True
    return False

  ##########################################################################
  #
  # These are the data transfer methods
  #

  def getFile( self, lfn, destinationDir = '' ):
    """ Get a local copy of a LFN from Storage Elements.

        'lfn' is the logical file name for the desired file
    """
    if type( lfn ) == ListType:
      lfns = lfn
    elif type( lfn ) == StringType:
      lfns = [lfn]
    else:
      errStr = "getFile: Supplied lfn must be string or list of strings."
      self.log.debug( errStr )
      return S_ERROR( errStr )
    self.log.debug( "getFile: Attempting to get %s files." % len( lfns ) )
    res = self.getActiveReplicas( lfns )
    if not res['OK']:
      return res
    failed = res['Value']['Failed']
    lfnReplicas = res['Value']['Successful']
    res = self.fc.getFileMetadata( lfnReplicas.keys() )
    if not res['OK']:
      return res
    failed.update( res['Value']['Failed'] )
    fileMetadata = res['Value']['Successful']
    successful = {}
    for lfn in fileMetadata:
      res = self.__getFile( lfn, lfnReplicas[lfn], fileMetadata[lfn], destinationDir )
      if not res['OK']:
        failed[lfn] = res['Message']
      else:
        successful[lfn] = res['Value']

    gDataStoreClient.commit()
    return S_OK( { 'Successful': successful, 'Failed' : failed } )

  def __getFile( self, lfn, replicas, metadata, destinationDir ):
    if not replicas:
      self.log.debug( "No accessible replicas found" )
      return S_ERROR( "No accessible replicas found" )
    # Determine the best replicas
    res = self._getSEProximity( replicas.keys() )
    if not res['OK']:
      return res
    for storageElementName in res['Value']:
      se = StorageElement( storageElementName )
      physicalFile = replicas[storageElementName]

      oDataOperation = self.__initialiseAccountingObject( 'getFile', storageElementName, 1 )
      oDataOperation.setStartTime()
      startTime = time.time()

      res = Utils.executeSingleFileOrDirWrapper( se.getFile( physicalFile, localPath = os.path.realpath( destinationDir ) ) )

      getTime = time.time() - startTime
      oDataOperation.setValueByKey( 'TransferTime', getTime )

      if not res['OK']:
        self.log.debug( "Failed to get %s from %s" % ( lfn, storageElementName ), res['Message'] )
        oDataOperation.setValueByKey( 'TransferOK', 0 )
        oDataOperation.setValueByKey( 'FinalStatus', 'Failed' )
        oDataOperation.setEndTime()

      else:
        oDataOperation.setValueByKey( 'TransferSize', res['Value'] )


        localFile = os.path.realpath( os.path.join( destinationDir, os.path.basename( lfn ) ) )
        localAdler = fileAdler( localFile )

        if ( metadata['Size'] != res['Value'] ):
          oDataOperation.setValueByKey( 'FinalStatus', 'FinishedDirty' )
          self.log.debug( "Size of downloaded file (%d) does not match catalog (%d)" % ( res['Value'],
                                                                                        metadata['Size'] ) )

        elif ( metadata['Checksum'] ) and ( not compareAdler( metadata['Checksum'], localAdler ) ):
          oDataOperation.setValueByKey( 'FinalStatus', 'FinishedDirty' )
          self.log.debug( "Checksum of downloaded file (%s) does not match catalog (%s)" % ( localAdler,
                                                                                            metadata['Checksum'] ) )

        else:
          oDataOperation.setEndTime()
          gDataStoreClient.addRegister( oDataOperation )
          return S_OK( localFile )

    gDataStoreClient.addRegister( oDataOperation )
    self.log.debug( "getFile: Failed to get local copy from any replicas.", lfn )

    return S_ERROR( "DataManager.getFile: Failed to get local copy from any replicas." )

  def _getSEProximity( self, ses ):
    """ get SE proximity """
    siteName = DIRAC.siteName()
    localSEs = [se for se in getSEsForSite( siteName )['Value'] if se in ses]
    countrySEs = []
    countryCode = str( siteName ).split( '.' )[-1]
    res = getSEsForCountry( countryCode )
    if res['OK']:
      countrySEs = [se for se in res['Value'] if se in ses and se not in localSEs]
    sortedSEs = randomize( localSEs ) + randomize( countrySEs )
    sortedSEs += randomize( [se for se in ses if se not in sortedSEs] )
    return S_OK( sortedSEs )

  def putAndRegister( self, lfn, fileName, diracSE, guid = None, path = None, checksum = None ):
    """ Put a local file to a Storage Element and register in the File Catalogues

        'lfn' is the file LFN
        'file' is the full path to the local file
        'diracSE' is the Storage Element to which to put the file
        'guid' is the guid with which the file is to be registered (if not provided will be generated)
        'path' is the path on the storage where the file will be put (if not provided the LFN will be used)
    """
#     ancestors = ancestors if ancestors else list()
    res = self.__verifyOperationWritePermission( os.path.dirname( lfn ) )
    if not res['OK']:
      return res
    if not res['Value']:
      errStr = "putAndRegister: Write access not permitted for this credential."
      self.log.debug( errStr, lfn )
      return S_ERROR( errStr )

    # Check that the local file exists
    if not os.path.exists( fileName ):
      errStr = "putAndRegister: Supplied file does not exist."
      self.log.debug( errStr, fileName )
      return S_ERROR( errStr )
    # If the path is not provided then use the LFN path
    if not path:
      path = os.path.dirname( lfn )
    # Obtain the size of the local file
    size = getSize( fileName )
    if size == 0:
      errStr = "putAndRegister: Supplied file is zero size."
      self.log.debug( errStr, fileName )
      return S_ERROR( errStr )
    # If the GUID is not given, generate it here
    if not guid:
      guid = makeGuid( fileName )
    if not checksum:
      self.log.debug( "putAndRegister: Checksum information not provided. Calculating adler32." )
      checksum = fileAdler( fileName )
      self.log.debug( "putAndRegister: Checksum calculated to be %s." % checksum )
    res = self.fc.exists( {lfn:guid} )
    if not res['OK']:
      errStr = "putAndRegister: Completey failed to determine existence of destination LFN."
      self.log.debug( errStr, lfn )
      return res
    if lfn not in res['Value']['Successful']:
      errStr = "putAndRegister: Failed to determine existence of destination LFN."
      self.log.debug( errStr, lfn )
      return S_ERROR( errStr )
    if res['Value']['Successful'][lfn]:
      if res['Value']['Successful'][lfn] == lfn:
        errStr = "putAndRegister: The supplied LFN already exists in the File Catalog."
        self.log.debug( errStr, lfn )
      else:
        errStr = "putAndRegister: This file GUID already exists for another file. " \
            "Please remove it and try again."
        self.log.debug( errStr, res['Value']['Successful'][lfn] )
      return S_ERROR( "%s %s" % ( errStr, res['Value']['Successful'][lfn] ) )

    ##########################################################
    #  Instantiate the destination storage element here.
    storageElement = StorageElement( diracSE )
    res = storageElement.isValid()
    if not res['OK']:
      errStr = "putAndRegister: The storage element is not currently valid."
      self.log.debug( errStr, "%s %s" % ( diracSE, res['Message'] ) )
      return S_ERROR( errStr )
    destinationSE = storageElement.getStorageElementName()['Value']
    res = Utils.executeSingleFileOrDirWrapper( storageElement.getPfnForLfn( lfn ) )
    if not res['OK']:
      errStr = "putAndRegister: Failed to generate destination PFN."
      self.log.debug( errStr, res['Message'] )
      return S_ERROR( errStr )
    destPfn = res['Value']
    fileDict = {destPfn:fileName}

    successful = {}
    failed = {}
    ##########################################################
    #  Perform the put here.
    oDataOperation = self.__initialiseAccountingObject( 'putAndRegister', diracSE, 1 )
    oDataOperation.setStartTime()
    oDataOperation.setValueByKey( 'TransferSize', size )
    startTime = time.time()
    res = storageElement.putFile( fileDict, singleFile = True )
    putTime = time.time() - startTime
    oDataOperation.setValueByKey( 'TransferTime', putTime )
    if not res['OK']:
      errStr = "putAndRegister: Failed to put file to Storage Element."
      oDataOperation.setValueByKey( 'TransferOK', 0 )
      oDataOperation.setValueByKey( 'FinalStatus', 'Failed' )
      oDataOperation.setEndTime()
      gDataStoreClient.addRegister( oDataOperation )
      startTime = time.time()
      gDataStoreClient.commit()
      self.log.debug( 'putAndRegister: Sending accounting took %.1f seconds' % ( time.time() - startTime ) )
      self.log.debug( errStr, "%s: %s" % ( fileName, res['Message'] ) )
      return S_ERROR( "%s %s" % ( errStr, res['Message'] ) )
    successful[lfn] = {'put': putTime}

    ###########################################################
    # Perform the registration here
    oDataOperation.setValueByKey( 'RegistrationTotal', 1 )
    fileTuple = ( lfn, destPfn, size, destinationSE, guid, checksum )
    registerDict = {'LFN':lfn, 'PFN':destPfn, 'Size':size, 'TargetSE':destinationSE, 'GUID':guid, 'Addler':checksum}
    startTime = time.time()
    res = self.registerFile( fileTuple )
    registerTime = time.time() - startTime
    oDataOperation.setValueByKey( 'RegistrationTime', registerTime )
    if not res['OK']:
      errStr = "putAndRegister: Completely failed to register file."
      self.log.debug( errStr, res['Message'] )
      failed[lfn] = { 'register' : registerDict }
      oDataOperation.setValueByKey( 'FinalStatus', 'Failed' )
    elif lfn in res['Value']['Failed']:
      errStr = "putAndRegister: Failed to register file."
      self.log.debug( errStr, "%s %s" % ( lfn, res['Value']['Failed'][lfn] ) )
      oDataOperation.setValueByKey( 'FinalStatus', 'Failed' )
      failed[lfn] = { 'register' : registerDict }
    else:
      successful[lfn]['register'] = registerTime
      oDataOperation.setValueByKey( 'RegistrationOK', 1 )
    oDataOperation.setEndTime()
    gDataStoreClient.addRegister( oDataOperation )
    startTime = time.time()
    gDataStoreClient.commit()
    self.log.debug( 'putAndRegister: Sending accounting took %.1f seconds' % ( time.time() - startTime ) )
    return S_OK( {'Successful': successful, 'Failed': failed } )

  def replicateAndRegister( self, lfn, destSE, sourceSE = '', destPath = '', localCache = '' , catalog = '' ):
    """ Replicate a LFN to a destination SE and register the replica.

        'lfn' is the LFN to be replicated
        'destSE' is the Storage Element the file should be replicated to
        'sourceSE' is the source for the file replication (where not specified all replicas will be attempted)
        'destPath' is the path on the destination storage element, if to be different from LHCb convention
        'localCache' is the local file system location to be used as a temporary cache
    """
    successful = {}
    failed = {}
    self.log.debug( "replicateAndRegister: Attempting to replicate %s to %s." % ( lfn, destSE ) )
    startReplication = time.time()
    res = self.__replicate( lfn, destSE, sourceSE, destPath, localCache )
    replicationTime = time.time() - startReplication
    if not res['OK']:
      errStr = "DataManager.replicateAndRegister: Completely failed to replicate file."
      self.log.debug( errStr, res['Message'] )
      return S_ERROR( errStr )
    if not res['Value']:
      # The file was already present at the destination SE
      self.log.debug( "replicateAndRegister: %s already present at %s." % ( lfn, destSE ) )
      successful[lfn] = { 'replicate' : 0, 'register' : 0 }
      resDict = { 'Successful' : successful, 'Failed' : failed }
      return S_OK( resDict )
    successful[lfn] = { 'replicate' : replicationTime }

    destPfn = res['Value']['DestPfn']
    destSE = res['Value']['DestSE']
    self.log.debug( "replicateAndRegister: Attempting to register %s at %s." % ( destPfn, destSE ) )
    replicaTuple = ( lfn, destPfn, destSE )
    startRegistration = time.time()
    res = self.registerReplica( replicaTuple, catalog = catalog )
    registrationTime = time.time() - startRegistration
    if not res['OK']:
      # Need to return to the client that the file was replicated but not registered
      errStr = "replicateAndRegister: Completely failed to register replica."
      self.log.debug( errStr, res['Message'] )
      failed[lfn] = { 'Registration' : { 'LFN' : lfn, 'TargetSE' : destSE, 'PFN' : destPfn } }
    else:
      if lfn in res['Value']['Successful']:
        self.log.debug( "replicateAndRegister: Successfully registered replica." )
        successful[lfn]['register'] = registrationTime
      else:
        errStr = "replicateAndRegister: Failed to register replica."
        self.log.debug( errStr, res['Value']['Failed'][lfn] )
        failed[lfn] = { 'Registration' : { 'LFN' : lfn, 'TargetSE' : destSE, 'PFN' : destPfn } }
    return S_OK( {'Successful': successful, 'Failed': failed} )

  def replicate( self, lfn, destSE, sourceSE = '', destPath = '', localCache = '' ):
    """ Replicate a LFN to a destination SE and register the replica.

        'lfn' is the LFN to be replicated
        'destSE' is the Storage Element the file should be replicated to
        'sourceSE' is the source for the file replication (where not specified all replicas will be attempted)
        'destPath' is the path on the destination storage element, if to be different from LHCb convention
        'localCache' is the local file system location to be used as a temporary cache
    """
    self.log.debug( "replicate: Attempting to replicate %s to %s." % ( lfn, destSE ) )
    res = self.__replicate( lfn, destSE, sourceSE, destPath, localCache )
    if not res['OK']:
      errStr = "replicate: Replication failed."
      self.log.debug( errStr, "%s %s" % ( lfn, destSE ) )
      return res
    if not res['Value']:
      # The file was already present at the destination SE
      self.log.debug( "replicate: %s already present at %s." % ( lfn, destSE ) )
      return res
    return S_OK( lfn )

  def __replicate( self, lfn, destSE, sourceSE = '', destPath = '', localCache = '' ):
    """ Replicate a LFN to a destination SE.

        'lfn' is the LFN to be replicated
        'destSE' is the Storage Element the file should be replicated to
        'sourceSE' is the source for the file replication (where not specified all replicas will be attempted)
        'destPath' is the path on the destination storage element, if to be different from LHCb convention
    """
    ###########################################################
    # Check that we have write permissions to this directory.
    res = self.__verifyOperationWritePermission( lfn )
    if not res['OK']:
      return res
    if not res['Value']:
      errStr = "__replicate: Write access not permitted for this credential."
      self.log.debug( errStr, lfn )
      return S_ERROR( errStr )

    self.log.debug( "__replicate: Performing replication initialization." )
    res = self.__initializeReplication( lfn, sourceSE, destSE )
    if not res['OK']:
      self.log.debug( "__replicate: Replication initialisation failed.", lfn )
      return res
    destStorageElement = res['Value']['DestStorage']
    lfnReplicas = res['Value']['Replicas']
    destSE = res['Value']['DestSE']
    catalogueSize = res['Value']['CatalogueSize']
    ###########################################################
    # If the LFN already exists at the destination we have nothing to do
    if destSE in lfnReplicas:
      self.log.debug( "__replicate: LFN is already registered at %s." % destSE )
      return S_OK()
    ###########################################################
    # Resolve the best source storage elements for replication
    self.log.debug( "__replicate: Determining the best source replicas." )
    res = self.__resolveBestReplicas( lfn, sourceSE, lfnReplicas, catalogueSize )
    if not res['OK']:
      self.log.debug( "__replicate: Best replica resolution failed.", lfn )
      return res
    replicaPreference = res['Value']
    ###########################################################
    # Now perform the replication for the file
    if destPath:
      destPath = '%s/%s' % ( destPath, os.path.basename( lfn ) )
    else:
      destPath = lfn
    res = Utils.executeSingleFileOrDirWrapper( destStorageElement.getPfnForLfn( destPath ) )
    if not res['OK']:
      errStr = "__replicate: Failed to generate destination PFN."
      self.log.debug( errStr, res['Message'] )
      return S_ERROR( errStr )
    destPfn = res['Value']
    # Find out if there is a replica already at the same site
    localReplicas = []
    otherReplicas = []
    for sourceSE, sourcePfn in replicaPreference:
      if sourcePfn == destPfn:
        continue
      res = isSameSiteSE( sourceSE, destSE )
      if res['OK'] and res['Value']:
        localReplicas.append( ( sourceSE, sourcePfn ) )
      else:
        otherReplicas.append( ( sourceSE, sourcePfn ) )
    replicaPreference = localReplicas + otherReplicas
    for sourceSE, sourcePfn in replicaPreference:
      self.log.debug( "__replicate: Attempting replication from %s to %s." % ( sourceSE, destSE ) )
      fileDict = {destPfn:sourcePfn}
      if sourcePfn == destPfn:
        continue

      localFile = ''
      #FIXME: this should not be hardcoded!!!
      if sourcePfn.find( 'srm' ) == -1 or destPfn.find( 'srm' ) == -1:
        # No third party transfer is possible, we have to replicate through the local cache
        localDir = '.'
        if localCache:
          localDir = localCache
        self.getFile( lfn, destinationDir = localDir )
        localFile = os.path.join( localDir, os.path.basename( lfn ) )
        fileDict = {destPfn:localFile}

      res = destStorageElement.replicateFile( fileDict, sourceSize = catalogueSize, singleFile = True )
      if localFile and os.path.exists( localFile ):
        os.remove( localFile )

      if res['OK']:
        self.log.debug( "__replicate: Replication successful." )
        resDict = {'DestSE':destSE, 'DestPfn':destPfn}
        return S_OK( resDict )
      else:
        errStr = "__replicate: Replication failed."
        self.log.debug( errStr, "%s from %s to %s." % ( lfn, sourceSE, destSE ) )
    ##########################################################
    # If the replication failed for all sources give up
    errStr = "__replicate: Failed to replicate with all sources."
    self.log.debug( errStr, lfn )
    return S_ERROR( errStr )

  def __initializeReplication( self, lfn, sourceSE, destSE ):

    # Horrible, but kept to not break current log messages
    logStr = "__initializeReplication:"

    ###########################################################
    # Check the sourceSE if specified
    self.log.verbose( "%s: Determining whether source Storage Element is sane." % logStr )

    if sourceSE:
      if not self.__SEActive( sourceSE ).get( 'Value', {} ).get( 'Read' ):
        infoStr = "%s Supplied source Storage Element is not currently allowed for Read." % ( logStr )
        self.log.info( infoStr, sourceSE )
        return S_ERROR( infoStr )

    ###########################################################
    # Check that the destination storage element is sane and resolve its name
    self.log.debug( "%s Verifying dest StorageElement validity (%s)." % ( logStr, destSE ) )
    destStorageElement = StorageElement( destSE )
    res = destStorageElement.isValid()
    if not res['OK']:
      errStr = "%s The storage element is not currently valid." % logStr
      self.log.debug( errStr, "%s %s" % ( destSE, res['Message'] ) )
      return S_ERROR( errStr )
    destSE = destStorageElement.getStorageElementName()['Value']
    self.log.info( "%s Destination Storage Element verified." % logStr )

    ###########################################################
    # Check whether the destination storage element is banned
    self.log.verbose( "%s Determining whether %s ( destination ) is Write-banned." % ( logStr, destSE ) )

    if not self.__SEActive( destSE ).get( 'Value', {} ).get( 'Write' ):
      infoStr = "%s Supplied destination Storage Element is not currently allowed for Write." % ( logStr )
      self.log.debug( infoStr, destSE )
      return S_ERROR( infoStr )

    ###########################################################
    # Get the LFN replicas from the file catalogue
    self.log.debug( "%s Attempting to obtain replicas for %s." % ( logStr, lfn ) )
    res = self.fc.getReplicas( lfn )
    if not res[ 'OK' ]:
      errStr = "%s Completely failed to get replicas for LFN." % logStr
      self.log.debug( errStr, "%s %s" % ( lfn, res['Message'] ) )
      return res
    if lfn not in res['Value']['Successful']:
      errStr = "%s Failed to get replicas for LFN." % logStr
      self.log.debug( errStr, "%s %s" % ( lfn, res['Value']['Failed'][lfn] ) )
      return S_ERROR( "%s %s" % ( errStr, res['Value']['Failed'][lfn] ) )
    self.log.debug( "%s Successfully obtained replicas for LFN." % logStr )
    lfnReplicas = res['Value']['Successful'][lfn]

    ###########################################################
    # Check the file is at the sourceSE
    self.log.debug( "%s: Determining whether source Storage Element is sane." % logStr )

    if sourceSE and sourceSE not in lfnReplicas:
      errStr = "%s LFN does not exist at supplied source SE." % logStr
      self.log.error( errStr, "%s %s" % ( lfn, sourceSE ) )
      return S_ERROR( errStr )

    ###########################################################
    # If the file catalogue size is zero fail the transfer
    self.log.debug( "%s Attempting to obtain size for %s." % ( logStr, lfn ) )
    res = self.fc.getFileSize( lfn )
    if not res['OK']:
      errStr = "%s Completely failed to get size for LFN." % logStr
      self.log.debug( errStr, "%s %s" % ( lfn, res['Message'] ) )
      return res
    if lfn not in res['Value']['Successful']:
      errStr = "%s Failed to get size for LFN." % logStr
      self.log.debug( errStr, "%s %s" % ( lfn, res['Value']['Failed'][lfn] ) )
      return S_ERROR( "%s %s" % ( errStr, res['Value']['Failed'][lfn] ) )
    catalogueSize = res['Value']['Successful'][lfn]
    if catalogueSize == 0:
      errStr = "%s Registered file size is 0." % logStr
      self.log.debug( errStr, lfn )
      return S_ERROR( errStr )
    self.log.debug( "%s File size determined to be %s." % ( logStr, catalogueSize ) )

    self.log.info( "%s Replication initialization successful." % logStr )

    resDict = {
               'DestStorage'   : destStorageElement,
               'DestSE'        : destSE,
               'Replicas'      : lfnReplicas,
               'CatalogueSize' : catalogueSize
               }

    return S_OK( resDict )

  def __resolveBestReplicas( self, lfn, sourceSE, lfnReplicas, catalogueSize ):
    """ find best replicas """

    ###########################################################
    # Determine the best replicas (remove banned sources, invalid storage elements and file with the wrong size)

    logStr = "__resolveBestReplicas:"

    replicaPreference = []

    for diracSE, pfn in lfnReplicas.items():

      if sourceSE and diracSE != sourceSE:
        self.log.debug( "%s %s replica not requested." % ( logStr, diracSE ) )
        continue

      if not self.__SEActive( diracSE ).get( 'Value', {} ).get( 'Read' ):
        self.log.debug( "%s %s is currently not allowed as a source." % ( logStr, diracSE ) )
      else:
        self.log.debug( "%s %s is available for use." % ( logStr, diracSE ) )
        storageElement = StorageElement( diracSE )
        res = storageElement.isValid()
        if not res['OK']:
          errStr = "%s The storage element is not currently valid." % logStr
          self.log.debug( errStr, "%s %s" % ( diracSE, res['Message'] ) )
        else:
          # pfn = Utils.executeSingleFileOrDirWrapper( storageElement.getPfnForLfn( lfn ) ).get( 'Value', pfn )
          if storageElement.getRemoteProtocols()['Value']:
            self.log.debug( "%s Attempting to get source pfns for remote protocols." % logStr )
            res = Utils.executeSingleFileOrDirWrapper( storageElement.getPfnForProtocol( pfn, protocol = self.thirdPartyProtocols ) )
            if res['OK']:
              sourcePfn = res['Value']
              self.log.debug( "%s Attempting to get source file size." % logStr )
              res = storageElement.getFileSize( sourcePfn )
              if res['OK']:
                if sourcePfn in res['Value']['Successful']:
                  sourceFileSize = res['Value']['Successful'][sourcePfn]
                  self.log.debug( "%s Source file size determined to be %s." % ( logStr, sourceFileSize ) )
                  if catalogueSize == sourceFileSize:
                    fileTuple = ( diracSE, sourcePfn )
                    replicaPreference.append( fileTuple )
                  else:
                    errStr = "%s Catalogue size and physical file size mismatch." % logStr
                    self.log.debug( errStr, "%s %s" % ( diracSE, sourcePfn ) )
                else:
                  errStr = "%s Failed to get physical file size." % logStr
                  self.log.debug( errStr, "%s %s: %s" % ( sourcePfn, diracSE, res['Value']['Failed'][sourcePfn] ) )
              else:
                errStr = "%s Completely failed to get physical file size." % logStr
                self.log.debug( errStr, "%s %s: %s" % ( sourcePfn, diracSE, res['Message'] ) )
            else:
              errStr = "%s Failed to get PFN for replication for StorageElement." % logStr
              self.log.debug( errStr, "%s %s" % ( diracSE, res['Message'] ) )
          else:
            errStr = "%s Source Storage Element has no remote protocols." % logStr
            self.log.debug( errStr, diracSE )

    if not replicaPreference:
      errStr = "%s Failed to find any valid source Storage Elements." % logStr
      self.log.debug( errStr )
      return S_ERROR( errStr )

    else:
      return S_OK( replicaPreference )

  ###################################################################
  #
  # These are the file catalog write methods
  #

  def registerFile( self, fileTuple, catalog = '' ):
    """ Register a file or a list of files

    :param self: self reference
    :param tuple fileTuple: (lfn, physicalFile, fileSize, storageElementName, fileGuid, checksum )
    :param str catalog: catalog name
    """
    if type( fileTuple ) == ListType:
      fileTuples = fileTuple
    elif type( fileTuple ) == TupleType:
      fileTuples = [fileTuple]
    else:
      errStr = "registerFile: Supplied file info must be tuple of list of tuples."
      self.log.debug( errStr )
      return S_ERROR( errStr )
    self.log.debug( "registerFile: Attempting to register %s files." % len( fileTuples ) )
    res = self.__registerFile( fileTuples, catalog )
    if not res['OK']:
      errStr = "registerFile: Completely failed to register files."
      self.log.debug( errStr, res['Message'] )
      return S_ERROR( errStr )
    return res

  def __registerFile( self, fileTuples, catalog ):
    """ register file to cataloge """

    fileDict = {}

    for lfn, physicalFile, fileSize, storageElementName, fileGuid, checksum in fileTuples:
      fileDict[lfn] = {'PFN':physicalFile, 'Size':fileSize, 'SE':storageElementName, 'GUID':fileGuid, 'Checksum':checksum}

    if catalog:
      fileCatalog = FileCatalog( catalog )
      if not fileCatalog.isOK():
        return S_ERROR( "Can't get FileCatalog %s" % catalog )
    else:
      fileCatalog = self.fc

    res = fileCatalog.addFile( fileDict )
    if not res['OK']:
      errStr = "__registerFile: Completely failed to register files."
      self.log.debug( errStr, res['Message'] )

    return res

  def registerReplica( self, replicaTuple, catalog = '' ):
    """ Register a replica (or list of) supplied in the replicaTuples.

        'replicaTuple' is a tuple or list of tuples of the form (lfn,pfn,se)
    """
    if type( replicaTuple ) == ListType:
      replicaTuples = replicaTuple
    elif type( replicaTuple ) == TupleType:
      replicaTuples = [ replicaTuple ]
    else:
      errStr = "registerReplica: Supplied file info must be tuple of list of tuples."
      self.log.debug( errStr )
      return S_ERROR( errStr )
    self.log.debug( "registerReplica: Attempting to register %s replicas." % len( replicaTuples ) )
    res = self.__registerReplica( replicaTuples, catalog )
    if not res['OK']:
      errStr = "registerReplica: Completely failed to register replicas."
      self.log.debug( errStr, res['Message'] )
    return res

  def __registerReplica( self, replicaTuples, catalog ):
    """ register replica to catalogue """
    seDict = {}
    for lfn, pfn, storageElementName in replicaTuples:
      seDict.setdefault( storageElementName, [] ).append( ( lfn, pfn ) )
    failed = {}
    replicaTuples = []
    for storageElementName, replicaTuple in seDict.items():
      destStorageElement = StorageElement( storageElementName )
      res = destStorageElement.isValid()
      if not res['OK']:
        errStr = "__registerReplica: The storage element is not currently valid."
        self.log.debug( errStr, "%s %s" % ( storageElementName, res['Message'] ) )
        for lfn, pfn in replicaTuple:
          failed[lfn] = errStr
      else:
        storageElementName = destStorageElement.getStorageElementName()['Value']
        for lfn, pfn in replicaTuple:
          res = Utils.executeSingleFileOrDirWrapper( destStorageElement.getPfnForProtocol( pfn, protocol = self.registrationProtocol, withPort = False ) )
          if not res['OK']:
            failed[lfn] = res['Message']
          else:
            replicaTuple = ( lfn, res['Value'], storageElementName, False )
            replicaTuples.append( replicaTuple )
    self.log.debug( "__registerReplica: Successfully resolved %s replicas for registration." % len( replicaTuples ) )
    # HACK!
    replicaDict = {}
    for lfn, pfn, se, _master in replicaTuples:
      replicaDict[lfn] = {'SE':se, 'PFN':pfn}

    if catalog:
      fileCatalog = FileCatalog( catalog )
      res = fileCatalog.addReplica( replicaDict )
    else:
      res = self.fc.addReplica( replicaDict )
    if not res['OK']:
      errStr = "__registerReplica: Completely failed to register replicas."
      self.log.debug( errStr, res['Message'] )
      return S_ERROR( errStr )
    failed.update( res['Value']['Failed'] )
    successful = res['Value']['Successful']
    resDict = {'Successful':successful, 'Failed':failed}
    return S_OK( resDict )

  ###################################################################
  #
  # These are the removal methods for physical and catalogue removal
  #

  def removeFile( self, lfn, force = None ):
    """ Remove the file (all replicas) from Storage Elements and file catalogue

        'lfn' is the file to be removed
    """
    if force == None:
      force = self.ignoreMissingInFC
    if type( lfn ) == ListType:
      lfns = lfn
    elif type( lfn ) == StringType:
      lfns = [lfn]
    else:
      errStr = "removeFile: Supplied lfns must be string or list of strings."
      self.log.debug( errStr )
      return S_ERROR( errStr )
    # First check if the file exists in the FC
    res = self.fc.exists( lfns )
    if not res['OK']:
      return res
    success = res['Value']['Successful']
    lfns = [lfn for lfn in success if success[lfn] ]
    if force:
      # Files that don't exist are removed successfully
      successful = dict.fromkeys( [lfn for lfn in success if not success[lfn] ], True )
      failed = {}
    else:
      successful = {}
      failed = dict.fromkeys( [lfn for lfn in success if not success[lfn] ], 'No such file or directory' )
    # Check that we have write permissions to this directory.
    if lfns:
      res = self.__verifyOperationWritePermission( lfns )
      if not res['OK']:
        return res
      if not res['Value']:
        errStr = "removeFile: Write access not permitted for this credential."
        self.log.error( errStr, lfns )
        return S_ERROR( errStr )


      self.log.debug( "removeFile: Attempting to remove %s files from Storage and Catalogue. Get replicas first" % len( lfns ) )
      res = self.fc.getReplicas( lfns, True )
      if not res['OK']:
        errStr = "DataManager.removeFile: Completely failed to get replicas for lfns."
        self.log.debug( errStr, res['Message'] )
        return res
      lfnDict = res['Value']['Successful']

      for lfn, reason in res['Value'].get( 'Failed', {} ).items():
        # Ignore files missing in FC if force is set
        if reason == 'No such file or directory' and force:
          successful[lfn] = True
        elif reason == 'File has zero replicas':
          lfnDict[lfn] = {}
        else:
          failed[lfn] = reason

      res = self.__removeFile( lfnDict )
      if not res['OK']:
        errStr = "removeFile: Completely failed to remove files."
        self.log.debug( errStr, res['Message'] )
        return res
      failed.update( res['Value']['Failed'] )
      successful.update( res['Value']['Successful'] )

    resDict = {'Successful':successful, 'Failed':failed}
    gDataStoreClient.commit()
    return S_OK( resDict )

  def __removeFile( self, lfnDict ):
    """ remove file """
    storageElementDict = {}
    # # sorted and reversed
    for lfn, repDict in sorted( lfnDict.items(), reverse = True ):
      for se, pfn in repDict.items():
        storageElementDict.setdefault( se, [] ).append( ( lfn, pfn ) )
    failed = {}
    successful = {}
    for storageElementName in sorted( storageElementDict ):
      fileTuple = storageElementDict[storageElementName]
      res = self.__removeReplica( storageElementName, fileTuple )
      if not res['OK']:
        errStr = res['Message']
        for lfn, pfn in fileTuple:
          failed[lfn] = failed.setdefault( lfn, '' ) + " %s" % errStr
      else:
        for lfn, errStr in res['Value']['Failed'].items():
          failed[lfn] = failed.setdefault( lfn, '' ) + " %s" % errStr
    completelyRemovedFiles = []
    for lfn in [lfn for lfn in lfnDict if lfn not in failed]:
      completelyRemovedFiles.append( lfn )
    if completelyRemovedFiles:
      res = self.fc.removeFile( completelyRemovedFiles )
      if not res['OK']:
        for lfn in completelyRemovedFiles:
          failed[lfn] = "Failed to remove file from the catalog: %s" % res['Message']
      else:
        failed.update( res['Value']['Failed'] )
        successful = res['Value']['Successful']
    return S_OK( { 'Successful' : successful, 'Failed' : failed } )

  def removeReplica( self, storageElementName, lfn ):
    """ Remove replica at the supplied Storage Element from Storage Element then file catalogue

       'storageElementName' is the storage where the file is to be removed
       'lfn' is the file to be removed
    """
    if type( lfn ) == ListType:
      lfns = lfn
    elif type( lfn ) == StringType:
      lfns = [lfn]
    else:
      errStr = "removeReplica: Supplied lfns must be string or list of strings."
      self.log.debug( errStr )
      return S_ERROR( errStr )
    # Check that we have write permissions to this directory.
    res = self.__verifyOperationWritePermission( lfns )
    if not res['OK']:
      return res
    if not res['Value']:
      errStr = "removeReplica: Write access not permitted for this credential."
      self.log.debug( errStr, lfns )
      return S_ERROR( errStr )
    self.log.debug( "removeReplica: Will remove catalogue entry for %s lfns at %s." % ( len( lfns ),
                                                                                          storageElementName ) )
    res = self.fc.getReplicas( lfns, True )
    if not res['OK']:
      errStr = "removeReplica: Completely failed to get replicas for lfns."
      self.log.debug( errStr, res['Message'] )
      return res
    failed = res['Value']['Failed']
    successful = {}
    replicaTuples = []
    for lfn, repDict in res['Value']['Successful'].items():
      if storageElementName not in repDict:
        # The file doesn't exist at the storage element so don't have to remove it
        successful[lfn] = True
      elif len( repDict ) == 1:
        # The file has only a single replica so don't remove
        self.log.debug( "The replica you are trying to remove is the only one.", "%s @ %s" % ( lfn,
                                                                                               storageElementName ) )
        failed[lfn] = "Failed to remove sole replica"
      else:
        replicaTuples.append( ( lfn, repDict[storageElementName] ) )
    res = self.__removeReplica( storageElementName, replicaTuples )
    if not res['OK']:
      return res
    failed.update( res['Value']['Failed'] )
    successful.update( res['Value']['Successful'] )
    gDataStoreClient.commit()
    return S_OK( { 'Successful' : successful, 'Failed' : failed } )

  def __removeReplica( self, storageElementName, fileTuple ):
    """ remove replica """
    lfnDict = {}
    failed = {}
    se = None if self.useCatalogPFN else StorageElement( storageElementName )  # Placeholder for the StorageElement object
    for lfn, pfn in fileTuple:
      res = self.__verifyOperationWritePermission( lfn )
      if not res['OK'] or not res['Value']:
        errStr = "__removeReplica: Write access not permitted for this credential."
        self.log.debug( errStr, lfn )
        failed[lfn] = errStr
      else:
        # This is the PFN as in the FC
        lfnDict[lfn] = pfn

    # Now we should use the constructed PFNs if needed, for the physical removal
    # Reverse lfnDict into pfnDict with required PFN
    if self.useCatalogPFN:
      pfnDict = dict( zip( lfnDict.values(), lfnDict.keys() ) )
    else:
      pfnDict = dict( [ ( se.getPfnForLfn( lfn )['Value'].get( 'Successful', {} ).get( lfn, lfnDict[lfn] ), lfn ) for lfn in lfnDict] )
    # removePhysicalReplicas is called with real PFN list
    res = self.__removePhysicalReplica( storageElementName, pfnDict.keys() )

    if not res['OK']:
      errStr = "__removeReplica: Failed to remove catalog replicas."
      self.log.debug( errStr, res['Message'] )
      return S_ERROR( errStr )

    failed.update( dict( [( pfnDict[pfn], error ) for pfn, error in res['Value']['Failed'].items()] ) )
    # Here we use the FC PFN...
    replicaTuples = [( pfnDict[pfn], lfnDict[pfnDict[pfn]], storageElementName ) for pfn in res['Value']['Successful']]

    res = self.__removeCatalogReplica( replicaTuples )
    if not res['OK']:
      errStr = "__removeReplica: Completely failed to remove physical files."
      self.log.debug( errStr, res['Message'] )
      failed.update( dict.fromkeys( [lfn for lfn, _pfn, _se in replicaTuples if lfn not in failed], res['Message'] ) )
      successful = {}
    else:
      failed.update( res['Value']['Failed'] )
      successful = res['Value']['Successful']
    return S_OK( { 'Successful' : successful, 'Failed' : failed } )

  def removeReplicaFromCatalog( self, storageElementName, lfn ):
    """ remove :lfn: replica from :storageElementName: SE

    :param self: self reference
    :param str storageElementName: SE name
    :param mixed lfn: a single LFN or list of LFNs
    """

    # Remove replica from the file catalog 'lfn' are the file
    # to be removed 'storageElementName' is the storage where the file is to be removed
    if type( lfn ) == ListType:
      lfns = lfn
    elif type( lfn ) == StringType:
      lfns = [lfn]
    else:
      errStr = "removeReplicaFromCatalog: Supplied lfns must be string or list of strings."
      self.log.debug( errStr )
      return S_ERROR( errStr )
    self.log.debug( "removeReplicaFromCatalog: Will remove catalogue entry for %s lfns at %s." % \
                        ( len( lfns ), storageElementName ) )
    res = self.fc.getReplicas( lfns, allStatus = True )
    if not res['OK']:
      errStr = "removeReplicaFromCatalog: Completely failed to get replicas for lfns."
      self.log.debug( errStr, res['Message'] )
      return res
    failed = {}
    successful = {}
    for lfn, reason in res['Value']['Failed'].items():
      if reason in ( 'No such file or directory', 'File has zero replicas' ):
        successful[lfn] = True
      else:
        failed[lfn] = reason
    replicaTuples = []
    for lfn, repDict in res['Value']['Successful'].items():
      if storageElementName not in repDict:
        # The file doesn't exist at the storage element so don't have to remove it
        successful[lfn] = True
      else:
        replicaTuples.append( ( lfn, repDict[storageElementName], storageElementName ) )
    self.log.debug( "removeReplicaFromCatalog: Resolved %s pfns for catalog removal at %s." % ( len( replicaTuples ),
                                                                                                  storageElementName ) )
    res = self.__removeCatalogReplica( replicaTuples )
    failed.update( res['Value']['Failed'] )
    successful.update( res['Value']['Successful'] )
    resDict = {'Successful':successful, 'Failed':failed}
    return S_OK( resDict )

  def removeCatalogPhysicalFileNames( self, replicaTuple ):
    """ Remove replicas from the file catalog specified by replica tuple

       'replicaTuple' is a tuple containing the replica to be removed and is of the form ( lfn, pfn, se )
    """
    if type( replicaTuple ) == ListType:
      replicaTuples = replicaTuple
    elif type( replicaTuple ) == TupleType:
      replicaTuples = [replicaTuple]
    else:
      errStr = "removeCatalogPhysicalFileNames: Supplied info must be tuple or list of tuples."
      self.log.debug( errStr )
      return S_ERROR( errStr )
    return self.__removeCatalogReplica( replicaTuples )

  def __removeCatalogReplica( self, replicaTuple ):
    """ remove replica form catalogue """
    oDataOperation = self.__initialiseAccountingObject( 'removeCatalogReplica', '', len( replicaTuple ) )
    oDataOperation.setStartTime()
    start = time.time()
    # HACK!
    replicaDict = {}
    for lfn, pfn, se in replicaTuple:
      replicaDict[lfn] = {'SE':se, 'PFN':pfn}
    res = self.fc.removeReplica( replicaDict )
    oDataOperation.setEndTime()
    oDataOperation.setValueByKey( 'RegistrationTime', time.time() - start )
    if not res['OK']:
      oDataOperation.setValueByKey( 'RegistrationOK', 0 )
      oDataOperation.setValueByKey( 'FinalStatus', 'Failed' )
      gDataStoreClient.addRegister( oDataOperation )
      errStr = "__removeCatalogReplica: Completely failed to remove replica."
      self.log.debug( errStr, res['Message'] )
      return S_ERROR( errStr )


    for lfn in res['Value']['Successful']:
      infoStr = "__removeCatalogReplica: Successfully removed replica."
      self.log.debug( infoStr, lfn )
    if res['Value']['Successful']:
      self.log.debug( "__removeCatalogReplica: Removed %d replicas" % len( res['Value']['Successful'] ) )

    success = res['Value']['Successful']
    if success:
      self.log.info( "__removeCatalogReplica: Removed %d replicas" % len( success ) )
      for lfn in success:
        self.log.debug( "__removeCatalogReplica: Successfully removed replica.", lfn )

    for lfn, error in res['Value']['Failed'].items():
      self.log.error( "__removeCatalogReplica: Failed to remove replica.", "%s %s" % ( lfn, error ) )

    oDataOperation.setValueByKey( 'RegistrationOK', len( success ) )
    gDataStoreClient.addRegister( oDataOperation )
    return res

  def removePhysicalReplica( self, storageElementName, lfn ):
    """ Remove replica from Storage Element.

       'lfn' are the files to be removed
       'storageElementName' is the storage where the file is to be removed
    """
    if type( lfn ) == ListType:
      lfns = lfn
    elif type( lfn ) == StringType:
      lfns = [lfn]
    else:
      errStr = "removePhysicalReplica: Supplied lfns must be string or list of strings."
      self.log.debug( errStr )
      return S_ERROR( errStr )
    # Check that we have write permissions to this directory.
    res = self.__verifyOperationWritePermission( lfns )
    if not res['OK']:
      return res
    if not res['Value']:
      errStr = "removePhysicalReplica: Write access not permitted for this credential."
      self.log.debug( errStr, lfns )
      return S_ERROR( errStr )
    self.log.debug( "removePhysicalReplica: Attempting to remove %s lfns at %s." % ( len( lfns ),
                                                                                       storageElementName ) )
    self.log.debug( "removePhysicalReplica: Attempting to resolve replicas." )
    res = self.getReplicas( lfns )
    if not res['OK']:
      errStr = "removePhysicalReplica: Completely failed to get replicas for lfns."
      self.log.debug( errStr, res['Message'] )
      return res
    failed = res['Value']['Failed']
    successful = {}
    pfnDict = {}
    for lfn, repDict in res['Value']['Successful'].items():
      if storageElementName not in repDict:
        # The file doesn't exist at the storage element so don't have to remove it
        successful[lfn] = True
      else:
        sePfn = repDict[storageElementName]
        pfnDict[sePfn] = lfn
    self.log.debug( "removePhysicalReplica: Resolved %s pfns for removal at %s." % ( len( pfnDict ),
                                                                                       storageElementName ) )
    res = self.__removePhysicalReplica( storageElementName, pfnDict.keys() )
    for pfn, error in res['Value']['Failed'].items():
      failed[pfnDict[pfn]] = error
    for pfn in res['Value']['Successful']:
      successful[pfnDict[pfn]] = True
    resDict = { 'Successful' : successful, 'Failed' : failed }
    return S_OK( resDict )

  def __removePhysicalReplica( self, storageElementName, pfnsToRemove ):
    """ remove replica from storage element """
    self.log.debug( "__removePhysicalReplica: Attempting to remove %s pfns at %s." % ( len( pfnsToRemove ),
                                                                                         storageElementName ) )
    storageElement = StorageElement( storageElementName )
    res = storageElement.isValid()
    if not res['OK']:
      errStr = "__removePhysicalReplica: The storage element is not currently valid."
      self.log.debug( errStr, "%s %s" % ( storageElementName, res['Message'] ) )
      return S_ERROR( errStr )
    oDataOperation = self.__initialiseAccountingObject( 'removePhysicalReplica',
                                                        storageElementName,
                                                        len( pfnsToRemove ) )
    oDataOperation.setStartTime()
    start = time.time()
    res = storageElement.removeFile( pfnsToRemove )
    oDataOperation.setEndTime()
    oDataOperation.setValueByKey( 'TransferTime', time.time() - start )
    if not res['OK']:
      oDataOperation.setValueByKey( 'TransferOK', 0 )
      oDataOperation.setValueByKey( 'FinalStatus', 'Failed' )
      gDataStoreClient.addRegister( oDataOperation )
      errStr = "__removePhysicalReplica: Failed to remove replicas."
      self.log.debug( errStr, res['Message'] )
      return S_ERROR( errStr )
    else:
      for surl, value in res['Value']['Failed'].items():
        if 'No such file or directory' in value:
          res['Value']['Successful'][surl] = surl
          res['Value']['Failed'].pop( surl )
      for surl in res['Value']['Successful']:
        ret = Utils.executeSingleFileOrDirWrapper( storageElement.getPfnForProtocol( surl, protocol = self.registrationProtocol, withPort = False ) )
        if not ret['OK']:
          res['Value']['Successful'][surl] = surl
        else:
          res['Value']['Successful'][surl] = ret['Value']

      ret = storageElement.getFileSize( res['Value']['Successful'] )
      deletedSize = sum( ret.get( 'Value', {} ).get( 'Successful', {} ).values() )
      oDataOperation.setValueByKey( 'TransferOK', deletedSize )

      gDataStoreClient.addRegister( oDataOperation )
      infoStr = "__removePhysicalReplica: Successfully issued accounting removal request."
      self.log.debug( infoStr )
      return res

  #########################################################################
  #
  # File transfer methods
  #

  def put( self, lfn, fileName, diracSE, path = None ):
    """ Put a local file to a Storage Element

    :param self: self reference
    :param str lfn: LFN
    :param str fileName: the full path to the local file
    :param str diracSE: the Storage Element to which to put the file
    :param str path: the path on the storage where the file will be put (if not provided the LFN will be used)

    """
    # Check that the local file exists
    if not os.path.exists( fileName ):
      errStr = "put: Supplied file does not exist."
      self.log.debug( errStr, fileName )
      return S_ERROR( errStr )
    # If the path is not provided then use the LFN path
    if not path:
      path = os.path.dirname( lfn )
    # Obtain the size of the local file
    size = getSize( fileName )
    if size == 0:
      errStr = "put: Supplied file is zero size."
      self.log.debug( errStr, fileName )
      return S_ERROR( errStr )

    ##########################################################
    #  Instantiate the destination storage element here.
    storageElement = StorageElement( diracSE )
    res = storageElement.isValid()
    if not res['OK']:
      errStr = "put: The storage element is not currently valid."
      self.log.debug( errStr, "%s %s" % ( diracSE, res['Message'] ) )
      return S_ERROR( errStr )
    res = Utils.executeSingleFileOrDirWrapper( storageElement.getPfnForLfn( lfn ) )
    if not res['OK']:
      errStr = "put: Failed to generate destination PFN."
      self.log.debug( errStr, res['Message'] )
      return S_ERROR( errStr )
    destPfn = res['Value']
    fileDict = {destPfn:fileName}

    successful = {}
    failed = {}
    ##########################################################
    #  Perform the put here.
    startTime = time.time()
    res = storageElement.putFile( fileDict, singleFile = True )
    putTime = time.time() - startTime
    if not res['OK']:
      errStr = "put: Failed to put file to Storage Element."
      failed[lfn] = res['Message']
      self.log.debug( errStr, "%s: %s" % ( fileName, res['Message'] ) )
    else:
      self.log.debug( "put: Put file to storage in %s seconds." % putTime )
      successful[lfn] = destPfn
    resDict = {'Successful': successful, 'Failed':failed}
    return S_OK( resDict )

  # def removeReplica(self,lfn,storageElementName,singleFile=False):
  # def putReplica(self,lfn,storageElementName,singleFile=False):
  # def replicateReplica(self,lfn,size,storageElementName,singleFile=False):

  def getActiveReplicas( self, lfns ):
    """ Get all the replicas for the SEs which are in Active status for reading.
    """
    res = self.getReplicas( lfns, allStatus = False )
    if not res['OK']:
      return res
    replicas = res['Value']
    return self.checkActiveReplicas( replicas )

  def checkActiveReplicas( self, replicaDict ):
    """ Check a replica dictionary for active replicas
    """

    if type( replicaDict ) != DictType:
      return S_ERROR( 'Wrong argument type %s, expected a dictionary' % type( replicaDict ) )

    for key in [ 'Successful', 'Failed' ]:
      if not key in replicaDict:
        return S_ERROR( 'Missing key "%s" in replica dictionary' % key )
      if type( replicaDict[key] ) != DictType:
        return S_ERROR( 'Wrong argument type %s, expected a dictionary' % type( replicaDict[key] ) )

    seReadStatus = {}
    for lfn, replicas in replicaDict['Successful'].items():
      if type( replicas ) != DictType:
        del replicaDict['Successful'][ lfn ]
        replicaDict['Failed'][lfn] = 'Wrong replica info'
        continue
      for se in replicas.keys():
        # Fix the caching
        readStatus = seReadStatus[se] if se in seReadStatus else seReadStatus.setdefault( se, self.__SEActive( se ).get( 'Value', {} ).get( 'Read', False ) )
        if not readStatus:
          replicas.pop( se )

    return S_OK( replicaDict )

  def __SEActive( self, se ):
    """ check is SE is active """
    result = StorageFactory().getStorageName( se )
    if not result['OK']:
      return S_ERROR( 'SE not known' )
    resolvedName = result['Value']
    res = self.resourceStatus.getStorageElementStatus( resolvedName, default = None )
    if not res[ 'OK' ]:
      return S_ERROR( 'SE not known' )

    seStatus = { 'Read' : True, 'Write' : True }
    if res['Value'][se].get( 'ReadAccess', 'Active' ) not in ( 'Active', 'Degraded' ):
      seStatus[ 'Read' ] = False
    if res['Value'][se].get( 'WriteAccess', 'Active' ) not in ( 'Active', 'Degraded' ):
      seStatus[ 'Write' ] = False

    return S_OK( seStatus )

  def __initialiseAccountingObject( self, operation, se, files ):
    """ create accouting record """
    accountingDict = {}
    accountingDict['OperationType'] = operation
    result = getProxyInfo()
    if not result['OK']:
      userName = 'system'
    else:
      userName = result['Value'].get( 'username', 'unknown' )
    accountingDict['User'] = userName
    accountingDict['Protocol'] = 'DataManager'
    accountingDict['RegistrationTime'] = 0.0
    accountingDict['RegistrationOK'] = 0
    accountingDict['RegistrationTotal'] = 0
    accountingDict['Destination'] = se
    accountingDict['TransferTotal'] = files
    accountingDict['TransferOK'] = files
    accountingDict['TransferSize'] = files
    accountingDict['TransferTime'] = 0.0
    accountingDict['FinalStatus'] = 'Successful'
    accountingDict['Source'] = DIRAC.siteName()
    oDataOperation = DataOperation()
    oDataOperation.setValuesFromDict( accountingDict )
    return oDataOperation

##########################################
  #
  # Defunct methods only there before checking backward compatability
  #


  def getReplicas( self, lfns, allStatus = True ):
    """ get replicas from catalogue """
    res = self.fc.getReplicas( lfns, allStatus = allStatus )
    
    if not self.useCatalogPFN:
      if res['OK']:
        se_lfn = {}
        catalogReplicas = res['Value']['Successful']

        # We group the query to getPfnForLfn by storage element to gain in speed
        for lfn in catalogReplicas:
          for se in catalogReplicas[lfn]:
            se_lfn.setdefault( se, [] ).append( lfn )

        for se in se_lfn:
          seObj = StorageElement( se )
          succPfn = seObj.getPfnForLfn( se_lfn[se] ).get( 'Value', {} ).get( 'Successful', {} )
          for lfn in succPfn:
            # catalogReplicas still points res["value"]["Successful"] so res will be updated
            catalogReplicas[lfn][se] = succPfn[lfn]

    return res


  ##################################################################################################3
  # Methods from the catalogToStorage. It would all work with the direct call to the SE, but this checks
  # first if the replica is known to the catalog


  def __executeIfReplicaExists( self, storageElementName, lfn, method, **argsDict ):
    """ a simple wrapper that allows replica querying then perform the StorageElement operation

    :param self: self reference
    :param str storageElementName: DIRAC SE name
    :param mixed lfn: a LFN str, list of LFNs or dict with LFNs as keys
    """
    # # default value
    argsDict = argsDict if argsDict else {}
    # # get replicas for lfn
    res = FileCatalog().getReplicas( lfn )
    if not res["OK"]:
      errStr = "_callReplicaSEFcn: Completely failed to get replicas for LFNs."
      self.log.debug( errStr, res["Message"] )
      return res
    # # returned dict, get failed replicase
    retDict = { "Failed": res["Value"]["Failed"],
                "Successful" : {} }
    # # print errors
    for lfn, reason in retDict["Failed"].items():
      self.log.error( "_callReplicaSEFcn: Failed to get replicas for file.", "%s %s" % ( lfn, reason ) )
    # # good replicas
    lfnReplicas = res["Value"]["Successful"]
    # # store PFN to LFN mapping
    pfnDict = {}
    se = None  # Placeholder for the StorageElement object
    for lfn, replicas in lfnReplicas.items():
      if storageElementName  in replicas:
        if self.useCatalogPFN:
          pfn = replicas[storageElementName]
        else:
          se = se if se else StorageElement( storageElementName )
          res = se.getPfnForLfn( lfn )
          pfn = res.get( 'Value', {} ).get( 'Successful', {} ).get( lfn, replicas[storageElementName] )
        pfnDict[pfn] = lfn
      else:
        errStr = "_callReplicaSEFcn: File hasn't got replica at supplied Storage Element."
        self.log.error( errStr, "%s %s" % ( lfn, storageElementName ) )
        retDict["Failed"][lfn] = errStr

    # # call StorageElement function at least
    se = se = se if se else StorageElement( storageElementName )
    fcn = getattr( se, method )
    res = fcn( pfnDict.keys(), **argsDict )
    # # check result
    if not res["OK"]:
      errStr = "_callReplicaSEFcn: Failed to execute %s StorageElement method." % method
      self.log.error( errStr, res["Message"] )
      return res

    # # filter out failed and successful
    for pfn, pfnRes in res["Value"]["Successful"].items():
      retDict["Successful"][pfnDict[pfn]] = pfnRes
    for pfn, errorMessage in res["Value"]["Failed"].items():
      retDict["Failed"][pfnDict[pfn]] = errorMessage

    return S_OK( retDict )

  def getReplicaIsFile( self, lfn, storageElementName ):
    """ determine whether the supplied lfns are files at the supplied StorageElement

    :param self: self reference
    :param mixed lfn: LFN string, list if LFNs or dict with LFNs as keys
    :param str storageElementName: DIRAC SE name
    :param bool singleFile: execute for the first LFN only
    """
    return self.__executeIfReplicaExists( storageElementName, lfn, "isFile" )

  def getReplicaSize( self, lfn, storageElementName ):
    """ get the size of files for the lfns at the supplied StorageElement

    :param self: self reference
    :param mixed lfn: LFN string, list if LFNs or dict with LFNs as keys
    :param str storageElementName: DIRAC SE name
    :param bool singleFile: execute for the first LFN only
    """
    return self.__executeIfReplicaExists( storageElementName, lfn, "getFileSize" )

  def getReplicaAccessUrl( self, lfn, storageElementName ):
    """ get the access url for lfns at the supplied StorageElement

    :param self: self reference
    :param mixed lfn: LFN string, list if LFNs or dict with LFNs as keys
    :param str storageElementName: DIRAC SE name
    :param bool singleFile: execute for the first LFN only
    """
    return self.__executeIfReplicaExists( storageElementName, lfn, "getAccessUrl" )

  def getReplicaMetadata( self, lfn, storageElementName ):
    """ get the file metadata for lfns at the supplied StorageElement

    :param self: self reference
    :param mixed lfn: LFN string, list if LFNs or dict with LFNs as keys
    :param str storageElementName: DIRAC SE name
    :param bool singleFile: execute for the first LFN only
    """
    return self.__executeIfReplicaExists( storageElementName, lfn, "getFileMetadata" )

  def prestageReplica( self, lfn, storageElementName, lifetime = 86400 ):
    """ issue a prestage requests for the lfns at the supplied StorageElement

    :param self: self reference
    :param mixed lfn: LFN string, list if LFNs or dict with LFNs as keys
    :param str storageElementName: DIRAC SE name
    :param int lifetime: 24h in seconds
    :param bool singleFile: execute for the first LFN only
    """
    return self.__executeIfReplicaExists( storageElementName, lfn,
                                                  "prestageFile", lifetime = lifetime )


  def pinReplica( self, lfn, storageElementName, lifetime = 86400 ):
    """ pin the lfns at the supplied StorageElement

    :param self: self reference
    :param mixed lfn: LFN string, list if LFNs or dict with LFNs as keys
    :param str storageElementName: DIRAC SE name
    :param int lifetime: 24h in seconds
    :param bool singleFile: execute for the first LFN only
    """
    return self.__executeIfReplicaExists( storageElementName, lfn,
                                                  "pinFile", lifetime = lifetime )

  def releaseReplica( self, lfn, storageElementName ):
    """ release pins for the lfns at the supplied StorageElement

    :param self: self reference
    :param mixed lfn: LFN string, list if LFNs or dict with LFNs as keys
    :param str storageElementName: DIRAC SE name
    :param bool singleFile: execute for the first LFN only
    """
    return self.__executeIfReplicaExists( storageElementName, lfn, "releaseFile" )

  def getReplica( self, lfn, storageElementName, localPath = False ):
    """ copy replicas from DIRAC SE to local directory

    :param self: self reference
    :param mixed lfn: LFN string, list if LFNs or dict with LFNs as keys
    :param str storageElementName: DIRAC SE name
    :param mixed localPath: path in the local file system, if False, os.getcwd() will be used
    :param bool singleFile: execute for the first LFN only
    """
    return self.__executeIfReplicaExists( storageElementName, lfn,
                                                  "getFile", localPath = localPath )



  # we should so something to get rid of this one
  def removeCatalogFile( self, lfn ):
    """ remove a file from the FileCatalog

    :param self: self reference
    :param mixed lfn: LFN as string or list of LFN strings or dict with LFNs as keys
    :param bool singleFile: execute for the first LFN only
    :param list catalogs: catalogs' names
    """

    # # make sure lfns are sorted from the longest to the shortest
    if type( lfn ) == ListType:
      lfn = sorted( lfn, reverse = True )
    return FileCatalog().removeFile( lfn )
