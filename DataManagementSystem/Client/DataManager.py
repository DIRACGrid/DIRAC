"""
:mod: DataManager

.. module: DataManager

:synopsis: DataManager links the functionalities of StorageElement and FileCatalog.

This module consists of DataManager and related classes.

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
from DIRAC.ConfigurationSystem.Client.Helpers.Resources     import getRegistrationProtocols, getThirdPartyProtocols
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
from DIRAC.Core.Utilities.ReturnValues import returnSingleResult

class DataManager( object ):
  """
  .. class:: DataManager

  A DataManager is taking all the actions that impact or require the FileCatalog and the StorageElement together
  """
  def __init__( self, catalogs = [], masterCatalogOnly = False, vo = False ):
    """ c'tor

    :param self: self reference
    :param catalogs: the list of catalog in which to perform the operations. This
                    list will be ignored if masterCatalogOnly is set to True
    :param masterCatalogOnly: if set to True, the operations will be performed only on the master catalog.
                              The catalogs parameter will be ignored.
    :param vo: the VO for which the DataManager is created, get VO from the current proxy if not specified
    """
    self.log = gLogger.getSubLogger( self.__class__.__name__, True )
    self.vo = vo

    catalogsToUse = FileCatalog( vo = self.vo ).getMasterCatalogNames()['Value'] if masterCatalogOnly else catalogs

    self.fc = FileCatalog( catalogs = catalogsToUse, vo = self.vo )
    self.accountingClient = None
    self.registrationProtocol = getRegistrationProtocols()
    self.thirdPartyProtocols = getThirdPartyProtocols()
    self.resourceStatus = ResourceStatus()
    self.ignoreMissingInFC = Operations( self.vo ).getValue( 'DataManagement/IgnoreMissingInFC', False )
    self.useCatalogPFN = Operations( self.vo ).getValue( 'DataManagement/UseCatalogPFN', True )

  def setAccountingClient( self, client ):
    """ Set Accounting Client instance
    """
    self.accountingClient = client

  def __verifyWritePermission( self, path ):
    """  Check if we have write permission to the given file (if exists) or its directory
    """
    if type( path ) in StringTypes:
      paths = [ path ]
    else:
      paths = path

    res = self.fc.getPathPermissions( paths )
    if not res['OK']:
      return res
    result = {'Successful':[], 'Failed':[]}
    for path in paths:
      if res['Value']['Successful'].get( path, {} ).get( 'Write', False ):
        result['Successful'].append( path )
      else:
        result['Failed'].append( path )
    return S_OK( result )

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
    res = self.__verifyWritePermission( folder )
    if folder not in res['Value']['Successful']:
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
    res = returnSingleResult( self.fc.removeDirectory( folder, recursive = True ) )
    if not res['OK']:
      return res
    return S_OK()

  def __removeStorageDirectory( self, directory, storageElement ):
    """ delete SE directory

    :param self: self reference
    :param str directory: folder to be removed
    :param str storageElement: DIRAC SE name
    """

    se = StorageElement( storageElement, vo = self.vo )
    res = returnSingleResult( se.exists( directory ) )

    if not res['OK']:
      self.log.debug( "Failed to obtain existance of directory", res['Message'] )
      return res

    exists = res['Value']
    if not exists:
      self.log.debug( "The directory %s does not exist at %s " % ( directory, storageElement ) )
      return S_OK()

    res = returnSingleResult( se.removeDirectory( directory, recursive = True ) )
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
      res = returnSingleResult( self.fc.listDirectory( currentDir ) )
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
      res = returnSingleResult( self.fc.listDirectory( currentDir, verbose = ( days != 0 ) ) )
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
      se = StorageElement( storageElementName, vo = self.vo )

      oDataOperation = self.__initialiseAccountingObject( 'getFile', storageElementName, 1 )
      oDataOperation.setStartTime()
      startTime = time.time()

      res = returnSingleResult( se.getFile( lfn, localPath = os.path.realpath( destinationDir ) ) )

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
#     ancestors = ancestors if ancestors else list(
    folder = os.path.dirname( lfn )
    res = self.__verifyWritePermission( folder )
    if folder not in res['Value']['Successful']:
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
    storageElement = StorageElement( diracSE, vo = self.vo )
    res = storageElement.isValid()
    if not res['OK']:
      errStr = "putAndRegister: The storage element is not currently valid."
      self.log.debug( errStr, "%s %s" % ( diracSE, res['Message'] ) )
      return S_ERROR( errStr )
    destinationSE = storageElement.getStorageElementName()['Value']

    res = returnSingleResult( storageElement.getURL( lfn ) )
    if not res['OK']:
      errStr = "putAndRegister: Failed to generate destination PFN."
      self.log.debug( errStr, res['Message'] )
      return S_ERROR( errStr )
    destUrl = res['Value']

    fileDict = {lfn:fileName}

    successful = {}
    failed = {}
    ##########################################################
    #  Perform the put here.
    oDataOperation = self.__initialiseAccountingObject( 'putAndRegister', diracSE, 1 )
    oDataOperation.setStartTime()
    oDataOperation.setValueByKey( 'TransferSize', size )
    startTime = time.time()
    res = returnSingleResult( storageElement.putFile( fileDict ) )
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
    fileTuple = ( lfn, destUrl, size, destinationSE, guid, checksum )
    registerDict = {'LFN':lfn, 'PFN':destUrl, 'Size':size, 'TargetSE':destinationSE, 'GUID':guid, 'Addler':checksum}
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


  def __replicate( self, lfn, destSEName, sourceSEName = None, destPath = None, localCache = None ):
    """ Replicate a LFN to a destination SE.

        'lfn' is the LFN to be replicated
        'destSE' is the Storage Element the file should be replicated to
        'sourceSE' is the source for the file replication (where not specified all replicas will be attempted)
        'destPath' is the path on the destination storage element, if to be different from LHCb convention
        'localCache' if cannot do third party transfer, we do get and put through this local directory
    """

    log = self.log.getSubLogger( '__replicate', True )

    ###########################################################
    # Check that we have write permissions to this directory.
    res = self.__verifyWritePermission( lfn )
    if lfn not in res['Value']['Successful']:
      errStr = "__replicate: Write access not permitted for this credential."
      log.debug( errStr, lfn )
      return S_ERROR( errStr )


    # Check that the destination storage element is sane and resolve its name
    log.debug( "Verifying destination StorageElement validity (%s)." % ( destSEName ) )

    destStorageElement = StorageElement( destSEName, vo = self.vo )
    res = destStorageElement.isValid()
    if not res['OK']:
      errStr = "The storage element is not currently valid."
      log.debug( errStr, "%s %s" % ( destSEName, res['Message'] ) )
      return S_ERROR( errStr )

    # Get the real name of the SE
    destSEName = destStorageElement.getStorageElementName()['Value']

    ###########################################################
    # Check whether the destination storage element is banned
    log.verbose( "Determining whether %s ( destination ) is Write-banned." % destSEName )

    if not self.__SEActive( destSEName ).get( 'Value', {} ).get( 'Write' ):
      infoStr = "Supplied destination Storage Element is not currently allowed for Write."
      log.debug( infoStr, destSEName )
      return S_ERROR( infoStr )


    # Get the LFN replicas from the file catalog
    log.debug( "Attempting to obtain replicas for %s." % ( lfn ) )
    res = returnSingleResult( self.getReplicas( lfn ) )
    if not res[ 'OK' ]:
      errStr = "%Failed to get replicas for LFN."
      log.debug( errStr, "%s %s" % ( lfn, res['Message'] ) )
      return S_ERROR( "%s %s" % ( errStr, res['Message'] ) )

    log.debug( "Successfully obtained replicas for LFN." )
    lfnReplicas = res['Value']


    ###########################################################
    # If the file catalog size is zero fail the transfer
    log.debug( "Attempting to obtain size for %s." % lfn )
    res = returnSingleResult( self.fc.getFileSize( lfn ) )
    if not res['OK']:
      errStr = "Failed to get size for LFN."
      log.debug( errStr, "%s %s" % ( lfn, res['Message'] ) )
      return S_ERROR( "%s %s" % ( errStr, res['Message'] ) )

    catalogSize = res['Value']

    if catalogSize == 0:
      errStr = "Registered file size is 0."
      log.debug( errStr, lfn )
      return S_ERROR( errStr )

    log.debug( "File size determined to be %s." % catalogSize )



    ###########################################################
    # If the LFN already exists at the destination we have nothing to do
    if destSEName in lfnReplicas:
      log.debug( "__replicate: LFN is already registered at %s." % destSEName )
      return S_OK()

    ###########################################################
    # If the source is specified, check that it is in the replicas
    
    if sourceSEName:
      log.debug( "Determining whether source Storage Element specified is sane." )

      if sourceSEName not in lfnReplicas:
        errStr = "LFN does not exist at supplied source SE."
        log.error( errStr, "%s %s" % ( lfn, sourceSEName ) )
        return S_ERROR( errStr )



    # If sourceSE is specified, then we consider this one only, otherwise
    # we consider them all

    possibleSourceSEs = [sourceSEName] if sourceSEName else  lfnReplicas.keys()
    
    # We sort the possibileSourceSEs with the SEs that are on the same site than the destination first
    # reverse = True because True > False
    possibleSourceSEs = sorted( possibleSourceSEs,
                                key = lambda x : isSameSiteSE( x, destSEName ).get( 'Value', False ),
                                reverse = True )

    # In case we manage to find SEs that would work as a source, but we can't negotiate a protocol
    # we will do a get and put using one of this sane SE
    possibleSEsForIntermediateTransfer = []

    # Take into account the destination path
    if destPath:
      destPath = '%s/%s' % ( destPath, os.path.basename( lfn ) )
    else:
      destPath = lfn


    for candidateSEName in possibleSourceSEs:

      log.debug( "Consider %s as a source" % candidateSEName )

      # Check that the candidate is active
      if not self.__SEActive( candidateSEName ).get( 'Value', {} ).get( 'Read' ):
        log.debug( "%s is currently not allowed as a source." % candidateSEName )
        continue
      else:
        log.debug( "%s is available for use." % candidateSEName )

      
      candidateSE = StorageElement( candidateSEName, vo = self.vo )

      # Check that the SE is valid
      res = candidateSE.isValid()
      if not res['OK']:
        log.debug( "The storage element is not currently valid.", "%s %s" % ( candidateSEName, res['Message'] ) )
        continue
      else:
        log.debug( "The storage is currently valid", candidateSEName )

      # Check that the file size corresponds to the one in the FC
      res = returnSingleResult( candidateSE.getFileSize( lfn ) )
      if not res['OK']:
        log.debug( "could not get fileSize on %s" % candidateSEName, res['Message'] )
        continue

      seFileSize = res['Value']

      
      if seFileSize != catalogSize:
        log.debug( "Catalog size and physical file size mismatch.", "%s %s" % ( catalogSize, seFileSize ) )
        continue
      else:
        log.debug( "Catalog size and physical size match" )


      res = destStorageElement.negociateProtocolWithOtherSE( candidateSE, protocols = self.thirdPartyProtocols )

      if not res['OK']:
        log.debug( "Error negotiating replication protocol", res['Message'] )
        continue
      

      replicationProtocol = res['Value']

      if not replicationProtocol:
        possibleSEsForIntermediateTransfer.append( candidateSE )
        log.debug( "No protocol suitable for replication found" )
        continue

      log.debug( 'Found common protocols', replicationProtocol )

      # THIS WOULD NOT WORK IF PROTO == file !!

      # Compare the urls to make sure we are not overwriting
      res = returnSingleResult( candidateSE.getURL( lfn, protocol = replicationProtocol ) )
      if not res['OK']:
        log.debug( "Cannot get sourceURL", res['Message'] )
        continue

      sourceURL = res['Value']
        
      res = returnSingleResult( destStorageElement.getURL( destPath, protocol = replicationProtocol ) )
      if not res['OK']:
        log.debug( "Cannot get destURL", res['Message'] )
        continue
      destURL = res['Value']

      if sourceURL == destURL:
        log.debug( "Same source and destination, give up" )
        continue

      # Attempt the transfer
      res = returnSingleResult( destStorageElement.replicateFile( {destPath:sourceURL}, sourceSize = catalogSize ) )

      if not res['OK']:
        log.debug( "Replication failed", "%s from %s to %s." % ( lfn, candidateSEName, destSEName ) )
        continue
      
      
      log.debug( "Replication successful.", res['value'] )
      
      res = returnSingleResult( destStorageElement.getURL(destPath,  protocol = self.registrationProtocol))
      if not res['OK']:
        log.debug( 'Error getting the registration URL', res['Message'] )
        # it's maybe pointless to try the other candidateSEs...
        continue
      
      registrationURL = res['Value']
      
      return S_OK( {'DestSE':destSEName, 'DestPfn':registrationURL} )

      

    # If we are here, that means that we could not make a third party transfer.
    # Check if we have some sane SEs from which we could do a get/put

    localDir = os.path.realpath( localCache if localCache else '.' )
    localFile = os.path.join( localDir, os.path.basename( lfn ) )

    log.debug( "Will try intermediate transfer from %s sources" % len( possibleSEsForIntermediateTransfer ) )

    for candidateSE in possibleSEsForIntermediateTransfer:

      res = returnSingleResult( candidateSE.getFile( lfn, localPath = localDir ) )
      if not res['OK']:
        log.debug( 'Error getting the file from %s' % candidateSE.name, res['Message'] )
        continue

      res = returnSingleResult( destStorageElement.putFile( {destPath:localFile} ) )
      if not res['OK']:
        log.debug( 'Error putting file coming from %s' % candidateSE.name, res['Message'] )
        # if the put is the problem, it's maybe pointless to try the other candidateSEs...
        continue

      # get URL with default protocol to return it
      res = returnSingleResult( destStorageElement.getURL( destPath, protocol = self.registrationProtocol ) )
      if not res['OK']:
        log.debug( 'Error getting the registration URL', res['Message'] )
        # it's maybe pointless to try the other candidateSEs...
        continue

      registrationURL = res['Value']
      return S_OK( {'DestSE':destSEName, 'DestPfn':registrationURL} )


    # If here, we are really doomed
    errStr = "Failed to replicate with all sources."
    log.debug( errStr, lfn )
    return S_ERROR( errStr )




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
      return res
    # Remove Failed LFNs if they are in success
    success = res['Value']['Successful']
    failed = res['Value']['Failed']
    return res

  def __registerFile( self, fileTuples, catalog ):
    """ register file to cataloge """

    fileDict = {}

    for lfn, physicalFile, fileSize, storageElementName, fileGuid, checksum in fileTuples:
      fileDict[lfn] = {'PFN':physicalFile, 'Size':fileSize, 'SE':storageElementName, 'GUID':fileGuid, 'Checksum':checksum}

    if catalog:
      fileCatalog = FileCatalog( catalog, vo = self.vo )
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
    # Remove Failed LFNs if they are in success
    success = res['Value']['Successful']
    failed = res['Value']['Failed']
    return res

  def __registerReplica( self, replicaTuples, catalog ):
    """ register replica to catalogue """
    seDict = {}
    for lfn, url, storageElementName in replicaTuples:
      seDict.setdefault( storageElementName, [] ).append( ( lfn, url ) )
    failed = {}
    replicaTuples = []
    for storageElementName, replicaTuple in seDict.items():
      destStorageElement = StorageElement( storageElementName, vo = self.vo )
      res = destStorageElement.isValid()
      if not res['OK']:
        errStr = "__registerReplica: The storage element is not currently valid."
        self.log.debug( errStr, "%s %s" % ( storageElementName, res['Message'] ) )
        for lfn, url in replicaTuple:
          failed[lfn] = errStr
      else:
        storageElementName = destStorageElement.getStorageElementName()['Value']
        for lfn, url in replicaTuple:
          res = returnSingleResult( destStorageElement.getURL( lfn, protocol = self.registrationProtocol ) )
          if not res['OK']:
            failed[lfn] = res['Message']
          else:
            replicaTuple = ( lfn, res['Value'], storageElementName, False )
            replicaTuples.append( replicaTuple )
    self.log.debug( "__registerReplica: Successfully resolved %s replicas for registration." % len( replicaTuples ) )
    # HACK!
    replicaDict = {}
    for lfn, url, se, _master in replicaTuples:
      replicaDict[lfn] = {'SE':se, 'PFN':url}

    if catalog:
      fileCatalog = FileCatalog( catalog, vo = self.vo )
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
    # Check that we have write permissions to this directory and to the file.
    if lfns:
      dir4lfns = {}
      for lfn in lfns:
        dir4lfns.setdefault( os.path.dirname( lfn ), [] ).append( lfn )
      res = self.__verifyWritePermission( dir4lfns.keys() )
      if res['Value']['Failed']:
        errStr = "removeFile: Write access not permitted for this credential."
        self.log.debug( errStr, 'for %d files' % len( res['Value']['Failed'] ) )
        failed.update( dict.fromkeys( [lfn for dirName in res['Value']['Failed'] for lfn in dir4lfns[dirName]], errStr ) )
        lfns = list( set( [lfn for dirName in res['Value']['Successful'] for lfn in dir4lfns[dirName] ] ) )

      if lfns:
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
        storageElementDict.setdefault( se, [] ).append( lfn )
    failed = {}
    successful = {}
    for storageElementName in sorted( storageElementDict ):
      lfns = storageElementDict[storageElementName]
      res = self.__removeReplica( storageElementName, lfns, replicaDict = lfnDict )
      if not res['OK']:
        errStr = res['Message']
        for lfn in lfns:
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
    successful = {}
    failed = {}
    # Check that we have write permissions to this file.
    res = self.__verifyWritePermission( lfns )
    if res['Value']['Failed']:
      errStr = "removeReplica: Write access not permitted for this credential."
      self.log.debug( errStr, 'for %d files' % len( res['Value']['Failed'] ) )
      failed.update( dict.fromkeys( res['Value']['Failed'], errStr ) )
      lfns = [lfn for lfn in lfns if lfn not in res['Value']['Failed']]

    self.log.debug( "removeReplica: Will remove catalogue entry for %s lfns at %s." % ( len( lfns ),
                                                                                          storageElementName ) )
    res = self.fc.getReplicas( lfns, True )
    if not res['OK']:
      errStr = "removeReplica: Completely failed to get replicas for lfns."
      self.log.debug( errStr, res['Message'] )
      return res
    failed.update( res['Value']['Failed'] )
    replicaDict = res['Value']['Successful']
    lfnsToRemove = []
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
        lfnsToRemove.append( lfn )
    if not lfnsToRemove:
      return S_OK( { 'Successful' : successful, 'Failed' : failed } )    
    res = self.__removeReplica( storageElementName, lfnsToRemove, replicaDict = replicaDict )
    if not res['OK']:
      return res
    failed.update( res['Value']['Failed'] )
    successful.update( res['Value']['Successful'] )
    gDataStoreClient.commit()
    return S_OK( { 'Successful' : successful, 'Failed' : failed } )


  def __removeReplica( self, storageElementName, lfns, replicaDict = None ):
    """ remove replica
        Remove the replica from the storageElement, and then from the catalog

        :param storageElementName : The name of the storage Element
        :param lfns list of lfn we want to remove
        :param replicaDict : cache of fc.getReplicas(lfns) : { lfn { se : catalog url } }

    """
    failed = {}
    successful = {}
    replicaDict = replicaDict if replicaDict else {}
    
    lfnsToRemove = []

    for lfn in lfns:
      res = self.__verifyWritePermission( lfn )
      if lfn not in res['Value']['Successful']:
        errStr = "__removeReplica: Write access not permitted for this credential."
        self.log.debug( errStr, lfn )
        failed[lfn] = errStr
      else:
        lfnsToRemove.append( lfn )


    res = self.__removePhysicalReplica( storageElementName, lfnsToRemove, replicaDict = replicaDict )

    if not res['OK']:
      errStr = "__removeReplica: Failed to remove physical replicas."
      self.log.debug( errStr, res['Message'] )
      return S_ERROR( res['Message'] )

    failed.update( dict( [( lfn, error ) for lfn, error in res['Value']['Failed'].items()] ) )

    # Here we use the FC PFN...
    replicaTuples = [( lfn, replicaDict[lfn][storageElementName], storageElementName ) for lfn in res['Value']['Successful']]

    if not replicaTuples:
      return S_OK( { 'Successful' : successful, 'Failed' : failed } )

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
    """ remove replica form catalogue
      :param replicaTuple : list of (lfn, catalogPFN, se)
     """
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

  def removePhysicalReplicaLegacy( self, storageElementName, lfn ):
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
    successful = {}
    failed = {}
    # Check that we have write permissions to this directory.
    res = self.__verifyWritePermission( lfns )
    if res['Value']['Failed']:
      errStr = "removePhysicalReplica: Write access not permitted for this credential."
      self.log.debug( errStr, 'for %d files' % len( res['Value']['Failed'] ) )
      failed.update( dict.fromkeys( res['Value']['Failed'], errStr ) )
      lfns = [lfn for lfn in lfns if lfn not in res['Value']['Failed']]
    self.log.debug( "removePhysicalReplica: Attempting to remove %s lfns at %s." % ( len( lfns ),
                                                                                       storageElementName ) )
    self.log.debug( "removePhysicalReplica: Attempting to resolve replicas." )
    res = self.getReplicas( lfns )
    if not res['OK']:
      errStr = "removePhysicalReplica: Completely failed to get replicas for lfns."
      self.log.debug( errStr, res['Message'] )
      return res
    failed.update( res['Value']['Failed'] )
    replicaDict = res['Value']['Successful']
    successful = {}
    lfnsToRemove = []
    for lfn, repDict in replicaDict.items():
      if storageElementName not in repDict:
        # The file doesn't exist at the storage element so don't have to remove it
        successful[lfn] = True
      else:
        lfnsToRemove.append( lfn )
    self.log.debug( "removePhysicalReplica: Resolved %s pfns for removal at %s." % ( len( lfnsToRemove ),
                                                                                       storageElementName ) )
    res = self.__removePhysicalReplica( storageElementName, lfnsToRemove, replicaDict = replicaDict )
    for lfn, error in res['Value']['Failed'].items():
      failed[lfn] = error
    for lfn in res['Value']['Successful']:
      successful[lfn] = True
    resDict = { 'Successful' : successful, 'Failed' : failed }
    return S_OK( resDict )


  def __removePhysicalReplica( self, storageElementName, lfnsToRemove, replicaDict = None ):
    """ remove replica from storage element

        :param storageElementName : name of the storage Element
        :param lfnsToRemove : list of lfn to removes
        :param replicaDict : cache of fc.getReplicas, to be passed to the SE
     """
    self.log.debug( "__removePhysicalReplica: Attempting to remove %s pfns at %s." % ( len( lfnsToRemove ),
                                                                                         storageElementName ) )
    storageElement = StorageElement( storageElementName, vo = self.vo )
    res = storageElement.isValid()
    if not res['OK']:
      errStr = "__removePhysicalReplica: The storage element is not currently valid."
      self.log.debug( errStr, "%s %s" % ( storageElementName, res['Message'] ) )
      return S_ERROR( errStr )
    oDataOperation = self.__initialiseAccountingObject( 'removePhysicalReplica',
                                                        storageElementName,
                                                        len( lfnsToRemove ) )
    oDataOperation.setStartTime()
    start = time.time()
    ret = storageElement.getFileSize( lfnsToRemove, replicaDict = replicaDict )
    deletedSizes = ret.get( 'Value', {} ).get( 'Successful', {} )
    res = storageElement.removeFile( lfnsToRemove, replicaDict = replicaDict )
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
      for lfn, value in res['Value']['Failed'].items():
        if 'No such file or directory' in value:
          res['Value']['Successful'][lfn] = lfn
          res['Value']['Failed'].pop( lfn )
      for lfn in res['Value']['Successful']:
        res['Value']['Successful'][lfn] = True

      deletedSize = sum( [size for lfn, size in deletedSizes.items() if lfn in res['Value']['Successful']] )
      oDataOperation.setValueByKey( 'TransferSize', deletedSize )
      oDataOperation.setValueByKey( 'TransferOK', len( res['Value']['Successful'] ) )

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
    storageElement = StorageElement( diracSE, vo = self.vo )
    res = storageElement.isValid()
    if not res['OK']:
      errStr = "put: The storage element is not currently valid."
      self.log.debug( errStr, "%s %s" % ( diracSE, res['Message'] ) )
      return S_ERROR( errStr )
    fileDict = {lfn:fileName}

    successful = {}
    failed = {}
    ##########################################################
    #  Perform the put here.
    startTime = time.time()
    res = returnSingleResult( storageElement.putFile( fileDict ) )
    putTime = time.time() - startTime
    if not res['OK']:
      errStr = "put: Failed to put file to Storage Element."
      failed[lfn] = res['Message']
      self.log.debug( errStr, "%s: %s" % ( fileName, res['Message'] ) )
    else:
      self.log.debug( "put: Put file to storage in %s seconds." % putTime )
      successful[lfn] = res['Value']
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
    if res['Value'][resolvedName].get( 'ReadAccess', 'Active' ) not in ( 'Active', 'Degraded' ):
      seStatus[ 'Read' ] = False
    if res['Value'][resolvedName].get( 'WriteAccess', 'Active' ) not in ( 'Active', 'Degraded' ):
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

        # We group the query to getURL by storage element to gain in speed
        for lfn in catalogReplicas:
          for se in catalogReplicas[lfn]:
            se_lfn.setdefault( se, [] ).append( lfn )

        for se in se_lfn:
          seObj = StorageElement( se, vo = self.vo )
          succPfn = seObj.getURL( se_lfn[se], protocol = self.registrationProtocol ).get( 'Value', {} ).get( 'Successful', {} )
          for lfn in succPfn:
            # catalogReplicas still points res["value"]["Successful"] so res will be updated
            catalogReplicas[lfn][se] = succPfn[lfn]

    return res


  ##################################################################################################3
  # Methods from the catalogToStorage. It would all work with the direct call to the SE, but this checks
  # first if the replica is known to the catalog


  def __executeIfReplicaExists( self, storageElementName, lfn, method, **kwargs ):
    """ a simple wrapper that allows replica querying then perform the StorageElement operation

    :param self: self reference
    :param str storageElementName: DIRAC SE name
    :param mixed lfn: a LFN str, list of LFNs or dict with LFNs as keys
    """
    # # default value
    kwargs = kwargs if kwargs else {}
    # # get replicas for lfn
    res = FileCatalog( vo = self.vo ).getReplicas( lfn )
    if not res["OK"]:
      errStr = "__executeIfReplicaExists: Completely failed to get replicas for LFNs."
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
    lfnList = []
    se = None  # Placeholder for the StorageElement object
    for lfn, replicas in lfnReplicas.items():
      if storageElementName  in replicas:
        lfnList.append( lfn )
      else:
        errStr = "__executeIfReplicaExists: File hasn't got replica at supplied Storage Element."
        self.log.error( errStr, "%s %s" % ( lfn, storageElementName ) )
        retDict["Failed"][lfn] = errStr

    if 'replicaDict' not in kwargs:
      kwargs['replicaDict'] = lfnReplicas

    # # call StorageElement function at least
    se = se = se if se else StorageElement( storageElementName, vo = self.vo )
    fcn = getattr( se, method )
    res = fcn( lfnList, **kwargs )
    # # check result
    if not res["OK"]:
      errStr = "__executeIfReplicaExists: Failed to execute %s StorageElement method." % method
      self.log.error( errStr, res["Message"] )
      return res

    # # filter out failed and successful
    for lfn, lfnRes in res["Value"]["Successful"].items():
      retDict["Successful"][lfn] = lfnRes
    for lfn, errorMessage in res["Value"]["Failed"].items():
      retDict["Failed"][lfn] = errorMessage

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

  def getReplicaAccessUrl( self, lfn, storageElementName, protocol = False ):
    """ get the access url for lfns at the supplied StorageElement

    :param self: self reference
    :param mixed lfn: LFN string, list if LFNs or dict with LFNs as keys
    :param str storageElementName: DIRAC SE name
    :param bool singleFile: execute for the first LFN only
    """
    return self.__executeIfReplicaExists( storageElementName, lfn, "getURL", protocol = protocol )

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

