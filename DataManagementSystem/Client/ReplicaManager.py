""" This is the Replica Manager which links the functionalities of StorageElement and FileCatalog. """

__RCSID__ = "$Id$"

import re, time, commands, random, os, fnmatch
import types
from datetime import datetime, timedelta
import DIRAC

from DIRAC                                               import S_OK, S_ERROR, gLogger, gConfig
from DIRAC.AccountingSystem.Client.Types.DataOperation   import DataOperation
from DIRAC.AccountingSystem.Client.DataStoreClient       import gDataStoreClient
from DIRAC.Core.Utilities.File                           import makeGuid, getSize
from DIRAC.Core.Utilities.Adler                          import fileAdler, compareAdler
from DIRAC.Core.Utilities.List                           import sortList, randomize
from DIRAC.Core.Utilities.SiteSEMapping                  import getSEsForSite, isSameSiteSE, getSEsForCountry
from DIRAC.Resources.Storage.StorageElement              import StorageElement
from DIRAC.Resources.Catalog.FileCatalog                 import FileCatalog

class CatalogBase:

  def __init__( self ):
    """ This class stores the two wrapper functions for interacting with the FileCatalog:

        _executeFileCatalogFunction(lfn,method,argsDict={},catalogs=[])
          Is a wrapper around the available FileCatlog() functions.
          The 'lfn' and 'method' arguments must be provided:
            'lfn' contains a single file string or a list or dictionary containing the required files.
            'method' is the name of the FileCatalog() to be invoked.
          'argsDict' contains aditional arguments that are requred for the method.
          'catalogs' is the list of catalogs the operation is to be performed on.
             By default this is all available catalogs.
             Examples are 'LcgFileCatalogCombined', 'BookkeepingDB', 'ProductionDB'

        _executeSingleFileCatalogFunction(lfn,method,argsDict={},catalogs=[])
          Is a wrapper around _executeFileCatalogFunction().
          It parses the output of _executeFileCatalogFunction() for the first file provided as input.
          If this file is found in:
            res['Value']['Successful'] an S_OK() is returned with the value.
            res['Value']['Failed'] an S_ERROR() is returned with the error message.
    """
    pass

  def _executeSingleFileCatalogFunction( self, lfn, method, argsDict = {}, catalogs = [] ):
    res = self._executeFileCatalogFunction( lfn, method, argsDict, catalogs = catalogs )
    if type( lfn ) == types.ListType:
      singleLfn = lfn[0]
    elif type( lfn ) == types.DictType:
      singleLfn = lfn.keys()[0]
    else:
      singleLfn = lfn
    if not res['OK']:
      return res
    elif res['Value']['Failed'].has_key( singleLfn ):
      errorMessage = res['Value']['Failed'][singleLfn]
      return S_ERROR( errorMessage )
    else:
      return S_OK( res['Value']['Successful'][singleLfn] )

  def _executeFileCatalogFunction( self, lfn, method, argsDict = {}, catalogs = [] ):
    """ A simple wrapper around the file catalog functionality
    """
    # First check the supplied lfn(s) are the correct format.
    if type( lfn ) in types.StringTypes:
      lfns = {lfn:False}
    elif type( lfn ) == types.ListType:
      lfns = {}
      for lfn in lfn:
        lfns[lfn] = False
    elif type( lfn ) == types.DictType:
      lfns = lfn.copy()
    else:
      errStr = "ReplicaManager._executeFileCatalogFunction: Supplied lfns must be string or list of strings or a dictionary."
      gLogger.error( errStr )
      return S_ERROR( errStr )
    # Check we have some lfns
    if not lfns:
      errMessage = "ReplicaManager._executeFileCatalogFunction: No lfns supplied."
      gLogger.error( errMessage )
      return S_ERROR( errMessage )
    gLogger.debug( "ReplicaManager._executeFileCatalogFunction: Attempting to perform '%s' operation with %s lfns." % ( method, len( lfns ) ) )
    # Check we can instantiate the file catalog correctly
    fileCatalog = FileCatalog( catalogs )
    # Generate the execution string
    if argsDict:
      execString = "res = fileCatalog.%s(lfns" % method
      for argument, value in argsDict.items():
        if type( value ) == types.StringType:
          execString = "%s, %s='%s'" % ( execString, argument, value )
        else:
          execString = "%s, %s=%s" % ( execString, argument, value )
      execString = "%s)" % execString
    else:
      execString = "res = fileCatalog.%s(lfns)" % method
    # Execute the execute string
    try:
      exec( execString )
    except AttributeError, errMessage:
      exceptStr = "ReplicaManager._executeFileCatalogFunction: Exception while perfoming %s." % method
      gLogger.exception( exceptStr, str( errMessage ) )
      return S_ERROR( exceptStr )
    # Return the output
    if not res['OK']:
      errStr = "ReplicaManager._executeFileCatalogFunction: Completely failed to perform %s." % method
      gLogger.error( errStr, res['Message'] )
    return res

class CatalogFile( CatalogBase ):

  def getCatalogExists( self, lfn, singleFile = False, catalogs = [] ):
    """ Determine whether the path is registered in the FileCatalog

        'lfn' is the files to check (can be a single file or list of lfns)
    """
    if singleFile:
      return self._executeSingleFileCatalogFunction( lfn, 'exists', catalogs = catalogs )
    else:
      return self._executeFileCatalogFunction( lfn, 'exists', catalogs = catalogs )

  def getCatalogIsFile( self, lfn, singleFile = False, catalogs = [] ):
    """ Determine whether the path is registered as a file in the FileCatalog

        'lfn' is the files to check (can be a single file or list of lfns)
    """
    if singleFile:
      return self._executeSingleFileCatalogFunction( lfn, 'isFile', catalogs = catalogs )
    else:
      return self._executeFileCatalogFunction( lfn, 'isFile', catalogs = catalogs )

  def getCatalogFileMetadata( self, lfn, singleFile = False, catalogs = [] ):
    """ Get the metadata associated to a file in the FileCatalog

        'lfn' is the files to check (can be a single file or list of lfns)
    """
    if singleFile:
      return self._executeSingleFileCatalogFunction( lfn, 'getFileMetadata', catalogs = catalogs )
    else:
      return self._executeFileCatalogFunction( lfn, 'getFileMetadata', catalogs = catalogs )

  def getCatalogFileSize( self, lfn, singleFile = False, catalogs = [] ):
    """ Get the size registered for files in the FileCatalog

        'lfn' is the files to check (can be a single lfn or list of lfns)
    """
    if singleFile:
      return self._executeSingleFileCatalogFunction( lfn, 'getFileSize', catalogs = catalogs )
    else:
      return self._executeFileCatalogFunction( lfn, 'getFileSize', catalogs = catalogs )

  def getCatalogReplicas( self, lfn, allStatus = False, singleFile = False, catalogs = [] ):
    """ Get the replicas registered for files in the FileCatalog

        'lfn' is the files to check (can be a single lfn or list of lfns)
    """
    if singleFile:
      return self._executeSingleFileCatalogFunction( lfn, 'getReplicas', argsDict = {'allStatus':allStatus}, catalogs = catalogs )
    else:
      return self._executeFileCatalogFunction( lfn, 'getReplicas', argsDict = {'allStatus':allStatus}, catalogs = catalogs )

  def getCatalogLFNForPFN( self, pfn, singleFile = False, catalogs = [] ):
    """ Get the LFNs registered with the supplied PFNs from the FileCatalog

        'pfn' is the files to obtain (can be a single pfn or list of pfns)
    """
    if singleFile:
      return self._executeSingleFileCatalogFunction( pfn, 'getLFNForPFN', catalogs = catalogs )
    else:
      return self._executeFileCatalogFunction( pfn, 'getLFNForPFN', catalogs = catalogs )

  def addCatalogFile( self, lfn, singleFile = False, catalogs = [] ):
    """ Add a new file to the FileCatalog

        'lfn' is the dictionary containing the file properties
    """
    if singleFile:
      return self._executeSingleFileCatalogFunction( lfn, 'addFile', catalogs = catalogs )
    else:
      return self._executeFileCatalogFunction( lfn, 'addFile', catalogs = catalogs )

  def removeCatalogFile( self, lfn, singleFile = False, catalogs = [] ):
    """ Remove a file from the FileCatalog

        'lfn' is the file to be removed
    """
    if singleFile:
      return self._executeSingleFileCatalogFunction( lfn, 'removeFile', catalogs = catalogs )
    else:
      return self._executeFileCatalogFunction( lfn, 'removeFile', catalogs = catalogs )

class CatalogReplica( CatalogBase ):

  def getCatalogReplicaStatus( self, lfn, singleFile = False, catalogs = [] ):
    """ Get the status of the replica as registered in the FileCatalog

        'lfn' is a dictionary containing {LFN:SE}
    """
    if singleFile:
      return self._executeSingleFileCatalogFunction( lfn, 'getReplicaStatus', catalogs = catalogs )
    else:
      return self._executeFileCatalogFunction( lfn, 'getReplicaStatus', catalogs = catalogs )

  def addCatalogReplica( self, lfn, singleFile = False, catalogs = [] ):
    """ Add a new replica to the FileCatalog

        'lfn' is the dictionary containing the replica properties
    """
    if singleFile:
      return self._executeSingleFileCatalogFunction( lfn, 'addReplica', catalogs = catalogs )
    else:
      return self._executeFileCatalogFunction( lfn, 'addReplica', catalogs = catalogs )

  def removeCatalogReplica( self, lfn, singleFile = False, catalogs = [] ):
    """ Remove a replica from the FileCatalog

         'lfn' is the file to be removed
    """
    if singleFile:
      return self._executeSingleFileCatalogFunction( lfn, 'removeReplica', catalogs = catalogs )
    else:
      return self._executeFileCatalogFunction( lfn, 'removeReplica', catalogs = catalogs )

  def setCatalogReplicaStatus( self, lfn, singleFile = False, catalogs = [] ):
    """ Change the status for a replica in the FileCatalog

        'lfn' is the replica information to change
    """
    if singleFile:
      return self._executeSingleFileCatalogFunction( lfn, 'setReplicaStatus', catalogs = catalogs )
    else:
      return self._executeFileCatalogFunction( lfn, 'setReplicaStatus', catalogs = catalogs )

  def setCatalogReplicaHost( self, lfn, singleFile = False, catalogs = [] ):
    """ Change the registered SE for a replica in the FileCatalog

        'lfn' is the replica information to change
    """
    if singleFile:
      return self._executeSingleFileCatalogFunction( lfn, 'setReplicaHost', catalogs = catalogs )
    else:
      return self._executeFileCatalogFunction( lfn, 'setReplicaHost', catalogs = catalogs )

class CatalogDirectory( CatalogBase ):

  def getCatalogIsDirectory( self, lfn, singleFile = False, catalogs = [] ):
    """ Determine whether the path is registered as a directory in the FileCatalog

        'lfn' is the files to check (can be a single file or list of lfns)
    """
    if singleFile:
      return self._executeSingleFileCatalogFunction( lfn, 'isDirectory', catalogs = catalogs )
    else:
      return self._executeFileCatalogFunction( lfn, 'isDirectory', catalogs = catalogs )

  def getCatalogDirectoryMetadata( self, lfn, singleFile = False, catalogs = [] ):
    """ Get the metadata associated to a directory in the FileCatalog

        'lfn' is the directories to check (can be a single directory or list of directories)
    """
    if singleFile:
      return self._executeSingleFileCatalogFunction( lfn, 'getDirectoryMetadata', catalogs = catalogs )
    else:
      return self._executeFileCatalogFunction( lfn, 'getDirectoryMetadata', catalogs = catalogs )

  def getCatalogDirectoryReplicas( self, lfn, singleFile = False, catalogs = [] ):
    """ Get the replicas for the contents of a directory in the FileCatalog

        'lfn' is the directories to check (can be a single directory or list of directories)
    """
    if singleFile:
      return self._executeSingleFileCatalogFunction( lfn, 'getDirectoryReplicas', catalogs = catalogs )
    else:
      return self._executeFileCatalogFunction( lfn, 'getDirectoryReplicas', catalogs = catalogs )

  def getCatalogListDirectory( self, lfn, verbose = False, singleFile = False, catalogs = [] ):
    """ Get the contents of a directory in the FileCatalog

        'lfn' is the directories to check (can be a single directory or list of directories)
    """
    if singleFile:
      return self._executeSingleFileCatalogFunction( lfn, 'listDirectory', argsDict = {'verbose':verbose}, catalogs = catalogs )
    else:
      return self._executeFileCatalogFunction( lfn, 'listDirectory', argsDict = {'verbose':verbose}, catalogs = catalogs )

  def getCatalogDirectorySize( self, lfn, singleFile = False, catalogs = [] ):
    """ Get the size a directory in the FileCatalog

        'lfn' is the directories to check (can be a single directory or list of directories)
    """
    if singleFile:
      return self._executeSingleFileCatalogFunction( lfn, 'getDirectorySize', catalogs = catalogs )
    else:
      return self._executeFileCatalogFunction( lfn, 'getDirectorySize', catalogs = catalogs )

  def createCatalogDirectory( self, lfn, singleFile = False, catalogs = [] ):
    """ Create the directory supplied in the FileCatalog

        'lfn' is the directory to create
    """
    if singleFile:
      return self._executeSingleFileCatalogFunction( lfn, 'createDirectory', catalogs = catalogs )
    else:
      return self._executeFileCatalogFunction( lfn, 'createDirectory', catalogs = catalogs )

  def removeCatalogDirectory( self, lfn, recursive = False, singleFile = False, catalogs = [] ):
    """ Remove the directory supplied from the FileCatalog

        'lfn' is the directory to remove
    """
    if singleFile:
      return self._executeSingleFileCatalogFunction( lfn, 'removeDirectory', argsDict = {'recursive':recursive}, catalogs = catalogs )
    else:
      return self._executeFileCatalogFunction( lfn, 'removeDirectory', argsDict = {'recursive':recursive}, catalogs = catalogs )

class CatalogLink( CatalogBase ):

  def getCatalogIsLink( self, lfn, singleFile = False, catalogs = [] ):
    """ Determine whether the path is registered as a link in the FileCatalog

        'lfn' is the paths to check (can be a single path or list of paths)
    """
    if singleFile:
      return self._executeSingleFileCatalogFunction( lfn, 'isLink', catalogs = catalogs )
    else:
      return self._executeFileCatalogFunction( lfn, 'isLink', catalogs = catalogs )

  def getCatalogReadLink( self, lfn, singleFile = False, catalogs = [] ):
    """ Get the target of a link as registered in the FileCatalog

        'lfn' is the links to check (can be a single link or list of links)
    """
    if singleFile:
      return self._executeSingleFileCatalogFunction( lfn, 'readLink', catalogs = catalogs )
    else:
      return self._executeFileCatalogFunction( lfn, 'readLink', catalogs = catalogs )

  def createCatalogLink( self, lfn, singleFile = False, catalogs = [] ):
    """ Create the link supplied in the FileCatalog

        'lfn' is the link dictionary containing the target lfn and link name to create
    """
    if singleFile:
      return self._executeSingleFileCatalogFunction( lfn, 'createLink', catalogs = catalogs )
    else:
      return self._executeFileCatalogFunction( lfn, 'createLink', catalogs = catalogs )

  def removeCatalogLink( self, lfn, singleFile = False, catalogs = [] ):
    """ Remove the link supplied from the FileCatalog

        'lfn' is the link to remove
    """
    if singleFile:
      return self._executeSingleFileCatalogFunction( lfn, 'removeLink', catalogs = catalogs )
    else:
      return self._executeFileCatalogFunction( lfn, 'removeLink', catalogs = catalogs )

class CatalogInterface( CatalogFile, CatalogReplica, CatalogDirectory, CatalogLink ):
  """ Dummy class to expose all the methods of the CatalogInterface
  """
  pass

class StorageBase:

  def __init__( self ):
    """ This class stores the two wrapper functions for interacting with the StorageElement:

        _executeStorageElementFunction(storageElementName,pfn,method,argsDict={})
          Is a wrapper around the available StorageElement() functions.
          The 'storageElementName', 'pfn' and 'method' arguments must be provided:
            'storageElementName' is the DIRAC SE name to be accessed e.g. CERN-DST.
            'pfn' contains a single pfn string or a list or dictionary containing the required files.
            'method' is the name of the StorageElement() method to be invoked.
          'argsDict' contains additional arguments that are required for the method.

        _executeSingleStorageElementFunction(storageElementName,pfn,method,argsDict={})
          Is a wrapper around _executeStorageElementFunction().
          It parses the output of _executeStorageElementFunction() for the first pfn provided as input.
          If this pfn is found in:
            res['Value']['Successful'] an S_OK() is returned with the value.
            res['Value']['Failed'] an S_ERROR() is returned with the error message.
    """
    pass

  def _executeSingleStorageElementFunction( self, storageElementName, pfn, method, argsDict = {} ):
    res = self._executeStorageElementFunction( storageElementName, pfn, method, argsDict )
    if type( pfn ) == types.ListType:
      pfn = pfn[0]
    elif type( pfn ) == types.DictType:
      pfn = pfn.keys()[0]
    if not res['OK']:
      return res
    elif res['Value']['Failed'].has_key( pfn ):
      errorMessage = res['Value']['Failed'][pfn]
      return S_ERROR( errorMessage )
    else:
      return S_OK( res['Value']['Successful'][pfn] )

  def _executeStorageElementFunction( self, storageElementName, pfn, method, argsDict = {} ):
    """ A simple wrapper around the storage element functionality
    """
    # First check the supplied pfn(s) are the correct format.
    if type( pfn ) in types.StringTypes:
      pfns = {pfn:False}
    elif type( pfn ) == types.ListType:
      pfns = {}
      for url in pfn:
        pfns[url] = False
    elif type( pfn ) == types.DictType:
      pfns = pfn.copy()
    else:
      errStr = "ReplicaManager._executeStorageElementFunction: Supplied pfns must be string or list of strings or a dictionary."
      gLogger.error( errStr )
      return S_ERROR( errStr )
    # Check we have some pfns
    if not pfns:
      errMessage = "ReplicaManager._executeStorageElementFunction: No pfns supplied."
      gLogger.error( errMessage )
      return S_ERROR( errMessage )
    gLogger.debug( "ReplicaManager._executeStorageElementFunction: Attempting to perform '%s' operation with %s pfns." % ( method, len( pfns ) ) )
    # Check we can instantiate the storage element correctly
    overwride = False
    if method  in ['removeFile', 'removeDirectory']:
      overwride = True
    storageElement = StorageElement( storageElementName, overwride = overwride )
    res = storageElement.isValid( method )
    if not res['OK']:
      errStr = "ReplicaManager._executeStorageElementFunction: Failed to instantiate Storage Element"
      gLogger.error( errStr, "for performing %s at %s." % ( method, storageElementName ) )
      return res
    # Generate the execution string
    if argsDict:
      execString = "res = storageElement.%s(pfns" % method
      for argument, value in argsDict.items():
        if type( value ) == types.StringType:
          execString = "%s, %s='%s'" % ( execString, argument, value )
        else:
          execString = "%s, %s=%s" % ( execString, argument, value )
      execString = "%s)" % execString
    else:
      execString = "res = storageElement.%s(pfns)" % method
    # Execute the execute string
    try:
      exec( execString )
    except AttributeError, errMessage:
      exceptStr = "ReplicaManager._executeStorageElementFunction: Exception while perfoming %s." % method
      gLogger.exception( exceptStr, str( errMessage ) )
      return S_ERROR( exceptStr )
    # Return the output
    if not res['OK']:
      errStr = "ReplicaManager._executeStorageElementFunction: Completely failed to perform %s." % method
      gLogger.error( errStr, '%s : %s' % ( storageElementName, res['Message'] ) )
    return res

  def getPfnForLfn( self, lfns, storageElementName ):
    storageElement = StorageElement( storageElementName )
    res = storageElement.isValid( 'getPfnForLfn' )
    if not res['OK']:
      errStr = "ReplicaManager.getPfnForLfn: Failed to instantiate Storage Element"
      gLogger.error( errStr, "for performing getPfnForLfn at %s." % ( storageElementName ) )
      return res
    successful = {}
    failed = {}
    for lfn in lfns:
      res = storageElement.getPfnForLfn( lfn )
      if res['OK']:
        successful[lfn] = res['Value']
      else:
        failed[lfn] = res['Message']
    resDict = {'Successful':successful, 'Failed':failed}
    return S_OK( resDict )

  def getLfnForPfn( self, pfns, storageElementName ):
    storageElement = StorageElement( storageElementName )
    res = storageElement.isValid( 'getPfnPath' )
    if not res['OK']:
      errStr = "ReplicaManager.getLfnForPfn: Failed to instantiate Storage Element"
      gLogger.error( errStr, "for performing getPfnPath at %s." % ( storageElementName ) )
      return res
    successful = {}
    failed = {}
    for pfn in pfns:
      res = storageElement.getPfnPath( pfn )
      if res['OK']:
        successful[pfn] = res['Value']
      else:
        failed[pfn] = res['Message']
    resDict = {'Successful':successful, 'Failed':failed}
    return S_OK( resDict )

  def getPfnForProtocol( self, pfns, storageElementName, protocol = 'SRM2', withPort = True ):
    storageElement = StorageElement( storageElementName )
    res = storageElement.isValid( 'getPfnForProtocol' )
    if not res['OK']:
      errStr = "ReplicaManager.getPfnForLfn: Failed to instantiate Storage Element"
      gLogger.error( errStr, "for performing getPfnForProtocol at %s." % ( storageElementName ) )
      return res
    successful = {}
    failed = {}
    for pfn in pfns:
      res = storageElement.getPfnForProtocol( pfn, protocol, withPort = withPort )
      if res['OK']:
        successful[pfn] = res['Value']
      else:
        failed[pfn] = res['Message']
    resDict = {'Successful':successful, 'Failed':failed}
    return S_OK( resDict )

class StorageFile( StorageBase ):

  ##########################################################################
  #
  # These are the storage element wrapper functions available for physical files
  #

  def getStorageFileExists( self, physicalFile, storageElementName, singleFile = False ):
    """ Determine the existance of the physical files

        'physicalFile' is the pfn(s) to be checked
        'storageElementName' is the Storage Element
    """
    if singleFile:
      return self._executeSingleStorageElementFunction( storageElementName, physicalFile, 'exists' )
    else:
      return self._executeStorageElementFunction( storageElementName, physicalFile, 'exists' )

  def getStorageFileIsFile( self, physicalFile, storageElementName, singleFile = False ):
    """ Determine the physical paths are files

        'physicalFile' is the pfn(s) to be checked
        'storageElementName' is the Storage Element
    """
    if singleFile:
      return self._executeSingleStorageElementFunction( storageElementName, physicalFile, 'isFile' )
    else:
      return self._executeStorageElementFunction( storageElementName, physicalFile, 'isFile' )

  def getStorageFileSize( self, physicalFile, storageElementName, singleFile = False ):
    """ Obtain the size of the physical files

        'physicalFile' is the pfn(s) size to be obtained
        'storageElementName' is the Storage Element
    """
    if singleFile:
      return self._executeSingleStorageElementFunction( storageElementName, physicalFile, 'getFileSize' )
    else:
      return self._executeStorageElementFunction( storageElementName, physicalFile, 'getFileSize' )

  def getStorageFileAccessUrl( self, physicalFile, storageElementName, protocol = [], singleFile = False ):
    """ Obtain the access url for a physical file

        'physicalFile' is the pfn(s) to access
        'storageElementName' is the Storage Element
    """
    if singleFile:
      return self._executeSingleStorageElementFunction( storageElementName, physicalFile, 'getAccessUrl', argsDict = {'protocol':protocol} )
    else:
      return self._executeStorageElementFunction( storageElementName, physicalFile, 'getAccessUrl', argsDict = {'protocol':protocol} )

  def getStorageFileMetadata( self, physicalFile, storageElementName, singleFile = False ):
    """ Obtain the metadata for physical files

        'physicalFile' is the pfn(s) to be checked
        'storageElementName' is the Storage Element to check
    """
    if singleFile:
      return self._executeSingleStorageElementFunction( storageElementName, physicalFile, 'getFileMetadata' )
    else:
      return self._executeStorageElementFunction( storageElementName, physicalFile, 'getFileMetadata' )

  def removeStorageFile( self, physicalFile, storageElementName, singleFile = False ):
    """ Remove physical files

       'physicalFile' is the pfn(s) to be removed
       'storageElementName' is the Storage Element
    """
    if singleFile:
      return self._executeSingleStorageElementFunction( storageElementName, physicalFile, 'removeFile' )
    else:
      return self._executeStorageElementFunction( storageElementName, physicalFile, 'removeFile' )

  def prestageStorageFile( self, physicalFile, storageElementName, lifetime = 60 * 60 * 24, singleFile = False ):
    """ Prestage physical files

        'physicalFile' is the pfn(s) to be pre-staged
        'storageElementName' is the Storage Element
    """
    if singleFile:
      return self._executeSingleStorageElementFunction( storageElementName, physicalFile, 'prestageFile', {'lifetime':lifetime} )
    else:
      return self._executeStorageElementFunction( storageElementName, physicalFile, 'prestageFile', {'lifetime':lifetime} )

  def getPrestageStorageFileStatus( self, physicalFile, storageElementName, singleFile = False ):
    """ Obtain the status of a pre-stage request

        'physicalFile' is the pfn(s) to obtain the status
        'storageElementName' is the Storage Element
    """
    if singleFile:
      return self._executeSingleStorageElementFunction( storageElementName, physicalFile, 'prestageFileStatus' )
    else:
      return self._executeStorageElementFunction( storageElementName, physicalFile, 'prestageFileStatus' )

  def pinStorageFile( self, physicalFile, storageElementName, lifetime = 60 * 60 * 24, singleFile = False ):
    """ Pin physical files with a given lifetime

        'physicalFile' is the pfn(s) to pin
        'storageElementName' is the Storage Element
    """
    if singleFile:
      return self._executeSingleStorageElementFunction( storageElementName, physicalFile, 'pinFile', {'lifetime':lifetime} )
    else:
      return self._executeStorageElementFunction( storageElementName, physicalFile, 'pinFile', {'lifetime':lifetime} )

  def releaseStorageFile( self, physicalFile, storageElementName, singleFile = False ):
    """ Release the pin on physical files

        'physicalFile' is the pfn(s) to release
        'storageElementName' is the Storage Element
    """
    if singleFile:
      return self._executeSingleStorageElementFunction( storageElementName, physicalFile, 'releaseFile' )
    else:
      return self._executeStorageElementFunction( storageElementName, physicalFile, 'releaseFile' )

  def getStorageFile( self, physicalFile, storageElementName, localPath = False, singleFile = False ):
    """ Get a local copy of a physical file

        'physicalFile' is the pfn(s) to get
        'storageElementName' is the Storage Element
    """
    if singleFile:
      return self._executeSingleStorageElementFunction( storageElementName, physicalFile, 'getFile', argsDict = {'localPath':localPath} )
    else:
      return self._executeStorageElementFunction( storageElementName, physicalFile, 'getFile', argsDict = {'localPath':localPath} )

  def putStorageFile( self, physicalFile, storageElementName, singleFile = False ):
    """ Put a local file to the storage element

        'physicalFile' is the pfn(s) dict to put
        'storageElementName' is the StorageElement
    """
    if singleFile:
      return self._executeSingleStorageElementFunction( storageElementName, physicalFile, 'putFile' )
    else:
      return self._executeStorageElementFunction( storageElementName, physicalFile, 'putFile' )

  def replicateStorageFile( self, physicalFile, size, storageElementName, singleFile = False ):
    """ Replicate a physical file to a storage element

        'physicalFile' is the pfn(s) dict to replicate
        'storageElementName' is the target StorageElement
    """
    if singleFile:
      return self._executeSingleStorageElementFunction( storageElementName, physicalFile, 'replicateFile', argsDict = {'sourceSize':size} )
    else:
      return self._executeStorageElementFunction( storageElementName, physicalFile, 'replicateFile', argsDict = {'sourceSize':size} )

class StorageDirectory( StorageBase ):

  ##########################################################################
  #
  # These are the storage element wrapper functions available for directories
  #

  def getStorageDirectoryIsDirectory( self, storageDirectory, storageElementName, singleDirectory = False ):
    """ Determine the storage paths are directories

        'storageDirectory' is the pfn(s) to be checked
        'storageElementName' is the Storage Element
    """
    if singleDirectory:
      return self._executeSingleStorageElementFunction( storageElementName, storageDirectory, 'isDirectory' )
    else:
      return self._executeStorageElementFunction( storageElementName, storageDirectory, 'isDirectory' )

  def getStorageDirectoryMetadata( self, storageDirectory, storageElementName, singleDirectory = False ):
    """ Obtain the metadata for storage directories

        'storageDirectory' is the pfn(s) to be checked
        'storageElementName' is the Storage Element to check
    """
    if singleDirectory:
      return self._executeSingleStorageElementFunction( storageElementName, storageDirectory, 'getDirectoryMetadata' )
    else:
      return self._executeStorageElementFunction( storageElementName, storageDirectory, 'getDirectoryMetadata' )

  def getStorageDirectorySize( self, storageDirectory, storageElementName, singleDirectory = False ):
    """ Obtain the size of the storage directories

        'storageDirectory' is the pfn(s) size to be obtained
        'storageElementName' is the Storage Element
    """
    if singleDirectory:
      return self._executeSingleStorageElementFunction( storageElementName, storageDirectory, 'getDirectorySize' )
    else:
      return self._executeStorageElementFunction( storageElementName, storageDirectory, 'getDirectorySize' )

  def getStorageListDirectory( self, storageDirectory, storageElementName, singleDirectory = False ):
    """  List the contents of a directory in the Storage Element

        'storageDirectory' is the pfn(s) directory to be obtained
        'storageElementName' is the Storage Element
    """
    if singleDirectory:
      return self._executeSingleStorageElementFunction( storageElementName, storageDirectory, 'listDirectory' )
    else:
      return self._executeStorageElementFunction( storageElementName, storageDirectory, 'listDirectory' )

  def getStorageDirectory( self, storageDirectory, storageElementName, localPath = False, singleDirectory = False ):
    """  Get locally the contents of a directory from the Storage Element

        'storageDirectory' is the pfn(s) directory to be obtained
        'storageElementName' is the Storage Element
    """
    if singleDirectory:
      return self._executeSingleStorageElementFunction( storageElementName, storageDirectory, 'getDirectory', argsDict = {'localPath':localPath} )
    else:
      return self._executeStorageElementFunction( storageElementName, storageDirectory, 'getDirectory', argsDict = {'localPath':localPath} )

  def putStorageDirectory( self, storageDirectory, storageElementName, singleDirectory = False ):
    """ Put a local directory to the storage element

        'storageDirectory' is the pfn(s) directory to be put
        'storageElementName' is the Storage Element
    """
    if singleDirectory:
      return self._executeSingleStorageElementFunction( storageElementName, storageDirectory, 'putDirectory' )
    else:
      return self._executeStorageElementFunction( storageElementName, storageDirectory, 'putDirectory' )

  def removeStorageDirectory( self, storageDirectory, storageElementName, recursive = False, singleDirectory = False ):
    """ Revove a directory from the storage element

        'storageDirectory' is the pfn(s) directory to be removed
        'storageElementName' is the Storage Element
    """
    if singleDirectory:
      return self._executeSingleStorageElementFunction( storageElementName, storageDirectory, 'removeDirectory', argsDict = {'recursive':recursive} )
    else:
      return self._executeStorageElementFunction( storageElementName, storageDirectory, 'removeDirectory', argsDict = {'recursive':recursive} )

class StorageInterface( StorageFile, StorageDirectory ):
  """ Dummy class to expose all the methods of the StorageInterface
  """
  pass

class CatalogToStorage( CatalogInterface, StorageInterface ):

  ##########################################################################
  #
  # These are the wrapper functions for doing simple replica->SE operations
  #

  def getReplicaIsFile( self, lfn, storageElementName, singleFile = False ):
    """ Determine whether the supplied lfns are files at the supplied StorageElement

        'lfn' is the file(s) to check
        'storageElementName' is the target Storage Element
    """
    if singleFile:
      return self.__executeSingleReplicaStorageElementOperation( storageElementName, lfn, 'isFile' )
    else:
      return self.__executeReplicaStorageElementOperation( storageElementName, lfn, 'isFile' )

  def getReplicaSize( self, lfn, storageElementName, singleFile = False ):
    """ Obtain the file size for the lfns at the supplied StorageElement

        'lfn' is the file(s) for which to get the size
        'storageElementName' is the target Storage Element
    """
    if singleFile:
      return self.__executeSingleReplicaStorageElementOperation( storageElementName, lfn, 'getFileSize' )
    else:
      return self.__executeReplicaStorageElementOperation( storageElementName, lfn, 'getFileSize' )

  def getReplicaAccessUrl( self, lfn, storageElementName, singleFile = False ):
    """ Obtain the access url for lfns at the supplied StorageElement

        'lfn' is the file(s) for which to obtain access URLs
        'storageElementName' is the target Storage Element
    """
    if singleFile:
      return self.__executeSingleReplicaStorageElementOperation( storageElementName, lfn, 'getAccessUrl' )
    else:
      return self.__executeReplicaStorageElementOperation( storageElementName, lfn, 'getAccessUrl' )

  def getReplicaMetadata( self, lfn, storageElementName, singleFile = False ):
    """ Obtain the file metadata for lfns at the supplied StorageElement

        'lfn' is the file(s) for which to get metadata
        'storageElementName' is the target Storage Element
    """
    if singleFile:
      return self.__executeSingleReplicaStorageElementOperation( storageElementName, lfn, 'getFileMetadata' )
    else:
      return self.__executeReplicaStorageElementOperation( storageElementName, lfn, 'getFileMetadata' )

  def prestageReplica( self, lfn, storageElementName, lifetime = 60 * 60 * 24, singleFile = False ):
    """ Issue prestage requests for the lfns at the supplied StorageElement

        'lfn' is the file(s) for which to issue prestage requests
        'storageElementName' is the target Storage Element
    """
    if singleFile:
      return self.__executeSingleReplicaStorageElementOperation( storageElementName, lfn, 'prestageFile', {'lifetime':lifetime} )
    else:
      return self.__executeReplicaStorageElementOperation( storageElementName, lfn, 'prestageFile', {'lifetime':lifetime} )

  def getPrestageReplicaStatus( self, lfn, storageElementName, singleFile = False ):
    """ This functionality is not supported.
    """
    return S_ERROR( "This functionality is not supported. Please use getReplicaMetadata and check the 'Cached' element." )

  def pinReplica( self, lfn, storageElementName, lifetime = 60 * 60 * 24, singleFile = False ):
    """ Issue a pin for the lfns at the supplied StorageElement

        'lfn' is the file(s) for which to issue pins
        'storageElementName' is the target Storage Element
        'lifetime' is the pin lifetime (default 1 day)
    """
    if singleFile:
      return self.__executeSingleReplicaStorageElementOperation( storageElementName, lfn, 'pinFile', {'lifetime':lifetime} )
    else:
      return self.__executeReplicaStorageElementOperation( storageElementName, lfn, 'pinFile', {'lifetime':lifetime} )

  def releaseReplica( self, lfn, storageElementName, singleFile = False ):
    """ Release pins for the lfns at the supplied StorageElement

        'lfn' is the file(s) for which to release pins
        'storageElementName' is the target Storage Element
    """
    if singleFile:
      return self.__executeSingleReplicaStorageElementOperation( storageElementName, lfn, 'releaseFile' )
    else:
      return self.__executeReplicaStorageElementOperation( storageElementName, lfn, 'releaseFile' )

  def getReplica( self, lfn, storageElementName, localPath = False, singleFile = False ):
    """ Get the lfns to the local disk from the supplied StorageElement

        'lfn' is the file(s) for which to release pins
        'storageElementName' is the target Storage Element
        'localPath' is the local target path (default '.')
    """
    if singleFile:
      return self.__executeSingleReplicaStorageElementOperation( storageElementName, lfn, 'getFile', {'localPath':localPath} )
    else:
      return self.__executeReplicaStorageElementOperation( storageElementName, lfn, 'getFile', {'localPath':localPath} )

  def __executeSingleReplicaStorageElementOperation( self, storageElementName, lfn, method, argsDict = {} ):
    res = self.__executeReplicaStorageElementOperation( storageElementName, lfn, method, argsDict )
    if type( lfn ) == types.ListType:
      lfn = lfn[0]
    elif type( lfn ) == types.DictType:
      lfn = lfn.keys()[0]
    if not res['OK']:
      return res
    elif res['Value']['Failed'].has_key( lfn ):
      errorMessage = res['Value']['Failed'][lfn]
      return S_ERROR( errorMessage )
    else:
      return S_OK( res['Value']['Successful'][lfn] )

  def __executeReplicaStorageElementOperation( self, storageElementName, lfn, method, argsDict = {} ):
    """ A simple wrapper that allows replica querying then perform the StorageElement operation
    """
    res = self._executeFileCatalogFunction( lfn, 'getReplicas' )
    if not res['OK']:
      errStr = "ReplicaManager.__executeReplicaStorageElementOperation: Completely failed to get replicas for LFNs."
      gLogger.error( errStr, res['Message'] )
      return res
    failed = res['Value']['Failed']
    for lfn, reason in res['Value']['Failed'].items():
      gLogger.error( "ReplicaManager.__executeReplicaStorageElementOperation: Failed to get replicas for file.", "%s %s" % ( lfn, reason ) )
    lfnReplicas = res['Value']['Successful']
    pfnDict = {}
    for lfn, replicas in lfnReplicas.items():
      if replicas.has_key( storageElementName ):
        pfnDict[replicas[storageElementName]] = lfn
      else:
        errStr = "ReplicaManager.__executeReplicaStorageElementOperation: File does not have replica at supplied Storage Element."
        gLogger.error( errStr, "%s %s" % ( lfn, storageElementName ) )
        failed[lfn] = errStr
    res = self._executeStorageElementFunction( storageElementName, pfnDict.keys(), method, argsDict )
    if not res['OK']:
      gLogger.error( "ReplicaManager.__executeReplicaStorageElementOperation: Failed to execute %s StorageElement operation." % method, res['Message'] )
      return res
    successful = {}
    for pfn, pfnRes in res['Value']['Successful'].items():
      successful[pfnDict[pfn]] = pfnRes
    for pfn, errorMessage in res['Value']['Failed'].items():
      failed[pfnDict[pfn]] = errorMessage
    resDict = {'Successful':successful, 'Failed':failed}
    return S_OK( resDict )

class ReplicaManager( CatalogToStorage ):

  def __init__( self ):
    """ Constructor function.
    """
    self.fileCatalogue = FileCatalog()
    self.accountingClient = None
    self.registrationProtocol = ['SRM2', 'DIP']
    self.thirdPartyProtocols = ['SRM2', 'DIP']

  def setAccountingClient( self, client ):
    """ Set Accounting Client instance
    """
    self.accountingClient = client

  def __verifyOperationPermission( self, path ):
    """  Check if we have write permission to the given directory
    """

    fc = FileCatalog()
    res = fc.getPathPermissions( path )
    if not res['OK']:
      return res

    paths = path
    if type(path) in types.StringTypes:
      paths = [path]

    for p in paths:
      if not res['Value']['Successful'].has_key( p ):
        return S_OK(False)
      catalogPerm = res['Value']['Successful'][p]
      if not ( catalogPerm.has_key( 'Write' ) and catalogPerm['Write'] ):
        return S_OK(False)

    return S_OK(True)

  ##########################################################################
  #
  # These are the bulk removal methods
  #

  def cleanLogicalDirectory( self, lfnDir ):
    """ Clean the logical directory from the catalog and storage
    """
    if type( lfnDir ) in types.StringTypes:
      lfnDir = [lfnDir]
    successful = {}
    failed = {}
    for dir in lfnDir:
      res = self.__cleanDirectory( dir )
      if not res['OK']:
        gLogger.error( "Failed to clean directory.", "%s %s" % ( dir, res['Message'] ) )
        failed[dir] = res['Message']
      else:
        gLogger.info( "Successfully removed directory.", dir )
        successful[dir] = res['Value']
    resDict = {'Successful':successful, 'Failed':failed}
    return S_OK( resDict )

  def __cleanDirectory( self, dir ):
    res = self.__verifyOperationPermission( dir )
    if not res['OK']:
      return res
    if not res['Value']:
      errStr = "ReplicaManager.__cleanDirectory: Write access not permitted for this credential."
      gLogger.error( errStr, dir )
      return S_ERROR( errStr )
    res = self.__getCatalogDirectoryContents( [dir] )
    if not res['OK']:
      return res
    replicaDict = {}
    for lfn, lfnDict in res['Value'].items():
      lfnReplicas = lfnDict['Replicas']
      replicaDict[lfn] = {}
      for se, seDict in lfnReplicas.items():
        replicaDict[lfn][se] = seDict['PFN']
    if replicaDict:
      gLogger.info( "Attempting to remove %d files from the catalog and storage" % len( replicaDict ) )
      res = self.__removeFile( replicaDict )
      if not res['OK']:
        return res
      for lfn, reason in res['Value']['Failed'].items():
        gLogger.error( "Failed to remove file found in the catalog", "%s %s" % ( lfn, reason ) )
      if res['Value']['Failed']:
        return S_ERROR( "Failed to remove all files found in the catalog" )
    storageElements = gConfig.getValue( 'Resources/StorageElementGroups/SE_Cleaning_List', [] )
    failed = False
    for storageElement in sortList( storageElements ):
      res = self.__removeStorageDirectory( dir, storageElement )
      if not res['OK']:
        failed = True
    if failed:
      return S_ERROR( "Failed to clean storage directory at all SEs" )
    res = self.removeCatalogDirectory( dir, recursive = True, singleFile = True )
    if not res['OK']:
      return res
    return S_OK()

  def __removeStorageDirectory( self, directory, storageElement ):
    gLogger.info( 'Removing the contents of %s at %s' % ( directory, storageElement ) )
    res = self.getPfnForLfn( [directory], storageElement )
    if not res['OK']:
      gLogger.error( "Failed to get PFN for directory", res['Message'] )
      return res
    for directory, error in res['Value']['Failed'].items():
      gLogger.error( 'Failed to obtain directory PFN from LFN', '%s %s' % ( directory, error ) )
    if res['Value']['Failed']:
      return S_ERROR( 'Failed to obtain directory PFN from LFNs' )
    storageDirectory = res['Value']['Successful'].values()[0]
    res = self.getStorageFileExists( storageDirectory, storageElement, singleFile = True )
    if not res['OK']:
      gLogger.error( "Failed to obtain existance of directory", res['Message'] )
      return res
    exists = res['Value']
    if not exists:
      gLogger.info( "The directory %s does not exist at %s " % ( directory, storageElement ) )
      return S_OK()
    res = self.removeStorageDirectory( storageDirectory, storageElement, recursive = True, singleDirectory = True )
    if not res['OK']:
      gLogger.error( "Failed to remove storage directory", res['Message'] )
      return res
    gLogger.info( "Successfully removed %d files from %s at %s" % ( res['Value']['FilesRemoved'], directory, storageElement ) )
    return S_OK()

  def __getCatalogDirectoryContents( self, directories ):
    gLogger.info( 'Obtaining the catalog contents for %d directories:' % len( directories ) )
    for directory in directories:
      gLogger.info( directory )
    activeDirs = directories
    allFiles = {}
    while len( activeDirs ) > 0:
      currentDir = activeDirs[0]
      res = self.getCatalogListDirectory( currentDir, singleFile = True )
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
    return S_OK( allFiles )

  def getReplicasFromDirectory( self, directory ):
    if type( directory ) in types.StringTypes:
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
    if type( directory ) in types.StringTypes:
      directories = [directory]
    else:
      directories = directory
    gLogger.info( "Obtaining the files older than %d days in %d directories:" % ( days, len( directories ) ) )
    for directory in directories:
      gLogger.info( directory )
    activeDirs = directories
    allFiles = []
    while len( activeDirs ) > 0:
      currentDir = activeDirs[0]
      res = self.getCatalogListDirectory( currentDir, True, singleFile = True )
      activeDirs.remove( currentDir )
      if not res['OK']:
        gLogger.error( "Error retrieving directory contents", "%s %s" % ( currentDir, res['Message'] ) )
      else:
        dirContents = res['Value']
        subdirs = dirContents['SubDirs']
        for subdir, metadata in subdirs.items():
          if ( not days ) or self.__isOlderThan( metadata['CreationDate'], days ):
            if subdir[0] != '/':
              subdir = currentDir + '/' + subdir
            activeDirs.append( subdir )
        for filename, fileInfo in dirContents['Files'].items():
          if fileInfo.has_key( 'MetaData' ):
            fileInfo = fileInfo['MetaData']
          if ( not days ) or self.__isOlderThan( fileInfo['CreationDate'], days ):
            if fnmatch.fnmatch( filename, wildcard ):
              if fileInfo.has_key( 'LFN' ):
                filename = fileInfo['LFN']
              allFiles.append( filename )
        files = dirContents['Files'].keys()
        gLogger.info( "%s: %d files, %d sub-directories" % ( currentDir, len( files ), len( subdirs ) ) )
    return S_OK( allFiles )

  def __isOlderThan( self, stringTime, days ):
    timeDelta = timedelta( days = days )
    maxCTime = datetime.utcnow() - timeDelta
    #st = time.strptime( stringTime, "%a %b %d %H:%M:%S %Y" )
    #cTimeStruct = datetime( st[0], st[1], st[2], st[3], st[4], st[5], st[6], None )
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
    if type( lfn ) == types.ListType:
      lfns = lfn
    elif type( lfn ) == types.StringType:
      lfns = [lfn]
    else:
      errStr = "ReplicaManager.getFile: Supplied lfn must be string or list of strings."
      gLogger.error( errStr )
      return S_ERROR( errStr )
    gLogger.verbose( "ReplicaManager.getFile: Attempting to get %s files." % len( lfns ) )
    res = self.getActiveReplicas( lfns )
    if not res['OK']:
      return res
    failed = res['Value']['Failed']
    lfnReplicas = res['Value']['Successful']
    res = self.getCatalogFileMetadata( lfnReplicas.keys() )
    if not res['OK']:
      return res
    failed.update( res['Value']['Failed'] )
    fileMetadata = res['Value']['Successful']
    successful = {}
    for lfn in fileMetadata.keys():
      res = self.__getFile( lfn, lfnReplicas[lfn], fileMetadata[lfn], destinationDir )
      if not res['OK']:
        failed[lfn] = res['Message']
      else:
        successful[lfn] = res['Value']
    resDict = {'Successful':successful, 'Failed':failed}
    return S_OK( resDict )

  def __getFile( self, lfn, replicas, metadata, destinationDir ):
    if not replicas:
      gLogger.error( "No accessible replicas found" )
      return S_ERROR( "No accessible replicas found" )
    # Determine the best replicas
    res = self._getSEProximity( replicas.keys() )
    if not res['OK']:
      return res
    for storageElementName in res['Value']:
      physicalFile = replicas[storageElementName]
      res = self.getStorageFile( physicalFile, storageElementName, localPath = os.path.realpath( destinationDir ), singleFile = True )
      if not res['OK']:
        gLogger.error( "Failed to get %s from %s" % ( lfn, storageElementName ), res['Message'] )
      else:
        if not destinationDir:
          destinationDir = '.'
        localFile = os.path.realpath( "%s/%s" % ( destinationDir, os.path.basename( lfn ) ) )
        localAdler = fileAdler( localFile )
        if ( metadata['Size'] != res['Value'] ):
          gLogger.error( "Size of downloaded file (%d) does not match catalog (%d)" % ( res['Value'], metadata['Size'] ) )
        elif ( metadata['Checksum'] ) and ( not compareAdler( metadata['Checksum'], localAdler ) ):
          gLogger.error( "Checksum of downloaded file (%s) does not match catalog (%s)" % ( localAdler, metadata['Checksum'] ) )
        else:
          return S_OK( localFile )
    gLogger.error( "ReplicaManager.getFile: Failed to get local copy from any replicas.", lfn )
    return S_ERROR( "ReplicaManager.getFile: Failed to get local copy from any replicas." )

  def _getSEProximity( self, ses ):
    siteName = DIRAC.siteName()
    localSEs = getSEsForSite( siteName )['Value']
    countrySEs = []
    countryCode = siteName.split( '.' )[-1]
    res = getSEsForCountry( countryCode )
    if res['OK']:
      countrySEs = res['Value']
    sortedSEs = [se for se in localSEs if se in ses]
    for se in randomize( ses ):
      if ( se in countrySEs ) and ( not se in sortedSEs ):
        sortedSEs.append( se )
    for se in randomize( ses ):
      if not se in sortedSEs:
        sortedSEs.append( se )
    return S_OK( sortedSEs )

  def putAndRegister( self, lfn, file, diracSE, guid = None, path = None, checksum = None, catalog = None, ancestors = [] ):
    """ Put a local file to a Storage Element and register in the File Catalogues

        'lfn' is the file LFN
        'file' is the full path to the local file
        'diracSE' is the Storage Element to which to put the file
        'guid' is the guid with which the file is to be registered (if not provided will be generated)
        'path' is the path on the storage where the file will be put (if not provided the LFN will be used)
    """
    res = self.__verifyOperationPermission( os.path.dirname( lfn ) )
    if not res['OK']:
      return res
    if not res['Value']:
      errStr = "ReplicaManager.putAndRegister: Write access not permitted for this credential."
      gLogger.error( errStr, lfn )
      return S_ERROR( errStr )
    # Instantiate the desired file catalog
    if catalog:
      self.fileCatalogue = FileCatalog( catalog )
    else:
      self.fileCatalogue = FileCatalog()
    # Check that the local file exists
    if not os.path.exists( file ):
      errStr = "ReplicaManager.putAndRegister: Supplied file does not exist."
      gLogger.error( errStr, file )
      return S_ERROR( errStr )
    # If the path is not provided then use the LFN path
    if not path:
      path = os.path.dirname( lfn )
    # Obtain the size of the local file
    size = getSize( file )
    if size == 0:
      errStr = "ReplicaManager.putAndRegister: Supplied file is zero size."
      gLogger.error( errStr, file )
      return S_ERROR( errStr )
    # If the GUID is not given, generate it here
    if not guid:
      guid = makeGuid( file )
    if not checksum:
      gLogger.info( "ReplicaManager.putAndRegister: Checksum information not provided. Calculating adler32." )
      checksum = fileAdler( file )
      gLogger.info( "ReplicaManager.putAndRegister: Checksum calculated to be %s." % checksum )
    res = self.fileCatalogue.exists( {lfn:guid} )
    if not res['OK']:
      errStr = "ReplicaManager.putAndRegister: Completey failed to determine existence of destination LFN."
      gLogger.error( errStr, lfn )
      return res
    if not res['Value']['Successful'].has_key( lfn ):
      errStr = "ReplicaManager.putAndRegister: Failed to determine existence of destination LFN."
      gLogger.error( errStr, lfn )
      return S_ERROR( errStr )
    if res['Value']['Successful'][lfn]:
      if res['Value']['Successful'][lfn] == lfn:
        errStr = "ReplicaManager.putAndRegister: The supplied LFN already exists in the File Catalog."
        gLogger.error( errStr, lfn )
      else:
        errStr = "ReplicaManager.putAndRegister: This file GUID already exists for another file. Please remove it and try again."
        gLogger.error( errStr, res['Value']['Successful'][lfn] )
      return S_ERROR( "%s %s" % ( errStr, res['Value']['Successful'][lfn] ) )
    # If the local file name is not the same as the LFN filename then use the LFN file name
    alternativeFile = None
    lfnFileName = os.path.basename( lfn )
    localFileName = os.path.basename( file )
    if not lfnFileName == localFileName:
      alternativeFile = lfnFileName

    ##########################################################
    #  Instantiate the destination storage element here.
    storageElement = StorageElement( diracSE )
    res = storageElement.isValid()
    if not res['OK']:
      errStr = "ReplicaManager.putAndRegister: The storage element is not currently valid."
      gLogger.error( errStr, "%s %s" % ( diracSE, res['Message'] ) )
      return S_ERROR( errStr )
    destinationSE = storageElement.getStorageElementName()['Value']
    res = storageElement.getPfnForLfn( lfn )
    if not res['OK']:
      errStr = "ReplicaManager.putAndRegister: Failed to generate destination PFN."
      gLogger.error( errStr, res['Message'] )
      return S_ERROR( errStr )
    destPfn = res['Value']
    fileDict = {destPfn:file}

    successful = {}
    failed = {}
    ##########################################################
    #  Perform the put here.
    oDataOperation = self.__initialiseAccountingObject( 'putAndRegister', diracSE, 1 )
    oDataOperation.setStartTime()
    oDataOperation.setValueByKey( 'TransferSize', size )
    startTime = time.time()
    res = storageElement.putFile( fileDict, True )
    putTime = time.time() - startTime
    oDataOperation.setValueByKey( 'TransferTime', putTime )
    if not res['OK']:
      errStr = "ReplicaManager.putAndRegister: Failed to put file to Storage Element."
      oDataOperation.setValueByKey( 'TransferOK', 0 )
      oDataOperation.setValueByKey( 'FinalStatus', 'Failed' )
      oDataOperation.setEndTime()
      gDataStoreClient.addRegister( oDataOperation )
      startTime = time.time()
      gDataStoreClient.commit()
      gLogger.info( 'ReplicaManager.putAndRegister: Sending accounting took %.1f seconds' % ( time.time() - startTime ) )
      gLogger.error( errStr, "%s: %s" % ( file, res['Message'] ) )
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
      errStr = "ReplicaManager.putAndRegister: Completely failed to register file."
      gLogger.error( errStr, res['Message'] )
      failed[lfn] = {'register':registerDict}
      oDataOperation.setValueByKey( 'FinalStatus', 'Failed' )
    elif res['Value']['Failed'].has_key( lfn ):
      errStr = "ReplicaManager.putAndRegister: Failed to register file."
      gLogger.error( errStr, "%s %s" % ( lfn, res['Value']['Failed'][lfn] ) )
      oDataOperation.setValueByKey( 'FinalStatus', 'Failed' )
      failed[lfn] = {'register':registerDict}
    else:
      successful[lfn]['register'] = registerTime
      oDataOperation.setValueByKey( 'RegistrationOK', 1 )
    oDataOperation.setEndTime()
    gDataStoreClient.addRegister( oDataOperation )
    startTime = time.time()
    gDataStoreClient.commit()
    gLogger.info( 'ReplicaManager.putAndRegister: Sending accounting took %.1f seconds' % ( time.time() - startTime ) )
    resDict = {'Successful': successful, 'Failed':failed}
    return S_OK( resDict )

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
    gLogger.verbose( "ReplicaManager.replicateAndRegister: Attempting to replicate %s to %s." % ( lfn, destSE ) )
    startReplication = time.time()
    res = self.__replicate( lfn, destSE, sourceSE, destPath, localCache )
    replicationTime = time.time() - startReplication
    if not res['OK']:
      errStr = "ReplicaManager.replicateAndRegister: Completely failed to replicate file."
      gLogger.error( errStr, res['Message'] )
      return S_ERROR( errStr )
    if not res['Value']:
      # The file was already present at the destination SE
      gLogger.info( "ReplicaManager.replicateAndRegister: %s already present at %s." % ( lfn, destSE ) )
      successful[lfn] = {'replicate':0, 'register':0}
      resDict = {'Successful':successful, 'Failed':failed}
      return S_OK( resDict )
    successful[lfn] = {'replicate':replicationTime}

    destPfn = res['Value']['DestPfn']
    destSE = res['Value']['DestSE']
    gLogger.verbose( "ReplicaManager.replicateAndRegister: Attempting to register %s at %s." % ( destPfn, destSE ) )
    replicaTuple = ( lfn, destPfn, destSE )
    startRegistration = time.time()
    res = self.registerReplica( replicaTuple, catalog = catalog )
    registrationTime = time.time() - startRegistration
    if not res['OK']:
      # Need to return to the client that the file was replicated but not registered
      errStr = "ReplicaManager.replicateAndRegister: Completely failed to register replica."
      gLogger.error( errStr, res['Message'] )
      failed[lfn] = {'Registration':{'LFN':lfn, 'TargetSE':destSE, 'PFN':destPfn}}
    else:
      if res['Value']['Successful'].has_key( lfn ):
        gLogger.info( "ReplicaManager.replicateAndRegister: Successfully registered replica." )
        successful[lfn]['register'] = registrationTime
      else:
        errStr = "ReplicaManager.replicateAndRegister: Failed to register replica."
        gLogger.info( errStr, res['Value']['Failed'][lfn] )
        failed[lfn] = {'Registration':{'LFN':lfn, 'TargetSE':destSE, 'PFN':destPfn}}
    resDict = {'Successful':successful, 'Failed':failed}
    return S_OK( resDict )

  def replicate( self, lfn, destSE, sourceSE = '', destPath = '', localCache = '' ):
    """ Replicate a LFN to a destination SE and register the replica.

        'lfn' is the LFN to be replicated
        'destSE' is the Storage Element the file should be replicated to
        'sourceSE' is the source for the file replication (where not specified all replicas will be attempted)
        'destPath' is the path on the destination storage element, if to be different from LHCb convention
        'localCache' is the local file system location to be used as a temporary cache
    """
    gLogger.verbose( "ReplicaManager.replicate: Attempting to replicate %s to %s." % ( lfn, destSE ) )
    res = self.__replicate( lfn, destSE, sourceSE, destPath, localCache )
    if not res['OK']:
      errStr = "ReplicaManager.replicate: Replication failed."
      gLogger.error( errStr, "%s %s" % ( lfn, destSE ) )
      return res
    if not res['Value']:
      # The file was already present at the destination SE
      gLogger.info( "ReplicaManager.replicate: %s already present at %s." % ( lfn, destSE ) )
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
    res = self.__verifyOperationPermission( lfn )
    if not res['OK']:
      return res
    if not res['Value']:
      errStr = "ReplicaManager.__replicate: Write access not permitted for this credential."
      gLogger.error( errStr, lfn )
      return S_ERROR( errStr )

    gLogger.verbose( "ReplicaManager.__replicate: Performing replication initialization." )
    res = self.__initializeReplication( lfn, sourceSE, destSE )
    if not res['OK']:
      gLogger.error( "ReplicaManager.__replicate: Replication initialisation failed.", lfn )
      return res
    destStorageElement = res['Value']['DestStorage']
    lfnReplicas = res['Value']['Replicas']
    destSE = res['Value']['DestSE']
    catalogueSize = res['Value']['CatalogueSize']
    ###########################################################
    # If the LFN already exists at the destination we have nothing to do
    if lfnReplicas.has_key( destSE ):
      gLogger.info( "ReplicaManager.__replicate: LFN is already registered at %s." % destSE )
      return S_OK()
    ###########################################################
    # Resolve the best source storage elements for replication
    gLogger.verbose( "ReplicaManager.__replicate: Determining the best source replicas." )
    res = self.__resolveBestReplicas( sourceSE, lfnReplicas, catalogueSize )
    if not res['OK']:
      gLogger.error( "ReplicaManager.__replicate: Best replica resolution failed.", lfn )
      return res
    replicaPreference = res['Value']
    ###########################################################
    # Now perform the replication for the file
    if destPath:
      destPath = '%s/%s' % ( destPath, os.path.basename( lfn ) )
    else:
      destPath = lfn
    res = destStorageElement.getPfnForLfn( destPath )
    if not res['OK']:
      errStr = "ReplicaManager.__replicate: Failed to generate destination PFN."
      gLogger.error( errStr, res['Message'] )
      return S_ERROR( errStr )
    destPfn = res['Value']
    # Find out if there is a replica already at the same site
    localReplicas = []
    otherReplicas = []
    for sourceSE, sourcePfn in replicaPreference:
      if sourcePfn == destPfn: continue
      res = isSameSiteSE( sourceSE, destSE )
      if res['OK'] and res['Value']:
        localReplicas.append( ( sourceSE, sourcePfn ) )
      else:
        otherReplicas.append( ( sourceSE, sourcePfn ) )
    replicaPreference = localReplicas + otherReplicas
    for sourceSE, sourcePfn in replicaPreference:
      gLogger.verbose( "ReplicaManager.__replicate: Attempting replication from %s to %s." % ( sourceSE, destSE ) )
      fileDict = {destPfn:sourcePfn}
      if sourcePfn == destPfn:
        continue

      localFile = ''
      if sourcePfn.find( 'srm' ) == -1 or destPfn.find( 'srm' ) == -1:
        # No third party transfer is possible, we have to replicate through the local cache
        localDir = '.'
        if localCache:
          localDir = localCache
        self.getFile( lfn, localDir )
        localFile = os.path.join( localDir, os.path.basename( lfn ) )
        fileDict = {destPfn:localFile}

      res = destStorageElement.replicateFile( fileDict, catalogueSize, singleFile = True )
      if localFile:
        os.remove( localFile )

      if res['OK']:
        gLogger.info( "ReplicaManager.__replicate: Replication successful." )
        resDict = {'DestSE':destSE, 'DestPfn':destPfn}
        return S_OK( resDict )
      else:
        errStr = "ReplicaManager.__replicate: Replication failed."
        gLogger.error( errStr, "%s from %s to %s." % ( lfn, sourceSE, destSE ) )
    ##########################################################
    # If the replication failed for all sources give up
    errStr = "ReplicaManager.__replicate: Failed to replicate with all sources."
    gLogger.error( errStr, lfn )
    return S_ERROR( errStr )

  def __initializeReplication( self, lfn, sourceSE, destSE ):
    ###########################################################
    # Check that the destination storage element is sane and resolve its name
    gLogger.verbose( "ReplicaManager.__initializeReplication: Verifying destination Storage Element validity (%s)." % destSE )
    destStorageElement = StorageElement( destSE )
    res = destStorageElement.isValid()
    if not res['OK']:
      errStr = "ReplicaManager.__initializeReplication: The storage element is not currently valid."
      gLogger.error( errStr, "%s %s" % ( destSE, res['Message'] ) )
      return S_ERROR( errStr )
    destSE = destStorageElement.getStorageElementName()['Value']
    gLogger.info( "ReplicaManager.__initializeReplication: Destination Storage Element verified." )
    ###########################################################
    # Get the LFN replicas from the file catalogue
    gLogger.verbose( "ReplicaManager.__initializeReplication: Attempting to obtain replicas for %s." % lfn )
    res = self.fileCatalogue.getReplicas( lfn )
    if not res['OK']:
      errStr = "ReplicaManager.__initializeReplication: Completely failed to get replicas for LFN."
      gLogger.error( errStr, "%s %s" % ( lfn, res['Message'] ) )
      return res
    if not res['Value']['Successful'].has_key( lfn ):
      errStr = "ReplicaManager.__initializeReplication: Failed to get replicas for LFN."
      gLogger.error( errStr, "%s %s" % ( lfn, res['Value']['Failed'][lfn] ) )
      return S_ERROR( "%s %s" % ( errStr, res['Value']['Failed'][lfn] ) )
    gLogger.info( "ReplicaManager.__initializeReplication: Successfully obtained replicas for LFN." )
    lfnReplicas = res['Value']['Successful'][lfn]
    ###########################################################
    # If the file catalogue size is zero fail the transfer
    gLogger.verbose( "ReplicaManager.__initializeReplication: Attempting to obtain size for %s." % lfn )
    res = self.fileCatalogue.getFileSize( lfn )
    if not res['OK']:
      errStr = "ReplicaManager.__initializeReplication: Completely failed to get size for LFN."
      gLogger.error( errStr, "%s %s" % ( lfn, res['Message'] ) )
      return res
    if not res['Value']['Successful'].has_key( lfn ):
      errStr = "ReplicaManager.__initializeReplication: Failed to get size for LFN."
      gLogger.error( errStr, "%s %s" % ( lfn, res['Value']['Failed'][lfn] ) )
      return S_ERROR( "%s %s" % ( errStr, res['Value']['Failed'][lfn] ) )
    catalogueSize = res['Value']['Successful'][lfn]
    if catalogueSize == 0:
      errStr = "ReplicaManager.__initializeReplication: Registered file size is 0."
      gLogger.error( errStr, lfn )
      return S_ERROR( errStr )
    gLogger.info( "ReplicaManager.__initializeReplication: File size determined to be %s." % catalogueSize )
    ###########################################################
    # Check whether the destination storage element is banned
    gLogger.verbose( "ReplicaManager.__initializeReplication: Determining whether %s is banned." % destSE )
    configStr = '/Resources/StorageElements/BannedTarget'
    bannedTargets = gConfig.getValue( configStr, [] )
    if destSE in bannedTargets:
      infoStr = "ReplicaManager.__initializeReplication: Destination Storage Element is currently banned."
      gLogger.info( infoStr, destSE )
      return S_ERROR( infoStr )
    gLogger.info( "ReplicaManager.__initializeReplication: Destination site not banned." )
    ###########################################################
    # Check whether the supplied source SE is sane
    gLogger.verbose( "ReplicaManager.__initializeReplication: Determining whether source Storage Element is sane." )
    configStr = '/Resources/StorageElements/BannedSource'
    bannedSources = gConfig.getValue( configStr, [] )
    if sourceSE:
      if not lfnReplicas.has_key( sourceSE ):
        errStr = "ReplicaManager.__initializeReplication: LFN does not exist at supplied source SE."
        gLogger.error( errStr, "%s %s" % ( lfn, sourceSE ) )
        return S_ERROR( errStr )
      elif sourceSE in bannedSources:
        infoStr = "ReplicaManager.__initializeReplication: Supplied source Storage Element is currently banned."
        gLogger.info( infoStr, sourceSE )
        return S_ERROR( errStr )
    gLogger.info( "ReplicaManager.__initializeReplication: Replication initialization successful." )
    resDict = {'DestStorage':destStorageElement, 'DestSE':destSE, 'Replicas':lfnReplicas, 'CatalogueSize':catalogueSize}
    return S_OK( resDict )

  def __resolveBestReplicas( self, sourceSE, lfnReplicas, catalogueSize ):
    ###########################################################
    # Determine the best replicas (remove banned sources, invalid storage elements and file with the wrong size)
    configStr = '/Resources/StorageElements/BannedSource'
    bannedSources = gConfig.getValue( configStr, [] )
    gLogger.info( "ReplicaManager.__resolveBestReplicas: Obtained current banned sources." )
    replicaPreference = []
    for diracSE, pfn in lfnReplicas.items():
      if sourceSE and diracSE != sourceSE:
        gLogger.info( "ReplicaManager.__resolveBestReplicas: %s replica not requested." % diracSE )
      elif diracSE in bannedSources:
        gLogger.info( "ReplicaManager.__resolveBestReplicas: %s is currently banned as a source." % diracSE )
      else:
        gLogger.info( "ReplicaManager.__resolveBestReplicas: %s is available for use." % diracSE )
        storageElement = StorageElement( diracSE )
        res = storageElement.isValid()
        if not res['OK']:
          errStr = "ReplicaManager.__resolveBestReplicas: The storage element is not currently valid."
          gLogger.error( errStr, "%s %s" % ( diracSE, res['Message'] ) )
        else:
          if storageElement.getRemoteProtocols()['Value']:
            gLogger.verbose( "ReplicaManager.__resolveBestReplicas: Attempting to get source pfns for remote protocols." )
            res = storageElement.getPfnForProtocol( pfn, self.thirdPartyProtocols )
            if res['OK']:
              sourcePfn = res['Value']
              gLogger.verbose( "ReplicaManager.__resolveBestReplicas: Attempting to get source file size." )
              res = storageElement.getFileSize( sourcePfn )
              if res['OK']:
                if res['Value']['Successful'].has_key( sourcePfn ):
                  sourceFileSize = res['Value']['Successful'][sourcePfn]
                  gLogger.info( "ReplicaManager.__resolveBestReplicas: Source file size determined to be %s." % sourceFileSize )
                  if catalogueSize == sourceFileSize:
                    fileTuple = ( diracSE, sourcePfn )
                    replicaPreference.append( fileTuple )
                  else:
                    errStr = "ReplicaManager.__resolveBestReplicas: Catalogue size and physical file size mismatch."
                    gLogger.error( errStr, "%s %s" % ( diracSE, sourcePfn ) )
                else:
                  errStr = "ReplicaManager.__resolveBestReplicas: Failed to get physical file size."
                  gLogger.error( errStr, "%s %s: %s" % ( sourcePfn, diracSE, res['Value']['Failed'][sourcePfn] ) )
              else:
                errStr = "ReplicaManager.__resolveBestReplicas: Completely failed to get physical file size."
                gLogger.error( errStr, "%s %s: %s" % ( sourcePfn, diracSE, res['Message'] ) )
            else:
              errStr = "ReplicaManager.__resolveBestReplicas: Failed to get PFN for replication for StorageElement."
              gLogger.error( errStr, "%s %s" % ( diracSE, res['Message'] ) )
          else:
            errStr = "ReplicaManager.__resolveBestReplicas: Source Storage Element has no remote protocols."
            gLogger.info( errStr, diracSE )
    if not replicaPreference:
      errStr = "ReplicaManager.__resolveBestReplicas: Failed to find any valid source Storage Elements."
      gLogger.error( errStr )
      return S_ERROR( errStr )
    else:
      return S_OK( replicaPreference )

  ###################################################################
  #
  # These are the file catalog write methods
  #

  def registerFile( self, fileTuple, catalog = '' ):
    """ Register a file.

        'fileTuple' is the file tuple to be registered of the form (lfn,physicalFile,fileSize,storageElementName,fileGuid)
    """
    if type( fileTuple ) == types.ListType:
      fileTuples = fileTuple
    elif type( fileTuple ) == types.TupleType:
      fileTuples = [fileTuple]
    else:
      errStr = "ReplicaManager.registerFile: Supplied file info must be tuple of list of tuples."
      gLogger.error( errStr )
      return S_ERROR( errStr )
    gLogger.verbose( "ReplicaManager.registerFile: Attempting to register %s files." % len( fileTuples ) )
    res = self.__registerFile( fileTuples, catalog )
    if not res['OK']:
      errStr = "ReplicaManager.registerFile: Completely failed to register files."
      gLogger.error( errStr, res['Message'] )
      return S_ERROR( errStr )
    return res

  def __registerFile( self, fileTuples, catalog ):
    seDict = {}
    for lfn, physicalFile, fileSize, storageElementName, fileGuid, checksum in fileTuples:
      if not seDict.has_key( storageElementName ):
        seDict[storageElementName] = []
      seDict[storageElementName].append( ( lfn, physicalFile, fileSize, storageElementName, fileGuid, checksum ) )
    successful = {}
    failed = {}
    fileDict = {}
    for storageElementName, fileTuple in seDict.items():
      destStorageElement = StorageElement( storageElementName, overwride = True )
      res = destStorageElement.isValid()
      if not res['OK']:
        errStr = "ReplicaManager.__registerFile: The storage element is not currently valid."
        gLogger.error( errStr, "%s %s" % ( storageElementName, res['Message'] ) )
        for lfn, physicalFile, fileSize, storageElementName, fileGuid, checksum in fileTuple:
          failed[lfn] = errStr
      else:
        storageElementName = destStorageElement.getStorageElementName()['Value']
        for lfn, physicalFile, fileSize, storageElementName, fileGuid, checksum in fileTuple:
          res = destStorageElement.getPfnForProtocol( physicalFile, self.registrationProtocol, withPort = False )
          if not res['OK']:
            pfn = physicalFile
          else:
            pfn = res['Value']
          tuple = ( lfn, pfn, fileSize, storageElementName, fileGuid, checksum )
          fileDict[lfn] = {'PFN':pfn, 'Size':fileSize, 'SE':storageElementName, 'GUID':fileGuid, 'Checksum':checksum}
    gLogger.verbose( "ReplicaManager.__registerFile: Resolved %s files for registration." % len( fileDict.keys() ) )
    if catalog:
      fileCatalog = FileCatalog( catalog )
      res = fileCatalog.addFile( fileDict )
    else:
      res = self.fileCatalogue.addFile( fileDict )
    if not res['OK']:
      errStr = "ReplicaManager.__registerFile: Completely failed to register files."
      gLogger.error( errStr, res['Message'] )
      return S_ERROR( errStr )
    failed.update( res['Value']['Failed'] )
    successful = res['Value']['Successful']
    resDict = {'Successful':successful, 'Failed':failed}
    return S_OK( resDict )

  def registerReplica( self, replicaTuple, catalog = '' ):
    """ Register a replica supplied in the replicaTuples.

        'replicaTuple' is a tuple or list of tuples of the form (lfn,pfn,se)
    """
    if type( replicaTuple ) == types.ListType:
      replicaTuples = replicaTuple
    elif type( replicaTuple ) == types.TupleType:
      replicaTuples = [replicaTuple]
    else:
      errStr = "ReplicaManager.registerReplica: Supplied file info must be tuple of list of tuples."
      gLogger.error( errStr )
      return S_ERROR( errStr )
    gLogger.verbose( "ReplicaManager.registerReplica: Attempting to register %s replicas." % len( replicaTuples ) )
    res = self.__registerReplica( replicaTuples, catalog )
    if not res['OK']:
      errStr = "ReplicaManager.registerReplica: Completely failed to register replicas."
      gLogger.error( errStr, res['Message'] )
    return res

  def __registerReplica( self, replicaTuples, catalog ):
    seDict = {}
    for lfn, pfn, storageElementName in replicaTuples:
      if not seDict.has_key( storageElementName ):
        seDict[storageElementName] = []
      seDict[storageElementName].append( ( lfn, pfn ) )
    successful = {}
    failed = {}
    replicaTuples = []
    for storageElementName, replicaTuple in seDict.items():
      destStorageElement = StorageElement( storageElementName )
      res = destStorageElement.isValid()
      if not res['OK']:
        errStr = "ReplicaManager.__registerReplica: The storage element is not currently valid."
        gLogger.error( errStr, "%s %s" % ( storageElementName, res['Message'] ) )
        for lfn, pfn in replicaTuple:
          failed[lfn] = errStr
      else:
        storageElementName = destStorageElement.getStorageElementName()['Value']
        for lfn, pfn in replicaTuple:
          res = destStorageElement.getPfnForProtocol( pfn, self.registrationProtocol, withPort = False )
          if not res['OK']:
            failed[lfn] = res['Message']
          else:
            replicaTuple = ( lfn, res['Value'], storageElementName, False )
            replicaTuples.append( replicaTuple )
    gLogger.verbose( "ReplicaManager.__registerReplica: Successfully resolved %s replicas for registration." % len( replicaTuples ) )
    #HACK!
    replicaDict = {}
    for lfn, pfn, se, master in replicaTuples:
      replicaDict[lfn] = {'SE':se, 'PFN':pfn}

    if catalog:
      fileCatalog = FileCatalog( catalog )
      res = fileCatalog.addReplica( replicaDict )
    else:
      res = self.fileCatalogue.addReplica( replicaDict )
    if not res['OK']:
      errStr = "ReplicaManager.__registerReplica: Completely failed to register replicas."
      gLogger.error( errStr, res['Message'] )
      return S_ERROR( errStr )
    failed.update( res['Value']['Failed'] )
    successful = res['Value']['Successful']
    resDict = {'Successful':successful, 'Failed':failed}
    return S_OK( resDict )

  ###################################################################
  #
  # These are the removal methods for physical and catalogue removal
  #

  def removeFile( self, lfn ):
    """ Remove the file (all replicas) from Storage Elements and file catalogue

        'lfn' is the file to be removed
    """
    if type( lfn ) == types.ListType:
      lfns = lfn
    elif type( lfn ) == types.StringType:
      lfns = [lfn]
    else:
      errStr = "ReplicaManager.removeFile: Supplied lfns must be string or list of strings."
      gLogger.error( errStr )
      return S_ERROR( errStr )
    # Check that we have write permissions to this directory.
    res = self.__verifyOperationPermission( lfns )
    if not res['OK']:
      return res
    if not res['Value']:
      errStr = "ReplicaManager.__replicate: Write access not permitted for this credential."
      gLogger.error( errStr, lfns )
      return S_ERROR( errStr )

    gLogger.verbose( "ReplicaManager.removeFile: Attempting to remove %s files from Storage and Catalogue." % len( lfns ) )
    gLogger.verbose( "ReplicaManager.removeFile: Attempting to obtain replicas for %s lfns." % len( lfns ) )
    res = self.fileCatalogue.exists( lfns )
    if not res['OK']:
      errStr = "ReplicaManager.removeFile: Completely failed to determine existance of lfns."
      gLogger.error( errStr, res['Message'] )
      return res
    successful = {}
    existingFiles = []
    for lfn, exists in res['Value']['Successful'].items():
      if not exists:
        successful[lfn] = True
      else:
        existingFiles.append( lfn )
    res = self.fileCatalogue.getReplicas( existingFiles, True )
    if not res['OK']:
      errStr = "ReplicaManager.removeFile: Completely failed to get replicas for lfns."
      gLogger.error( errStr, res['Message'] )
      return res
    lfnDict = res['Value']['Successful']
    failed = res['Value']['Failed']
    for lfn, reason in failed.items():
      if reason == 'File has zero replicas':
        lfnDict[lfn] = {}
        failed.pop( lfn )
    res = self.__removeFile( lfnDict )
    if not res['OK']:
      errStr = "ReplicaManager.removeFile: Completely failed to remove files."
      gLogger.error( errStr, res['Message'] )
      return res
    failed.update( res['Value']['Failed'] )
    successful.update( res['Value']['Successful'] )
    resDict = {'Successful':successful, 'Failed':failed}
    gDataStoreClient.commit()
    return S_OK( resDict )

  def __removeFile( self, lfnDict ):
    storageElementDict = {}
    for lfn, repDict in lfnDict.items():
      for se, pfn in repDict.items():
        if not storageElementDict.has_key( se ):
          storageElementDict[se] = []
        storageElementDict[se].append( ( lfn, pfn ) )
    failed = {}
    for storageElementName, fileTuple in storageElementDict.items():
      res = self.__removeReplica( storageElementName, fileTuple )
      if not res['OK']:
        errStr = res['Message']
        for lfn, pfn in fileTuple:
          if not failed.has_key( lfn ):
            failed[lfn] = ''
          failed[lfn] = "%s %s" % ( failed[lfn], errStr )
      else:
        for lfn, error in res['Value']['Failed'].items():
          if not failed.has_key( lfn ):
            failed[lfn] = ''
          failed[lfn] = "%s %s" % ( failed[lfn], error )
    completelyRemovedFiles = []
    for lfn in lfnDict.keys():
      if not failed.has_key( lfn ):
        completelyRemovedFiles.append( lfn )
    res = self.fileCatalogue.removeFile( completelyRemovedFiles )
    failed.update( res['Value']['Failed'] )
    successful = res['Value']['Successful']
    resDict = {'Successful':successful, 'Failed':failed}
    return S_OK( resDict )

  def removeReplica( self, storageElementName, lfn ):
    """ Remove replica at the supplied Storage Element from Storage Element then file catalogue

       'storageElementName' is the storage where the file is to be removed
       'lfn' is the file to be removed
    """
    if type( lfn ) == types.ListType:
      lfns = lfn
    elif type( lfn ) == types.StringType:
      lfns = [lfn]
    else:
      errStr = "ReplicaManager.removeReplica: Supplied lfns must be string or list of strings."
      gLogger.error( errStr )
      return S_ERROR( errStr )
    # Check that we have write permissions to this directory.
    res = self.__verifyOperationPermission( lfns )
    if not res['OK']:
      return res
    if not res['Value']:
      errStr = "ReplicaManager.__replicate: Write access not permitted for this credential."
      gLogger.error( errStr, lfns )
      return S_ERROR( errStr )
    gLogger.verbose( "ReplicaManager.removeReplica: Attempting to remove catalogue entry for %s lfns at %s." % ( len( lfns ), storageElementName ) )
    res = self.fileCatalogue.getReplicas( lfns, True )
    if not res['OK']:
      errStr = "ReplicaManager.removeReplica: Completely failed to get replicas for lfns."
      gLogger.error( errStr, res['Message'] )
      return res
    failed = res['Value']['Failed']
    successful = {}
    replicaTuples = []
    for lfn, repDict in res['Value']['Successful'].items():
      if not repDict.has_key( storageElementName ):
        # The file doesn't exist at the storage element so don't have to remove it
        successful[lfn] = True
      elif len( repDict.keys() ) == 1:
        # The file has only a single replica so don't remove
        gLogger.error( "The replica you are trying to remove is the only one.", "%s @ %s" % ( lfn, storageElementName ) )
        failed[lfn] = "Failed to remove sole replica"
      else:
        sePfn = repDict[storageElementName]
        replicaTuple = ( lfn, sePfn )
        replicaTuples.append( replicaTuple )
    res = self.__removeReplica( storageElementName, replicaTuples )
    if not res['OK']:
      return res
    failed.update( res['Value']['Failed'] )
    successful.update( res['Value']['Successful'] )
    resDict = {'Successful':successful, 'Failed':failed}
    gDataStoreClient.commit()
    return S_OK( resDict )

  def __removeReplica( self, storageElementName, fileTuple ):
    pfnDict = {}
    failed = {}
    for lfn, pfn in fileTuple:
      res = self.__verifyOperationPermission( lfn )
      if not res['OK'] or not res['Value']:
        errStr = "ReplicaManager.__removeReplica: Write access not permitted for this credential."
        gLogger.error( errStr, lfn )
        failed[lfn] = errStr
        continue
      pfnDict[pfn] = lfn
    res = self.__removePhysicalReplica( storageElementName, pfnDict.keys() )
    if not res['OK']:
      errStr = "ReplicaManager.__removeReplica: Failed to remove catalog replicas."
      gLogger.error( errStr, res['Message'] )
      return S_ERROR( errStr )
    for pfn, error in res['Value']['Failed'].items():
      failed[pfnDict[pfn]] = error
    replicaTuples = []
    for pfn,surl in res['Value']['Successful'].items():
      replicaTuple = ( pfnDict[pfn], surl, storageElementName )
      replicaTuples.append( replicaTuple )
    successful = {}
    res = self.__removeCatalogReplica( replicaTuples )
    if not res['OK']:
      errStr = "ReplicaManager.__removeReplica: Completely failed to remove physical files."
      gLogger.error( errStr, res['Message'] )
      for lfn in pfnDict.values():
        if not failed.has_key( lfn ):
          failed[lfn] = errStr
    else:
      failed.update( res['Value']['Failed'] )
      successful = res['Value']['Successful']
    resDict = {'Successful':successful, 'Failed':failed}
    return S_OK( resDict )

  def removeReplicaFromCatalog( self, storageElementName, lfn ):
    # Remove replica from the file catalog 'lfn' are the file to be removed 'storageElementName' is the storage where the file is to be removed
    if type( lfn ) == types.ListType:
      lfns = lfn
    elif type( lfn ) == types.StringType:
      lfns = [lfn]
    else:
      errStr = "ReplicaManager.removeCatalogReplica: Supplied lfns must be string or list of strings."
      gLogger.error( errStr )
      return S_ERROR( errStr )
    gLogger.verbose( "ReplicaManager.removeCatalogReplica: Attempting to remove catalogue entry for %s lfns at %s." % ( len( lfns ), storageElementName ) )
    res = self.getCatalogReplicas( lfns, allStatus = True )
    if not res['OK']:
      errStr = "ReplicaManager.removeCatalogReplica: Completely failed to get replicas for lfns."
      gLogger.error( errStr, res['Message'] )
      return res
    failed = res['Value']['Failed']
    successful = {}
    replicaTuples = []
    for lfn, repDict in res['Value']['Successful'].items():
      if not repDict.has_key( storageElementName ):
        # The file doesn't exist at the storage element so don't have to remove it
        successful[lfn] = True
      else:
        sePfn = repDict[storageElementName]
        replicaTuple = ( lfn, sePfn, storageElementName )
        replicaTuples.append( replicaTuple )
    gLogger.verbose( "ReplicaManager.removeCatalogReplica: Resolved %s pfns for catalog removal at %s." % ( len( replicaTuples ), storageElementName ) )
    res = self.__removeCatalogReplica( replicaTuples )
    failed.update( res['Value']['Failed'] )
    successful.update( res['Value']['Successful'] )
    resDict = {'Successful':successful, 'Failed':failed}
    return S_OK( resDict )

  def removeCatalogPhysicalFileNames( self, replicaTuple ):
    """ Remove replicas from the file catalog specified by replica tuple

       'replicaTuple' is a tuple containing the replica to be removed and is of the form (lfn,pfn,se)
    """
    if type( replicaTuple ) == types.ListType:
      replicaTuples = replicaTuple
    elif type( replicaTuple ) == types.TupleType:
      replicaTuples = [replicaTuple]
    else:
      errStr = "ReplicaManager.removeCatalogPhysicalFileNames: Supplied info must be tuple or list of tuples."
      gLogger.error( errStr )
      return S_ERROR( errStr )
    res = self.__removeCatalogReplica( replicaTuples )
    return res

  def __removeCatalogReplica( self, replicaTuple ):
    oDataOperation = self.__initialiseAccountingObject( 'removeCatalogReplica', '', len( replicaTuple ) )
    oDataOperation.setStartTime()
    start = time.time()
    #HACK!
    replicaDict = {}
    for lfn, pfn, se in replicaTuple:
      replicaDict[lfn] = {'SE':se, 'PFN':pfn}
    res = self.fileCatalogue.removeReplica( replicaDict )
    oDataOperation.setEndTime()
    oDataOperation.setValueByKey( 'RegistrationTime', time.time() - start )
    if not res['OK']:
      oDataOperation.setValueByKey( 'RegistrationOK', 0 )
      oDataOperation.setValueByKey( 'FinalStatus', 'Failed' )
      gDataStoreClient.addRegister( oDataOperation )
      errStr = "ReplicaManager.__removeCatalogReplica: Completely failed to remove replica."
      gLogger.error( errStr, res['Message'] )
      return S_ERROR( errStr )
    for lfn in res['Value']['Successful'].keys():
      infoStr = "ReplicaManager.__removeCatalogReplica: Successfully removed replica."
      gLogger.debug( infoStr, lfn )
    if res['Value']['Successful']:
      gLogger.info( "ReplicaManager.__removeCatalogReplica: Removed %d replicas" % len( res['Value']['Successful'] ) )
    for lfn, error in res['Value']['Failed'].items():
      errStr = "ReplicaManager.__removeCatalogReplica: Failed to remove replica."
      gLogger.error( errStr, "%s %s" % ( lfn, error ) )
    oDataOperation.setValueByKey( 'RegistrationOK', len( res['Value']['Successful'].keys() ) )
    gDataStoreClient.addRegister( oDataOperation )
    return res

  def removePhysicalReplica( self, storageElementName, lfn ):
    """ Remove replica from Storage Element.

       'lfn' are the files to be removed
       'storageElementName' is the storage where the file is to be removed
    """
    if type( lfn ) == types.ListType:
      lfns = lfn
    elif type( lfn ) == types.StringType:
      lfns = [lfn]
    else:
      errStr = "ReplicaManager.removePhysicalReplica: Supplied lfns must be string or list of strings."
      gLogger.error( errStr )
      return S_ERROR( errStr )
    # Check that we have write permissions to this directory.
    res = self.__verifyOperationPermission( lfns )
    if not res['OK']:
      return res
    if not res['Value']:
      errStr = "ReplicaManager.__replicate: Write access not permitted for this credential."
      gLogger.error( errStr, lfns )
      return S_ERROR( errStr )
    gLogger.verbose( "ReplicaManager.removePhysicalReplica: Attempting to remove %s lfns at %s." % ( len( lfns ), storageElementName ) )
    gLogger.verbose( "ReplicaManager.removePhysicalReplica: Attempting to resolve replicas." )
    res = self.fileCatalogue.getReplicas( lfns )
    if not res['OK']:
      errStr = "ReplicaManager.removePhysicalReplica: Completely failed to get replicas for lfns."
      gLogger.error( errStr, res['Message'] )
      return res
    failed = res['Value']['Failed']
    successful = {}
    pfnDict = {}
    for lfn, repDict in res['Value']['Successful'].items():
      if not repDict.has_key( storageElementName ):
        # The file doesn't exist at the storage element so don't have to remove it
        successful[lfn] = True
      else:
        sePfn = repDict[storageElementName]
        pfnDict[sePfn] = lfn
    gLogger.verbose( "ReplicaManager.removePhysicalReplica: Resolved %s pfns for removal at %s." % ( len( pfnDict.keys() ), storageElementName ) )
    res = self.__removePhysicalReplica( storageElementName, pfnDict.keys() )
    for pfn, error in res['Value']['Failed'].items():
      failed[pfnDict[pfn]] = error
    for pfn in res['Value']['Successful'].keys():
      successful[pfnDict[pfn]]
    resDict = {'Successful':successful, 'Failed':failed}
    return S_OK( resDict )

  def __removePhysicalReplica( self, storageElementName, pfnsToRemove ):
    gLogger.verbose( "ReplicaManager.__removePhysicalReplica: Attempting to remove %s pfns at %s." % ( len( pfnsToRemove ), storageElementName ) )
    storageElement = StorageElement( storageElementName, overwride = True )
    res = storageElement.isValid()
    if not res['OK']:
      errStr = "ReplicaManager.__removePhysicalReplica: The storage element is not currently valid."
      gLogger.error( errStr, "%s %s" % ( storageElementName, res['Message'] ) )
      return S_ERROR( errStr )
    oDataOperation = self.__initialiseAccountingObject( 'removePhysicalReplica', storageElementName, len( pfnsToRemove ) )
    oDataOperation.setStartTime()
    start = time.time()
    res = storageElement.removeFile( pfnsToRemove )
    oDataOperation.setEndTime()
    oDataOperation.setValueByKey( 'TransferTime', time.time() - start )
    if not res['OK']:
      oDataOperation.setValueByKey( 'TransferOK', 0 )
      oDataOperation.setValueByKey( 'FinalStatus', 'Failed' )
      gDataStoreClient.addRegister( oDataOperation )
      errStr = "ReplicaManager.__removePhysicalReplica: Failed to remove replicas."
      gLogger.error( errStr, res['Message'] )
      return S_ERROR( errStr )
    else:
      oDataOperation.setValueByKey( 'TransferOK', len( res['Value']['Successful'].keys() ) )
      gDataStoreClient.addRegister( oDataOperation )
      infoStr = "ReplicaManager.__removePhysicalReplica: Successfully issued accounting removal request."
      gLogger.info( infoStr )
      for surl,value in res['Value']['Successful'].items():
        ret = storageElement.getPfnForProtocol( surl, self.registrationProtocol, withPort = False )
        if not ret['OK']:
          res['Value']['Successful'][surl] = surl
        else:
          res['Value']['Successful'][surl] = ret['Value']
      return res

  #########################################################################
  #
  # File transfer methods
  #

  def put( self, lfn, file, diracSE, path = None ):
    """ Put a local file to a Storage Element

        'lfn' is the file LFN
        'file' is the full path to the local file
        'diracSE' is the Storage Element to which to put the file
        'path' is the path on the storage where the file will be put (if not provided the LFN will be used)
    """
    # Check that the local file exists
    if not os.path.exists( file ):
      errStr = "ReplicaManager.put: Supplied file does not exist."
      gLogger.error( errStr, file )
      return S_ERROR( errStr )
    # If the path is not provided then use the LFN path
    if not path:
      path = os.path.dirname( lfn )
    # Obtain the size of the local file
    size = getSize( file )
    if size == 0:
      errStr = "ReplicaManager.put: Supplied file is zero size."
      gLogger.error( errStr, file )
      return S_ERROR( errStr )
    # If the local file name is not the same as the LFN filename then use the LFN file name
    alternativeFile = None
    lfnFileName = os.path.basename( lfn )
    localFileName = os.path.basename( file )
    if not lfnFileName == localFileName:
      alternativeFile = lfnFileName

    ##########################################################
    #  Instantiate the destination storage element here.
    storageElement = StorageElement( diracSE )
    res = storageElement.isValid()
    if not res['OK']:
      errStr = "ReplicaManager.put: The storage element is not currently valid."
      gLogger.error( errStr, "%s %s" % ( diracSE, res['Message'] ) )
      return S_ERROR( errStr )
    destinationSE = storageElement.getStorageElementName()['Value']
    res = storageElement.getPfnForLfn( lfn )
    if not res['OK']:
      errStr = "ReplicaManager.put: Failed to generate destination PFN."
      gLogger.error( errStr, res['Message'] )
      return S_ERROR( errStr )
    destPfn = res['Value']
    fileDict = {destPfn:file}

    successful = {}
    failed = {}
    ##########################################################
    #  Perform the put here.
    startTime = time.time()
    res = storageElement.putFile( fileDict, singleFile = True )
    putTime = time.time() - startTime
    if not res['OK']:
      errStr = "ReplicaManager.put: Failed to put file to Storage Element."
      failed[lfn] = res['Message']
      gLogger.error( errStr, "%s: %s" % ( file, res['Message'] ) )
    else:
      gLogger.info( "ReplicaManager.put: Put file to storage in %s seconds." % putTime )
      successful[lfn] = destPfn
    resDict = {'Successful': successful, 'Failed':failed}
    return S_OK( resDict )

  #def removeReplica(self,lfn,storageElementName,singleFile=False):
  #def putReplica(self,lfn,storageElementName,singleFile=False):
  #def replicateReplica(self,lfn,size,storageElementName,singleFile=False):

  def getActiveReplicas( self, lfns ):
    """ Get all the replicas for the SEs which are in Active status for reading.
    """
    res = self.getCatalogReplicas( lfns )
    if not res['OK']:
      return res
    replicas = res['Value']
    return self.checkActiveReplicas( replicas )

  def checkActiveReplicas( self, replicaDict ):
    """ Check a replica dictionary for active replicas
    """

    if type( replicaDict ) != types.DictType:
      return S_ERROR( 'Wrong argument type %s, expected a Dictionary' % type( replicaDict ) )

    for key in [ 'Successful', 'Failed' ]:
      if not key in replicaDict:
        return S_ERROR( 'Missing key "%s" in replica Dictionary' % key )
      if type( replicaDict[key] ) != types.DictType:
        return S_ERROR( 'Wrong argument type %s, expected a Dictionary' % type( replicaDict[key] ) )

    seReadStatus = {}
    for lfn, replicas in replicaDict['Successful'].items():
      if type( replicas ) != types.DictType:
        del replicaDict['Successful'][ lfn ]
        replicaDict['Failed'][lfn] = 'Wrong replica info'
        continue
      for se in replicas.keys():
        if not seReadStatus.has_key( se ):
          res = self.__SEActive( se )
          if res['OK']:
            seReadStatus[se] = res['Value']['Read']
          else:
            seReadStatus[se] = False
        if not seReadStatus[se]:
          replicas.pop( se )

    return S_OK( replicaDict )

  def __SEActive( self, se ):
    storageCFGBase = "/Resources/StorageElements"
    res = gConfig.getOptionsDict( "%s/%s" % ( storageCFGBase, se ) )
    if not res['OK']:
      return S_ERROR( "SE not known" )
    seStatus = {'Read':True, 'Write':True}
    if ( res['Value'].has_key( "ReadAccess" ) ) and ( res['Value']['ReadAccess'] != 'Active' ):
      seStatus['Read'] = False
    if ( res['Value'].has_key( "WriteAccess" ) ) and ( res['Value']['WriteAccess'] != 'Active' ):
      seStatus['Write'] = False
    return S_OK( seStatus )

  def __initialiseAccountingObject( self, operation, se, files ):
    import DIRAC
    accountingDict = {}
    accountingDict['OperationType'] = operation
    accountingDict['User'] = 'acsmith'
    accountingDict['Protocol'] = 'ReplicaManager'
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

  def  onlineRetransfer( self, storageElementName, physicalFile ):
    """ Requests the online system to re-transfer files

        'storageElementName' is the storage element where the file should be removed from
        'physicalFile' is the physical files
    """
    return self._executeStorageElementFunction( storageElementName, physicalFile, 'retransferOnlineFile' )

  def getReplicas( self, lfn ):
    return self.getCatalogReplicas( lfn )

  def getFileSize( self, lfn ):
    return self.getCatalogFileSize( lfn )

