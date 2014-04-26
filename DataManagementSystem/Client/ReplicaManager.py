""" :mod: ReplicaManager
    =======================

    .. module: ReplicaManager
    :synopsis: ReplicaManager links the functionalities of StorageElement and FileCatalog.

    This module consists ReplicaManager and related classes.

"""

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
from DIRAC.Resources.Utilities import Utils
from DIRAC.ResourceStatusSystem.Client.ResourceStatus import ResourceStatus
from DIRAC.Core.Security.ProxyInfo import getProxyInfo

class CatalogBase( object ):
  """
  .. class:: CatalogBase

  This class stores the two wrapper functions for interacting with the FileCatalog.
  """
  def __init__( self ):
    self.log = gLogger.getSubLogger( self.__class__.__name__, True )
    self.useCatalogPFN = Operations().getValue( 'DataManagement/UseCatalogPFN', True )

  def _callFileCatalogFcnSingleFile( self, lfn, method, argsDict = None, catalogs = None ):
    """ A wrapper around :CatalogBase._callFileCatalogFcn: for a single file. It parses
    the output of :CatalogBase_callFileCatalogFcn: for the first file provided as input.
    If this file is found in::

    * res['Value']['Successful'] an S_OK() is returned with the value.
    * res['Value']['Failed'] an S_ERROR() is returned with the error message.

    :warning: this function is executed only for the first LFN provided, in case of dict of LFNs
    the order of keys are NOT preserved, so the output is undefined

    :param self: self reference
    :param mixed lfn: LFN as string or list with LFNs or dict with LFNs as keys
    :param str method: :FileCatalog: method name
    :param dict argsDict: kwargs for method
    :param list catalogs: list with catalog names
    """
    # # default values
    argsDict = argsDict if argsDict else dict()
    catalogs = catalogs if catalogs else list()
    # # checjk type
    if not lfn or type( lfn ) not in StringTypes + ( ListType, DictType ):
      return S_ERROR( "wrong type (%s) for argument 'lfn'" % type( lfn ) )
    singleLfn = lfn
    if type( lfn ) == ListType:
      singleLfn = lfn[0]
    elif type( lfn ) == DictType:
      singleLfn = lfn.keys()[0]
    # # call only for single lfn
    res = self._callFileCatalogFcn( lfn, method, argsDict, catalogs = catalogs )
    if not res["OK"]:
      return res
    elif singleLfn in res["Value"]["Failed"]:
      return S_ERROR( res["Value"]["Failed"][singleLfn] )
    if not singleLfn in res["Value"]["Successful"]:
      result = S_OK( {} )
      for catalog in catalogs:
        result['Value'][catalog] = 'OK'
      return result
    return S_OK( res["Value"]["Successful"][singleLfn] )

  def _callFileCatalogFcn( self, lfn, method, argsDict = None, catalogs = None ):
    """ A simple wrapper around the file catalog functionality

    This is a wrapper around the available :FileCatalog: functions.
    The :lfn: and :method: arguments must be provided.

    :param self: self reference
    :param mixed lfn: a single LFN string or a list of LFNs or dictionary with LFNs stored as keys.
    :param str method: name of the FileCatalog function to be invoked
    :param dict argsDict: aditional keyword arguments that are requred for the :method:
    :param list catalogs: list of catalogs the operation is to be performed on, by default this
                          is all available catalogs; examples are 'LcgFileCatalogCombined', 'BookkeepingDB',
                          'ProductionDB' etc.
    """
    # # default values
    argsDict = argsDict if argsDict else dict()
    catalogs = catalogs if catalogs else list()
    lfns = None
    if not lfn or type( lfn ) not in StringTypes + ( ListType, DictType ):
      errStr = "_callFileCatalogFcn: Wrong 'lfn' argument."
      self.log.error( errStr )
      return S_ERROR( errStr )
    elif type( lfn ) in StringTypes:
      lfns = { lfn : False }
    elif type( lfn ) == ListType:
      lfns = dict.fromkeys( lfn, False )
    elif type( lfn ) == DictType:
      lfns = lfn.copy()

    # # lfns supplied?
    if not lfns:
      errMsg = "_callFileCatalogFcn: No lfns supplied."
      self.log.error( errMsg )
      return S_ERROR( errMsg )
    self.log.debug( "_callFileCatalogFcn: Will execute '%s' method with %s lfns." % ( method, len( lfns ) ) )
    # # create FileCatalog instance
    fileCatalog = FileCatalog( catalogs = catalogs )
    if not fileCatalog.isOK():
      return S_ERROR( "Can't get FileCatalogs %s" % catalogs )
    # # get symbol
    fcFcn = getattr( fileCatalog, method ) if hasattr( fileCatalog, method ) else None
    # # check if it is callable
    fcFcn = fcFcn if callable( fcFcn ) else None
    if not fcFcn:
      errMsg = "_callFileCatalogFcn: '%s' isn't a member function in FileCatalog." % method
      self.log.error( errMsg )
      return S_ERROR( errMsg )
    # # call it at least
    res = fcFcn( lfns, **argsDict )
    if not res["OK"]:
      self.log.error( "_callFileCatalogFcn: Failed to execute '%s'." % method, res["Message"] )
    return res

  def _fcFuncWrapper( self, singleFile = False ):
    """ choose wrapper to call

    :param self: self reference
    :param bool singleFile: flag to choose wrapper function, default :False: will
    execute :FileCatalog._callFileCatalogFcn:
    """
    return { True: self._callFileCatalogFcnSingleFile,
             False: self._callFileCatalogFcn }[singleFile]

class CatalogFile( CatalogBase ):
  """
  .. class:: CatalogFile

  Wrappers for various :FileCatalog: methods concering operations on files.
  """
  def __init__( self ):
    """ c'tor """
    CatalogBase.__init__( self )

  def getCatalogExists( self, lfn, singleFile = False, catalogs = None ):
    """ determine whether the path is registered in the :FileCatalog: by calling
    :FileCatalog.exists: method.

    :param self: self reference
    :param mixed lfn: LFN as string or list of LFN strings or dict with LFNs as keys
    :param bool singleFile: execute for the first LFN only
    :param list catalogs: catalogs' names
    """
    catalogs = catalogs if catalogs else list()
    return self._fcFuncWrapper( singleFile )( lfn, "exists", catalogs = catalogs )

  def getCatalogIsFile( self, lfn, singleFile = False, catalogs = None ):
    """ determine whether the path is registered as a file in the :FileCatalog:

    :param self: self reference
    :param mixed lfn: LFN as string or list of LFN strings or dict with LFNs as keys
    :param bool singleFile: execute for the first LFN only
    :param list catalogs: catalogs' names
    """
    catalogs = catalogs if catalogs else list()
    return self._fcFuncWrapper( singleFile )( lfn, "isFile", catalogs = catalogs )

  def getCatalogFileMetadata( self, lfn, singleFile = False, catalogs = None ):
    """ get the metadata associated to the LFN in the :FileCatalog:

    :param self: self reference
    :param mixed lfn: LFN as string or list of LFN strings or dict with LFNs as keys
    :param bool singleFile: execute for the first LFN only
    :param list catalogs: catalogs' names
    """
    catalogs = catalogs if catalogs else list()
    return self._fcFuncWrapper( singleFile )( lfn, "getFileMetadata", catalogs = catalogs )

  def getCatalogFileSize( self, lfn, singleFile = False, catalogs = None ):
    """ get the size registered for files in the FileCatalog

    :param self: self reference
    :param mixed lfn: LFN as string or list of LFN strings or dict with LFNs as keys
    :param bool singleFile: execute for the first LFN only
    :param list catalogs: catalogs' names
    """
    catalogs = catalogs if catalogs else list()
    return self._fcFuncWrapper( singleFile )( lfn, "getFileSize", catalogs = catalogs )

  def getCatalogReplicas( self, lfn, allStatus = False, singleFile = False, catalogs = None ):
    """ Get the replicas registered for files in the FileCatalog

    :param self: self reference
    :param mixed lfn: LFN as string or list of LFN strings or dict with LFNs as keys
    :param bool allStatus: ???
    :param bool singleFile: execute for the first LFN only
    :param list catalogs: catalogs' names
    """
    catalogs = catalogs if catalogs else list()
    return self._fcFuncWrapper( singleFile )( lfn, "getReplicas", argsDict = { "allStatus" : allStatus },
                                            catalogs = catalogs )

  def getCatalogLFNForPFN( self, pfn, singleFile = False, catalogs = None ):
    """ get the LFNs registered with the supplied PFNs from the FileCatalog

    :param self: self reference
    :param mixed pfn: the files to obtain (can be a single PFN or list of PFNs)
    :param bool singleFile: execute for the first LFN only
    :param list catalogs: catalogs' names
    """
    catalogs = catalogs if catalogs else list()
    return self._fcFuncWrapper( singleFile )( pfn, 'getLFNForPFN', catalogs = catalogs )

  def addCatalogFile( self, lfn, singleFile = False, catalogs = None ):
    """ Add a new file to the FileCatalog

    :param self: self reference
    :param mixed lfn: LFN as string or list of LFN strings or dict with LFNs as keys
    :param bool singleFile: execute for the first LFN only
    :param list catalogs: catalogs' names
    """
    catalogs = catalogs if catalogs else list()
    return self._fcFuncWrapper( singleFile )( lfn, "addFile", catalogs = catalogs )

  def removeCatalogFile( self, lfn, singleFile = False, catalogs = None ):
    """ remove a file from the FileCatalog

    :param self: self reference
    :param mixed lfn: LFN as string or list of LFN strings or dict with LFNs as keys
    :param bool singleFile: execute for the first LFN only
    :param list catalogs: catalogs' names
    """
    catalogs = catalogs if catalogs else list()
    # # make sure lfns are sorted from the longest to the shortest
    if type( lfn ) == ListType:
      lfn = sorted( lfn, reverse = True )
    return self._fcFuncWrapper( singleFile )( lfn, "removeFile", catalogs = catalogs )

class CatalogReplica( CatalogBase ):
  """
  .. class:: CatalogReplica

  Wrappers for various :FileCatalog: methods concering operations on replicas.
  """
  def getCatalogReplicaStatus( self, lfn, singleFile = False, catalogs = None ):
    """ get the status of the replica as registered in the :FileCatalog:

    :param self: self reference
    :param dict lfn: dict containing { LFN : SE }
    :param bool singleFile: execute for the first LFN only
    :param list catalogs: catalogs' names
    """
    catalogs = catalogs if catalogs else list()
    return self._fcFuncWrapper( singleFile )( lfn, "getReplicaStatus", catalogs = catalogs )

  def addCatalogReplica( self, lfn, singleFile = False, catalogs = None ):
    """ add a new replica to the :FileCatalog:

    :param self: self reference
    :param dict lfn: dictionary containing the replica properties
    :param bool singleFile: execute for the first LFN only
    :param list catalogs: catalogs' names
    """
    catalogs = catalogs if catalogs else list()
    return self._fcFuncWrapper( singleFile )( lfn, "addReplica", catalogs = catalogs )

  def removeCatalogReplica( self, lfn, singleFile = False, catalogs = None ):
    """ remove a replica from the :FileCatalog:

    :param self: self reference
    :param mixed lfn: lfn to be removed
    :param bool singleFile: execute for the first LFN only
    :param list catalogs: catalogs' names
    """
    catalogs = catalogs if catalogs else list()
    return self._fcFuncWrapper( singleFile )( lfn, "removeReplica", catalogs = catalogs )

  def setCatalogReplicaStatus( self, lfn, singleFile = False, catalogs = None ):
    """ Change the status for a replica in the :FileCatalog:

    :param self: self reference
    :param mixed lfn: dict with replica information to change
    :param bool singleFile: execute for the first LFN only
    :param list catalogs: catalogs' names
    """
    catalogs = catalogs if catalogs else list()
    return self._fcFuncWrapper( singleFile )( lfn, "setReplicaStatus", catalogs = catalogs )

  def setCatalogReplicaHost( self, lfn, singleFile = False, catalogs = None ):
    """ change the registered SE for a replica in the :FileCatalog:

    :param self: self reference
    :param mixed lfn: dict with replica information to change
    :param bool singleFile: execute for the first LFN only
    :param list catalogs: catalogs' names
    """
    catalogs = catalogs if catalogs else list()
    return self._fcFuncWrapper( singleFile )( lfn, "setReplicaHost", catalogs = catalogs )

class CatalogDirectory( CatalogBase ):
  """
  .. class:: CatalogDirectory

  Wrappers for various :FileCatalog: methods concering operations on folders.
  """
  def __init__( self ):
    """ c'tor """
    CatalogBase.__init__( self )

  def getCatalogIsDirectory( self, lfn, singleFile = False, catalogs = None ):
    """ determine whether the path is registered as a directory in the :FileCatalog:

    :param self: self reference
    :param mixed lfn: files to check (can be a single file or list of lfns)
    :param bool singleFile: execute for the first LFN only
    :param list catalogs: catalogs' names
    """
    catalogs = catalogs if catalogs else list()
    return self._fcFuncWrapper( singleFile )( lfn, "isDirectory", catalogs = catalogs )

  def getCatalogDirectoryMetadata( self, lfn, singleFile = False, catalogs = None ):
    """ get the metadata associated to a directory in the :FileCatalog:

    :param self: self reference
    :param mixed lfn: folders to check (can be a single directory or list of directories)
    :param bool singleFile: execute for the first LFN only
    :param list catalogs: catalogs' names
    """
    catalogs = catalogs if catalogs else list()
    return self._fcFuncWrapper( singleFile )( lfn, "getDirectoryMetadata", catalogs = catalogs )

  def getCatalogDirectoryReplicas( self, lfn, singleFile = False, catalogs = None ):
    """ get the replicas for the contents of a directory in the FileCatalog

    :param self: self reference
    :param mixed lfn: folders to check (can be a single directory or list of directories)
    :param bool singleFile: execute for the first LFN only
    :param list catalogs: catalogs' names
    """
    catalogs = catalogs if catalogs else list()
    return self._fcFuncWrapper( singleFile )( lfn, "getDirectoryReplicas", catalogs = catalogs )

  def getCatalogListDirectory( self, lfn, verbose = False, singleFile = False, catalogs = None ):
    """ get the contents of a directory in the :FileCatalog:

    :param self: self reference
    :param mixed lfn: folders to check (can be a single directory or list of directories)
    :param bool verbose: shout
    :param bool singleFile: execute for the first LFN only
    :param list catalogs: catalogs' names
    """
    catalogs = catalogs if catalogs else list()
    return self._fcFuncWrapper( singleFile )( lfn, "listDirectory", argsDict = {"verbose": verbose},
                                            catalogs = catalogs )

  def getCatalogDirectorySize( self, lfn, singleFile = False, catalogs = None ):
    """ get the size a directory in the :FileCatalog:

    :param self: self reference
    :param mixed lfn: folders to check (can be a single directory or list of directories)
    :param bool singleFile: execute for the first LFN only
    :param list catalogs: catalogs' names
    """
    catalogs = catalogs if catalogs else list()
    return self._fcFuncWrapper( singleFile )( lfn, "getDirectorySize", catalogs = catalogs )

  def createCatalogDirectory( self, lfn, singleFile = False, catalogs = None ):
    """ mkdir in the :FileCatalog:

    :param self: self reference
    :param mixed lfn: the directory to create
    :param bool singleFile: execute for the first LFN only
    :param list catalogs: catalogs' names
    """
    catalogs = catalogs if catalogs else list()
    return self._fcFuncWrapper( singleFile )( lfn, "createDirectory", catalogs = catalogs )

  def removeCatalogDirectory( self, lfn, recursive = False, singleFile = False, catalogs = None ):
    """ rmdir from the :FileCatalog:

    :param self: self reference
    :param mixed lfn: the directory to remove
    :param bool singleFile: execute for the first LFN only
    :param list catalogs: catalogs' names
    """
    catalogs = catalogs if catalogs else list()
    return self._fcFuncWrapper( singleFile )( lfn, "removeDirectory", argsDict = {"recursive" : recursive},
                                            catalogs = catalogs )

class CatalogLink( CatalogBase ):
  """
  .. class:: CatalogReplica

  Wrappers for various :FileCatalog: methods concering operations on links.
  """
  def __init__( self ):
    """ c'tor """
    CatalogBase.__init__( self )

  def getCatalogIsLink( self, lfn, singleFile = False, catalogs = None ):
    """ determine whether the path is registered as a link in the :FileCatalog:

    :param self: self reference
    :param mixed lfn: path to be checked (string of list of strings)
    :param bool singleFile: execute for the first LFN only
    :param list catalogs: catalogs' names
    """
    catalogs = catalogs if catalogs else list()
    return self._fcFuncWrapper( singleFile )( lfn, "isLink", catalogs = catalogs )

  def getCatalogReadLink( self, lfn, singleFile = False, catalogs = None ):
    """ get the target of a link as registered in the :FileCatalog:

    :param self: self reference
    :param mixed lfn: path to be checked (string of list of strings)
    :param bool singleFile: execute for the first LFN only
    :param list catalogs: catalogs' names
    """
    catalogs = catalogs if catalogs else list()
    return self._fcFuncWrapper( singleFile )( lfn, "readLink", catalogs = catalogs )

  def createCatalogLink( self, lfn, singleFile = False, catalogs = None ):
    """ ln in the :FileCatalog: (create the link)

    :param self: self reference
    :param mixed lfn: link dictionary containing the target lfn and link name to create
    :param bool singleFile: execute for the first LFN only
    :param list catalogs: catalogs' names
    """
    catalogs = catalogs if catalogs else list()
    return self._fcFuncWrapper( singleFile )( lfn, "createLink", catalogs = catalogs )

  def removeCatalogLink( self, lfn, singleFile = False, catalogs = None ):
    """ rm the link supplied from the :FileCatalog:

    :param self: self reference
    :param mixed lfn: link to be removed (string of list of strings)
    :param bool singleFile: execute for the first LFN only
    :param list catalogs: catalogs' names
    """
    catalogs = catalogs if catalogs else list()
    self._fcFuncWrapper( singleFile )( lfn, "removeLink", catalogs = catalogs )

class CatalogInterface( CatalogFile, CatalogReplica, CatalogDirectory, CatalogLink ):
  """
  .. class:: CatalogInterface

  Dummy class to expose all the methods of the CatalogInterface
  """
  pass

class StorageBase( object ):
  """
  .. class:: StorageBase

  This class stores the two wrapper functions for interacting with the StorageElement.
  """
  def __init__( self ):
    """ c'tor """
    self.log = gLogger.getSubLogger( self.__class__.__name__, True )

  def _callStorageElementFcnSingleFile( self, storageElementName, pfn, method, argsDict = None ):
    """ wrapper around :StorageBase._callStorageElementFcn: for single file execution

    It parses the output of :StorageBase._callStorageElementFcn: for the first pfn provided as input.
    If this pfn is found in::

    * res['Value']['Successful'] an S_OK() is returned with the value.
    * res['Value']['Failed'] an S_ERROR() is returned with the error message.

    :param self: self reference
    :param str storageElementName:  DIRAC SE name to be accessed e.g. CERN-DST
    :param mixed pfn: contains a single PFN string or a list of PFNs or dictionary containing PFNs
    :param str method: name of the :StorageElement: method to be invoked
    :param dict argsDict: additional keyword arguments that are required for the :method:
    """
    argsDict = argsDict if argsDict else {}
    # # call wrapper
    res = self._callStorageElementFcn( storageElementName, pfn, method, argsDict )
    # # check type
    if type( pfn ) == ListType:
      pfn = pfn[0]
    elif type( pfn ) == DictType:
      pfn = pfn.keys()[0]
    # # check results
    if not res["OK"]:
      return res
    elif pfn in res["Value"]["Failed"]:
      errorMessage = res["Value"]["Failed"][pfn]
      return S_ERROR( errorMessage )
    else:
      return S_OK( res["Value"]["Successful"][pfn] )

  def _callStorageElementFcn( self, storageElementName, pfn, method, argsDict = None ):
    """ a simple wrapper around the :StorageElement: functionality

    :param self: self reference
    :param str storageElementName:  DIRAC SE name to be accessed e.g. CERN-DST
    :param mixed pfn: contains a single PFN string or a list of PFNs or dictionary containing PFNs
    :param str method: name of the :StorageElement: method to be invoked
    :param dict argsDict: additional keyword arguments that are required for the :method:
    """
    argsDict = argsDict if argsDict else {}
    # # check pfn type
    if type( pfn ) in StringTypes:
      pfns = {pfn : False}
    elif type( pfn ) == ListType:
      pfns = dict.fromkeys( pfn, False )
    elif type( pfn ) == DictType:
      pfns = pfn.copy()
    else:
      errStr = "_callStorageElementFcn: Supplied pfns must be a str, list of str or dict."
      self.log.error( errStr )
      return S_ERROR( errStr )
    # # have we got some pfns?
    if not pfns:
      errMessage = "_callStorageElementFcn: No pfns supplied."
      self.log.error( errMessage )
      return S_ERROR( errMessage )
    self.log.debug( "_callStorageElementFcn: Will execute '%s' with %s pfns." % ( method, len( pfns ) ) )
    storageElement = StorageElement( storageElementName )
    res = storageElement.isValid( method )
    if not res['OK']:
      errStr = "_callStorageElementFcn: Failed to instantiate Storage Element"
      self.log.error( errStr, "for performing %s at %s." % ( method, storageElementName ) )
      return res
    # # get sybmbol
    fcFcn = getattr( storageElement, method ) if hasattr( storageElement, method ) else None
    # # make sure it is callable
    fcFcn = fcFcn if callable( fcFcn ) else None
    if not fcFcn:
      errMsg = "_callStorageElementFcn: '%s' isn't a member function in StorageElement." % method
      self.log.error( errMsg )
      return S_ERROR( errMsg )
    # # call it at least
    res = fcFcn( pfns, **argsDict )
    # # return the output
    if not res["OK"]:
      errStr = "_callStorageElementFcn: Completely failed to perform %s." % method
      self.log.error( errStr, '%s : %s' % ( storageElementName, res["Message"] ) )
    return res

  def _seFuncWrapper( self, singleFile = False ):
    """ choose wrapper to call

    :param self: self reference
    :param bool singleFile: flag to choose wrapper function, default :False: will
    execute :StorageBase._callStorageElementFcn:
    """
    return { True: self._callStorageElementFcnSingleFile,
             False: self._callStorageElementFcn }[singleFile]

  def getPfnForLfn( self, lfns, storageElementName ):
    """ get PFNs for supplied LFNs at :storageElementName: SE

    :param self: self reference
    :param list lfns: list of LFNs
    :param str stotrageElementName: DIRAC SE name
    """
    if type( lfns ) == type( '' ):
      lfns = [lfns]
    storageElement = StorageElement( storageElementName )
    res = storageElement.isValid( "getPfnForLfn" )
    if not res['OK']:
      self.log.error( "getPfnForLfn: Failed to instantiate StorageElement at %s" % storageElementName )
      return res
    retDict = { "Successful" : {}, "Failed" : {} }
    for lfn in lfns:
      res = storageElement.getPfnForLfn( lfn )
      if res["OK"] and lfn in res['Value']['Successful']:
        retDict["Successful"][lfn] = res["Value"]['Successful'][lfn]
      else:
        retDict["Failed"][lfn] = res.get( "Message", res.get( 'Value', {} ).get( 'Failed', {} ).get( lfn ) )
    return S_OK( retDict )

  def getLfnForPfn( self, pfns, storageElementName ):
    """ get LFNs for supplied PFNs at :storageElementName: SE

    :param self: self reference
    :param list lfns: list of LFNs
    :param str stotrageElementName: DIRAC SE name
    """
    storageElement = StorageElement( storageElementName )
    res = storageElement.isValid( "getPfnPath" )
    if not res['OK']:
      self.log.error( "getLfnForPfn: Failed to instantiate StorageElement at %s" % storageElementName )
      return res
    retDict = { "Successful" : {}, "Failed" : {} }
    for pfn in pfns:
      res = storageElement.getPfnPath( pfn )
      if res["OK"]:
        retDict["Successful"][pfn] = res["Value"]
      else:
        retDict["Failed"][pfn] = res["Message"]
    return S_OK( retDict )

  def getPfnForProtocol( self, pfns, storageElementName, protocol = "SRM2", withPort = True ):
    """ create PFNs strings at :storageElementName: SE using protocol :protocol:

    :param self: self reference
    :param list pfns: list of PFNs
    :param str storageElementName: DIRAC SE name
    :param str protocol: protocol name (default: 'SRM2')
    :param bool withPort: flag to include port in PFN (default: True)
    """
    storageElement = StorageElement( storageElementName )
    res = storageElement.isValid( "getPfnForProtocol" )
    if not res["OK"]:
      self.log.error( "getPfnForProtocol: Failed to instantiate StorageElement at %s" % storageElementName )
      return res
    retDict = { "Successful" : {}, "Failed" : {}}
    for pfn in pfns:
      res = Utils.executeSingleFileOrDirWrapper( storageElement.getPfnForProtocol( pfn, protocol, withPort = withPort ) )
      if res["OK"]:
        retDict["Successful"][pfn] = res["Value"]
      else:
        retDict["Failed"][pfn] = res["Message"]
    return S_OK( retDict )

class StorageFile( StorageBase ):
  """
  .. class:: StorageFile

  Wrappers for various :StorageElement: methods concering operations on files.
  """
  def __init__( self ):
    """ c'tor """
    StorageBase.__init__( self )

  def getStorageFileExists( self, physicalFile, storageElementName, singleFile = False ):
    """ determine the existance of the physical files

    :param self: self reference
    :param mixed physicalFile: string with PFN or list with PFNs or dictionary with PFNs as keys
    :param str storageElementName: DIRAC SE name
    :param bool singleFile: execute for the first PFN only
    """
    return self._seFuncWrapper( singleFile )( storageElementName, physicalFile, "exists" )

  def getStorageFileIsFile( self, physicalFile, storageElementName, singleFile = False ):
    """ determine if supplied physical paths are files

    :param self: self reference
    :param mixed physicalFile: string with PFN or list with PFNs or dictionary with PFNs as keys
    :param str storageElementName: DIRAC SE name
    :param bool singleFile: execute for the first PFN only
    """
    return self._seFuncWrapper( singleFile )( storageElementName, physicalFile, "isFile" )

  def getStorageFileSize( self, physicalFile, storageElementName, singleFile = False ):
    """ get the size of the physical files

    :param self: self reference
    :param mixed physicalFile: string with PFN or list with PFNs or dictionary with PFNs as keys
    :param str storageElementName: DIRAC SE name
    :param bool singleFile: execute for the first PFN only
    """
    return self._seFuncWrapper( singleFile )( storageElementName, physicalFile, "getFileSize" )

  def getStorageFileAccessUrl( self, physicalFile, storageElementName, protocol = None, singleFile = False ):
    """ get the access url for a physical file

    :param self: self reference
    :param mixed physicalFile: string with PFN or list with PFNs or dictionary with PFNs as keys
    :param str storageElementName: DIRAC SE name
    :param bool singleFile: execute for the first PFN only
    """
    protocol = protocol if protocol else list()
    return self._seFuncWrapper( singleFile )( storageElementName, physicalFile,
                                            "getAccessUrl", argsDict = {"protocol" : protocol} )

  def getStorageFileMetadata( self, physicalFile, storageElementName, singleFile = False ):
    """ get the metadatas for physical files

    :param self: self reference
    :param mixed physicalFile: string with PFN or list with PFNs or dictionary with PFNs as keys
    :param str storageElementName: DIRAC SE name
    :param bool singleFile: execute for the first PFN only
    """
    return self._seFuncWrapper( singleFile )( storageElementName, physicalFile, "getFileMetadata" )

  def removeStorageFile( self, physicalFile, storageElementName, singleFile = False ):
    """ rm supplied physical files from :storageElementName: DIRAC SE

    :param self: self reference
    :param mixed physicalFile: string with PFN or list with PFNs or dictionary with PFNs as keys
    :param str storageElementName: DIRAC SE name
    :param bool singleFile: execute for the first PFN only
    """
    return self._seFuncWrapper( singleFile )( storageElementName, physicalFile, "removeFile" )

  def prestageStorageFile( self, physicalFile, storageElementName, lifetime = 86400, singleFile = False ):
    """ prestage physical files

    :param self: self reference
    :param mixed physicalFile: PFNs to be prestaged
    :param str storageElement: SE name
    :param int lifetime: 24h in seconds
    :param bool singleFile: flag to prestage only one file
    """
    return self._seFuncWrapper( singleFile )( storageElementName, physicalFile,
                                              "prestageFile", argsDict = {"lifetime" : lifetime} )

  def getPrestageStorageFileStatus( self, physicalFile, storageElementName, singleFile = False ):
    """ get the status of a pre-stage request

    :param self: self reference
    :param mixed physicalFile: string with PFN or list with PFNs or dictionary with PFNs as keys
    :param str storageElementName: DIRAC SE name
    :param bool singleFile: execute for the first PFN only
    """
    return self._seFuncWrapper( singleFile )( storageElementName, physicalFile, "prestageFileStatus" )

  def pinStorageFile( self, physicalFile, storageElementName, lifetime = 86400, singleFile = False ):
    """ pin physical files with a given lifetime

    :param self: self reference
    :param mixed physicalFile: string with PFN or list with PFNs or dictionary with PFNs as keys
    :param str storageElementName: DIRAC SE name
    :param int lifetime: 24h in seconds
    :param bool singleFile: execute for the first PFN only
    """
    return self._seFuncWrapper( singleFile )( storageElementName, physicalFile,
                                              "pinFile", argsDict = {"lifetime": lifetime} )

  def releaseStorageFile( self, physicalFile, storageElementName, singleFile = False ):
    """ release the pin on physical files

    :param self: self reference
    :param mixed physicalFile: string with PFN or list with PFNs or dictionary with PFNs as keys
    :param str storageElementName: DIRAC SE name
    :param bool singleFile: execute for the first PFN only
    """
    return self._seFuncWrapper( singleFile )( storageElementName, physicalFile, "releaseFile" )

  def getStorageFile( self, physicalFile, storageElementName, localPath = False, singleFile = False ):
    """ create a local copy of a physical file

    :param self: self reference
    :param mixed physicalFile: string with PFN or list with PFNs or dictionary with PFNs as keys
    :param str storageElementName: DIRAC SE name
    :param mixed localPath: string with local paht to use or False (if False, os.getcwd() will be used)
    :param bool singleFile: execute for the first PFN only
    """
    return self._seFuncWrapper( singleFile )( storageElementName, physicalFile,
                                              "getFile", argsDict = {"localPath": localPath} )

  def putStorageFile( self, physicalFile, storageElementName, singleFile = False ):
    """ put the local file to the storage element

    :param self: self reference
    :param mixed physicalFile: dictionary with PFNs as keys
    :param str storageElementName: DIRAC SE name
    :param bool singleFile: execute for the first PFN only
    """
    return self._seFuncWrapper( singleFile )( storageElementName, physicalFile, "putFile" )

  def replicateStorageFile( self, physicalFile, size, storageElementName, singleFile = False ):
    """ replicate a physical file to a storage element

    :param self: self reference
    :param mixed physicalFile: dictionary with PFN information
    :param int size: size of PFN in bytes
    :param str storageElementName: DIRAC SE name
    :param bool singleFile: execute for the first PFN only
    """
    return self._seFuncWrapper( singleFile )( storageElementName, physicalFile,
                                              'replicateFile', argsDict = {'sourceSize': size} )

class StorageDirectory( StorageBase ):
  """
  .. class:: StorageDirectory

  Wrappers for various :StorageElement: methods concering operations on folders.
  """
  def __init__( self ):
    """ c'tor """
    StorageBase.__init__( self )

  def getStorageDirectoryIsDirectory( self, storageDirectory, storageElementName, singleDirectory = False ):
    """ determine if the storage paths are directories

    :param self: self reference
    :param mixed storageDirectory: string with PFN or list with PFNs or dictionary with PFNs as keys
    :param str storageElementName: DIRAC SE name
    :param bool singleDirectory: execute for the first PFN only
    """
    return self._seFuncWrapper( singleDirectory )( storageElementName, storageDirectory, "isDirectory" )

  def getStorageDirectoryMetadata( self, storageDirectory, storageElementName, singleDirectory = False ):
    """ get the metadata for storage directories

    :param self: self reference
    :param mixed storageDirectory: string with PFN or list with PFNs or dictionary with PFNs as keys
    :param str storageElementName: DIRAC SE name
    :param bool singleDirectory: execute for the first PFN only
    """
    return self._seFuncWrapper( singleDirectory )( storageElementName, storageDirectory, "getDirectoryMetadata" )

  def getStorageDirectorySize( self, storageDirectory, storageElementName, singleDirectory = False ):
    """ get the size of the storage directories

    :param self: self reference
    :param mixed storageDirectory: string with PFN or list with PFNs or dictionary with PFNs as keys
    :param str storageElementName: DIRAC SE name
    :param bool singleDirectory: execute for the first PFN only
    """
    return self._seFuncWrapper( singleDirectory )( storageElementName, storageDirectory, "getDirectorySize" )

  def getStorageListDirectory( self, storageDirectory, storageElementName, singleDirectory = False ):
    """ ls of a directory in the Storage Element

    :param self: self reference
    :param mixed storageDirectory: string with PFN or list with PFNs or dictionary with PFNs as keys
    :param str storageElementName: DIRAC SE name
    :param bool singleDirectory: execute for the first PFN only
    """
    return self._seFuncWrapper( singleDirectory )( storageElementName, storageDirectory, "listDirectory" )

  def getStorageDirectory( self, storageDirectory, storageElementName, localPath = False, singleDirectory = False ):
    """  copy the contents of a directory from the Storage Element to local folder

    :param self: self reference
    :param mixed storageDirectory: string with PFN or list with PFNs or dictionary with PFNs as keys
    :param mixed localPath: destination folder, if False, so.getcwd() will be used
    :param str storageElementName: DIRAC SE name
    :param bool singleDirectory: execute for the first PFN only
    """
    return self._seFuncWrapper( singleDirectory )( storageElementName, storageDirectory,
                                                   "getDirectory", argsDict = {'localPath': localPath} )

  def putStorageDirectory( self, storageDirectory, storageElementName, singleDirectory = False ):
    """ put the local directory to the storage element

    :param self: self reference
    :param mixed storageDirectory: string with PFN or list with PFNs or dictionary with PFNs as keys
    :param str storageElementName: DIRAC SE name
    :param bool singleDirectory: execute for the first PFN only
    """
    return self._seFuncWrapper( singleDirectory )( storageElementName, storageDirectory, "putDirectory" )

  def removeStorageDirectory( self, storageDirectory, storageElementName, recursive = False, singleDirectory = False ):
    """ rmdir a directory from the storage element

    :param self: self reference
    :param mixed storageDirectory: string with PFN or list with PFNs or dictionary with PFNs as keys
    :param str storageElementName: DIRAC SE name
    :param bool singleDirectory: execute for the first PFN only
    """
    return self._seFuncWrapper( singleDirectory )( storageElementName, storageDirectory,
                                                   "removeDirectory", argsDict = {"recursive": recursive} )

class StorageInterface( StorageFile, StorageDirectory ):
  """
  .. class:: StorageInterface

  Dummy class to expose all the methods of the StorageInterface
  """
  def __init__( self ):
    """ c'tor """
    StorageFile.__init__( self )
    StorageDirectory.__init__( self )
    self.log = gLogger.getSubLogger( self.__class__.__name__, True )

class CatalogToStorage( CatalogInterface, StorageInterface ):
  """
  .. class:: CatalogToStorage

  Collection of functions doing simple replica<-->Storage element operations.
  """
  def __init__( self ):
    """ c'tor """
    CatalogInterface.__init__( self )
    StorageInterface.__init__( self )
    self.log = gLogger.getSubLogger( self.__class__.__name__, True )

  def _replicaSEFcnWrapper( self, singleFile = False ):
    """ choose wrapper to call

    :param self: self reference
    :param bool singleFile: flag to choose wrapper function, default :False: will
    execute :CatalogToStorage._callReplicaSEFcn:
    """
    return { True: self._callReplicaSEFcnSingleFile,
             False: self._callReplicaSEFcn }[singleFile]

  def _callReplicaSEFcnSingleFile( self, storageElementName, lfn, method, argsDict = None ):
    """ call :method: of StorageElement :storageElementName: for single :lfn: using :argsDict: kwargs

    :param self: self reference
    :param str storageElementName: DIRAC SE name
    :param mixed lfn: LFN
    :param str method: StorageElement function name
    :param dict argsDict: kwargs of :method:
    """
    # # default value
    argsDict = argsDict if argsDict else {}
    # # get single LFN
    singleLfn = lfn
    if type( lfn ) == ListType:
      singleLfn = lfn[0]
    elif type( lfn ) == DictType:
      singleLfn = lfn.keys()[0]
    # # call method
    res = self._callReplicaSEFcn( storageElementName, singleLfn, method, argsDict )
    # # check results
    if not res["OK"]:
      return res
    elif singleLfn in res["Value"]["Failed"]:
      return S_ERROR( res["Value"]["Failed"][singleLfn] )
    return S_OK( res["Value"]["Successful"][singleLfn] )

  def _callReplicaSEFcn( self, storageElementName, lfn, method, argsDict = None ):
    """ a simple wrapper that allows replica querying then perform the StorageElement operation

    :param self: self reference
    :param str storageElementName: DIRAC SE name
    :param mixed lfn: a LFN str, list of LFNs or dict with LFNs as keys
    """
    # # default value
    argsDict = argsDict if argsDict else {}
    # # get replicas for lfn
    res = self._callFileCatalogFcn( lfn, "getReplicas" )
    if not res["OK"]:
      errStr = "_callReplicaSEFcn: Completely failed to get replicas for LFNs."
      self.log.error( errStr, res["Message"] )
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
    for lfn, replicas in lfnReplicas.items():
      if storageElementName in replicas:
        useCatalogPFN = Operations().getValue( 'DataManagement/UseCatalogPFN', True )
        if useCatalogPFN:
          pfn = replicas[storageElementName]
        else:  
          res = self.getPfnForLfn( lfn, storageElementName )
          pfn = res.get( 'Value', {} ).get( 'Successful', {} ).get( lfn, replicas[storageElementName] )
        pfnDict[pfn] = lfn
      else:
        errStr = "_callReplicaSEFcn: File hasn't got replica at supplied Storage Element."
        self.log.error( errStr, "%s %s" % ( lfn, storageElementName ) )
        retDict["Failed"][lfn] = errStr
    # # call StorageElement function at least
    res = self._callStorageElementFcn( storageElementName, pfnDict.keys(), method, argsDict )
    # # check result
    if not res["OK"]:
      errStr = "_callReplicaSEFcn: Failed to execute %s StorageElement method." % method
      self.log.error( errStr, res["Message"] )
      return res
    # # filter out failed nad successful
    for pfn, pfnRes in res["Value"]["Successful"].items():
      retDict["Successful"][pfnDict[pfn]] = pfnRes
    for pfn, errorMessage in res["Value"]["Failed"].items():
      retDict["Failed"][pfnDict[pfn]] = errorMessage
    return S_OK( retDict )

  def getReplicaIsFile( self, lfn, storageElementName, singleFile = False ):
    """ determine whether the supplied lfns are files at the supplied StorageElement

    :param self: self reference
    :param mixed lfn: LFN string, list if LFNs or dict with LFNs as keys
    :param str storageElementName: DIRAC SE name
    :param bool singleFile: execute for the first LFN only
    """
    return self._replicaSEFcnWrapper( singleFile )( storageElementName, lfn, "isFile" )

  def getReplicaSize( self, lfn, storageElementName, singleFile = False ):
    """ get the size of files for the lfns at the supplied StorageElement

    :param self: self reference
    :param mixed lfn: LFN string, list if LFNs or dict with LFNs as keys
    :param str storageElementName: DIRAC SE name
    :param bool singleFile: execute for the first LFN only
    """
    return self._replicaSEFcnWrapper( singleFile )( storageElementName, lfn, "getFileSize" )

  def getReplicaAccessUrl( self, lfn, storageElementName, singleFile = False ):
    """ get the access url for lfns at the supplied StorageElement

    :param self: self reference
    :param mixed lfn: LFN string, list if LFNs or dict with LFNs as keys
    :param str storageElementName: DIRAC SE name
    :param bool singleFile: execute for the first LFN only
    """
    return self._replicaSEFcnWrapper( singleFile )( storageElementName, lfn, "getAccessUrl" )

  def getReplicaMetadata( self, lfn, storageElementName, singleFile = False ):
    """ get the file metadata for lfns at the supplied StorageElement

    :param self: self reference
    :param mixed lfn: LFN string, list if LFNs or dict with LFNs as keys
    :param str storageElementName: DIRAC SE name
    :param bool singleFile: execute for the first LFN only
    """
    return self._replicaSEFcnWrapper( singleFile )( storageElementName, lfn, "getFileMetadata" )

  def prestageReplica( self, lfn, storageElementName, lifetime = 86400, singleFile = False ):
    """ issue a prestage requests for the lfns at the supplied StorageElement

    :param self: self reference
    :param mixed lfn: LFN string, list if LFNs or dict with LFNs as keys
    :param str storageElementName: DIRAC SE name
    :param int lifetime: 24h in seconds
    :param bool singleFile: execute for the first LFN only
    """
    return self._replicaSEFcnWrapper( singleFile )( storageElementName, lfn,
                                                  "prestageFile", argsDict = {"lifetime": lifetime} )

  def getPrestageReplicaStatus( self, lfn, storageElementName, singleFile = False ):
    """ This functionality is not supported.

    Then what is it doing here? Not supported -> delete it!
    """
    return S_ERROR( "Not supported functionality. Please use getReplicaMetadata and check the 'Cached' element." )

  def pinReplica( self, lfn, storageElementName, lifetime = 86400, singleFile = False ):
    """ pin the lfns at the supplied StorageElement

    :param self: self reference
    :param mixed lfn: LFN string, list if LFNs or dict with LFNs as keys
    :param str storageElementName: DIRAC SE name
    :param int lifetime: 24h in seconds
    :param bool singleFile: execute for the first LFN only
    """
    return self._replicaSEFcnWrapper( singleFile )( storageElementName, lfn,
                                                  "pinFile", argsDict = {"lifetime": lifetime} )

  def releaseReplica( self, lfn, storageElementName, singleFile = False ):
    """ release pins for the lfns at the supplied StorageElement

    :param self: self reference
    :param mixed lfn: LFN string, list if LFNs or dict with LFNs as keys
    :param str storageElementName: DIRAC SE name
    :param bool singleFile: execute for the first LFN only
    """
    return self._replicaSEFcnWrapper( singleFile )( storageElementName, lfn, "releaseFile" )

  def getReplica( self, lfn, storageElementName, localPath = False, singleFile = False ):
    """ copy replicas from DIRAC SE to local directory

    :param self: self reference
    :param mixed lfn: LFN string, list if LFNs or dict with LFNs as keys
    :param str storageElementName: DIRAC SE name
    :param mixed localPath: path in the local file system, if False, os.getcwd() will be used
    :param bool singleFile: execute for the first LFN only
    """
    return self._replicaSEFcnWrapper( singleFile )( storageElementName, lfn,
                                                  "getFile", argsDict = {"localPath": localPath} )

class ReplicaManager( CatalogToStorage ):
  """
  .. class:: ReplicaManager

  A ReplicaManager is putting all possible StorageElement and FileCatalog functionalities togehter.
  """
  def __init__( self ):
    """ c'tor

    :param self: self reference
    """
    CatalogToStorage.__init__( self )
    self.fileCatalogue = FileCatalog()
    self.accountingClient = None
    self.registrationProtocol = ['SRM2', 'DIP']
    self.thirdPartyProtocols = ['SRM2', 'DIP']
    self.resourceStatus = ResourceStatus()
    self.ignoreMissingInFC = Operations().getValue( 'DataManagement/IgnoreMissingInFC', False )

  def setAccountingClient( self, client ):
    """ Set Accounting Client instance
    """
    self.accountingClient = client

  def __verifyOperationPermission( self, path ):
    """  Check if we have write permission to the given directory
    """
    if type( path ) in StringTypes:
      paths = [ path ]
    else:
      paths = path
    fc = FileCatalog()
    res = fc.getPathPermissions( paths )
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
        self.log.error( "Failed to clean directory.", "%s %s" % ( folder, res['Message'] ) )
        retDict["Failed"][folder] = res['Message']
      else:
        self.log.info( "Successfully removed directory.", folder )
        retDict["Successful"][folder] = res['Value']
    return S_OK( retDict )

  def __cleanDirectory( self, folder ):
    """ delete all files from directory :folder: in FileCatalog and StorageElement

    :param self: self reference
    :param str folder: directory name
    """
    res = self.__verifyOperationPermission( folder )
    if not res['OK']:
      return res
    if not res['Value']:
      errStr = "__cleanDirectory: Write access not permitted for this credential."
      self.log.error( errStr, folder )
      return S_ERROR( errStr )
    res = self.__getCatalogDirectoryContents( [ folder ] )
    if not res['OK']:
      return res
    res = self.removeFile( res['Value'].keys() + [ '%s/dirac_directory' % folder ] )
    if not res['OK']:
      return res
    for lfn, reason in res['Value']['Failed'].items():
      self.log.error( "Failed to remove file found in the catalog", "%s %s" % ( lfn, reason ) )
    storageElements = gConfig.getValue( 'Resources/StorageElementGroups/SE_Cleaning_List', [] )
    failed = False
    for storageElement in sorted( storageElements ):
      res = self.__removeStorageDirectory( folder, storageElement )
      if not res['OK']:
        failed = True
    if failed:
      return S_ERROR( "Failed to clean storage directory at all SEs" )
    res = self.removeCatalogDirectory( folder, recursive = True, singleFile = True )
    if not res['OK']:
      return res
    return S_OK()

  def __removeStorageDirectory( self, directory, storageElement ):
    """ delete SE directory

    :param self: self reference
    :param str directory: folder to be removed
    :param str storageElement: DIRAC SE name
    """
    self.log.info( 'Removing the contents of %s at %s' % ( directory, storageElement ) )
    res = self.getPfnForLfn( [directory], storageElement )
    if not res['OK']:
      self.log.error( "Failed to get PFN for directory", res['Message'] )
      return res
    for directory, error in res['Value']['Failed'].items():
      self.log.error( 'Failed to obtain directory PFN from LFN', '%s %s' % ( directory, error ) )
    if res['Value']['Failed']:
      return S_ERROR( 'Failed to obtain directory PFN from LFNs' )
    storageDirectory = res['Value']['Successful'].values()[0]
    res = self.getStorageFileExists( storageDirectory, storageElement, singleFile = True )
    if not res['OK']:
      self.log.error( "Failed to obtain existance of directory", res['Message'] )
      return res
    exists = res['Value']
    if not exists:
      self.log.info( "The directory %s does not exist at %s " % ( directory, storageElement ) )
      return S_OK()
    res = self.removeStorageDirectory( storageDirectory, storageElement, recursive = True, singleDirectory = True )
    if not res['OK']:
      self.log.error( "Failed to remove storage directory", res['Message'] )
      return res
    self.log.info( "Successfully removed %d files from %s at %s" % ( res['Value']['FilesRemoved'],
                                                                    directory,
                                                                    storageElement ) )
    return S_OK()

  def __getCatalogDirectoryContents( self, directories ):
    """ ls recursively all files in directories

    :param self: self reference
    :param list directories: folder names
    """
    self.log.info( 'Obtaining the catalog contents for %d directories:' % len( directories ) )
    for directory in directories:
      self.log.info( directory )
    activeDirs = directories
    allFiles = {}
    while len( activeDirs ) > 0:
      currentDir = activeDirs[0]
      res = self.getCatalogListDirectory( currentDir, singleFile = True )
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
    self.log.info( "Obtaining the files older than %d days in %d directories:" % ( days, len( directories ) ) )
    for folder in directories:
      self.log.info( folder )
    activeDirs = directories
    allFiles = []
    while len( activeDirs ) > 0:
      currentDir = activeDirs[0]
      # We only need the metadata (verbose) if a limit date is given
      res = self.getCatalogListDirectory( currentDir, verbose = ( days != 0 ), singleFile = True )
      activeDirs.remove( currentDir )
      if not res['OK']:
        self.log.error( "Error retrieving directory contents", "%s %s" % ( currentDir, res['Message'] ) )
      else:
        dirContents = res['Value']
        subdirs = dirContents['SubDirs']
        files = dirContents['Files']
        self.log.info( "%s: %d files, %d sub-directories" % ( currentDir, len( files ), len( subdirs ) ) )
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
      self.log.error( errStr )
      return S_ERROR( errStr )
    self.log.verbose( "getFile: Attempting to get %s files." % len( lfns ) )
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
    for lfn in fileMetadata:
      res = self.__getFile( lfn, lfnReplicas[lfn], fileMetadata[lfn], destinationDir )
      if not res['OK']:
        failed[lfn] = res['Message']
      else:
        successful[lfn] = res['Value']
    return S_OK( { 'Successful': successful, 'Failed' : failed } )

  def __getFile( self, lfn, replicas, metadata, destinationDir ):
    if not replicas:
      self.log.error( "No accessible replicas found" )
      return S_ERROR( "No accessible replicas found" )
    # Determine the best replicas
    res = self._getSEProximity( replicas.keys() )
    if not res['OK']:
      return res
    for storageElementName in res['Value']:
      physicalFile = replicas[storageElementName]
      # print '__getFile', physicalFile, replicas[storageElementName]
      res = self.getStorageFile( physicalFile,
                                 storageElementName,
                                 localPath = os.path.realpath( destinationDir ),
                                 singleFile = True )
      if not res['OK']:
        self.log.error( "Failed to get %s from %s" % ( lfn, storageElementName ), res['Message'] )
      else:
        localFile = os.path.realpath( os.path.join( destinationDir, os.path.basename( lfn ) ) )
        localAdler = fileAdler( localFile )
        if ( metadata['Size'] != res['Value'] ):
          self.log.error( "Size of downloaded file (%d) does not match catalog (%d)" % ( res['Value'],
                                                                                        metadata['Size'] ) )
        elif ( metadata['Checksum'] ) and ( not compareAdler( metadata['Checksum'], localAdler ) ):
          self.log.error( "Checksum of downloaded file (%s) does not match catalog (%s)" % ( localAdler,
                                                                                            metadata['Checksum'] ) )
        else:
          return S_OK( localFile )
    self.log.error( "getFile: Failed to get local copy from any replicas.", lfn )
    return S_ERROR( "ReplicaManager.getFile: Failed to get local copy from any replicas." )

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

  def putAndRegister( self, lfn, fileName, diracSE, guid = None, path = None, checksum = None, catalog = None, ancestors = None ):
    """ Put a local file to a Storage Element and register in the File Catalogues

        'lfn' is the file LFN
        'file' is the full path to the local file
        'diracSE' is the Storage Element to which to put the file
        'guid' is the guid with which the file is to be registered (if not provided will be generated)
        'path' is the path on the storage where the file will be put (if not provided the LFN will be used)
    """
    ancestors = ancestors if ancestors else list()
    res = self.__verifyOperationPermission( os.path.dirname( lfn ) )
    if not res['OK']:
      return res
    if not res['Value']:
      errStr = "putAndRegister: Write access not permitted for this credential."
      self.log.error( errStr, lfn )
      return S_ERROR( errStr )
    # Instantiate the desired file catalog
    if catalog:
      self.fileCatalogue = FileCatalog( catalog )
      if not self.fileCatalogue.isOK():
        return S_ERROR( "Can't get FileCatalog %s" % catalog )
    else:
      self.fileCatalogue = FileCatalog()
    # Check that the local file exists
    if not os.path.exists( fileName ):
      errStr = "putAndRegister: Supplied file does not exist."
      self.log.error( errStr, fileName )
      return S_ERROR( errStr )
    # If the path is not provided then use the LFN path
    if not path:
      path = os.path.dirname( lfn )
    # Obtain the size of the local file
    size = getSize( fileName )
    if size == 0:
      errStr = "putAndRegister: Supplied file is zero size."
      self.log.error( errStr, fileName )
      return S_ERROR( errStr )
    # If the GUID is not given, generate it here
    if not guid:
      guid = makeGuid( fileName )
    if not checksum:
      self.log.info( "putAndRegister: Checksum information not provided. Calculating adler32." )
      checksum = fileAdler( fileName )
      self.log.info( "putAndRegister: Checksum calculated to be %s." % checksum )
    res = self.fileCatalogue.exists( {lfn:guid} )
    if not res['OK']:
      errStr = "putAndRegister: Completely failed to determine existence of destination LFN."
      self.log.error( errStr, lfn )
      return res
    if lfn not in res['Value']['Successful']:
      errStr = "putAndRegister: Failed to determine existence of destination LFN."
      self.log.error( errStr, lfn )
      return S_ERROR( errStr )
    if res['Value']['Successful'][lfn]:
      if res['Value']['Successful'][lfn] == lfn:
        errStr = "putAndRegister: The supplied LFN already exists in the File Catalog."
        self.log.error( errStr, lfn )
      else:
        errStr = "putAndRegister: This file GUID already exists for another file. " \
            "Please remove it and try again."
        self.log.error( errStr, res['Value']['Successful'][lfn] )
      return S_ERROR( "%s %s" % ( errStr, res['Value']['Successful'][lfn] ) )

    ##########################################################
    #  Instantiate the destination storage element here.
    storageElement = StorageElement( diracSE )
    res = storageElement.isValid()
    if not res['OK']:
      errStr = "putAndRegister: The storage element is not currently valid."
      self.log.error( errStr, "%s %s" % ( diracSE, res['Message'] ) )
      return S_ERROR( errStr )
    destinationSE = storageElement.getStorageElementName()['Value']
    res = storageElement.getPfnForLfn( lfn )
    if not res['OK'] or lfn not in res['Value']['Successful']:
      errStr = "putAndRegister: Failed to generate destination PFN."
      self.log.error( errStr, res.get( 'Message', res.get( 'Value', {} ).get( 'Failed', {} ).get( lfn ) ) )
      return S_ERROR( errStr )
    destPfn = res['Value']['Successful'][lfn]
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
      self.log.info( 'putAndRegister: Sending accounting took %.1f seconds' % ( time.time() - startTime ) )
      self.log.error( errStr, "%s: %s" % ( fileName, res['Message'] ) )
      return S_ERROR( "%s %s" % ( errStr, res['Message'] ) )
    successful[lfn] = {'put': putTime}

    ###########################################################
    # Perform the registration here
    oDataOperation.setValueByKey( 'RegistrationTotal', 1 )
    fileTuple = ( lfn, destPfn, size, destinationSE, guid, checksum )
    registerDict = {'LFN':lfn, 'PFN':destPfn, 'Size':size, 'TargetSE':destinationSE, 'GUID':guid, 'Addler':checksum}
    startTime = time.time()
    res = self.registerFile( fileTuple, catalog = catalog )
    registerTime = time.time() - startTime
    oDataOperation.setValueByKey( 'RegistrationTime', registerTime )
    if not res['OK']:
      errStr = "putAndRegister: Completely failed to register file."
      self.log.error( errStr, res['Message'] )
      failed[lfn] = { 'register' : registerDict }
      oDataOperation.setValueByKey( 'FinalStatus', 'Failed' )
    elif lfn in res['Value']['Failed']:
      errStr = "putAndRegister: Failed to register file."
      self.log.error( errStr, "%s %s" % ( lfn, res['Value']['Failed'][lfn] ) )
      oDataOperation.setValueByKey( 'FinalStatus', 'Failed' )
      failed[lfn] = { 'register' : registerDict }
    else:
      successful[lfn]['register'] = registerTime
      oDataOperation.setValueByKey( 'RegistrationOK', 1 )
    oDataOperation.setEndTime()
    gDataStoreClient.addRegister( oDataOperation )
    startTime = time.time()
    gDataStoreClient.commit()
    self.log.info( 'putAndRegister: Sending accounting took %.1f seconds' % ( time.time() - startTime ) )
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
    self.log.verbose( "replicateAndRegister: Attempting to replicate %s to %s." % ( lfn, destSE ) )
    startReplication = time.time()
    res = self.__replicate( lfn, destSE, sourceSE, destPath, localCache )
    replicationTime = time.time() - startReplication
    if not res['OK']:
      errStr = "ReplicaManager.replicateAndRegister: Completely failed to replicate file."
      self.log.error( errStr, res['Message'] )
      return S_ERROR( errStr )
    if not res['Value']:
      # The file was already present at the destination SE
      self.log.info( "replicateAndRegister: %s already present at %s." % ( lfn, destSE ) )
      successful[lfn] = { 'replicate' : 0, 'register' : 0 }
      resDict = { 'Successful' : successful, 'Failed' : failed }
      return S_OK( resDict )
    successful[lfn] = { 'replicate' : replicationTime }

    destPfn = res['Value']['DestPfn']
    destSE = res['Value']['DestSE']
    self.log.verbose( "replicateAndRegister: Attempting to register %s at %s." % ( destPfn, destSE ) )
    replicaTuple = ( lfn, destPfn, destSE )
    startRegistration = time.time()
    res = self.registerReplica( replicaTuple, catalog = catalog )
    registrationTime = time.time() - startRegistration
    if not res['OK']:
      # Need to return to the client that the file was replicated but not registered
      errStr = "replicateAndRegister: Completely failed to register replica."
      self.log.error( errStr, res['Message'] )
      failed[lfn] = { 'Registration' : { 'LFN' : lfn, 'TargetSE' : destSE, 'PFN' : destPfn } }
    else:
      if lfn in res['Value']['Successful']:
        self.log.info( "replicateAndRegister: Successfully registered replica." )
        successful[lfn]['register'] = registrationTime
      else:
        errStr = "replicateAndRegister: Failed to register replica."
        self.log.info( errStr, res['Value']['Failed'][lfn] )
        failed[lfn] = { 'Registration' : { 'LFN' : lfn, 'TargetSE' : destSE, 'PFN' : destPfn } }
    return S_OK( {'Successful': successful, 'Failed': failed} )

  def replicate( self, lfn, destSE, sourceSE = '', destPath = '', localCache = '' ):
    """ Replicate a LFN to a destination SE without registering the replica.

        'lfn' is the LFN to be replicated
        'destSE' is the Storage Element the file should be replicated to
        'sourceSE' is the source for the file replication (where not specified all replicas will be attempted)
        'destPath' is the path on the destination storage element, if to be different from LHCb convention
        'localCache' is the local file system location to be used as a temporary cache
    """
    self.log.verbose( "replicate: Attempting to replicate %s to %s." % ( lfn, destSE ) )
    res = self.__replicate( lfn, destSE, sourceSE, destPath, localCache )
    if not res['OK']:
      errStr = "replicate: Replication failed."
      self.log.error( errStr, "%s %s" % ( lfn, destSE ) )
      return res
    if not res['Value']:
      # The file was already present at the destination SE
      self.log.info( "replicate: %s already present at %s." % ( lfn, destSE ) )
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
      errStr = "__replicate: Write access not permitted for this credential."
      self.log.error( errStr, lfn )
      return S_ERROR( errStr )

    self.log.verbose( "__replicate: Performing replication initialization." )
    res = self.__initializeReplication( lfn, sourceSE, destSE )
    if not res['OK']:
      self.log.error( "__replicate: Replication initialisation failed.", lfn )
      return res
    destStorageElement = res['Value']['DestStorage']
    lfnReplicas = res['Value']['Replicas']
    destSE = res['Value']['DestSE']
    catalogueSize = res['Value']['CatalogueSize']
    ###########################################################
    # If the LFN already exists at the destination we have nothing to do
    if destSE in lfnReplicas:
      self.log.info( "__replicate: LFN is already registered at %s." % destSE )
      return S_OK()
    ###########################################################
    # Resolve the best source storage elements for replication
    self.log.verbose( "__replicate: Determining the best source replicas." )
    res = self.__resolveBestReplicas( lfn, sourceSE, lfnReplicas, catalogueSize )
    if not res['OK']:
      self.log.error( "__replicate: Best replica resolution failed.", lfn )
      return res
    replicaPreference = res['Value']
    ###########################################################
    # Now perform the replication for the file
    if destPath:
      destPath = '%s/%s' % ( destPath, os.path.basename( lfn ) )
    else:
      destPath = lfn
    res = destStorageElement.getPfnForLfn( destPath )
    if not res['OK'] or destPath not in res['Value']['Successful']:
      errStr = "__replicate: Failed to generate destination PFN."
      self.log.error( errStr, res.get( 'Message', res.get( 'Value', {} ).get( 'Failed', {} ).get( destPath ) ) )
      return S_ERROR( errStr )
    destPfn = res['Value']['Successful'][destPath]
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
      self.log.verbose( "__replicate: Attempting replication from %s to %s." % ( sourceSE, destSE ) )
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
        self.getFile( lfn, localDir )
        localFile = os.path.join( localDir, os.path.basename( lfn ) )
        fileDict = {destPfn:localFile}

      res = destStorageElement.replicateFile( fileDict, catalogueSize, singleFile = True )
      if localFile and os.path.exists( localFile ):
        os.remove( localFile )

      if res['OK']:
        self.log.info( "__replicate: Replication successful." )
        resDict = {'DestSE':destSE, 'DestPfn':destPfn}
        return S_OK( resDict )
      else:
        errStr = "__replicate: Replication failed."
        self.log.error( errStr, "%s from %s to %s." % ( lfn, sourceSE, destSE ) )
    ##########################################################
    # If the replication failed for all sources give up
    errStr = "__replicate: Failed to replicate with all sources."
    self.log.error( errStr, lfn )
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
    self.log.verbose( "%s Verifying dest StorageElement validity (%s)." % ( logStr, destSE ) )

    destStorageElement = StorageElement( destSE )
    res = destStorageElement.isValid()
    if not res['OK']:
      errStr = "%s The storage element is not currently valid." % logStr
      self.log.error( errStr, "%s %s" % ( destSE, res['Message'] ) )
      return S_ERROR( errStr )
    destSE = destStorageElement.getStorageElementName()['Value']
    self.log.info( "%s Destination Storage Element verified." % logStr )

    ###########################################################
    # Check whether the destination storage element is banned
    self.log.verbose( "%s Determining whether %s ( destination ) is Write-banned." % ( logStr, destSE ) )

    if not self.__SEActive( destSE ).get( 'Value', {} ).get( 'Write' ):
      infoStr = "%s Supplied destination Storage Element is not currently allowed for Write." % ( logStr )
      self.log.info( infoStr, destSE )
      return S_ERROR( infoStr )

    ###########################################################
    # Get the LFN replicas from the file catalogue
    self.log.verbose( "%s Attempting to obtain replicas for %s." % ( logStr, lfn ) )

    res = self.getReplicas( lfn )
    if not res[ 'OK' ]:
      errStr = "%s Completely failed to get replicas for LFN." % logStr
      self.log.error( errStr, "%s %s" % ( lfn, res['Message'] ) )
      return res
    if lfn not in res['Value']['Successful']:
      errStr = "%s Failed to get replicas for LFN." % logStr
      self.log.error( errStr, "%s %s" % ( lfn, res['Value']['Failed'][lfn] ) )
      return S_ERROR( "%s %s" % ( errStr, res['Value']['Failed'][lfn] ) )
    self.log.info( "%s Successfully obtained replicas for LFN." % logStr )
    lfnReplicas = res['Value']['Successful'][lfn]

    ###########################################################
    # Check the file is at the sourceSE
    self.log.verbose( "%s: Determining whether source Storage Element is sane." % logStr )

    if sourceSE and sourceSE not in lfnReplicas:
      errStr = "%s LFN does not exist at supplied source SE." % logStr
      self.log.error( errStr, "%s %s" % ( lfn, sourceSE ) )
      return S_ERROR( errStr )

    ###########################################################
    # If the file catalogue size is zero fail the transfer
    self.log.verbose( "%s Attempting to obtain size for %s." % ( logStr, lfn ) )

    res = self.getFileSize( lfn )
    if not res['OK']:
      errStr = "%s Completely failed to get size for LFN." % logStr
      self.log.error( errStr, "%s %s" % ( lfn, res['Message'] ) )
      return res
    if lfn not in res['Value']['Successful']:
      errStr = "%s Failed to get size for LFN." % logStr
      self.log.error( errStr, "%s %s" % ( lfn, res['Value']['Failed'][lfn] ) )
      return S_ERROR( "%s %s" % ( errStr, res['Value']['Failed'][lfn] ) )
    catalogueSize = res['Value']['Successful'][lfn]
    if catalogueSize == 0:
      errStr = "%s Registered file size is 0." % logStr
      self.log.error( errStr, lfn )
      return S_ERROR( errStr )
    self.log.info( "%s File size determined to be %s." % ( logStr, catalogueSize ) )

    ###########################################################
    # Check whether the destination storage element is banned

    self.log.verbose( "%s Determining whether %s ( destination ) is Write-banned." % ( logStr, destSE ) )

    usableDestSE = self.resourceStatus.isUsableStorage( destSE, 'WriteAccess' )
    if not usableDestSE:
      infoStr = "%s Destination Storage Element is currently unusable for Write" % logStr
      self.log.info( infoStr, destSE )
      return S_ERROR( infoStr )

    self.log.info( "%s Destination site not banned for Write." % logStr )

    ###########################################################
    # Check whether the supplied source SE is sane

    self.log.verbose( "%s: Determining whether source Storage Element is sane." % logStr )

    if sourceSE:

      usableSourceSE = self.resourceStatus.isUsableStorage( sourceSE, 'ReadAccess' )

      if sourceSE not in lfnReplicas:
        errStr = "%s LFN does not exist at supplied source SE." % logStr
        self.log.error( errStr, "%s %s" % ( lfn, sourceSE ) )
        return S_ERROR( errStr )
      elif not usableSourceSE:
        infoStr = "%s Supplied source Storage Element is currently unusable for Read." % logStr
        self.log.info( infoStr, sourceSE )
        return S_ERROR( infoStr )

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
        self.log.info( "%s %s replica not requested." % ( logStr, diracSE ) )
        continue

      usableDiracSE = self.resourceStatus.isUsableStorage( diracSE, 'ReadAccess' )

      if not usableDiracSE:
        self.log.info( "%s %s is currently unusable as a source." % ( logStr, diracSE ) )

      # elif diracSE in bannedSources:
      #  self.log.info( "__resolveBestReplicas: %s is currently banned as a source." % diracSE )
      else:
        self.log.info( "%s %s is available for use." % ( logStr, diracSE ) )
        storageElement = StorageElement( diracSE )
        res = storageElement.isValid()
        if not res['OK']:
          errStr = "%s The storage element is not currently valid." % logStr
          self.log.error( errStr, "%s %s" % ( diracSE, res['Message'] ) )
        else:
          #useCatalogPFN = Operations().getValue( 'DataManagement/UseCatalogPFN', True )
          #if not useCatalogPFN:
          #  pfn = storageElement.getPfnForLfn( lfn ).get( 'Value', pfn )
          if storageElement.getRemoteProtocols()['Value']:
            self.log.verbose( "%s Attempting to get source pfns for remote protocols." % logStr )
            res = Utils.executeSingleFileOrDirWrapper( storageElement.getPfnForProtocol( pfn, self.thirdPartyProtocols ) )
            if res['OK']:
              sourcePfn = res['Value']
              self.log.verbose( "%s Attempting to get source file size." % logStr )
              res = storageElement.getFileSize( sourcePfn )
              if res['OK']:
                if sourcePfn in res['Value']['Successful']:
                  sourceFileSize = res['Value']['Successful'][sourcePfn]
                  self.log.info( "%s Source file size determined to be %s." % ( logStr, sourceFileSize ) )
                  if catalogueSize == sourceFileSize:
                    fileTuple = ( diracSE, sourcePfn )
                    replicaPreference.append( fileTuple )
                  else:
                    errStr = "%s Catalogue size and physical file size mismatch." % logStr
                    self.log.error( errStr, "%s %s" % ( diracSE, sourcePfn ) )
                else:
                  errStr = "%s Failed to get physical file size." % logStr
                  self.log.error( errStr, "%s %s: %s" % ( sourcePfn, diracSE, res['Value']['Failed'][sourcePfn] ) )
              else:
                errStr = "%s Completely failed to get physical file size." % logStr
                self.log.error( errStr, "%s %s: %s" % ( sourcePfn, diracSE, res['Message'] ) )
            else:
              errStr = "%s Failed to get PFN for replication for StorageElement." % logStr
              self.log.error( errStr, "%s %s" % ( diracSE, res['Message'] ) )
          else:
            errStr = "%s Source Storage Element has no remote protocols." % logStr
            self.log.info( errStr, diracSE )

    if not replicaPreference:
      errStr = "%s Failed to find any valid source Storage Elements." % logStr
      self.log.error( errStr )
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
      self.log.error( errStr )
      return S_ERROR( errStr )
    self.log.verbose( "registerFile: Attempting to register %s files." % len( fileTuples ) )
    res = self.__registerFile( fileTuples, catalog )
    if not res['OK']:
      errStr = "registerFile: Completely failed to register files."
      self.log.error( errStr, res['Message'] )
      return S_ERROR( errStr )
    return res

  def __registerFile( self, fileTuples, catalog ):
    """ register file to cataloge """
    seDict = {}
    fileDict = {}
    for lfn, physicalFile, fileSize, storageElementName, fileGuid, checksum in fileTuples:
      if storageElementName:
        seDict.setdefault( storageElementName, [] ).append( ( lfn, physicalFile, fileSize, storageElementName, fileGuid, checksum ) )
      else:
        # If no SE name, this could be just registration in a dummy catalog like LHCb bookkeeping
        fileDict[lfn] = {'PFN':'', 'Size':fileSize, 'SE':storageElementName, 'GUID':fileGuid, 'Checksum':checksum}
    failed = {}
    for storageElementName, fileTuple in seDict.items():
      destStorageElement = StorageElement( storageElementName )
      res = destStorageElement.isValid()
      if not res['OK']:
        errStr = "__registerFile: The storage element is not currently valid."
        self.log.error( errStr, "%s %s" % ( storageElementName, res['Message'] ) )
        for lfn, physicalFile, fileSize, storageElementName, fileGuid, checksum in fileTuple:
          failed[lfn] = errStr
      else:
        storageElementName = destStorageElement.getStorageElementName()['Value']
        for lfn, physicalFile, fileSize, storageElementName, fileGuid, checksum in fileTuple:
          res = Utils.executeSingleFileOrDirWrapper( destStorageElement.getPfnForProtocol( physicalFile, self.registrationProtocol, withPort = False ) )
          if not res['OK']:
            pfn = physicalFile
          else:
            pfn = res['Value']
          # tuple = ( lfn, pfn, fileSize, storageElementName, fileGuid, checksum )
          fileDict[lfn] = {'PFN':pfn, 'Size':fileSize, 'SE':storageElementName, 'GUID':fileGuid, 'Checksum':checksum}
    self.log.verbose( "__registerFile: Resolved %s files for registration." % len( fileDict ) )
    if catalog:
      fileCatalog = FileCatalog( catalog )
      if not fileCatalog.isOK():
        return S_ERROR( "Can't get FileCatalog %s" % catalog )
      res = fileCatalog.addFile( fileDict )
    else:
      res = self.fileCatalogue.addFile( fileDict )
    if not res['OK']:
      errStr = "__registerFile: Completely failed to register files."
      self.log.error( errStr, res['Message'] )
      return S_ERROR( errStr )
    failed.update( res['Value']['Failed'] )
    successful = res['Value']['Successful']
    resDict = {'Successful':successful, 'Failed':failed}
    return S_OK( resDict )

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
      self.log.error( errStr )
      return S_ERROR( errStr )
    self.log.verbose( "registerReplica: Attempting to register %s replicas." % len( replicaTuples ) )
    res = self.__registerReplica( replicaTuples, catalog )
    if not res['OK']:
      errStr = "registerReplica: Completely failed to register replicas."
      self.log.error( errStr, res['Message'] )
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
        self.log.error( errStr, "%s %s" % ( storageElementName, res['Message'] ) )
        for lfn, pfn in replicaTuple:
          failed[lfn] = errStr
      else:
        storageElementName = destStorageElement.getStorageElementName()['Value']
        for lfn, pfn in replicaTuple:
          res = Utils.executeSingleFileOrDirWrapper( destStorageElement.getPfnForProtocol( pfn, self.registrationProtocol, withPort = False ) )
          if not res['OK']:
            failed[lfn] = res['Message']
          else:
            replicaTuple = ( lfn, res['Value'], storageElementName, False )
            replicaTuples.append( replicaTuple )
    self.log.verbose( "__registerReplica: Successfully resolved %s replicas for registration." % len( replicaTuples ) )
    # HACK!
    replicaDict = {}
    for lfn, pfn, se, _master in replicaTuples:
      replicaDict[lfn] = {'SE':se, 'PFN':pfn}

    if catalog:
      fileCatalog = FileCatalog( catalog )
      res = fileCatalog.addReplica( replicaDict )
    else:
      res = self.fileCatalogue.addReplica( replicaDict )
    if not res['OK']:
      errStr = "__registerReplica: Completely failed to register replicas."
      self.log.error( errStr, res['Message'] )
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
    if force is None:
      force = self.ignoreMissingInFC
    if type( lfn ) == ListType:
      lfns = lfn
    elif type( lfn ) == StringType:
      lfns = [lfn]
    else:
      errStr = "removeFile: Supplied lfns must be string or list of strings."
      self.log.error( errStr )
      return S_ERROR( errStr )
    # First check if the file exists in the FC
    res = self.fileCatalogue.exists( lfns )
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
      res = self.__verifyOperationPermission( lfns )
      if not res['OK']:
        return res
      if not res['Value']:
        errStr = "removeFile: Write access not permitted for this credential."
        self.log.error( errStr, lfns )
        return S_ERROR( errStr )

      self.log.verbose( "removeFile: Attempting to remove %s files from Storage and Catalogue. Get replicas first" % len( lfns ) )
      res = self.fileCatalogue.getReplicas( lfns, True )
      if not res['OK']:
        errStr = "ReplicaManager.removeFile: Completely failed to get replicas for lfns."
        self.log.error( errStr, res['Message'] )
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
        self.log.error( errStr, res['Message'] )
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
      res = self.fileCatalogue.removeFile( completelyRemovedFiles )
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
      self.log.error( errStr )
      return S_ERROR( errStr )
    # Check that we have write permissions to this directory.
    res = self.__verifyOperationPermission( lfns )
    if not res['OK']:
      return res
    if not res['Value']:
      errStr = "removaReplica: Write access not permitted for this credential."
      self.log.error( errStr, lfns )
      return S_ERROR( errStr )
    self.log.verbose( "removeReplica: Will remove catalogue entry for %s lfns at %s." % ( len( lfns ),
                                                                                          storageElementName ) )
    res = self.fileCatalogue.getReplicas( lfns, True )
    if not res['OK']:
      errStr = "removeReplica: Completely failed to get replicas for lfns."
      self.log.error( errStr, res['Message'] )
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
        self.log.error( "The replica you are trying to remove is the only one.", "%s @ %s" % ( lfn,
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
    for lfn, pfn in fileTuple:
      res = self.__verifyOperationPermission( lfn )
      if not res['OK'] or not res['Value']:
        errStr = "__removeReplica: Write access not permitted for this credential."
        self.log.error( errStr, lfn )
        failed[lfn] = errStr
      else:
        # This is the PFN as in hte FC
        lfnDict[lfn] = pfn
    # Now we should use the constructed PFNs if needed, for the physical removal
    # Reverse lfnDict into pfnDict with required PFN
    if self.useCatalogPFN:
      pfnsDict = dict( zip( lfnDict.values(), lfnDict.keys() ) )
    else:
      pfnsDict = dict( [ ( self.getPfnForLfn( lfn, storageElementName )['Value'].get( 'Successful', {} ).get( lfn, lfnDict[lfn] ), lfn ) for lfn in lfnDict] )
    # removePhysicalReplicas is called with real PFN list
    res = self.__removePhysicalReplica( storageElementName, pfnDict.keys() )
    if not res['OK']:
      errStr = "__removeReplica: Failed to remove catalog replicas."
      self.log.error( errStr, res['Message'] )
      return S_ERROR( errStr )
    failed.update( dict( [( pfnDict[pfn], error ) for pfn, error in res['Value']['Failed'].items()] ) )
    # Here we use the FC PFN...
    replicaTuples = [( pfnDict[pfn], lfnDict[pfnDict[lfn]], storageElementName ) for pfn in res['Value']['Successful']]
    res = self.__removeCatalogReplica( replicaTuples )
    if not res['OK']:
      errStr = "__removeReplica: Completely failed to remove physical files."
      self.log.error( errStr, res['Message'] )
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
      self.log.error( errStr )
      return S_ERROR( errStr )
    self.log.verbose( "removeReplicaFromCatalog: Will remove catalogue entry for %s lfns at %s." % \
                        ( len( lfns ), storageElementName ) )
    res = self.getCatalogReplicas( lfns, allStatus = True )
    if not res['OK']:
      errStr = "removeReplicaFromCatalog: Completely failed to get replicas for lfns."
      self.log.error( errStr, res['Message'] )
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
    self.log.verbose( "removeReplicaFromCatalog: Resolved %s pfns for catalog removal at %s." % ( len( replicaTuples ),
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
      self.log.error( errStr )
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
    res = self.fileCatalogue.removeReplica( replicaDict )
    oDataOperation.setEndTime()
    oDataOperation.setValueByKey( 'RegistrationTime', time.time() - start )
    if not res['OK']:
      oDataOperation.setValueByKey( 'RegistrationOK', 0 )
      oDataOperation.setValueByKey( 'FinalStatus', 'Failed' )
      gDataStoreClient.addRegister( oDataOperation )
      errStr = "__removeCatalogReplica: Completely failed to remove replica."
      self.log.error( errStr, res['Message'] )
      return S_ERROR( errStr )
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
      self.log.error( errStr )
      return S_ERROR( errStr )
    # Check that we have write permissions to this directory.
    res = self.__verifyOperationPermission( lfns )
    if not res['OK']:
      return res
    if not res['Value']:
      errStr = "removePhysicalReplica: Write access not permitted for this credential."
      self.log.error( errStr, lfns )
      return S_ERROR( errStr )
    self.log.verbose( "removePhysicalReplica: Attempting to remove %s lfns at %s." % ( len( lfns ),
                                                                                       storageElementName ) )
    self.log.verbose( "removePhysicalReplica: Attempting to resolve replicas." )
    res = self.getReplicas( lfns )
    if not res['OK']:
      errStr = "removePhysicalReplica: Completely failed to get replicas for lfns."
      self.log.error( errStr, res['Message'] )
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
    self.log.verbose( "removePhysicalReplica: Resolved %s pfns for removal at %s." % ( len( pfnDict ),
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
    self.log.verbose( "__removePhysicalReplica: Attempting to remove %s pfns at %s." % ( len( pfnsToRemove ),
                                                                                         storageElementName ) )
    storageElement = StorageElement( storageElementName )
    res = storageElement.isValid()
    if not res['OK']:
      errStr = "__removePhysicalReplica: The storage element is not currently valid."
      self.log.error( errStr, "%s %s" % ( storageElementName, res['Message'] ) )
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
      self.log.error( errStr, res['Message'] )
      return S_ERROR( errStr )
    else:
      for surl, value in res['Value']['Failed'].items():
        if 'No such file or directory' in value:
          res['Value']['Successful'][surl] = surl
          res['Value']['Failed'].pop( surl )
      for surl in res['Value']['Successful']:
        ret = storageElement.getPfnForProtocol( surl, self.registrationProtocol, withPort = False )
        if not ret['OK']:
          res['Value']['Successful'][surl] = surl
        else:
          res['Value']['Successful'][surl] = ret['Value']
      oDataOperation.setValueByKey( 'TransferOK', len( res['Value']['Successful'] ) )
      gDataStoreClient.addRegister( oDataOperation )
      infoStr = "__removePhysicalReplica: Successfully issued accounting removal request."
      self.log.verbose( infoStr )
      return res

  #########################################################################
  #
  # File transfer methods
  #

  def put( self, lfn, fileName, diracSE, path = None ):
    """ Put a local file to a Storage Element

    :param self: self reference
    :param str lfn: LFN
    :param :

        'lfn' is the file LFN
        'file' is the full path to the local file
        'diracSE' is the Storage Element to which to put the file
        'path' is the path on the storage where the file will be put (if not provided the LFN will be used)
    """
    # Check that the local file exists
    if not os.path.exists( fileName ):
      errStr = "put: Supplied file does not exist."
      self.log.error( errStr, fileName )
      return S_ERROR( errStr )
    # If the path is not provided then use the LFN path
    if not path:
      path = os.path.dirname( lfn )
    # Obtain the size of the local file
    size = getSize( fileName )
    if size == 0:
      errStr = "put: Supplied file is zero size."
      self.log.error( errStr, fileName )
      return S_ERROR( errStr )

    ##########################################################
    #  Instantiate the destination storage element here.
    storageElement = StorageElement( diracSE )
    res = storageElement.isValid()
    if not res['OK']:
      errStr = "put: The storage element is not currently valid."
      self.log.error( errStr, "%s %s" % ( diracSE, res['Message'] ) )
      return S_ERROR( errStr )
    res = storageElement.getPfnForLfn( lfn )
    if not res['OK']or lfn not in res['Value']['Successful']:
      errStr = "put: Failed to generate destination PFN."
      self.log.error( errStr, res.get( 'Message', res.get( 'Value', {} ).get( 'Failed', {} ).get( lfn ) ) )
      return S_ERROR( errStr )
    destPfn = res['Value']['Successful'][lfn]
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
      self.log.error( errStr, "%s: %s" % ( fileName, res['Message'] ) )
    else:
      self.log.info( "put: Put file to storage in %s seconds." % putTime )
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
        if se not in seReadStatus:
          res = self.getSEStatus( se )
          if res['OK']:
            seReadStatus[se] = res['Value']['Read']
          else:
            seReadStatus[se] = False
        if not seReadStatus[se]:
          replicas.pop( se )

    return S_OK( replicaDict )

  def getSEStatus( self, se ):
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
    return self._callStorageElementFcn( storageElementName, physicalFile, 'retransferOnlineFile' )

  def getReplicas( self, lfns, allStatus = True ):
    """ get replicas from catalogue """
    res = self.getCatalogReplicas( lfns, allStatus = allStatus )

    if not self.useCatalogPFN:
      if res['OK']:
        se_lfn = {}
        catalogReplicas = res['Value']['Successful']

        # We group the query to getPfnForLfn by storage element to gain in speed
        for lfn in catalogReplicas:
          for se in catalogReplicas[lfn]:
            se_lfn.setdefault( se, [] ).append( lfn )

        for se in se_lfn:
          succPfn = self.getPfnForLfn( se_lfn[se], se ).get( 'Value', {} ).get( 'Successful', {} )
          for lfn in succPfn:
            # catalogReplicas still points res["value"]["Successful"] so res will be updated
            catalogReplicas[lfn][se] = succPfn[lfn]

    return res

  def getFileSize( self, lfn ):
    """ get file size from catalogue """
    return self.getCatalogFileSize( lfn )

