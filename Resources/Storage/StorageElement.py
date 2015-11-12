""" This is the StorageElement class.
"""
from types import ListType

__RCSID__ = "$Id$"
# # custom duty
import re
import time
import datetime
import copy
import errno
# # from DIRAC
from DIRAC import gLogger, gConfig, siteName
from DIRAC.Core.Utilities import DErrno, DError
from DIRAC.Core.Utilities.ReturnValues import S_OK, S_ERROR, returnSingleResult
from DIRAC.Resources.Storage.StorageFactory import StorageFactory
from DIRAC.Core.Utilities.Pfn import pfnparse
from DIRAC.Core.Utilities.SiteSEMapping import getSEsForSite
from DIRAC.Core.Security.ProxyInfo import getVOfromProxyGroup
from DIRAC.ConfigurationSystem.Client.Helpers.Operations import Operations
from DIRAC.Core.Utilities.DictCache import DictCache
from DIRAC.Resources.Storage.Utilities import checkArgumentFormat
from DIRAC.Resources.Catalog.FileCatalog import FileCatalog
from DIRAC.Core.Security.ProxyInfo import getProxyInfo
from DIRAC.AccountingSystem.Client.Types.DataOperation import DataOperation
from DIRAC.AccountingSystem.Client.DataStoreClient import gDataStoreClient
from DIRAC.DataManagementSystem.Utilities.DMSHelpers import DMSHelpers


class StorageElementCache( object ):

  def __init__( self ):
    self.seCache = DictCache()

  def __call__( self, name, protocols = None, vo = None, hideExceptions = False ):
    self.seCache.purgeExpired( expiredInSeconds = 60 )
    argTuple = ( name, protocols, vo )
    seObj = self.seCache.get( argTuple )

    if not seObj:
      seObj = StorageElementItem( name, protocols, vo, hideExceptions = hideExceptions )
      # Add the StorageElement to the cache for 1/2 hour
      self.seCache.add( argTuple, 1800, seObj )

    return seObj

class StorageElementItem( object ):
  """
  .. class:: StorageElement

  common interface to the grid storage element



  self.name is the resolved name of the StorageElement i.e CERN-tape
  self.options is dictionary containing the general options defined in the CS e.g. self.options['Backend] = 'Castor2'
  self.storages is a list of the stub objects created by StorageFactory for the protocols found in the CS.
  self.localPlugins is a list of the local protocols that were created by StorageFactory
  self.remotePlugins is a list of the remote protocols that were created by StorageFactory
  self.protocolOptions is a list of dictionaries containing the options found in the CS. (should be removed)



  dynamic method :
  retransferOnlineFile( lfn )
  exists( lfn )
  isFile( lfn )
  getFile( lfn, localPath = False )
  putFile( lfnLocal, sourceSize = 0 ) : {lfn:local}
  replicateFile( lfn, sourceSize = 0 )
  getFileMetadata( lfn )
  getFileSize( lfn )
  removeFile( lfn )
  prestageFile( lfn, lifetime = 86400 )
  prestageFileStatus( lfn )
  pinFile( lfn, lifetime = 60 * 60 * 24 )
  releaseFile( lfn )
  isDirectory( lfn )
  getDirectoryMetadata( lfn )
  getDirectorySize( lfn )
  listDirectory( lfn )
  removeDirectory( lfn, recursive = False )
  createDirectory( lfn )
  putDirectory( lfn )
  getDirectory( lfn, localPath = False )


  """

  __deprecatedArguments = ["singleFile", "singleDirectory"]  # Arguments that are now useless

  # Some methods have a different name in the StorageElement and the plugins...
  # We could avoid this static list in the __getattr__ by checking the storage plugin and so on
  # but fine... let's not be too smart, otherwise it becomes unreadable :-)
  __equivalentMethodNames = {"exists" : "exists",
                            "isFile" : "isFile",
                            "getFile" : "getFile",
                            "putFile" : "putFile",
                            "replicateFile" : "putFile",
                            "getFileMetadata" : "getFileMetadata",
                            "getFileSize" : "getFileSize",
                            "removeFile" : "removeFile",
                            "prestageFile" : "prestageFile",
                            "prestageFileStatus" : "prestageFileStatus",
                            "pinFile" : "pinFile",
                            "releaseFile" : "releaseFile",
                            "isDirectory" : "isDirectory",
                            "getDirectoryMetadata" : "getDirectoryMetadata",
                            "getDirectorySize" : "getDirectorySize",
                            "listDirectory" : "listDirectory",
                            "removeDirectory" : "removeDirectory",
                            "createDirectory" : "createDirectory",
                            "putDirectory" : "putDirectory",
                            "getDirectory" : "getDirectory",
                            }

  # We can set default argument in the __executeFunction which impacts all plugins
  __defaultsArguments = {"putFile" : {"sourceSize"  : 0 },
                         "getFile": { "localPath": False },
                         "prestageFile" : { "lifetime" : 86400 },
                         "pinFile" : { "lifetime" : 60 * 60 * 24 },
                         "removeDirectory" : { "recursive" : False },
                         "getDirectory" : { "localPath" : False },
                         }

  def __init__( self, name, plugins = None, vo = None, hideExceptions = False ):
    """ c'tor

    :param str name: SE name
    :param list plugins: requested storage plugins
    :param: vo
    """

    self.methodName = None

    if vo:
      self.vo = vo
    else:
      result = getVOfromProxyGroup()
      if not result['OK']:
        return
      self.vo = result['Value']
    self.opHelper = Operations( vo = self.vo )

    # These things will soon have to go as well. 'AccessProtocol.1' is all but flexible.
    proxiedProtocols = gConfig.getValue( '/LocalSite/StorageElements/ProxyProtocols', "" ).split( ',' )
    useProxy = ( gConfig.getValue( "/Resources/StorageElements/%s/AccessProtocol.1/Protocol" % name, "UnknownProtocol" )
                in proxiedProtocols )

    if not useProxy:
      useProxy = gConfig.getValue( '/LocalSite/StorageElements/%s/UseProxy' % name, False )
    if not useProxy:
      useProxy = self.opHelper.getValue( '/Services/StorageElements/%s/UseProxy' % name, False )

    self.valid = True
    if plugins == None:
      res = StorageFactory( useProxy = useProxy, vo = self.vo ).getStorages( name, pluginList = [], hideExceptions = hideExceptions )
    else:
      res = StorageFactory( useProxy = useProxy, vo = self.vo ).getStorages( name, pluginList = plugins, hideExceptions = hideExceptions )

    if not res['OK']:
      self.valid = False
      self.name = name
      self.errorReason = res['Message']
    else:
      factoryDict = res['Value']
      self.name = factoryDict['StorageName']
      self.options = factoryDict['StorageOptions']
      self.localPlugins = factoryDict['LocalPlugins']
      self.remotePlugins = factoryDict['RemotePlugins']
      self.storages = factoryDict['StorageObjects']
      self.protocolOptions = factoryDict['ProtocolOptions']
      self.turlProtocols = factoryDict['TurlProtocols']
      for storage in self.storages:
        storage.setStorageElement( self )


    self.log = gLogger.getSubLogger( "SE[%s]" % self.name )
    self.useCatalogURL = gConfig.getValue( '/Resources/StorageElements/%s/UseCatalogURL' % self.name, False )
    self.log.debug( "useCatalogURL: %s" % self.useCatalogURL )


    self.__dmsHelper = DMSHelpers( vo = vo )

    # Allow SE to overwrite general operation config
    accessProto = self.options.get( 'AccessProtocols' )
    self.localAccessProtocolList = accessProto if accessProto else self.__dmsHelper.getAccessProtocols()
    self.log.debug( "localAccessProtocolList %s" % self.localAccessProtocolList )

    writeProto = self.options.get( 'WriteProtocols' )
    self.localWriteProtocolList = writeProto if writeProto else self.__dmsHelper.getWriteProtocols()
    self.log.debug( "localWriteProtocolList %s" % self.localWriteProtocolList )




    #                         'getTransportURL',
    self.readMethods = [ 'getFile',
                         'prestageFile',
                         'prestageFileStatus',
                         'getDirectory']

    self.writeMethods = [ 'retransferOnlineFile',
                          'putFile',
                          'replicateFile',
                          'pinFile',
                          'releaseFile',
                          'createDirectory',
                          'putDirectory' ]

    self.removeMethods = [ 'removeFile', 'removeDirectory' ]

    self.checkMethods = [ 'exists',
                          'getDirectoryMetadata',
                          'getDirectorySize',
                          'getFileSize',
                          'getFileMetadata',
                          'listDirectory',
                          'isDirectory',
                          'isFile',
                           ]

    self.okMethods = [ 'getLocalProtocols',
                       'getProtocols',
                       'getRemoteProtocols',
                       'getStorageElementName',
                       'getStorageParameters',
                       'getTransportURL',
                       'isLocalSE' ]

    self.__fileCatalog = None

  def dump( self ):
    """ Dump to the logger a summary of the StorageElement items. """
    log = self.log.getSubLogger( 'dump', True )
    log.verbose( "Preparing dump for StorageElement %s." % self.name )
    if not self.valid:
      log.debug( "Failed to create StorageElement plugins.", self.errorReason )
      return
    i = 1
    outStr = "\n\n============ Options ============\n"
    for key in sorted( self.options ):
      outStr = "%s%s: %s\n" % ( outStr, key.ljust( 15 ), self.options[key] )

    for storage in self.storages:
      outStr = "%s============Protocol %s ============\n" % ( outStr, i )
      storageParameters = storage.getParameters()
      for key in sorted( storageParameters ):
        outStr = "%s%s: %s\n" % ( outStr, key.ljust( 15 ), storageParameters[key] )
      i = i + 1
    log.verbose( outStr )

  #################################################################################################
  #
  # These are the basic get functions for storage configuration
  #

  def getStorageElementName( self ):
    """ SE name getter """
    self.log.getSubLogger( 'getStorageElementName' ).verbose( "The Storage Element name is %s." % self.name )
    return S_OK( self.name )

  def getChecksumType( self ):
    """ get local /Resources/StorageElements/SEName/ChecksumType option if defined, otherwise
        global /Resources/StorageElements/ChecksumType
    """
    self.log.getSubLogger( 'getChecksumType' ).verbose( "get checksum type for %s." % self.name )
    return S_OK( str( gConfig.getValue( "/Resources/StorageElements/ChecksumType", "ADLER32" ) ).upper()
                 if "ChecksumType" not in self.options else str( self.options["ChecksumType"] ).upper() )

  def getStatus( self ):
    """
     Return Status of the SE, a dictionary with:
      - Read: True (is allowed), False (it is not allowed)
      - Write: True (is allowed), False (it is not allowed)
      - Remove: True (is allowed), False (it is not allowed)
      - Check: True (is allowed), False (it is not allowed).
      NB: Check always allowed IF Read is allowed (regardless of what set in the Check option of the configuration)
      - DiskSE: True if TXDY with Y > 0 (defaults to True)
      - TapeSE: True if TXDY with X > 0 (defaults to False)
      - TotalCapacityTB: float (-1 if not defined)
      - DiskCacheTB: float (-1 if not defined)
    """

    self.log.getSubLogger( 'getStatus' ).verbose( "determining status of %s." % self.name )

    retDict = {}
    if not self.valid:
      retDict['Read'] = False
      retDict['Write'] = False
      retDict['Remove'] = False
      retDict['Check'] = False
      retDict['DiskSE'] = False
      retDict['TapeSE'] = False
      retDict['TotalCapacityTB'] = -1
      retDict['DiskCacheTB'] = -1
      return S_OK( retDict )

    # If nothing is defined in the CS Access is allowed
    # If something is defined, then it must be set to Active
    retDict['Read'] = not ( 'ReadAccess' in self.options and self.options['ReadAccess'] not in ( 'Active', 'Degraded' ) )
    retDict['Write'] = not ( 'WriteAccess' in self.options and self.options['WriteAccess'] not in ( 'Active', 'Degraded' ) )
    retDict['Remove'] = not ( 'RemoveAccess' in self.options and self.options['RemoveAccess'] not in ( 'Active', 'Degraded' ) )
    if retDict['Read']:
      retDict['Check'] = True
    else:
      retDict['Check'] = not ( 'CheckAccess' in self.options and self.options['CheckAccess'] not in ( 'Active', 'Degraded' ) )
    diskSE = True
    tapeSE = False
    if 'SEType' in self.options:
      # Type should follow the convention TXDY
      seType = self.options['SEType']
      diskSE = re.search( 'D[1-9]', seType ) != None
      tapeSE = re.search( 'T[1-9]', seType ) != None
    retDict['DiskSE'] = diskSE
    retDict['TapeSE'] = tapeSE
    try:
      retDict['TotalCapacityTB'] = float( self.options['TotalCapacityTB'] )
    except Exception:
      retDict['TotalCapacityTB'] = -1
    try:
      retDict['DiskCacheTB'] = float( self.options['DiskCacheTB'] )
    except Exception:
      retDict['DiskCacheTB'] = -1

    return S_OK( retDict )

  def isValid( self, operation = '' ):
    """ check CS/RSS statuses for :operation:

    :param str operation: operation name
    """
    log = self.log.getSubLogger( 'isValid', True )
    log.verbose( "Determining if the StorageElement %s is valid for VO %s" % ( self.name, self.vo ) )

    if not self.valid:
      log.debug( "Failed to create StorageElement plugins.", self.errorReason )
      return S_ERROR( "SE.isValid: Failed to create StorageElement plugins: %s" % self.errorReason )

    # Check if the Storage Element is eligible for the user's VO
    if 'VO' in self.options and not self.vo in self.options['VO']:
      log.debug( "StorageElement is not allowed for VO", self.vo )
      return DError( errno.EACCES, "StorageElement.isValid: StorageElement is not allowed for VO" )
    log.verbose( "Determining if the StorageElement %s is valid for %s" % ( self.name, operation ) )
    if ( not operation ) or ( operation in self.okMethods ):
      return S_OK()

    # Determine whether the StorageElement is valid for checking, reading, writing
    res = self.getStatus()
    if not res[ 'OK' ]:
      log.debug( "Could not call getStatus", res['Message'] )
      return S_ERROR( "SE.isValid could not call the getStatus method" )
    checking = res[ 'Value' ][ 'Check' ]
    reading = res[ 'Value' ][ 'Read' ]
    writing = res[ 'Value' ][ 'Write' ]
    removing = res[ 'Value' ][ 'Remove' ]

    # Determine whether the requested operation can be fulfilled
    if ( not operation ) and ( not reading ) and ( not writing ) and ( not checking ):
      log.debug( "Read, write and check access not permitted." )
      return DError( errno.EACCES, "SE.isValid: Read, write and check access not permitted." )


    # The supplied operation can be 'Read','Write' or any of the possible StorageElement methods.
    if ( operation in self.readMethods ) or ( operation.lower() in ( 'read', 'readaccess' ) ):
      operation = 'ReadAccess'
    elif operation in self.writeMethods or ( operation.lower() in ( 'write', 'writeaccess' ) ):
      operation = 'WriteAccess'
    elif operation in self.removeMethods or ( operation.lower() in ( 'remove', 'removeaccess' ) ):
      operation = 'RemoveAccess'
    elif operation in self.checkMethods or ( operation.lower() in ( 'check', 'checkaccess' ) ):
      operation = 'CheckAccess'
    else:
      log.debug( "The supplied operation is not known.", operation )
      return DError( DErrno.ENOMETH , "SE.isValid: The supplied operation is not known." )
    log.debug( "check the operation: %s " % operation )

    # Check if the operation is valid
    if operation == 'CheckAccess':
      if not reading:
        if not checking:
          log.debug( "Check access not currently permitted." )
          return DError( errno.EACCES, "SE.isValid: Check access not currently permitted." )
    if operation == 'ReadAccess':
      if not reading:
        log.debug( "Read access not currently permitted." )
        return DError( errno.EACCES, "SE.isValid: Read access not currently permitted." )
    if operation == 'WriteAccess':
      if not writing:
        log.debug( "Write access not currently permitted." )
        return DError( errno.EACCES, "SE.isValid: Write access not currently permitted." )
    if operation == 'RemoveAccess':
      if not removing:
        log.debug( "Remove access not currently permitted." )
        return DError( errno.EACCES, "SE.isValid: Remove access not currently permitted." )
    return S_OK()

  def getPlugins( self ):
    """ Get the list of all the plugins defined for this Storage Element
    """
    self.log.getSubLogger( 'getPlugins' ).verbose( "Obtaining all plugins of %s." % self.name )
    if not self.valid:
      return S_ERROR( self.errorReason )
    allPlugins = self.localPlugins + self.remotePlugins
    return S_OK( allPlugins )

  def getRemotePlugins( self ):
    """ Get the list of all the remote access protocols defined for this Storage Element
    """
    self.log.getSubLogger( 'getRemotePlugins' ).verbose( "Obtaining remote protocols for %s." % self.name )
    if not self.valid:
      return S_ERROR( self.errorReason )
    return S_OK( self.remotePlugins )

  def getLocalPlugins( self ):
    """ Get the list of all the local access protocols defined for this Storage Element
    """
    self.log.getSubLogger( 'getLocalPlugins' ).verbose( "Obtaining local protocols for %s." % self.name )
    if not self.valid:
      return S_ERROR( self.errorReason )
    return S_OK( self.localPlugins )

  def getStorageParameters( self, plugin ):
    """ Get plugin specific options
      :param plugin : plugin we are interested in
    """

    log = self.log.getSubLogger( 'getStorageParameters' )
    log.verbose( "Obtaining storage parameters for %s plugin %s." % ( self.name,
                                                                                                                plugin ) )
    res = self.getPlugins()
    if not res['OK']:
      return res
    availablePlugins = res['Value']
    if not plugin in availablePlugins:
      errStr = "Requested plugin not available for SE."
      log.debug( errStr, '%s for %s' % ( plugin, self.name ) )
      return S_ERROR( errStr )
    for storage in self.storages:
      storageParameters = storage.getParameters()
      if storageParameters['PluginName'] == plugin:
        return S_OK( storageParameters )
    errStr = "Requested plugin supported but no object found."
    log.debug( errStr, "%s for %s" % ( plugin, self.name ) )
    return S_ERROR( errStr )

  def __getAllProtocols( self, protoType ):
    """ Returns the list of all protocols for Input or Output
        :param proto = InputProtocols or OutputProtocols
    """
    return set( reduce( lambda x, y:x + y, [plugin.protocolParameters[protoType]  for plugin in self.storages ] ) )


  def _getAllInputProtocols( self ):
    """ Returns all the protocols supported by the SE for Input
    """
    return self.__getAllProtocols( 'InputProtocols' )

  def _getAllOutputProtocols( self ):
    """ Returns all the protocols supported by the SE for Output
    """
    return self.__getAllProtocols( 'OutputProtocols' )



  def negociateProtocolWithOtherSE( self, sourceSE, protocols = None ):
    """ Negotiate what protocol could be used for a third party transfer
        between the sourceSE and ourselves. If protocols is given,
        the chosen protocol has to be among those

        :param sourceSE : storageElement instance of the sourceSE
        :param protocols: ordered protocol restriction list

        :return: a list protocols that fits the needs, or None

    """

    log = self.log.getSubLogger( 'negociateProtocolWithOtherSE', child = True )
    
    log.debug( "Negociating protocols between %s and %s (protocols %s)" % ( sourceSE.name, self.name, protocols ) )

    # Take all the protocols the destination can accept as input
    destProtocols = self._getAllInputProtocols()

    log.debug( "Destination input protocols %s" % destProtocols )

    # Take all the protocols the source can provide
    sourceProtocols = sourceSE._getAllOutputProtocols()

    log.debug( "Source output protocols %s" % sourceProtocols )

    commonProtocols = destProtocols & sourceProtocols

    # If a restriction list is defined
    # take the intersection, and sort the commonProtocols
    if protocols:
      protocolList = list( protocols )
      commonProtocols = sorted( commonProtocols & set( protocolList ), key = lambda x : protocolList )

    log.debug( "Common protocols %s" % commonProtocols )

    return S_OK( list( commonProtocols ) )

  #################################################################################################
  #
  # These are the basic get functions for lfn manipulation
  #

  def __getURLPath( self, url ):
    """  Get the part of the URL path below the basic storage path.
         This path must coincide with the LFN of the file in order to be compliant with the DIRAC conventions.
    """
    log = self.log.getSubLogger( '__getURLPath' )
    log.verbose( "Getting path from url in %s." % self.name )
    if not self.valid:
      return S_ERROR( self.errorReason )
    res = pfnparse( url )
    if not res['OK']:
      return res
    fullURLPath = '%s/%s' % ( res['Value']['Path'], res['Value']['FileName'] )

    # Check all available storages and check whether the url is for that protocol
    urlPath = ''
    for storage in self.storages:
      res = storage.isNativeURL( url )
      if res['OK']:
        if res['Value']:
          parameters = storage.getParameters()
          saPath = parameters['Path']
          if not saPath:
            # If the sa path doesn't exist then the url path is the entire string
            urlPath = fullURLPath
          else:
            if re.search( saPath, fullURLPath ):
              # Remove the sa path from the fullURLPath
              urlPath = fullURLPath.replace( saPath, '' )
      if urlPath:
        return S_OK( urlPath )
    # This should never happen. DANGER!!
    errStr = "Failed to get the url path for any of the protocols!!"
    log.debug( errStr )
    return S_ERROR( errStr )

  def getLFNFromURL( self, urls ):
    """ Get the LFN from the PFNS .
        :param lfn : input lfn or lfns (list/dict)
    """
    result = checkArgumentFormat( urls )
    if result['OK']:
      urlDict = result['Value']
    else:
      errStr = "Supplied urls must be string, list of strings or a dictionary."
      self.log.getSubLogger( 'getLFNFromURL' ).debug( errStr )
      return DError( errno.EINVAL, errStr )

    retDict = { "Successful" : {}, "Failed" : {} }
    for url in urlDict:
      res = self.__getURLPath( url )
      if res["OK"]:
        retDict["Successful"][url] = res["Value"]
      else:
        retDict["Failed"][url] = res["Message"]
    return S_OK( retDict )

  ###########################################################################################
  #
  # This is the generic wrapper for file operations
  #

  def getURL( self, lfn, protocol = False, replicaDict = None ):
    """ execute 'getTransportURL' operation.
      :param str lfn: string, list or dictionary of lfns
      :param protocol: if no protocol is specified, we will request self.turlProtocols
      :param replicaDict: optional results from the File Catalog replica query
    """

    self.log.getSubLogger( 'getURL' ).verbose( "Getting accessUrl %s for lfn in %s." % ( "(%s)" % protocol if protocol else "", self.name ) )

    if not protocol:
      # This turlProtocols seems totally useless.
      # Get ride of it when gfal2 is totally ready
      # and replace it with the localAccessProtocol list
      protocols = self.turlProtocols
    elif type( protocol ) is ListType:
      protocols = protocol
    elif type( protocol ) == type( '' ):
      protocols = [protocol]

    self.methodName = "getTransportURL"
    result = self.__executeMethod( lfn, protocols = protocols )
    return result

  def __isLocalSE( self ):
    """ Test if the Storage Element is local in the current context
    """
    self.log.getSubLogger( 'LocalSE' ).verbose( "Determining whether %s is a local SE." % self.name )

    import DIRAC
    localSEs = getSEsForSite( DIRAC.siteName() )['Value']
    if self.name in localSEs:
      return S_OK( True )
    else:
      return S_OK( False )

  def __getFileCatalog( self ):

    if not self.__fileCatalog:
      self.__fileCatalog = FileCatalog( vo = self.vo )
    return self.__fileCatalog

  def __generateURLDict( self, lfns, storage, replicaDict = None ):
    """ Generates a dictionary (url : lfn ), where the url are constructed
        from the lfn using the constructURLFromLFN method of the storage plugins.
        :param: lfns : dictionary {lfn:whatever}
        :returns dictionary {constructed url : lfn}
    """
    log = self.log.getSubLogger( "__generateURLDict" )
    log.verbose( "generating url dict for %s lfn in %s." % ( len( lfns ), self.name ) )

    if not replicaDict:
      replicaDict = {}

    urlDict = {}  # url : lfn
    failed = {}  # lfn : string with errors
    for lfn in lfns:
      if self.useCatalogURL:
        # Is this self.name alias proof?
        url = replicaDict.get( lfn, {} ).get( self.name, '' )
        if url:
          urlDict[url] = lfn
          continue
        else:
          fc = self.__getFileCatalog()
          result = fc.getReplicas()
          if not result['OK']:
            failed[lfn] = result['Message']
          url = result['Value']['Successful'].get( lfn, {} ).get( self.name, '' )

        if not url:
          failed[lfn] = 'Failed to get catalog replica'
        else:
          # Update the URL according to the current SE description
          result = returnSingleResult( storage.updateURL( url ) )
          if not result['OK']:
            failed[lfn] = result['Message']
          else:
            urlDict[result['Value']] = lfn
      else:
        result = storage.constructURLFromLFN( lfn, withWSUrl = True )
        if not result['OK']:
          errStr = result['Message']
          log.debug( errStr, 'for %s' % ( lfn ) )
          failed[lfn] = "%s %s" % ( failed[lfn], errStr ) if lfn in failed else errStr
        else:
          urlDict[result['Value']] = lfn

    res = S_OK( {'Successful': urlDict, 'Failed' : failed} )
#     res['Failed'] = failed
    return res


  def __filterPlugins( self, methodName, protocols = None, inputProtocol = None ):
    """ Determine the list of plugins that
        can be used for a particular action

        :param method: method to execute
        :param protocol: specific protocols might be requested
        :param inputProtocol: in case the method is putFile, this specifies
                              the protocol given as source
    """

    log = self.log.getSubLogger( '__filterPlugins', child = True )

    log.debug( "Filtering plugins for %s (protocol = %s ; inputProtocol = %s)" % ( methodName, protocols, inputProtocol ) )

    if isinstance( protocols, basestring ):
      protocols = [protocols]

    pluginsToUse = []
    
    potentialProtocols = []
    allowedProtocols = []
    
    if methodName in self.readMethods + self.checkMethods:
      allowedProtocols = self.localAccessProtocolList
    elif methodName in self.removeMethods + self.writeMethods :
      allowedProtocols = self.localWriteProtocolList
    else:
      # OK methods
      # If a protocol or protocol list is specified, we only use the plugins that
      # can generate such protocol
      # otherwise we return them all
      if protocols:
        setProtocol = set( protocols )
        for plugin in self.storages:
          if set( plugin.protocolParameters.get( "OutputProtocols", [] ) ) & setProtocol:
            pluginsToUse.append( plugin )
      else:
        pluginsToUse = self.storages

      log.debug( "Plugins to be used for %s: %s" % ( methodName, [p.pluginName for p in self.storages] ) )
      return self.storages

    
    # if a list of protocol is specified, take it into account
    if protocols:
      potentialProtocols = list( set( allowedProtocols ) & set( protocols ) )
    else:
      potentialProtocols = allowedProtocols
      
    log.debug( 'Potential protocols %s' % potentialProtocols )

    localSE = self.__isLocalSE()['Value']

    for plugin in self.storages:
      # Determine whether to use this storage object
      pluginParameters = plugin.getParameters()
      pluginName = pluginParameters.get( 'PluginName' )

      if not pluginParameters:
        log.debug( "Failed to get storage parameters.", "%s %s" % ( self.name, pluginName ) )
        continue

      if not ( pluginName in self.remotePlugins ) and not localSE and not pluginName == "Proxy":
        # If the SE is not local then we can't use local protocols
        log.debug( "Local protocol not appropriate for remote use: %s." % pluginName )
        continue
      
      if pluginParameters['Protocol'] not in potentialProtocols:
        log.debug( "Plugin %s not allowed for %s." % ( pluginName, methodName ) )
        continue

      # If we are attempting a putFile and we know the inputProtocol
      if methodName == 'putFile' and inputProtocol:
        if inputProtocol not in pluginParameters['InputProtocols']:
          log.debug( "Plugin %s not appropriate for %s protocol as input." % ( pluginName, inputProtocol ) )
          continue
 
      pluginsToUse.append( plugin )

    # sort the plugins according to the lists in the CS
    pluginsToUse.sort( key = lambda x: allowedProtocols.index( x.protocolParameters['Protocol'] ) )

    log.debug( "Plugins to be used for %s: %s" % ( methodName, [p.pluginName for p in pluginsToUse] ) )

    return pluginsToUse


  def __executeMethod( self, lfn, *args, **kwargs ):
    """ Forward the call to each storage in turn until one works.
        The method to be executed is stored in self.methodName
        :param lfn : string, list or dictionnary
        :param *args : variable amount of non-keyword arguments. SHOULD BE EMPTY
        :param **kwargs : keyword arguments
        :returns S_OK( { 'Failed': {lfn : reason} , 'Successful': {lfn : value} } )
                The Failed dict contains the lfn only if the operation failed on all the storages
                The Successful dict contains the value returned by the successful storages.

        A special kwargs is 'inputProtocol', which can be specified for putFile. It describes
        the protocol used as source protocol, since there is in principle only one.
    """


    removedArgs = {}
    log = self.log.getSubLogger( '__executeMethod' )
    log.verbose( "preparing the execution of %s" % ( self.methodName ) )

    # args should normaly be empty to avoid problem...
    if len( args ):
      log.verbose( "args should be empty!%s" % args )
      # because there is normally only one kw argument, I can move it from args to kwargs
      methDefaultArgs = StorageElementItem.__defaultsArguments.get( self.methodName, {} ).keys()
      if len( methDefaultArgs ):
        kwargs[methDefaultArgs[0] ] = args[0]
        args = args[1:]
      log.verbose( "put it in kwargs, but dirty and might be dangerous!args %s kwargs %s" % ( args, kwargs ) )


    # We check the deprecated arguments
    for depArg in StorageElementItem.__deprecatedArguments:
      if depArg in kwargs:
        log.verbose( "%s is not an allowed argument anymore. Please change your code!" % depArg )
        removedArgs[depArg] = kwargs[depArg]
        del kwargs[depArg]



    # Set default argument if any
    methDefaultArgs = StorageElementItem.__defaultsArguments.get( self.methodName, {} )
    for argName in methDefaultArgs:
      if argName not in kwargs:
        log.debug( "default argument %s for %s not present.\
         Setting value %s." % ( argName, self.methodName, methDefaultArgs[argName] ) )
        kwargs[argName] = methDefaultArgs[argName]

    res = checkArgumentFormat( lfn )
    if not res['OK']:
      errStr = "Supplied lfns must be string, list of strings or a dictionary."
      log.debug( errStr )
      return res
    lfnDict = res['Value']

    log.verbose( "Attempting to perform '%s' operation with %s lfns." % ( self.methodName, len( lfnDict ) ) )

    res = self.isValid( operation = self.methodName )
    if not res['OK']:
      return res
    else:
      if not self.valid:
        return S_ERROR( self.errorReason )

    # In case executing putFile, we can assume that all the source urls
    # are from the same protocol. This optional parameter, if defined
    # can be used to ignore some storage plugins and thus save time
    # and avoid fake failures showing in the accounting
    inputProtocol = kwargs.pop( 'inputProtocol', None )
    

    successful = {}
    failed = {}
    filteredPlugins = self.__filterPlugins( self.methodName, kwargs.get( 'protocols' ), inputProtocol )
    if not filteredPlugins:
      return DError( errno.EPROTONOSUPPORT, "No storage plugins matching the requirements\
                                           (operation %s protocols %s inputProtocol %s)"\
                                            % ( self.methodName, kwargs.get( 'protocols' ), inputProtocol ) )
    # Try all of the storages one by one
    for storage in filteredPlugins:
      # Determine whether to use this storage object
      storageParameters = storage.getParameters()
      pluginName = storageParameters['PluginName']

      if not lfnDict:
        log.debug( "No lfns to be attempted for %s protocol." % pluginName )
        continue

      log.verbose( "Generating %s protocol URLs for %s." % ( len( lfnDict ), pluginName ) )
      replicaDict = kwargs.pop( 'replicaDict', {} )
      if storage.pluginName != "Proxy":
        res = self.__generateURLDict( lfnDict, storage, replicaDict = replicaDict )
        urlDict = res['Value']['Successful']  # url : lfn
        failed.update( res['Value']['Failed'] )
      else:
        urlDict = dict( [ ( lfn, lfn ) for lfn in lfnDict ] )
      if not len( urlDict ):
        log.verbose( "__executeMethod No urls generated for protocol %s." % pluginName )
      else:
        log.verbose( "Attempting to perform '%s' for %s physical files" % ( self.methodName, len( urlDict ) ) )
        fcn = None
        if hasattr( storage, self.methodName ) and callable( getattr( storage, self.methodName ) ):
          fcn = getattr( storage, self.methodName )
        if not fcn:
          return DError( DErrno.ENOMETH, "SE.__executeMethod: unable to invoke %s, it isn't a member function of storage" )
        urlsToUse = {}  # url : the value of the lfn dictionary for the lfn of this url
        for url in urlDict:
          urlsToUse[url] = lfnDict[urlDict[url]]

        startDate = datetime.datetime.utcnow()
        startTime = time.time()
        res = fcn( urlsToUse, *args, **kwargs )
        elapsedTime = time.time() - startTime


        self.addAccountingOperation( urlsToUse, startDate, elapsedTime, storageParameters, res )

        if not res['OK']:
          errStr = "Completely failed to perform %s." % self.methodName
          log.debug( errStr, 'with plugin %s: %s' % ( pluginName, res['Message'] ) )
          for lfn in urlDict.values():
            if lfn not in failed:
              failed[lfn] = ''
            failed[lfn] = "%s %s" % ( failed[lfn], res['Message'] ) if failed[lfn] else res['Message']

        else:
          for url, lfn in urlDict.items():
            if url not in res['Value']['Successful']:
              if lfn not in failed:
                failed[lfn] = ''
              if url in res['Value']['Failed']:
                self.log.debug( res['Value']['Failed'][url] )
                failed[lfn] = "%s %s" % ( failed[lfn], res['Value']['Failed'][url] ) if failed[lfn] else res['Value']['Failed'][url]
              else:
                errStr = 'No error returned from plug-in'
                failed[lfn] = "%s %s" % ( failed[lfn], errStr ) if failed[lfn] else errStr
            else:
              successful[lfn] = res['Value']['Successful'][url]
              if lfn in failed:
                failed.pop( lfn )
              lfnDict.pop( lfn )



    gDataStoreClient.commit()



    return S_OK( { 'Failed': failed, 'Successful': successful } )


  def __getattr__( self, name ):
    """ Forwards the equivalent Storage calls to __executeMethod"""
    # We take either the equivalent name, or the name itself
    self.methodName = StorageElementItem.__equivalentMethodNames.get( name, None )

    if self.methodName:
      return self.__executeMethod

    raise AttributeError( "StorageElement does not have a method '%s'" % name )
  

      

  def addAccountingOperation( self, lfns, startDate, elapsedTime, storageParameters, callRes ):
    """
        Generates a DataOperation accounting if needs to be, and adds it to the DataStore client cache

        :param lfns : list of lfns on which we attempted the operation
        :param startDate : datetime, start of the operation
        :param elapsedTime : time (seconds) the operation took
        :param storageParameters : the parameters of the plugins used to perform the operation
        :param callRes : the return of the method call, S_OK or S_ERROR

        The operation is generated with the OperationType "se.methodName"
        The TransferSize and TransferTotal for directory methods actually take into
        account the files inside the directory, and not the amount of directory given
        as parameter


    """
  
    if self.methodName not in ( self.readMethods + self.writeMethods + self.removeMethods ):
      return
  
    baseAccountingDict = {}
    baseAccountingDict['OperationType'] = 'se.%s' % self.methodName
    baseAccountingDict['User'] = getProxyInfo().get( 'Value', {} ).get( 'username', 'unknown' )
    baseAccountingDict['RegistrationTime'] = 0.0
    baseAccountingDict['RegistrationOK'] = 0
    baseAccountingDict['RegistrationTotal'] = 0

    # if it is a get method, then source and destination of the transfer should be inverted
    if self.methodName in ( 'putFile', 'getFile' ):
      baseAccountingDict['Destination'] = siteName()
      baseAccountingDict[ 'Source'] = self.name
    else:
      baseAccountingDict['Destination'] = self.name
      baseAccountingDict['Source'] = siteName()

    baseAccountingDict['TransferTotal'] = 0
    baseAccountingDict['TransferOK'] = 0
    baseAccountingDict['TransferSize'] = 0
    baseAccountingDict['TransferTime'] = 0.0
    baseAccountingDict['FinalStatus'] = 'Successful'

    oDataOperation = DataOperation()
    oDataOperation.setValuesFromDict( baseAccountingDict )
    oDataOperation.setStartTime( startDate )
    oDataOperation.setEndTime( startDate + datetime.timedelta( seconds = elapsedTime ) )
    oDataOperation.setValueByKey( 'TransferTime', elapsedTime )
    oDataOperation.setValueByKey( 'Protocol', storageParameters.get( 'Protocol', 'unknown' ) )
  
    if not callRes['OK']:
      # Everything failed
      oDataOperation.setValueByKey( 'TransferTotal', len( lfns ) )
      oDataOperation.setValueByKey( 'FinalStatus', 'Failed' )
    else:

      succ = callRes.get( 'Value', {} ).get( 'Successful', {} )
      failed = callRes.get( 'Value', {} ).get( 'Failed', {} )

      totalSize = 0
      # We don't take len(lfns) in order to make two
      # separate entries in case of few failures
      totalSucc = len( succ )

      if self.methodName in ( 'putFile', 'getFile' ):
        # putFile and getFile return for each entry
        # in the successful dir the size of the corresponding file
        totalSize = sum( succ.values() )

      elif self.methodName in ( 'putDirectory', 'getDirectory' ):
        # putDirectory and getDirectory return for each dir name
        # a dictionnary with the keys 'Files' and 'Size'
        totalSize = sum( val.get( 'Size', 0 ) for val in succ.values() if isinstance( val, dict ) )
        totalSucc = sum( val.get( 'Files', 0 ) for val in succ.values() if isinstance( val, dict ) )
        oDataOperation.setValueByKey( 'TransferOK', len( succ ) )

      oDataOperation.setValueByKey( 'TransferSize', totalSize )
      oDataOperation.setValueByKey( 'TransferTotal', totalSucc )
      oDataOperation.setValueByKey( 'TransferOK', totalSucc )
      
      if callRes['Value']['Failed']:
        oDataOperationFailed = copy.deepcopy( oDataOperation )
        oDataOperationFailed.setValueByKey( 'TransferTotal', len( failed ) )
        oDataOperationFailed.setValueByKey( 'TransferOK', 0 )
        oDataOperationFailed.setValueByKey( 'TransferSize', 0 )
        oDataOperationFailed.setValueByKey( 'FinalStatus', 'Failed' )

        accRes = gDataStoreClient.addRegister( oDataOperationFailed )
        if not accRes['OK']:
          self.log.error( "Could not send failed accounting report", accRes['Message'] )


    accRes = gDataStoreClient.addRegister( oDataOperation )
    if not accRes['OK']:
      self.log.error( "Could not send accounting report", accRes['Message'] )



StorageElement = StorageElementCache()
