########################################################################
# $HeadURL$
########################################################################
""" This is the StorageElement class.

"""

__RCSID__ = "$Id$"
# # custom duty
import re
from types import ListType, StringType, StringTypes, DictType
# # from DIRAC
from DIRAC import gLogger, S_OK, S_ERROR, gConfig
from DIRAC.Resources.Storage.StorageFactory import StorageFactory
from DIRAC.Core.Utilities.Pfn import pfnparse
from DIRAC.Core.Utilities.SiteSEMapping import getSEsForSite
from DIRAC.Core.Security.ProxyInfo import getVOfromProxyGroup
from DIRAC.ConfigurationSystem.Client.Helpers.Operations import Operations
from DIRAC.ConfigurationSystem.Client.Helpers.Resources import Resources
from DIRAC.ResourceStatusSystem.Client.ResourceStatus import ResourceStatus 
from DIRAC.Resources.Utilities import Utils

class StorageElement:
  """
  .. class:: StorageElement

  common interface to the grid storage element



  self.name is the resolved name of the StorageElement i.e CERN-tape
  self.options is dictionary containing the general options defined in the CS e.g. self.options['Backend] = 'Castor2'
  self.storages is a list of the stub objects created by StorageFactory for the protocols found in the CS.
  self.localProtocols is a list of the local protocols that were created by StorageFactory
  self.remoteProtocols is a list of the remote protocols that were created by StorageFactory
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

  def __init__( self, name, protocols = None, vo = None ):
    """ c'tor

    :param str name: SE name
    :param list protocols: requested protocols
    :param vo
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
    self.resources = Resources( vo = self.vo )

    proxiedProtocols = gConfig.getValue( '/LocalSite/StorageElements/ProxyProtocols', "" ).split( ',' )
    result = self.resources.getAccessProtocols( name )
    if result['OK']:
      ap = result['Value'][0]
      useProxy = ( self.resources.getAccessProtocolValue( ap, "Protocol", "UnknownProtocol" )
                   in proxiedProtocols )

    #print "Proxy", name, proxiedProtocols, \
    #gConfig.getValue( "/Resources/StorageElements/%s/AccessProtocol.1/Protocol" % name, "xxx" )

    if not useProxy:
      useProxy = gConfig.getValue( '/LocalSite/StorageElements/%s/UseProxy' % name, False )
    if not useProxy:
      useProxy = self.opHelper.getValue( '/Services/StorageElements/%s/UseProxy' % name, False )

    self.valid = True
    if protocols == None:
      res = StorageFactory( useProxy ).getStorages( name, protocolList = [] )
    else:
      res = StorageFactory( useProxy ).getStorages( name, protocolList = protocols )
    if not res['OK']:
      self.valid = False
      self.name = name
      self.errorReason = res['Message']
    else:
      factoryDict = res['Value']
      self.name = factoryDict['StorageName']
      self.options = factoryDict['StorageOptions']
      self.localProtocols = factoryDict['LocalProtocols']
      self.remoteProtocols = factoryDict['RemoteProtocols']
      self.storages = factoryDict['StorageObjects']
      self.protocolOptions = factoryDict['ProtocolOptions']
      self.turlProtocols = factoryDict['TurlProtocols']

    self.log = gLogger.getSubLogger( "SE[%s]" % self.name )

    self.readMethods = [ 'getFile',
                         'getAccessUrl',
                         'getTransportURL',
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
                       'getPfnForProtocol',
                       'getPfnForLfn',
                       'getPfnPath',
                       'getProtocols',
                       'getRemoteProtocols',
                       'getStorageElementName',
                       'getStorageElementOption',
                       'getStorageParameters',
                       'isLocalSE' ]

    self.__resourceStatus = ResourceStatus()
    
  def dump( self ):
    """ Dump to the logger a summary of the StorageElement items. """
    self.log.verbose( "dump: Preparing dump for StorageElement %s." % self.name )
    if not self.valid:
      self.log.debug( "dump: Failed to create StorageElement plugins.", self.errorReason )
      return
    i = 1
    outStr = "\n\n============ Options ============\n"
    for key in sorted( self.options ):
      outStr = "%s%s: %s\n" % ( outStr, key.ljust( 15 ), self.options[key] )

    for storage in self.storages:
      outStr = "%s============Protocol %s ============\n" % ( outStr, i )
      res = storage.getParameters()
      storageParameters = res['Value']
      for key in sorted( storageParameters ):
        outStr = "%s%s: %s\n" % ( outStr, key.ljust( 15 ), storageParameters[key] )
      i = i + 1
    self.log.verbose( outStr )

  #################################################################################################
  #
  # These are the basic get functions for storage configuration
  #

  def getStorageElementName( self ):
    """ SE name getter """
    self.log.verbose( "StorageElement.getStorageElementName: The Storage Element name is %s." % self.name )
    return S_OK( self.name )

  def getChecksumType( self ):
    """ get local /Resources/StorageElements/SEName/ChecksumType option if defined, otherwise
        global /Resources/StorageElements/ChecksumType
    """
    self.log.verbose( "StorageElement.getChecksumType : get checksum type for %s." % self.name )
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

    self.log.verbose( "StorageElement.getStatus : determining status of %s." % self.name )

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
    retDict['Read'] = self.__resourceStatus.isUsableStorage( self.name, 'ReadAccess' )
    retDict['Write'] = self.__resourceStatus.isUsableStorage( self.name, 'WriteAccess' )
    retDict['Remove'] = self.__resourceStatus.isUsableStorage( self.name, 'RemoveAccess' )
    if retDict['Read']:
      retDict['Check'] = True
    else:
      retDict['Check'] = self.__resourceStatus.isUsableStorage( self.name, 'CheckAccess' )
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
    self.log.verbose( "StorageElement.isValid: Determining whether the StorageElement %s is valid for %s" % ( self.name,
                                                                                             operation ) )
    if ( not operation ) or ( operation in self.okMethods ):
      return S_OK()

    if not self.valid:
      self.log.debug( "StorageElement.isValid: Failed to create StorageElement plugins.", self.errorReason )
      return S_ERROR( self.errorReason )
    # Determine whether the StorageElement is valid for checking, reading, writing
    res = self.getStatus()
    if not res[ 'OK' ]:
      self.log.debug( "Could not call getStatus" )
      return S_ERROR( "StorageElement.isValid could not call the getStatus method" )
    checking = res[ 'Value' ][ 'Check' ]
    reading = res[ 'Value' ][ 'Read' ]
    writing = res[ 'Value' ][ 'Write' ]
    removing = res[ 'Value' ][ 'Remove' ]

    # Determine whether the requested operation can be fulfilled
    if ( not operation ) and ( not reading ) and ( not writing ) and ( not checking ):
      self.log.debug( "StorageElement.isValid: Read, write and check access not permitted." )
      return S_ERROR( "StorageElement.isValid: Read, write and check access not permitted." )

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
      self.log.debug( "StorageElement.isValid: The supplied operation is not known.", operation )
      return S_ERROR( "StorageElement.isValid: The supplied operation is not known." )
    self.log.debug( "in isValid check the operation: %s " % operation )
    # Check if the operation is valid
    if operation == 'CheckAccess':
      if not reading:
        if not checking:
          self.log.debug( "StorageElement.isValid: Check access not currently permitted." )
          return S_ERROR( "StorageElement.isValid: Check access not currently permitted." )
    if operation == 'ReadAccess':
      if not reading:
        self.log.debug( "StorageElement.isValid: Read access not currently permitted." )
        return S_ERROR( "StorageElement.isValid: Read access not currently permitted." )
    if operation == 'WriteAccess':
      if not writing:
        self.log.debug( "StorageElementisValid: Write access not currently permitted." )
        return S_ERROR( "StorageElement.isValid: Write access not currently permitted." )
    if operation == 'RemoveAccess':
      if not removing:
        self.log.debug( "StorageElement.isValid: Remove access not currently permitted." )
        return S_ERROR( "StorageElement.isValid: Remove access not currently permitted." )
    return S_OK()

  def getProtocols( self ):
    """ Get the list of all the protocols defined for this Storage Element
    """
    self.log.verbose( "StorageElement.getProtocols : Obtaining all protocols of %s." % self.name )
    if not self.valid:
      return S_ERROR( self.errorReason )
    allProtocols = self.localProtocols + self.remoteProtocols
    return S_OK( allProtocols )

  def getRemoteProtocols( self ):
    """ Get the list of all the remote access protocols defined for this Storage Element
    """
    self.log.verbose( "StorageElement.getRemoteProtocols: Obtaining remote protocols for %s." % self.name )
    if not self.valid:
      return S_ERROR( self.errorReason )
    return S_OK( self.remoteProtocols )

  def getLocalProtocols( self ):
    """ Get the list of all the local access protocols defined for this Storage Element
    """
    self.log.verbose( "StorageElement.getLocalProtocols: Obtaining local protocols for %s." % self.name )
    if not self.valid:
      return S_ERROR( self.errorReason )
    return S_OK( self.localProtocols )

  def getStorageElementOption( self, option ):
    """ Get the value for the option supplied from self.options
        :param option : option we are interested in
    """
    self.log.verbose( "StorageElement.getStorageElementOption: Obtaining %s option for Storage Element %s." % ( option,
                                                                                                 self.name ) )
    if not self.valid:
      return S_ERROR( self.errorReason )
    if option in self.options:
      optionValue = self.options[option]
      return S_OK( optionValue )
    else:
      errStr = "StorageElement.getStorageElementOption: Option not defined for SE."
      self.log.debug( errStr, "%s for %s" % ( option, self.name ) )
      return S_ERROR( errStr )

  def getStorageParameters( self, protocol ):
    """ Get protocol specific options
      :param protocol : protocol we are interested in
    """
    self.log.verbose( "StorageElement.getStorageParameters: Obtaining storage parameters for %s protocol %s." % ( self.name,
                                                                                                   protocol ) )
    res = self.getProtocols()
    if not res['OK']:
      return res
    availableProtocols = res['Value']
    if not protocol in availableProtocols:
      errStr = "StorageElement.getStorageParameters: Requested protocol not available for SE."
      self.log.debug( errStr, '%s for %s' % ( protocol, self.name ) )
      return S_ERROR( errStr )
    for storage in self.storages:
      res = storage.getParameters()
      storageParameters = res['Value']
      if storageParameters['ProtocolName'] == protocol:
        return S_OK( storageParameters )
    errStr = "StorageElement.getStorageParameters: Requested protocol supported but no object found."
    self.log.debug( errStr, "%s for %s" % ( protocol, self.name ) )
    return S_ERROR( errStr )

  def isLocalSE( self ):
    """ Test if the Storage Element is local in the current context
    """
    self.log.verbose( "StorageElement.isLocalSE: Determining whether %s is a local SE." % self.name )

    import DIRAC
    localSEs = getSEsForSite( DIRAC.siteName() )['Value']
    if self.name in localSEs:
      return S_OK( True )
    else:
      return S_OK( False )

  #################################################################################################
  #
  # These are the basic get functions for lfn manipulation
  #


  def __getSinglePfnForProtocol( self, pfn, protocol, withPort = True ):
    """ Transform the input pfn into a pfn with the given protocol for the Storage Element.
      :param pfn : input PFN
      :param protocol : string or list of string of the protocol we want
      :param withPort : includes the port in the returned pfn
    """
    self.log.verbose( "StorageElement.getSinglePfnForProtocol: Getting pfn for given protocols in %s." % self.name )


    # This test of the available protocols could actually be done in getPfnForProtocol once for all
    # but it is safer to put it here in case we decide to call this method internally (which I doubt!)
    res = self.getProtocols()
    if not res['OK']:
      return res
    if type( protocol ) == StringType:
      protocols = [protocol]
    elif type( protocol ) == ListType:
      protocols = protocol
    else:
      errStr = "StorageElement.getSinglePfnForProtocol: Supplied protocol must be string or list of strings."
      self.log.debug( errStr, "%s %s" % ( protocol, self.name ) )
      return S_ERROR( errStr )
    availableProtocols = res['Value']
    protocolsToTry = []
    for protocol in protocols:
      if protocol in availableProtocols:
        protocolsToTry.append( protocol )
      else:
        errStr = "StorageElement.getSinglePfnForProtocol: Requested protocol not available for SE."
        self.log.debug( errStr, '%s for %s' % ( protocol, self.name ) )
    if not protocolsToTry:
      errStr = "StorageElement.getSinglePfnForProtocol: None of the requested protocols were available for SE."
      self.log.debug( errStr, '%s for %s' % ( protocol, self.name ) )
      return S_ERROR( errStr )
    # Check all available storages for required protocol then contruct the PFN
    for storage in self.storages:
      res = storage.getParameters()
      if res['Value']['ProtocolName'] in protocolsToTry:
        res = pfnparse( pfn )
        if res['OK']:
          res = storage.getProtocolPfn( res['Value'], withPort )
          if res['OK']:
            return res
    errStr = "StorageElement.getSinglePfnForProtocol: Failed to get PFN for requested protocols."
    self.log.debug( errStr, "%s for %s" % ( protocols, self.name ) )
    return S_ERROR( errStr )



  def getPfnForProtocol( self, pfns, protocol = "SRM2", withPort = True ):
    """ create PFNs strings using protocol :protocol:

    :param self: self reference
    :param list pfns: list of PFNs
    :param str protocol: protocol name (default: 'SRM2')
    :param bool withPort: flag to include port in PFN (default: True)
    """

    if type( pfns ) in StringTypes:
      pfnDict = {pfns:False}
    elif type( pfns ) == ListType:
      pfnDict = {}
      for pfn in pfns:
        pfnDict[pfn] = False
    elif type( pfns ) == DictType:
      pfnDict = pfns
    else:
      errStr = "StorageElement.getLfnForPfn: Supplied pfns must be string, list of strings or a dictionary."
      self.log.debug( errStr )
      return S_ERROR( errStr )


    res = self.isValid( "getPfnForProtocol" )
    if not res["OK"]:
      return res
    retDict = { "Successful" : {}, "Failed" : {}}
    for pfn in pfnDict:
      res = self.__getSinglePfnForProtocol( pfn, protocol, withPort = withPort )
      if res["OK"]:
        retDict["Successful"][pfn] = res["Value"]
      else:
        retDict["Failed"][pfn] = res["Message"]
    return S_OK( retDict )


  def getPfnPath( self, pfn ):
    """  Get the part of the PFN path below the basic storage path.
         This path must coincide with the LFN of the file in order to be compliant with the LHCb conventions.
    """
    self.log.verbose( "StorageElement.getPfnPath: Getting path from pfn in %s." % self.name )
    if not self.valid:
      return S_ERROR( self.errorReason )
    res = pfnparse( pfn )
    if not res['OK']:
      return res
    fullPfnPath = '%s/%s' % ( res['Value']['Path'], res['Value']['FileName'] )

    # Check all available storages and check whether the pfn is for that protocol
    pfnPath = ''
    for storage in self.storages:
      res = storage.isPfnForProtocol( pfn )
      if res['OK']:
        if res['Value']:
          res = storage.getParameters()
          saPath = res['Value']['Path']
          if not saPath:
            # If the sa path doesn't exist then the pfn path is the entire string
            pfnPath = fullPfnPath
          else:
            if re.search( saPath, fullPfnPath ):
              # Remove the sa path from the fullPfnPath
              pfnPath = fullPfnPath.replace( saPath, '' )
      if pfnPath:
        return S_OK( pfnPath )
    # This should never happen. DANGER!!
    errStr = "StorageElement.getPfnPath: Failed to get the pfn path for any of the protocols!!"
    self.log.debug( errStr )
    return S_ERROR( errStr )



  def getLfnForPfn( self, pfns ):
    """ Get the LFN from the PFNS .
        :param lfn : input lfn or lfns (list/dict)
    """

    if type( pfns ) in StringTypes:
      pfnDict = {pfns:False}
    elif type( pfns ) == ListType:
      pfnDict = {}
      for pfn in pfns:
        pfnDict[pfn] = False
    elif type( pfns ) == DictType:
      pfnDict = pfns.copy()
    else:
      errStr = "StorageElement.getLfnForPfn: Supplied pfns must be string, list of strings or a dictionary."
      self.log.debug( errStr )
      return S_ERROR( errStr )


    res = self.isValid( "getPfnPath" )
    if not res['OK']:
      self.log.error( "StorageElement.getLfnForPfn: Failed to instantiate StorageElement at %s" % self.name )
      return res
    retDict = { "Successful" : {}, "Failed" : {} }
    for pfn in pfnDict:
      res = self.getPfnPath( pfn )
      if res["OK"]:
        retDict["Successful"][pfn] = res["Value"]
      else:
        retDict["Failed"][pfn] = res["Message"]
    return S_OK( retDict )



  def __getSinglePfnForLfn( self, lfn ):
    """ Get the full PFN constructed from the LFN.
        :param lfn : input lfn or lfns (list/dict)
    """
    self.log.debug( "StorageElement.__getSinglePfnForLfn: Getting pfn from lfn in %s." % self.name )

    for storage in self.storages:
      res = storage.getPFNBase()
      if res['OK']:
        fullPath = "%s%s" % ( res['Value'], lfn )
        return S_OK( fullPath )
    # This should never happen. DANGER!!
    errStr = "StorageElement.__getSinglePfnForLfn: Failed to get the full pfn for any of the protocols (%s)!!" % ( self.name )
    self.log.debug( errStr )
    return S_ERROR( errStr )

  def getPfnForLfn( self, lfns ):
    """ get PFNs for supplied LFNs at :storageElementName: SE

    :param self: self reference
    :param list lfns: list of LFNs
    :param str stotrageElementName: DIRAC SE name
    """

    if type( lfns ) in StringTypes:
      lfnDict = {lfns:False}
    elif type( lfns ) == ListType:
      lfnDict = {}
      for lfn in lfns:
        lfnDict[lfn] = False
    elif type( lfns ) == DictType:
      lfnDict = lfns.copy()
    else:
      errStr = "StorageElement.getPfnForLfn: Supplied lfns must be string, list of strings or a dictionary."
      self.log.debug( errStr )
      return S_ERROR( errStr )

    if not self.valid:
      return S_ERROR( self.errorReason )

    retDict = { "Successful" : {}, "Failed" : {} }
    for lfn in lfnDict:
      res = self.__getSinglePfnForLfn( lfn )
      if res["OK"]:
        retDict["Successful"][lfn] = res["Value"]
      else:
        retDict["Failed"][lfn] = res["Message"]
    return S_OK( retDict )


  def getPFNBase( self ):
    """ Get the base to construct a PFN
    """
    self.log.verbose( "StorageElement.getPFNBase: Getting pfn base for %s." % self.name )

    if not self.storages:
      return S_ERROR( 'No storages defined' )
    for storage in self.storages:
      result = storage.getPFNBase()
      if result['OK']:
        return result

    return result

  ###########################################################################################
  #
  # This is the generic wrapper for file operations
  #

  def getAccessUrl( self, lfn, protocol = False, singleFile = None ):
    """ execute 'getTransportURL' operation.
      :param str lfn: string, list or dictionnary of lfns
      :param protocol: if no protocol is specified, we will request self.turlProtocols
    """

    self.log.verbose( "StorageElement.getAccessUrl: Getting accessUrl for lfn in %s." % self.name )

    if not protocol:
      protocols = self.turlProtocols
    else:
      protocols = [protocol]


    argDict = {"protocols" : protocols}
    if singleFile is not None:
      argDict["singleFile"] = singleFile

    self.methodName = "getTransportURL"
    return self.__executeMethod( lfn, **argDict )


  def __generatePfnDict( self, lfns, storage ):
    """ Generates a dictionnary (pfn : lfn ), where the pfn are constructed
        from the lfn using the getProtocolPfn method of the storage plugins.
        :param: lfns : dictionnary {lfn:whatever}
        :returns dictionnary {constructed pfn : lfn}
    """
    self.log.verbose( "StorageElement.__generatePfnDict: generating pfn dict for %s lfn in %s." % ( len( lfns ), self.name ) )

    pfnDict = {}  # pfn : lfn
    failed = {}  # lfn : string with errors
    for lfn in lfns:

      if ":" in lfn:
        errStr = "StorageElement.__generatePfnDict: received a pfn as input. It should not happen anymore, please check your code"
        self.log.verbose( errStr, lfn )
      res = pfnparse( lfn )  # pfnparse can take an lfn as input, it will just fill the path and filename
      if not res['OK']:
        errStr = "StorageElement.__generatePfnDict: Failed to parse supplied LFN."
        self.log.debug( errStr, "%s: %s" % ( lfn, res['Message'] ) )
        if lfn not in failed:
          failed[lfn] = ''
        failed[lfn] = "%s %s" % ( failed[lfn], errStr )
      else:
        res = storage.getProtocolPfn( res['Value'], True )
        if not res['OK']:
          errStr = "StorageElement.__generatePfnDict %s." % res['Message']
          self.log.debug( errStr, 'for %s' % ( lfn ) )
          if lfn not in failed:
            failed[lfn] = ''
          failed[lfn] = "%s %s" % ( failed[lfn], errStr )
        else:
          pfnDict[res['Value']] = lfn
    res = S_OK( pfnDict )
    res['Failed'] = failed
    return res


  def __executeMethod( self, lfn, *args, **kwargs ):
    """ Forward the call to each storage in turn until one works.
        The method to be executed is stored in self.methodName
        :param lfn : string, list or dictionnary
        :param *args : variable amount of non-keyword arguments. SHOULD BE EMPTY
        :param **kwargs : keyword arguments
        :returns S_OK( { 'Failed': {lfn : reason} , 'Successful': {lfn : value} } )
                The Failed dict contains the lfn only if the operation failed on all the storages
                The Successful dict contains the value returned by the successful storages.
    """

    removedArgs = {}
    self.log.verbose( "StorageElement.__executeMethod : preparing the execution of %s" % ( self.methodName ) )

    # args should normaly be empty to avoid problem...
    if len( args ):
      self.log.verbose( "StorageElement.__executeMethod: args should be empty!%s" % args )
      # because there is normaly normaly only one kw argument, I can move it from args to kwargs
      methDefaultArgs = StorageElement.__defaultsArguments.get( self.methodName, {} ).keys()
      if len( methDefaultArgs ):
        kwargs[methDefaultArgs[0] ] = args[0]
        args = args[1:]
      self.log.verbose( "StorageElement.__executeMethod: put it in kwargs, but dirty and might be dangerous!args %s kwargs %s" % ( args, kwargs ) )


    # We check the deprecated arguments
    for depArg in StorageElement.__deprecatedArguments:
      if depArg in kwargs:
        self.log.verbose( "StorageElement.__executeMethod: %s is not an allowed argument anymore. Please change your code!" % depArg )
        removedArgs[depArg] = kwargs[depArg]
        del kwargs[depArg]



    # Set default argument if any
    methDefaultArgs = StorageElement.__defaultsArguments.get( self.methodName, {} )
    for argName in methDefaultArgs:
      if argName not in kwargs:
        self.log.debug( "StorageElement.__executeMethod : default argument %s for %s not present.\
         Setting value %s." % ( argName, self.methodName, methDefaultArgs[argName] ) )
        kwargs[argName] = methDefaultArgs[argName]


    if type( lfn ) in StringTypes:
      lfnDict = {lfn:False}
    elif type( lfn ) == ListType:
      lfnDict = {}
      for url in lfn:
        lfnDict[url] = False
    elif type( lfn ) == DictType:
      lfnDict = lfn.copy()
    else:
      errStr = "StorageElement.__executeMethod: Supplied lfns must be string, list of strings or a dictionary."
      self.log.debug( errStr )
      return S_ERROR( errStr )

    self.log.verbose( "StorageElement.__executeMethod: Attempting to perform '%s' operation with %s lfns." % ( self.methodName,
                                                                                                  len( lfnDict ) ) )

    res = self.isValid( operation = self.methodName )
    if not res['OK']:
      return res
    else:
      if not self.valid:
        return S_ERROR( self.errorReason )

    successful = {}
    failed = {}
    localSE = self.isLocalSE()['Value']
    # Try all of the storages one by one
    for storage in self.storages:
      # Determine whether to use this storage object
      res = storage.getParameters()
      useProtocol = True
      if not res['OK']:
        self.log.debug( "StorageElement.__executeMethod: Failed to get storage parameters.", "%s %s" % ( self.name,
                                                                                            res['Message'] ) )
        useProtocol = False
      else:
        protocolName = res['Value']['ProtocolName']
        if not lfnDict:
          useProtocol = False
          self.log.debug( "StorageElement.__executeMethod: No lfns to be attempted for %s protocol." % protocolName )
        elif not ( protocolName in self.remoteProtocols ) and not localSE:
          # If the SE is not local then we can't use local protocols
          useProtocol = False
          self.log.debug( "StorageElement.__executeMethod: Local protocol not appropriate for remote use: %s." % protocolName )
      if useProtocol:
        self.log.verbose( "StorageElement.__executeMethod: Generating %s protocol PFNs for %s." % ( len( lfnDict ),
                                                                                       protocolName ) )
        res = self.__generatePfnDict( lfnDict, storage )
        pfnDict = res['Value']  # pfn : lfn
        failed.update( res['Failed'] )
        if not len( pfnDict ):
          self.log.verbose( "StorageElement.__executeMethod No pfns generated for protocol %s." % protocolName )
        else:
          self.log.verbose( "StorageElement.__executeMethod: Attempting to perform '%s' for %s physical files" % ( self.methodName,
                                                                                                      len( pfnDict ) ) )
          fcn = None
          if hasattr( storage, self.methodName ) and callable( getattr( storage, self.methodName ) ):
            fcn = getattr( storage, self.methodName )
          if not fcn:
            return S_ERROR( "StorageElement.__executeMethod: unable to invoke %s, it isn't a member function of storage" )

          pfnsToUse = {}  # pfn : the value of the lfn dictionary for the lfn of this pfn
          for pfn in pfnDict:
            pfnsToUse[pfn] = lfnDict[pfnDict[pfn]]

          res = fcn( pfnsToUse, *args, **kwargs )

          if not res['OK']:
            errStr = "StorageElement.__executeMethod: Completely failed to perform %s." % self.methodName
            self.log.debug( errStr, '%s for protocol %s: %s' % ( self.name, protocolName, res['Message'] ) )
            for lfn in pfnDict.values():
              if lfn not in failed:
                failed[lfn] = ''
              failed[lfn] += " %s" % ( res['Message'] )  # Concatenate! Not '=' :-)
          else:
            for pfn, lfn in pfnDict.items():
              if pfn not in res['Value']['Successful']:
                if lfn not in failed:
                  failed[lfn] = ''
                if pfn in res['Value']['Failed']:
                  failed[lfn] = "%s %s" % ( failed[lfn], res['Value']['Failed'][pfn] )
                else:
                  failed[lfn] = "%s %s" % ( failed[lfn], 'No error returned from plug-in' )
              else:
                successful[lfn] = res['Value']['Successful'][pfn]
                if lfn in failed:
                  failed.pop( lfn )
                lfnDict.pop( lfn )


    # Ensure backward compatibility for singleFile and singleDirectory for the time of a version
    singleFileOrDir = removedArgs.get( "singleFile", False ) or removedArgs.get( "singleDirectory", False )

    retValue = S_OK( { 'Failed': failed, 'Successful': successful } )

    if singleFileOrDir:
      self.log.verbose( "StorageElement.__executeMethod : use Utils.executeSingleFileOrDirWrapper for backward compatibility. You should fix your code " )
      retValue = Utils.executeSingleFileOrDirWrapper( retValue )

    return retValue



  def __getattr__( self, name ):
    """ Forwards the equivalent Storage calls to StorageElement.__executeMethod"""
    # We take either the equivalent name, or the name itself
    self.methodName = StorageElement.__equivalentMethodNames.get( name, None )

    if self.methodName:
      return self.__executeMethod

    raise AttributeError







