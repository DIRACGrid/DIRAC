""" :mod: SRM2Storage
    =================

    .. module: python
    :synopsis: SRM v2 interface to StorageElement
"""
# # imports
import os
import re
import time
import errno
from types import StringType, StringTypes, ListType, IntType
from stat import S_ISREG, S_ISDIR, S_IMODE, ST_MODE, ST_SIZE
# # from DIRAC
from DIRAC import gLogger, gConfig, S_OK, S_ERROR
from DIRAC.Resources.Utilities.Utils import checkArgumentFormat
from DIRAC.Resources.Storage.StorageBase import StorageBase
from DIRAC.Core.Security.ProxyInfo import getProxyInfo
from DIRAC.ConfigurationSystem.Client.Helpers.Registry import getVOForGroup
from DIRAC.Core.Utilities.Subprocess import pythonCall
from DIRAC.Core.Utilities.Pfn import pfnparse, pfnunparse
from DIRAC.Core.Utilities.List import breakListIntoChunks
from DIRAC.Core.Utilities.File import getSize
from DIRAC.AccountingSystem.Client.Types.DataOperation import DataOperation
from DIRAC.AccountingSystem.Client.DataStoreClient import gDataStoreClient

# # RCSID
__RCSID__ = "$Id$"

class SRM2Storage( StorageBase ):
  """ .. class:: SRM2Storage

  SRM v2 interafce to StorageElement using lcg_util and gfal
  """

  def __init__( self, storageName, protocol, path, host, port, spaceToken, wspath ):
    """ c'tor

    :param self: self reference
    :param str storageName: SE name
    :param str protocol: protocol to use
    :param str path: base path for vo files
    :param str host: SE host
    :param int port: port to use to communicate with :host:
    :param str spaceToken: space token
    :param str wspath: location of SRM on :host:
    """

    self.log = gLogger.getSubLogger( "SRM2Storage", True )

    self.isok = True

    # # placeholder for gfal reference
    self.gfal = None
    # # placeholder for lcg_util reference
    self.lcg_util = None

    # # save c'tor params
    self.protocolName = 'SRM2'
    self.name = storageName
    self.protocol = protocol
    self.path = path
    self.host = host
    self.port = port
    self.wspath = wspath
    self.spaceToken = spaceToken
    self.cwd = self.path
    # # init base class
    StorageBase.__init__( self, self.name, self.path )

    # # stage limit - 12h
    self.stageTimeout = gConfig.getValue( '/Resources/StorageElements/StageTimeout', 12 * 60 * 60 )
    # # 1 file timeout
    self.fileTimeout = gConfig.getValue( '/Resources/StorageElements/FileTimeout', 30 )
    # # nb of surls per gfal call
    self.filesPerCall = gConfig.getValue( '/Resources/StorageElements/FilesPerCall', 20 )
    # # gfal timeout
    self.gfalTimeout = gConfig.getValue( "/Resources/StorageElements/GFAL_Timeout", 100 )
    # # gfal long timeout
    self.gfalLongTimeOut = gConfig.getValue( "/Resources/StorageElements/GFAL_LongTimeout", 1200 )
    # # gfal retry on errno.ECONN
    self.gfalRetry = gConfig.getValue( "/Resources/StorageElements/GFAL_Retry", 3 )

    # # set checksum type, by default this is 0 (GFAL_CKSM_NONE)
    self.checksumType = gConfig.getValue( "/Resources/StorageElements/ChecksumType", 0 )
    # enum gfal_cksm_type, all in lcg_util
    # 	GFAL_CKSM_NONE = 0,
    # 	GFAL_CKSM_CRC32,
    # 	GFAL_CKSM_ADLER32,
    # 	GFAL_CKSM_MD5,
    # 	GFAL_CKSM_SHA1
    # GFAL_CKSM_NULL = 0
    self.checksumTypes = { None : 0, "CRC32" : 1, "ADLER32" : 2,
                           "MD5" : 3, "SHA1" : 4, "NONE" : 0, "NULL" : 0 }
    if self.checksumType:
      if str( self.checksumType ).upper() in self.checksumTypes:
        gLogger.debug( "SRM2Storage: will use %s checksum check" % self.checksumType )
        self.checksumType = self.checksumTypes[ self.checksumType.upper() ]
      else:
        gLogger.warn( "SRM2Storage: unknown checksum type %s, checksum check disabled" )
        # # GFAL_CKSM_NONE
        self.checksumType = 0
    else:
      # # invert and get name
      self.log.debug( "SRM2Storage: will use %s checksum" % dict( zip( self.checksumTypes.values(),
                                                                     self.checksumTypes.keys() ) )[self.checksumType] )

    # setting some variables for use with lcg_utils
    self.nobdii = 1
    self.defaulttype = 2
    self.voName = None
    ret = getProxyInfo( disableVOMS = True )
    if ret['OK'] and 'group' in ret['Value']:
      self.voName = getVOForGroup( ret['Value']['group'] )
    self.verbose = 0
    self.conf_file = 'ignored'
    self.insecure = 0
    self.defaultLocalProtocols = gConfig.getValue( '/Resources/StorageElements/DefaultProtocols', [] )

    self.MAX_SINGLE_STREAM_SIZE = 1024 * 1024 * 10  # 10 MB ???
    self.MIN_BANDWIDTH = 0.5 * ( 1024 * 1024 )  # 0.5 MB/s ???

  def __importExternals( self ):
    """ import lcg_util and gfalthr or gfal

    :param self: self reference
    """
    if ( self.lcg_util ) and ( self.gfal ):
      return S_OK()
    # # get lcg_util
    try:
      import lcg_util
      self.log.debug( "Using lcg_util version %s from %s" % ( lcg_util.lcg_util_version(),
                                                              lcg_util.__file__ ) )
    except ImportError, error:
      errStr = "__importExternals: Failed to import lcg_util"
      gLogger.exception( errStr, "", error )
      return S_ERROR( errStr )
    # # and gfalthr
    try:
      import gfalthr as gfal
      self.log.debug( 'Using gfalthr version %s from %s' % ( gfal.gfal_version(),
                                                             gfal.__file__ ) )
    except ImportError, error:
      self.log.warn( "__importExternals: Failed to import gfalthr: %s." % error )
      # # so gfal maybe?
      try:
        import gfal
        self.log.debug( "Using gfal version %s from %s" % ( gfal.gfal_version(),
                                                            gfal.__file__ ) )
      except ImportError, error:
        errStr = "__importExternals: Failed to import gfal"
        gLogger.exception( errStr, "", error )
        return S_ERROR( errStr )
    self.lcg_util = lcg_util
    self.gfal = gfal
    return S_OK()

################################################################################
#
# The methods below are for manipulating the client
#
################################################################################

  def resetWorkingDirectory( self ):
    """ reset the working directory to the base dir

    :param self: self reference
    """
    self.cwd = self.path

  def changeDirectory( self, directory ):
    """ cd to :directory:

    :param self: self reference
    :param str directory: dir path
    """
    if directory[0] == '/':
      directory = directory.lstrip( '/' )
    self.cwd = '%s/%s' % ( self.cwd, directory )

  def getCurrentURL( self, fileName ):
    """ Obtain the current file URL from the current working directory and the filename

    :param self: self reference
    :param str fileName: path on storage
    """
    # # strip leading / if fileName arg is present
    fileName = fileName.lstrip( "/" ) if fileName else fileName
    try:
      fullUrl = "%s://%s:%s%s%s/%s" % ( self.protocol, self.host, self.port, self.wspath, self.cwd, fileName )
      fullUrl = fullUrl.rstrip( "/" )
      return S_OK( fullUrl )
    except TypeError, error:
      return S_ERROR( "Failed to create URL %s" % error )

  def isPfnForProtocol( self, pfn ):
    """ check if PFN :pfn: is valid for :self.protocol:

    :param self: self reference
    :param str pfn: PFN
    """
    res = pfnparse( pfn )
    if not res['OK']:
      return res
    pfnDict = res['Value']
    return S_OK( pfnDict['Protocol'] == self.protocol )

  def getProtocolPfn( self, pfnDict, withPort ):
    """ construct SURL using :self.host:, :self.protocol: and optionally :self.port: and :self.wspath:

    :param self: self reference
    :param dict pfnDict: pfn dict
    :param bool withPort: include port information
    """
    # For srm2 keep the file name and path
    pfnDict['Protocol'] = self.protocol
    pfnDict['Host'] = self.host
    if not pfnDict['Path'].startswith( self.path ):
      pfnDict['Path'] = os.path.join( self.path, pfnDict['Path'].strip( '/' ) )
    if withPort:
      pfnDict['Port'] = self.port
      pfnDict['WSUrl'] = self.wspath
    else:
      pfnDict['Port'] = ''
      pfnDict['WSUrl'] = ''
    return pfnunparse( pfnDict )

################################################################################
#
# The methods below are URL manipulation methods
#
################################################################################

  def getPFNBase( self, withPort = False ):
    """ This will get the pfn base. This is then appended with the LFN in DIRAC convention.

    :param self: self reference
    :param bool withPort: flag to include port
    """
    return S_OK( { True : 'srm://%s:%s%s' % ( self.host, self.port, self.path ),
                   False : 'srm://%s%s' % ( self.host, self.path ) }[withPort] )

  def getUrl( self, path, withPort = True ):
    """ get SRM PFN for :path: with optional port info

    :param self: self reference
    :param str path: file path
    :param bool withPort: toggle port info
    """
    pfnDict = pfnparse( path )
    if not pfnDict["OK"]:
      self.log.error( "getUrl: %s" % pfnDict["Message"] )
      return pfnDict
    pfnDict = pfnDict['Value']
    if not pfnDict['Path'].startswith( self.path ):
      pfnDict['Path'] = os.path.join( self.path, pfnDict['Path'].strip( '/' ) )
    pfnDict['Protocol'] = 'srm'
    pfnDict['Host'] = self.host
    pfnDict['Port'] = self.port
    pfnDict['WSUrl'] = self.wspath
    if not withPort:
      pfnDict['Port'] = ''
      pfnDict['WSUrl'] = ''
    return pfnunparse( pfnDict )

  def getParameters( self ):
    """ gets all the storage specific parameters pass when instantiating the storage

    :param self: self reference
    """
    return S_OK( { "StorageName" : self.name,
                   "ProtocolName" : self.protocolName,
                   "Protocol" : self.protocol,
                   "Host" : self.host,
                   "Path" : self.path,
                   "Port" : self.port,
                   "SpaceToken" : self.spaceToken,
                   "WSUrl" : self.wspath } )

  #############################################################
  #
  # These are the methods for directory manipulation
  #

  ######################################################################
  #
  # This has to be updated once the new gfal_makedir() becomes available
  # TODO: isn't it there? when somebody made above comment?
  #

  def createDirectory( self, path ):
    """ mkdir -p path on storage

    :param self: self reference
    :param str path:
    """
    urls = checkArgumentFormat( path )
    if not urls['OK']:
      return urls
    urls = urls['Value']

    successful = {}
    failed = {}
    self.log.debug( "createDirectory: Attempting to create %s directories." % len( urls ) )
    for url in urls:
      strippedUrl = url.rstrip( '/' )
      res = self.__makeDirs( strippedUrl )
      if res['OK']:
        self.log.debug( "createDirectory: Successfully created directory on storage: %s" % url )
        successful[url] = True
      else:
        self.log.error( "createDirectory: Failed to create directory on storage.",
                       "\n%s: \n%s" % ( url, res['Message'] ) )
        failed[url] = res['Message']
    return S_OK( { 'Failed' : failed, 'Successful' : successful } )

  def __makeDir( self, path ):
    """ mkdir path in a weird way

    :param self: self reference
    :param str path:
    """
    srcFile = os.path.join( os.environ.get( 'TMPDIR', os.environ.get( 'TMP', '/tmp' ) ), 'dirac_directory' )
    if not os.path.exists( srcFile ):
      dfile = open( srcFile, 'w' )
      dfile.write( " " )
      dfile.close()
    destFile = os.path.join( path, 'dirac_directory.%s' % time.time() )
    res = self.__putFile( srcFile, destFile, 0 )
    self.__executeOperation( destFile, 'removeFile' )
    return res

  def __makeDirs( self, path ):
    """ black magic contained within...

    :param self: self reference
    :param str path: dir name
    """
    res = self.__executeOperation( path, 'exists' )
    if not res['OK']:
      return res
    if res['Value']:
      return S_OK()
    # directory doesn't exist, create it
    dirName = os.path.dirname( path )
    res = self.__executeOperation( dirName, 'exists' )
    if not res['OK']:
      return res
    if not res['Value']:
      res = self.__makeDirs( dirName )
      if not res['OK']:
        return res
    return self.__makeDir( path )

################################################################################
#
# The methods below use the new generic methods for executing operations
#
################################################################################

  def removeFile( self, path ):
    """ rm path on storage

    :param self: self reference
    :param str path: file path
    """
    res = checkArgumentFormat( path )
    if not res['OK']:
      return res
    urls = res['Value']
    self.log.debug( "removeFile: Performing the removal of %s file(s)" % len( urls ) )
    resDict = self.__gfaldeletesurls_wrapper( urls )
    if not resDict["OK"]:
      self.log.error( "removeFile: %s" % resDict["Message"] )
      return resDict
    resDict = resDict['Value']
    failed = resDict['Failed']
    allResults = resDict['AllResults']
    successful = {}
    for urlDict in allResults:
      if urlDict.get( 'surl' ):
        pathSURL = self.getUrl( urlDict['surl'] )
        if not pathSURL["OK"]:
          self.log.error( "removeFile: %s" % pathSURL["Message"] )
          failed[ urlDict['surl'] ] = pathSURL["Message"]
          continue
        pathSURL = pathSURL['Value']
        if urlDict['status'] == 0:
          self.log.debug( "removeFile: Successfully removed file: %s" % pathSURL )
          successful[pathSURL] = True
        elif urlDict['status'] == 2:
          # This is the case where the file doesn't exist.
          self.log.debug( "removeFile: File did not exist, successfully removed: %s" % pathSURL )
          successful[pathSURL] = True
        else:
          errStr = "removeFile: Failed to remove file."
          errMessage = urlDict['ErrorMessage']
          self.log.error( errStr, "%s: %s" % ( pathSURL, errMessage ) )
          failed[pathSURL] = "%s %s" % ( errStr, errMessage )
    return S_OK( { 'Failed' : failed, 'Successful' : successful } )

  def getTransportURL( self, path, protocols = False ):
    """ obtain the tURLs for the supplied path and protocols

    :param self: self reference
    :param str path: path on storage
    :param mixed protocols: protocols to use
    """
    res = checkArgumentFormat( path )
    if not res['OK']:
      return res
    urls = res['Value']

    if not protocols:
      protocols = self.__getProtocols()
      if not protocols['OK']:
        return protocols
      listProtocols = protocols['Value']
    elif type( protocols ) == StringType:
      listProtocols = [protocols]
    elif type( protocols ) == ListType:
      listProtocols = protocols
    else:
      return S_ERROR( "getTransportURL: Must supply desired protocols to this plug-in." )

    self.log.debug( "getTransportURL: Obtaining tURLs for %s file(s)." % len( urls ) )
    resDict = self.__gfalturlsfromsurls_wrapper( urls, listProtocols )
    if not resDict["OK"]:
      self.log.error( "getTransportURL: %s" % resDict["Message"] )
      return resDict
    resDict = resDict['Value']
    failed = resDict['Failed']
    allResults = resDict['AllResults']
    successful = {}
    for urlDict in allResults:
      if urlDict.get( 'surl' ):
        pathSURL = self.getUrl( urlDict['surl'] )
        if not pathSURL["OK"]:
          self.log.error( "getTransportURL: %s" % pathSURL["Message"] )
          failed[ urlDict['surl'] ] = pathSURL["Message"]
          continue
        pathSURL = pathSURL['Value']
        if urlDict['status'] == 0:
          self.log.debug( "getTransportURL: Obtained tURL for file. %s" % pathSURL )
          successful[pathSURL] = urlDict['turl']
        elif urlDict['status'] == 2:
          errMessage = "getTransportURL: File does not exist."
          self.log.error( errMessage, pathSURL )
          failed[pathSURL] = errMessage
        else:
          errStr = "getTransportURL: Failed to obtain turls."
          errMessage = urlDict['ErrorMessage']
          self.log.error( errStr, "%s: %s" % ( pathSURL, errMessage ) )
          failed[pathSURL] = "%s %s" % ( errStr, errMessage )
    return S_OK( { 'Failed' : failed, 'Successful' : successful } )

  def prestageFile( self, path, lifetime = 86400 ):
    """ Issue prestage request for file

    :param self: self reference
    :param str path: PFN path
    :param int lifetime: prestage lifetime in seconds (default 24h)
    """
    res = checkArgumentFormat( path )
    if not res['OK']:
      return res
    urls = res['Value']

    self.log.debug( "prestageFile: Attempting to issue stage requests for %s file(s)." % len( urls ) )
    resDict = self.__gfal_prestage_wrapper( urls, lifetime )
    if not resDict["OK"]:
      self.log.error( "prestageFile: %s" % resDict["Message"] )
      return resDict
    resDict = resDict["Value"]
    failed = resDict['Failed']
    allResults = resDict['AllResults']
    successful = {}
    for urlDict in allResults:
      if urlDict.get( 'surl' ):
        pathSURL = self.getUrl( urlDict['surl'] )
        if not pathSURL["OK"]:
          self.log.error( "prestageFile: %s" % pathSURL["Message"] )
          failed[ urlDict['surl'] ] = pathSURL["Message"]
          continue
        pathSURL = pathSURL['Value']
        if urlDict['status'] == 0:
          self.log.debug( "prestageFile: Issued stage request for file %s." % pathSURL )
          successful[pathSURL] = urlDict['SRMReqID']
        elif urlDict['status'] == 1:
          self.log.debug( "prestageFile: File found to be already staged.", pathSURL )
          successful[pathSURL] = urlDict['SRMReqID']
        elif urlDict['status'] == 22:
          self.log.debug( "prestageFile: Stage request for file %s queued.", pathSURL )
          successful[pathSURL] = urlDict['SRMReqID']
        elif urlDict['status'] == 2:
          errMessage = "prestageFile: File does not exist."
          self.log.error( errMessage, pathSURL )
          failed[pathSURL] = errMessage
        else:
          errStr = "prestageFile: Failed issue stage request."
          errMessage = urlDict['ErrorMessage']
          self.log.error( errStr, "%s: %s" % ( errMessage, pathSURL ) )
          failed[pathSURL] = "%s %s" % ( errStr, errMessage )
    return S_OK( { 'Failed' : failed, 'Successful' : successful } )

  def prestageFileStatus( self, path ):
    """ Monitor prestage request for files
    """
    res = checkArgumentFormat( path )
    if not res['OK']:
      return res
    urls = res['Value']

    self.log.debug( "prestageFileStatus: Attempting to get status "
                    "of stage requests for %s file(s)." % len( urls ) )
    resDict = self.__gfal_prestagestatus_wrapper( urls )
    if not resDict["OK"]:
      self.log.error( "prestageFileStatus: %s" % resDict["Message"] )
      return resDict
    resDict = resDict["Value"]
    failed = resDict['Failed']
    allResults = resDict['AllResults']
    successful = {}
    for urlDict in allResults:
      if urlDict.get( 'surl' ):
        pathSURL = self.getUrl( urlDict['surl'] )
        if not pathSURL["OK"]:
          self.log.error( "prestageFileStatus: %s" % pathSURL["Message"] )
          failed[ urlDict['surl'] ] = pathSURL["Message"]
          continue
        pathSURL = pathSURL['Value']
        if urlDict['status'] == 1:
          self.log.debug( "SRM2Storage.prestageFileStatus: File found to be staged %s." % pathSURL )
          successful[pathSURL] = True
        elif urlDict['status'] == 0:
          self.log.debug( "SRM2Storage.prestageFileStatus: File not staged %s." % pathSURL )
          successful[pathSURL] = False
        elif urlDict['status'] == 2:
          errMessage = "SRM2Storage.prestageFileStatus: File does not exist."
          self.log.error( errMessage, pathSURL )
          failed[pathSURL] = errMessage
        else:
          errStr = "SRM2Storage.prestageFileStatus: Failed get prestage status."
          errMessage = urlDict['ErrorMessage']
          self.log.error( errStr, "%s: %s" % ( errMessage, pathSURL ) )
          failed[pathSURL] = "%s %s" % ( errStr, errMessage )
    return S_OK( { 'Failed' : failed, 'Successful' : successful } )

  def getFileMetadata( self, path ):
    """  Get metadata associated to the file
    """
    res = checkArgumentFormat( path )
    if not res['OK']:
      return res
    urls = {}
    failed = {}
    for url in res['Value']:
      pathSURL = self.getUrl( url )
      if not pathSURL['OK']:
        self.log.error( "getFileMetadata: %s" % pathSURL["Message"] )
        failed[ url ] = pathSURL["Message"]
      else:
        urls[pathSURL['Value'] ] = url

    self.log.debug( "getFileMetadata: Obtaining metadata for %s file(s)." % len( urls ) )
    resDict = self.__gfal_ls_wrapper( urls, 0 )
    if not resDict["OK"]:
      self.log.error( "getFileMetadata: %s" % resDict["Message"] )
      return resDict
    resDict = resDict["Value"]
    failed.update( resDict['Failed'] )
    listOfResults = resDict['AllResults']
    successful = {}
    for urlDict in listOfResults:
      if urlDict.get( 'surl' ):
        # Get back the input value for that surl
        path = urls[self.getUrl( urlDict['surl'] )['Value']]
        if urlDict['status'] == 0:
          statDict = self.__parse_file_metadata( urlDict )
          if statDict['File']:
            successful[path] = statDict
          else:
            errStr = "getFileMetadata: Supplied path is not a file."
            self.log.error( errStr, path )
            failed[path] = errStr
        elif urlDict['status'] == 2:
          errMessage = "getFileMetadata: File does not exist."
          self.log.error( errMessage, path )
          failed[path] = errMessage
        else:
          errStr = "SRM2Storage.getFileMetadata: Failed to get file metadata."
          errMessage = "%s: %s" % ( path, urlDict['ErrorMessage'] )
          self.log.error( errStr, errMessage )
          failed[path] = "%s %s" % ( errStr, urlDict['ErrorMessage'] )
      else:
        errStr = "getFileMetadata: Returned element does not contain surl."
        self.log.fatal( errStr, self.name )
        return S_ERROR( errStr )
    return S_OK( { 'Failed' : failed, 'Successful' : successful } )

  def isFile( self, path ):
    """Check if the given path exists and it is a file
    """
    res = checkArgumentFormat( path )
    if not res['OK']:
      return res
    urls = res['Value']

    self.log.debug( "isFile: Checking whether %s path(s) are file(s)." % len( urls ) )
    resDict = self.__gfal_ls_wrapper( urls, 0 )
    if not resDict["OK"]:
      self.log.error( "isFile: %s" % resDict["Message"] )
      return resDict
    resDict = resDict["Value"]
    failed = resDict['Failed']
    listOfResults = resDict['AllResults']
    successful = {}
    for urlDict in listOfResults:
      if urlDict.get( 'surl' ):
        pathSURL = self.getUrl( urlDict['surl'] )
        if not pathSURL["OK"]:
          self.log.error( "isFile: %s" % pathSURL["Message"] )
          failed[ urlDict['surl'] ] = pathSURL["Message"]
          continue
        pathSURL = pathSURL['Value']
        if urlDict['status'] == 0:
          statDict = self.__parse_file_metadata( urlDict )
          if statDict['File']:
            successful[pathSURL] = True
          else:
            self.log.debug( "isFile: Path is not a file: %s" % pathSURL )
            successful[pathSURL] = False
        elif urlDict['status'] == 2:
          errMessage = "isFile: File does not exist."
          self.log.error( errMessage, pathSURL )
          failed[pathSURL] = errMessage
        else:
          errStr = "isFile: Failed to get file metadata."
          errMessage = urlDict['ErrorMessage']
          self.log.error( errStr, "%s: %s" % ( pathSURL, errMessage ) )
          failed[pathSURL] = "%s %s" % ( errStr, errMessage )
      else:
        errStr = "isFile: Returned element does not contain surl."
        self.log.fatal( errStr, self.name )
        return S_ERROR( errStr )
    return S_OK( { 'Failed' : failed, 'Successful' : successful } )

  def pinFile( self, path, lifetime = 86400 ):
    """ Pin a file with a given lifetime

    :param self: self reference
    :param str path: PFN path
    :param int lifetime: pin lifetime in seconds (default 24h)
    """
    res = checkArgumentFormat( path )
    if not res['OK']:
      return res
    urls = res['Value']

    self.log.debug( "pinFile: Attempting to pin %s file(s)." % len( urls ) )
    resDict = self.__gfal_pin_wrapper( urls, lifetime )
    if not resDict["OK"]:
      self.log.error( "pinFile: %s" % resDict["Message"] )
      return resDict
    resDict = resDict["Value"]
    failed = resDict['Failed']
    allResults = resDict['AllResults']
    successful = {}
    for urlDict in allResults:
      if urlDict.get( 'surl' ):
        pathSURL = self.getUrl( urlDict['surl'] )
        if not pathSURL["OK"]:
          self.log.error( "pinFile: %s" % pathSURL["Message"] )
          failed[ urlDict['surl'] ] = pathSURL["Message"]
          continue
        pathSURL = pathSURL['Value']
        if urlDict['status'] == 0:
          self.log.debug( "pinFile: Issued pin request for file %s." % pathSURL )
          successful[pathSURL] = urlDict['SRMReqID']
        elif urlDict['status'] == 2:
          errMessage = "pinFile: File does not exist."
          self.log.error( errMessage, pathSURL )
          failed[pathSURL] = errMessage
        else:
          errStr = "pinFile: Failed issue pin request."
          errMessage = urlDict['ErrorMessage']
          self.log.error( errStr, "%s: %s" % ( errMessage, pathSURL ) )
          failed[pathSURL] = "%s %s" % ( errStr, errMessage )
    return S_OK( { 'Failed' : failed, 'Successful' : successful } )

  def releaseFile( self, path ):
    """ Release a pinned file

    :param self: self reference
    :param str path: PFN path
    """
    res = checkArgumentFormat( path )
    if not res['OK']:
      return res
    urls = res['Value']

    self.log.debug( "releaseFile: Attempting to release %s file(s)." % len( urls ) )
    resDict = self.__gfal_release_wrapper( urls )
    if not resDict["OK"]:
      self.log.error( "releaseFile: %s" % resDict["Message"] )
      return resDict
    resDict = resDict["Value"]
    failed = resDict['Failed']
    allResults = resDict['AllResults']
    successful = {}
    for urlDict in allResults:
      if urlDict.get( 'surl' ):
        pathSURL = self.getUrl( urlDict['surl'] )
        if not pathSURL["OK"]:
          self.log.error( "releaseFile: %s" % pathSURL["Message"] )
          failed[ urlDict['surl'] ] = pathSURL["Message"]
          continue
        pathSURL = pathSURL['Value']
        if urlDict['status'] == 0:
          self.log.debug( "releaseFile: Issued release request for file %s." % pathSURL )
          successful[pathSURL] = urlDict['SRMReqID']
        elif urlDict['status'] == 2:
          errMessage = "releaseFile: File does not exist."
          self.log.error( errMessage, pathSURL )
          failed[pathSURL] = errMessage
        else:
          errStr = "releaseFile: Failed issue release request."
          errMessage = urlDict['ErrorMessage']
          self.log.error( errStr, "%s: %s" % ( errMessage, pathSURL ) )
          failed[pathSURL] = "%s %s" % ( errStr, errMessage )
    return S_OK( { 'Failed' : failed, 'Successful' : successful } )

  def exists( self, path ):
    """ Check if the given path exists. """
    res = checkArgumentFormat( path )
    if not res['OK']:
      return res
    urls = res['Value']

    self.log.debug( "SRM2Storage.exists: Checking the existance of %s path(s)" % len( urls ) )
    resDict = self.__gfal_ls_wrapper( urls, 0 )
    if not resDict["OK"]:
      self.log.error( "exists: %s" % resDict["Message"] )
      return resDict
    resDict = resDict["Value"]
    failed = resDict['Failed']
    listOfResults = resDict['AllResults']
    successful = {}
    for urlDict in listOfResults:
      if urlDict.get( 'surl' ):
        pathSURL = self.getUrl( urlDict["surl"] )
        if not pathSURL["OK"]:
          self.log.error( "SRM2Storage.exists: %s" % pathSURL["Message"] )
          failed[ urlDict["surl"] ] = pathSURL["Message"]
          continue
        pathSURL = pathSURL["Value"]
        if urlDict['status'] == 0:
          self.log.debug( "SRM2Storage.exists: Path exists: %s" % pathSURL )
          successful[pathSURL] = True
        elif urlDict['status'] == 2:
          self.log.debug( "SRM2Storage.exists: Path does not exist: %s" % pathSURL )
          successful[pathSURL] = False
        else:
          errStr = "SRM2Storage.exists: Failed to get path metadata."
          errMessage = urlDict['ErrorMessage']
          self.log.error( errStr, "%s: %s" % ( pathSURL, errMessage ) )
          failed[pathSURL] = "%s %s" % ( errStr, errMessage )
      else:
        errStr = "SRM2Storage.exists: Returned element does not contain surl."
        self.log.fatal( errStr, self.name )
        return S_ERROR( errStr )
    return S_OK( { 'Failed' : failed, 'Successful' : successful } )

  def getFileSize( self, path ):
    """Get the physical size of the given file
    """
    res = checkArgumentFormat( path )
    if not res['OK']:
      return res
    urls = res['Value']

    self.log.debug( "SRM2Storage.getFileSize: Obtaining the size of %s file(s)." % len( urls ) )
    resDict = self.__gfal_ls_wrapper( urls, 0 )
    if not resDict["OK"]:
      self.log.error( "getFileSize: %s" % resDict["Message"] )
      return resDict
    resDict = resDict["Value"]
    failed = resDict['Failed']
    listOfResults = resDict['AllResults']
    successful = {}
    for urlDict in listOfResults:
      if urlDict.get( 'surl' ):
        pathSURL = self.getUrl( urlDict['surl'] )
        if not pathSURL["OK"]:
          self.log.error( "getFileSize: %s" % pathSURL["Message"] )
          failed[ urlDict['surl'] ] = pathSURL["Message"]
          continue
        pathSURL = pathSURL['Value']
        if urlDict['status'] == 0:
          statDict = self.__parse_file_metadata( urlDict )
          if statDict['File']:
            successful[pathSURL] = statDict['Size']
          else:
            errStr = "SRM2Storage.getFileSize: Supplied path is not a file."
            self.log.error( errStr, pathSURL )
            failed[pathSURL] = errStr
        elif urlDict['status'] == 2:
          errMessage = "SRM2Storage.getFileSize: File does not exist."
          self.log.error( errMessage, pathSURL )
          failed[pathSURL] = errMessage
        else:
          errStr = "SRM2Storage.getFileSize: Failed to get file metadata."
          errMessage = urlDict['ErrorMessage']
          self.log.error( errStr, "%s: %s" % ( pathSURL, errMessage ) )
          failed[pathSURL] = "%s %s" % ( errStr, errMessage )
      else:
        errStr = "SRM2Storage.getFileSize: Returned element does not contain surl."
        self.log.fatal( errStr, self.name )
        return S_ERROR( errStr )
    return S_OK( { 'Failed' : failed, 'Successful' : successful } )

  def putFile( self, path, sourceSize = 0 ):
    res = checkArgumentFormat( path )
    if not res['OK']:
      return res
    urls = res['Value']
    failed = {}
    successful = {}
    for dest_url, src_file in urls.items():
    # Create destination directory
      res = self.__executeOperation( os.path.dirname( dest_url ), 'createDirectory' )
      if not res['OK']:
        failed[dest_url] = res['Message']
      else:
        res = self.__putFile( src_file, dest_url, sourceSize )
        if res['OK']:
          successful[dest_url] = res['Value']
        else:
          failed[dest_url] = res['Message']
    return S_OK( { 'Failed' : failed, 'Successful' : successful } )

  def __putFile( self, src_file, dest_url, sourceSize ):
    """ put :src_file: to :dest_url:

    :param self: self reference
    :param str src_file: file path in local fs
    :param str dest_url: destination url on storage
    :param int sourceSize: :src_file: size in B
    """
    # Pre-transfer check
    res = self.__executeOperation( dest_url, 'exists' )
    if not res['OK']:
      self.log.debug( "__putFile: Failed to find pre-existance of destination file." )
      return res
    if res['Value']:
      res = self.__executeOperation( dest_url, 'removeFile' )
      if not res['OK']:
        self.log.debug( "__putFile: Failed to remove remote file %s." % dest_url )
      else:
        self.log.debug( "__putFile: Removed remote file %s." % dest_url )
    dsttype = self.defaulttype
    src_spacetokendesc = ''
    dest_spacetokendesc = self.spaceToken
    if re.search( 'srm:', src_file ):
      src_url = src_file
      srctype = 2
      if not sourceSize:
        return S_ERROR( "__putFile: For file replication the source file size must be provided." )
    else:
      if not os.path.exists( src_file ):
        errStr = "__putFile: The source local file does not exist."
        self.log.error( errStr, src_file )
        return S_ERROR( errStr )
      sourceSize = getSize( src_file )
      if sourceSize == -1:
        errStr = "__putFile: Failed to get file size."
        self.log.error( errStr, src_file )
        return S_ERROR( errStr )
      src_url = 'file:%s' % src_file
      srctype = 0
    if sourceSize == 0:
      errStr = "__putFile: Source file is zero size."
      self.log.error( errStr, src_file )
      return S_ERROR( errStr )
    timeout = int( sourceSize / self.MIN_BANDWIDTH + 300 )
    if sourceSize > self.MAX_SINGLE_STREAM_SIZE:
      nbstreams = 4
    else:
      nbstreams = 1
    self.log.info( "__putFile: Executing transfer of %s to %s using %s streams" % ( src_url, dest_url, nbstreams ) )
    res = pythonCall( ( timeout + 10 ), self.__lcg_cp_wrapper, src_url, dest_url,
                      srctype, dsttype, nbstreams, timeout, src_spacetokendesc, dest_spacetokendesc )
    if not res['OK']:
      # Remove the failed replica, just in case
      result = self.__executeOperation( dest_url, 'removeFile' )
      if result['OK']:
        self.log.debug( "__putFile: Removed remote file remnant %s." % dest_url )
      else:
        self.log.debug( "__putFile: Unable to remove remote file remnant %s." % dest_url )
      return res
    res = res['Value']
    if not res['OK']:
      # Remove the failed replica, just in case
      result = self.__executeOperation( dest_url, 'removeFile' )
      if result['OK']:
        self.log.debug( "__putFile: Removed remote file remnant %s." % dest_url )
      else:
        self.log.debug( "__putFile: Unable to remove remote file remnant %s." % dest_url )
      return res
    errCode, errStr = res['Value']
    if errCode == 0:
      self.log.info( '__putFile: Successfully put file to storage.' )
      # # checksum check? return!
      if self.checksumType:
        return S_OK( sourceSize )
      # # else compare sizes
      res = self.__executeOperation( dest_url, 'getFileSize' )
      if res['OK']:
        destinationSize = res['Value']
        if sourceSize == destinationSize :
          self.log.debug( "__putFile: Post transfer check successful." )
          return S_OK( destinationSize )
      errorMessage = "__putFile: Source and destination file sizes do not match."
      self.log.error( errorMessage, src_url )
    else:
      errorMessage = "__putFile: Failed to put file to storage."
      if errCode > 0:
        errStr = "%s %s" % ( errStr, os.strerror( errCode ) )
      self.log.error( errorMessage, errStr )
    res = self.__executeOperation( dest_url, 'removeFile' )
    if res['OK']:
      self.log.debug( "__putFile: Removed remote file remnant %s." % dest_url )
    else:
      self.log.debug( "__putFile: Unable to remove remote file remnant %s." % dest_url )
    return S_ERROR( errorMessage )

  def __lcg_cp_wrapper( self, src_url, dest_url, srctype, dsttype, nbstreams,
                        timeout, src_spacetokendesc, dest_spacetokendesc ):
    """ lcg_util.lcg_cp wrapper

    :param self: self reference
    :param str src_url: source SURL
    :param str dest_url: destination SURL
    :param srctype: source SE type
    :param dsttype: destination SE type
    :param int nbstreams: nb of streams used for trasnfer
    :param int timeout: timeout in seconds
    :param str src_spacetoken: source space token
    :param str dest_spacetoken: destination space token
    """
    try:
      errCode, errStr = self.lcg_util.lcg_cp4( src_url,
                                               dest_url,
                                               self.defaulttype,
                                               srctype,
                                               dsttype,
                                               self.nobdii,
                                               self.voName,
                                               nbstreams,
                                               self.conf_file,
                                               self.insecure,
                                               self.verbose,
                                               timeout,
                                               src_spacetokendesc,
                                               dest_spacetokendesc,
                                               self.checksumType )

      if type( errCode ) != IntType:
        self.log.error( "__lcg_cp_wrapper: Returned errCode was not an integer",
                        "%s %s" % ( errCode, type( errCode ) ) )
        if type( errCode ) == ListType:
          msg = []
          for err in errCode:
            msg.append( '%s of type %s' % ( err, type( err ) ) )
          self.log.error( "__lcg_cp_wrapper: Returned errCode was List:\n" , "\n".join( msg ) )
        return S_ERROR( "__lcg_cp_wrapper: Returned errCode was not an integer" )
      if type( errStr ) not in StringTypes:
        self.log.error( "__lcg_cp_wrapper: Returned errStr was not a string",
                       "%s %s" % ( errCode, type( errStr ) ) )
        return S_ERROR( "__lcg_cp_wrapper: Returned errStr was not a string" )
      return S_OK( ( errCode, errStr ) )
    except Exception, error:
      self.log.exception( "__lcg_cp_wrapper", "", error )
      return S_ERROR( "Exception while attempting file upload" )

  def getFile( self, path, localPath = False ):
    """ make a local copy of a storage :path:

    :param self: self reference
    :param str path: path on storage
    :param mixed localPath: if not specified, os.getcwd()
    """
    res = checkArgumentFormat( path )
    if not res['OK']:
      return res
    urls = res['Value']

    failed = {}
    successful = {}
    for src_url in urls:
      fileName = os.path.basename( src_url )
      if localPath:
        dest_file = "%s/%s" % ( localPath, fileName )
      else:
        dest_file = "%s/%s" % ( os.getcwd(), fileName )
      res = self.__getFile( src_url, dest_file )
      if res['OK']:
        successful[src_url] = res['Value']
      else:
        failed[src_url] = res['Message']
    return S_OK( { 'Failed' : failed, 'Successful' : successful } )

  def __getFile( self, src_url, dest_file ):
    """ do a real copy of storage file :src_url: to local fs under :dest_file:

    :param self: self reference
    :param str src_url: SE url to cp
    :param str dest_file: local fs path
    """
    if not os.path.exists( os.path.dirname( dest_file ) ):
      os.makedirs( os.path.dirname( dest_file ) )
    if os.path.exists( dest_file ):
      self.log.debug( "__getFile: Local file already exists %s. Removing..." % dest_file )
      os.remove( dest_file )
    srctype = self.defaulttype
    src_spacetokendesc = self.spaceToken
    dsttype = 0
    dest_spacetokendesc = ''
    dest_url = 'file:%s' % dest_file
    res = self.__executeOperation( src_url, 'getFileSize' )
    if not res['OK']:
      return S_ERROR( res['Message'] )
    remoteSize = res['Value']
    timeout = int( remoteSize / self.MIN_BANDWIDTH * 4 + 300 )
    nbstreams = 1
    self.log.info( "__getFile: Using %d streams" % nbstreams )
    self.log.info( "__getFile: Executing transfer of %s to %s" % ( src_url, dest_url ) )
    res = pythonCall( ( timeout + 10 ), self.__lcg_cp_wrapper, src_url, dest_url, srctype, dsttype,
                      nbstreams, timeout, src_spacetokendesc, dest_spacetokendesc )
    if not res['OK']:
      return res
    res = res['Value']
    if not res['OK']:
      return res
    errCode, errStr = res['Value']
    if errCode == 0:
      self.log.debug( '__getFile: Got a file from storage.' )
      localSize = getSize( dest_file )
      if localSize == remoteSize:
        self.log.debug( "__getFile: Post transfer check successful." )
        return S_OK( localSize )
      errorMessage = "__getFile: Source and destination file sizes do not match."
      self.log.error( errorMessage, src_url )
    else:
      errorMessage = "__getFile: Failed to get file from storage."
      if errCode > 0:
        errStr = "%s %s" % ( errStr, os.strerror( errCode ) )
      self.log.error( errorMessage, errStr )
    if os.path.exists( dest_file ):
      self.log.debug( "__getFile: Removing local file %s." % dest_file )
      os.remove( dest_file )
    return S_ERROR( errorMessage )

  def __executeOperation( self, url, method ):
    """ executes the requested :method: with the supplied url

    :param self: self reference
    :param str url: SE url
    :param str method: fcn name
    """
    fcn = None
    if hasattr( self, method ) and callable( getattr( self, method ) ):
      fcn = getattr( self, method )
    if not fcn:
      return S_ERROR( "Unable to invoke %s, it isn't a member funtion of SRM2Storage" % method )
    res = fcn( url )

    if not res['OK']:
      return res
    elif url not in res['Value']['Successful']:
      if url not in res['Value']['Failed']:
        if res['Value']['Failed'].values():
          return S_ERROR( res['Value']['Failed'].values()[0] )
        elif res['Value']['Successful'].values():
          return S_OK( res['Value']['Successful'].values()[0] )
        else:
          self.log.error( 'Wrong Return structure', str( res['Value'] ) )
          return S_ERROR( 'Wrong Return structure' )
      return S_ERROR( res['Value']['Failed'][url] )
    return S_OK( res['Value']['Successful'][url] )

  ############################################################################################
  #
  # Directory based methods
  #

  def isDirectory( self, path ):
    """ isdir on storage path

    :param self: self reference
    :param str path: SE path
    """
    res = checkArgumentFormat( path )
    if not res['OK']:
      return res
    urls = res['Value']

    self.log.debug( "SRM2Storage.isDirectory: Checking whether %s path(s) are directory(ies)" % len( urls ) )
    resDict = self.__gfal_ls_wrapper( urls, 0 )
    if not resDict["OK"]:
      self.log.error( "isDirectory: %s" % resDict["Message"] )
      return resDict
    resDict = resDict["Value"]
    failed = resDict['Failed']
    listOfResults = resDict['AllResults']
    successful = {}
    for urlDict in listOfResults:
      if urlDict.get( 'surl' ):
        dirSURL = self.getUrl( urlDict['surl'] )
        if not dirSURL["OK"]:
          self.log.error( "isDirectory: %s" % dirSURL["Message"] )
          failed[ urlDict['surl'] ] = dirSURL["Message"]
          continue
        dirSURL = dirSURL['Value']
        if urlDict['status'] == 0:
          statDict = self.__parse_file_metadata( urlDict )
          if statDict['Directory']:
            successful[dirSURL] = True
          else:
            self.log.debug( "SRM2Storage.isDirectory: Path is not a directory: %s" % dirSURL )
            successful[dirSURL] = False
        elif urlDict['status'] == 2:
          self.log.debug( "SRM2Storage.isDirectory: Supplied path does not exist: %s" % dirSURL )
          failed[dirSURL] = 'Directory does not exist'
        else:
          errStr = "SRM2Storage.isDirectory: Failed to get file metadata."
          errMessage = urlDict['ErrorMessage']
          self.log.error( errStr, "%s: %s" % ( dirSURL, errMessage ) )
          failed[dirSURL] = "%s %s" % ( errStr, errMessage )
      else:
        errStr = "SRM2Storage.isDirectory: Returned element does not contain surl."
        self.log.fatal( errStr, self.name )
        return S_ERROR( errStr )
    return S_OK( { 'Failed' : failed, 'Successful' : successful } )

  def getDirectoryMetadata( self, path ):
    """ get the metadata for the directory :path:

    :param self: self reference
    :param str path: SE path
    """
    res = checkArgumentFormat( path )
    if not res['OK']:
      return res
    urls = res['Value']

    self.log.debug( "getDirectoryMetadata: Attempting to obtain metadata for %s directories." % len( urls ) )
    resDict = self.__gfal_ls_wrapper( urls, 0 )
    if not resDict["OK"]:
      self.log.error( "getDirectoryMetadata: %s" % resDict["Message"] )
      return resDict
    resDict = resDict["Value"]
    failed = resDict['Failed']
    listOfResults = resDict['AllResults']
    successful = {}
    for urlDict in listOfResults:
      if "surl" in urlDict and urlDict["surl"]:
        pathSURL = self.getUrl( urlDict['surl'] )
        if not pathSURL["OK"]:
          self.log.error( "getDirectoryMetadata: %s" % pathSURL["Message"] )
          failed[ urlDict['surl'] ] = pathSURL["Message"]
          continue
        pathSURL = pathSURL['Value']
        if urlDict['status'] == 0:
          statDict = self.__parse_file_metadata( urlDict )
          if statDict['Directory']:
            successful[pathSURL] = statDict
          else:
            errStr = "SRM2Storage.getDirectoryMetadata: Supplied path is not a directory."
            self.log.error( errStr, pathSURL )
            failed[pathSURL] = errStr
        elif urlDict['status'] == 2:
          errMessage = "SRM2Storage.getDirectoryMetadata: Directory does not exist."
          self.log.error( errMessage, pathSURL )
          failed[pathSURL] = errMessage
        else:
          errStr = "SRM2Storage.getDirectoryMetadata: Failed to get directory metadata."
          errMessage = urlDict['ErrorMessage']
          self.log.error( errStr, "%s: %s" % ( pathSURL, errMessage ) )
          failed[pathSURL] = "%s %s" % ( errStr, errMessage )
      else:
        errStr = "SRM2Storage.getDirectoryMetadata: Returned element does not contain surl."
        self.log.fatal( errStr, self.name )
        return S_ERROR( errStr )
    return S_OK( { 'Failed' : failed, 'Successful' : successful } )

  def getDirectorySize( self, path ):
    """ Get the size of the directory on the storage
    """
    res = checkArgumentFormat( path )
    if not res['OK']:
      return res
    urls = res['Value']

    self.log.debug( "SRM2Storage.getDirectorySize: Attempting to get size of %s directories." % len( urls ) )
    res = self.listDirectory( urls )
    if not res['OK']:
      return res
    failed = res['Value']['Failed']
    successful = {}
    for directory, dirDict in res['Value']['Successful'].items():
      directorySize = 0
      directoryFiles = 0
      filesDict = dirDict['Files']
      for fileDict in filesDict.itervalues():
        directorySize += fileDict['Size']
        directoryFiles += 1
      self.log.debug( "SRM2Storage.getDirectorySize: Successfully obtained size of %s." % directory )
      subDirectories = len( dirDict['SubDirs'] )
      successful[directory] = { 'Files' : directoryFiles, 'Size' : directorySize, 'SubDirs' : subDirectories }
    return S_OK( { 'Failed' : failed, 'Successful' : successful } )

  def listDirectory( self, path ):
    """ List the contents of the directory on the storage
    """
    res = checkArgumentFormat( path )
    if not res['OK']:
      return res
    urls = res['Value']

    self.log.debug( "SRM2Storage.listDirectory: Attempting to list %s directories." % len( urls ) )

    res = self.isDirectory( urls )
    if not res['OK']:
      return res
    failed = res['Value']['Failed']
    directories = {}
    for url, isDirectory in res['Value']['Successful'].items():
      if isDirectory:
        directories[url] = False
      else:
        errStr = "SRM2Storage.listDirectory: Directory does not exist."
        self.log.error( errStr, url )
        failed[url] = errStr

    resDict = self.__gfal_lsdir_wrapper( directories )
    if not resDict["OK"]:
      self.log.error( "listDirectory: %s" % resDict["Message"] )
      return resDict
    resDict = resDict["Value"]
    # resDict = self.__gfalls_wrapper(directories,1)['Value']
    failed.update( resDict['Failed'] )
    listOfResults = resDict['AllResults']
    successful = {}
    for urlDict in listOfResults:
      if "surl" in urlDict and urlDict["surl"]:
        pathSURL = self.getUrl( urlDict['surl'] )
        if not pathSURL["OK"]:
          self.log.error( "listDirectory: %s" % pathSURL["Message"] )
          failed[ urlDict['surl'] ] = pathSURL["Message"]
          continue
        pathSURL = pathSURL['Value']
        if urlDict['status'] == 0:
          successful[pathSURL] = {}
          self.log.debug( "SRM2Storage.listDirectory: Successfully listed directory %s" % pathSURL )
          subPathDirs = {}
          subPathFiles = {}
          if "subpaths" in urlDict:
            subPaths = urlDict['subpaths']
            # Parse the subpaths for the directory
            for subPathDict in subPaths:
              subPathSURL = self.getUrl( subPathDict['surl'] )['Value']
              if subPathDict['status'] == 22:
                self.log.error( "File found with status 22", subPathDict )
              elif subPathDict['status'] == 0:
                statDict = self.__parse_file_metadata( subPathDict )
                if statDict['File']:
                  subPathFiles[subPathSURL] = statDict
                elif statDict['Directory']:
                  subPathDirs[subPathSURL] = statDict
          # Keep the infomation about this path's subpaths
          successful[pathSURL]['SubDirs'] = subPathDirs
          successful[pathSURL]['Files'] = subPathFiles
        else:
          errStr = "SRM2Storage.listDirectory: Failed to list directory."
          errMessage = urlDict['ErrorMessage']
          self.log.error( errStr, "%s: %s" % ( pathSURL, errMessage ) )
          failed[pathSURL] = "%s %s" % ( errStr, errMessage )
      else:
        errStr = "SRM2Storage.listDirectory: Returned element does not contain surl."
        self.log.fatal( errStr, self.name )
        return S_ERROR( errStr )
    return S_OK( { 'Failed' : failed, 'Successful' : successful } )

  def putDirectory( self, path ):
    """ cp -R local SE
    puts a local directory to the physical storage together with all its files and subdirectories

    :param self: self reference
    :param str path: local fs path
    """
    res = checkArgumentFormat( path )
    if not res['OK']:
      return res
    urls = res['Value']

    successful = {}
    failed = {}
    self.log.debug( "SRM2Storage.putDirectory: Attemping to put %s directories to remote storage." % len( urls ) )
    for destDir, sourceDir in urls.items():
      res = self.__putDir( sourceDir, destDir )
      if res['OK']:
        if res['Value']['AllPut']:
          self.log.debug( "SRM2Storage.putDirectory: Successfully put directory to remote storage: %s" % destDir )
          successful[destDir] = { 'Files' : res['Value']['Files'], 'Size' : res['Value']['Size']}
        else:
          self.log.error( "SRM2Storage.putDirectory: Failed to put entire directory to remote storage.", destDir )
          failed[destDir] = { 'Files' : res['Value']['Files'], 'Size' : res['Value']['Size']}
      else:
        self.log.error( "SRM2Storage.putDirectory: Completely failed to put directory to remote storage.", destDir )
        failed[destDir] = { "Files" : 0, "Size" : 0 }
    return S_OK( { "Failed" : failed, "Successful" : successful } )

  def __putDir( self, src_directory, dest_directory ):
    """ Black magic contained within...
    """
    filesPut = 0
    sizePut = 0
    # Check the local directory exists
    if not os.path.isdir( src_directory ):
      errStr = "SRM2Storage.__putDir: The supplied directory does not exist."
      self.log.error( errStr, src_directory )
      return S_ERROR( errStr )

    # Get the local directory contents
    contents = os.listdir( src_directory )
    allSuccessful = True
    directoryFiles = {}
    for fileName in contents:
      localPath = '%s/%s' % ( src_directory, fileName )
      remotePath = '%s/%s' % ( dest_directory, fileName )
      if not os.path.isdir( localPath ):
        directoryFiles[remotePath] = localPath
      else:
        res = self.__putDir( localPath, remotePath )
        if not res['OK']:
          errStr = "SRM2Storage.__putDir: Failed to put directory to storage."
          self.log.error( errStr, res['Message'] )
        else:
          if not res['Value']['AllPut']:
            pathSuccessful = False
          filesPut += res['Value']['Files']
          sizePut += res['Value']['Size']

    if directoryFiles:
      res = self.putFile( directoryFiles )
      if not res['OK']:
        self.log.error( "SRM2Storage.__putDir: Failed to put files to storage.", res['Message'] )
        allSuccessful = False
      else:
        for fileSize in res['Value']['Successful'].itervalues():
          filesPut += 1
          sizePut += fileSize
        if res['Value']['Failed']:
          allSuccessful = False
    return S_OK( { 'AllPut' : allSuccessful, 'Files' : filesPut, 'Size' : sizePut } )

  def getDirectory( self, path, localPath = False ):
    """ Get a local copy in the current directory of a physical file specified by its path
    """
    res = checkArgumentFormat( path )
    if not res['OK']:
      return res
    urls = res['Value']

    failed = {}
    successful = {}
    self.log.debug( "SRM2Storage.getDirectory: Attempting to get local copies of %s directories." % len( urls ) )
    for src_dir in urls:
      dirName = os.path.basename( src_dir )
      if localPath:
        dest_dir = "%s/%s" % ( localPath, dirName )
      else:
        dest_dir = "%s/%s" % ( os.getcwd(), dirName )
      res = self.__getDir( src_dir, dest_dir )
      if res['OK']:
        if res['Value']['AllGot']:
          self.log.debug( "SRM2Storage.getDirectory: Successfully got local copy of %s" % src_dir )
          successful[src_dir] = {'Files':res['Value']['Files'], 'Size':res['Value']['Size']}
        else:
          self.log.error( "SRM2Storage.getDirectory: Failed to get entire directory.", src_dir )
          failed[src_dir] = {'Files':res['Value']['Files'], 'Size':res['Value']['Size']}
      else:
        self.log.error( "SRM2Storage.getDirectory: Completely failed to get local copy of directory.", src_dir )
        failed[src_dir] = {'Files':0, 'Size':0}
    return S_OK( {'Failed' : failed, 'Successful' : successful } )

  def __getDir( self, srcDirectory, destDirectory ):
    """ Black magic contained within...
    """
    filesGot = 0
    sizeGot = 0

    # Check the remote directory exists
    res = self.__executeOperation( srcDirectory, 'isDirectory' )
    if not res['OK']:
      self.log.error( "SRM2Storage.__getDir: Failed to find the supplied source directory.", srcDirectory )
      return res
    if not res['Value']:
      errStr = "SRM2Storage.__getDir: The supplied source path is not a directory."
      self.log.error( errStr, srcDirectory )
      return S_ERROR( errStr )

    # Check the local directory exists and create it if not
    if not os.path.exists( destDirectory ):
      os.makedirs( destDirectory )

    # Get the remote directory contents
    res = self.__getDirectoryContents( srcDirectory )
    if not res['OK']:
      errStr = "SRM2Storage.__getDir: Failed to list the source directory."
      self.log.error( errStr, srcDirectory )
    filesToGet = res['Value']['Files']
    subDirs = res['Value']['SubDirs']

    allSuccessful = True
    res = self.getFile( filesToGet.keys(), destDirectory )
    if not res['OK']:
      self.log.error( "SRM2Storage.__getDir: Failed to get files from storage.", res['Message'] )
      allSuccessful = False
    else:
      for fileSize in res['Value']['Successful'].itervalues():
        filesGot += 1
        sizeGot += fileSize
      if res['Value']['Failed']:
        allSuccessful = False

    for subDir in subDirs:
      subDirName = os.path.basename( subDir )
      localPath = '%s/%s' % ( destDirectory, subDirName )
      res = self.__getDir( subDir, localPath )
      if res['OK']:
        if not res['Value']['AllGot']:
          allSuccessful = True
        filesGot += res['Value']['Files']
        sizeGot += res['Value']['Size']

    return S_OK( { 'AllGot' : allSuccessful, 'Files' : filesGot, 'Size' : sizeGot } )

  def removeDirectory( self, path, recursive = False ):
    """ Remove a directory
    """
    if recursive:
      return self.__removeDirectoryRecursive( path )
    else:
      return self.__removeDirectory( path )

  def __removeDirectory( self, directory ):
    """ This function removes the directory on the storage
    """
    res = checkArgumentFormat( directory )
    if not res['OK']:
      return res
    urls = res['Value']
    self.log.debug( "SRM2Storage.__removeDirectory: Attempting to remove %s directories." % len( urls ) )
    resDict = self.__gfal_removedir_wrapper( urls )
    if not resDict["OK"]:
      self.log.error( "__removeDirectory: %s" % resDict["Message"] )
      return resDict
    resDict = resDict["Value"]
    failed = resDict['Failed']
    allResults = resDict['AllResults']
    successful = {}
    for urlDict in allResults:
      if "surl" in urlDict:
        pathSURL = urlDict['surl']
        if urlDict['status'] == 0:
          self.log.debug( "__removeDirectory: Successfully removed directory: %s" % pathSURL )
          successful[pathSURL] = True
        elif urlDict['status'] == 2:
          # This is the case where the file doesn't exist.
          self.log.debug( "__removeDirectory: Directory did not exist, sucessfully removed: %s" % pathSURL )
          successful[pathSURL] = True
        else:
          errStr = "removeDirectory: Failed to remove directory."
          errMessage = urlDict['ErrorMessage']
          self.log.error( errStr, "%s: %s" % ( pathSURL, errMessage ) )
          failed[pathSURL] = "%s %s" % ( errStr, errMessage )
    return S_OK( { 'Failed' : failed, 'Successful' : successful } )

  def __removeDirectoryRecursive( self, directory ):
    """ Recursively removes the directory and sub dirs. Repeatedly calls itself to delete recursively.
    """
    res = checkArgumentFormat( directory )
    if not res['OK']:
      return res
    urls = res['Value']

    successful = {}
    failed = {}
    self.log.debug( "SRM2Storage.__removeDirectory: Attempting to recursively remove %s directories." % len( urls ) )
    for directory in urls:
      self.log.debug( "SRM2Storage.removeDirectory: Attempting to remove %s" % directory )
      res = self.__getDirectoryContents( directory )
      resDict = {'FilesRemoved':0, 'SizeRemoved':0}
      if not res['OK']:
        failed[directory] = resDict
      else:
        filesToRemove = res['Value']['Files']
        subDirs = res['Value']['SubDirs']
        # Remove all the files in the directory
        res = self.__removeDirectoryFiles( filesToRemove )
        resDict['FilesRemoved'] += res['FilesRemoved']
        resDict['SizeRemoved'] += res['SizeRemoved']
        allFilesRemoved = res['AllRemoved']
        # Remove all the sub-directories
        res = self.__removeSubDirectories( subDirs )
        resDict['FilesRemoved'] += res['FilesRemoved']
        resDict['SizeRemoved'] += res['SizeRemoved']
        allSubDirsRemoved = res['AllRemoved']
        # If all the files and sub-directories are removed then remove the directory
        allRemoved = False
        if allFilesRemoved and allSubDirsRemoved:
          self.log.debug( "SRM2Storage.removeDirectory: Successfully removed all files and sub-directories." )
          res = self.__removeDirectory( directory )
          if res['OK']:
            if directory in res['Value']['Successful']:
              self.log.debug( "SRM2Storage.removeDirectory: Successfully removed the directory %s." % directory )
              allRemoved = True
        # Report the result
        if allRemoved:
          successful[directory] = resDict
        else:
          failed[directory] = resDict
    return S_OK ( { 'Failed' : failed, 'Successful' : successful } )

  def __getDirectoryContents( self, directory ):
    """ ls of storage element :directory:

    :param self: self reference
    :param str directory: SE path
    """
    directory = directory.rstrip( '/' )
    errMessage = "SRM2Storage.__getDirectoryContents: Failed to list directory."
    res = self.__executeOperation( directory, 'listDirectory' )
    if not res['OK']:
      self.log.error( errMessage, res['Message'] )
      return S_ERROR( errMessage )
    surlsDict = res['Value']['Files']
    subDirsDict = res['Value']['SubDirs']
    filesToRemove = dict( [ ( url, surlsDict[url]['Size'] ) for url in  surlsDict  ] )
    return S_OK ( { 'Files' : filesToRemove, 'SubDirs' : subDirsDict.keys() } )

  def __removeDirectoryFiles( self, filesToRemove ):
    """ rm files from SE

    :param self: self reference
    :param dict filesToRemove: dict with surls as keys
    """
    resDict = { 'FilesRemoved' : 0, 'SizeRemoved' : 0, 'AllRemoved' : True }
    if len( filesToRemove ) > 0:
      res = self.removeFile( filesToRemove.keys() )
      if res['OK']:
        for removedSurl in res['Value']['Successful']:
          resDict['FilesRemoved'] += 1
          resDict['SizeRemoved'] += filesToRemove[removedSurl]
        if res['Value']['Failed']:
          resDict['AllRemoved'] = False
    self.log.debug( "SRM2Storage.__removeDirectoryFiles:",
                    "Removed %s files of size %s bytes." % ( resDict['FilesRemoved'], resDict['SizeRemoved'] ) )
    return resDict

  def __removeSubDirectories( self, subDirectories ):
    """ rm -rf sub-directories

    :param self: self reference
    :param dict subDirectories: dict with surls as keys
    """
    resDict = { 'FilesRemoved' : 0, 'SizeRemoved' : 0, 'AllRemoved' : True }
    if len( subDirectories ) > 0:
      res = self.__removeDirectoryRecursive( subDirectories )
      if res['OK']:
        for removedSubDir, removedDict in res['Value']['Successful'].items():
          resDict['FilesRemoved'] += removedDict['FilesRemoved']
          resDict['SizeRemoved'] += removedDict['SizeRemoved']
          self.log.debug( "SRM2Storage.__removeSubDirectories:",
                         "Removed %s files of size %s bytes from %s." % ( removedDict['FilesRemoved'],
                                                                          removedDict['SizeRemoved'],
                                                                          removedSubDir ) )
        for removedSubDir, removedDict in res['Value']['Failed'].items():
          resDict['FilesRemoved'] += removedDict['FilesRemoved']
          resDict['SizeRemoved'] += removedDict['SizeRemoved']
          self.log.debug( "SRM2Storage.__removeSubDirectories:",
                         "Removed %s files of size %s bytes from %s." % ( removedDict['FilesRemoved'],
                                                                          removedDict['SizeRemoved'],
                                                                          removedSubDir ) )
        if len( res['Value']['Failed'] ) != 0:
          resDict['AllRemoved'] = False
    return resDict

  @staticmethod
  def __parse_stat( stat ):
    """ get size, ftype and mode from stat struct

    :param stat: stat struct
    """
    statDict = { 'File' : False, 'Directory' : False }
    if S_ISREG( stat[ST_MODE] ):
      statDict['File'] = True
      statDict['Size'] = stat[ST_SIZE]
    if S_ISDIR( stat[ST_MODE] ):
      statDict['Directory'] = True
    statDict['Mode'] = S_IMODE( stat[ST_MODE] )
    return statDict

  def __parse_file_metadata( self, urlDict ):
    """ parse and save bits and pieces of metadata info

    :param self: self reference
    :param urlDict: gfal call results
    """

    statDict = self.__parse_stat( urlDict['stat'] )
    if statDict['File']:

      statDict.setdefault( "Checksum", "" )
      if "checksum" in urlDict and ( urlDict['checksum'] != '0x' ):
        statDict["Checksum"] = urlDict["checksum"]

      if 'locality' in urlDict:
        urlLocality = urlDict['locality']
        if re.search( 'ONLINE', urlLocality ):
          statDict['Cached'] = 1
        else:
          statDict['Cached'] = 0
        if re.search( 'NEARLINE', urlLocality ):
          statDict['Migrated'] = 1
        else:
          statDict['Migrated'] = 0
        statDict['Lost'] = 0
        if re.search( 'LOST', urlLocality ):
          statDict['Lost'] = 1
        statDict['Unavailable'] = 0
        if re.search( 'UNAVAILABLE', urlLocality ):
          statDict['Unavailable'] = 1

    return statDict

  def __getProtocols( self ):
    """ returns list of protocols to use at a given site

    :warn: priority is given to a protocols list defined in the CS

    :param self: self reference
    """
    sections = gConfig.getSections( '/Resources/StorageElements/%s/' % ( self.name ) )
    if not sections['OK']:
      return sections

    protocolsList = []
    for section in sections['Value']:
      path = '/Resources/StorageElements/%s/%s/ProtocolName' % ( self.name, section )
      if gConfig.getValue( path, '' ) == self.protocolName:
        protPath = '/Resources/StorageElements/%s/%s/ProtocolsList' % ( self.name, section )
        siteProtocols = gConfig.getValue( protPath, [] )
        if siteProtocols:
          self.log.debug( 'Found SE protocols list to override defaults:', ', '.join( siteProtocols, ) )
          protocolsList = siteProtocols

    if not protocolsList:
      self.log.debug( "SRM2Storage.getTransportURL: No protocols provided, using defaults." )
      protocolsList = gConfig.getValue( '/Resources/StorageElements/DefaultProtocols', [] )

    if not protocolsList:
      return S_ERROR( "SRM2Storage.getTransportURL: No local protocols defined and no defaults found" )

    return S_OK( protocolsList )

  def getCurrentStatus(self):
    """ Get the current status (lcg_stmd), needed for RSS
    """
    res = self.__importExternals()
    if not res['OK']:
      return S_ERROR("Cannot import externals")
    if not self.spaceToken:
      return S_ERROR("Space token not defined for SE")
    #make the endpoint
    endpoint = 'httpg://%s:%s%s' % ( self.host, self.port, self.wspath )
    endpoint = endpoint.replace( '?SFN=', '' )
    res = pythonCall(10, self.lcg_util.lcg_stmd, self.spaceToken, endpoint, 1, 0)
    if not res['OK']:
      return res
    status, resdict, errmessage = res['Value']
    if status != 0:
      return S_ERROR("lcg_util.lcg_stmd failed: %s" % errmessage)
    
    return S_OK(resdict[0])

#######################################################################
#
# These methods wrap the gfal functionality with the accounting. All these are based on __gfal_operation_wrapper()
#
#######################################################################

  def __gfal_lsdir_wrapper( self, urls ):
    """ This is a hack because the structures returned by the different SEs are different
    """
    step = 200
    gfalDict = {}
    gfalDict['defaultsetype'] = 'srmv2'
    gfalDict['no_bdii_check'] = 1
    gfalDict['srmv2_lslevels'] = 1
    gfalDict['srmv2_lscount'] = step
    failed = {}
    successful = []
    for url in urls:
      allResults = []
      gfalDict['surls'] = [url]
      gfalDict['nbfiles'] = 1
      gfalDict['timeout'] = self.gfalLongTimeOut
      allObtained = False
      iteration = 0
      while not allObtained:
        gfalDict['srmv2_lsoffset'] = iteration * step
        iteration += 1
        res = self.__gfal_operation_wrapper( 'gfal_ls', gfalDict )
        # gDataStoreClient.addRegister( res['AccountingOperation'] )
        if not res['OK']:
          if re.search( '\[SE\]\[Ls\]\[SRM_FAILURE\]', res['Message'] ):
            allObtained = True
          else:
            failed[url] = res['Message']
        else:
          results = res['Value']
          tempStep = step
          if len( results ) == 1:
            for result in results:
              if 'subpaths' in result:
                results = result['subpaths']
                tempStep = step - 1
              elif re.search( result['surl'], url ):
                results = []
          allResults.extend( results )
          if len( results ) < tempStep:
            allObtained = True
      successful.append( { 'surl' : url, 'status' : 0, 'subpaths' : allResults } )
    # gDataStoreClient.commit()
    return S_OK( { "AllResults" : successful, "Failed" : failed } )

  def __gfal_ls_wrapper( self, urls, depth ):
    """ gfal_ls wrapper

    :param self: self reference
    :param list urls: urls to check
    :param int depth: srmv2_lslevel (0 or 1)
    """
    gfalDict = {}
    gfalDict['defaultsetype'] = 'srmv2'
    gfalDict['no_bdii_check'] = 1
    gfalDict['srmv2_lslevels'] = depth

    allResults = []
    failed = {}
    listOfLists = breakListIntoChunks( urls.keys(), self.filesPerCall )
    for urls in listOfLists:
      gfalDict['surls'] = urls
      gfalDict['nbfiles'] = len( urls )
      gfalDict['timeout'] = self.fileTimeout * len( urls )
      res = self.__gfal_operation_wrapper( 'gfal_ls', gfalDict )
      # gDataStoreClient.addRegister( res['AccountingOperation'] )
      if not res['OK']:
        for url in urls:
          failed[url] = res['Message']
      else:
        allResults.extend( res['Value'] )

    # gDataStoreClient.commit()
    return S_OK( { "AllResults" : allResults, "Failed" : failed } )

  def __gfal_prestage_wrapper( self, urls, lifetime ):
    """ gfal_prestage wrapper

    :param self: self refefence
    :param list urls: urls to prestage
    :param int lifetime: prestage lifetime
    """
    gfalDict = {}
    gfalDict['defaultsetype'] = 'srmv2'
    gfalDict['no_bdii_check'] = 1
    gfalDict['srmv2_spacetokendesc'] = self.spaceToken
    gfalDict['srmv2_desiredpintime'] = lifetime
    gfalDict['protocols'] = self.defaultLocalProtocols
    allResults = []
    failed = {}

    listOfLists = breakListIntoChunks( urls.keys(), self.filesPerCall )
    for urls in listOfLists:
      gfalDict['surls'] = urls
      gfalDict['nbfiles'] = len( urls )
      gfalDict['timeout'] = self.stageTimeout
      res = self.__gfal_operation_wrapper( 'gfal_prestage',
                                           gfalDict,
                                           timeout_sendreceive = self.fileTimeout * len( urls ) )
      gDataStoreClient.addRegister( res['AccountingOperation'] )
      if not res['OK']:
        for url in urls:
          failed[url] = res['Message']
      else:
        allResults.extend( res['Value'] )

    # gDataStoreClient.commit()
    return S_OK( { "AllResults" : allResults, "Failed" : failed } )

  def __gfalturlsfromsurls_wrapper( self, urls, listProtocols ):
    """ This is a function that can be reused everywhere to perform the gfal_turlsfromsurls
    """
    gfalDict = {}
    gfalDict['defaultsetype'] = 'srmv2'
    gfalDict['no_bdii_check'] = 1
    gfalDict['protocols'] = listProtocols
    gfalDict['srmv2_spacetokendesc'] = self.spaceToken
    allResults = []
    failed = {}

    listOfLists = breakListIntoChunks( urls.keys(), self.filesPerCall )
    for urls in listOfLists:
      gfalDict['surls'] = urls
      gfalDict['nbfiles'] = len( urls )
      gfalDict['timeout'] = self.fileTimeout * len( urls )
      res = self.__gfal_operation_wrapper( 'gfal_turlsfromsurls', gfalDict )
      gDataStoreClient.addRegister( res['AccountingOperation'] )
      if not res['OK']:
        for url in urls:
          failed[url] = res['Message']
      else:
        allResults.extend( res['Value'] )

    # gDataStoreClient.commit()
    return S_OK( { "AllResults" : allResults, "Failed" : failed } )

  def __gfaldeletesurls_wrapper( self, urls ):
    """ This is a function that can be reused everywhere to perform the gfal_deletesurls
    """
    gfalDict = {}
    gfalDict['defaultsetype'] = 'srmv2'
    gfalDict['no_bdii_check'] = 1

    allResults = []
    failed = {}

    listOfLists = breakListIntoChunks( urls.keys(), self.filesPerCall )
    for urls in listOfLists:
      gfalDict['surls'] = urls
      gfalDict['nbfiles'] = len( urls )
      gfalDict['timeout'] = self.fileTimeout * len( urls )
      res = self.__gfal_operation_wrapper( 'gfal_deletesurls', gfalDict )
      gDataStoreClient.addRegister( res['AccountingOperation'] )
      if not res['OK']:
        for url in urls:
          failed[url] = res['Message']
      else:
        allResults.extend( res['Value'] )

    # gDataStoreClient.commit()
    return S_OK( { "AllResults" : allResults, "Failed" : failed } )

  def __gfal_removedir_wrapper( self, urls ):
    """ This is a function that can be reused everywhere to perform the gfal_removedir
    """
    gfalDict = {}
    gfalDict['defaultsetype'] = 'srmv2'
    gfalDict['no_bdii_check'] = 1
    gfalDict['srmv2_spacetokendesc'] = self.spaceToken
    allResults = []
    failed = {}

    listOfLists = breakListIntoChunks( urls.keys(), self.filesPerCall )
    for urls in listOfLists:
      gfalDict['surls'] = urls
      gfalDict['nbfiles'] = len( urls )
      gfalDict['timeout'] = self.fileTimeout * len( urls )
      res = self.__gfal_operation_wrapper( 'gfal_removedir', gfalDict )
      gDataStoreClient.addRegister( res['AccountingOperation'] )
      if not res['OK']:
        for url in urls:
          failed[url] = res['Message']
      else:
        allResults.extend( res['Value'] )

    # gDataStoreClient.commit()
    return S_OK( { "AllResults" : allResults, "Failed" : failed } )

  def __gfal_pin_wrapper( self, urls, lifetime ):
    """ gfal_pin wrapper

    :param self: self reference
    :param dict urls: dict { url : srmRequestID }
    :param int lifetime: pin lifetime in seconds
    """
    gfalDict = {}
    gfalDict['defaultsetype'] = 'srmv2'
    gfalDict['no_bdii_check'] = 0
    gfalDict['srmv2_spacetokendesc'] = self.spaceToken
    gfalDict['srmv2_desiredpintime'] = lifetime

    allResults = []
    failed = {}

    srmRequestFiles = {}
    for url, srmRequestID in urls.items():
      if srmRequestID not in srmRequestFiles:
        srmRequestFiles[srmRequestID] = []
      srmRequestFiles[srmRequestID].append( url )

    for srmRequestID, urls in srmRequestFiles.items():
      listOfLists = breakListIntoChunks( urls, self.filesPerCall )
      for urls in listOfLists:
        gfalDict['surls'] = urls
        gfalDict['nbfiles'] = len( urls )
        gfalDict['timeout'] = self.fileTimeout * len( urls )
        res = self.__gfal_operation_wrapper( 'gfal_pin', gfalDict, srmRequestID = srmRequestID )
        gDataStoreClient.addRegister( res['AccountingOperation'] )
        if not res['OK']:
          for url in urls:
            failed[url] = res['Message']
        else:
          allResults.extend( res['Value'] )

    # gDataStoreClient.commit()
    return S_OK( { "AllResults" : allResults, "Failed" : failed } )

  def __gfal_prestagestatus_wrapper( self, urls ):
    """ gfal_prestagestatus wrapper

    :param self: self reference
    :param dict urls: dict { srmRequestID : [ url, url ] }
    """
    gfalDict = {}
    gfalDict['defaultsetype'] = 'srmv2'
    gfalDict['no_bdii_check'] = 0
    gfalDict['srmv2_spacetokendesc'] = self.spaceToken

    allResults = []
    failed = {}

    srmRequestFiles = {}
    for url, srmRequestID in urls.items():
      if srmRequestID not in srmRequestFiles:
        srmRequestFiles[srmRequestID] = []
      srmRequestFiles[srmRequestID].append( url )

    for srmRequestID, urls in srmRequestFiles.items():
      listOfLists = breakListIntoChunks( urls, self.filesPerCall )
      for urls in listOfLists:
        gfalDict['surls'] = urls
        gfalDict['nbfiles'] = len( urls )
        gfalDict['timeout'] = self.fileTimeout * len( urls )
        res = self.__gfal_operation_wrapper( 'gfal_prestagestatus', gfalDict, srmRequestID = srmRequestID )
        gDataStoreClient.addRegister( res['AccountingOperation'] )
        if not res['OK']:
          for url in urls:
            failed[url] = res['Message']
        else:
          allResults.extend( res['Value'] )

    # gDataStoreClient.commit()
    return S_OK( { "AllResults" : allResults, "Failed" : failed } )

  def __gfal_release_wrapper( self, urls ):
    """ gfal_release wrapper

    :param self: self reference
    :param dict urls: dict { url : srmRequestID }
    """
    gfalDict = {}
    gfalDict['defaultsetype'] = 'srmv2'
    gfalDict['no_bdii_check'] = 0

    allResults = []
    failed = {}

    srmRequestFiles = {}
    for url, srmRequestID in urls.items():
      if srmRequestID not in srmRequestFiles:
        srmRequestFiles[srmRequestID] = []
      srmRequestFiles[srmRequestID].append( url )

    for srmRequestID, urls in srmRequestFiles.items():
      listOfLists = breakListIntoChunks( urls, self.filesPerCall )
      for urls in listOfLists:
        gfalDict['surls'] = urls
        gfalDict['nbfiles'] = len( urls )
        gfalDict['timeout'] = self.fileTimeout * len( urls )
        res = self.__gfal_operation_wrapper( 'gfal_release', gfalDict, srmRequestID = srmRequestID )
        gDataStoreClient.addRegister( res['AccountingOperation'] )
        if not res['OK']:
          for url in urls:
            failed[url] = res['Message']
        else:
          allResults.extend( res['Value'] )

    # gDataStoreClient.commit()
    return S_OK( { "AllResults" : allResults, "Failed" : failed } )

  def __gfal_operation_wrapper( self, operation, gfalDict, srmRequestID = None, timeout_sendreceive = None ):
    """ gfal fcn call wrapper

    :param self: self reference
    :param str operation: gfal fcn name
    :param dict gfalDict: gfal dict passed to create gfal object
    :param srmRequestID: srmRequestID
    :param int timeout_sendreceive: gfal sendreceive timeout in seconds
    """
    # Create an accounting DataOperation record for each operation
    oDataOperation = self.__initialiseAccountingObject( operation, self.name, gfalDict['nbfiles'] )

    oDataOperation.setStartTime()
    start = time.time()

    res = self.__importExternals()
    if not res['OK']:
      oDataOperation.setEndTime()
      oDataOperation.setValueByKey( 'TransferTime', 0. )
      oDataOperation.setValueByKey( 'TransferOK', 0 )
      oDataOperation.setValueByKey( 'FinalStatus', 'Failed' )
      res['AccountingOperation'] = oDataOperation
      return res

    # # timeout for one gfal_exec call
    timeout = gfalDict['timeout'] if not timeout_sendreceive else timeout_sendreceive
    # # pythonCall timeout ( const + timeout * ( 2 ** retry )
    pyTimeout = 300 + ( timeout * ( 2 ** self.gfalRetry ) )
    res = pythonCall( pyTimeout, self.__gfal_wrapper, operation, gfalDict, srmRequestID, timeout_sendreceive )

    end = time.time()
    oDataOperation.setEndTime()
    oDataOperation.setValueByKey( 'TransferTime', end - start )

    if not res['OK']:
      oDataOperation.setValueByKey( 'TransferOK', 0 )
      oDataOperation.setValueByKey( 'FinalStatus', 'Failed' )
      res['AccountingOperation'] = oDataOperation
      return res
    res = res['Value']
    if not res['OK']:
      oDataOperation.setValueByKey( 'TransferOK', 0 )
      oDataOperation.setValueByKey( 'FinalStatus', 'Failed' )

    res['AccountingOperation'] = oDataOperation
    return res

  def __gfal_wrapper( self, operation, gfalDict, srmRequestID = None, timeout_sendreceive = None ):
    """ execute gfal :operation:

    1. create gfalObject from gfalDict
    2. set srmRequestID
    3. call __gfal_exec
    4. get gfal ids
    5. get gfal results
    6. destroy gfal object

    :param self: self reference
    :param str operation: fcn to call
    :param dict gfalDict: gfal config dict
    :param srmRequestID: srm request id
    :param int timeout_sendrecieve: timeout for gfal send request and recieve results in seconds
    """
    gfalObject = self.__create_gfal_object( gfalDict )
    if not gfalObject["OK"]:
      return gfalObject
    gfalObject = gfalObject['Value']

    if srmRequestID:
      res = self.__gfal_set_ids( gfalObject, srmRequestID )
      if not res['OK']:
        return res

    res = self.__gfal_exec( gfalObject, operation, timeout_sendreceive )
    if not res['OK']:
      return res

    gfalObject = res['Value']
    res = self.__gfal_get_ids( gfalObject )
    if not res['OK']:
      newSRMRequestID = srmRequestID
    else:
      newSRMRequestID = res['Value']

    res = self.__get_results( gfalObject )
    if not res['OK']:
      return res

    resultList = []
    pfnRes = res['Value']
    for myDict in pfnRes:
      myDict['SRMReqID'] = newSRMRequestID
      resultList.append( myDict )

    self.__destroy_gfal_object( gfalObject )

    return S_OK( resultList )

  @staticmethod
  def __initialiseAccountingObject( operation, se, files ):
    """ create DataOperation accounting object

    :param str operation: operation performed
    :param str se: destination SE name
    :param int files: nb of files
    """
    import DIRAC
    accountingDict = {}
    accountingDict['OperationType'] = operation
    result = getProxyInfo()
    if not result['OK']:
      userName = 'system'
    else:
      userName = result['Value'].get( 'username', 'unknown' )
    accountingDict['User'] = userName
    accountingDict['Protocol'] = 'gfal'
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

#######################################################################
#
# The following methods provide the interaction with gfal functionality
#
#######################################################################

  def __create_gfal_object( self, gfalDict ):
    """ create gfal object by calling gfal.gfal_init

    :param self: self reference
    :param dict gfalDict: gfal params dict
    """
    self.log.debug( "SRM2Storage.__create_gfal_object: Performing gfal_init." )
    errCode, gfalObject, errMessage = self.gfal.gfal_init( gfalDict )
    if not errCode == 0:
      errStr = "SRM2Storage.__create_gfal_object: Failed to perform gfal_init."
      if not errMessage:
        errMessage = os.strerror( self.gfal.gfal_get_errno() )
      self.log.error( errStr, errMessage )
      return S_ERROR( "%s%s" % ( errStr, errMessage ) )
    else:
      self.log.debug( "SRM2Storage.__create_gfal_object: Successfully performed gfal_init." )
      return S_OK( gfalObject )

  def __gfal_set_ids( self, gfalObject, srmRequestID ):
    """ set :srmRequestID:

    :param self: self reference
    :param gfalObject: gfal object
    :param str srmRequestID: srm request id
    """
    self.log.debug( "SRM2Storage.__gfal_set_ids: Performing gfal_set_ids." )
    errCode, gfalObject, errMessage = self.gfal.gfal_set_ids( gfalObject, None, 0, str( srmRequestID ) )
    if not errCode == 0:
      errStr = "SRM2Storage.__gfal_set_ids: Failed to perform gfal_set_ids."
      if not errMessage:
        errMessage = os.strerror( errCode )
      self.log.error( errStr, errMessage )
      return S_ERROR( "%s%s" % ( errStr, errMessage ) )
    else:
      self.log.debug( "SRM2Storage.__gfal_set_ids: Successfully performed gfal_set_ids." )
      return S_OK( gfalObject )

  def __gfal_exec( self, gfalObject, method, timeout_sendreceive = None ):
    """
      In gfal, for every method (synchronous or asynchronous), you can define a sendreceive timeout and a connect timeout.
      The connect timeout sets the maximum amount of time a client accepts to wait before establishing a successful TCP
      connection to SRM (default 60 seconds).
      The sendreceive timeout, allows a client to set the maximum time the send
      of a request to SRM can take (normally all send operations return immediately unless there is no free TCP buffer)
      and the maximum time to receive a reply (a token for example). Default 0, i.e. no timeout.
      The srm timeout for asynchronous requests default to 3600 seconds

      gfal_set_timeout_connect (int value)

      gfal_set_timeout_sendreceive (int value)

      gfal_set_timeout_bdii (int value)

      gfal_set_timeout_srm (int value)

    """
    self.log.debug( "SRM2Storage.__gfal_exec(%s): Starting" % method )
    fcn = None
    if hasattr( self.gfal, method ) and callable( getattr( self.gfal, method ) ):
      fcn = getattr( self.gfal, method )
    if not fcn:
      return S_ERROR( "Unable to invoke %s for gfal, it isn't a member function" % method )

    # # retry
    retry = self.gfalRetry if self.gfalRetry else 1
    # # initial timeout
    timeout = timeout_sendreceive if timeout_sendreceive else self.gfalTimeout
    # # errCode, errMessage, errNo
    errCode, errMessage, errNo = 0, "", 0
    while retry:
      retry -= 1
      self.gfal.gfal_set_timeout_sendreceive( timeout )
      errCode, gfalObject, errMessage = fcn( gfalObject )
      if errCode == -1:
        errNo = self.gfal.gfal_get_errno()
      if errCode == -1 and errNo == errno.ECOMM:
        timeout *= 2
        self.log.debug( "SRM2Storage.__gfal_exec(%s): got ECOMM, extending timeout to %s s" % ( method, timeout ) )
        continue
      else:
        break
    if errCode:
      errStr = "SRM2Storage.__gfal_exec(%s): Execution failed." % method
      if not errMessage:
        errMessage = os.strerror( errNo ) if errNo else "UNKNOWN ERROR"
        self.log.error( errStr, errMessage )
      return S_ERROR( "%s %s" % ( errStr, errMessage ) )
    self.log.debug( "SRM2Storage.__gfal_exec(%s): Successfully invoked." % method )
    return S_OK( gfalObject )

  def __get_results( self, gfalObject ):
    """ retrive gfal results

    :param self: self reference
    :param gfalObject: gfal object
    """
    self.log.debug( "SRM2Storage.__get_results: Performing gfal_get_results" )
    numberOfResults, gfalObject, listOfResults = self.gfal.gfal_get_results( gfalObject )
    if numberOfResults <= 0:
      errStr = "SRM2Storage.__get_results: Did not obtain results with gfal_get_results."
      self.log.error( errStr )
      return S_ERROR( errStr )
    else:
      self.log.debug( "SRM2Storage.__get_results: Retrieved %s results from gfal_get_results." % numberOfResults )
      for result in listOfResults:
        if result['status'] != 0:
          if result['explanation']:
            errMessage = result['explanation']
          elif result['status'] > 0:
            errMessage = os.strerror( result['status'] )
          result['ErrorMessage'] = errMessage
      return S_OK( listOfResults )

  def __gfal_get_ids( self, gfalObject ):
    """ get srmRequestToken

    :param self: self reference
    :param gfalObject: gfalObject
    """
    self.log.debug( "SRM2Storage.__gfal_get_ids: Performing gfal_get_ids." )
    numberOfResults, gfalObject, _srm1RequestID, _srm1FileIDs, srmRequestToken = self.gfal.gfal_get_ids( gfalObject )
    if numberOfResults <= 0:
      errStr = "SRM2Storage.__gfal_get_ids: Did not obtain SRM request ID."
      self.log.error( errStr )
      return S_ERROR( errStr )
    else:
      self.log.debug( "SRM2Storage.__get_gfal_ids: Retrieved SRM request ID %s." % srmRequestToken )
      return S_OK( srmRequestToken )

  def __destroy_gfal_object( self, gfalObject ):
    """ del gfal object by calling gfal.gfal_internal_free

    :param self: self reference
    :param gfalObject: gfalObject
    """
    self.log.debug( "SRM2Storage.__destroy_gfal_object: Performing gfal_internal_free." )
    self.gfal.gfal_internal_free( gfalObject )
    return S_OK()

