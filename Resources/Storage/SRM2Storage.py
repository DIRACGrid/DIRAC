########################################################################
# $HeadURL $
# File: SRM2Storage.py
########################################################################

""" :mod: SRM2Storage 
    =================
 
    .. module: python
    :synopsis: SRM v2 interface to StorageElement
"""

## imports 
import os
import re
import time
import types
from stat import S_ISREG, S_ISDIR, S_IMODE, ST_MODE, ST_SIZE
## from DIRAC
from DIRAC import gLogger, gConfig, S_OK, S_ERROR
from DIRAC.Resources.Storage.StorageBase import StorageBase
from DIRAC.Core.Security.ProxyInfo import getProxyInfo
from DIRAC.ConfigurationSystem.Client.Helpers.Registry import getVOForGroup
from DIRAC.Core.Utilities.Subprocess import pythonCall
from DIRAC.Core.Utilities.Pfn import pfnparse, pfnunparse
from DIRAC.Core.Utilities.List import breakListIntoChunks
from DIRAC.Core.Utilities.File import getSize
from DIRAC.AccountingSystem.Client.Types.DataOperation import DataOperation
from DIRAC.AccountingSystem.Client.DataStoreClient import gDataStoreClient

## RSCID
__RCSID__ = "$Id$"

class SRM2Storage( StorageBase ):
  """ .. class:: SRM2Storage


  """

  ## placeholder for lcg_util
  __lcg_util = None
  ## placeholder for gfal
  __gfal = None

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

    self.gfal = None
    self.lcg_util = None

    self.protocolName = 'SRM2'
    self.name = storageName
    self.protocol = protocol
    self.path = path
    self.host = host
    self.port = port
    self.wspath = wspath
    self.spaceToken = spaceToken
    self.cwd = self.path

    StorageBase.__init__( self, self.name, self.path )

    self.timeout = 100
    self.long_timeout = 1200
    self.stageTimeout = gConfig.getValue( '/Resources/StorageElements/StageTimeout', 12 * 60 * 60 )
    self.fileTimeout = gConfig.getValue( '/Resources/StorageElements/FileTimeout', 30 )
    self.filesPerCall = gConfig.getValue( '/Resources/StorageElements/FilesPerCall', 20 )

    ## set checksum type, by default this is 0 (GFAL_CKSM_NONE)
    self.checksumType = gConfig.getValue( "/Resources/StorageElements/ChecksumType", 0 )
    # enum gfal_cksm_type
    #	GFAL_CKSM_NONE = 0,
    #	GFAL_CKSM_CRC32,
    #	GFAL_CKSM_ADLER32,
    #	GFAL_CKSM_MD5,
    #	GFAL_CKSM_SHA1    
    # GFAL_CKSM_NULL = 0
    self.checksumTypes = { None : 0, "CRC32" : 1, "ADLER32" : 2, 
                           "MD5" : 3, "SHA1" : 4, "NONE" : 0, "NULL" : 0 }
    if self.checksumType:
      if str(self.checksumType).upper() in self.checksumTypes: 
        self.log.debug("SRM2Storage: will use %s checksum check" % self.checksumType )
        self.checksumType = self.checksumTypes[ self.checksumType.upper() ]
      else:
        self.log.warn("SRM2Storage: unknown checksum type %s, checksum check disabled" % self.checksumType )
        ## GFAL_CKSM_NONE
        self.checksumType = 0

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

    self.MAX_SINGLE_STREAM_SIZE = 1024 * 1024 * 10 # 10 MB ???
    self.MIN_BANDWIDTH = 0.5 * ( 1024 * 1024 ) # 0.5 MB/s ???

  def lcg_util( self ):
    """ import and get lcg_util module

    :param self: self reference
    """
    if not self.__lcg_util:
      import lcg_util
      self.__lcg_util = lcg_util
      self.log.debug( "using lcg_util version %s from %s" % ( lcg_util.lcg_util_version(), lcg_util.__file__ ) )
    return self.__lcg_util

  def gfal( self ):
    """ import and get gfalthr or gfal module

    :param self: self reference
    """
    if not self.__gfal:
      try:
        import gfalthr as gfal
        self.__gfal = gfal
        self.log.debug( "Using gfalthr version %s from %s" % ( gfal.gfal_version(), gfal.__file__ ) )
      except ImportError, error:
        self.log.warn( "Failed to import gfalthr: %s" % str(error) )
        import gfal
        self.__gfal = gfal
        self.log.debug( "Using gfal version %s from %s" % ( gfal.gfal_version(), gfal.__file__ ) )                                         
    return self.__gfal

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
    """
    if fileName:
      if fileName[0] == '/':
        fileName = fileName.lstrip( '/' )
    try:
      fullUrl = '%s://%s:%s%s%s/%s' % ( self.protocol, self.host, self.port, self.wspath, self.cwd, fileName )
      fullUrl = fullUrl.rstrip( '/' )
      return S_OK( fullUrl )
    except Exception, x:
      errStr = "Failed to create URL %s" % x
      return S_ERROR( errStr )

  def isPfnForProtocol( self, pfn ):
    res = pfnparse( pfn )
    if not res['OK']:
      return res
    pfnDict = res['Value']
    if pfnDict['Protocol'] == self.protocol:
      return S_OK( True )
    else:
      return S_OK( False )

  def getProtocolPfn( self, pfnDict, withPort ):
    """ From the pfn dict construct the SURL to be used
    """
    #For srm2 keep the file name and path
    pfnDict['Protocol'] = self.protocol
    pfnDict['Host'] = self.host
    if withPort:
      pfnDict['Port'] = self.port
      pfnDict['WSUrl'] = self.wspath
    else:
      pfnDict['Port'] = ''
      pfnDict['WSUrl'] = ''
    res = pfnunparse( pfnDict )
    return res

################################################################################
#
# The methods below are URL manipulation methods
#
################################################################################

  def getPFNBase( self, withPort = False ):
    """ This will get the pfn base. This is then appended with the LFN in LHCb convention.
    """
    if withPort:
      pfnBase = 'srm://%s:%s%s' % ( self.host, self.port, self.path )
    else:
      pfnBase = 'srm://%s%s' % ( self.host, self.path )
    return S_OK( pfnBase )

  def getUrl( self, path, withPort = True ):
    """ This gets the URL for path supplied. With port is optional.
    """
    pfnDict = pfnparse( path )['Value']
    if not re.search( self.path, path ):
      pfnDict['Path'] = "%s/%s" % ( self.path, pfnDict['Path'] )

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
    parameterDict = {}
    parameterDict['StorageName'] = self.name
    parameterDict['ProtocolName'] = self.protocolName
    parameterDict['Protocol'] = self.protocol
    parameterDict['Host'] = self.host
    parameterDict['Path'] = self.path
    parameterDict['Port'] = self.port
    parameterDict['SpaceToken'] = self.spaceToken
    parameterDict['WSUrl'] = self.wspath
    return S_OK( parameterDict )

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
    """ Make recursively new directory(ies) on the physical storage
    """
    res = self.checkArgumentFormat( path )
    if not res['OK']:
      return res
    urls = res['Value']

    successful = {}
    failed = {}
    self.log.debug( "SRM2Storage.createDirectory: Attempting to create %s directories." % len( urls ) )
    for url in urls:
      strippedUrl = url.rstrip( '/' )
      res = self.__makeDirs( strippedUrl )
      if res['OK']:
        self.log.debug( "SRM2Storage.createDirectory: Successfully created directory on storage: %s" % url )
        successful[url] = True
      else:
        self.log.error( "SRM2Storage.createDirectory: Failed to create directory on storage.",
                       "%s: %s" % ( url, res['Message'] ) )
        failed[url] = res['Message']
    resDict = {'Failed':failed, 'Successful':successful}
    return S_OK( resDict )

  def __makeDir( self, path ):
    srcFile = '%s/%s' % ( os.getcwd(), 'dirac_directory' )
    dfile = open( srcFile, 'w' )
    dfile.write( " " )
    dfile.close()
    destFile = '%s/%s' % ( path, 'dirac_directory.%s' % time.time() )
    res = self.__putFile( srcFile, destFile, 0 )
    if os.path.exists( srcFile ):
      os.remove( srcFile )
    self.__executeOperation( destFile, 'removeFile' )
    return res

  def __makeDirs( self, path ):
    """  Black magic contained within....
    """
    dirName = os.path.dirname( path )
    res = self.__executeOperation( path, 'exists' )
    if not res['OK']:
      return res
    if res['Value']:
      return S_OK()
    res = self.__executeOperation( dirName, 'exists' )
    if not res['OK']:
      return res
    if res['Value']:
      res = self.__makeDir( path )
    else:
      res = self.__makeDirs( dirName )
      if not res['OK']:
        return res
      res = self.__makeDir( path )
    return res

################################################################################
#
# The methods below use the new generic methods for executing operations
#
################################################################################

  def removeFile( self, path ):
    """Remove physically the file specified by its path
    """
    res = self.checkArgumentFormat( path )
    if not res['OK']:
      return res
    urls = res['Value']
    self.log.debug( "removeFile: Performing the removal of %s file(s)" % len( urls ) )
    resDict = self.__gfaldeletesurls_wrapper( urls )['Value']
    failed = resDict['Failed']
    allResults = resDict['AllResults']
    successful = {}
    for urlDict in allResults:
      if "surl" in urlDict and urlDict['surl']:
        pathSURL = self.getUrl( urlDict['surl'] )['Value']
        if urlDict['status'] == 0:
          self.log.debug( "removeFile: Successfully removed file: %s" % pathSURL )
          successful[pathSURL] = True
        elif urlDict['status'] == 2:
          # This is the case where the file doesn't exist.
          self.log.debug( "removeFile: File did not exist, sucessfully removed: %s" % pathSURL )
          successful[pathSURL] = True
        else:
          errStr = "removeFile: Failed to remove file."
          errMessage = urlDict['ErrorMessage']
          self.log.error( errStr, "%s: %s" % ( pathSURL, errMessage ) )
          failed[pathSURL] = "%s %s" % ( errStr, errMessage )
    resDict = {'Failed':failed, 'Successful':successful}
    return S_OK( resDict )

  def getTransportURL( self, path, protocols = False ):
    """ Obtain the tURLs for the supplied path and protocols
    """
    res = self.checkArgumentFormat( path )
    if not res['OK']:
      return res
    urls = res['Value']

    if not protocols:
      protocols = self.__getProtocols()
      if not protocols['OK']:
        return protocols
      listProtocols = protocols['Value']
    elif type( protocols ) == types.StringType:
      listProtocols = [protocols]
    elif type( protocols ) == types.ListType:
      listProtocols = protocols
    else:
      return S_ERROR( "SRM2Storage.getTransportURL: Must supply desired protocols to this plug-in." )

    self.log.debug( "getTransportURL: Obtaining tURLs for %s file(s)." % len( urls ) )
    resDict = self.__gfalturlsfromsurls_wrapper( urls, listProtocols )['Value']
    failed = resDict['Failed']
    allResults = resDict['AllResults']
    successful = {}
    for urlDict in allResults:
      if urlDict.has_key( 'surl' ) and urlDict['surl']:
        pathSURL = self.getUrl( urlDict['surl'] )['Value']
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
    resDict = { 'Failed' : failed, 'Successful' : successful }
    return S_OK( resDict )

  def prestageFile( self, path, lifetime = 60 * 60 * 24 ):
    """ Issue prestage request for file
    """
    res = self.checkArgumentFormat( path )
    if not res['OK']:
      return res
    urls = res['Value']

    self.log.debug( "SRM2Storage.prestageFile: Attempting to issue stage requests for %s file(s)." % len( urls ) )
    resDict = self.__gfalprestage_wrapper( urls, lifetime )['Value']
    failed = resDict['Failed']
    allResults = resDict['AllResults']
    successful = {}
    for urlDict in allResults:
      if urlDict.has_key( 'surl' ) and urlDict['surl']:
        pathSURL = self.getUrl( urlDict['surl'] )['Value']
        if urlDict['status'] == 0:
          self.log.debug( "SRM2Storage.prestageFile: Issued stage request for file %s." % pathSURL )
          successful[pathSURL] = urlDict['SRMReqID']
        elif urlDict['status'] == 1:
          self.log.debug( "SRM2Storage.prestageFile: File found to be already staged.", pathSURL )
          successful[pathSURL] = urlDict['SRMReqID']
        elif urlDict['status'] == 2:
          errMessage = "SRM2Storage.prestageFile: File does not exist."
          self.log.error( errMessage, pathSURL )
          failed[pathSURL] = errMessage
        else:
          errStr = "SRM2Storage.prestageFile: Failed issue stage request."
          errMessage = urlDict['ErrorMessage']
          self.log.error( errStr, "%s: %s" % ( errMessage, pathSURL ) )
          failed[pathSURL] = "%s %s" % ( errStr, errMessage )
    resDict = {'Failed':failed, 'Successful':successful}
    return S_OK( resDict )

  def prestageFileStatus( self, path ):
    """ Monitor prestage request for files
    """
    res = self.checkArgumentFormat( path )
    if not res['OK']:
      return res
    urls = res['Value']

    self.log.debug( "SRM2Storage.prestageFileStatus: Attempting to get status "
                   "of stage requests for %s file(s)." % len( urls ) )
    resDict = self.__gfal_prestagestatus_wrapper( urls )['Value']
    failed = resDict['Failed']
    allResults = resDict['AllResults']
    successful = {}
    for urlDict in allResults:
      if urlDict.has_key( 'surl' ) and urlDict['surl']:
        pathSURL = self.getUrl( urlDict['surl'] )['Value']
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
    resDict = {'Failed':failed, 'Successful':successful}
    return S_OK( resDict )

  def getFileMetadata( self, path ):
    """  Get metadata associated to the file
    """
    res = self.checkArgumentFormat( path )
    if not res['OK']:
      return res
    urls = res['Value']

    self.log.debug( "SRM2Storage.getFileMetadata: Obtaining metadata for %s file(s)." % len( urls ) )
    resDict = self.__gfalls_wrapper( urls, 0 )['Value']
    failed = resDict['Failed']
    listOfResults = resDict['AllResults']
    successful = {}
    for urlDict in listOfResults:
      if urlDict.has_key( 'surl' ) and urlDict['surl']:
        pathSURL = self.getUrl( urlDict['surl'] )['Value']
        if urlDict['status'] == 0:
          statDict = self.__parse_file_metadata( urlDict )
          if statDict['File']:
            successful[pathSURL] = statDict
          else:
            errStr = "SRM2Storage.getFileMetadata: Supplied path is not a file."
            self.log.error( errStr, pathSURL )
            failed[pathSURL] = errStr
        elif urlDict['status'] == 2:
          errMessage = "SRM2Storage.getFileMetadata: File does not exist."
          self.log.error( errMessage, pathSURL )
          failed[pathSURL] = errMessage
        else:
          errStr = "SRM2Storage.getFileMetadata: Failed to get file metadata."
          errMessage = urlDict['ErrorMessage']
          self.log.error( errStr, "%s: %s" % ( pathSURL, errMessage ) )
          failed[pathSURL] = "%s %s" % ( errStr, errMessage )
      else:
        errStr = "SRM2Storage.getFileMetadata: Returned element does not contain surl."
        self.log.fatal( errStr, self.name )
        return S_ERROR( errStr )
    resDict = {'Failed':failed, 'Successful':successful}
    return S_OK( resDict )

  def isFile( self, path ):
    """Check if the given path exists and it is a file
    """
    res = self.checkArgumentFormat( path )
    if not res['OK']:
      return res
    urls = res['Value']

    self.log.debug( "SRM2Storage.isFile: Checking whether %s path(s) are file(s)." % len( urls ) )
    resDict = self.__gfalls_wrapper( urls, 0 )['Value']
    failed = resDict['Failed']
    listOfResults = resDict['AllResults']
    successful = {}
    for urlDict in listOfResults:
      if urlDict.has_key( 'surl' ) and urlDict['surl']:
        pathSURL = self.getUrl( urlDict['surl'] )['Value']
        if urlDict['status'] == 0:
          statDict = self.__parse_file_metadata( urlDict )
          if statDict['File']:
            successful[pathSURL] = True
          else:
            self.log.debug( "SRM2Storage.isFile: Path is not a file: %s" % pathSURL )
            successful[pathSURL] = False
        elif urlDict['status'] == 2:
          errMessage = "SRM2Storage.isFile: File does not exist."
          self.log.error( errMessage, pathSURL )
          failed[pathSURL] = errMessage
        else:
          errStr = "SRM2Storage.isFile: Failed to get file metadata."
          errMessage = urlDict['ErrorMessage']
          self.log.error( errStr, "%s: %s" % ( pathSURL, errMessage ) )
          failed[pathSURL] = "%s %s" % ( errStr, errMessage )
      else:
        errStr = "SRM2Storage.isFile: Returned element does not contain surl."
        self.log.fatal( errStr, self.name )
        return S_ERROR( errStr )
    resDict = {'Failed':failed, 'Successful':successful}
    return S_OK( resDict )

  def pinFile( self, path, lifetime = 60 * 60 * 24 ):
    """ Pin a file with a given lifetime
    """
    res = self.checkArgumentFormat( path )
    if not res['OK']:
      return res
    urls = res['Value']

    self.log.debug( "SRM2Storage.pinFile: Attempting to pin %s file(s)." % len( urls ) )
    resDict = self.__gfal_pin_wrapper( urls, lifetime )['Value']
    failed = resDict['Failed']
    allResults = resDict['AllResults']
    successful = {}
    for urlDict in allResults:
      if urlDict.has_key( 'surl' ) and urlDict['surl']:
        pathSURL = self.getUrl( urlDict['surl'] )['Value']
        if urlDict['status'] == 0:
          self.log.debug( "SRM2Storage.pinFile: Issued pin request for file %s." % pathSURL )
          successful[pathSURL] = urlDict['SRMReqID']
        elif urlDict['status'] == 2:
          errMessage = "SRM2Storage.pinFile: File does not exist."
          self.log.error( errMessage, pathSURL )
          failed[pathSURL] = errMessage
        else:
          errStr = "SRM2Storage.pinFile: Failed issue pin request."
          errMessage = urlDict['ErrorMessage']
          self.log.error( errStr, "%s: %s" % ( errMessage, pathSURL ) )
          failed[pathSURL] = "%s %s" % ( errStr, errMessage )
    resDict = {'Failed':failed, 'Successful':successful}
    return S_OK( resDict )

  def releaseFile( self, path ):
    """ Release a file
    """
    res = self.checkArgumentFormat( path )
    if not res['OK']:
      return res
    urls = res['Value']

    self.log.debug( "SRM2Storage.releaseFile: Attempting to release %s file(s)." % len( urls ) )
    resDict = self.__gfal_release_wrapper( urls )['Value']
    failed = resDict['Failed']
    allResults = resDict['AllResults']
    successful = {}
    for urlDict in allResults:
      if urlDict.has_key( 'surl' ) and urlDict['surl']:
        pathSURL = self.getUrl( urlDict['surl'] )['Value']
        if urlDict['status'] == 0:
          self.log.debug( "SRM2Storage.releaseFile: Issued release request for file %s." % pathSURL )
          successful[pathSURL] = urlDict['SRMReqID']
        elif urlDict['status'] == 2:
          errMessage = "SRM2Storage.releaseFile: File does not exist."
          self.log.error( errMessage, pathSURL )
          failed[pathSURL] = errMessage
        else:
          errStr = "SRM2Storage.releaseFile: Failed issue release request."
          errMessage = urlDict['ErrorMessage']
          self.log.error( errStr, "%s: %s" % ( errMessage, pathSURL ) )
          failed[pathSURL] = "%s %s" % ( errStr, errMessage )
    resDict = {'Failed':failed, 'Successful':successful}
    return S_OK( resDict )

  def exists( self, path ):
    """ Check if the given path exists. """
    res = self.checkArgumentFormat( path )
    if not res['OK']:
      return res
    urls = res['Value']

    self.log.debug( "SRM2Storage.exists: Checking the existance of %s path(s)" % len( urls ) )
    resDict = self.__gfalls_wrapper( urls, 0 )['Value']
    failed = resDict['Failed']
    listOfResults = resDict['AllResults']
    successful = {}
    for urlDict in listOfResults:
      if urlDict.has_key( 'surl' ) and urlDict['surl']:
        pathSURL = self.getUrl( urlDict['surl'] )['Value']
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
    resDict = {'Failed':failed, 'Successful':successful}
    return S_OK( resDict )

  def getFileSize( self, path ):
    """Get the physical size of the given file
    """
    res = self.checkArgumentFormat( path )
    if not res['OK']:
      return res
    urls = res['Value']

    self.log.debug( "SRM2Storage.getFileSize: Obtaining the size of %s file(s)." % len( urls.keys() ) )
    resDict = self.__gfalls_wrapper( urls, 0 )['Value']
    failed = resDict['Failed']
    listOfResults = resDict['AllResults']
    successful = {}
    for urlDict in listOfResults:
      if urlDict.has_key( 'surl' ) and urlDict['surl']:
        pathSURL = self.getUrl( urlDict['surl'] )['Value']
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
    resDict = {'Failed':failed, 'Successful':successful}
    return S_OK( resDict )

  def putFile( self, path, sourceSize = 0 ):
    res = self.checkArgumentFormat( path )
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
    resDict = {'Failed':failed, 'Successful':successful}
    return S_OK( resDict )

  def __putFile( self, src_file, dest_url, sourceSize ):
    # Pre-transfer check
    res = self.__executeOperation( dest_url, 'exists' )
    if not res['OK']:
      self.log.debug( "SRM2Storage.__putFile: Failed to find pre-existance of destination file." )
      return res
    if res['Value']:
      res = self.__executeOperation( dest_url, 'removeFile' )
      if not res['OK']:
        self.log.debug( "SRM2Storage.__putFile: Failed to remove remote file %s." % dest_url )
      else:
        self.log.debug( "SRM2Storage.__putFile: Removed remote file %s." % dest_url )
    dsttype = self.defaulttype
    src_spacetokendesc = ''
    dest_spacetokendesc = self.spaceToken
    if re.search( 'srm:', src_file ):
      src_url = src_file
      srctype = 2
      if not sourceSize:
        return S_ERROR( "SRM2Storage.__putFile: For file replication the source file size must be provided." )
    else:
      if not os.path.exists( src_file ):
        errStr = "SRM2Storage.__putFile: The source local file does not exist."
        self.log.error( errStr, src_file )
        return S_ERROR( errStr )
      sourceSize = getSize( src_file )
      if sourceSize == -1:
        errStr = "SRM2Storage.__putFile: Failed to get file size."
        self.log.error( errStr, src_file )
        return S_ERROR( errStr )
      src_url = 'file:%s' % src_file
      srctype = 0
    if sourceSize == 0:
      errStr = "SRM2Storage.__putFile: Source file is zero size."
      self.log.error( errStr, src_file )
      return S_ERROR( errStr )
    timeout = int( sourceSize / self.MIN_BANDWIDTH + 300 )
    if sourceSize > self.MAX_SINGLE_STREAM_SIZE:
      nbstreams = 4
    else:
      nbstreams = 1
    self.log.info( "SRM2Storage.__putFile: Using %d streams" % nbstreams )
    self.log.info( "SRM2Storage.__putFile: Executing transfer of %s to %s" % ( src_url, dest_url ) )
    res = pythonCall( ( timeout + 10 ), self.__lcg_cp_wrapper, src_url, dest_url,
                      srctype, dsttype, nbstreams, timeout, src_spacetokendesc, dest_spacetokendesc )
    if not res['OK']:
      # Remove the failed replica, just in case
      result = self.__executeOperation( dest_url, 'removeFile' )
      if result['OK']:
        self.log.debug( "SRM2Storage.__putFile: Removed remote file remnant %s." % dest_url )
      else:
        self.log.debug( "SRM2Storage.__putFile: Unable to remove remote file remnant %s." % dest_url )
      return res
    res = res['Value']
    if not res['OK']:
      # Remove the failed replica, just in case
      result = self.__executeOperation( dest_url, 'removeFile' )
      if result['OK']:
        self.log.debug( "SRM2Storage.__putFile: Removed remote file remnant %s." % dest_url )
      else:
        self.log.debug( "SRM2Storage.__putFile: Unable to remove remote file remnant %s." % dest_url )
      return res
    errCode, errStr = res['Value']
    if errCode == 0:
      self.log.info( 'SRM2Storage.__putFile: Successfully put file to storage.' )
      ## checksum check? return!
      if self.checksumType: 
        return S_OK( sourceSize )
      ## else compare sizes
      res = self.__executeOperation( dest_url, 'getFileSize' )
      if res['OK']:
        destinationSize = res['Value']
        if sourceSize == destinationSize :
          self.log.debug( "SRM2Storage.__putFile: Post transfer check successful." )
          return S_OK( destinationSize )
      errorMessage = "SRM2Storage.__putFile: Source and destination file sizes do not match."
      self.log.error( errorMessage, src_url )
    else:
      errorMessage = "SRM2Storage.__putFile: Failed to put file to storage."
      if errCode > 0:
        errStr = "%s %s" % ( errStr, os.strerror( errCode ) )
      self.log.error( errorMessage, errStr )
    res = self.__executeOperation( dest_url, 'removeFile' )
    if res['OK']:
      self.log.debug( "SRM2Storage.__putFile: Removed remote file remnant %s." % dest_url )
    else:
      self.log.debug( "SRM2Storage.__putFile: Unable to remove remote file remnant %s." % dest_url )
    return S_ERROR( errorMessage )

  def __lcg_cp_wrapper( self, src_url, dest_url, srctype, dsttype, nbstreams,
                        timeout, src_spacetokendesc, dest_spacetokendesc ):

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

      if type( errCode ) not in [types.IntType]:
        self.log.error( "SRM2Storage.__lcg_cp_wrapper: Returned errCode was not an integer",
                       "%s %s" % ( errCode, type( errCode ) ) )
        if type( errCode ) in [types.ListType]:
          msg = []
          for err in errCode:
            msg.append( '%s of type %s' % ( err, type( err ) ) )
          self.log.error( "SRM2Storage.__lcg_cp_wrapper: Returned errCode was List:\n" , "\n".join( msg ) )
        return S_ERROR( "SRM2Storage.__lcg_cp_wrapper: Returned errCode was not an integer" )
      if type( errStr ) not in types.StringTypes:
        self.log.error( "SRM2Storage.__lcg_cp_wrapper: Returned errStr was not a string",
                       "%s %s" % ( errCode, type( errStr ) ) )
        return S_ERROR( "SRM2Storage.__lcg_cp_wrapper: Returned errStr was not a string" )
      return S_OK( ( errCode, errStr ) )
    except Exception, x:
      self.log.exception( "SRM2Storage.__lcg_cp_wrapper", "", x )
      return S_ERROR( "Exception while attempting file upload" )

  def getFile( self, path, localPath = False ):
    """ Get a local copy in the current directory of a physical file specified by its path
    """
    res = self.checkArgumentFormat( path )
    if not res['OK']:
      return res
    urls = res['Value']

    failed = {}
    successful = {}
    for src_url in urls.keys():
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
    resDict = {'Failed':failed, 'Successful':successful}
    return S_OK( resDict )

  def __getFile( self, src_url, dest_file ):
    if not os.path.exists( os.path.dirname( dest_file ) ):
      os.makedirs( os.path.dirname( dest_file ) )
    if os.path.exists( dest_file ):
      self.log.debug( "SRM2Storage.__getFile: Local file already exists %s. Removing..." % dest_file )
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
    self.log.info( "SRM2Storage.__getFile: Using %d streams" % nbstreams )
    self.log.info( "SRM2Storage.__getFile: Executing transfer of %s to %s" % ( src_url, dest_url ) )
    res = pythonCall( ( timeout + 10 ), self.__lcg_cp_wrapper, src_url, dest_url, srctype, dsttype,
                      nbstreams, timeout, src_spacetokendesc, dest_spacetokendesc )
    if not res['OK']:
      return res
    res = res['Value']
    if not res['OK']:
      return res
    errCode, errStr = res['Value']
    if errCode == 0:
      self.log.debug( 'SRM2Storage.__getFile: Got a file from storage.' )
      localSize = getSize( dest_file )
      if localSize == remoteSize:
        self.log.debug( "SRM2Storage.__getFile: Post transfer check successful." )
        return S_OK( localSize )
      errorMessage = "SRM2Storage.__getFile: Source and destination file sizes do not match."
      self.log.error( errorMessage, src_url )
    else:
      errorMessage = "SRM2Storage.__getFile: Failed to get file from storage."
      if errCode > 0:
        errStr = "%s %s" % ( errStr, os.strerror( errCode ) )
      self.log.error( errorMessage, errStr )
    if os.path.exists( dest_file ):
      self.log.debug( "SRM2Storage.getFile: Removing local file %s." % dest_file )
      os.remove( dest_file )
    return S_ERROR( errorMessage )

  def __executeOperation( self, url, method ):
    """ Executes the requested functionality with the supplied url
    """
    fcn = None
    if hasattr( self, method ) and callable( getattr(self, method) ):
      fcn = getattr( self, method )
    if not fcn:
      return S_ERROR("Unable to invoke %s, it isn't a member funtion of SRM2Storage" % method )
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
    """Check if the given path exists and it is a directory
    """
    res = self.checkArgumentFormat( path )
    if not res['OK']:
      return res
    urls = res['Value']

    self.log.debug( "SRM2Storage.isDirectory: Checking whether %s path(s) are directory(ies)" % len( urls.keys() ) )
    resDict = self.__gfalls_wrapper( urls, 0 )['Value']
    failed = resDict['Failed']
    listOfResults = resDict['AllResults']
    successful = {}
    for urlDict in listOfResults:
      if urlDict.has_key( 'surl' ) and urlDict['surl']:
        dirSURL = self.getUrl( urlDict['surl'] )['Value']
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
    resDict = {'Failed':failed, 'Successful':successful}
    return S_OK( resDict )

  def getDirectoryMetadata( self, path ):
    """ Get the metadata for the directory
    """
    res = self.checkArgumentFormat( path )
    if not res['OK']:
      return res
    urls = res['Value']

    self.log.debug( "SRM2Storage.getDirectoryMetadata: Attempting to obtain metadata for %s directories." % len( urls ) )
    resDict = self.__gfalls_wrapper( urls, 0 )['Value']
    failed = resDict['Failed']
    listOfResults = resDict['AllResults']
    successful = {}
    for urlDict in listOfResults:
      if urlDict.has_key( 'surl' ) and urlDict['surl']:
        pathSURL = self.getUrl( urlDict['surl'] )['Value']
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
    resDict = {'Failed':failed, 'Successful':successful}
    return S_OK( resDict )

  def getDirectorySize( self, path ):
    """ Get the size of the directory on the storage
    """
    res = self.checkArgumentFormat( path )
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
      successful[directory] = {'Files':directoryFiles, 'Size':directorySize, 'SubDirs':subDirectories}
    resDict = {'Failed':failed, 'Successful':successful}
    return S_OK( resDict )

  def listDirectory( self, path ):
    """ List the contents of the directory on the storage
    """
    res = self.checkArgumentFormat( path )
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

    resDict = self.__gfal_lsdir_wrapper( directories )['Value']
    #resDict = self.__gfalls_wrapper(directories,1)['Value']
    failed.update( resDict['Failed'] )
    listOfResults = resDict['AllResults']
    successful = {}
    for urlDict in listOfResults:
      if urlDict.has_key( 'surl' ) and urlDict['surl']:
        pathSURL = self.getUrl( urlDict['surl'] )['Value']
        if urlDict['status'] == 0:
          successful[pathSURL] = {}
          self.log.debug( "SRM2Storage.listDirectory: Successfully listed directory %s" % pathSURL )
          subPathDirs = {}
          subPathFiles = {}
          if urlDict.has_key( 'subpaths' ):
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
    resDict = {'Failed':failed, 'Successful':successful}
    return S_OK( resDict )

  def putDirectory( self, path ):
    """ Put a local directory to the physical storage together with all its files and subdirectories.
    """
    res = self.checkArgumentFormat( path )
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
          successful[destDir] = {'Files':res['Value']['Files'], 'Size':res['Value']['Size']}
        else:
          self.log.error( "SRM2Storage.putDirectory: Failed to put entire directory to remote storage.", destDir )
          failed[destDir] = {'Files':res['Value']['Files'], 'Size':res['Value']['Size']}
      else:
        self.log.error( "SRM2Storage.putDirectory: Completely failed to put directory to remote storage.", destDir )
        failed[destDir] = {'Files':0, 'Size':0}
    resDict = {'Failed':failed, 'Successful':successful}
    return S_OK( resDict )

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
    resDict = {'AllPut':allSuccessful, 'Files':filesPut, 'Size':sizePut}
    return S_OK( resDict )

  def getDirectory( self, path, localPath = False ):
    """ Get a local copy in the current directory of a physical file specified by its path
    """
    res = self.checkArgumentFormat( path )
    if not res['OK']:
      return res
    urls = res['Value']

    failed = {}
    successful = {}
    self.log.debug( "SRM2Storage.getDirectory: Attempting to get local copies of %s directories." % len( urls ) )
    for src_dir in urls.keys():
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
    resDict = {'Failed':failed, 'Successful':successful}
    return S_OK( resDict )

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

    resDict = {'AllGot':allSuccessful, 'Files':filesGot, 'Size':sizeGot}
    return S_OK( resDict )

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
    res = self.checkArgumentFormat( directory )
    if not res['OK']:
      return res
    urls = res['Value']
    self.log.debug( "SRM2Storage.__removeDirectory: Attempting to remove %s directories." % len( urls ) )
    resDict = self.__gfalremovedir_wrapper( urls )['Value']
    failed = resDict['Failed']
    allResults = resDict['AllResults']
    successful = {}
    for urlDict in allResults:
      if urlDict.has_key( 'surl' ):
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
    resDict = { 'Failed' : failed, 'Successful' : successful }
    return S_OK( resDict )

  def __removeDirectoryRecursive( self, directory ):
    """ Recursively removes the directory and sub dirs. Repeatedly calls itself to delete recursively.
    """
    res = self.checkArgumentFormat( directory )
    if not res['OK']:
      return res
    urls = res['Value']

    successful = {}
    failed = {}
    self.log.debug( "SRM2Storage.__removeDirectory: Attempting to recursively remove %s directories." % len( urls ) )
    for directory in urls.keys():
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
            if res['Value']['Successful'].has_key( directory ):
              self.log.debug( "SRM2Storage.removeDirectory: Successfully removed the directory %s." % directory )
              allRemoved = True
        # Report the result
        if allRemoved:
          successful[directory] = resDict
        else:
          failed[directory] = resDict
    resDict = {'Failed':failed, 'Successful':successful}
    return S_OK( resDict )

  def __getDirectoryContents( self, directory ):
    directory = directory.rstrip( '/' )
    errMessage = "SRM2Storage.__getDirectoryContents: Failed to list directory."
    res = self.__executeOperation( directory, 'listDirectory' )
    if not res['OK']:
      self.log.error( errMessage, res['Message'] )
      return S_ERROR( errMessage )
    surlsDict = res['Value']['Files']
    subDirsDict = res['Value']['SubDirs']
    filesToRemove = {}
    for url in surlsDict.keys():
      filesToRemove[url] = surlsDict[url]['Size']
    resDict = {'Files':filesToRemove, 'SubDirs':subDirsDict.keys()}
    return S_OK( resDict )

  def __removeDirectoryFiles( self, filesToRemove ):
    resDict = {'FilesRemoved':0, 'SizeRemoved':0, 'AllRemoved':True}
    if len( filesToRemove ) > 0:
      res = self.removeFile( filesToRemove.keys() )
      if res['OK']:
        for removedSurl in res['Value']['Successful'].keys():
          resDict['FilesRemoved'] += 1
          resDict['SizeRemoved'] += filesToRemove[removedSurl]
        if len( res['Value']['Failed'].keys() ) != 0:
          resDict['AllRemoved'] = False
    self.log.debug( "SRM2Storage.__removeDirectoryFiles:",
                   "Removed %s files of size %s bytes." % ( resDict['FilesRemoved'], resDict['SizeRemoved'] ) )
    return resDict

  def __removeSubDirectories( self, subDirectories ):
    resDict = {'FilesRemoved':0, 'SizeRemoved':0, 'AllRemoved':True}
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
        if len( res['Value']['Failed'].keys() ) != 0:
          resDict['AllRemoved'] = False
    return resDict

  def checkArgumentFormat( self, path ):
    if type( path ) in types.StringTypes:
      urls = {path:False}
    elif type( path ) == types.ListType:
      urls = {}
      for url in path:
        urls[url] = False
    elif type( path ) == types.DictType:
      urls = path
    else:
      return S_ERROR( "SRM2Storage.checkArgumentFormat: Supplied path is not of the correct format." )
    return S_OK( urls )

  def __parse_stat( self, stat ):
    statDict = {'File':False, 'Directory':False}
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

      statDict.setdefault("Checksum", "")
      if "checksum" in urlDict and ( urlDict['checksum'] != '0x' ):
        statDict["Checksum"] = urlDict["checksum"]
        
      if urlDict.has_key( 'locality' ):
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
    """Returns list of protocols to use at given site.  Priority is given to a protocols list
       defined in the CS.
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
      gfalDict['timeout'] = self.long_timeout
      allObtained = False
      iteration = 0
      while not allObtained:
        gfalDict['srmv2_lsoffset'] = iteration * step
        iteration += 1
        res = self.__gfal_operation_wrapper( 'gfal_ls', gfalDict )
        gDataStoreClient.addRegister( res['AccountingOperation'] )
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
              if result.has_key( 'subpaths' ):
                results = result['subpaths']
                tempStep = step - 1
              elif re.search( result['surl'], url ):
                results = []
          allResults.extend( results )
          if len( results ) < tempStep:
            allObtained = True
      successful.append( {'surl':url, 'status':0, 'subpaths':allResults} )
    #gDataStoreClient.commit()
    resDict = {}
    resDict['AllResults'] = successful
    resDict['Failed'] = failed
    return S_OK( resDict )

  def __gfalls_wrapper( self, urls, depth ):
    """ This is a function that can be reused everywhere to perform the gfal_ls
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
      gDataStoreClient.addRegister( res['AccountingOperation'] )
      if not res['OK']:
        for url in urls:
          failed[url] = res['Message']
      else:
        allResults.extend( res['Value'] )

    #gDataStoreClient.commit()
    resDict = {}
    resDict['AllResults'] = allResults
    resDict['Failed'] = failed
    return S_OK( resDict )

  def __gfalprestage_wrapper( self, urls, lifetime ):
    """ This is a function that can be reused everywhere to perform the gfal_prestage
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
      res = self.__gfal_operation_wrapper( 'gfal_prestage', gfalDict, timeout_sendreceive = self.fileTimeout * len( urls ) )
      gDataStoreClient.addRegister( res['AccountingOperation'] )
      if not res['OK']:
        for url in urls:
          failed[url] = res['Message']
      else:
        allResults.extend( res['Value'] )

    #gDataStoreClient.commit()
    resDict = {}
    resDict['AllResults'] = allResults
    resDict['Failed'] = failed
    return S_OK( resDict )

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

    #gDataStoreClient.commit()
    resDict = {}
    resDict['AllResults'] = allResults
    resDict['Failed'] = failed
    return S_OK( resDict )

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

    #gDataStoreClient.commit()
    resDict = {}
    resDict['AllResults'] = allResults
    resDict['Failed'] = failed
    return S_OK( resDict )

  def __gfalremovedir_wrapper( self, urls ):
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

    #gDataStoreClient.commit()
    resDict = {}
    resDict['AllResults'] = allResults
    resDict['Failed'] = failed
    return S_OK( resDict )

  def __gfal_pin_wrapper( self, urls, lifetime ):
    gfalDict = {}
    gfalDict['defaultsetype'] = 'srmv2'
    gfalDict['no_bdii_check'] = 0
    gfalDict['srmv2_spacetokendesc'] = self.spaceToken
    gfalDict['srmv2_desiredpintime'] = lifetime

    allResults = []
    failed = {}

    srmRequestFiles = {}
    for url, srmRequestID in urls.items():
      if not srmRequestFiles.has_key( srmRequestID ):
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

    #gDataStoreClient.commit()
    resDict = {}
    resDict['AllResults'] = allResults
    resDict['Failed'] = failed
    return S_OK( resDict )

  def __gfal_prestagestatus_wrapper( self, urls ):
    gfalDict = {}
    gfalDict['defaultsetype'] = 'srmv2'
    gfalDict['no_bdii_check'] = 0
    gfalDict['srmv2_spacetokendesc'] = self.spaceToken

    allResults = []
    failed = {}

    srmRequestFiles = {}
    for url, srmRequestID in urls.items():
      if not srmRequestFiles.has_key( srmRequestID ):
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

    #gDataStoreClient.commit()
    resDict = {}
    resDict['AllResults'] = allResults
    resDict['Failed'] = failed
    return S_OK( resDict )

  def __gfal_release_wrapper( self, urls ):
    gfalDict = {}
    gfalDict['defaultsetype'] = 'srmv2'
    gfalDict['no_bdii_check'] = 0

    allResults = []
    failed = {}

    srmRequestFiles = {}
    for url, srmRequestID in urls.items():
      if not srmRequestFiles.has_key( srmRequestID ):
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

    #gDataStoreClient.commit()
    resDict = {}
    resDict['AllResults'] = allResults
    resDict['Failed'] = failed
    return S_OK( resDict )

  def __gfal_operation_wrapper( self, operation, gfalDict, srmRequestID = None, timeout_sendreceive = None ):

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


    timeout = gfalDict['timeout']
    if timeout_sendreceive:
      timeout = timeout_sendreceive
    res = pythonCall( ( timeout + 300 ), self.__gfal_wrapper, operation, gfalDict, srmRequestID, timeout_sendreceive )
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

    res = self.__create_gfal_object( gfalDict )
    if not res['OK']:
      result = S_ERROR( res['Message'] )
      return result

    gfalObject = res['Value']
    if srmRequestID:
      res = self.__gfal_set_ids( gfalObject, srmRequestID )
      if not res['OK']:
        result = S_ERROR( res['Message'] )
        return result

    res = self.__gfal_exec( gfalObject, operation, timeout_sendreceive )
    if not res['OK']:
      result = S_ERROR( res['Message'] )
      return result

    gfalObject = res['Value']
    res = self.__gfal_get_ids( gfalObject )
    if not res['OK']:
      newSRMRequestID = srmRequestID
    else:
      newSRMRequestID = res['Value']

    res = self.__get_results( gfalObject )
    if not res['OK']:
      result = S_ERROR( res['Message'] )
      return result

    resultList = []
    pfnRes = res['Value']
    for myDict in pfnRes:
      myDict['SRMReqID'] = newSRMRequestID
      resultList.append( myDict )

    self.__destroy_gfal_object( gfalObject )
    result = S_OK( resultList )
    return result

  def __initialiseAccountingObject( self, operation, se, files ):
    import DIRAC
    accountingDict = {}
    accountingDict['OperationType'] = operation
    accountingDict['User'] = 'acsmith'
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

  # These methods are for the creation of the gfal object

  def __create_gfal_object( self, gfalDict ):
    self.log.debug( "SRM2Storage.__create_gfal_object: Performing gfal_init." )
    errCode, gfalObject, errMessage = self.gfal.gfal_init( gfalDict )
    if not errCode == 0:
      errStr = "SRM2Storage.__create_gfal_object: Failed to perform gfal_init."
      if not errMessage:
        errMessage = os.strerror( errCode )
      self.log.error( errStr, errMessage )
      return S_ERROR( "%s%s" % ( errStr, errMessage ) )
    else:
      self.log.debug( "SRM2Storage.__create_gfal_object: Successfully performed gfal_init." )
      return S_OK( gfalObject )

  def __gfal_set_ids( self, gfalObject, srmRequestID ):
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

  # These methods are for the execution of the functionality

  def __gfal_exec( self, gfalObject, method, timeout_sendreceive = None ):
    """
      In gfal, for every method (synchronous or asynchronous), you can define a sendreceive timeout and a connect timeout.
      The connect timeout sets the maximum amount of time a client accepts to wait before establishing a successful TCP connection to SRM (default 60 seconds).
      The sendreceive timeout, allows a client to set the maximum time the send
      of a request to SRM can take (normally all send operations return immediately unless there is no free TCP buffer) 
      and the maximum time to receive a reply (a token for example). Default 0, i.e. no timeout.
      The srm timeout for asynchronous requests default to 3600 seconds
    
      gfal_set_timeout_connect (int value)

      gfal_set_timeout_sendreceive (int value)

      gfal_set_timeout_bdii (int value)

      gfal_set_timeout_srm (int value)
      
    """
    self.log.debug( "SRM2Storage.__gfal_exec: Performing %s." % method )
    fcn = None
    if hasattr( self.gfal, method ) and callable( getattr( self.gfal, method) ):
      fcn = getattr( self.gfal, method )
    if not fcn:
      return S_ERROR( "Unable to invoke %s for gfal, it isn't a member function" % method )
    if timeout_sendreceive:
      self.gfal.gfal_set_timeout_sendreceive( timeout_sendreceive )
    errCode, gfalObject, errMessage = fcn( gfalObject )
    if errCode:
      errStr = "SRM2Storage.__gfal_exec: Failed to perform %s." % method
      if not errMessage:
        errMessage = os.strerror( errCode )
      self.log.error( errStr, errMessage )
      return S_ERROR( "%s%s" % ( errStr, errMessage ) )
    self.log.debug( "SRM2Storage.__gfal_exec: Successfully performed %s." % method )
    return S_OK( gfalObject )
    
  # These methods are for retrieving output information

  def __get_results( self, gfalObject ):
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
    self.log.debug( "SRM2Storage.__gfal_get_ids: Performing gfal_get_ids." )
    numberOfResults, gfalObject, srm1RequestID, srm1FileIDs, srmRequestToken = self.gfal.gfal_get_ids( gfalObject )
    if numberOfResults <= 0:
      errStr = "SRM2Storage.__gfal_get_ids: Did not obtain SRM request ID."
      self.log.error( errStr )
      return S_ERROR( errStr )
    else:
      self.log.debug( "SRM2Storage.__get_gfal_ids: Retrieved SRM request ID %s." % srmRequestToken )
      return S_OK( srmRequestToken )

  # Destroy the gfal object after use

  def __destroy_gfal_object( self, gfalObject ):
    self.log.debug( "SRM2Storage.__destroy_gfal_object: Performing gfal_internal_free." )
    self.gfal.gfal_internal_free( gfalObject )
    return S_OK()

