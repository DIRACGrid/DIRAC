""" :mod: SRM2V2Storage
    =================

    .. module: python
    :synopsis: SRM v2 interface to StorageElement using gfal2
"""
# # imports
import os
import datetime
import errno
import gfal2
from types import StringType, ListType
from stat import S_ISREG, S_ISDIR, S_IXUSR, S_IRUSR, S_IWUSR, \
  S_IRWXG, S_IRWXU, S_IRWXO
# # from DIRAC
from DIRAC import gLogger, gConfig, S_OK, S_ERROR
from DIRAC.Resources.Utilities import checkArgumentFormat
from DIRAC.Resources.Storage.StorageBase import StorageBase
from DIRAC.Core.Security.ProxyInfo import getProxyInfo
from DIRAC.ConfigurationSystem.Client.Helpers.Registry import getVOForGroup
from DIRAC.Core.Utilities.Pfn import pfnparse, pfnunparse
from DIRAC.Core.Utilities.File import getSize


# # RCSID
__RCSID__ = "$Id$"

class SRM2V2v13Storage( StorageBase ):
  """ .. class:: SRM2V2Storage

  SRM v2 interface to StorageElement using gfal2
  """

  def __init__( self, storageName, parameters ):
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

#     os.environ['GLOBUS_THREAD_MODEL'] = "pthread"
#     import gfal2

    self.log = gLogger.getSubLogger( "SRM2V2Storage", True )

    # Different levels or verbosity:
    # gfal2.verbose_level.normal,
    # gfal2.verbose_level.verbose,
    # gfal2.verbose_level.debug,
    # gfal2.verbose_level.trace

    dlevel = self.log.getLevel()
    if dlevel == 'DEBUG':
      gfal2.set_verbose( gfal2.verbose_level.trace )


    self.isok = True

    # # gfal2 API
    self.gfal2 = gfal2.creat_context()

    # # save c'tor params
    self.protocolName = 'SRM2V2'
    self.name = storageName
    self.protocol = parameters['Protocol']
    self.spaceToken = parameters['SpaceToken']

    # # init base class
    StorageBase.__init__( self, storageName, parameters )

    # #stage limit - 12h
    self.stageTimeout = gConfig.getValue( '/Resources/StorageElements/StageTimeout', 12 * 60 * 60 )  # gConfig -> [get] ConfigurationClient()
    # # 1 file timeout
    self.fileTimeout = gConfig.getValue( '/Resources/StorageElements/FileTimeout', 30 )
    # # nb of surls per gfal2 call
    self.filesPerCall = gConfig.getValue( '/Resources/StorageElements/FilesPerCall', 20 )
    # # gfal2 timeout
    self.gfal2Timeout = gConfig.getValue( "/Resources/StorageElements/GFAL_Timeout", 100 )
    # # gfal2 long timeout
    self.gfal2LongTimeOut = gConfig.getValue( "/Resources/StorageElements/GFAL_LongTimeout", 1200 )
    # # gfal2 retry on errno.ECONN
    self.gfal2Retry = gConfig.getValue( "/Resources/StorageElements/GFAL_Retry", 3 )


    # # set checksum type, by default this is 0 (GFAL_CKSM_NONE)
    self.checksumType = gConfig.getValue( "/Resources/StorageElements/ChecksumType", 0 )
    # enum gfal_cksm_type, all in lcg_util
    #   GFAL_CKSM_NONE = 0,
    #   GFAL_CKSM_CRC32,
    #   GFAL_CKSM_ADLER32,
    #   GFAL_CKSM_MD5,
    #   GFAL_CKSM_SHA1
    # GFAL_CKSM_NULL = 0
    self.checksumTypes = { None : 0, "CRC32" : 1, "ADLER32" : 2,
                           "MD5" : 3, "SHA1" : 4, "NONE" : 0, "NULL" : 0 }
    if self.checksumType:
      if str( self.checksumType ).upper() in self.checksumTypes:
        gLogger.debug( "SRM2V2Storage: will use %s checksum check" % self.checksumType )
        self.checksumType = self.checksumTypes[ self.checksumType.upper() ]
      else:
        gLogger.warn( "SRM2V2Storage: unknown checksum type %s, checksum check disabled" )
        # # GFAL_CKSM_NONE
        self.checksumType = 0
    else:
      # # invert and get name
      self.log.debug( "SRM2V2Storage: will use %s checksum" % dict( zip( self.checksumTypes.values(),
                                                                     self.checksumTypes.keys() ) )[self.checksumType] )
    self.voName = None
    ret = getProxyInfo( disableVOMS = True )
    if ret['OK'] and 'group' in ret['Value']:
      self.voName = getVOForGroup( ret['Value']['group'] )
    self.defaultLocalProtocols = gConfig.getValue( '/Resources/StorageElements/DefaultProtocols', [] )

    self.MAX_SINGLE_STREAM_SIZE = 1024 * 1024 * 10  # 10 MB ???
    self.MIN_BANDWIDTH = 0.5 * ( 1024 * 1024 )  # 0.5 MB/s ???




  def exists( self, path ):
    """ Check if the path exists on the storage

    :param self: self reference
    :param str path: path or list of paths to be checked
    :returns Failed dictionary: {pfn : error message}
             Successful dictionary: {pfn : bool}
             S_ERROR in case of argument problems
    """
    res = checkArgumentFormat( path )
    if not res['OK']:
      return res
    urls = res['Value']

    self.log.debug( "SRM2V2Storage.exists: Checking the existence of %s path(s)" % len( urls ) )

    successful = {}
    failed = {}

    for url in urls:
      res = self.__singleExists( url )

      if res['OK']:
        successful[url] = res['Value']
      else:  # something went wrong with the query
        failed[url] = res['Message']

    resDict = { 'Failed':failed, 'Successful':successful }
    return S_OK( resDict )



  def __singleExists( self, path ):
    """ Check if :path: exists on the storage

    :param self: self reference
    :param str: path to be checked (srm://...)
    :returns
        S_OK ( boolean exists ) a boolean whether it exists or not
        S_ERROR( errStr ) there is a problem with getting the information
    """
    self.log.debug( "SRM2V2Storage._singleExists: Determining whether %s exists or not" % path )

    try:
      self.gfal2.set_opt_boolean( "BDII", "ENABLE", False )
      self.gfal2.set_opt_integer( "SRM PLUGIN", "OPERATION_TIMEOUT", self.gfal2Timeout )
      self.gfal2.stat( path )  # If path doesn't exist this will raise an error - otherwise path exists
      self.log.debug( "SRM2V2Storage.__singleExists: path exists" )
      return S_OK( True )
    except gfal2.GError, e:
      if e.code == errno.ENOENT:
        errStr = "SRM2V2Storage.__singleExists: Path does not exist"
        self.log.debug( errStr )
        return  S_OK( False )
      else:
        errStr = "SRM2V2Storage.__singleExists: Failed to determine existence of path"
        self.log.debug( errStr, e.message )
        return S_ERROR( errStr )



### methods for manipulating files ###

  def isFile( self, path ):
    """ Check if the path provided is a file or not

    :param self: self reference
    :param str: path or list of paths to be checked ( 'srm://...')
    :returns Failed dict: {path : error message}
             Successful dict: {path : bool}
             S_ERROR in case of argument problems

    """
    res = checkArgumentFormat( path )
    if not res['OK']:
      return res
    urls = res['Value']

    self.log.debug( "SRM2V2Storage.isFile: checking whether %s path(s) are file(s)." % len( urls ) )

    successful = {}
    failed = {}

    for url in urls:
      res = self.__isSingleFile( url )

      if res['OK']:
        successful[url] = res['Value']
      else:
        failed[url] = res['Message']

    return S_OK( { 'Failed' : failed, 'Successful' : successful } )



  def __isSingleFile( self, path ):
    """ Checking if :path: exists and is a file
    :param self: self reference
    :param str path: single path on the storage (srm://...)

    :returns
        S_ERROR if there is a fatal error
        S_OK ( boolean) if it is a file or not
    """

    self.log.debug( "SRM2V2Storage.__isSingleFile: Determining whether %s is a file or not." % path )

    try:
      self.gfal2.set_opt_boolean( "BDII", "ENABLE", False )
      self.gfal2.set_opt_integer( "SRM PLUGIN", "OPERATION_TIMEOUT", self.gfal2Timeout )
      statInfo = self.gfal2.stat( path )
      if ( S_ISREG( statInfo.st_mode ) ):  # alternatively drop if/else and just return S_OK ( S_ISREG( statInfo.st_mode ) ) but that's not really readable and we can't write the log.
        return S_OK( True )
      else:
        self.log.debug( "SRM2V2Storage.__isSingleFile: Path is not a file" )
        return S_OK( False )
    except gfal2.GError, e:
      if e.code == errno.ENOENT:
        errStr = "SRM2V2Storage.__isSingleFile: File does not exist."
        self.log.debug( errStr, e.message )
        return S_ERROR( errStr )
      else:
        errStr = "SRM2V2Storage.__isSingleFile: failed to determine if path %s is a file." % path
        self.log.error( errStr, e.message )
        return S_ERROR( errStr )



  def putFile( self, path, sourceSize = 0 ):
    """ Put a copy of a local file or a file on another srm storage to a directory on the
        physical storage.
        :param path: dictionary { lfn (srm://...) : localFile }
        :returns Successful dict: { path : size }
                 Failed dict: { path : error message }
                 S_ERROR in case of argument problems
    """
    res = checkArgumentFormat( path )
    if not res['OK']:
      return res
    urls = res['Value']
    
    failed = {}
    successful = {}

    for dest_url, src_file in urls.items():
      if not src_file:
        errStr = "SRM2V2Storage.putFile: Source file not set. Argument must be a dictionary (or a list of a dictionary) {url : local path}"
        self.log.debug( errStr )
        return S_ERROR( errStr )
      res = self.__putSingleFile( src_file, dest_url, sourceSize )

      if res['OK']:
        successful[dest_url] = res['Value']
      else:
        failed[dest_url] = res['Message']

    return S_OK( { 'Failed' : failed, 'Successful' : successful } )



  def __putSingleFile( self, src_file, dest_url, sourceSize ):
    """Put a copy of the local file to the current directory on the
       physical storage

       For gfal2 version 2.7.8 and lower the environment variable GLOBUS_THREAD_MODEL has to be
       set to 'pthread' otherwise dirac-dms-add-file will go in a deadlock. This is fixed with gfal2 2.8.

       :param str src_file: local file to copy
       :param str dest_file: pfn (srm://...)
       :param int sourceSize: size of the source file
       :returns: S_OK( fileSize ) if everything went fine, S_ERROR otherwise
    """
    self.log.debug( "SRM2V2Storage.__putSingleFile: trying to upload %s to %s" % ( src_file, dest_url ) )

    # in case src_file is not set (this is also done in putFile but some methods directly access this method,
    # so that's why it's checked here one more time
    if not src_file:
      errStr = 'SRM2V2Storage.__putSingleFile: no source defined, please check argument format { destination : localfile }'
      return S_ERROR( errStr )
    else:
      # check whether the source is local or on another srm storage
      if src_file.startswith( 'srm' ):
        src_url = src_file
        if not sourceSize:
          errStr = "SRM2V2Storage.__putFile: For file replication the source file size in bytes must be provided."
          self.log.error( errStr, src_file )
          return S_ERROR( errStr )
      
    # file is local so we can set the protocol and determine source size accordingly
      else:
        if not os.path.exists( src_file ) or not os.path.isfile( src_file ):
          errStr = "SRM2V2Storage.__putFile: The local source file does not exist or is a directory"
          self.log.error( errStr, src_file )
          return S_ERROR( errStr )
        src_url = 'file://%s' % src_file
        sourceSize = getSize( src_file )
        if sourceSize == -1:
          errStr = "SRM2V2Storage.__putFile: Failed to get file size"
          self.log.error( errStr, src_file )
          return S_ERROR( errStr )
        if sourceSize == 0:
          errStr = "SRM2V2Storage.__putFile: Source file size is zero."
          self.log.error( errStr, src_file )
          return S_ERROR( errStr )


    # source is OK, creating the destination folder
    dest_path = os.path.dirname( dest_url )
    res = self.__createSingleDirectory( dest_path )
    if not res['OK']:
      errStr = "SRM2V2Storage.__putSingleFile: Failed to create destination folder %s" % dest_path
      gLogger.error( errStr, res['Message'] )
      return S_ERROR( errStr )
    # folder is created and file exists, setting known copy parameters
    params = self.gfal2.transfer_parameters()
    params.timeout = int( sourceSize / self.MIN_BANDWIDTH + 300 )
    if sourceSize > self.MAX_SINGLE_STREAM_SIZE:
      params.nbstreams = 4
    else:
      params.nbstreams = 1
    params.overwrite = True  # old gfal removed old file first, gfal2 can just overwrite it with this flag set to True
    params.dst_spacetoken = self.spaceToken

    params.checksum_check = True if self.checksumType else False

    # Params set, copying file now
    try:
      self.gfal2.set_opt_string( "SRM PLUGIN", "SPACETOKENDESC", self.spaceToken )
      self.gfal2.set_opt_boolean( "BDII", "ENABLE", False )
      self.gfal2.filecopy( params, src_url, dest_url )
      if self.checksumType:
        # checksum check is done by gfal2
        return S_OK( sourceSize )
      # no checksum check, compare file sizes for verfication
      else:
        res = self.__getSingleFileSize( dest_url )
        if res['OK']:
          destSize = res['Value']
        else:
          destSize = None
        self.log.debug( 'SRM2V2Storage.__putSingleFile: destSize: %s, sourceSize: %s' % ( destSize, sourceSize ) )
        if destSize == sourceSize:
          return S_OK( destSize )
        else:
          self.log.debug( "SRM2V2Storage.__putSingleFile: Source and destination file size don't match. Trying to remove destination file" )
          res = self.__removeSingleFile( dest_url )
          if not res['OK']:
            errStr = 'SRM2V2Storage.__putSingleFile: Failed to remove destination file: %' % res['Message']
            return S_ERROR( errStr )
          errStr = "SRM2V2Storage.__putSingleFile: Source and destination file size don't match. Removed destination file"
          self.log.error( errStr, {sourceSize : destSize} )
          return S_ERROR( errStr )
    except gfal2.GError, e:
      # extended error message because otherwise we could only guess what the error could be when we copy from another srm to our srm-SE
      errStr = "SRM2V2Storage.__putSingleFile: Failed to copy file %s to destination url %s: [%d] %s" % ( src_file, dest_url, e.code, e.message )
      return S_ERROR( errStr )



  def getFile( self, path, localPath = False ):
    """ Make a local copy of storage :path:

    :param self: self reference
    :param str path: path (or list of paths) on storage (srm://...)
    :returns Successful dict: {path : size}
             Failed dict: {path : errorMessage}
             S_ERROR in case of argument problems
    """

    res = checkArgumentFormat( path )
    if not res['OK']:
      return res
    urls = res['Value']

    self.log.debug( "SRM2V2Storage.getFile: Trying to download %s files." % len( urls ) )

    failed = {}
    successful = {}

    for src_url in urls:
      fileName = os.path.basename( src_url )
      if localPath:
        dest_file = '%s/%s' % ( localPath, fileName )
      else:
        dest_file = '%s/%s' % ( os.getcwd(), fileName )
      res = self.__getSingleFile( src_url, dest_file )

      if not res['OK']:
        failed[src_url] = res['Message']
      else:
        successful[src_url] = res['Value']
        
    return S_OK( { 'Failed' : failed, 'Successful' : successful} )



  def __getSingleFile( self, src_url, dest_file ):
    """ Copy a storage file :src_url: to a local fs under :dest_file:

    :param self: self reference
    :param str src_url: SE url that is to be copied (srm://...)
    :param str dest_file: local fs path
    :returns: S_ERROR( errStr ) in case of an error
              S_OK( size of file ) if copying is successful
    """
    self.log.info( "SRM2V2Storage.__getSingleFile: Trying to download %s to %s" % ( src_url, dest_file ) )

    if not os.path.exists( os.path.dirname( dest_file ) ):
      self.log.debug( "SRM2V2Storage.__getSingleFile: Local directory does not yet exist. Creating...", os.path.dirname( dest_file ) )
      try:
        os.makedirs( os.path.dirname( dest_file ) )
      except OSError, error:
        errStr = "SRM2V2Storage.__getSingleFile: Error while creating the destination folder"
        self.log.exception( errStr, error )
        return S_ERROR( errStr )

    res = self.__getSingleFileSize( src_url )


    if not res['OK']:
      errStr = "SRM2V2Storage.__getSingleFile: Error while determining file size: %s" % res['Message']
      self.log.error( res['Message'] )
      return S_ERROR( errStr )

    remoteSize = res['Value']

    # Set gfal2 copy parameters
    # folder is created and file exists, setting known copy parameters
    params = self.gfal2.transfer_parameters()
    params.timeout = int( remoteSize / self.MIN_BANDWIDTH + 300 )
    if remoteSize > self.MAX_SINGLE_STREAM_SIZE:
      params.nbstreams = 4
    else:
      params.nbstreams = 1
    params.overwrite = True  # old gfal removed old file first, gfal2 can just overwrite it with this flag set to True
    params.src_spacetoken = self.spaceToken


    params.checksum_check = True if self.checksumType else False

    # Params set, copying file now
    try:
      # gfal2 needs a protocol to copy local which is 'file:'
      dest = 'file://' + dest_file
      self.gfal2.set_opt_string( "SRM PLUGIN", "SPACETOKENDESC", self.spaceToken )
      self.gfal2.set_opt_boolean( "BDII", "ENABLE", False )
      self.gfal2.filecopy( params, src_url, dest )
      if self.checksumType:
        # gfal2 did a checksum check, so we should be good
        return S_OK( remoteSize )
      else:
        # No checksum check was done so we compare file sizes
        destSize = getSize( dest_file )
        if destSize == remoteSize:
          return S_OK( destSize )
        else:
          errStr = "SRM2V2Storage.__getSingleFile: File sizes don't match. Something went wrong. Removing local file %s" % dest_file
          self.log.error( errStr, {remoteSize : destSize} )
          if os.path.exists( dest_file ):
            os.remove( dest_file )
          return S_ERROR( errStr )
    except gfal2.GError, e:
      errStr = 'SRM2V2Storage.__getSingleFile: Could not copy %s to %s, [%d] %s' % ( src_url, dest, e.code, e.message )
      # errStr = "SRM2V2Storage.__getSingleFile: Failed to copy file %s to destination url %s " % ( src_url, dest_file )
      self.log.error( errStr )
      return S_ERROR( errStr )


  def removeFile( self, path ):
    """ Physically remove the file specified by path

    A non existing file will be considered as successfully removed

    :param str path: path (or list of paths) on storage (srm://...)
    :returns Successful dict {path : True}
             Failed dict {path : error message}
             S_ERROR in case of argument problems
    """
    res = checkArgumentFormat( path )
    if not res['OK']:
      return res
    urls = res['Value']

    self.log.debug( "SRM2V2Storage.removeFile: Attemping to remove %s files" % len( urls ) )

    failed = {}
    successful = {}

    for url in urls:
      res = self.__removeSingleFile( url )

      if res['OK']:
        successful[url] = res['Value']
      else:
        failed [url] = res['Message']

    return S_OK( { 'Failed' : failed, 'Successful' : successful } )



  def __removeSingleFile( self, path ):
    """ Physically remove the file specified by path
    :param str path: path on storage (srm://...)
    :returns
             S_OK( True )  if the removal was successful (also if file didnt exist in the first place)
             S_ERROR( errStr ) if there was a problem removing the file
    """
    self.log.debug( "SRM2V2Storage.__removeSingleFile: Attemping to remove single file %s" % path )

    try:
      self.gfal2.set_opt_boolean( "BDII", "ENABLE", False )
      self.gfal2.set_opt_integer( "SRM PLUGIN", "OPERATION_TIMEOUT", self.gfal2Timeout )
      status = self.gfal2.unlink( path )
      if status == 0:
        self.log.debug( "SRM2V2Storage.__removeSingleFile: File successfully removed" )
        return S_OK( True )
      elif status < 0:
        errStr = 'SRM2V2Storage.__removeSingleFile: return status < 0. Error occured.'
        return S_ERROR( errStr )
    except gfal2.GError, e:
      # file doesn't exist so operation was successful
      if e.code == errno.ENOENT:
        errStr = "SRM2V2Storage.__removeSingleFile: File does not exist."
        self.log.debug( "SRM2V2Storage.__removeSingleFile: Error while removing file." )
        return S_OK( True )
      elif e.code == errno.EISDIR:
        errStr = "SRM2V2Storage.__removeSingleFile: path is a directory."
        self.log.debug( "SRM2V2Storage.__removeSingleFile: Path is a directory." )
        return S_ERROR( errStr )
      else:
        errStr = "SRM2V2Storage.__removeSingleFile: Failed to remove file."
        self.log.debug( "SRM2V2Storage.__removeSingleFile: Failed to remove file: [%d] %s" % ( e.code, e.message ) )
        return  S_ERROR( errStr )



  def getFileSize( self, path ):
    """Get the physical size of the given file

      :param self: self reference
      :param path: path (or list of path) on storage (pfn : srm://...)
      :returns Successful dict {path : size}
             Failed dict {path : error message }
             S_ERROR in case of argument problems
    """

    res = checkArgumentFormat( path )
    if not res['OK']:
      return res
    urls = res['Value']

    self.log.debug( "SRM2V2Storage.getFileSize: Trying to determine file size of %s files" % len( urls ) )

    failed = {}
    successful = {}

    for url in urls:
      res = self.__getSingleFileSize( url )

      if not res['OK']:
        failed[url] = res['Message']
      else:
        successful[url] = res['Value']
        
    return S_OK( { 'Failed' : failed, 'Successful' : successful } )



  def __getSingleFileSize( self, path ):
    """ Get the physical size of the given file

    :param self: self reference
    :param path: single path on the storage (srm://...)
    :returns S_OK( filesize ) when successfully determined filesize
             S_ERROR( errStr ) filesize could not be determined
    """
    self.log.debug( "SRM2V2Storage.__getSingleFileSize: Determining file size of %s" % path )

    res = self.__isSingleFile( path )
    if not res['OK']:
      return res

    if not res['Value']:
      errStr = 'SRM2V2Storage.__getSingleFileSize: path is not a file'
      self.log.debug( 'SRM2V2Storage.__getSingleFileSize: path is not a file' )
      return S_ERROR( errStr )
    else:  # if this is true, path is a file
      try:
        self.gfal2.set_opt_boolean( "BDII", "ENABLE", False )
        self.gfal2.set_opt_integer( "SRM PLUGIN", "OPERATION_TIMEOUT", self.gfal2Timeout )
        statInfo = self.gfal2.stat( path )  # keeps info like size, mode.
        self.log.debug( "SRM2V2Storage.__singleExists: File size successfully determined" )
        return S_OK( long ( statInfo.st_size ) )
      except gfal2.GError, e:
          errStr = "SRM2V2Storage.__singleExists: Failed to determine file size."
          self.log.error( errStr, e.message )
          return S_ERROR( errStr )



  def getFileMetadata( self, path ):
    """ Get metadata associated to the file(s)

    :param self: self reference
    :param str path: path (or list of paths) on the storage (srm://...)
    :returns successful dict { path : metadata }
             failed dict { path : error message }
             S_ERROR in case of argument problems
    """
    res = checkArgumentFormat( path )
    if not res['OK']:
      return res
    urls = res['Value']

    self.log.debug( 'SRM2V2Storage.getFileMetadata: trying to read metadata for %s paths' % len( urls ) )

    failed = {}
    successful = {}

    for url in urls:
      res = self.__getSingleFileMetadata( url )

      if not res['OK']:
        failed[url] = res['Message']
      else:
        successful[url] = res['Value']

    return S_OK( { 'Failed' : failed, 'Successful' : successful } )



  def __getSingleFileMetadata( self, path ):
    """  Fetch the metadata associated to the file
      :param self: self reference
      :param path: path (only 1) on storage (srm://...)
      :returns:
          S_OK (MetadataDict) if we could get the metadata
          S_ERROR (errorMsg) if there was a problem getting the metadata or if it is not a file
    """
    self.log.debug( 'SRM2V2Storage.__getSingleFileMetadata: trying to read metadata for %s' % path )

    res = self.__getSingleMetadata( path )

    if not res['OK']:
      return res  # res is S_ERROR ( errStr )
    
    metaDict = res['Value']

    if not metaDict['File']:
      errStr = "SRM2V2Storage.__getSingleFileMetadata: supplied path is not a file"
      self.log.error( errStr, path )
      return S_ERROR( errStr )

    return S_OK( metaDict )



  def __getSingleMetadata(self, path):
    """  Fetches the metadata of a single file or directory via gfal2.stat
         and getExtendedAttributes

      :param self: self reference
      :param path: path (only 1) on storage (srm://...)
      :returns:
          S_OK ( MetadataDict ) if we could get the metadata
          S_ERROR ( errorMsg ) if there was a problem getting the metadata
    """
    self.log.debug( 'SRM2V2Storage.__getSingleMetadata: reading metadata for %s' % path )

    try:
      self.gfal2.set_opt_integer( "SRM PLUGIN", "OPERATION_TIMEOUT", self.gfal2Timeout )
      statInfo = self.gfal2.stat( path )
      metadataDict = self.__parseStatInfoFromApiOutput( statInfo )

      res = self.__getExtendedAttributes( path )

      # add extended attributes to the dict if available
      if res['OK']:
        attributeDict = res['Value']
      # 'user.status' is the extended attribute we are interested in
      if metadataDict['File']:
        res = self.__getChecksum( path )
        if res['OK']:
          metadataDict['Checksum'] = res['Value']
        if 'user.status' in attributeDict.keys():
          if attributeDict['user.status'] == 'ONLINE':
            metadataDict['Cached'] = 1
          else:
            metadataDict['Cached'] = 0
          if attributeDict['user.status'] == 'NEARLINE':
            metadataDict['Migrated'] = 1
          else:
            metadataDict['Migrated'] = 0
          if attributeDict['user.status'] == 'LOST':
            metadataDict['Lost'] = 1
          else:
            metadataDict['Lost'] = 0
          if attributeDict['user.status'] == 'UNAVAILABLE':
            metadataDict['Unavailable'] = 1
          else:
            metadataDict['Unavailable'] = 0

      return S_OK ( metadataDict )

    except gfal2.GError, e:
      if e.code == errno.ENOENT:
        errStr = "SRM2V2Storage.__getSingleMetadata: Path does not exist"
        self.log.error( errStr, e.message )
        return S_ERROR ( errStr )
      else:
        errStr = "SRM2V2Storage.__getSingleMetadata: Failed to retrieve metadata from path."
        self.log.error( errStr, e.message )
        return S_ERROR( errStr )



  def prestageFile( self, path, lifetime = 86400 ):
    """ Issue prestage request for file(s)

    :param self: self reference
    :param str path: path or list of paths to be prestaged
    :param int lifetime: prestage lifetime in seconds (default 24h)

    :return succesful dict { url : token }
            failed dict { url : message }
            S_ERROR in case of argument problems
    """
    res = checkArgumentFormat( path )
    if not res['OK']:
      return res
    urls = res['Value']

    self.log.debug( 'SRM2V2Storage.prestageFile: Attempting to issue stage requests for %s file(s).' % len( urls ) )

    failed = {}
    successful = {}
    for url in urls:
      res = self.__prestageSingleFile( url, lifetime )

      if not res['OK']:
        failed[url] = res['Message']
      else:
        successful[url] = res['Value']
    return S_OK( { 'Failed' : failed, 'Successful' : successful } )



  def __prestageSingleFile( self, path, lifetime ):
    """ Issue prestage for single file

    :param self: self reference
    :param str path: path to be prestaged
    :param int lifetime: prestage lifetime in seconds (default 24h)

    :return S_ structure
                            S_OK( token ) ) if status >= 0 (0 - staging is pending, 1 - file is pinned)
                            S_ERROR( errMsg ) ) in case of an error: status -1
    """
    self.log.debug( "SRM2V2Storage.__prestageSingleFile: Attempting to issue stage request for single file: %s" % path )

    try:
      self.log.debug( "SRM2V2Storage.__prestageSingleFile: spaceToken: %s" % self.spaceToken )
      self.gfal2.set_opt_string( "SRM PLUGIN", "SPACETOKENDESC", self.spaceToken )
      self.gfal2.set_opt_string_list( "SRM PLUGIN", "TURL_PROTOCOLS", self.defaultLocalProtocols )
      self.gfal2.set_opt_boolean( "BDII", "ENABLE", False )
      self.gfal2.set_opt_integer( "SRM PLUGIN", "OPERATION_TIMEOUT", self.gfal2Timeout )
      ( status, token ) = self.gfal2.bring_online( path, lifetime, self.stageTimeout, True )
      self.log.debug( "SRM2V2Storage.__prestageSingleFile: Staging issued - Status: %s" % status )
      if status >= 0:
        return S_OK( token )
      else:
        errStr = 'SRM2V2Storage.__prestageSingleFile: an error occured while issuing prestaging.'
        return S_ERROR( errStr )
    except gfal2.GError, e:
      errStr = "SRM2V2Storage.__prestageSingleFile: Error occured while prestaging file %s. [%d] %s" % ( path, e.code, e.message )
      self.log.error( errStr, e.message )
      return S_ERROR( errStr )



  def prestageFileStatus( self, path ):
    """ Checking the staging status of file(s) on the storage

    :param self: self reference
    :param dict path: dict { url : token }
    :return succesful dict { url : bool }
            failed dict { url : message }
            S_ERROR in case of argument problems
    """
    res = checkArgumentFormat( path )
    if not res['OK']:
      return res
    urls = res['Value']

    self.log.debug( 'SRM2V2Storage.prestageFileStatus: Checking the staging status for %s file(s).' % len( urls ) )

    failed = {}
    successful = {}
    for path, token in urls.items():

      res = self.__prestageSingleFileStatus( path, token )
      if not res['OK']:
        failed[path] = res['Message']
      else:
        successful[path] = res['Value']
    return S_OK( { 'Failed' : failed, 'Successful' : successful } )



  def __prestageSingleFileStatus( self, path, token ):
    """ Check prestage status for single file

    :param self: self reference
    :param str path: path to be checked
    :param str token: token of the file

    :return S_ structure
                            S_OK( True ) if file is staged
                            S_OK( False ) if file is not staged yet
                            S_ERROR( errMsg ) ) in case of an error: status -1
    """
    
    self.log.debug( "SRM2V2Storage.__prestageSingleFileStatus: Checking prestage file status for %s" % path )
    # also allow int as token - converting them to strings
    if not type( token ) == StringType:
        token = str( token )
    try:
      self.gfal2.set_opt_string( "SRM PLUGIN", "SPACETOKENDESC", self.spaceToken )
      self.gfal2.set_opt_string_list( "SRM PLUGIN", "TURL_PROTOCOLS", self.defaultLocalProtocols )
      self.gfal2.set_opt_integer( "SRM PLUGIN", "OPERATION_TIMEOUT", self.gfal2Timeout )
      self.gfal2.set_opt_boolean( "BDII", "ENABLE", True )
      status = self.gfal2.bring_online_poll( path, token )
      if status == 0:
        self.log.debug( "SRM2V2Storage.__prestageSingleFileStatus: File not staged" )
        return S_OK( False )
      elif status == 1:
        self.log.debug( "SRM2V2Storage.__prestageSingleFileStatus: File is staged" )
        return S_OK( True )
      else:
        errStr = 'SRM2V2Storage.__prestageSingleFileStatus: an error occured while checking prestage status.'
        return S_ERROR( errStr )
    except gfal2.GError, e:
      if e.code == errno.EAGAIN:
        self.log.debug( "SRM2V2Storage.__prestageSingleFileStatus: File not staged" )
        return S_OK( False )
      elif e.code == errno.ETIMEDOUT:
        errStr = 'SRM2V2Storage.__prestageSingleFileStatus: Polling request timed out'
        self.log.debug( errStr )
        return S_ERROR( errStr )
      else:
        errStr = "SRM2V2Storage.__prestageSingleFileStatus: Error occured while polling for prestaging file %s. [%d] %s" % ( path, e.code, e.message )
        self.log.error( errStr, e.message )
        return S_ERROR( errStr )



  def pinFile( self, path, lifetime = 86400 ):
    """ Pin a staged file

    :param self: self reference
    :param str path: path of list of paths to be pinned
    :param int lifetime: pinning time in seconds (default 24h)

    :return successful dict {url : token},
            failed dict {url : message}
            S_ERROR in case of argument problems
    """

    res = checkArgumentFormat( path )
    if not res['OK']:
      return res
    urls = res['Value']

    self.log.debug( 'SRM2V2Storage.pinFile: Attempting to pin %s file(s).' % len( urls ) )
    failed = {}
    successful = {}
    for url in urls:
      res = self.__pinSingleFile( url, lifetime )
      if not res['OK']:
        failed[url] = res['Message']
      else:
        successful[url] = res['Value']
    return S_OK( { 'Failed' : failed, 'Successful' : successful } )



  def __pinSingleFile( self, path, lifetime ):
    """ Pin a single staged file

    :param self: self reference
    :param str path: path to be pinned
    :param int lifetime: pinning lifetime in seconds (default 24h)

    :return  S_OK( token ) ) if status >= 0 (0 - staging is pending, 1 - file is pinned). EAGAIN is also considered pending
             S_ERROR( errMsg ) ) in case of an error: status -1
    """

    self.log.debug( "SRM2V2Storage.__pinSingleFile: Attempting to issue pinning request for single file: %s" % path )

    try:
      self.log.debug( "SRM2V2Storage.__pinSingleFile: spaceToken: %s" % self.spaceToken )
      self.gfal2.set_opt_string( "SRM PLUGIN", "SPACETOKENDESC", self.spaceToken )
      self.gfal2.set_opt_string_list( "SRM PLUGIN", "TURL_PROTOCOLS", self.defaultLocalProtocols )
      self.gfal2.set_opt_boolean( "BDII", "ENABLE", True )
      self.gfal2.set_opt_integer( "SRM PLUGIN", "OPERATION_TIMEOUT", self.gfal2Timeout )
      ( status, token ) = self.gfal2.bring_online( path, lifetime, self.stageTimeout, True )
      self.log.debug( "SRM2V2Storage.__pinSingleFile: pinning issued - Status: %s" % status )
      if status >= 0:
        return S_OK( token )
      else:
        errStr = 'SRM2V2Storage.__pinSingleFile: an error occured while issuing pinning.'
        return S_ERROR( errStr )
    except gfal2.GError, e:
      errStr = "SRM2V2Storage.__pinSingleFile: Error occured while pinning file %s. [%d] %s" % ( path, e.code, e.message )
      self.log.error( errStr )
      return S_ERROR( errStr )



  def releaseFile( self, path ):
    """ Release a pinned file

    :param self: self reference
    :param str path: PFN path { pfn : token } - pfn can be an empty string, then all files that have that same token get released.
                     Just as you can pass an empty token string and a directory as pfn which then releases all the files in the directory
                     an its subdirectories

    :return successful dict {url : token},
            failed dict {url : message}
            S_ERROR in case of argument problems
    """
    res = checkArgumentFormat( path )
    if not res['OK']:
      return res
    urls = res['Value']

    self.log.debug( "SRM2V2Storage.releaseFile: Attempting to release %s file(s)." % len( urls ) )

    failed = {}
    successful = {}
    for path, token in urls.items():
      res = self.__releaseSingleFile( path, token )

      if not res['OK']:
        failed[path] = res['Message']
      else:
        successful[path] = res['Value']
    return S_OK( { 'Failed' : failed, 'Successful' : successful } )



  def __releaseSingleFile( self, path, token ):
    """ release a single pinned file

    :param self: self reference
    :param str path: path to the file to be released
    :token str token: token belonging to the path

    :returns S_OK( token ) when releasing was successful, S_ERROR( errMessage ) in case of an error
    """

    self.log.debug( "SRM2V2Storage.__releaseSingleFile: Attempting to release single file: %s" % path )
    if not type( token ) == StringType:
      token = str( token )
    try:
      self.log.debug( "SRM2V2Storage.__releaseSingleFile: spaceToken: %s" % self.spaceToken )
      self.gfal2.set_opt_string( "SRM PLUGIN", "SPACETOKENDESC", self.spaceToken )
      self.gfal2.set_opt_string_list( "SRM PLUGIN", "TURL_PROTOCOLS", self.defaultLocalProtocols )
      self.gfal2.set_opt_boolean( "BDII", "ENABLE", True )
      self.gfal2.set_opt_integer( "SRM PLUGIN", "OPERATION_TIMEOUT", self.gfal2Timeout )
      status = self.gfal2.release( path, token )
      if status >= 0:
        return S_OK( token )
      else:
        errStr = "SRM2V2Storage.__releaseSingleFile: Error occured: Return status < 0"
        return S_ERROR( errStr )
    except gfal2.GError, e:
      errStr = "SRM2V2Storage.__releaseSingleFile: Error occured while releasing file %s. [%d] %s" % ( path, e.code, e.message )
      self.log.error( errStr )
      return S_ERROR( errStr )



  def __getChecksum( self, path, checksumType = 'ADLER32' ):
    """ Calculate the 'ADLER32' checksum of a file on the storage

    :param self: self reference
    :param str path: path to single file on storage (srm://...)
    :returns S_OK( checksum ) if checksum could be calculated
             S_ERROR( errMsg ) if something failed
    """

    self.log.debug( 'SRM2V2Storage.__getChecksum: Trying to calculate checksum of file %s' % path )
    res = self.__isSingleFile( path )

    if not res['OK']:
      errStr = 'SRM2V2Storage.__getChecksum: provided path is not a file'
      self.log.error( errStr, path )
      return S_ERROR( errStr )
    else:
      try:
        self.gfal2.set_opt_boolean( "BDII", "ENABLE", False )
        self.gfal2.set_opt_integer( "SRM PLUGIN", "OPERATION_TIMEOUT", self.gfal2Timeout )
        fileChecksum = self.gfal2.checksum( path, checksumType )
        return S_OK( fileChecksum )

      except gfal2.GError, e:
        errStr = 'SRM2V2Storage.__getChecksum: failed to calculate checksum.'
        self.log.error( errStr, e.message )
        return S_ERROR( e.message )



  def __parseStatInfoFromApiOutput( self, statInfo ):
    """ Fill the metaDict with the information obtained with gfal2.stat()

        returns metaDict with following keys:

        st_dev: ID of device containing file
        st_ino: file serial number
        st_mode: mode of file
        st_nlink: number of links to the file
        st_uid: user ID of file
        st_gid: group ID of file
        st_size: file size in bytes
        st_atime: time of last access
        st_mtime: time of last modification
        st_ctime: time of last status chage
        File (bool): whether object is a file or not
        Directory (bool): whether object is a directory or not
    """
    metaDict = {}
    # to identify whether statInfo are from file or directory
    metaDict['File'] = S_ISREG( statInfo.st_mode )
    metaDict['Directory'] = S_ISDIR( statInfo.st_mode )

    if metaDict['File'] :
      metaDict['FileSerialNumber'] = statInfo.st_ino
      metaDict['Mode'] = statInfo.st_mode & ( S_IRWXU | S_IRWXG | S_IRWXO )
      metaDict['Links'] = statInfo.st_nlink
      metaDict['UserID'] = statInfo.st_uid
      metaDict['GroupID'] = statInfo.st_gid
      metaDict['Size'] = long( statInfo.st_size )
      metaDict['LastAccess'] = self.__convertTime( statInfo.st_atime ) if statInfo.st_atime else 'Never'
      metaDict['ModTime'] = self.__convertTime( statInfo.st_mtime ) if statInfo.st_mtime else 'Never'
      metaDict['StatusChange'] = self.__convertTime( statInfo.st_ctime ) if statInfo.st_ctime else 'Never'
      metaDict['Executable'] = bool( statInfo.st_mode & S_IXUSR )
      metaDict['Readable'] = bool( statInfo.st_mode & S_IRUSR )
      metaDict['Writeable'] = bool( statInfo.st_mode & S_IWUSR )
    elif metaDict['Directory']:
      # metaDict['FileSerialNumber'] = statInfo.st_ino
      metaDict['Mode'] = statInfo.st_mode & ( S_IRWXU | S_IRWXG | S_IRWXO )
      # metaDict['Links'] = statInfo.st_nlink
      # metaDict['UserID'] = statInfo.st_uid
      # metaDict['GroupID'] = statInfo.st_gid
      # metaDict['Size'] = long( statInfo.st_size )
      # metaDict['LastAccess'] = self.__convertTime( statInfo.st_atime ) if statInfo.st_atime else 'Never'
      # metaDict['ModTime'] = self.__convertTime( statInfo.st_mtime ) if statInfo.st_mtime else 'Never'
      # metaDict['StatusChange'] = self.__convertTime( statInfo.st_ctime ) if statInfo.st_ctime else 'Never'
      # metaDict['Executable'] = bool( statInfo.st_mode & S_IXUSR )
      # metaDict['Readable'] = bool( statInfo.st_mode & S_IRUSR )
      # metaDict['Writeable'] = bool( statInfo.st_mode & S_IWUSR )


    return metaDict



  def __convertTime( self, time ):
    """ Converts unix time to proper time format

    :param self: self reference
    :param time: unix time
    :return Date in following format: 2014-10-29 14:32:10
    """
    return datetime.datetime.fromtimestamp( time ).strftime( '%Y-%m-%d %H:%M:%S' )

### methods for manipulating directories ###



  def createDirectory( self, path ):
    """ Create directory on the storage

    :param self: self reference
    :param str path: path to be created on the storage (pfn : srm://...)
    :returns Successful dict {path : True }
             Failed dict     {path : error message }
             S_ERROR in case of argument problems
    """
    urls = checkArgumentFormat( path )
    if not urls['OK']:
      return urls
    urls = urls['Value']

    successful = {}
    failed = {}
    self.log.debug( "createDirectory: Attempting to create %s directories." % len( urls ) )
    for url in urls:
      res = self.__createSingleDirectory( url )
      if res['OK']:
        self.log.debug( "SRM2V2Storage.createDirectory: Successfully created directory on storage %s" % url )
        successful[url] = True
      else:
        self.log.error( "SRM2V2Storage.createDirectory: Failed to create directory on storage.", "%s: %s" % ( url, res['Message'] ) )
        failed[url] = res['Message']
    return S_OK( { 'Failed' : failed, 'Successful' : successful } )



  def __createSingleDirectory( self, path ):
    """ Create directory :path: on the storage
    if no exception is caught the creation was successful. Also if the
    directory already exists we return S_OK().
    :param self: self reference
    :param str path: path to be created (srm://...)

    :returns S_OK() if creation was successful or directory already exists
             S_ERROR() in case of an error during creation
    """

#     # path is a dict {path : False}, and it's only 1 key, value pair in the dict
#     # so the loop just extracts the path string from the dict
#     if path is DictType:
#       for key in path.keys():
#         path = key
#       self.log.debug( "SRM2V2Storage.__createSingleDirectory: Attempting to create directory %s." % path )

    # creating directory with default rights
    try:
      self.gfal2.set_opt_boolean( "BDII", "ENABLE", False )
      self.gfal2.set_opt_integer( "SRM PLUGIN", "OPERATION_TIMEOUT", self.gfal2Timeout )
      status = self.gfal2.mkdir( path, 755 )
      self.log.debug( 'SRM2V2Storage.__createSingleDirectory: Status return of mkdir: %s' % status )
      if status >= 0:
        return S_OK()
      else:
        errStr = 'SRM2V2Storage.__createSingleDirectory: Status return > 0. Error.'
        return S_ERROR( errStr )
    except gfal2.GError, e:
      # error: directory already exists
      if e.code == errno.EEXIST:
        self.log.debug( "SRM2V2Storage.__createSingleDirectory: Directory already exists" )
        return S_OK()
      # any other error: failed to create directory
      else:
        errStr = "SRM2V2Storage.__createSingleDirectory: failed to create directory."
        self.log.error( errStr, e.message )
        return S_ERROR( errStr )



  def isDirectory( self, path ):
    """ check if the path provided is a directory or not

    :param self: self reference
    :param str: path or list of paths to be checked ( 'srm://...')
    :returns dict 'Failed' : failed, 'Successful' : succesful
             S_ERROR in case of argument problems

    """
    res = checkArgumentFormat( path )
    if not res['OK']:
      return res
    urls = res['Value']

    self.log.debug( "SRM2V2Storage.isDirectory: checking whether %s path(s) are directory(ies)." % len( urls ) )

    successful = {}
    failed = {}

    for url in urls:
      res = self.__isSingleDirectory( url )
      if res['OK']:
        successful[url] = res['Value']
      else:
        failed[url] = res['Message']

    resDict = { 'Failed' : failed, 'Successful' : successful }
    return S_OK( resDict )



  def __isSingleDirectory( self, path ):
    """ Checking if :path: exists and is a directory
    :param self: self reference
    :param str path: single path on the storage (srm://...)

    :returns
        S_OK ( boolean) if it is a directory or not
        S_ERROR ( errStr ) when there was a problem getting the info
    """

    self.log.debug( "SRM2V2Storage.__isSingleDirectory: Determining whether %s is a directory or not." % path )
    try:
      self.gfal2.set_opt_boolean( "BDII", "ENABLE", False )
      self.gfal2.set_opt_integer( "SRM PLUGIN", "OPERATION_TIMEOUT", self.gfal2Timeout )
      statInfo = self.gfal2.stat( path )
      if ( S_ISDIR( statInfo.st_mode ) ):  # alternatively drop if/else and just return S_OK ( S_OK ( S_ISDIR( statInfo.st_mode ) ) but that's not really readable and we can't write the log.
        return S_OK ( True )
      else:
        self.log.debug( "SRM2V2Storage.__isSingleDirectory: Path is not a directory" )
        return S_OK( False )
    except gfal2.GError, e:
      if e.code == errno.ENOENT:
        errStr = "SRM2V2Storage.__isSingleDirectory: Directory doesn't exist."
        self.log.error( errStr, e.message )
        return S_ERROR( errStr )
      else:
        errStr = "SRM2V2Storage.__isSingleDirectory: failed to determine if path %s is a directory." % path
        self.log.error( errStr, e.message )
        return S_ERROR( errStr )



  def listDirectory( self, path ):
    """ List the content of the path provided

    :param str path: single or list of paths (srm://...)
    :return failed  dict {path : message }
            successful dict { path :  {'SubDirs' : subDirs, 'Files' : files} }.
            They keys are the paths, the values are the dictionary 'SubDirs' and 'Files'.
            Each are dictionaries with path as key and metadata as values
            S_ERROR in case of argument problems
    """

    res = checkArgumentFormat( path )
    if not res['OK']:
      return res
    urls = res['Value']

    self.log.debug( "SRM2V2Storage.listDirectory: Attempting to list %s directories" % len( urls ) )
    self.log.debug( 'SRM2V2Storage.listDirectory: Directories to list: %s' % urls )
    res = self.isDirectory( urls )
    if not res['OK']:
      return res
    successful = {}
    failed = res['Value']['Failed']

    directories = []

    for url, isDirectory in res['Value']['Successful'].items():
      if isDirectory:
        directories.append( url )
      else:
        errStr = "SRM2V2Storage.listDirectory: path is not a directory"
        gLogger.error( errStr, url )
        failed[url] = errStr

    for directory in directories:
      res = self.__listSingleDirectory( directory )
      if not res['OK']:
        failed[directory] = res['Message']
      else:
        successful[directory] = res['Value']

    resDict = { 'Failed' : failed, 'Successful' : successful }
    return S_OK( resDict )



  def __listSingleDirectory( self, path ):
    """ List the content of the single directory provided
    :param self: self reference
    :param str path: single path on storage (srm://...)
    :returns S_ERROR( errStr ) if there is an error
             S_OK( dictionary ), the dictionary has 2 keys: SubDirs and Files
                                                            The values of the Files are dictionaries with Filename as key and metadata as value
                                                            The values of SubDirs are just the dirnames as key and True as value
    """
    self.log.debug( "SRM2V2Storage.__listSingleDirectory: Attempting to list content of single directory" )

    try:
      self.gfal2.set_opt_boolean( "BDII", "ENABLE", False )
      self.gfal2.set_opt_integer( "SRM PLUGIN", "OPERATION_TIMEOUT", self.gfal2Timeout )
      listing = self.gfal2.listdir( path )
      
      files = {}
      subDirs = {}

      for entry in listing:
        fullPath = '/'.join( [ path, entry ] )
        self.log.debug( 'SRM2V2Storage.__listSingleDirectory: path: %s' % fullPath )
        res = self.__getSingleMetadata( fullPath )
        metadataDict = res['Value']
        if metadataDict['Directory']:
          subDirs[fullPath] = metadataDict
        elif metadataDict['File']:
          files[fullPath] = metadataDict
        else:
          self.log.debug( "SRM2V2Storage.__listSingleDirectory: found item which is neither file nor directory", fullPath )

      return S_OK( {'SubDirs' : subDirs, 'Files' : files} )

    except gfal2.GError, e:
      if e.code == errno.ENOENT:
        errStr = 'SRM2V2Storage.__listSingleDirectory: directory does not exist'
        self.log.error( errStr, e.message )
        return S_ERROR( errStr )
      else:
        errStr = 'SRM2V2Storage.__listSingleDirectory: could not list directory content.'
        self.log.error( errStr, e.message )
        return S_ERROR( errStr )





  def getDirectory( self, path, localPath = False ):
    """ get a directory from the SE to a local path with all its files and subdirectories
    :param str path: path (or list of paths) on the storage (srm://...)
    :param str localPath: local path where the content of the remote directory will be saved, if not defined it takes current working directory.
    :return successful and failed dictionaries. The keys are the paths,
            the values are dictionary {'Files': amount of files downloaded, 'Size' : amount of data downloaded}
            S_ERROR in case of argument problems
    """
    res = checkArgumentFormat( path )
    if not res['OK']:
      return res
    urls = res['Value']

    self.log.debug( "SRM2V2Storage.getDirectory: Attempting to get local copies of %s directories. %s" % ( len( urls ), urls ) )

    failed = {}
    successful = {}

    for src_dir in urls:
      dirName = os.path.basename( src_dir )
      if localPath:
        dest_dir = '%s/%s' % ( localPath, dirName )
      else:
        dest_dir = '%s/%s' % ( os.getcwd(), dirName )

      res = self.__getSingleDirectory( src_dir, dest_dir )

      if res['OK']:
        if res['Value']['AllGot']:
          self.log.debug( "SRM2V2Storage.getDirectory: Successfully got local copy of %s" % src_dir )
          successful[src_dir] = {'Files':res['Value']['Files'], 'Size':res['Value']['Size']}
        else:
          self.log.error( "SRM2V2Storage.getDirectory: Failed to get entire directory.", src_dir )
          failed[src_dir] = {'Files':res['Value']['Files'], 'Size':res['Value']['Size']}
      else:
        self.log.error( "SRM2V2Storage.getDirectory: Completely failed to get local copy of directory.", src_dir )
        failed[src_dir] = {'Files':0, 'Size':0}

    return S_OK( {'Failed' : failed, 'Successful' : successful } )



  def __getSingleDirectory( self, src_dir, dest_dir ):
    """Download a single directory recursively
      :param self: self reference
      :param src_dir : remote directory to download (srm://...)
      :param dest_dir: local destination path
      :returns: S_ERROR if there is a fatal error
              S_OK if we could download something :
                            'AllGot': boolean of whether we could download everything
                            'Files': amount of files received
                            'Size': amount of data received
    """

    self.log.debug( "SRM2V2Storage.__getSingleDirectory: Attempting to download directory %s at %s" % ( src_dir, dest_dir ) )

    filesReceived = 0
    sizeReceived = 0

    res = self.__isSingleDirectory( src_dir )
    if not res['OK']:
      errStr = 'SRM2V2Storage.__getSingleDirectory: Failed to find the source directory'
      self.log.debug( res['Message'], src_dir )
      return S_ERROR( errStr )

    # res['Value'] is False if it's not a directory
    if not res['Value']:
      errStr = 'SRM2V2Storage.__getSingleDirectory: The path provided is not a directory'
      self.log.error( errStr, src_dir )
      return S_ERROR( errStr )
    
    if not os.path.exists( dest_dir ):
      try:
        os.makedirs( dest_dir )
      except OSError, error:
        errStr = 'SRM2V2Storage.__getSingleDirectory: Error trying to create destination directory %s' % error
        self.log.exception( errStr )
        return S_ERROR( errStr )

    # Get the remote directory contents
    res = self.__listSingleDirectory( src_dir )
    if not res['OK']:
      errStr = 'SRM2V2Storage.__getSingleDirectory: Failed to list the source directory.'
      self.log.error( errStr, src_dir )
      return S_ERROR( errStr )

    sFilesDict = res['Value']['Files']
    subDirsDict = res['Value']['SubDirs']

    # Get all the files in the directory
    receivedAllFiles = True
    self.log.debug( 'SRM2V2Storage.__getSingleDirectory: Trying to download the %s files' % len( sFilesDict ) )
    for sFile in sFilesDict:
      # Returns S_OK(fileSize) if successful
      res = self.__getSingleFile( sFile, '/'.join( [dest_dir, os.path.basename( sFile ) ] ) )
      if res['OK']:
        filesReceived += 1
        sizeReceived += res['Value']
      else:
        receivedAllFiles = False

    # recursion to get contents of sub directoryies
    receivedAllDirs = True
    self.log.debug( 'SRM2V2Storage.__getSingleDirectory: Trying to recursively download the %s directories' % len( subDirsDict ) )
    for subDir in subDirsDict:
      subDirName = os.path.basename( subDir )
      localPath = '%s/%s' % ( dest_dir, subDirName )
      res = self.__getSingleDirectory( subDir, localPath )

      if not res['OK']:
        receivedAllDirs = False
      else:
        if not res['Value']['AllGot']:
          receivedAllDirs = False
        filesReceived += res['Value']['Files']
        sizeReceived += res['Value']['Size']


    if receivedAllDirs and receivedAllFiles:
      allGot = True
    else:
      allGot = False

    resDict = { 'AllGot' : allGot, 'Files' : filesReceived, 'Size' : sizeReceived }
    return S_OK( resDict )



  def putDirectory( self, path ):
    """ Puts one or more local directories to the physical storage together with all its files
    :param self: self reference
    :param str path: dictionary { srm://... (destination) : localdir (source dir) }
    :return successful and failed dictionaries. The keys are the paths,
            the values are dictionary {'Files' : amount of files uploaded, 'Size' : amount of data upload }
            S_ERROR in case of argument problems
    """

    res = checkArgumentFormat( path )
    if not res['OK']:
      return res
    urls = res['Value']

    self.log.debug( 'SRM2V2Storage.putDirectory: Attempting to put %s directories to remote storage' % len( urls ) )

    successful = {}
    failed = {}

    for destDir, sourceDir in urls.items():
      if not sourceDir:
        self.log.debug( 'SourceDir: %s' % sourceDir )
        errStr = 'SRM2V2Storage.putDirectory: No source directory set, make sure the input format is correct { dest. dir : source dir }'
        return S_ERROR( errStr )
      res = self.__putSingleDirectory( sourceDir, destDir )
      if res['OK']:
        if res['Value']['AllPut']:
          self.log.debug( "SRM2V2Storage.putDirectory: Successfully put directory to remote storage: %s" % destDir )
          successful[destDir] = { 'Files' : res['Value']['Files'], 'Size' : res['Value']['Size']}
        else:
          self.log.error( "SRM2V2Storage.putDirectory: Failed to put entire directory to remote storage.", destDir )
          failed[destDir] = { 'Files' : res['Value']['Files'], 'Size' : res['Value']['Size']}
      else:
        self.log.error( "SRM2V2Storage.putDirectory: Completely failed to put directory to remote storage.", destDir )
        failed[destDir] = { "Files" : 0, "Size" : 0 }
    return S_OK( { "Failed" : failed, "Successful" : successful } )



  def __putSingleDirectory( self, src_directory, dest_directory ):
    """ puts one local directory to the physical storage together with all its files and subdirectories
        :param self: self reference
        :param src_directory : the local directory to copy
        :param dest_directory: pfn (srm://...) where to copy
        :returns: S_ERROR if there is a fatal error
                  S_OK if we could upload something :
                                    'AllPut': boolean of whether we could upload everything
                                    'Files': amount of files uploaded
                                    'Size': amount of data uploaded
    """
    self.log.debug( 'SRM2V2Storage.__putSingleDirectory: trying to upload %s to %s' % ( src_directory, dest_directory ) )

    filesPut = 0
    sizePut = 0
    
    if not os.path.isdir( src_directory ):
      errStr = 'SRM2V2Storage.__putSingleDirectory: The supplied source directory does not exist or is not a directory.'
      self.log.error( errStr, src_directory )
      return S_ERROR( errStr )

    contents = os.listdir( src_directory )
    allSuccessful = True
    directoryFiles = {}
    for fileName in contents:
      localPath = '%s/%s' % ( src_directory, fileName )
      remotePath = '%s/%s' % ( dest_directory, fileName )
      # if localPath is not a directory put it to the files dict that needs to be uploaded
      if not os.path.isdir( localPath ):
        directoryFiles[remotePath] = localPath
      # localPath is another folder, start recursion
      else:
        res = self.__putSingleDirectory( localPath, remotePath )
        if not res['OK']:
          errStr = 'SRM2V2Storage.__putSingleDirectory: Failed to put directory to storage.'
          self.log.error( errStr, res['Message'] )
        else:
          if not res['Value']['AllPut']:
            allSuccessful = False
          filesPut += res['Value']['Files']
          sizePut += res['Value']['Size']

    if directoryFiles:
      res = self.putFile( directoryFiles )
      if not res['OK']:
        self.log.error( 'SRM2V2Storage.__putSingleDirectory: Failed to put files to storage.', res['Message'] )
        allSuccessful = False
      else:
        for fileSize in res['Value']['Successful'].itervalues():
          filesPut += 1
          sizePut += fileSize
        if res['Value']['Failed']:
          allSuccessful = False
    return S_OK( { 'AllPut' : allSuccessful, 'Files' : filesPut, 'Size' : sizePut} )



  def removeDirectory( self, path, recursive = False ):
    """Remove a directory on the physical storage together with all its files and
       subdirectories.
       :param path : single or list of path (srm://..)
       :param recursive : if True, we recursively delete the subdir
       :return: successful and failed dictionaries. The keys are the pathes,
             the values are dictionary {'Files': amount of files deleted, 'Size': amount of data deleted}
                S_ERROR in case of argument problems
    """
    res = checkArgumentFormat( path )
    if not res['OK']:
      return res
    urls = res['Value']

    self.log.debug( "SRM2V2Storage.removeDirectory: Attempting to remove %s directories." % len( urls ) )

    successful = {}
    failed = {}

    for url in urls:
      res = self.__removeSingleDirectory( url, recursive )

      if res['OK']:
        if res['Value']['AllRemoved']:
          self.log.debug( "SRM2V2Storage.removeDirectory: Successfully removed %s" % url )
          successful[url] = {'FilesRemoved':res['Value']['FilesRemoved'], 'SizeRemoved':res['Value']['SizeRemoved']}
        else:
          self.log.error( "SRM2V2Storage.removeDirectory: Failed to remove entire directory.", path )
          failed[url] = {'FilesRemoved':res['Value']['FilesRemoved'], 'SizeRemoved':res['Value']['SizeRemoved']}
      else:
        self.log.error( "SRM2V2Storage.removeDirectory: Completely failed to remove directory.", url )
        failed[url] = res['Message']  # {'FilesRemoved':0, 'SizeRemoved':0}

    return S_OK( {'Failed' : failed, 'Successful' : successful } )



  def __removeSingleDirectory( self, path, recursive = False ):
    """Remove a directory on the physical storage together with all its files and
       subdirectories.
       :param path: pfn (srm://...) of a directory to remove
       :param recursive : if True, we recursively delete the subdir
       :returns: S_ERROR if there is a fatal error
                  S_OK (statistics dictionary ) if we could upload something :
                                    'AllRemoved': boolean of whether we could delete everything
                                    'FilesRemoved': amount of files deleted
                                    'SizeRemoved': amount of data deleted
    """
    filesRemoved = 0
    sizeRemoved = 0

    # Check the remote directory exists

    res = self.__isSingleDirectory( path )

    if not res['OK']:
      errStr = "SRM2V2Storage.__removeSingleDirectory: %s" % res['Message']
      self.log.error( errStr, path )
      return S_ERROR( errStr )

    # res['Value'] is True if it is a directory
    if not res['Value']:
      errStr = "SRM2V2Storage.__removeSingleDirectory: The supplied path is not a directory."
      self.log.error( errStr, path )
      return S_ERROR( errStr )

    # Get the remote directory contents
    res = self.__listSingleDirectory( path )
    if not res['OK']:
      errStr = "SRM2V2Storage.__removeSingleDirectory: Failed to list the directory."
      self.log.error( errStr, path )
      return S_ERROR( errStr )

    sFilesDict = res['Value']['Files']
    subDirsDict = res['Value']['SubDirs']

    removedAllFiles = True
    removedAllDirs = True
    allRemoved = True

    # if recursive, we call ourselves on all the subdirs
    if recursive:
      # Recursively remove the sub directories
      self.log.debug( "SRM2V2Storage.__removeSingleDirectory: Trying to recursively remove %s folder." % len( subDirsDict ) )
      for subDir in subDirsDict:
        subDirName = os.path.basename( subDir )
        localPath = '%s/%s' % ( path, subDirName )
        res = self.__removeSingleDirectory( localPath, recursive )

        if not res['OK']:
          removedAllDirs = False
        if res['OK']:
          if not res['Value']['AllRemoved']:
            removedAllDirs = False
          filesRemoved += res['Value']['FilesRemoved']
          sizeRemoved += res['Value']['SizeRemoved']

    # Remove all the files in the directory
    self.log.debug( "SRM2V2Storage.__removeSingleDirectory: Trying to remove %s files." % len( sFilesDict ) )
    for sFile in sFilesDict:
      # Returns S__OK(Filesize) if it worked
      res = self.__removeSingleFile( sFile )

      if res['OK']:
        filesRemoved += 1
        sizeRemoved += sFilesDict[sFile]['Size']
      else:
        removedAllFiles = False

    # Check whether all the operations were successful
    if removedAllDirs and removedAllFiles:
      allRemoved = True
    else:
      allRemoved = False


    # Now we try to remove the directory itself
    # We do it only if :
    # If we wanted to remove recursively and everything was deleted
    # We didn't want to remove recursively but we deleted all the files and there are no subfolders

    if ( recursive and allRemoved ) or ( not recursive and removedAllFiles and ( len( subDirsDict ) == 0 ) ):
      try:
        self.gfal2.set_opt_boolean( "BDII", "ENABLE", False )
        self.gfal2.set_opt_integer( "SRM PLUGIN", "OPERATION_TIMEOUT", self.gfal2Timeout )
        self.gfal2.set_opt_string( "SRM PLUGIN", "SPACETOKENDESC", self.spaceToken )
        status = self.gfal2.rmdir( path )
        if status < 0:
          errStr = "SRM2V2Storage.__removeSingleDirectory: Error occured while removing directory. Status: %s" % status
          self.log.debug( errStr )
          allRemoved = False
      except gfal2.GError, e:
        if e.code == errno.ENOENT:
          errStr = 'SRM2V2Storage.__removeSingleDirectory: Files does not exist'
          self.log.debug( errStr )
        else:
          errStr = 'SRM2V2Storage.__removeSingleDirectory: Failed to remove directory %s' % path
          self.log.debug( errStr )
          allRemoved = False

    resDict = {'AllRemoved': allRemoved, 'FilesRemoved': filesRemoved, 'SizeRemoved': sizeRemoved}
    return S_OK( resDict )



  def getDirectorySize( self, path ):
    """ Get the size of the directory on the storage
      CAUTION: it is not recursive
      :param self: self reference
      :param str path: path or list of paths on storage (srm://...)
      :returns list of successful and failed dictionaries, both indexed by the path
               In the failed, the value is the error message
               In the successful the values are dictionaries: Files : amount of files in the dir
                                                              Size : summed up size of all files
                                                              subDirs : amount of sub dirs
              S_ERROR in case of argument problems
    """
    res = checkArgumentFormat( path )
    if not res['OK']:
      return res
    urls = res['Value']

    self.log.debug( 'SRM2V2Storage.getDirectorySize: Attempting to get size of %s directories' % len( urls ) )

    failed = {}
    successful = {}

    for url in urls:
      res = self.__getSingleDirectorySize( url )

      if not res['OK']:
        failed[url] = res['Message']
      else:
        successful[url] = res['Value']

    return S_OK( { 'Failed' : failed, 'Successful' : successful} )



  def __getSingleDirectorySize( self, path ):
    """ Get the size of the directory on the storage
      CAUTION : the size is not recursive, and does not go into subfolders
      :param self: self reference
      :param path: path (single) on storage (srm://...)
      :return: S_ERROR in case of problem
                S_OK (Dictionary) Files : amount of files in the directory
                                  Size : summed up size of files
                                  subDirs : amount of sub directories
    """

    self.log.debug( "SRM2V2Storage.__getSingleDirectorySize: Attempting to get the size of directory %s" % path )

    res = self.__listSingleDirectory( path )
    if not res['OK']:
      return res

    directorySize = 0
    directoryFiles = 0
    # itervalues returns a list of values of the dictionnary
    for fileDict in res['Value']['Files'].itervalues():
      directorySize += fileDict['Size']
      directoryFiles += 1

    self.log.debug( "SRM2V2Storage.__getSingleDirectorySize: Successfully obtained size of %s." % path )
    subDirectories = len( res['Value']['SubDirs'] )
    return S_OK( { 'Files' : directoryFiles, 'Size' : directorySize, 'SubDirs' : subDirectories } )



  def getDirectoryMetadata( self, path ):
    """ Get metadata for the directory(ies) provided

    :param self: self reference
    :param str path: path (or list of paths) on storage (srm://...)
    :returns Successful dict {path : metadata}
             Failed dict {path : errStr}
             S_ERROR in case of argument problems
    """
    res = checkArgumentFormat( path )
    if not res['OK']:
      return res
    urls = res['Value']

    self.log.debug( "SRM2V2Storage.getDirectoryMetadata: Attempting to fetch metadata." )

    failed = {}
    successful = {}

    for url in urls:
      res = self.__getSingleDirectoryMetadata( url )

      if not res['OK']:
        failed[url] = res['Message']
      else:
        successful[url] = res['Value']

    return S_OK( { 'Failed' : failed, 'Successful' : successful} )



  def __getSingleDirectoryMetadata( self, path ):
    """ Fetch the metadata of the provided path
    :param self: self reference
    :param str path: path (only 1) on the storage (srm://...)
    :returns
      S_OK( metadataDict ) if we could get the metadata
      S_ERROR( errStr )if there was a problem getting the metadata or path isn't a directory
    """
    self.log.debug( "SRM2V2Storage.__getSingleDirectoryMetadata: Fetching metadata of directory %s." % path )

    res = self.__getSingleMetadata( path )

    if not res['OK']:
      return res

    metadataDict = res['Value']

    if not metadataDict['Directory']:
      errStr = "SRM2V2Storage.__getSingleDirectoryMetadata: Provided path is not a directory."
      self.log.error( errStr, path )
      return S_ERROR( errStr )

    return S_OK( metadataDict )

### methods for manipulating the client ###



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


##################################################################
#
#    ALL INHERITED FROM StorageBase.py
#
##################################################################
#   def isOK( self ):
#     return self.isok
#
#   def changeDirectory( self, newdir ):
#     """ Change the current directory
#     """
#     self.cwd = newdir
#     return S_OK()
#
#   def getCurrentDirectory( self ):
#     """ Get the current directory
#     """
#     return S_OK( self.cwd )
#
#   def getName( self ):
#     """ The name with which the storage was instantiated
#     """
#     return S_OK( self.name )
#
#   def setParameters( self, parameters ):
#     """ Set extra storage parameters, non-mandatory method
#     """
#     return S_OK()
##################################################################



  def getTransportURL( self, path, protocols = False ):
    """ obtain the tURLs for the supplied path and protocols

    :param self: self reference
    :param str path: path on storage
    :param mixed protocols: protocols to use
    :returns Failed dict {path : error message}
             Successful dict {path : transport url}
             S_ERROR in case of argument problems
    """
    res = checkArgumentFormat( path )
    if not res['OK']:
      return res
    urls = res['Value']

    self.log.debug( 'SRM2V2Storage.getTransportURL: Attempting to retrieve tURL for %s paths' % len( urls ) )

    failed = {}
    successful = {}

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

    for url in urls:
      res = self.__getSingleTransportURL( url, listProtocols )
      self.log.debug('res = %s' % res)

      if not res['OK']:
        failed[url] = res['Message']
      else:
        successful[url] = res['Value']

    return S_OK( { 'Failed' : failed, 'Successful' : successful } )



  def __getSingleTransportURL( self, path, protocols = False ):
    """ Get the tURL from path with getxattr from gfal2

    :param self: self reference
    :param str path: path on the storage
    :returns S_OK( Transport_URL ) in case of success
             S_ERROR( errStr ) in case of a failure
    """
    self.log.debug( 'SRM2V2Storage.__getSingleTransportURL: trying to retrieve tURL for %s' % path )
    if protocols:
      res = self.__getExtendedAttributes( path, protocols )
    else:
      res = self.__getExtendedAttributes( path )
    if res['OK']:
      attributeDict = res['Value']
      # 'user.replicas' is the extended attribute we are interested in
      if 'user.replicas' in attributeDict.keys():
        turl = attributeDict['user.replicas']
        return S_OK( turl )
      else:
        errStr = 'SRM2V2Storage.__getSingleTransportURL: Extended attribute tURL is not set.'
        self.log.debug( errStr )
        return S_ERROR( errStr )
    else:
      errStr = 'SRM2V2Storage.__getSingleTransportURL: %s' % res['Message']
      return S_ERROR( errStr )



  def __getProtocols( self ):
    """ returns list of protocols to use at a given site

    :warn: priority is given to a protocols list defined in the CS

    :param self: self reference
    """
    sections = gConfig.getSections( '/Resources/StorageElements/%s/' % ( self.name ) )
    self.log.debug( "SRM2V2Storage.__getProtocols: Trying to get protocols for storage %s." % self.name )
    if not sections['OK']:
      return sections

    protocolsList = []
    for section in sections['Value']:
      path = '/Resources/StorageElements/%s/%s/ProtocolName' % ( self.name, section )
      if gConfig.getValue( path, '' ) == self.protocolName:
        protPath = '/Resources/StorageElements/%s/%s/ProtocolsList' % ( self.name, section )
        siteProtocols = gConfig.getValue( protPath, [] )
        if siteProtocols:
          self.log.debug( 'SRM2V2Storage.__getProtocols: Found SE protocols list to override defaults:', ', '.join( siteProtocols, ) )
          protocolsList = siteProtocols

    if not protocolsList:
      self.log.debug( "SRM2V2Storage.__getProtocols: No protocols provided, using the default protocols." )
      protocolsList = gConfig.getValue( '/Resources/StorageElements/DefaultProtocols', [] )
      
    # if there is even no default protocol
    if not protocolsList:
      return S_ERROR( "SRM2V2Storage.__getProtocols: No local protocols defined and no defaults found." )

    return S_OK( protocolsList )



  def __getExtendedAttributes( self, path, protocols = False ):
    """ Get all the available extended attributes of path

    :param self: self reference
    :param str path: path of which we wan't extended attributes
    :return S_OK( attributeDict ) if successful. Where the keys of the dict are the attributes and values the respective values
    """
    attributeDict = {}

    # get all the extended attributes from path
    try:
      self.gfal2.set_opt_boolean( "BDII", "ENABLE", False )
      self.gfal2.set_opt_integer( "SRM PLUGIN", "OPERATION_TIMEOUT", self.gfal2Timeout )
      self.gfal2.set_opt_string( "SRM PLUGIN", "SPACETOKENDESC", self.spaceToken )
      if protocols:
        self.gfal2.set_opt_string_list( "SRM PLUGIN", "TURL_PROTOCOLS", protocols )
      else:
        self.gfal2.set_opt_string_list( "SRM PLUGIN", "TURL_PROTOCOLS", self.defaultLocalProtocols )
      attributes = self.gfal2.listxattr( path )
  
      # get all the respective values of the extended attributes of path
      for attribute in attributes:
        attributeDict[attribute] = self.gfal2.getxattr( path, attribute )
  
      return S_OK( attributeDict )
    # simple error messages, the method that is calling them adds the source of error.
    except gfal2.GError, e:
      if e.code == errno.ENOENT:
        errStr = 'Path does not exist.'
        self.log.error( errStr, e.message )
        return S_ERROR( errStr )
      else:
        errStr = 'Something went wrong while checking for extended attributes. Please see error log for more information.'
        self.log.error( errStr, e.message )
        return S_ERROR( errStr )


