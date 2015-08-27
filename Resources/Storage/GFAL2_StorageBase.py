""" :mod: GFAL2_StorageBase
    =================

    .. module: python
    :synopsis: GFAL2 class from StorageElement using gfal2. Other modules can inherit from this use the gfal2 methods.
"""
# # imports
import os
import datetime
import errno
import gfal2
from types import StringType
from stat import S_ISREG, S_ISDIR, S_IXUSR, S_IRUSR, S_IWUSR, \
  S_IRWXG, S_IRWXU, S_IRWXO
# # from DIRAC
from DIRAC import gLogger, gConfig, S_OK, S_ERROR
from DIRAC.Resources.Utilities import checkArgumentFormat
from DIRAC.Resources.Storage.StorageBase import StorageBase
from DIRAC.Core.Security.ProxyInfo import getProxyInfo
from DIRAC.ConfigurationSystem.Client.Helpers.Registry import getVOForGroup
from DIRAC.Core.Utilities.File import getSize


# # RCSID
__RCSID__ = "e6bba13 (2015-07-22 15:24:34 +0200) Andrei Tsaregorodtsev <atsareg@diracgrid.org>"

class GFAL2_StorageBase( StorageBase ):
  """ .. class:: GFAL2_StorageBase

  SRM v2 interface to StorageElement using gfal2
  """

  def __init__( self, storageName, parameters ):
    """ c'tor

    :param self: self reference
    :param str storageName: SE name
    :param dict parameters: storage parameters
    """

    StorageBase.__init__( self, storageName, parameters )

    self.log = gLogger.getSubLogger( "GFAL2_StorageBase", True )

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

    # by default turn off BDII checks
    self.gfal2.set_opt_boolean( "BDII", "ENABLE", False )
    # spaceToken used for copying from and to the storage element
    self.spaceToken = parameters['SpaceToken']
    # stageTimeout, default timeout to try and stage/pin a file
    self.stageTimeout = gConfig.getValue( '/Resources/StorageElements/StageTimeout', 12 * 60 * 60 )
    # gfal2Timeout, amount of time it takes until an operation times out
    self.gfal2Timeout = gConfig.getValue( "/Resources/StorageElements/GFAL_Timeout", 100 )
    # set the gfal2 default protocols, e.g. used when trying to retrieve transport url
    self.defaultLocalProtocols = gConfig.getValue( '/Resources/StorageElements/DefaultProtocols', [] )

    # # set checksum type, by default this is 0 (GFAL_CKSM_NONE)
    self.checksumType = gConfig.getValue( "/Resources/StorageElements/ChecksumType", '0' )

    if self.checksumType == '0':
      self.checksumType = None

    self.log.debug( 'GFAL2_StorageBase: using %s checksum' % self.checksumType )

    self.voName = None
    ret = getProxyInfo( disableVOMS = True )
    if ret['OK'] and 'group' in ret['Value']:
      self.voName = getVOForGroup( ret['Value']['group'] )


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

    self.log.debug( "GFAL2_StorageBase.exists: Checking the existence of %s path(s)" % len( urls ) )

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
    self.log.debug( "GFAL2_StorageBase._singleExists: Determining whether %s exists or not" % path )

    try:
      self.gfal2.stat( path )  # If path doesn't exist this will raise an error - otherwise path exists
      self.log.debug( "GFAL2_StorageBase.__singleExists: path exists" )
      return S_OK( True )
    except gfal2.GError, e:
      if e.code == errno.ENOENT:
        errStr = "GFAL2_StorageBase.__singleExists: Path does not exist"
        self.log.debug( errStr )
        return  S_OK( False )
      if e.code == errno.EPROTONOSUPPORT:
        errStr = "GFAL2_StorageBase.__singleExists: Protocol not supported"
        self.log.debug( errStr )
        return S_ERROR( errStr )
      else:
        errStr = "GFAL2_StorageBase.__singleExists: Failed to determine existence of path"
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

    self.log.debug( "GFAL2_StorageBase.isFile: checking whether %s path(s) are file(s)." % len( urls ) )

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

    self.log.debug( "GFAL2_StorageBase.__isSingleFile: Determining whether %s is a file or not." % path )

    try:
      statInfo = self.gfal2.stat( path )
      # instead of return S_OK( S_ISDIR( statInfo.st_mode ) ) we use if/else. So we can use the log.
      if S_ISREG( statInfo.st_mode ):
        return S_OK( True )
      else:
        self.log.debug( "GFAL2_StorageBase.__isSingleFile: Path is not a file" )
        return S_OK( False )
    except gfal2.GError, e:
      if e.code == errno.ENOENT:
        errStr = "GFAL2_StorageBase.__isSingleFile: File does not exist."
        self.log.debug( errStr, e.message )
        return S_ERROR( errStr )
      else:
        errStr = "GFAL2_StorageBase.__isSingleFile: failed to determine if path %s is a file." % path
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
        errStr = "GFAL2_StorageBase.putFile: Source file not set. Argument must be a dictionary \
                                             (or a list of a dictionary) {url : local path}"
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
    self.log.debug( "GFAL2_StorageBase.__putSingleFile: trying to upload %s to %s" % ( src_file, dest_url ) )

    # in case src_file is not set (this is also done in putFile but some methods directly access this method,
    # so that's why it's checked here one more time
    if not src_file:
      errStr = 'GFAL2_StorageBase.__putSingleFile: no source defined, please check argument format { destination : localfile }'
      return S_ERROR( errStr )
    else:
      # check whether the source is local or on another storage

      # TODO: as soon as self.protocolParameters contains all known protocols to the SE
      # we wont need this hard coded list below anymore but can implement a function in StorageBase
      # similar to isNativeURL which returns true if the src_file contains a known protocol.
      protocols = ['srm', 'root']
      if any( src_file.startswith( protocol + ':' ) for protocol in protocols ):
        src_url = src_file
        if not sourceSize:
          errStr = "GFAL2_StorageBase.__putFile: For file replication the source file size in bytes must be provided."
          self.log.error( errStr, src_file )
          return S_ERROR( errStr )

    # file is local so we can set the protocol and determine source size accordingly
      else:
        if not os.path.exists( src_file ) or not os.path.isfile( src_file ):
          errStr = "GFAL2_StorageBase.__putFile: The local source file does not exist or is a directory"
          self.log.error( errStr, src_file )
          return S_ERROR( errStr )
        src_url = 'file:%s' % os.path.abspath( src_file )
        sourceSize = getSize( src_file )
        if sourceSize == -1:
          errStr = "GFAL2_StorageBase.__putFile: Failed to get file size"
          self.log.error( errStr, src_file )
          return S_ERROR( errStr )
        if sourceSize == 0:
          errStr = "GFAL2_StorageBase.__putFile: Source file size is zero."
          self.log.error( errStr, src_file )
          return S_ERROR( errStr )


    # source is OK, creating the destination folder
    dest_path = os.path.dirname( dest_url )
    res = self.__createSingleDirectory( dest_path )
    if not res['OK']:
      errStr = "GFAL2_StorageBase.__putSingleFile: Failed to create destination folder %s" % dest_path
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

  #  params.checksum_check = True if self.checksumType else False

    # Params set, copying file now
    try:
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
        self.log.debug( 'GFAL2_StorageBase.__putSingleFile: destSize: %s, sourceSize: %s' % ( destSize, sourceSize ) )
        if destSize == sourceSize:
          return S_OK( destSize )
        else:
          self.log.debug( "GFAL2_StorageBase.__putSingleFile: Source and destination file size don't match. \
                                                                        Trying to remove destination file" )
          res = self.__removeSingleFile( dest_url )
          if not res['OK']:
            errStr = 'GFAL2_StorageBase.__putSingleFile: Failed to remove destination file: %s' % res['Message']
            return S_ERROR( errStr )
          errStr = "GFAL2_StorageBase.__putSingleFile: Source and destination file size don't match. Removed destination file"
          self.log.error( errStr, {sourceSize : destSize} )
          return S_ERROR( errStr )
    except gfal2.GError, e:
      # ##
      # extended error message because otherwise we could only guess what the error could be when we copy
      # from another srm to our srm-SE '''
      errStr = "GFAL2_StorageBase.__putSingleFile: Failed to copy file %s to destination url %s: [%d] %s" \
                                                                % ( src_file, dest_url, e.code, e.message )
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

    self.log.debug( "GFAL2_StorageBase.getFile: Trying to download %s files." % len( urls ) )

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
    self.log.info( "GFAL2_StorageBase.__getSingleFile: Trying to download %s to %s" % ( src_url, dest_file ) )

    if not os.path.exists( os.path.dirname( dest_file ) ):
      self.log.debug( "GFAL2_StorageBase.__getSingleFile: Local directory does not yet exist. Creating it", os.path.dirname( dest_file ) )
      try:
        os.makedirs( os.path.dirname( dest_file ) )
      except OSError, error:
        errStr = "GFAL2_StorageBase.__getSingleFile: Error while creating the destination folder"
        self.log.exception( errStr, error )
        return S_ERROR( errStr )

    res = self.__getSingleFileSize( src_url )


    if not res['OK']:
      errStr = "GFAL2_StorageBase.__getSingleFile: Error while determining file size: %s" % res['Message']
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
      dest = 'file:' + os.path.abspath( dest_file )
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
          errStr = "GFAL2_StorageBase.__getSingleFile: File sizes don't match. Something went wrong. Removing local file %s" % dest_file
          self.log.error( errStr, {remoteSize : destSize} )
          if os.path.exists( dest_file ):
            os.remove( dest_file )
          return S_ERROR( errStr )
    except gfal2.GError, e:
      errStr = 'GFAL2_StorageBase.__getSingleFile: Could not copy %s to %s, [%d] %s' % ( src_url, dest, e.code, e.message )
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

    self.log.debug( "GFAL2_StorageBase.removeFile: Attemping to remove %s files" % len( urls ) )

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
    self.log.debug( "GFAL2_StorageBase.__removeSingleFile: Attemping to remove single file %s" % path )

    try:
      status = self.gfal2.unlink( path )
      if status == 0:
        self.log.debug( "GFAL2_StorageBase.__removeSingleFile: File successfully removed" )
        return S_OK( True )
      elif status < 0:
        errStr = 'GFAL2_StorageBase.__removeSingleFile: return status < 0. Error occured.'
        return S_ERROR( errStr )
    except gfal2.GError, e:
      # file doesn't exist so operation was successful
      if e.code == errno.ENOENT:
        errStr = "GFAL2_StorageBase.__removeSingleFile: File does not exist."
        self.log.debug( "GFAL2_StorageBase.__removeSingleFile: Error while removing file." )
        return S_OK( True )
      elif e.code == errno.EISDIR:
        errStr = "GFAL2_StorageBase.__removeSingleFile: path is a directory."
        self.log.debug( "GFAL2_StorageBase.__removeSingleFile: Path is a directory." )
        return S_ERROR( errStr )
      else:
        errStr = "GFAL2_StorageBase.__removeSingleFile: Failed to remove file."
        self.log.debug( "GFAL2_StorageBase.__removeSingleFile: Failed to remove file: [%d] %s" % ( e.code, e.message ) )
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

    self.log.debug( "GFAL2_StorageBase.getFileSize: Trying to determine file size of %s files" % len( urls ) )

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
    self.log.debug( "GFAL2_StorageBase.__getSingleFileSize: Determining file size of %s" % path )

    res = self.__isSingleFile( path )
    if not res['OK']:
      return res

    if not res['Value']:
      errStr = 'GFAL2_StorageBase.__getSingleFileSize: path is not a file'
      self.log.debug( 'GFAL2_StorageBase.__getSingleFileSize: path is not a file' )
      return S_ERROR( errStr )
    else:  # if this is true, path is a file
      try:
        statInfo = self.gfal2.stat( path )  # keeps info like size, mode.
        self.log.debug( "GFAL2_StorageBase.__singleExists: File size successfully determined" )
        return S_OK( long ( statInfo.st_size ) )
      except gfal2.GError, e:
        errStr = "GFAL2_StorageBase.__singleExists: Failed to determine file size."
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

    self.log.debug( 'GFAL2_StorageBase.getFileMetadata: trying to read metadata for %s paths' % len( urls ) )

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
    self.log.debug( 'GFAL2_StorageBase.__getSingleFileMetadata: trying to read metadata for %s' % path )

    res = self.__getSingleMetadata( path )

    if not res['OK']:
      return res  # res is S_ERROR ( errStr )

    metaDict = res['Value']
    # Add metadata expected in some places if not provided by itself
    metaDict['Lost'] = metaDict.get( 'Lost', 0 )
    metaDict['Cached'] = metaDict.get( 'Cached', 1 )
    metaDict['Unavailable'] = metaDict.get('Unavailable', 0)

    if not metaDict['File']:
      errStr = "GFAL2_StorageBase.__getSingleFileMetadata: supplied path is not a file"
      self.log.error( errStr, path )
      return S_ERROR( errStr )

    return S_OK( metaDict )



  def __getSingleMetadata( self, path ):
    """  Fetches the metadata of a single file or directory via gfal2.stat
         and getExtendedAttributes

      :param self: self reference
      :param path: path (only 1) on storage (srm://...)
      :returns:
          S_OK ( MetadataDict ) if we could get the metadata
          S_ERROR ( errorMsg ) if there was a problem getting the metadata
    """
    self.log.debug( 'GFAL2_StorageBase.__getSingleMetadata: reading metadata for %s' % path )

    try:
      statInfo = self.gfal2.stat( path )

    except gfal2.GError, e:
      if e.code == errno.ENOENT:
        errStr = "GFAL2_StorageBase.__getSingleMetadata: Path does not exist"
        self.log.error( errStr, e.message )
        return S_ERROR ( errStr )
      else:
        errStr = "GFAL2_StorageBase.__getSingleMetadata: Failed to retrieve metadata from path."
        self.log.error( errStr, e.message )
        return S_ERROR( errStr )

    metadataDict = self.__parseStatInfoFromApiOutput( statInfo )
    res = self._getExtendedAttributes( path )
    # add extended attributes to the dict if available
    if res['OK']:
      attributeDict = res['Value']
    else:
      # no extendted attributes could be retrieved. Ignore it
      attributeDict = {}

    if metadataDict['File']:
      if self.checksumType:
        res = self.__getChecksum( path, self.checksumType )
      if res['OK']:
        metadataDict['Checksum'] = res['Value']
      else:
        metadataDict['Checksum'] = ""

      # 'user.status' is the extended attribute we are interested in
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
	if attributeDict['user.status'] == 'ONLINE_AND_NEARLINE':
	  metadataDict['Cached'] = 1
          metadataDict['Migrated'] = 1

    return S_OK ( metadataDict )

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

    self.log.debug( 'GFAL2_StorageBase.prestageFile: Attempting to issue stage requests for %s file(s).' % len( urls ) )

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
    self.log.debug( "GFAL2_StorageBase.__prestageSingleFile: Attempting to issue stage request for single file: %s" % path )

    try:
      ( status, token ) = self.gfal2.bring_online( path, lifetime, self.stageTimeout, True )
      self.log.debug( "GFAL2_StorageBase.__prestageSingleFile: Staging issued - Status: %s" % status )
      if status >= 0:
        return S_OK( token )
      else:
        errStr = 'GFAL2_StorageBase.__prestageSingleFile: an error occured while issuing prestaging.'
        return S_ERROR( errStr )
    except gfal2.GError, e:
      errStr = "GFAL2_StorageBase.__prestageSingleFile: Error occured while prestaging file %s. [%d] %s" % ( path, e.code, e.message )
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

    self.log.debug( 'GFAL2_StorageBase.prestageFileStatus: Checking the staging status for %s file(s).' % len( urls ) )

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

    self.log.debug( "GFAL2_StorageBase.__prestageSingleFileStatus: Checking prestage file status for %s" % path )
    # also allow int as token - converting them to strings
    if not type( token ) == StringType:
      token = str( token )
    try:
      self.gfal2.set_opt_boolean( "BDII", "ENABLE", True )
      status = self.gfal2.bring_online_poll( path, token )
      if status == 0:
        self.log.debug( "GFAL2_StorageBase.__prestageSingleFileStatus: File not staged" )
        return S_OK( False )
      elif status == 1:
        self.log.debug( "GFAL2_StorageBase.__prestageSingleFileStatus: File is staged" )
        return S_OK( True )
      else:
        errStr = 'GFAL2_StorageBase.__prestageSingleFileStatus: an error occured while checking prestage status.'
        return S_ERROR( errStr )
    except gfal2.GError, e:
      if e.code == errno.EAGAIN:
        self.log.debug( "GFAL2_StorageBase.__prestageSingleFileStatus: File not staged" )
        return S_OK( False )
      elif e.code == errno.ETIMEDOUT:
        errStr = 'GFAL2_StorageBase.__prestageSingleFileStatus: Polling request timed out'
        self.log.debug( errStr )
        return S_ERROR( errStr )
      else:
        errStr = "GFAL2_StorageBase.__prestageSingleFileStatus: Error occured while polling for prestaging file %s. [%d] %s" \
                                                                                                % ( path, e.code, e.message )
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

    self.log.debug( 'GFAL2_StorageBase.pinFile: Attempting to pin %s file(s).' % len( urls ) )
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

    self.log.debug( "GFAL2_StorageBase.__pinSingleFile: Attempting to issue pinning request for single file: %s" % path )

    try:
      self.gfal2.set_opt_boolean( "BDII", "ENABLE", True )
      ( status, token ) = self.gfal2.bring_online( path, lifetime, self.stageTimeout, True )
      self.log.debug( "GFAL2_StorageBase.__pinSingleFile: pinning issued - Status: %s" % status )
      if status >= 0:
        return S_OK( token )
      else:
        errStr = 'GFAL2_StorageBase.__pinSingleFile: an error occured while issuing pinning.'
        return S_ERROR( errStr )
    except gfal2.GError, e:
      errStr = "GFAL2_StorageBase.__pinSingleFile: Error occured while pinning file %s. [%d] %s" % ( path, e.code, e.message )
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

    self.log.debug( "GFAL2_StorageBase.releaseFile: Attempting to release %s file(s)." % len( urls ) )

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

    self.log.debug( "GFAL2_StorageBase.__releaseSingleFile: Attempting to release single file: %s" % path )
    if not type( token ) == StringType:
      token = str( token )
    try:
      self.gfal2.set_opt_boolean( "BDII", "ENABLE", True )
      status = self.gfal2.release( path, token )
      if status >= 0:
        return S_OK( token )
      else:
        errStr = "GFAL2_StorageBase.__releaseSingleFile: Error occured: Return status < 0"
        return S_ERROR( errStr )
    except gfal2.GError, e:
      errStr = "GFAL2_StorageBase.__releaseSingleFile: Error occured while releasing file %s. [%d] %s" % ( path, e.code, e.message )
      self.log.error( errStr )
      return S_ERROR( errStr )



  def __getChecksum( self, path, checksumType = None ):
    """ Calculate the checksum (ADLER32 by default) of a file on the storage

    :param self: self reference
    :param str path: path to single file on storage (srm://...)
    :returns S_OK( checksum ) if checksum could be calculated
             S_ERROR( errMsg ) if something failed
    """
    if not checksumType:
      errStr = "GFAL2_StorageBase.__getChecksum: No checksum type set by the storage element. Can't retrieve checksum"
      self.log.error( errStr, path )
      return S_ERROR( errStr )

    self.log.debug( 'GFAL2_StorageBase.__getChecksum: Trying to calculate checksum of file %s' % path )

    res = self.__isSingleFile( path )

    if not res['OK']:
      errStr = 'GFAL2_StorageBase.__getChecksum: provided path is not a file'
      self.log.error( errStr, path )
      return S_ERROR( errStr )
    else:
      try:
        self.log.debug( "GFAL2_StorageBase.__getChecksum: using %s checksum" % checksumType )
        fileChecksum = self.gfal2.checksum( path, checksumType )
        return S_OK( fileChecksum )

      except gfal2.GError, e:
        errStr = 'GFAL2_StorageBase.__getChecksum: failed to calculate checksum.'
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
      metaDict['Mode'] = statInfo.st_mode & ( S_IRWXU | S_IRWXG | S_IRWXO )

    return metaDict


  @staticmethod
  def __convertTime( time ):
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
        self.log.debug( "GFAL2_StorageBase.createDirectory: Successfully created directory on storage %s" % url )
        successful[url] = True
      else:
        self.log.error( "GFAL2_StorageBase.createDirectory: Failed to create directory on storage.", "%s: %s" % ( url, res['Message'] ) )
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


    try:
      self.log.debug( "GFAL2_StorageBase.__createSingleDirectory: %s" % path )
      status = self.gfal2.mkdir( path, 755 )
      self.log.debug( 'GFAL2_StorageBase.__createSingleDirectory: Status return of mkdir: %s' % status )
      if status >= 0:
        return S_OK()
      else:
        errStr = 'GFAL2_StorageBase.__createSingleDirectory: Status return > 0. Error.'
        return S_ERROR( errStr )
    except gfal2.GError, e:
      # error: directory already exists
      if e.code == errno.EEXIST:  # or e.code == errno.EACCES:
        self.log.debug( "GFAL2_StorageBase.__createSingleDirectory: Directory already exists" )
        return S_OK()
      # any other error: failed to create directory
      else:
        errStr = "GFAL2_StorageBase.__createSingleDirectory: failed to create directory."
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

    self.log.debug( "GFAL2_StorageBase.isDirectory: checking whether %s path(s) are directory(ies)." % len( urls ) )

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

    self.log.debug( "GFAL2_StorageBase.__isSingleDirectory: Determining whether %s is a directory or not." % path )
    try:
      statInfo = self.gfal2.stat( path )
      # instead of return S_OK( S_ISDIR( statInfo.st_mode ) ) we use if/else. So we can use the log.
      if S_ISDIR( statInfo.st_mode ):
        return S_OK ( True )
      else:
        self.log.debug( "GFAL2_StorageBase.__isSingleDirectory: Path is not a directory" )
        return S_OK( False )
    except gfal2.GError, e:
      if e.code == errno.ENOENT:
        errStr = "GFAL2_StorageBase.__isSingleDirectory: Directory doesn't exist."
        self.log.error( errStr, e.message )
        return S_ERROR( errStr )
      else:
        errStr = "GFAL2_StorageBase.__isSingleDirectory: failed to determine if path %s is a directory." % path
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

    self.log.debug( "GFAL2_StorageBase.listDirectory: Attempting to list %s directories" % len( urls ) )
    self.log.debug( 'GFAL2_StorageBase.listDirectory: Directories to list: %s' % urls )
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
        errStr = "GFAL2_StorageBase.listDirectory: path is not a directory"
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



  def __listSingleDirectory( self, path, internalCall=False ):
    """ List the content of the single directory provided
    :param self: self reference
    :param str path: single path on storage (srm://...)
    :returns S_ERROR( errStr ) if there is an error
             S_OK( dictionary ): Key: SubDirs and Files
                                 The values of the Files are dictionaries with filename as key and metadata as value
                                 The values of SubDirs are just the dirnames as key and True as value
    """
    self.log.debug( "GFAL2_StorageBase.__listSingleDirectory: Attempting to list content of single directory" )
    try:
      listing = self.gfal2.listdir( path )

    except gfal2.GError, e:
      if e.code == errno.ENOENT:
        errStr = 'GFAL2_StorageBase.__listSingleDirectory: directory does not exist'
        self.log.error( errStr, e.message )
        return S_ERROR( errStr )
      else:
        errStr = 'GFAL2_StorageBase.__listSingleDirectory: could not list directory content.'
        self.log.error( errStr, e.message )
        return S_ERROR( errStr )

    files = {}
    subDirs = {}
    urlStart = self.getURLBase( withWSUrl=True )['Value']
    for entry in listing:
      fullPath = os.path.join( path, entry )
      self.log.debug( 'GFAL2_StorageBase.__listSingleDirectory: path: %s' % fullPath )
      res = self.__getSingleMetadata( fullPath )
      if res['OK']:
        metadataDict = res['Value']
	subPathLFN = fullPath if internalCall else fullPath.replace( urlStart, '' )
        if metadataDict['Directory']:
          subDirs[subPathLFN] = metadataDict
        elif metadataDict['File']:
          files[subPathLFN] = metadataDict
        else:
          self.log.debug( "GFAL2_StorageBase.__listSingleDirectory: found item which is neither file nor directory", fullPath )

    return S_OK( {'SubDirs' : subDirs, 'Files' : files} )



  def getDirectory( self, path, localPath = False ):
    """ get a directory from the SE to a local path with all its files and subdirectories
    :param str path: path (or list of paths) on the storage (srm://...)
    :param str localPath: local path where the content of the remote directory will be saved,
                          if not defined it takes current working directory.
    :return successful and failed dictionaries. The keys are the paths,
            the values are dictionary {'Files': amount of files downloaded, 'Size' : amount of data downloaded}
            S_ERROR in case of argument problems
    """
    res = checkArgumentFormat( path )
    if not res['OK']:
      return res
    urls = res['Value']

    self.log.debug( "GFAL2_StorageBase.getDirectory: Attempting to get local copies of %s directories. %s" % ( len( urls ), urls ) )

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
          self.log.debug( "GFAL2_StorageBase.getDirectory: Successfully got local copy of %s" % src_dir )
          successful[src_dir] = {'Files':res['Value']['Files'], 'Size':res['Value']['Size']}
        else:
          self.log.error( "GFAL2_StorageBase.getDirectory: Failed to get entire directory.", src_dir )
          failed[src_dir] = {'Files':res['Value']['Files'], 'Size':res['Value']['Size']}
      else:
        self.log.error( "GFAL2_StorageBase.getDirectory: Completely failed to get local copy of directory.", src_dir )
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

    self.log.debug( "GFAL2_StorageBase.__getSingleDirectory: Attempting to download directory %s at %s" % ( src_dir, dest_dir ) )

    filesReceived = 0
    sizeReceived = 0

    res = self.__isSingleDirectory( src_dir )
    if not res['OK']:
      errStr = 'GFAL2_StorageBase.__getSingleDirectory: Failed to find the source directory'
      self.log.debug( res['Message'], src_dir )
      return S_ERROR( errStr )

    # res['Value'] is False if it's not a directory
    if not res['Value']:
      errStr = 'GFAL2_StorageBase.__getSingleDirectory: The path provided is not a directory'
      self.log.error( errStr, src_dir )
      return S_ERROR( errStr )

    if not os.path.exists( dest_dir ):
      try:
        os.makedirs( dest_dir )
      except OSError, error:
        errStr = 'GFAL2_StorageBase.__getSingleDirectory: Error trying to create destination directory %s' % error
        self.log.exception( errStr )
        return S_ERROR( errStr )

    # Get the remote directory contents
    res = self.__listSingleDirectory( src_dir, internalCall = True )
    if not res['OK']:
      errStr = 'GFAL2_StorageBase.__getSingleDirectory: Failed to list the source directory.'
      self.log.error( errStr, src_dir )
      return S_ERROR( errStr )

    sFilesDict = res['Value']['Files']
    subDirsDict = res['Value']['SubDirs']

    # Get all the files in the directory
    receivedAllFiles = True
    self.log.debug( 'GFAL2_StorageBase.__getSingleDirectory: Trying to download the %s files' % len( sFilesDict ) )
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
    self.log.debug( 'GFAL2_StorageBase.__getSingleDirectory: Trying to recursively download the %s directories' % len( subDirsDict ) )
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

    self.log.debug( 'GFAL2_StorageBase.putDirectory: Attempting to put %s directories to remote storage' % len( urls ) )

    successful = {}
    failed = {}
    for destDir, sourceDir in urls.items():
      if not sourceDir:
        self.log.debug( 'SourceDir: %s' % sourceDir )
        errStr = 'GFAL2_StorageBase.putDirectory: No source directory set, make sure the input format is correct { dest. dir : source dir }'
        return S_ERROR( errStr )
      res = self.__putSingleDirectory( sourceDir, destDir )
      if res['OK']:
        if res['Value']['AllPut']:
          self.log.debug( "GFAL2_StorageBase.putDirectory: Successfully put directory to remote storage: %s" % destDir )
          successful[destDir] = { 'Files' : res['Value']['Files'], 'Size' : res['Value']['Size']}
        else:
          self.log.error( "GFAL2_StorageBase.putDirectory: Failed to put entire directory to remote storage.", destDir )
          failed[destDir] = { 'Files' : res['Value']['Files'], 'Size' : res['Value']['Size']}
      else:
        self.log.error( "GFAL2_StorageBase.putDirectory: Completely failed to put directory to remote storage.", destDir )
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
    self.log.debug( 'GFAL2_StorageBase.__putSingleDirectory: trying to upload %s to %s' % ( src_directory, dest_directory ) )

    filesPut = 0
    sizePut = 0

    if not os.path.isdir( src_directory ):
      errStr = 'GFAL2_StorageBase.__putSingleDirectory: The supplied source directory does not exist or is not a directory.'
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
          errStr = 'GFAL2_StorageBase.__putSingleDirectory: Failed to put directory to storage.'
          self.log.error( errStr, res['Message'] )
        else:
          if not res['Value']['AllPut']:
            allSuccessful = False
          filesPut += res['Value']['Files']
          sizePut += res['Value']['Size']

    if directoryFiles:
      res = self.putFile( directoryFiles )
      if not res['OK']:
        self.log.error( 'GFAL2_StorageBase.__putSingleDirectory: Failed to put files to storage.', res['Message'] )
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

    self.log.debug( "GFAL2_StorageBase.removeDirectory: Attempting to remove %s directories." % len( urls ) )

    successful = {}
    failed = {}

    for url in urls:
      res = self.__removeSingleDirectory( url, recursive )

      if res['OK']:
        if res['Value']['AllRemoved']:
          self.log.debug( "GFAL2_StorageBase.removeDirectory: Successfully removed %s" % url )
          successful[url] = {'FilesRemoved':res['Value']['FilesRemoved'], 'SizeRemoved':res['Value']['SizeRemoved']}
        else:
          self.log.error( "GFAL2_StorageBase.removeDirectory: Failed to remove entire directory.", path )
          failed[url] = {'FilesRemoved':res['Value']['FilesRemoved'], 'SizeRemoved':res['Value']['SizeRemoved']}
      else:
        self.log.error( "GFAL2_StorageBase.removeDirectory: Completely failed to remove directory.", url )
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
      errStr = "GFAL2_StorageBase.__removeSingleDirectory: %s" % res['Message']
      self.log.error( errStr, path )
      return S_ERROR( errStr )

    # res['Value'] is True if it is a directory
    if not res['Value']:
      errStr = "GFAL2_StorageBase.__removeSingleDirectory: The supplied path is not a directory."
      self.log.error( errStr, path )
      return S_ERROR( errStr )

    # Get the remote directory contents
    res = self.__listSingleDirectory( path, internalCall = True )
    if not res['OK']:
      errStr = "GFAL2_StorageBase.__removeSingleDirectory: Failed to list the directory."
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
      self.log.debug( "GFAL2_StorageBase.__removeSingleDirectory: Trying to recursively remove %s folder." % len( subDirsDict ) )
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
    self.log.debug( "GFAL2_StorageBase.__removeSingleDirectory: Trying to remove %s files." % len( sFilesDict ) )
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
        status = self.gfal2.rmdir( path )
        if status < 0:
          errStr = "GFAL2_StorageBase.__removeSingleDirectory: Error occured while removing directory. Status: %s" % status
          self.log.debug( errStr )
          allRemoved = False
      except gfal2.GError, e:
        if e.code == errno.ENOENT:
          errStr = 'GFAL2_StorageBase.__removeSingleDirectory: Files does not exist'
          self.log.debug( errStr )
        else:
          errStr = 'GFAL2_StorageBase.__removeSingleDirectory: Failed to remove directory %s' % path
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

    self.log.debug( 'GFAL2_StorageBase.getDirectorySize: Attempting to get size of %s directories' % len( urls ) )

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

    self.log.debug( "GFAL2_StorageBase.__getSingleDirectorySize: Attempting to get the size of directory %s" % path )

    res = self.__listSingleDirectory( path )
    if not res['OK']:
      return res

    directorySize = 0
    directoryFiles = 0
    # itervalues returns a list of values of the dictionnary
    for fileDict in res['Value']['Files'].itervalues():
      directorySize += fileDict['Size']
      directoryFiles += 1

    self.log.debug( "GFAL2_StorageBase.__getSingleDirectorySize: Successfully obtained size of %s." % path )
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

    self.log.debug( "GFAL2_StorageBase.getDirectoryMetadata: Attempting to fetch metadata." )

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
    self.log.debug( "GFAL2_StorageBase.__getSingleDirectoryMetadata: Fetching metadata of directory %s." % path )

    res = self.__getSingleMetadata( path )

    if not res['OK']:
      return res

    metadataDict = res['Value']

    if not metadataDict['Directory']:
      errStr = "GFAL2_StorageBase.__getSingleDirectoryMetadata: Provided path is not a directory."
      self.log.error( errStr, path )
      return S_ERROR( errStr )

    return S_OK( metadataDict )

### methods for manipulating the client ###


#
#   def isPfnForProtocol( self, *parms, **kws ):
#     """ check if PFN :pfn: is valid for :self.protocol:
#     """
#     return S_ERROR( "GFAL2_StorageBase.isPfnForProtocol: Implement me!" )

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


  def _getExtendedAttributes( self, path, attributes = None ):
    """ Get all the available extended attributes of path

    :param self: self reference
    :param str path: path of which we wan't extended attributes
    :param str list attributes: list of extended attributes we want to receive
    :return S_OK( attributeDict ) if successful. Where the keys of the dict are the attributes and values the respective values
    """
    attributeDict = {}
    # get all the extended attributes from path
    try:
      if not attributes:
        attributes = self.gfal2.listxattr( path )
        # castor storages time out when file is not staged so we remove it for
        # the metadata call since it's not used there anyway and only when we 
        # call getTransportURL we add it as keyword in the function parameters
        attributes.remove('user.replicas')
      # get all the respective values of the extended attributes of path
      for attribute in attributes:
        self.log.debug( "GFAL2_StorageBase._getExtendedAttributes: Path is %s" % path )
        attributeDict[attribute] = self.gfal2.getxattr( path, attribute )

      return S_OK( attributeDict )
    # simple error messages, the method that is calling them adds the source of error.
    except gfal2.GError, e:
      if e.code == errno.ENOENT:
        errStr = 'GFAL2_StorageBase._getExtendedAttributesPath does not exist.'
        self.log.error( errStr, e.message )
        return S_ERROR( errStr )
      else:
        errStr = 'GFAL2_StorageBase._getExtendedAttributes: Something went wrong while checking for extended attributes. Please see error log for more information.'
        self.log.error( errStr, e.message )
        return S_ERROR( errStr )

