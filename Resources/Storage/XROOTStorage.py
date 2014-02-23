"""
 This is the XROOTD StorageClass
 """

__RCSID__ = "$Id$"


from DIRAC                                      import gLogger, S_OK, S_ERROR
from DIRAC.Resources.Utilities.Utils            import checkArgumentFormat
from DIRAC.Resources.Storage.StorageBase        import StorageBase
from DIRAC.Core.Utilities.Pfn                   import pfnparse, pfnunparse
from DIRAC.Core.Utilities.File                  import getSize
import os
from types import StringType, ListType, DictType


from XRootD import client
from XRootD.client.flags import DirListFlags, OpenFlags, MkDirFlags, QueryCode, StatInfoFlags


class XROOTStorage( StorageBase ):
  """ .. class:: XROOTStorage

  Xroot interface to StorageElement using pyxrootd
  """

  def __init__( self, storageName, protocol, rootdir, host, port, spaceToken, wspath ):
    """ c'tor

    :param self: self reference
    :param str storageName: SE name
    :param str protocol: protocol to use
    :param str rootdir: base path for vo files
    :param str host: SE host
    :param int port: port to use to communicate with :host:
    :param str spaceToken: space token
    :param str wspath: location of SRM on :host:
    """

    # # init base class
    StorageBase.__init__( self, storageName, rootdir )
    self.log = gLogger.getSubLogger( "XROOTStorage", True )
#     self.log.setLevel( "DEBUG" )

    self.protocolName = 'XROOT'
    self.protocol = protocol
    self.host = host

    self.port = port
    self.wspath = wspath
    self.spaceToken = spaceToken

    # Aweful hack to cope for the moment with the anability of RSS to deal with something else than SRM

    self.port = ""
    self.wspath = ""
    self.spaceToken = ""

    # The API instance to be used
    self.xrootClient = client.FileSystem( host )


  def exists( self, path ):
    """Check if the given path exists. The 'path' variable can be a string or a list of strings.

    :param self: self reference
    :param path: path (or list of path) on storage (it's a pfn root://blablabla)
    :returns Failed dictionary: {pfn : errorMsg}
            Successful dictionary: {pfn : bool}
    """

    res = checkArgumentFormat( path )
    if not res['OK']:
      return res
    urls = res['Value']
    self.log.debug( "XROOTStorage.exists: Checking the existence of %s path(s)" % len( urls ) )

    successful = {}
    failed = {}

    for url in urls:
      res = self.__singleExists( url )

      # Check if there was a fatal error
      if not res['OK']:
        return res

      # No fatal error, lets check if we could verify the existance
      res = res['Value']

      if res['OK']:
        successful[url] = res['Value']
      else:  # something went wrong with the query
        failed[url] = res['Message']

    resDict = {'Failed':failed, 'Successful':successful}
    return S_OK( resDict )


  def __singleExists( self, path ):
    """Check if the given path exists. The 'path' variable can be a string or a list of strings.

    :param self: self reference
    :param path: path (only 1) on storage (it's a pfn root://blablabla)
    :returns: A 2 level nested S_ structure :
        S_ERROR if there is a fatal error
        S_OK (S_OK (boolean exists)) a boolean whether it exists or not
        S_OK (S_ERROR (errorMsg)) if there was a problem getting the information
    """

    self.log.debug( "XROOTStorage.__singleExists: Determining whether %s exists." % path )

    res = pfnparse( path )
    if not res['OK']:
      return res
    pfnDict = res['Value']
    xFilePath = '/'.join( [pfnDict['Path'], pfnDict['FileName']] )
    status, statInfo = self.xrootClient.stat( xFilePath )

    if status.ok:
      self.log.debug( "XROOTStorage.__singleExists: Path exists." )
      return S_OK( S_OK( True ) )
    else:
      # I don't know when the fatal flag is set, or if it is even ever set
      if status.fatal:
        errStr = "XROOTStorage.__singleExists: Completely failed to determine the existence of file."
        self.log.fatal( errStr, "%s %s" % ( self.name, status.message ) )
        return S_ERROR( errStr )
      elif status.error:
        # errno 3011 corresponds to the file not existing
        if status.errno == 3011:
          errStr = "XROOTStorage.__singleExists: Path does not exists"
          self.log.debug( errStr, path )
          return S_OK( S_OK( False ) )
        else:
          errStr = "XROOTStorage.__singleExists: Failed to determine the existence of file"
          self.log.debug( errStr, status.message )
          return S_OK( S_ERROR( errStr ) )

    errStr = "XROOTStorage.__singleExists : reached end of method, should not happen"
    self.log.error( errStr )
    return S_ERROR ( errStr )



  #############################################################
  #
  # These are the methods for file manipulation
  #

  def isFile( self, path ):
    """Check if the given path exists and it is a file
      :param self: self reference
      :param path: path (or list of path) on storage
      :returns: Successful dict {path : boolean}
                Failed dict {path : error message }
    """
    res = checkArgumentFormat( path )
    if not res['OK']:
      return res
    urls = res['Value']

    self.log.debug( "XROOTStorage.isFile: Determining whether %s paths are files." % len( urls ) )

    successful = {}
    failed = {}

    for url in urls:

      res = self.__isSingleFile( url )
      if not res['OK']:
        return res
      # No fatal error, nested structure
      res = res['Value']

      if res['OK']:
        successful[url] = res['Value']
      else:  # something went wrong with the query
        failed[url] = res['Message']

    resDict = {'Failed':failed, 'Successful':successful}
    return S_OK( resDict )


  def __isSingleFile( self, path ):
    """Check if the given path exists and it is a file
      :param self: self reference
      :param path: single path on storage (pfn : root://...)

      :returns: A 2 level nested S_ structure :
          S_ERROR if there is a fatal error
          S_OK (S_OK (boolean)) if it is a file or not
          S_OK (S_ERROR (errorMsg)) if there was a problem getting the info
    """
    self.log.debug( "XROOTStorage.__isSingleFile: Determining whether %s is a file." % path )
    return self.__getSingleMetadata( path, 'File' )



  def getFile( self, path, localPath = False ):
    """ make a local copy of a storage :path:

    :param self: self reference
    :param str path: path (pfn root://) on storage
    :param mixed localPath: if not specified, self.cwd
    :returns Successful dict {path : size}
             Failed dict {path : error message }
    """

    res = checkArgumentFormat( path )
    if not res['OK']:
      return res
    urls = res['Value']

    self.log.debug( "XROOTStorage.getFile: Trying to download %s files." % len( urls ) )

    failed = {}
    successful = {}
    for src_url in urls:
      fileName = os.path.basename( src_url )
      if localPath:
        dest_file = "%s/%s" % ( localPath, fileName )
      else:
        # other plugins use os.getcwd insted of self.cwd
        # -> error self.cwd is for remote, ot is os.getcwd the right one
        dest_file = "%s/%s" % ( os.getcwd(), fileName )
      res = self.__getSingleFile( src_url, dest_file )
      if res['OK']:
        successful[src_url] = res['Value']
      else:
        failed[src_url] = res['Message']
    return S_OK( { 'Failed' : failed, 'Successful' : successful } )



  def __getSingleFile( self, src_url, dest_file ):
    """ do a real copy of storage file :src_url: to local fs under :dest_file:

    :param self: self reference
    :param str src_url: SE url to cp (root://...)
    :param str dest_file: local fs path
    :returns: S_ERROR(errorMsg) in case of any problem
              S_OK(size of file) if all goes well
    """

    self.log.info( "XROOTStorage.__getSingleFile: Trying to download %s to %s" % ( src_url, dest_file ) )


    if not os.path.exists( os.path.dirname( dest_file ) ):
      self.log.debug( "XROOTStorage.__getSingleFile: Local directory does not yet exist. Creating...", os.path.dirname( dest_file ) )
      try:
        os.makedirs( os.path.dirname( dest_file ) )
      except OSError, error:
        errStr = "XROOTStorage.__getSingleFile: Exception creation the destination directory"
        self.log.exception( errStr, error )
        return S_ERROR( errStr )



    # Fetch the remote file size
    # I know that logicalLy I should create the local path first
    # but this gives a more coherent errors in case it is a directory
    # ("not a file" rather than "cannot delete local directory"

    res = self.__getSingleFileSize( src_url )
    if not res['OK']:
      return res
    # No fatal error, nested structure
    res = res['Value']

    # Error getting the size
    if not res['OK']:
      errStr = "XROOTStorage.__getSingleFile: Error getting the file size."
      self.log.exception( errStr, res['Message'] )
      return S_ERROR( errStr )

    remoteSize = res['Value']


    # I could also just use the Force option of the copy method of the API...
    if os.path.exists( dest_file ):
      self.log.debug( "XROOTStorage.__getSingleFile: Local file already exists. Removing...", dest_file )
      try:
        os.remove( dest_file )
      except OSError, error:
        errStr = "XROOTStorage.__getSingleFile: Exception removing the file."
        self.log.exception( errStr, "%s" % error )
        return S_ERROR( errStr )


    status = self.xrootClient.copy( src_url, dest_file )
    # For some reason, the copy method returns a tuple (status,None)
    status = status[0]

    if status.ok:
      self.log.debug( 'XROOTStorage.__getSingleFile: Got a file from storage.' )
      localSize = getSize( dest_file )
      if localSize == remoteSize:
        self.log.debug( "XROOTStorage.__getSingleFile: Post transfer check successful." )
        return S_OK( localSize )
      errorMessage = "XROOTStorage.__getSingleFile: Source and destination file sizes do not match (%s vs %s)." % ( remoteSize, localSize )
      self.log.error( errorMessage, src_url )
    else:
      errorMessage = "XROOTStorage.__getSingleFile: Failed to get file from storage."
      errStr = "%s %s" % ( status.message, status.errno )
      self.log.error( errorMessage, errStr )

    if os.path.exists( dest_file ):
      self.log.debug( "XROOTStorage.__getSingleFile: Removing local file %s." % dest_file )
      try:
        os.remove( dest_file )
      except OSError, error:
        errorMessage = "XROOTStorage.__getSingleFile: Exception removing local file "
        self.log.exception( errorMessage, error )

    return S_ERROR( errorMessage )



  def putFile( self, path, sourceSize = 0 ):
    """Put a copy of the local file to the current directory on the
       physical storage
       :param path: dictionnary {pfn (root://...) : localFile}
       :param sourceSize : size in B (NOT USED)
       :returns Successful dict {path : size}
                Failed dict {path : error message }
                S_ERROR(errMsg) in case of arguments problems
    """

    if type( path ) is StringType:
      return S_ERROR ( "XROOTStorage.putFile: path argument must be a dictionary (or a list of dictionary) { url : local path}" )
    elif type( path ) is ListType:
      if not len( path ):
        return S_OK( { 'Failed' : {}, 'Successful' : {} } )
      else:
        urls = dict( [( url, False ) for url in path] )
    elif type( path )  is DictType:
      if len( path ) != 1:
        return S_ERROR ( "XROOTStorage.putFile: path argument must be a dictionary (or a list of dictionary) { url : local path}" )
      urls = path




    failed = {}
    successful = {}

    for dest_url, src_file in urls.items():
      res = self.__putSingleFile( src_file, dest_url, sourceSize )
      if res['OK']:
        successful[dest_url] = res['Value']
      else:
        failed[dest_url] = res['Message']

    return S_OK( { 'Failed' : failed, 'Successful' : successful } )


  def __putSingleFile( self, src_file, dest_url, sourceSize = 0 ):
    """Put a copy of the local file to the current directory on the
       physical storage
       :param str dest_file: pfn (root://...)
       :param str src_file: local file to copy
       :param int sourceSize: size in B (NOT USED)
       :returns: S_OK( file size ) if everything went fine, S_ERROR otherwise
    """

    self.log.debug( "XROOTStorage.__putSingleFile: trying to upload %s to %s" % ( src_file, dest_url ) )

    # We create the folder first
    res = pfnparse( dest_url )
    if not res['OK']:
      return res
    pfnDict = res['Value']

    # There is a bug in xrootd-python-0.1.2-1 (fixed in master branch) which
    # forbids the MAKEPATH flag to work.
    status = self.xrootClient.mkdir( pfnDict['Path'], MkDirFlags.MAKEPATH )

    # the API returns (status,None...)
    status = status[0]

    if status.fatal:
      errStr = "XROOTStorage.__putSingleFile: Completely failed to create the destination folder."
      gLogger.error( errStr, status.message )
      return S_ERROR( errStr )
    # if it is only an error(Folder exists...), we try to keep going
    if status.error:
      errStr = "XROOTStorage.__putSingleFile: failed to create the destination folder."
      gLogger.debug( errStr, status.message )


    # Now we check if there is already a remote file. If yes, we remove it
    res = self.__singleExists( dest_url )

    if not res['OK']:
      return res

    # No fatal error, nested structure
    res = res['Value']
    if not res['OK']:
      errStr = "XROOTStorage.__putSingleFile: failed to determine pre-existance of remote file."
      gLogger.debug( errStr, res['Message'] )

    # This is true only if the file exists. Then we remove it
    if res['Value']:
      self.log.debug( "XROOTStorage.__putSingleFile: Remote file exists and needs to be removed" )
      res = self.__removeSingleFile( dest_url )
      # Fatal error during removal
      if not res['OK']:
        return res
      else:
        res = res['Value']
        if not res['OK']:
          self.log.debug( "XROOTStorage.__putSingleFile: Failed to remove remote file", res['Message'] )
        else:
          self.log.debug( "XROOTStorage.__putSingleFile: Successfully removed remote file" )

    # get the absolute path needed by the xroot api
    src_file = os.path.abspath( src_file )
    if not os.path.exists( src_file ):
      errStr = "XROOTStorage.__putSingleFile: The local source file does not exist."
      gLogger.error( errStr, src_file )
      return S_ERROR( errStr )
    sourceSize = getSize( src_file )
    if sourceSize == -1:
      errStr = "XROOTStorage.__putSingleFile: Failed to get file size."
      gLogger.error( errStr, src_file )
      return S_ERROR( errStr )

    # Perform the copy with the API
    status = self.xrootClient.copy( src_file, dest_url )
    # For some reason, the copy method returns a tuple (status,None)
    status = status[0]

    if status.ok:
      self.log.debug( 'XROOTStorage.__putSingleFile: Put file on storage.' )
      res = self.__getSingleFileSize( dest_url )
      # There was a fatal error
      if not res['OK']:
        return res
      # No fatal error, let see if we could get the size
      res = res['Value']

      if res['OK']:  # we could get the size for that url
        remoteSize = res['Value']
      else:
        errMsg = "XROOTStorage.__putSingleFile: Could not get remote file size"
        self.log.error( errMsg, res['Value'] )
        return S_ERROR( "Could not get remote file size" )

      if sourceSize == remoteSize:
        self.log.debug( "XROOTStorage.__putSingleFile: Post transfer check successful." )
        return S_OK( sourceSize )
      errorMessage = "XROOTStorage.__putSingleFile: Source and destination file sizes do not match (%s vs %s)." % ( sourceSize, remoteSize )
      self.log.error( errorMessage, src_file )
    else:
      errorMessage = "XROOTStorage.__putSingleFile: Failed to put file on storage."
      errStr = "%s %s" % ( status.message, status.errno )
      self.log.error( errorMessage, errStr )

    res = self.__singleExists( dest_url )

    if not res['OK']:
      return res

    # This is true only if the file exists. Then we remove it
    if res['Value'] == True:
      self.log.debug( "XROOTStorage.__putSingleFile: Removing remote residual file.", dest_url )

      res = self.__removeSingleFile( dest_url )
      # Fatal error during removal
      if not res['OK']:
        return res
      else:
        res = res['Value']
        if res['OK']:
          self.log.debug( "XROOTStorage.__putSingleFile: Failed to remove remote file.", dest_url )
        else:
          self.log.debug( "XROOTStorage.__putSingleFile: Successfully removed remote file.", dest_url )


    return S_ERROR( errorMessage )



  def removeFile( self, path ):
    """Remove physically the file specified by its path

      A non existing file will be considered as successfully removed.

      :param path: path (or list of path) on storage (pfn : root://...)
      :returns Successful dict {path : True}
               Failed dict {path : error message }
    """

    res = checkArgumentFormat( path )
    if not res['OK']:
      return res
    urls = res['Value']
    gLogger.debug( "RFIOStorage.removeFile: Attempting to remove %s files." % len( urls ) )

    failed = {}
    successful = {}

    for url in urls:
      res = self.__removeSingleFile( url )

      # The removal did not have a big problem
      if res['OK']:
        res = res['Value']
        # We could perform the removal
        if res['OK']:
          successful[url] = res['Value']
        else:
          failed [url] = res['Message']
      else:
        return res

    return S_OK( { 'Failed' : failed, 'Successful' : successful } )


  def __removeSingleFile( self, path ):
    """Remove physically the file specified by its path
      :param path: path on storage (pfn : root://...)
      :returns: A 2 level nested S_ structure :
                S_ERROR if there is a fatal error
                S_OK (S_OK (True)) if the file is not present anymore (deleted or did not exist)
                S_OK (S_ERROR (errorMsg)) if there was a problem removing the file
    """

    res = pfnparse( path )
    if not res['OK']:
      return res
    pfnDict = res['Value']
    xFilePath = '/'.join( [pfnDict['Path'], pfnDict['FileName']] )

    status = self.xrootClient.rm( xFilePath )
    # For some reason, the rm method returns a tuple (status,None)
    status = status[0]

    if status.ok:
      self.log.debug( "XROOTStorage.__removeSingleFile: Successfully removed file: %s" % path )
      return S_OK( S_OK( True ) )
    else:
      # I don't know when the fatal flag is set, or if it is even ever set
      if status.fatal:
        errStr = "XROOTStorage.__removeSingleFile: Completely failed to remove the file."
        self.log.fatal( errStr, "%s %s" % ( self.name, status.message ) )
        return S_ERROR( errStr )
      elif status.error:
        # errno 3011 corresponds to the file not existing
        if status.errno == 3011:
          self.log.debug( "XROOTStorage.__removeSingleFile: File does not exist" )
          return S_OK( S_OK( True ) )
        else:
          errStr = "XROOTStorage.__removeSingleFile: Failed to remove the file"
          self.log.debug( errStr, status.message )
          return S_OK( S_ERROR( errStr ) )

    return S_ERROR ( "XROOTStorage.__removeSingleFile: reached the end of the method, should not happen" )



  def getFileMetadata( self, path ):
    """  Get metadata associated to the file(s)

      :param self: self reference
      :param path: path (or list of path) on storage (pfn : root://...)
      :returns Successful dict {path : metadata}
         Failed dict {path : error message }
    """

    res = checkArgumentFormat( path )
    if not res['OK']:
      return res
    urls = res['Value']

    failed = {}
    successful = {}
    for url in urls:
      res = self.__getSingleFileMetadata( url )
      if not res['OK']:
        errStr = "XROOTStorage.getPathMetadata: Completely failed to get path metadata."
        gLogger.error( errStr, res['Message'] )
        return S_ERROR( errStr )

      # There were no fatal errors, so now we see if there were any other errors
      res = res['Value']
      if not res['OK']:
        failed[url] = res['Message']
      else:
        successful[url] = res['Value']

    return S_OK( { 'Failed' : failed, 'Successful' : successful } )


  def __getSingleMetadata( self, path, expectedType = None ):
    """  Fetches the metadata of a single file or directory

      If expectedType is None (default), then we fetch the metadata and return them.
      If it is set, then we return a S_OK(boolean) depending on whether the type matches or not

      :param self: self reference
      :param path: path (only 1) on storage (pfn : root://...)
      :param: expectedType : type that we expect the path to be ('File' or 'Directory')
      :returns: A 2 level nested S_ structure :
          S_ERROR if there is a fatal error
          S_OK (S_OK (MetadataDict)) if we could get the metadata
          S_OK (S_OK (Bool)) if we could get the metadata and is of type expectedType
          S_OK (S_ERROR (errorMsg)) if there was a problem geting the metadata
    """

    if expectedType and expectedType not in ['File', 'Directory']:
      return S_ERROR( "XROOTStorage.__getSingleMetadata : the 'expectedType' argument must be either 'File' or 'Directory'" )

    res = pfnparse( path )
    if not res['OK']:
      return res

    pfnDict = res['Value']
    xFilePath = '/'.join( [pfnDict['Path'], pfnDict['FileName']] )
    status, statInfo = self.xrootClient.stat( xFilePath )

    if status.ok:
      # Transform the api output into a dictionary
      metadataDict = self.__parseStatInfoFromApiOutput( statInfo )

      # If we expect a given type, we return a boolean
      if expectedType:
        isExpectedType = metadataDict[expectedType]
        return S_OK( S_OK( isExpectedType ) )
      # otherwise we return the metadata dictionnary
      return S_OK( S_OK( metadataDict ) )

    else:
      # I don't know when the fatal flag is set, or if it is even ever set
      if status.fatal:
        errStr = "XROOTStorage.__getSingleMetadata: Completely failed to get path metadata."
        self.log.fatal( errStr, "%s %s" % ( self.name, status.message ) )
        return S_ERROR( errStr )
      elif status.error:
        # errno 3011 corresponds to the file not existing
        if status.errno == 3011:
          errStr = "XROOTStorage.__getSingleMetadata: Path does not exist"
        else:
          errStr = "XROOTStorage.__getSingleMetadata: Error in querying: %s" % status.message
        self.log.debug( errStr )
        return S_OK( S_ERROR( errStr ) )

    return S_ERROR( "XROOTStorage.__getSingeFileMetadata : reached end of method. Should not happen" )


  def __getSingleFileMetadata( self, path ):
    """  Fetch the metadata associated to the file
      :param self: self reference
      :param path: path (only 1) on storage (pfn : root://...)
      :returns: A 2 level nested S_ structure :
          S_ERROR if there is a fatal error
          S_OK (S_OK (MetadataDict)) if we could get the metadata
          S_OK (S_ERROR (errorMsg)) if there was a problem getting the metadata or if it is not a file
    """

    res = self.__getSingleMetadata( path )

    if not res['OK']:
      return res

    # No fatal error, nested structure
    res = res['Value']

    if not res['OK']:
      return S_OK( res )

    metadataDic = res['Value']
    # If it is not a file
    if not metadataDic['File']:
      errStr = "XROOTStorage.__getSingleFileMetadata: Supplied path is not a file."
      self.log.error( errStr, path )
      return S_OK( S_ERROR( errStr ) )

    return S_OK( S_OK( metadataDic ) )





  def __parseStatInfoFromApiOutput( self, statInfo ):
    """  Split the content of the statInfo object into a dictionary
      :param self: self reference
      :param statInfo: XRootD.client.responses.StatInfo returned by the API
      :returns: a dictionary. List of keys :
                ModTime (str)
                ModTimeStr (str)
                Id (int)
                Size (int)
                Executable  (bool)
                Directory  (bool)
                Other  (bool)
                File  (bool)
                Offline  (bool)
                PoscPending  (bool)
                Readable  (bool)
                Writable (bool)
    """
    metadataDict = {'File' : False, 'Directory' : False}
    metadataDict['ModTime'] = statInfo.modtime
    metadataDict['ModTimeStr'] = statInfo.modtimestr
    metadataDict['Id'] = statInfo.id
    metadataDict['Size'] = statInfo.size

    statFlags = statInfo.flags
    metadataDict['Executable'] = bool( statFlags & StatInfoFlags.X_BIT_SET )
    metadataDict['Directory'] = bool( statFlags & StatInfoFlags.IS_DIR )
    metadataDict['Other'] = bool( statFlags & StatInfoFlags.OTHER )
    metadataDict['File'] = ( not metadataDict['Other'] and not metadataDict['Directory'] )
    metadataDict['Offline'] = bool( statFlags & StatInfoFlags.OFFLINE )
    metadataDict['PoscPending'] = bool( statFlags & StatInfoFlags.POSC_PENDING )
    metadataDict['Readable'] = bool( statFlags & StatInfoFlags.IS_READABLE )
    metadataDict['Writable'] = bool( statFlags & StatInfoFlags.IS_WRITABLE )

    return metadataDict

  def getFileSize( self, path ):
    """Get the physical size of the given file

      :param self: self reference
      :param path: path (or list of path) on storage (pfn : root://...)
      :returns Successful dict {path : size}
             Failed dict {path : error message }
    """

    res = checkArgumentFormat( path )
    if not res['OK']:
      return res
    urls = res['Value']

    failed = {}
    successful = {}

    for url in urls:
      res = self.__getSingleFileSize( url )

      # if there is a fatal error getting the size
      if not res['OK']:
        errStr = "XROOTStorage.getFileSize: Completely failed to get file size."
        gLogger.error( errStr, res['Message'] )
        return S_ERROR( errStr )

      # There was no fatal error, so we see if we could get the size
      res = res['Value']

      if not res['OK']:
        failed[url] = res['Message']
      else:
        successful[url] = res['Value']

    return S_OK( { 'Failed' : failed, 'Successful' : successful } )


  def __getSingleFileSize( self, path ):
    """Get the physical size of the given file

      :param self: self reference
      :param path: single path on storage (pfn : root://...)
      :returns: A 2 level nested S_ structure :
          S_ERROR if there is a fatal error
          S_OK (S_OK (size)) if we could get the size
          S_OK (S_ERROR (errorMsg)) if there was a problem geting the size
    """

    # We fetch all the metadata
    res = self.__getSingleFileMetadata( path )

    # If there was a fatal error
    if not res['OK']:
      errStr = "XROOTStorage.__getSingleFileSize: Completely failed to get file size."
      gLogger.error( errStr, res['Message'] )
      return S_ERROR( errStr )

    # No fatal error, so we check if the api called succeded
    res = res['Value']

    # We could not get the metadata
    if not res['OK']:
      return S_OK( S_ERROR( res['Message'] ) )
    else:
      return S_OK( S_OK( res['Value']['Size'] ) )



  def getTransportURL( self, path, protocols = False ):
    """ obtain the tURLs for the supplied path and protocols

    :param self: self reference
    :param str path: path on storage (pfn : root://...)
    :param mixed protocols: protocols to use (must be or include 'root')
    :returns Successful dict {path : path}
             Failed dict {path : error message }
    """
    res = checkArgumentFormat( path )
    if not res['OK']:
      return res
    urls = res['Value']

    if protocols:
      if type( protocols ) is StringType:
        if protocols != self.protocol:
          return S_ERROR( "getTransportURL: Must supply desired protocols to this plug-in (%s)." % self.protocol )
      elif type( protocols ) is ListType:
        if self.protocol not in protocols:
          return S_ERROR( "getTransportURL: Must supply desired protocols to this plug-in (%s)." % self.protocol )

    # For the time being, I assume I should not check whether the file exists or not
    # So I just return the list of urls keys
    successful = dict( [rootUrl, rootUrl] for rootUrl in urls )
    failed = {}

    return S_OK( { 'Failed' : failed, 'Successful' : successful } )



##################################################################
#
#    DO NOT REALLY MAKE SENSE FOR XROOT
#
##################################################################


  def prestageFile( self, *parms, **kws ):
    """ Issue prestage request for file
    """
    return S_ERROR( "XROOTStorage.prestageFile: not implemented!" )

  def prestageFileStatus( self, *parms, **kws ):
    """ Obtain the status of the prestage request
    """
    return S_ERROR( "XROOTStorage.prestageFile: not implemented!" )

  def pinFile( self, *parms, **kws ):
    """ Pin the file on the destination storage element
    """
    return S_ERROR( "XROOTStorage.prestageFile: not implemented!" )

  def releaseFile( self, *parms, **kws ):
    """ Release the file on the destination storage element
    """
    return S_ERROR( "XROOTStorage.prestageFile: not implemented!" )
#
##################################################################


  #############################################################
  #
  # These are the methods for directory manipulation
  #

  def isDirectory( self, path ):
    """Check if the given path exists and it is a directory
      :param self: self reference
      :param path: path (or list of path) on storage (pfn : root://...)
      :returns: Successful dict {path : boolean}
                Failed dict {path : error message }
    """
    res = checkArgumentFormat( path )
    if not res['OK']:
      return res
    urls = res['Value']

    self.log.debug( "XROOTStorage.isDirectory: Determining whether %s paths are directories." % len( urls ) )

    successful = {}
    failed = {}

    for url in urls:
      res = self.__isSingleDirectory( url )
      if not res['OK']:
        return res
      # No fatal error, nested structure
      res = res['Value']

      if res['OK']:
        successful[url] = res['Value']
      else:  # something went wrong with the query
        failed[url] = res['Message']

    resDict = {'Failed':failed, 'Successful':successful}

    return S_OK( resDict )


  def __isSingleDirectory( self, path ):
    """Check if the given path exists and it is a file
      :param self: self reference
      :param path: single path on storage (pfn : root://...)
      :returns: A 2 level nested S_ structure :
          S_ERROR if there is a fatal error
          S_OK (S_OK (boolean)) if it is a directory or not
          S_OK (S_ERROR (errorMsg)) if there was a problem geting the info

       We could have called 'not __isSingleFile', but since the API
       offers Directory, File and Other, we don't take the risk
    """

    return self.__getSingleMetadata( path, 'Directory' )


  def getDirectory( self, path, localPath = False ):
    """Get locally a directory from the physical storage together with all its
       files and subdirectories.
       :param: path: path (or list of path) on storage (pfn : root://...)
       :param: localPath: local path where to store what is downloaded
       :return: successful and failed dictionaries. The keys are the pathes,
               the values are dictionary {'Files': amount of files downloaded, 'Size': amount of data downloaded}
    """
    res = checkArgumentFormat( path )
    if not res['OK']:
      return res
    urls = res['Value']

    self.log.debug( "XROOTStorage.getDirectory: Attempting to get local copies of %s directories." % len( urls ) )

    failed = {}
    successful = {}

    for src_dir in urls:
      dirName = os.path.basename( src_dir )
      if localPath:
        dest_dir = "%s/%s" % ( localPath, dirName )
      else:
        # The other storage objects use os.getcwd(), I think it is a bug
        # -> no, self.cwd is remote
        dest_dir = "%s/%s" % ( os.getcwd(), dirName )

      res = self.__getSingleDirectory( src_dir, dest_dir )

      if res['OK']:
        if res['Value']['AllGot']:
          self.log.debug( "XROOTStorage.getDirectory: Successfully got local copy of %s" % src_dir )
          successful[src_dir] = {'Files':res['Value']['Files'], 'Size':res['Value']['Size']}
        else:
          self.log.error( "XROOTStorage.getDirectory: Failed to get entire directory.", src_dir )
          failed[src_dir] = {'Files':res['Value']['Files'], 'Size':res['Value']['Size']}
      else:
        self.log.error( "XROOTStorage.getDirectory: Completely failed to get local copy of directory.", src_dir )
        failed[src_dir] = {'Files':0, 'Size':0}

    return S_OK( {'Failed' : failed, 'Successful' : successful } )


  def __getSingleDirectory( self, src_dir, dest_dir ):
    """Download a single directory recursively
      :param self: self reference
      :param src_dir : remote directory to download (root://...)
      :param dest_dir: local destination path
      :returns: S_ERROR if there is a fatal error
              S_OK (statistics dictionary ) if we could download something :
                            'AllGot': boolean of whether we could download everything
                            'Files': amount of files received
                            'Size': amount of data received
    """

    self.log.debug( "XROOTStorage.__getSingleDirectory: Attempting to download directory %s at %s" % ( src_dir, dest_dir ) )

    filesReceived = 0  # counter for the amount of files received
    sizeReceived = 0  # counter for the data size received

    # Check the remote directory exists
    res = self.__isSingleDirectory( src_dir )
    if not res['OK']:
      errStr = "XROOTStorage.__getSingleDirectory: Completely failed (fatal error) to find the supplied source directory."
      self.log.error( errStr, src_dir )
      return S_ERROR( errStr )

    # No fatal error, nested return
    res = res['Value']

    if not res['OK']:
      errStr = "XROOTStorage.__getSingleDirectory: Failed to find the supplied source directory."
      self.log.error( errStr, src_dir )
      return S_ERROR( errStr )

    # res['Value'] is True if it is a directory
    if not res['Value']:
      errStr = "XROOTStorage.__getSingleDirectory: The supplied source is not a directory."
      self.log.error( errStr, src_dir )
      return S_ERROR( errStr )

    # Check the local directory exists and create it if not
    if not os.path.exists( dest_dir ):
      try:
        os.makedirs( dest_dir )
      except OSError, error:
        errStr = "XROOTStorage.__getSingleDirectory: Exception creation the destination directory %s" % error
        self.log.exception( errStr )
        return S_ERROR( errStr )

    # Get the remote directory contents
    res = self.__listSingleDirectory( src_dir )
    if not res['OK']:
      errStr = "XROOTStorage.__getSingleDirectory: Failed to list the source directory."
      self.log.error( errStr, src_dir )
      return S_ERROR( errStr )

    sFilesDict = res['Value']['Files']
    subDirsDict = res['Value']['SubDirs']

    # First get all the files in the directory
    receivedAllFiles = True
    self.log.debug( "XROOTStorage.__getSingleDirectory: Trying to first download the %s files." % len( sFilesDict ) )
    for sFile in sFilesDict:
      # Returns S__OK(Filesize) if it worked
      res = self.__getSingleFile( sFile, "/".join( [ dest_dir, os.path.basename( sFile ) ] ) )
      if res['OK']:
        filesReceived += 1
        sizeReceived += res['Value']
      else:
        receivedAllFiles = False


    # Then recursively get the sub directories
    receivedAllDirs = True
    self.log.debug( "XROOTStorage.__getSingleDirectory: Trying to recursively download the %s folder." % len( subDirsDict ) )
    for subDir in subDirsDict:
      subDirName = os.path.basename( subDir )
      localPath = '%s/%s' % ( dest_dir, subDirName )
      res = self.__getSingleDirectory( subDir, localPath )

      if not res['OK']:
        receivedAllDirs = False
      if res['OK']:
        if not res['Value']['AllGot']:
          receivedAllDirs = False
        filesReceived += res['Value']['Files']
        sizeReceived += res['Value']['Size']


    # Check whether all the operations were successful
    if receivedAllDirs and receivedAllFiles:
      allGot = True
    else:
      allGot = False

    resDict = {'AllGot':allGot, 'Files':filesReceived, 'Size':sizeReceived}
    return S_OK( resDict )


  def putDirectory( self, path ):
    """ puts a or several local directory to the physical storage together with all its files and subdirectories
        :param self: self reference
        :param str  path: dictionnary {pfn (root://...) : local dir}
        :return: successful and failed dictionaries. The keys are the pathes,
             the values are dictionary {'Files': amount of files uploaded, 'Size': amount of data uploaded}
    """
    res = checkArgumentFormat( path )
    if not res['OK']:
      return res
    urls = res['Value']

    successful = {}
    failed = {}
    self.log.debug( "XROOTStorage.putDirectory: Attemping to put %s directories to remote storage." % len( urls ) )
    for destDir, sourceDir in urls.items():
      res = self.__putSingleDirectory( sourceDir, destDir )
      if res['OK']:
        if res['Value']['AllPut']:
          self.log.debug( "XROOTStorage.putDirectory: Successfully put directory to remote storage: %s" % destDir )
          successful[destDir] = { 'Files' : res['Value']['Files'], 'Size' : res['Value']['Size']}
        else:
          self.log.error( "XROOTStorage.putDirectory: Failed to put entire directory to remote storage.", destDir )
          failed[destDir] = { 'Files' : res['Value']['Files'], 'Size' : res['Value']['Size']}
      else:
        self.log.error( "XROOTStorage.putDirectory: Completely failed to put directory to remote storage.", destDir )
        failed[destDir] = { "Files" : 0, "Size" : 0 }
    return S_OK( { "Failed" : failed, "Successful" : successful } )


  def __putSingleDirectory( self, src_directory, dest_directory ):
    """ puts one local directory to the physical storage together with all its files and subdirectories
        :param self: self reference
        :param src_directory : the local directory to copy
        :param dest_directory: pfn (root://...) where to copy
        :returns: S_ERROR if there is a fatal error
                  S_OK (statistics dictionary ) if we could upload something :
                                    'AllPut': boolean of whether we could upload everything
                                    'Files': amount of files uploaded
                                    'Size': amount of data uploaded
    """

    self.log.debug( "XROOTStorage.__putSingleDirectory: trying to upload %s to %s" % ( src_directory, dest_directory ) )

    filesPut = 0
    sizePut = 0
    # Check the local directory exists
    if not os.path.isdir( src_directory ):
      errStr = "XROOTStorage.__putSingleDirectory: The supplied source directory does not exist or is not a directory."
      self.log.error( errStr, src_directory )
      return S_ERROR( errStr )

    # Get the local directory contents
    contents = os.listdir( src_directory )
    allSuccessful = True
    directoryFiles = {}
    for fileName in contents:
      self.log.debug( "FILENAME %s" % fileName )
      localPath = '%s/%s' % ( src_directory, fileName )
      remotePath = '%s/%s' % ( dest_directory, fileName )
      if not os.path.isdir( localPath ):
        directoryFiles[remotePath] = localPath
      else:
        res = self.__putSingleDirectory( localPath, remotePath )
        if not res['OK']:
          errStr = "XROOTStorage.__putSingleDirectory: Failed to put directory to storage."
          self.log.error( errStr, res['Message'] )
        else:
          if not res['Value']['AllPut']:
            allSuccessful = False
          filesPut += res['Value']['Files']
          sizePut += res['Value']['Size']

    if directoryFiles:
      res = self.putFile( directoryFiles )
      if not res['OK']:
        self.log.error( "XROOTStorage.__putSingleDirectory: Failed to put files to storage.", res['Message'] )
        allSuccessful = False
      else:
        for fileSize in res['Value']['Successful'].itervalues():
          filesPut += 1
          sizePut += fileSize
        if res['Value']['Failed']:
          allSuccessful = False
    return S_OK( { 'AllPut' : allSuccessful, 'Files' : filesPut, 'Size' : sizePut } )


  def createDirectory( self, path ):
    """ Make a/several new directory on the physical storage
        This method creates all the intermediate directory

    :param self: self reference
    :param str path: path (or list of path) on storage (pfn : root://...)
    :returns Successful dict {path : True}
         Failed dict {path : error message }
    """
    urls = checkArgumentFormat( path )
    if not urls['OK']:
      return urls
    urls = urls['Value']

    successful = {}
    failed = {}
    self.log.debug( "XROOTStorage.createDirectory: Attempting to create %s directories." % len( urls ) )
    for url in urls:
      res = self.__createSingleDirectory( url )
      if res['OK']:
        self.log.debug( "XROOTStorage.createDirectory: Successfully created directory on storage: %s" % url )
        successful[url] = True
      else:
        self.log.error( "XROOTStorage.createDirectory: Failed to create directory on storage.",
                       "%s: %s" % ( url, res['Message'] ) )
        failed[url] = res['Message']
    return S_OK( { 'Failed' : failed, 'Successful' : successful } )

  def __createSingleDirectory( self, path ):
    """ Make a new directory on the physical storage
        This method creates all the intermediate directory

    :param self: self reference
    :param str path: single path on storage (pfn : root://...)
    :returns S_OK() if all went well
              S_ERROR(errMsg) in case of any problem
    """
    self.log.debug( "XROOTStorage.__createSingleDirectory: Attempting to create directory %s." % path )
    res = pfnparse( path )
    if not res['OK']:
      return res
    pfnDict = res['Value']
    xFilePath = '/'.join( [pfnDict['Path'], pfnDict['FileName']] )

    status = self.xrootClient.mkdir( xFilePath, MkDirFlags.MAKEPATH )

    if status.ok:
      return S_OK()
    else:
      if status.fatal:
        errMsg = "XROOTStorage.__createSingleDir : Completely failed to create directory"
      else:
        errMsg = "XROOTStorage.__createSingleDir : failed to create directory"
        self.log.error( errMsg, status.message )
      return S_ERROR( errMsg )


  def removeDirectory( self, path, recursive = False ):
    """Remove a directory on the physical storage together with all its files and
       subdirectories.
       :param path : single or list of path (root://..)
       :param recursive : if True, we recursively delete the subdir
       :return: successful and failed dictionaries. The keys are the pathes,
             the values are dictionary {'Files': amount of files deleted, 'Size': amount of data deleted}
    """
    res = checkArgumentFormat( path )
    if not res['OK']:
      return res
    urls = res['Value']

    self.log.debug( "XROOTStorage.removeDirectory: Attempting to remove %s directories." % len( urls ) )

    successful = {}
    failed = {}

    for url in urls:
      res = self.__removeSingleDirectory( url, recursive )
      if res['OK']:
        if res['Value']['AllRemoved']:
          self.log.debug( "XROOTStorage.removeDirectory: Successfully removed %s" % url )
          successful[url] = {'FilesRemoved':res['Value']['FilesRemoved'], 'SizeRemoved':res['Value']['SizeRemoved']}
        else:
          self.log.error( "XROOTStorage.removeDirectory: Failed to remove entire directory.", path )
          failed[url] = {'FilesRemoved':res['Value']['FilesRemoved'], 'SizeRemoved':res['Value']['SizeRemoved']}
      else:
        self.log.error( "XROOTStorage.removeDirectory: Completely failed to remove directory.", url )
        failed[url] = {'FilesRemoved':0, 'SizeRemoved':0}

    return S_OK( {'Failed' : failed, 'Successful' : successful } )



  def __removeSingleDirectory( self, path, recursive = False ):
    """Remove a directory on the physical storage together with all its files and
       subdirectories.
       :param path: pfn (root://...) of a directory to remove
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
      errStr = "XROOTStorage.__removeSingleDirectory: Completely failed (fatal error) to find the directory."
      self.log.error( errStr, path )
      return S_ERROR( errStr )

    # No fatal error, nested return
    res = res['Value']

    if not res['OK']:
      errStr = "XROOTStorage.__removeSingleDirectory: Failed to find the directory."
      self.log.error( errStr, path )
      return S_ERROR( errStr )

    # res['Value'] is True if it is a directory
    if not res['Value']:
      errStr = "XROOTStorage.__removeSingleDirectory: The supplied path is not a directory."
      self.log.error( errStr, path )
      return S_ERROR( errStr )


    # Get the remote directory contents
    res = self.__listSingleDirectory( path )
    if not res['OK']:
      errStr = "XROOTStorage.__removeSingleDirectory: Failed to list the directory."
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
      self.log.debug( "XROOTStorage.__removeSingleDirectory: Trying to recursively remove %s folder." % len( subDirsDict ) )
      for subDir in subDirsDict:
        subDirName = os.path.basename( subDir )
        localPath = '%s/%s' % ( path, subDirName )
        res = self.__removeSingleDirectory( localPath, recursive )  # recursive should be true..

        if not res['OK']:
          removedAllDirs = False
        if res['OK']:
          if not res['Value']['AllRemoved']:
            removedAllDirs = False
          filesRemoved += res['Value']['FilesRemoved']
          sizeRemoved += res['Value']['SizeRemoved']


    # Remove all the files in the directory
    self.log.debug( "XROOTStorage.__removeSingleDirectory: Trying to remove %s files." % len( sFilesDict ) )
    for sFile in sFilesDict:
      # Returns S__OK(Filesize) if it worked
      res = self.__removeSingleFile( sFile )
      if not res['OK']:
        return res
      # Nothing fatal, nested structure
      res = res['Value']

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


    # Now I try to remove the directory itself
    # We do it only if :
    #  - we go recursive, and everything was deleted
    # - we don't go recursive, but we deleted all files and there are no subfolders

    if ( recursive and allRemoved ) or ( not recursive and removedAllFiles and ( len( subDirsDict ) == 0 ) ):
      res = pfnparse( path )
      if not res['OK']:
        return res
      pfnDict = res['Value']
      xFilePath = '/'.join( [pfnDict['Path'], pfnDict['FileName']] )

      status = self.xrootClient.rmdir( xFilePath )

      # For some reason, the rmdir method returns a tuple (status,None)
      status = status[0]

      if not status.ok:
        if status.errno == 3011:
          errStr = "XROOTStorage.__removeSingleDirectory: File does not exist"
          self.log.debug( errStr )
        else:
          errStr = "XROOTStorage.__removeSingleDirectory: Error in querying: %s" % status.message
          self.log.debug( errStr )
          allRemoved = False

    resDict = {'AllRemoved': allRemoved, 'FilesRemoved': filesRemoved, 'SizeRemoved': sizeRemoved}
    return S_OK( resDict )



  def listDirectory( self, path ):
    """ List the supplied path
        CAUTION : It is not recursive!
       :param path : single or list of path (root://..)
       :return: successful and failed dictionaries. The keys are the pathes,
             the values are dictionary 'SubDirs' and 'Files'. Each are dictionaries with
            path as key and metadata as values (for Files only, SubDirs has just True as value)
    """

    res = checkArgumentFormat( path )
    if not res['OK']:
      return res
    urls = res['Value']

    self.log.debug( "XROOTStorage.listDirectory: Attempting to list %s directories." % len( urls ) )

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
        errStr = "XROOTStorage.listDirectory: path is not a directory."
        gLogger.error( errStr, url )
        failed[url] = errStr

    for directory in directories:
      res = self.__listSingleDirectory( directory )
      if not res['OK']:
        failed[directory] = res['Message']
        continue
      successful[directory] = res['Value']

    resDict = {'Failed':failed, 'Successful':successful}
    return S_OK( resDict )


  def __listSingleDirectory( self, path ):
    """List the content of a single directory, NOT RECURSIVE
      :param self: self reference
      :param path: single path on storage (pfn : root://...)
      :returns: A 2 level nested S_ structure :
          S_ERROR if there is an error (fatal or not)
          S_OK (dictionary)) The dictionnary has 2 keys : SubDirs and Files
                             The values of Files are dictionary with Filename as key and metadata as value
                             The values of SubDirs are just Dirname as key and True as value
    """

    res = pfnparse( path )
    if not res['OK']:
      return res

    self.log.debug( "XROOTStorage.__listSingleDirectory: Attempting to list directory %s." % path )

    pfnDict = res['Value']
    xFilePath = '/'.join( [pfnDict['Path'], pfnDict['FileName']] )

    status, listing = self.xrootClient.dirlist( xFilePath, DirListFlags.STAT )

    if not status.ok:
      errorMsg = "XROOTStorage.__listSingleDirectory : could not list the directory content"
      self.log.error( errorMsg, status.message )
      return S_ERROR ( errorMsg )

    files = {}
    subDirs = {}

    for entry in listing:
      fullPath = "root://%s%s%s" % ( self.host, xFilePath, entry.name )
      metadataDict = self.__parseStatInfoFromApiOutput( entry.statinfo )
      if metadataDict['Directory']:
        subDirs[fullPath] = True
        continue
      elif metadataDict['File']:
        files[fullPath] = metadataDict
      else:  # This "other", whatever that is
        self.log.debug( "XROOTStorage.__listSingleDirectory : found an item which is not a file nor a directory", fullPath )

    return S_OK( {'SubDirs' : subDirs, 'Files' : files } )



  def __getSingleDirectoryMetadata( self, path ):
    """ Fetch the metadata associated to the directory
      :param self: self reference
      :param path: path (only 1) on storage (pfn : root://...)
      :returns: A 2 level nested S_ structure :
          S_ERROR if there is a fatal error
          S_OK (S_OK (MetadataDict)) if we could get the metadata
          S_OK (S_ERROR (errorMsg)) if there was a problem getting the metadata or if it is not a directory
    """

    self.log.debug( "XROOTStorage.__getSingleDirectoryMetadata: Fetching metadata of directory %s." % path )

    res = self.__getSingleMetadata( path )

    if not res['OK']:
      return res

    # No fatal error, nested structure
    res = res['Value']

    if not res['OK']:
      return S_OK( res )

    metadataDic = res['Value']
    # If it is not a file
    if not metadataDic['Directory']:
      errStr = "XROOTStorage.__getSingleDirectoryMetadata: Supplied path is not a directory."
      self.log.error( errStr, path )
      return S_OK( S_ERROR( errStr ) )

    return S_OK( S_OK( metadataDic ) )


  def getDirectoryMetadata( self, path ):
    """  Get metadata associated to the directory(ies)
      :param self: self reference
      :param path: path (or list of path) on storage (pfn : root://...)
      :returns Successful dict {path : metadata}
               Failed dict {path : error message }
    """

    res = checkArgumentFormat( path )
    if not res['OK']:
      return res
    urls = res['Value']

    self.log.debug( "XROOTStorage.getDirectoryMetadata: Attempting to fetch metadata of %s directories." % len( urls ) )

    failed = {}
    successful = {}

    for url in urls:
      res = self.__getSingleDirectoryMetadata( url )
      if not res['OK']:
        errStr = "XROOTStorage.getDirectoryMetadata: Completely failed to get path metadata."
        gLogger.error( errStr, res['Message'] )
        return S_ERROR( errStr )

      # There were no fatal errors, so now we see if there were any other errors
      res = res['Value']
      if not res['OK']:
        failed[url] = res['Message']
      else:
        successful[url] = res['Value']

    return S_OK( { 'Failed' : failed, 'Successful' : successful } )




  def __getSingleDirectorySize( self, path ):
    """ Get the size of the directory on the storage
      CAUTION : the size is not recursive, and does not go into subfolders
      :param self: self reference
      :param path: path (single) on storage (pfn : root://...)
      :return: S_ERROR in case of problem
                S_OK (Dictionary) Files : amount of files in the directory
                                  Size : summed up size of files
                                  subDirs : amount of sub directories
    """

    self.log.debug( "XROOTStorage.__getSingleDirectorySize: Attempting to get the size of directory %s" % path )

    res = self.__listSingleDirectory( path )
    if not res['OK']:
      return res

    directorySize = 0
    directoryFiles = 0
    # itervalues returns a list of values of the dictionnary
    for fileDict in res['Value']['Files'].itervalues():
      directorySize += fileDict['Size']
      directoryFiles += 1

    self.log.debug( "XROOTStorage.__getSingleDirectorySize: Successfully obtained size of %s." % path )
    subDirectories = len( res['Value']['SubDirs'] )
    return S_OK( { 'Files' : directoryFiles, 'Size' : directorySize, 'SubDirs' : subDirectories } )


  def getDirectorySize( self, path ):
    """ Get the size of the directory on the storage
      CAUTION : the size is not recursive, and does not go into subfolders
      :param self: self reference
      :param path: path (or list of path) on storage (pfn : root://...)
      :returns: list of successfull and failed dictionnary, both indexed by the path
                In the failed, the value is the error message
                In the successful the values are dictionnaries : Files : amount of files in the directory
                                                                Size : summed up size of files
                                                                subDirs : amount of sub directories
    """
    res = checkArgumentFormat( path )
    if not res['OK']:
      return res
    urls = res['Value']

    self.log.debug( "XROOTStorage.getDirectorySize: Attempting to get size of %s directories." % len( urls ) )

    failed = {}
    successful = {}

    for url in urls:
      res = self.__getSingleDirectorySize( url )
      if not res['OK']:
        failed[url] = res['Message']
      else:
        successful[url] = res['Value']

    return S_OK( { 'Failed' : failed, 'Successful' : successful } )



  #############################################################
  #
  # These are the methods for manipulating the client
  #

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


  def getParameters( self ):
    """ This gets all the storage specific parameters pass when instantiating the storage
      :returns Dictionary with keys : StorageName, ProtocolName, Protocol, Host, Path, Port, SpaceToken, WSUrl
    """
    parameterDict = {}
    parameterDict['StorageName'] = self.name
    parameterDict['ProtocolName'] = self.protocolName
    parameterDict['Protocol'] = self.protocol
    parameterDict['Host'] = self.host
    parameterDict['Path'] = self.rootdir  # I don't know why it's called rootdir in the baseclass and Path here...
    parameterDict['Port'] = self.port
    parameterDict['SpaceToken'] = self.spaceToken
    parameterDict['WSUrl'] = self.wspath
    return S_OK( parameterDict )

  def getProtocolPfn( self, pfnDict, withPort ):
    """ Get the PFN for the protocol with or without the port
      :param self:
      :param pfnDict: dictionary where the keys/values are the different part of the surl
      :param bool withPort: include port information
      :returns S_OK(pfn)
    """

    pfnDict['Protocol'] = self.protocol
    pfnDict['Host'] = self.host

    if not pfnDict['Path'].startswith( self.rootdir ):
      pfnDict['Path'] = os.path.join( self.rootdir, pfnDict['Path'].strip( '/' ) )

    # These lines should be checked
    if withPort:
      pfnDict['Port'] = self.port
#    pfnDict['WSUrl'] = self.wspath
    ###################3

    # pfnunparse does not take into account the double // so I have to trick it
    # The problem is that I cannot take into account the port, which is always empty (it seems..)
    return S_OK( 'root://%s%s/%s' % ( self.host, pfnDict['Path'], pfnDict['FileName'] ) )


  def getCurrentURL( self, fileName ):
    """ Create the full URL for the storage using the configuration, self.cwd and the fileName
        :param fileName : name of the file for which we want the URL
        :returns full URL
    """
    if fileName:
      if fileName[0] == '/':
        fileName = fileName.lstrip( '/' )
    try:
      fullUrl = "%s://%s/%s/%s" % ( self.protocol, self.host, self.cwd, fileName )
      fullUrl = fullUrl.rstrip( "/" )
      return S_OK( fullUrl )
    except TypeError, error:
      return S_ERROR( "Failed to create URL %s" % error )

  def getPFNBase( self, withPort = False ):
    """ This will get the pfn base. This is then appended with the LFN in DIRAC convention.

    :param self: self reference
    :param bool withPort: flag to include port
    :returns PFN
    """
    return S_OK( { True : 'root://%s:%s/%s' % ( self.host, self.port, self.rootdir ),
                   False : 'root://%s%s' % ( self.host, self.rootdir ) }[withPort] )
