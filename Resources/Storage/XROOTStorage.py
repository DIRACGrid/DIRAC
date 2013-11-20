""" This is the XROOTD StorageClass """

__RCSID__ = "$Id$"


from DIRAC                                      import gLogger, S_OK, S_ERROR
from DIRAC.Resources.Utilities.Utils            import checkArgumentFormat
from DIRAC.Resources.Storage.StorageBase        import StorageBase
from DIRAC.Core.Utilities.Subprocess            import shellCall
from DIRAC.Core.Utilities.Pfn                   import pfnparse, pfnunparse
from DIRAC.Core.Utilities.List                  import breakListIntoChunks
from DIRAC.Core.Utilities.File                  import getSize
import os
from types import StringType, StringTypes, DictType, ListType, IntType, BooleanType


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
    self.log.setLevel( "DEBUG" )

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
    """

    res = checkArgumentFormat( path )
    if not res['OK']:
      return res
    urls = res['Value'].keys()
    self.log.debug( "XROOTStorage.exists: Checking the existence of %s path(s)" % len( urls ) )

    successful = {}
    failed = {}

    for url in urls:
      res = self.__singleExists(url)
      if not res['OK']:
        return res

      if type( res['Value'] ) == BooleanType:
        successful[url] = res['Value']
      elif type( res['Value'] ) in StringTypes:  # something went wrong with the query
        failed[url] = res['Value']
      else:  # that's not normal and should never happen
        errStr = "XROOTStorage.exists: Completely failed to check the existance of path (got impossible return type (%s) from __getSingleFileMetadata)." % type( res['Value'] )
        gLogger.error( errStr, res['Message'] )
        return S_ERROR( errStr )

    resDict = {'Failed':failed, 'Successful':successful}
    return S_OK( resDict )


  def __singleExists( self, path ):
    """Check if the given path exists. The 'path' variable can be a string or a list of strings.

    :param self: self reference
    :param path: path (only 1) on storage (it's a pfn root://blablabla)
    """

    res = pfnparse( path )
    if not res['OK']:
      return res
    pfnDict = res['Value']
    xFilePath = '/'.join( [pfnDict['Path'], pfnDict['FileName']] )
    status, statInfo = self.xrootClient.stat( xFilePath )
    if status.ok:
      self.log.debug( "XROOTStorage.exists: Path exists: %s" % path )
      return S_OK( True )
    else:
      # I don't know when the fatal flag is set, or if it is even ever set
      if status.fatal:
        errStr = "XROOTStorage.__singleExists: Completely failed to determine the existence of files."
        self.log.fatal( errStr, "%s %s" % ( self.name, status.message ) )
        return S_ERROR( errStr )
      elif status.error:
        # errno 3011 corresponds to the file not existing
        if status.errno == 3011:
          errStr = "XROOTStorage.__singleExists: Path does not exists: %s" % path
          self.log.debug( errStr )
          return S_OK( False )
        else:
          errStr = "XROOTStorage.__singleExists: Error in querying: %s" % status.message
          self.log.debug( errStr )
          return S_OK( errStr )

    return S_ERROR ( "XROOTStorage.__singleExists : reached end of method, should not happen" )



  #############################################################
  #
  # These are the methods for file manipulation
  #

  def isFile( self, path ):
    """Check if the given path exists and it is a file
      :param self: self reference
      :param path: path (or list of path) on storage
    """
    res = checkArgumentFormat( path )
    if not res['OK']:
      return res
    urls = res['Value'].keys()
    self.log.debug( "XROOTStorage.isFile: Determining whether %s paths are files." % len( urls ) )

    successful = {}
    failed = {}

    for url in urls:

      res = self.__isSingleFile( url )
      if not res['OK']:
        return res

      if type( res['Value'] ) == BooleanType:
        successful[url] = res['Value']
      elif type( res['Value'] ) in StringTypes:  # something went wrong with the query
        failed[url] = res['Value']
      else:  # that's not normal and should never happen
        errStr = "XROOTStorage.isFile: Completely failed to check whether the path is a file (got impossible return type (%s) from __getSingleFileMetadata)." % type( res['Value'] )
        gLogger.error( errStr, res['Message'] )
        return S_ERROR( errStr )    
            

    resDict = {'Failed':failed, 'Successful':successful}
    return S_OK( resDict )


  def __isSingleFile( self, path ):
    """Check if the given path exists and it is a file
      :param self: self reference
      :param path: single path on storage (pfn : root://...)
    """

    # NEEDED???
    # res = self.exists( path )

    res = self.__getSingleFileMetadata( path )
    if not res['OK']:
      errStr = "XROOTStorage.__isSingleFile: Completely failed to get path metadata."
      gLogger.error( errStr, res['Message'] )
      return S_ERROR( errStr )

    if type( res['Value'] ) == DictType:  # we could get the metadata for that url
      return S_OK( res['Value']['File'] )
    elif type( res['Value'] ) in StringTypes:  # something went wrong with the query
      return S_OK( res['Value'] )
    else:  # that's not normal and should never happen
      errStr = "XROOTStorage.__isSingleFile: Completely failed to determine whether path is a file (got impossible return type (%s) from __getSingleFileMetadata)." % type( res['Value'] )
      gLogger.error( errStr, res['Message'] )
      return S_ERROR( errStr )


    return S_ERROR ( "XROOTStorage.__isSingleFile : reached end of method, should not happen" )



  def getFile( self, path, localPath = False ):
    """ make a local copy of a storage :path:

    :param self: self reference
    :param str path: path (pfn root://) on storage
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
    """

    self.log.info( "XROOTStorage.__getSingleFile: Executing transfer of %s to %s" % ( src_url, dest_file ) )


    if not os.path.exists( os.path.dirname( dest_file ) ):
      self.log.debug( "XROOTStorage.__getSingleFile: Local directory does not yet exist %s. Creating..." % os.path.dirname( dest_file ) )
      try:
        os.makedirs( os.path.dirname( dest_file ) )
      except OSError, error:
        errStr = "XROOTStorage.__getSingleFile: Exception creation the destination directory %s" % error
        self.log.exception( errStr )
        return S_ERROR( errStr )

    # I could also just use the Force option of the copy method of the API...
    if os.path.exists( dest_file ):
      self.log.debug( "XROOTStorage.__getSingleFile: Local file already exists %s. Removing..." % dest_file )
      try:
        os.remove( dest_file )
      except OSError, error:
        errStr = "XROOTStorage.__getSingleFile: Exception removing the file %s" % error
        self.log.exception( errStr )
        return S_ERROR( errStr )
       


    res = self.__getSingleFileSize( src_url )
    if not res['OK']:
      return S_ERROR( res['Message'] )
    remoteSize = res['Value']

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
        errStr = "XROOTStorage.__getSingleFile: Exception removing local file %s" % error
        self.log.exception( errStr )
        errorMessage = "%s (exception removing local file %s)." % ( errorMessage, error )

    return S_ERROR( errorMessage )



  def putFile( self, path, sourceSize = 0 ):
    """Put a copy of the local file to the current directory on the
       physical storage
    """

    print "PATH %s" % path
    return S_ERROR( "Storage.putFile: implement me!" )


  def removeFile( self, *parms, **kws ):
    """Remove physically the file specified by its path
    """
    return S_ERROR( "Storage.removeFile: implement me!" )

  def getFileMetadata( self, path ):
    """  Get metadata associated to the file(s)
      :param self: self reference
      :param path: path (or list of path) on storage (pfn : root://...)
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

      if type( res['Value'] ) == DictType:  # we could get the metadata for that url
        successful[url] = res['Value']
      elif type( res['Value'] ) in StringTypes:  # something went wrong with the query
        failed[url] = res['Value']
      else:  # that's not normal and should never happen
        errStr = "XROOTStorage.getPathMetadata: Completely failed to get path metadata (got impossible return type (%s) from __getSingleFileMetadata)." % type( res['Value'] )
        gLogger.error( errStr, res['Message'] )
        return S_ERROR( errStr )


    return S_OK( { 'Failed' : failed, 'Successful' : successful } )

  def __getSingleFileMetadata( self, path ):
    """  Actually fetch the metadata associated to the file
      :param self: self reference
      :param path: path (only 1) on storage (pfn : root://...)
    """

    res = pfnparse( path )
    if not res['OK']:
      return res
    pfnDict = res['Value']
    xFilePath = '/'.join( [pfnDict['Path'], pfnDict['FileName']] )
    status, statInfo = self.xrootClient.stat( xFilePath )

    if status.ok:
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

      return S_OK( metadataDict )

    else:
      # I don't know when the fatal flag is set, or if it is even ever set
      if status.fatal:
        errStr = "XROOTStorage.__getSingleFileMetadata: Completely failed to get path metadata."
        self.log.fatal( errStr, "%s %s" % ( self.name, status.message ) )
        return S_ERROR( errStr )
      elif status.error:
        # errno 3011 corresponds to the file not existing
        if status.errno == 3011:
          errStr = "XROOTStorage.__getSingleFileMetadata: Path does not exist"
        else:
          errStr = "XROOTStorage.__getSingleFileMetadata: Error in querying: %d" % status.message
        self.log.debug( errStr )
        return S_OK( errStr )


    return S_OK( metadataDict )

  def getFileSize( self, path ):
    """Get the physical size of the given file

      :param self: self reference
      :param path: path (or list of path) on storage (pfn : root://...)
    """

    res = checkArgumentFormat( path )
    if not res['OK']:
      return res
    urls = res['Value']

    failed = {}
    successful = {}
    for url in urls:

      res = self.__getSingleFileSize( url )

      if not res['OK']:
        errStr = "XROOTStorage.getFileSize: Completely failed to get file size."
        gLogger.error( errStr, res['Message'] )
        return S_ERROR( errStr )

      if type( res['Value'] ) == IntType:  # we could get the metadata for that url
        successful[url] = res['Value']
      elif type( res['Value'] ) in StringTypes:  # something went wrong with the query
        failed[url] = res['Value']
      else:  # that's not normal and should never happen
        errStr = "XROOTStorage.getFileSize: Completely failed to get path metadata (got impossible return type (%s) from __getSingleFileMetadata)." % type( res['Value'] )
        gLogger.error( errStr, res['Message'] )
        return S_ERROR( errStr )

    return S_OK( { 'Failed' : failed, 'Successful' : successful } )


  def __getSingleFileSize( self, path ):
    """Get the physical size of the given file

      :param self: self reference
      :param path: single path on storage (pfn : root://...)
    """

    # We fetch all the metadata
    res = self.__getSingleFileMetadata( path )

    if not res['OK']:
      errStr = "XROOTStorage.__getSingleFileSize: Completely failed to get file size."
      gLogger.error( errStr, res['Message'] )
      return S_ERROR( errStr )

    if type( res['Value'] ) == DictType:  # we could get the metadata for that url
      return S_OK( int( res['Value']['Size'] ) )
    elif type( res['Value'] ) in StringTypes:  # something went wrong with the query
      return S_OK( res['Value'] )
    else:  # that's not normal and should never happen
      errStr = "XROOTStorage.__getSingleFileSize: Completely failed to get path metadata (got impossible return type (%s) from __getSingleFileMetadata)." % type( res['Value'] )
      gLogger.error( errStr, res['Message'] )
      return S_ERROR( errStr )


  def getTransportURL( self, path, protocols = False ):
    """ obtain the tURLs for the supplied path and protocols

    :param self: self reference
    :param str path: path on storage (pfn : root://...)
    :param mixed protocols: protocols to use
    """
    res = checkArgumentFormat( path )
    if not res['OK']:
      return res
    urls = res['Value']

    if protocols:
      if type( protocols ) == StringType:
        if protocols != self.protocol:
          S_ERROR( "getTransportURL: Must supply desired protocols to this plug-in (root)." )
      elif type( protocols ) == ListType:
        if self.protocol not in protocols:
          S_ERROR( "getTransportURL: Must supply desired protocols to this plug-in (root)." )

    # For the time being, I assume I should not check whether the file exists or not
    # So I just return the list of urls keys
    successful = dict( [rootUrl, rootUrl] for rootUrl in urls )
    failed = {}

    return S_OK( { 'Failed' : failed, 'Successful' : successful } )





  def prestageFile( self, *parms, **kws ):
    """ Issue prestage request for file
    """
    return S_ERROR( "Storage.prestageFile: implement me!" )

  def prestageFileStatus( self, *parms, **kws ):
    """ Obtain the status of the prestage request
    """
    return S_ERROR( "Storage.prestageFileStatus: implement me!" )

  def pinFile( self, *parms, **kws ):
    """ Pin the file on the destination storage element
    """
    return S_ERROR( "Storage.pinFile: implement me!" )

  def releaseFile( self, *parms, **kws ):
    """ Release the file on the destination storage element
    """
    return S_ERROR( "Storage.releaseFile: implement me!" )

  #############################################################
  #
  # These are the methods for directory manipulation
  #

  def isDirectory( self, *parms, **kws ):
    """Check if the given path exists and it is a directory
    """
    return S_ERROR( "Storage.isDirectory: implement me!" )

  def getDirectory( self, *parms, **kws ):
    """Get locally a directory from the physical storage together with all its
       files and subdirectories.
    """
    return S_ERROR( "Storage.getDirectory: implement me!" )

  def putDirectory( self, *parms, **kws ):
    """Put a local directory to the physical storage together with all its
       files and subdirectories.
    """
    return S_ERROR( "Storage.putDirectory: implement me!" )

  def createDirectory( self, *parms, **kws ):
    """ Make a new directory on the physical storage
    """
    return S_ERROR( "Storage.createDirectory: implement me!" )

  def removeDirectory( self, *parms, **kws ):
    """Remove a directory on the physical storage together with all its files and
       subdirectories.
    """
    return S_ERROR( "Storage.removeDirectory: implement me!" )

  def listDirectory( self, *parms, **kws ):
    """ List the supplied path
    """
    return S_ERROR( "Storage.listDirectory: implement me!" )

  def getDirectoryMetadata( self, *parms, **kws ):
    """ Get the metadata for the directory
    """
    return S_ERROR( "Storage.getDirectoryMetadata: implement me!" )

  def getDirectorySize( self, *parms, **kws ):
    """ Get the size of the directory on the storage
    """
    return S_ERROR( "Storage.getDirectorySize: implement me!" )


  #############################################################
  #
  # These are the methods for manipulating the client
  #

  def isOK( self ):
    return self.isok

  def changeDirectory( self, newdir ):
    """ Change the current directory
    """
    self.cwd = newdir
    return S_OK()

  def getCurrentDirectory( self ):
    """ Get the current directory
    """
    return S_OK( self.cwd )

  def getName( self ):
    """ The name with which the storage was instantiated
    """
    return S_OK( self.name )

  def setParameters( self, parameters ):
    """ Set extra storage parameters, non-mandatory method
    """
    return S_OK()

  def getParameters( self ):
    """ This gets all the storage specific parameters pass when instantiating the storage
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
    """

    pfnDict['Protocol'] = self.protocol
    pfnDict['Host'] = self.host

    # These lines should be checked
    if withPort:
      pfnDict['Port'] = self.port
#    pfnDict['WSUrl'] = self.wspath
    ###################3
    return pfnunparse( pfnDict )

  def getCurrentURL( self, *parms, **kws ):
    """ Create the full URL for the storage using the configuration, self.cwd and the fileName
    """
    return S_ERROR( "Storage.getCurrentURL: implement me!" )
  
  def getPFNBase( self, withPort = False ):
    """ This will get the pfn base. This is then appended with the LFN in DIRAC convention.

    :param self: self reference
    :param bool withPort: flag to include port
    """
    return S_OK( { True : 'root://%s:%s/%s' % ( self.host, self.port, self.rootdir ),
                   False : 'root://%s/%s' % ( self.host, self.rootdir ) }[withPort] )
