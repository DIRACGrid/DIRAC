""" DIPStorage class is the client of the DIRAC Storage Element.

    The following methods are available in the Service interface

    getMetadata()
    get()
    getDir()
    put()
    putDir()
    remove()

"""

__RCSID__ = "$Id$"

from DIRAC                                          import gLogger, S_OK, S_ERROR
from DIRAC.Resources.Utilities.Utils                import checkArgumentFormat
from DIRAC.Resources.Storage.StorageBase            import StorageBase
from DIRAC.Core.Utilities.Pfn                       import pfnparse, pfnunparse
from DIRAC.Core.DISET.TransferClient                import TransferClient
from DIRAC.Core.DISET.RPCClient                     import RPCClient
from DIRAC.Core.Utilities.File                      import getSize
import os, types, random

class DIPStorage( StorageBase ):

  def __init__( self, storageName, protocol, path, host, port, spaceToken, wspath ):
    """
    """
    if path:
      wspath = path
      path = ''

    self.protocolName = 'DIP'
    self.name = storageName
    self.protocol = protocol
    self.path = path
    self.host = host

    # Several ports can be specified as comma separated list, choose
    # randomly one of those ports
    ports = port.split( ',' )
    random.shuffle( ports )
    self.port = ports[0]

    self.wspath = wspath
    self.spaceToken = spaceToken

    self.url = protocol + "://" + host + ":" + self.port + wspath
    self.cwd = ''
    self.checkSum = "CheckSum"
    self.isok = True

  def setParameters( self, parameters ):
    """ Applying extra storage parameters
    """
    if "CheckSum" in parameters and parameters['CheckSum'].lower() in ['no', 'false', 'off']:
      self.checkSum = "NoCheckSum"
    return S_OK()

  ################################################################################
  #
  # The methods below are for manipulating the client
  #

  def isPfnForProtocol( self, path ):
    """ Check that this is a path
    """
    if path.startswith( '/' ):
      return S_OK( True )
    else:
      return S_OK( False )

  def getPFNBase( self ):
    return S_OK( '' )

  def resetWorkingDirectory( self ):
    """ Reset the working directory to the base dir
    """
    self.cwd = self.path

  def changeDirectory( self, directory ):
    """ Change the directory to the supplied directory
    """
    if directory[0] == '/':
      directory = directory.lstrip( '/' )
    self.cwd = '%s/%s' % ( self.cwd, directory )

  def getParameters( self ):
    """ This gets all the storage specific parameters pass when instantiating the storage
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

  def getCurrentURL( self, fileName ):
    """ Obtain the current file URL from the current working directory and the filename
    """
    if fileName:
      if fileName[0] == '/':
        fileName = fileName.lstrip( '/' )
    try:
      fullUrl = '%s/%s' % ( self.cwd, fileName )
      return S_OK( fullUrl )
    except Exception, x:
      errStr = "Failed to create URL %s" % x
      return S_ERROR( errStr )

  def getProtocolPfn( self, pfnDict, withPort ):
    """ From the pfn dict construct the pfn to be used
    """
    # pfnDict['Protocol'] = ''
    # pfnDict['Host'] = ''
    # pfnDict['Port'] = ''
    # pfnDict['WSUrl'] = ''
    res = pfnunparse( pfnDict )
    return res

  def getTransportURL( self, path, protocols = False ):
    """ Obtain the TURLs for the supplied path and protocols
    """
    res = self.exists( path )
    if res['OK']:
      for url in res['Value']['Successful']:
        if protocols and not self.protocol in protocols:
          res['Value']['Successful'].pop( url )
          res['Value']['Failed'][url] = 'Protocol not supported'
          continue
        if url[0] == '/':
          nameDict = self.getParameters()['Value']
          nameDict['FileName'] = url
          ret = pfnunparse( nameDict )
          if ret['OK']:
            res['Value']['Successful'][url] = ret['Value']
          else:
            res['Value']['Successful'].pop( url )
            res['Value']['Failed'][url] = ret['Message']
        else:
          res['Value']['Successful'][url] = url
    return res

  #############################################################
  #
  # These are the methods for file manipulation
  #

  def exists( self, path ):
    """ Check if the given path exists. The 'path' variable can be a string or a list of strings.
    """
    res = self.__checkArgumentFormat( path )
    if not res['OK']:
      return res
    urls = res['Value']
    successful = {}
    failed = {}
    serviceClient = RPCClient( self.url )
    for url in urls:
      gLogger.debug( "DIPStorage.exists: Determining existence of %s." % url )
      res = serviceClient.exists( url )
      if res['OK']:
        successful[url] = res['Value']
      else:
        failed[url] = res['Message']
    resDict = {'Failed':failed, 'Successful':successful}
    return S_OK( resDict )

  def putFile( self, path, sourceSize = 0 ):
    """Put a file to the physical storage
    """
    res = checkArgumentFormat( path )
    if not res['OK']:
      return res
    urls = res['Value']
    successful = {}
    failed = {}
    for dest_url, src_file in urls.items():
      gLogger.debug( "DIPStorage.putFile: Executing transfer of %s to %s" % ( src_file, dest_url ) )
      res = self.__putFile( src_file, dest_url )
      if res['OK']:
        successful[dest_url] = res['Value']
      else:
        failed[dest_url] = res['Message']
    resDict = {'Failed':failed, 'Successful':successful}
    return S_OK( resDict )

  def __putFile( self, src_file, dest_url ):
    res = pfnparse( src_file )
    if not res['OK']:
      return res
    localCache = False
    srcDict = res['Value']
    if srcDict['Protocol'] in ['dips', 'dip']:
      localCache = True
      srcSEURL = srcDict['Protocol'] + '://' + srcDict['Host'] + ':' + srcDict['Port'] + srcDict['WSUrl']
      transferClient = TransferClient( srcSEURL )
      res = transferClient.receiveFile( srcDict['FileName'], os.path.join( srcDict['Path'], srcDict['FileName'] ) )
      if not res['OK']:
        return res
      src_file = srcDict['FileName']

    if not os.path.exists( src_file ):
      errStr = "DIPStorage.__putFile: The source local file does not exist."
      gLogger.error( errStr, src_file )
      return S_ERROR( errStr )
    sourceSize = getSize( src_file )
    if sourceSize == -1:
      errStr = "DIPStorage.__putFile: Failed to get file size."
      gLogger.error( errStr, src_file )
      return S_ERROR( errStr )
    transferClient = TransferClient( self.url )
    res = transferClient.sendFile( src_file, dest_url, token = self.checkSum )
    if localCache:
      os.unlink( src_file )
    if res['OK']:
      return S_OK( sourceSize )
    else:
      return res

  def getFile( self, path, localPath = False ):
    """Get a local copy in the current directory of a physical file specified by its path
    """
    res = self.__checkArgumentFormat( path )
    if not res['OK']:
      return res
    urls = res['Value']
    successful = {}
    failed = {}
    for src_url in urls:
      fileName = os.path.basename( src_url )
      if localPath:
        dest_file = "%s/%s" % ( localPath, fileName )
      else:
        dest_file = "%s/%s" % ( os.getcwd(), fileName )
      gLogger.debug( "DIPStorage.getFile: Executing transfer of %s to %s" % ( src_url, dest_file ) )
      res = self.__getFile( src_url, dest_file )
      if res['OK']:
        successful[src_url] = res['Value']
      else:
        failed[src_url] = res['Message']
    resDict = {'Failed':failed, 'Successful':successful}
    return S_OK( resDict )

  def __getFile( self, src_url, dest_file ):
    transferClient = TransferClient( self.url )
    res = transferClient.receiveFile( dest_file, src_url, token = self.checkSum )
    if not res['OK']:
      return res
    if not os.path.exists( dest_file ):
      errStr = "DIPStorage.__getFile: The destination local file does not exist."
      gLogger.error( errStr, dest_file )
      return S_ERROR( errStr )
    destSize = getSize( dest_file )
    if destSize == -1:
      errStr = "DIPStorage.__getFile: Failed to get the local file size."
      gLogger.error( errStr, dest_file )
      return S_ERROR( errStr )
    return S_OK( destSize )

  def removeFile( self, path ):
    """Remove physically the file specified by its path
    """
    res = self.__checkArgumentFormat( path )
    if not res['OK']:
      return res
    urls = res['Value']
    if not len( urls ) > 0:
      return S_ERROR( "DIPStorage.removeFile: No surls supplied." )
    successful = {}
    failed = {}
    serviceClient = RPCClient( self.url )
    for url in urls:
      gLogger.debug( "DIPStorage.removeFile: Attempting to remove %s." % url )
      res = serviceClient.remove( url, '' )
      if res['OK']:
        successful[url] = True
      else:
        failed[url] = res['Message']
    resDict = {'Failed':failed, 'Successful':successful}
    return S_OK( resDict )

  def isFile( self, path ):
    """ Determine whether the path is a directory
    """
    res = self.__checkArgumentFormat( path )
    if not res['OK']:
      return res
    urls = res['Value']
    successful = {}
    failed = {}
    gLogger.debug( "DIPStorage.isFile: Attempting to determine whether %s paths are files." % len( urls ) )
    serviceClient = RPCClient( self.url )
    for url in urls:
      res = serviceClient.getMetadata( url )
      if res['OK']:
        if res['Value']['Exists']:
          if res['Value']['Type'] == 'File':
            gLogger.debug( "DIPStorage.isFile: Successfully obtained metadata for %s." % url )
            successful[url] = True
          else:
            successful[url] = False
        else:
          failed[url] = 'File does not exist'
      else:
        gLogger.error( "DIPStorage.isFile: Failed to get metdata for %s." % url, res['Message'] )
        failed[url] = res['Message']
    resDict = {'Failed':failed, 'Successful':successful}
    return S_OK( resDict )

  def getFileSize( self, path ):
    """ Get size of supplied files
    """
    res = self.__checkArgumentFormat( path )
    if not res['OK']:
      return res
    urls = res['Value']
    successful = {}
    failed = {}
    gLogger.debug( "DIPStorage.getFileSize: Attempting to obtain size for %s files." % len( urls ) )
    res = self.getFileMetadata( urls )
    if not res['OK']:
      return res
    for url, urlDict in res['Value']['Successful'].items():
      if urlDict['Exists']:
        successful[url] = urlDict['Size']
      else:
        failed[url] = 'File does not exist'
    for url, error in res['Value']['Failed'].items():
      failed[url] = error
    resDict = {'Failed':failed, 'Successful':successful}
    return S_OK( resDict )

  def getFileMetadata( self, path ):
    """  Get metadata associated to the file
    """
    res = self.__checkArgumentFormat( path )
    if not res['OK']:
      return res
    urls = res['Value']
    successful = {}
    failed = {}
    gLogger.debug( "DIPStorage.getFileMetadata: Attempting to obtain metadata for %s files." % len( urls ) )
    serviceClient = RPCClient( self.url )
    for url in urls:
      pfn = url
      if url.find( self.url ) == 0:
        pfn = url[ ( len( self.url ) ):]
      res = serviceClient.getMetadata( pfn )
      if res['OK']:
        if res['Value']['Exists']:
          if res['Value']['Type'] == 'File':
            gLogger.debug( "DIPStorage.getFileMetadata: Successfully obtained metadata for %s." % url )
            successful[url] = res['Value']
          else:
            failed[url] = 'Supplied path is not a file'
        else:
          failed[url] = 'File does not exist'
      else:
        gLogger.error( "DIPStorage.getFileMetadata: Failed to get metdata for %s." % url, res['Message'] )
        failed[url] = res['Message']
    resDict = {'Failed':failed, 'Successful':successful}
    return S_OK( resDict )

  #############################################################
  #
  # These are the methods for directory manipulation
  #

  def listDirectory( self, path ):
    """ List the contents of the directory
    """
    res = self.__checkArgumentFormat( path )
    if not res['OK']:
      return res
    urls = res['Value']
    successful = {}
    failed = {}
    gLogger.debug( "DIPStorage.listDirectory: Attempting to list %s directories." % len( urls ) )
    serviceClient = RPCClient( self.url )
    for url in urls:
      res = serviceClient.listDirectory( url, 'l' )
      if not res['OK']:
        failed[url] = res['Message']
      else:
        files = {}
        subDirs = {}
        for subPath, pathDict in res['Value'].items():
          if pathDict['Type'] == 'File':
            files[subPath] = pathDict
          elif pathDict['Type'] == 'Directory':
            subDirs[subPath] = pathDict
        successful[url] = {}
        successful[url]['SubDirs'] = subDirs
        successful[url]['Files'] = files
    resDict = {'Failed':failed, 'Successful':successful}
    return S_OK( resDict )

  def isDirectory( self, path ):
    """ Determine whether the path is a directory
    """
    res = self.__checkArgumentFormat( path )
    if not res['OK']:
      return res
    urls = res['Value']
    successful = {}
    failed = {}
    gLogger.debug( "DIPStorage.isDirectory: Attempting to determine whether %s paths are directories." % len( urls ) )
    serviceClient = RPCClient( self.url )
    for url in urls:
      res = serviceClient.getMetadata( url )
      if res['OK']:
        if res['Value']['Exists']:
          if res['Value']['Type'] == 'Directory':
            gLogger.debug( "DIPStorage.isDirectory: Successfully obtained metadata for %s." % url )
            successful[url] = True
          else:
            successful[url] = False
        else:
          failed[url] = 'Directory does not exist'
      else:
        gLogger.error( "DIPStorage.isDirectory: Failed to get metdata for %s." % url, res['Message'] )
        failed[url] = res['Message']
    resDict = {'Failed':failed, 'Successful':successful}
    return S_OK( resDict )

  def getDirectorySize( self, path ):
    """ Get the size of the contents of the directory
    """
    res = self.__checkArgumentFormat( path )
    if not res['OK']:
      return res
    urls = res['Value']
    successful = {}
    failed = {}
    gLogger.debug( "DIPStorage.isDirectory: Attempting to determine whether %s paths are directories." % len( urls ) )
    serviceClient = RPCClient( self.url )
    for url in urls:
      res = serviceClient.getDirectorySize( url )
      if not res['OK']:
        failed[url] = res['Message']
      else:
        successful[url] = {'Files':0, 'Size':res['Value'], 'SubDirs':0}
    resDict = {'Failed':failed, 'Successful':successful}
    return S_OK( resDict )

  def getDirectoryMetadata( self, path ):
    """  Get metadata associated to the directory
    """
    res = self.__checkArgumentFormat( path )
    if not res['OK']:
      return res
    urls = res['Value']
    successful = {}
    failed = {}
    gLogger.debug( "DIPStorage.getFileMetadata: Attempting to obtain metadata for %s directories." % len( urls ) )
    serviceClient = RPCClient( self.url )
    for url in urls:
      res = serviceClient.getMetadata( url )
      if res['OK']:
        if res['Value']['Exists']:
          if res['Value']['Type'] == 'Directory':
            gLogger.debug( "DIPStorage.getFileMetadata: Successfully obtained metadata for %s." % url )
            successful[url] = res['Value']
          else:
            failed[url] = 'Supplied path is not a directory'
        else:
          failed[url] = 'Directory does not exist'
      else:
        gLogger.error( "DIPStorage.getFileMetadata: Failed to get metdata for %s." % url, res['Message'] )
        failed[url] = res['Message']
    resDict = {'Failed':failed, 'Successful':successful}
    return S_OK( resDict )

  def createDirectory( self, path ):
    """ Create the remote directory
    """
    res = self.__checkArgumentFormat( path )
    if not res['OK']:
      return res
    urls = res['Value']
    successful = {}
    failed = {}
    gLogger.debug( "DIPStorage.createDirectory: Attempting to create %s directories." % len( urls ) )
    serviceClient = RPCClient( self.url )
    for url in urls:
      res = serviceClient.createDirectory( url )
      if res['OK']:
        gLogger.debug( "DIPStorage.createDirectory: Successfully created directory on storage: %s" % url )
        successful[url] = True
      else:
        gLogger.error( "DIPStorage.createDirectory: Failed to create directory on storage.", "%s: %s" % ( url, res['Message'] ) )
        failed[url] = res['Message']
    resDict = {'Failed':failed, 'Successful':successful}
    return S_OK( resDict )

  def putDirectory( self, path ):
    """ Put a local directory to the physical storage together with all its files and subdirectories.
    """
    res = checkArgumentFormat( path )
    if not res['OK']:
      return res
    urls = res['Value']
    successful = {}
    failed = {}
    gLogger.debug( "DIPStorage.putDirectory: Attemping to put %s directories to remote storage." % len( urls ) )
    transferClient = TransferClient( self.url )
    for destDir, sourceDir in urls.items():
      tmpList = os.listdir( sourceDir )
      sourceFiles = [ "%s/%s" % ( sourceDir, x ) for x in tmpList ]
      res = transferClient.sendBulk( sourceFiles, destDir )
      if res['OK']:
        successful[destDir] = {'Files':0, 'Size':0}
      else:
        failed[destDir] = res['Message']
    resDict = {'Failed':failed, 'Successful':successful}
    return S_OK( resDict )

  def removeDirectory( self, path, recursive = False ):
    """ Remove a directory from the storage together with all its files and subdirectories.
    """
    res = self.__checkArgumentFormat( path )
    if not res['OK']:
      return res
    urls = res['Value']
    successful = {}
    failed = {}
    gLogger.debug( "DIPStorage.removeDirectory: Attempting to remove %s directories." % len( urls ) )
    serviceClient = RPCClient( self.url )
    for url in urls:
      res = serviceClient.removeDirectory( url, '' )
      if res['OK']:
        gLogger.debug( "DIPStorage.removeDirectory: Successfully removed directory on storage: %s" % url )
        successful[url] = {'FilesRemoved':0, 'SizeRemoved':0}
      else:
        gLogger.error( "DIPStorage.removeDirectory: Failed to remove directory from storage.", "%s: %s" % ( url, res['Message'] ) )
        failed[url] = res['Message']
    resDict = {'Failed':failed, 'Successful':successful}
    return S_OK( resDict )

  def getDirectory( self, path, localPath = False ):
    """ Get a local copy in the current directory of a physical file specified by its path
    """
    res = self.__checkArgumentFormat( path )
    if not res['OK']:
      return res
    urls = res['Value']

    failed = {}
    successful = {}
    gLogger.debug( "DIPStorage.getDirectory: Attempting to get local copies of %s directories." % len( urls ) )
    transferClient = TransferClient( self.url )
    for src_dir in urls:
      if localPath:
        dest_dir = localPath
      else:
        dest_dir = os.getcwd()
      if not os.path.exists( dest_dir ):
        os.mkdir( dest_dir )
      res = transferClient.receiveBulk( dest_dir, src_dir )
      if res['OK']:
        gLogger.debug( "DIPStorage.getDirectory: Successfully got local copy of %s" % src_dir )
        successful[src_dir] = {'Files':0, 'Size':0}
      else:
        gLogger.error( "DIPStorage.getDirectory: Failed to get entire directory.", src_dir )
        failed[src_dir] = res['Message']
    resDict = {'Failed':failed, 'Successful':successful}
    return S_OK( resDict )

  def __checkArgumentFormat( self, path ):
    if type( path ) in types.StringTypes:
      urls = [path]
    elif type( path ) == types.ListType:
      urls = path
    elif type( path ) == types.DictType:
      urls = path.keys()
    else:
      return S_ERROR( "DIPStorage.__checkArgumentFormat: Supplied path is not of the correct format." )
    return S_OK( urls )

  def __executeOperation( self, url, method ):
    """ Executes the requested functionality with the supplied url
    """
    fcn = None
    if hasattr( self, method ) and callable( getattr( self, method ) ):
      fcn = getattr( self, method )
    if not fcn:
      return S_ERROR( "Unable to invoke %s, it isn't a member function of DIPStorage" % method )

    res = fcn( url )
    if not res['OK']:
      return res
    elif url not in res['Value']['Successful']:
      return S_ERROR( res['Value']['Failed'][url] )

    return S_OK( res['Value']['Successful'][url] )

  def getCurrentStatus(self):
    """ Ask the server the current status (disk usage). Needed for RSS
    """
    serviceClient = RPCClient( self.url )
    res = serviceClient.getAdminInfo()
    if not res['OK']:
      return res
    se_status = {}
    se_status['totalsize'] = res['Value']['AvailableSpace'] + res['Value']['UsedSpace']
    se_status['unusedsize'] = res['Value']['AvailableSpace']
    se_status['guaranteedsize'] = res['Value']['MaxCapacity']
    return S_OK(se_status)
