########################################################################
# $HeadURL $
# File: StorageElementHandler.py
########################################################################
""" 
:mod: StorageElementHandler 

.. module: StorageElementHandler
:synopsis: StorageElementHandler is the implementation of a simple StorageElement
           service in the DISET framework

The following methods are available in the Service interface

getMetadata()      - get file metadata
listDirectory()    - get directory listing
remove()           - remove one file
removeDirectory()  - remove on directory recursively
removeFileList()   - remove files in the list
getAdminInfo()     - get administration information about the SE status

The handler implements also the DISET data transfer calls
toClient(), fromClient(), bulkToClient(), bulkFromClient
which support single file, directory and file list upload and download

The class can be used as the basis for more advanced StorageElement implementations

"""

__RCSID__ = "$Id$"

## imports
import os
import shutil
import re
from stat import ST_MODE, ST_SIZE, ST_ATIME, ST_CTIME, ST_MTIME, S_ISDIR, S_IMODE
from types import StringType, StringTypes, ListType
## from DIRAC
from DIRAC import gLogger, S_OK, S_ERROR
from DIRAC.Core.DISET.RequestHandler import RequestHandler, getServiceOption
from DIRAC.Core.Utilities.Os import getDirectorySize
from DIRAC.Core.Utilities.Subprocess import shellCall

BASE_PATH = ""
MAX_STORAGE_SIZE = 0
USE_TOKENS = False

def initializeStorageElementHandler( serviceInfo ):
  """  Initialize Storage Element global settings
  """

  global BASE_PATH
  global USE_TOKENS
  global MAX_STORAGE_SIZE

  BASE_PATH = getServiceOption( serviceInfo, "BasePath", BASE_PATH )
  if not BASE_PATH:
    gLogger.error( 'Failed to get the base path' )
    return S_ERROR( 'Failed to get the base path' )
  if not os.path.exists( BASE_PATH ):
    os.makedirs( BASE_PATH )

  USE_TOKENS = getServiceOption( serviceInfo, "%UseTokens", USE_TOKENS )
  MAX_STORAGE_SIZE = getServiceOption( serviceInfo, "MaxStorageSize", MAX_STORAGE_SIZE )

  gLogger.info( 'Starting DIRAC Storage Element' )
  gLogger.info( 'Base Path: %s' % BASE_PATH )
  gLogger.info( 'Max size: %d MB' % MAX_STORAGE_SIZE )
  gLogger.info( 'Use access control tokens: ' + str( USE_TOKENS ) )
  return S_OK()

class StorageElementHandler( RequestHandler ):
  """
  .. class:: StorageElementHandler

  """

  def __confirmToken( self, token, path, mode ):
    """ Confirm the access rights for the path in a given mode
    """
    # Not yet implemented
    return True

  @staticmethod
  def __checkForDiskSpace( dpath, size ):
    """ Check if the directory dpath can accommodate 'size' volume of data
    """
    stats = os.statvfs( dpath )
    dsize = stats.f_bsize * stats.f_bavail
    maxStorageSizeBytes = MAX_STORAGE_SIZE * 1024 * 1024
    return ( min( dsize, maxStorageSizeBytes ) > size )

  def __resolveFileID( self, fileID ):
    """ get path to file for a given :fileID: """

    port = self.getCSOption( 'Port', '' )
    if not port:
      return ''

    if ":%s" % port in fileID:
      loc = fileID.find( ":%s" % port )
      if loc >= 0:
        fileID = fileID[loc + len( ":%s" % port ):]

    serviceName = self.serviceInfoDict['serviceName']
    loc = fileID.find( serviceName )
    if loc >= 0:
      fileID = fileID[loc + len( serviceName ):]

    loc = fileID.find( '?=' )
    if loc >= 0:
      fileID = fileID[loc + 2:]

    if fileID.find( BASE_PATH ) == 0:
      return fileID
    while fileID and fileID[0] == '/':
      fileID = fileID[1:]
    return os.path.join( BASE_PATH, fileID )

  @staticmethod
  def __getFileStat( path ):
    """ Get the file stat information
    """
    resultDict = {}
    try:
      statTuple = os.stat( path )
    except OSError, x:
      if str( x ).find( 'No such file' ) >= 0:
        resultDict['Exists'] = False
        return S_OK( resultDict )
      else:
        return S_ERROR( 'Failed to get metadata for %s' % path )

    resultDict['Exists'] = True
    mode = statTuple[ST_MODE]
    resultDict['Type'] = "File"
    if S_ISDIR( mode ):
      resultDict['Type'] = "Directory"
    resultDict['Size'] = statTuple[ST_SIZE]
    resultDict['TimeStamps'] = ( statTuple[ST_ATIME], statTuple[ST_MTIME], statTuple[ST_CTIME] )
    resultDict['Cached'] = 1
    resultDict['Migrated'] = 0
    resultDict['Lost'] = 0
    resultDict['Unavailable'] = 0
    resultDict['Mode'] = S_IMODE( mode )
    return S_OK( resultDict )

  types_exists = [StringTypes]
  def export_exists( self, fileID ):
    """ Check existance of the fileID """
    if os.path.exists( self.__resolveFileID( fileID ) ):
      return S_OK( True )
    return S_OK( False )

  types_getMetadata = [StringType]
  def export_getMetadata( self, fileID ):
    """ Get metadata for the file or directory specified by fileID
    """
    return self.__getFileStat( self.__resolveFileID( fileID ) )

  types_createDirectory = [StringType]
  def export_createDirectory( self, dir_path ):
    """ Creates the directory on the storage
    """
    path = self.__resolveFileID( dir_path )
    gLogger.info( "StorageElementHandler.createDirectory: Attempting to create %s." % path )
    if os.path.exists( path ):
      if os.path.isfile( path ):
        errStr = "Supplied path exists and is a file"
        gLogger.error( "StorageElementHandler.createDirectory: %s." % errStr, path )
        return S_ERROR( errStr )
      else:
        gLogger.info( "StorageElementHandler.createDirectory: %s already exists." % path )
        return S_OK()
    # Need to think about permissions.
    try:
      os.makedirs( path )
      return S_OK()
    except Exception, x:
      errStr = "Exception creating directory."
      gLogger.error( "StorageElementHandler.createDirectory: %s" % errStr, str( x ) )
      return S_ERROR( errStr )

  types_listDirectory = [StringType, StringType]
  def export_listDirectory( self, dir_path, mode ):
    """ Return the dir_path directory listing
    """
    is_file = False
    path = self.__resolveFileID( dir_path )
    if not os.path.exists( path ):
      return S_ERROR( 'Directory %s does not exist' % dir_path )
    elif os.path.isfile( path ):
      fname = os.path.basename( path )
      is_file = True
    else:
      dirList = os.listdir( path )

    resultDict = {}
    if mode == 'l':
      if is_file:
        result = self.__getFileStat( fname )
        if result['OK']:
          resultDict[fname] = result['Value']
          return S_OK( resultDict )
        else:
          return S_ERROR( 'Failed to get the file stat info' )
      else:
        failed_list = []
        one_OK = False
        for fname in dirList:
          result = self.__getFileStat( path + '/' + fname )
          if result['OK']:
            resultDict[fname] = result['Value']
            one_OK = True
          else:
            failed_list.append( fname )
        if failed_list:
          if one_OK:
            result = S_ERROR( 'Failed partially to get the file stat info' )
          else:
            result = S_ERROR( 'Failed to get the file stat info' )
          result['FailedList'] = failed_list
          result['Value'] = resultDict
        else:
          result = S_OK( resultDict )

        return result
    else:
      return S_OK( dirList )

  def transfer_fromClient( self, fileID, token, fileSize, fileHelper ):
    """ Method to receive file from clients.
        fileID is the local file name in the SE.
        fileSize can be Xbytes or -1 if unknown.
        token is used for access rights confirmation.
    """
    if not self.__checkForDiskSpace( BASE_PATH, fileSize ):
      return S_ERROR( 'Not enough disk space' )
    file_path = self.__resolveFileID( fileID )
    if not os.path.exists( os.path.dirname( file_path ) ):
      os.makedirs( os.path.dirname( file_path ) )
    try:
      fd = open( file_path, "wb" )
    except Exception, error:
      return S_ERROR( "Cannot open to write destination file %s: %s" % ( file_path, str( error ) ) )
    if "NoCheckSum" in token:
      fileHelper.disableCheckSum()
    result = fileHelper.networkToDataSink( fd, maxFileSize = ( MAX_STORAGE_SIZE * 1024 * 1024 ) )
    if not result[ 'OK' ]:
      return result
    fd.close()
    return result

  def transfer_toClient( self, fileID, token, fileHelper ):
    """ Method to send files to clients.
        fileID is the local file name in the SE.
        token is used for access rights confirmation.
    """
    file_path = self.__resolveFileID( fileID )
    if "NoCheckSum" in token:
      fileHelper.disableCheckSum()
    result = fileHelper.getFileDescriptor( file_path, 'r' )
    if not result['OK']:
      result = fileHelper.sendEOF()
      # check if the file does not really exist
      if not os.path.exists( file_path ):
        return S_ERROR( 'File %s does not exist' % os.path.basename( file_path ) )
      else:
        return S_ERROR( 'Failed to get file descriptor' )

    fileDescriptor = result['Value']
    result = fileHelper.FDToNetwork( fileDescriptor )
    fileHelper.oFile.close()
    if not result['OK']:
      return S_ERROR( 'Failed to get file ' + fileID )
    else:
      return result

  def transfer_bulkFromClient( self, fileID, token, ignoredSize, fileHelper ):
    """ Receive files packed into a tar archive by the fileHelper logic.
        token is used for access rights confirmation.
    """
    if not self.__checkForDiskSpace( BASE_PATH, 10 * 1024 * 1024 ):
      return S_ERROR( 'Less than 10MB remaining' )
    dirName = fileID.replace( '.bz2', '' ).replace( '.tar', '' )
    dir_path = self.__resolveFileID( dirName )
    res = fileHelper.networkToBulk( dir_path )
    if not res['OK']:
      gLogger.error( 'Failed to receive network to bulk.', res['Message'] )
      return res
    if not os.path.exists( dir_path ):
      return S_ERROR( 'Failed to receive data' )
    try:
      os.chmod( dir_path, 0755 )
    except Exception, error:
      gLogger.exception( 'Could not set permissions of destination directory.', dir_path, error )
    return S_OK()

  def transfer_bulkToClient( self, fileId, token, fileHelper ):
    """ Send directories and files specified in the fileID.
        The fileID string can be a single directory name or a list of
        colon (:) separated file/directory names.
        token is used for access rights confirmation.
    """
    tmpList = fileId.split( ':' )
    tmpList = [ os.path.join( BASE_PATH, x ) for x in tmpList ]
    strippedFiles = []
    compress = False
    for fileID in tmpList:
      if re.search( '.bz2', fileID ):
        fileID = fileID.replace( '.bz2', '' )
        compress = True
      fileID = fileID.replace( '.tar', '' )
      strippedFiles.append( self.__resolveFileID( fileID ) )
    res = fileHelper.bulkToNetwork( strippedFiles, compress = compress )
    if not res['OK']:
      gLogger.error( 'Failed to send bulk to network', res['Message'] )
    return res

  types_remove = [StringType, StringType]
  def export_remove( self, fileID, token ):
    """ Remove fileID from the storage. token is used for access rights confirmation. """
    return self.__removeFile( self.__resolveFileID( fileID ), token )

  def __removeFile( self, fileID, token ):
    """ Remove one file with fileID name from the storage
    """
    filename = self.__resolveFileID( fileID )
    if self.__confirmToken( token, fileID, 'x' ):
      try:
        os.remove( filename )
        return S_OK()
      except OSError, error:
        if str( error ).find( 'No such file' ) >= 0:
          # File does not exist anyway
          return S_OK()
        else:
          return S_ERROR( 'Failed to remove file %s' % fileID )
    else:
      return S_ERROR( 'File removal %s not authorized' % fileID )

  types_getDirectorySize = [StringType]
  def export_getDirectorySize( self, fileID ):
    """ Get the size occupied by the given directory
    """
    dir_path = self.__resolveFileID( fileID )
    if os.path.exists( dir_path ):
      try:
        space = self.__getDirectorySize( dir_path )
        return S_OK( space )
      except Exception, error:
        gLogger.exception( "Exception while getting size of directory", dir_path, error )
        return S_ERROR( "Exception while getting size of directory" )
    else:
      return S_ERROR( "Directory does not exists" )

  types_removeDirectory = [StringType, StringType]
  def export_removeDirectory( self, fileID, token ):
    """ Remove the given directory from the storage
    """

    dir_path = self.__resolveFileID( fileID )
    if not self.__confirmToken( token, fileID, 'x' ):
      return S_ERROR( 'Directory removal %s not authorized' % fileID )
    else:
      if not os.path.exists( dir_path ):
        return S_OK()
      else:
        try:
          shutil.rmtree( dir_path )
          return S_OK()
        except Exception, error:
          gLogger.error( "Failed to remove directory", dir_path )
          gLogger.error( str( error ) )
          return S_ERROR( "Failed to remove directory %s" % dir_path )

  types_removeFileList = [ ListType, StringType ]
  def export_removeFileList( self, fileList, token ):
    """ Remove files in the given list
    """
    failed_list = []
    partial_success = False
    for fileItem in fileList:
      result = self.__removeFile( fileItem, token )
      if not result['OK']:
        failed_list.append( fileItem )
      else:
        partial_success = True

    if not failed_list:
      return S_OK()
    else:
      if partial_success:
        result = S_ERROR( 'Bulk file removal partially failed' )
        result['FailedList'] = failed_list
      else:
        result = S_ERROR( 'Bulk file removal failed' )
      return result

###################################################################

  types_getAdminInfo = []
  @staticmethod
  def export_getAdminInfo():
    """ Send the storage element administration information
    """
    storageDict = {}
    storageDict['BasePath'] = BASE_PATH
    storageDict['MaxCapacity'] = MAX_STORAGE_SIZE
    used_space = getDirectorySize( BASE_PATH )
    stats = os.statvfs( BASE_PATH )
    available_space = ( stats.f_bsize * stats.f_bavail ) / 1024 / 1024
    allowed_space = MAX_STORAGE_SIZE - used_space
    actual_space = min( available_space, allowed_space )
    storageDict['AvailableSpace'] = actual_space
    storageDict['UsedSpace'] = used_space
    return S_OK( storageDict )

  @staticmethod
  def __getDirectorySize( path ):
    """ Get the total size of the given directory in bytes
    """
    comm = "du -sb %s" % path
    result = shellCall( 10, comm )
    if not result['OK'] or result['Value'][0]:
      return 0
    else:
      output = result['Value'][1]
      size = int( output.split()[0] )
      return size
