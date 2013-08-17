########################################################################
# $HeadURL$
# File: IRODSStorageElementHandler.py
########################################################################
""" :mod: IRODSStorageElementHandler
===========================
.. module: IRODSStorageElementHandler
:synopsis: IRODSStorageElementHandler is the implementation of a simple StorageElement  
service with the iRods SE as a backend

The following methods are available in the Service interface

getMetadata() - get file metadata
listDirectory() - get directory listing
remove() - remove one file
removeDirectory() - remove on directory recursively
removeFileList() - remove files in the list
getAdminInfo() - get administration information about the SE status

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
from DIRAC import gLogger, S_OK, S_ERROR, gConfig
from DIRAC.Core.DISET.RequestHandler import RequestHandler

from irods import rcConnect , rcDisconnect , clientLoginWithPassword, \
                  irodsCollection, iRodsOpen, \
                  getResources , getResource

"""
iRods Glossary:

Server: An iRODS Server is software that interacts with the access protocol of a 
specific storage system; enables storing and sharing data distributed geographically 
and across administrative domains.

Resource: A resource, or storage resource, is a software/hardware system that stores 
digital data. Currently iRODS can use a Unix file system as a resource. As other 
iRODS drivers are written, additional types of resources will be included. iRODS 
clients can operate on local or remote data stored on different types of resources, 
through a common interface.

Zone: An iRODS Zone is an independent iRODS system consisting of an iCAT-enabled server, 
optional additional distributed iRODS Servers (which can reach hundreds, worldwide) and 
clients. Each Zone has a unique name. An iRODS Zone can interoperate with other Zones 
in what is called Federation

Collection: All Data Objects stored in an iRODS/iCat system are stored in some Collection, 
which is a logical name for that set of data objects. A Collection can have sub-collections, 
and hence provides a hierarchical structure. An iRODS/iCAT Collection is like a directory 
in a Unix file system (or Folder in Windows), but is not limited to a single device or 
partition. A Collection is logical or so that the data objects can span separate and 
heterogeneous storage devices (i.e. is infrastructure and administrative domain independent). 
Each Data Object in a Collection or sub-collection must have a unique name in that Collection.

Data Object: A Data Object is a single a "stream-of-bytes" entity that can be uniquely 
identified, basically a file stored in iRODS. It is given a Unique Internal Identifier in 
iRODS (allowing a global name space), and is associated with a Collection.

Collection is a directory
DataObject is a file
"""



## TODO: Should it be SE per resource or per iRods server?
IRODS_HOST = None
IRODS_PORT = None
IRODS_USER = None
IRODS_ZONE = None
BASE_PATH = ""
IRODS_HOME = ""
MAX_STORAGE_SIZE = 2048
IRODS_RESOURCE = None


def initializeIRODSStorageElementHandler( serviceInfo ):
  """ Initialize Storage Element global settings
"""
  
  global IRODS_HOST
  global IRODS_PORT
  global IRODS_ZONE
  global BASE_PATH
  global IRODS_HOME
  global IRODS_RESOURCE

  cfgPath = serviceInfo['serviceSectionPath']

  IRODS_HOST = gConfig.getValue( "%s/iRodsServer" % cfgPath , IRODS_HOST )
  if not IRODS_HOST:
    gLogger.error( 'Failed to get iRods server host' )
    return S_ERROR( 'Failed to get iRods server host' )

  IRODS_PORT = gConfig.getValue( "%s/iRodsPort" % cfgPath , IRODS_PORT )
  try:
    IRODS_PORT = int( IRODS_PORT )
  except:
    pass
  if not IRODS_PORT:
    gLogger.error( 'Failed to get iRods server port' )
    return S_ERROR( 'Failed to get iRods server port' )

  IRODS_ZONE = gConfig.getValue( "%s/iRodsZone" % cfgPath , IRODS_ZONE )
  if not IRODS_ZONE:
    gLogger.error( 'Failed to get iRods zone' )
    return S_ERROR( 'Failed to get iRods zone' )

  IRODS_HOME = gConfig.getValue( "%s/iRodsHome" % cfgPath, IRODS_HOME )
  if not IRODS_HOME:
    gLogger.error( 'Failed to get the base path' )
    return S_ERROR( 'Failed to get the base path' )

  IRODS_RESOURCE = gConfig.getValue( "%s/iRodsResource" % cfgPath, IRODS_RESOURCE )

  gLogger.info( 'Starting iRods Storage Element' )
  gLogger.info( 'iRods server: %s' % IRODS_HOST )
  gLogger.info( 'iRods port: %s' % IRODS_PORT )
  gLogger.info( 'iRods zone: %s' % IRODS_ZONE )
  gLogger.info( 'iRods home: %s' % IRODS_HOME )
  gLogger.info( 'iRods resource: %s' % IRODS_RESOURCE )
  return S_OK()

class IRODSStorageElementHandler( RequestHandler ):
  """
.. class:: StorageElementHandler

"""

  def __checkForDiskSpace( self , path , size ):
    """ Check if iRods resource has enough space
"""
## dsize = ( getDiskSpace( dpath ) - 1 ) * 1024 * 1024
## maxStorageSizeBytes = MAX_STORAGE_SIZE * 1024 * 1024
## return ( min( dsize, maxStorageSizeBytes ) > size )
    return True

  def __resolveFileID( self, fileID ):
    """ get path to file for a given :fileID: """
    
    port = self.getCSOption('Port','')
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

  def __changeCollection( self , coll , path ):
    if not len( path ) > 0:
      return coll
    name = path.pop( 0 )
    if not len( name ) > 0:
      return self.__changeCollection( coll , path )
    gLogger.info( 'Check subcollection: %s' % name )
    subs = coll.getSubCollections()
    if not name in subs:
      gLogger.info( 'Create subcollection: %s' % name )
      coll.createCollection( name )
    gLogger.info( 'Open subcollection: %s' % name )
    coll.openCollection( name )
    return self.__changeCollection( coll , path )


  def transfer_fromClient( self, fileID, token, fileSize, fileHelper ):
    """ Method to receive file from clients.
fileID is the local file name in the SE.
fileSize can be Xbytes or -1 if unknown.
token is used for access rights confirmation.
"""

    conn , error = self.__irodsClient( "w" )
    if not conn:
      return S_ERROR( error )
    coll = irodsCollection( conn, IRODS_HOME )

    if not self.__checkForDiskSpace( IRODS_HOME, fileSize ):
      rcDisconnect( conn )
      return S_ERROR( 'Not enough disk space' )

    file_path = self.__resolveFileID( fileID )

    path = file_path.split( "/" )
    file_ = path.pop()

    if len( path ) > 0:
      coll = self.__changeCollection( coll , path )

    file_path = IRODS_HOME + file_path
 
    try:
      if IRODS_RESOURCE:
        fd = coll.create( file_ , IRODS_RESOURCE )
      else:
        fd = coll.create( file_ , "w" )
    except Exception, error:
      rcDisconnect( conn )
      return S_ERROR( "Cannot open to write destination file %s: %s" % ( file_path, str(error) ) )

    result = fileHelper.networkToDataSink( fd, maxFileSize=( MAX_STORAGE_SIZE * 1024 * 1024 ) )
    fd.close()
    rcDisconnect( conn )
    if not result[ 'OK' ]:
      return result
    return result

  def transfer_toClient( self, fileID, token, fileHelper ):
    """ Method to send files to clients.
fileID is the local file name in the SE.
token is used for access rights confirmation.
"""

    conn , error = self.__irodsClient( "r" )
    if not conn:
      return S_ERROR( error )

    file_path = self.__resolveFileID( fileID )
    file_path = IRODS_HOME + file_path
    gLogger.debug( "file_path to read: %s" % file_path )

    fd = iRodsOpen( conn , file_path , "r" )
    if not fd:
      rcDisconnect( conn )
      gLogger.error( "Failed to get file object" )
      return S_ERROR( "Failed to get file object" )

    result = fileHelper.FileToNetwork( fd )
    fd.close()

    rcDisconnect( conn )
    if not result[ "OK" ]:
      gLogger.error( "Failed to get file " + fileID )
      return S_ERROR( "Failed to get file " + fileID )
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
    res = fileHelper.bulkToNetwork( strippedFiles, compress=compress )
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

  def export_getResourceInfo( self , resource = None ):
    """ Send the storage element resource information
"""

    conn , error = self.__irodsClient( "r" )
    if not conn:
      return S_ERROR( error )

    storageDict = {}
    for resource in getResources( conn ):
      name = resource.getName()
      if resource and resource != name:
        continue
      storageDict[ name ] = {}
      storageDict[ name ][ "Id" ] = resource.getId()
      storageDict[ name ][ "AvailableSpace" ] = resource.getFreeSpace()
      storageDict[ name ][ "Zone" ] = resource.getZone()
      storageDict[ name ][ "Type" ] = resource.getTypeName()
      storageDict[ name ][ "Class" ] = resource.getClassName()
      storageDict[ name ][ "Host" ] = resource.getHost()
      storageDict[ name ][ "Path" ] = resource.getPath()
      storageDict[ name ][ "Free Space TS" ] = resource.getFreeSpaceTs()
      storageDict[ name ][ "Info" ] = resource.getInfo()
      storageDict[ name ][ "Comment" ] = resource.getComment()
    rcDisconnect( conn )

    return S_OK( storageDict )


  def __irodsClient( self , user = None ):

    global IRODS_USER
    password = None

    cfgPath = self.serviceInfoDict[ 'serviceSectionPath' ]
    gLogger.debug( "cfgPath: %s" % cfgPath )

    if not user:
      credentials = self.getRemoteCredentials()
      if credentials and ( "username" in credentials ):
        IRODS_USER = credentials[ "username" ]
        ## TODO: should get user password somehow
    elif user == "r":
      IRODS_USER = gConfig.getValue( "%s/read" % cfgPath , IRODS_USER )
    elif user == "w":
      IRODS_USER = gConfig.getValue( "%s/write" % cfgPath , IRODS_USER )

    if not IRODS_USER:
      return False , "Failed to get iRods user"
    gLogger.debug( "iRods user: %s" % IRODS_USER )

    password = gConfig.getValue( "%s/%s" % ( cfgPath , IRODS_USER ) , password )

    conn , errMsg = rcConnect( IRODS_HOST , IRODS_PORT , IRODS_USER , IRODS_ZONE )

    status = clientLoginWithPassword( conn , password )

    if not status == 0:
      return False , "Failed to authenticate user '%s'" % IRODS_USER

    return conn , errMsg
  