########################################################################
# $Id$
########################################################################
""" DIRAC DirectoryTree base class """

__RCSID__ = "$Id$"

from DIRAC.DataManagementSystem.DB.FileCatalogComponents.Utilities  import checkArgumentFormat
from DIRAC                                                          import S_OK, S_ERROR, gLogger
import time, threading, os
from types import StringTypes, ListType
import stat

DEBUG = 0

#############################################################################
class DirectoryTreeBase:

  _base_tables = {}
  _base_tables["FC_DirectoryUsage"] = { "Fields":
                                        {
                                          "DirID": "INTEGER NOT NULL",
                                          "SEID": "INTEGER NOT NULL",
                                          "SESize": "BIGINT NOT NULL",
                                          "SEFiles": "BIGINT NOT NULL",
                                          "LastUpdate": "DATETIME NOT NULL"
                                        },
                                        "UniqueIndexes" : {"DirID_SEID": ["DirID","SEID"]},
                                        "Indexes": {
                                                     "DirID": ["DirID"],
                                                     "SEID": ["SEID"]
                                                   }  
                                       }
  _base_tables["FC_DirectoryInfo"] = { "Fields": {
                                                    "DirID": "INTEGER NOT NULL",
                                                    "UID": "SMALLINT UNSIGNED NOT NULL DEFAULT 0",
                                                    "GID": "SMALLINT UNSIGNED NOT NULL DEFAULT 0",
                                                    "CreationDate": "DATETIME",
                                                    "ModificationDate": "DATETIME",
                                                    "Mode": "SMALLINT UNSIGNED NOT NULL DEFAULT 775",
                                                    "Status": "SMALLINT UNSIGNED NOT NULL DEFAULT 0"
                                                  }, 
                                        "PrimaryKey": "DirID"
                                       }
  
  def __init__( self, database = None ):
    self.db = None
    if database is not None:
      self.setDatabase( database )
    self.lock = threading.Lock()
    self.treeTable = ''

  def _getConnection( self, connection ):
    if connection:
      return connection
    res = self.db._getConnection()
    if res['OK']:
      return res['Value']
    gLogger.warn( "Failed to get MySQL connection", res['Message'] )
    return connection

  def getTreeTable( self ):
    """ Get the string of the Directory Tree type
    """
    return self.treeTable
    
  def setDatabase( self, database ):
    self.db = database
    result = self.db._createTables( self._base_tables )
    if not result['OK']:
      gLogger.error( "Failed to create tables", str( self._base_tables.keys() ) )
      return result
    if result['Value']:
      gLogger.info( "Tables created: %s" % ','.join( result['Value'] ) )
    result = self.db._createTables( self._tables )
    if not result['OK']:
      gLogger.error( "Failed to create tables", str( self._tables.keys() ) )
    elif result['Value']:
      gLogger.info( "Tables created: %s" % ','.join( result['Value'] ) )  
    return result

  def makeDir( self, path ):    
    return S_ERROR( 'Should be implemented in a derived class' )
  
  def removeDir( self, path ):    
    return S_ERROR( 'Should be implemented in a derived class' )
  
  def findDir( self, path ):    
    return S_ERROR( 'Should be implemented in a derived class' )
  
  def getChildren( self, path ):    
    return S_ERROR( 'Should be implemented in a derived class' )
    
  def getDirectoryPath( self, path ):    
    return S_ERROR( 'Should be implemented in a derived class' )  

  def getSubdirectoriesByID( self, path, requestString, includeParent ):    
    return S_ERROR( 'Should be implemented in a derived class' )  

  def makeDirectory(self,path,credDict,status=0):
    """Create a new directory. The return value is the dictionary
       containing all the parameters of the newly created directory
    """
    if path[0] != '/':
      return S_ERROR( 'Not an absolute path' )
    # Strip off the trailing slash if necessary
    if len( path ) > 1 and path[-1] == '/':
      path = path[:-1]

    if path == '/':
      # Create the root directory
      l_uid = 0
      l_gid = 0
    else:
      result = self.db.ugManager.getUserAndGroupID( credDict )
      if not result['OK']:
        return result
      ( l_uid, l_gid ) = result['Value']

    dirDict = {}
    result = self.makeDir( path )
    if not result['OK']:
      return result
    dirID = result['Value']
    if result['NewDirectory']:
      req = "INSERT INTO FC_DirectoryInfo (DirID,UID,GID,CreationDate,ModificationDate,Mode,Status) Values "
      req = req + "(%d,%d,%d,UTC_TIMESTAMP(),UTC_TIMESTAMP(),%d,%d)" % ( dirID, l_uid, l_gid, self.db.umask, status )
      result = self.db._update( req )
      if result['OK']:
        resGet = self.getDirectoryParameters( dirID )
        if resGet['OK']:
          dirDict = resGet['Value']
    else:
      return S_OK( dirID )

    if not dirDict:
      self.removeDir( path )
      return S_ERROR( 'Failed to create directory %s' % path )
    return S_OK( dirID )

#####################################################################
  def makeDirectories( self, path, credDict ):
    """Make all the directories recursively in the path. The return value
       is the dictionary containing all the parameters of the newly created
       directory
    """
    result = self.existsDir( path )
    if not result['OK']:
      return result
    result = result['Value']
    if result['Exists']:
      return S_OK( result['DirID'] )

    if path == '/':
      result = self.makeDirectory( path, credDict )
      return result

    parentDir = os.path.dirname( path )
    result = self.existsDir( parentDir )
    if not result['OK']:
      return result
    result = result['Value']
    if result['Exists']:
      result = self.makeDirectory( path, credDict )
    else:
      result = self.makeDirectories( parentDir, credDict )
      if not result['OK']:
        return result
      result = self.makeDirectory( path, credDict )

    return result

#####################################################################
  def exists( self, lfns ):
    successful = {}
    failed = {}
    for lfn in lfns:
      res = self.findDir( lfn )
      if not res['OK']:
        failed[lfn] = res['Message']
      if not res['Value']:
        successful[lfn] = False
      else:
        successful[lfn] = True
    return S_OK( {'Successful':successful, 'Failed':failed} )

  def existsDir( self, path ):
    """ Check the existence of the directory path
    """
    result = self.findDir( path )
    if not result['OK']:
      return result
    if result['Value']:
      result = S_OK( int( result['Value'] ) )
      result['Exists'] = True
    else:
      result = S_OK( 0 )
      result['Exists'] = False

    return result

  #####################################################################
  def isDirectory( self, paths ):
    """ Checking for existence of directories
    """
    dirs = paths.keys()
    successful = {}
    failed = {}
    for dir_ in dirs:
      result = self.existsDir( dir_ )
      if not result['OK']:
        failed[dir_] = result['Message']
      elif result['Value']['Exists']:
        successful[dir_] = True
      else: 
        successful[dir_] = False  
          
    return S_OK({'Successful':successful,'Failed':failed})
  
  #####################################################################
  def createDirectory( self, dirs, credDict ):
    """ Checking for existence of directories
    """
    successful = {}
    failed = {}
    for dir_ in dirs:
      result = self.makeDirectories( dir_, credDict )
      if not result['OK']:
        failed[dir_] = result['Message']
      else: 
        successful[dir_] = True  
          
    return S_OK({'Successful':successful,'Failed':failed}) 

  #####################################################################
  def isEmpty( self, path ):
    """ Find out if the given directory is empty
    """
    # Check if there are subdirectories
    result = self.getChildren( path )
    if not result['OK']:
      return result
    childIDs = result['Value']
    if childIDs:
      return S_OK( False )

    #Check if there are files
    result = self.__getDirID( path )
    if not result['OK']:
      return result
    dirID = result['Value']
    result = self.db.fileManager.getFilesInDirectory( dirID )
    if not result['OK']:
      return result
    files = result['Value']
    if files:
      return S_OK( False )

    return S_OK( True )

#####################################################################
  def removeDirectory( self, dirs, force = False ):
    """Remove an empty directory from the catalog """
    successful = {}
    failed = {}
    
    # Check if requested directories exist in the catalog
    result = self.findDirs( dirs )
    if not result['OK']:
      return result
    dirDict = result['Value']
    for d in dirs:
      if not d in dirDict:
        failed[d] = "Directory does not exist" 
    dirList = dirDict.keys()

    for dir_ in dirList:
      result = self.isEmpty( dir_ )
      if not result['OK']:
        return result
      if not result['Value']:
        failed[dir_] = 'Failed to remove non-empty directory'
        continue
      result = self.removeDir(dir_)
      if not result['OK']:
        failed[dir_] = result['Message']
      else: 
        successful[dir_] = result
    return S_OK({'Successful':successful,'Failed':failed}) 

#####################################################################
  def __getDirID( self, path ):
    """ Get directory ID from the given path or already evaluated ID
    """

    if type( path ) in StringTypes:
      result = self.findDir( path )
      if not result['OK']:
        return result
      dirID = result['Value']
      if not dirID:
        return S_ERROR( '%s: not found' % str( path ) )
      return S_OK( dirID )
    else:
      return S_OK( path )

#####################################################################
  def getDirectoryParameters( self, path ):
    """ Get the given directory parameters
    """

    result = self.__getDirID( path )
    if not result['OK']:
      return result
    dirID = result['Value']

    query = "SELECT DirID,UID,GID,Status,Mode,CreationDate,ModificationDate from FC_DirectoryInfo"
    query = query + " WHERE DirID=%d" % dirID
    resQuery = self.db._query( query )
    if not resQuery['OK']:
      return resQuery

    if not resQuery['Value']:
      return S_ERROR( 'Directory not found' )

    dirDict = {}
    dirDict['DirID'] = int( resQuery['Value'][0][0] )
    uid = int( resQuery['Value'][0][1] )
    dirDict['UID'] = uid
    owner = 'unknown'
    result = self.db.ugManager.getUserName( uid )
    if result['OK']:
      owner = result['Value']
    dirDict['Owner'] = owner
    gid = int( resQuery['Value'][0][2] )
    dirDict['GID'] = int( resQuery['Value'][0][2] )
    group = 'unknown'
    result = self.db.ugManager.getGroupName( gid )
    if result['OK']:
      group = result['Value']
    dirDict['OwnerGroup'] = group
    dirDict['Status'] = int( resQuery['Value'][0][3] )
    dirDict['Mode'] = int( resQuery['Value'][0][4] )
    dirDict['CreationDate'] = resQuery['Value'][0][5]
    dirDict['ModificationDate'] = resQuery['Value'][0][6]

    return S_OK( dirDict )

#####################################################################
  def __setDirectoryParameter( self, path, pname, pvalue ):
    """ Set a numerical directory parameter
    """
    result = self.__getDirID( path )
    if not result['OK']:
      return result
    dirID = result['Value']
    req = "UPDATE FC_DirectoryInfo SET %s=%d WHERE DirID=%d" % ( pname, pvalue, dirID )
    result = self.db._update( req )
    return result

#####################################################################
  def _setDirectoryUid( self, path, uid ):
    """ Set the directory owner
    """
    return self.__setDirectoryParameter( path, 'UID', uid )

#####################################################################
  def _setDirectoryGid( self, path, gid ):
    """ Set the directory group
    """
    return self.__setDirectoryParameter( path, 'GID', gid )

#####################################################################
  def setDirectoryOwner( self, path, owner ):
    """ Set the directory owner
    """

    result = self.__getDirID( path )
    if not result['OK']:
      return result
    dirID = result['Value']
    result = self.db.ugManager.findUser( owner )
    uid = result['Value']
    result = self._setDirectoryUid( dirID, uid )
    return result

#####################################################################
  def changeDirectoryOwner( self, paths, s_uid = 0, s_gid = 0 ):
    """ Bulk setting of the directory owner
    """
    result = self.db.ugManager.findUser( s_uid )
    if not result['OK']:
      return result
    uid = result['Value']
    result = self.db.ugManager.findGroup( s_gid )
    if not result['OK']:
      return result
    gid = result['Value']

    result = checkArgumentFormat( paths )
    if not result['OK']:
      return result
    arguments = result['Value']
    successful = {}
    failed = {}
    for path,dict_ in arguments.items():
      owner = dict_['Owner']
      result = self.setDirectoryOwner(path,owner)
      if not result['OK']:
        failed[path] = result['Message']
      else:
        successful[path] = True

    return S_OK( {'Successful':successful, 'Failed':failed} )

#####################################################################
  def setDirectoryGroup( self, path, gname ):
    """ Set the directory owner
    """

    result = self.__getDirID( path )
    if not result['OK']:
      return result
    dirID = result['Value']
    result = self.db.ugManager.findGroup( gname )
    gid = result['Value']
    result = self._setDirectoryGid( dirID, gid )
    return result

#####################################################################
  def changeDirectoryGroup( self, paths, s_uid = 0, s_gid = 0 ):
    """ Bulk setting of the directory owner
    """
    result = self.db.ugManager.findUser( s_uid )
    if not result['OK']:
      return result
    uid = result['Value']
    result = self.db.ugManager.findGroup( s_gid )
    if not result['OK']:
      return result
    gid = result['Value']

    result = checkArgumentFormat( paths )
    if not result['OK']:
      return result
    arguments = result['Value']
    successful = {}
    failed = {}
    for path, dict_ in arguments.items():
      group = dict_['Group']
      result = self.setDirectoryGroup( path, group )
      if not result['OK']:
        failed[path] = result['Message']
      else:
        successful[path] = True

    return S_OK( {'Successful':successful, 'Failed':failed} )

#####################################################################
  def setDirectoryMode( self, path, mode ):
    """ set the directory mask
    """
    return self.__setDirectoryParameter( path, 'Mode', mode )

#####################################################################
  def changeDirectoryMode( self, paths, s_uid = 0, s_gid = 0 ):
    """ Bulk setting of the directory owner
    """
    result = self.db.ugManager.findUser( s_uid )
    if not result['OK']:
      return result
    uid = result['Value']
    result = self.db.ugManager.findGroup( s_gid )
    if not result['OK']:
      return result
    gid = result['Value']

    result = checkArgumentFormat( paths )
    if not result['OK']:
      return result
    arguments = result['Value']
    successful = {}
    failed = {}
    for path, dict_ in arguments.items():
      mode = dict_['Mode']
      result = self.setDirectoryMode( path, mode )
      if not result['OK']:
        failed[path] = result['Message']
      else:
        successful[path] = True

    return S_OK( {'Successful':successful, 'Failed':failed} )

  #####################################################################
  def setDirectoryStatus( self, path, status ):
    """ set the directory mask
    """
    return self.__setDirectoryParameter( path, 'Status', status )

  def getPathPermissions( self, lfns, credDict ):
    """ Get permissions for the given user/group to manipulate the given lfns 
    """
    successful = {}
    failed = {}
    for path in lfns:
      result = self.getDirectoryPermissions( path, credDict )
      if not result['OK']:
        failed[path] = result['Message']
      else:
        successful[path] = result['Value']

    return S_OK( {'Successful':successful, 'Failed':failed} )

  #####################################################################
  def getDirectoryPermissions( self, path, credDict ):
    """ Get permissions for the given user/group to manipulate the given directory 
    """
    result = self.db.ugManager.getUserAndGroupID( credDict )
    if not result['OK']:
      return result
    uid, gid = result['Value']

    result = self.getDirectoryParameters( path )
    if not result['OK']:
      if "not found" in result['Message']:
        # If the directory does not exist, check the nearest parent for the permissions
        if path == '/':
          # Nothing yet exists, starting from the scratch
          resultDict = {}  
          resultDict['Write'] = True
          resultDict['Read'] = True 
          resultDict['Execute'] = True
          return S_OK( resultDict )
        else:
          pDir = os.path.dirname( path )
          result = self.getDirectoryPermissions( pDir, credDict )
          return result
      else:
        return result

    dUid = result['Value']['UID']
    dGid = result['Value']['GID']
    mode = result['Value']['Mode']

    owner = uid == dUid
    group = gid == dGid

    resultDict = {}
    if self.db.globalReadAccess:
      resultDict['Read'] = True
    else:
      resultDict['Read'] = ( owner and mode & stat.S_IRUSR > 0 ) or ( group and mode & stat.S_IRGRP > 0 ) or mode & stat.S_IROTH > 0
    resultDict['Write'] = ( owner and mode & stat.S_IWUSR > 0 ) or ( group and mode & stat.S_IWGRP > 0 ) or mode & stat.S_IWOTH > 0
    resultDict['Execute'] = ( owner and mode & stat.S_IXUSR > 0 ) or ( group and mode & stat.S_IXGRP > 0 ) or mode & stat.S_IXOTH > 0
    return S_OK( resultDict )

  def getFileIDsInDirectory( self, dirID, credDict, startItem = 1, maxItems = 25 ):
    """ Get file IDs for the given directory
    """
    dirs = dirID
    if type( dirID ) != ListType:
      dirs = [dirID]

    if not dirs:
      dirs = [ -1 ]

    dirListString = ','.join( [ str( dir_ ) for dir_ in dirs ] )

    req = "SELECT COUNT( DirID ) FROM FC_Files USE INDEX (DirID) WHERE DirID IN ( %s )" % dirListString
    result = self.db._query( req )
    if not result['OK']:
      return result

    totalRecords = result['Value'][0][0]

    if not totalRecords:
      result = S_OK( [] )
      result['TotalRecords'] = totalRecords
      return result

    req = "SELECT FileID FROM FC_Files WHERE DirID IN ( %s ) LIMIT %s, %s " % ( dirListString, startItem, maxItems )
    result = self.db._query( req )
    if not result['OK']:
      return result
    result = S_OK( [ fileId[0] for fileId in result['Value'] ] )
    result['TotalRecords'] = totalRecords
    return result


  def getFilesInDirectory( self, dirID, credDict ):
    """ Get file IDs for the given directory
    """
    dirs = dirID
    if type( dirID ) != ListType:
      dirs = [dirID]
      
    dirListString = ','.join( [ str(dir_) for dir_ in dirs ] )
    req = "SELECT FileID,DirID,FileName FROM FC_Files WHERE DirID IN ( %s )" % dirListString
    result = self.db._query( req )
    return result

  def getFileLFNsInDirectory( self, dirID, credDict ):
    """ Get file lfns for the given directory or directory list 
    """
    dirs = dirID
    if type( dirID ) != ListType:
      dirs = [dirID]
      
    dirListString = ','.join( [ str(dir_) for dir_ in dirs ] )
    treeTable = self.getTreeTable()
    req = "SELECT CONCAT(D.DirName,'/',F.FileName) FROM FC_Files as F, %s as D WHERE D.DirID IN ( %s ) and D.DirID=F.DirID"
    req = req % ( treeTable, dirListString )
    result = self.db._query( req )
    if not result['OK']:
      return result
    lfnList = [ x[0] for x in result['Value'] ]
    return S_OK( lfnList )

  def getFileLFNsInDirectoryByDirectory( self, dirID, credDict ):
    """ Get file lfns for the given directory or directory list 
    """
    dirs = dirID
    if type( dirID ) != ListType:
      dirs = [dirID]

    dirListString = ','.join( [ str( dir_ ) for dir_ in dirs ] )
    treeTable = self.getTreeTable()
    req = "SELECT D.DirName,F.FileName,F.FileID FROM FC_Files as F, %s as D WHERE D.DirID IN ( %s ) and D.DirID=F.DirID"
    req = req % ( treeTable, dirListString )
    result = self.db._query( req )
    if not result['OK']:
      return result

    lfnDict = {}
    lfnIDList = []
    for dir_, fname, fileID in result['Value']:
      lfnDict.setdefault( dir_, [] )
      lfnDict[dir_].append( fname )
      lfnIDList.append( fileID )

    result = S_OK( lfnDict )
    result['LFNIDList'] = lfnIDList
    return result

  def __getDirectoryContents( self, path, details = False ):
    """ Get contents of a given directory
    """
    result = self.findDir( path )
    if not result['OK']:
      return result
    directoryID = result['Value']
    directories = {}
    links = {}
    result = self.getChildren( path )
    if not result['OK']:
      return result

    # Get subdirectories
    dirIDList = result['Value']
    for dirID in dirIDList:
      result = self.getDirectoryPath( dirID )
      if not result['OK']:
        return result
      dirName = result['Value']
      if details:
        result = self.getDirectoryParameters( dirID )
        if not result['OK']:
          directories[dirName] = False
        else:
          directories[dirName] = result['Value']
      else:
        directories[dirName] = True
    result = self.db.fileManager.getFilesInDirectory( directoryID, verbose = details )
    if not result['OK']:
      return result
    files = result['Value']
    result = self.db.datasetManager.getDatasetsInDirectory( directoryID, verbose = details )
    if not result['OK']:
      return result
    datasets = result['Value']
    pathDict = {'Files': files, 'SubDirs':directories, 'Links':links, 'Datasets':datasets }

    return S_OK( pathDict )

  def listDirectory( self, lfns, verbose = False ):
    """ Get the directory listing
    """
    paths = lfns.keys()
    successful = {}
    failed = {}
    for path in paths:
      result = self.__getDirectoryContents( path, details = verbose )
      if not result['OK']:
        failed[path] = result['Message']
      else:
        successful[path] = result['Value']

    return S_OK( {'Successful':successful, 'Failed':failed} )
  
  def getDirectoryReplicas( self, lfns, allStatus = False ):
    """ Get replicas for files in the given directories
    """
    paths = lfns.keys()
    successful = {}
    failed = {}
    for path in paths:
      result = self.findDir( path )
      if not result['OK']:
        failed[path] = result['Message']
        continue
      directoryID = result['Value']
      result = self.db.fileManager.getDirectoryReplicas( directoryID, path, allStatus )
      if not result['OK']:
        failed[path] = result['Message']
        continue
      fileDict = result['Value']
      successful[path] = {} 
      for fileName in fileDict:
        successful[path][fileName] = fileDict[fileName]  
        
    result = S_OK( {'Successful':successful, 'Failed':failed} )
    
    if self.db.lfnPfnConvention:
      sePrefixDict = {}
      resSE = self.db.seManager.getSEPrefixes()
      if resSE['OK']:
        sePrefixDict = resSE['Value']
      result['Value']['SEPrefixes'] = sePrefixDict
      
    return result

  def getDirectorySize( self, lfns, longOutput = False, rawFileTables = False ):
    """ Get the total size of the requested directories. If long flag
        is True, get also physical size per Storage Element
    """
    start = time.time()

    result = self.db._getConnection()
    if not result['OK']:
      return result
    connection = result['Value']

    if rawFileTables:
      resultLogical = self._getDirectoryLogicalSize( lfns, connection )
    else:
      resultLogical = self._getDirectoryLogicalSizeFromUsage( lfns, connection )
    if not resultLogical['OK']:
      connection.close()
      return resultLogical

    resultDict = resultLogical['Value']
    if not resultDict['Successful']:
      connection.close()
      return resultLogical

    if longOutput:
      # Continue with only successful directories
      if rawFileTables:
        resultPhysical = self._getDirectoryPhysicalSize( resultDict['Successful'], connection )
      else:
        resultPhysical = self._getDirectoryPhysicalSizeFromUsage( resultDict['Successful'], connection )
      if not resultPhysical['OK']:
        resultDict['QueryTime'] = time.time() - start
        result = S_OK( resultDict )
        result['Message'] = "Failed to get the physical size on storage"
        connection.close()
        return result
      for lfn in resultPhysical['Value']['Successful']:
        resultDict['Successful'][lfn]['PhysicalSize'] = resultPhysical['Value']['Successful'][lfn]
    connection.close()
    resultDict['QueryTime'] = time.time() - start
    return S_OK( resultDict )

  def _getDirectoryLogicalSizeFromUsage( self, lfns, connection ):
    """ Get the total "logical" size of the requested directories
    """
    paths = lfns.keys()
    successful = {}
    failed = {}
    for path in paths:
      result = self.findDir( path )
      if not result['OK']:
        failed[path] = "Directory not found"
        continue
      if not result['Value']:
        failed[path] = "Directory not found"
        continue
      dirID = result['Value']
      result = self.getSubdirectoriesByID( dirID, requestString = True, includeParent = True )
      if not result['OK']:
        failed[path] = result['Message']
        continue
      else:
        dirString = result['Value']
        req = "SELECT SESize, SEFiles FROM FC_DirectoryUsage WHERE SEID=0 AND DirID=%d" % dirID
        reqDir = dirString.replace( 'SELECT DirID FROM', 'SELECT count(*) FROM' )

      result = self.db._query( req, connection )
      if not result['OK']:
        failed[path] = result['Message']
      elif not result['Value']:
        successful[path] = {"LogicalSize":0, "LogicalFiles":0, 'LogicalDirectories':0}
      elif result['Value'][0][0]:
        successful[path] = {"LogicalSize":int( result['Value'][0][0] ),
                            "LogicalFiles":int( result['Value'][0][1] )}
        result = self.db._query( reqDir, connection )
        if result['OK'] and result['Value']:
          successful[path]['LogicalDirectories'] = result['Value'][0][0] - 1
        else:
          successful[path]['LogicalDirectories'] = -1


      else:
        successful[path] = {"LogicalSize":0, "LogicalFiles":0, 'LogicalDirectories':0}

    return S_OK( {'Successful':successful, 'Failed':failed} )


  def _getDirectoryLogicalSize( self, lfns, connection ):
    """ Get the total "logical" size of the requested directories
    """
    paths = lfns.keys()
    successful = {}
    failed = {}
    treeTable = self.getTreeTable()
    for path in paths:

      if path == "/":
        req = "SELECT SUM(Size),COUNT(*) FROM FC_Files"
        reqDir = "SELECT count(*) FROM %s" % treeTable
      else:
        result = self.findDir( path )
        if not result['OK']:
          failed[path] = "Directory not found"
          continue
        if not result['Value']:
          failed[path] = "Directory not found"
          continue
        dirID = result['Value']
        result = self.getSubdirectoriesByID( dirID, requestString = True, includeParent = True )
        if not result['OK']:
          failed[path] = result['Message']
          continue
        else:
          dirString = result['Value']
          req = "SELECT SUM(F.Size),COUNT(*) FROM FC_Files as F JOIN (%s) as T WHERE F.DirID=T.DirID" % dirString
          reqDir = dirString.replace( 'SELECT DirID FROM', 'SELECT count(*) FROM' )

      result = self.db._query( req, connection )
      if not result['OK']:
        failed[path] = result['Message']
      elif not result['Value']:
        successful[path] = {"LogicalSize":0, "LogicalFiles":0, 'LogicalDirectories':0}
      elif result['Value'][0][0]:
        successful[path] = {"LogicalSize":int( result['Value'][0][0] ),
                            "LogicalFiles":int( result['Value'][0][1] )}
        result = self.db._query( reqDir, connection )
        if result['OK'] and result['Value']:
          successful[path]['LogicalDirectories'] = result['Value'][0][0] - 1
        else:
          successful[path]['LogicalDirectories'] = -1


      else:
        successful[path] = {"LogicalSize":0, "LogicalFiles":0, 'LogicalDirectories':0}

    return S_OK( {'Successful':successful, 'Failed':failed} )

  def _getDirectoryPhysicalSizeFromUsage( self, lfns, connection ):
    """ Get the total size of the requested directories
    """
    paths = lfns.keys()
    successful = {}
    failed = {}
    for path in paths:
      result = self.findDir( path )
      if not result['OK']:
        failed[path] = "Directory not found"
        continue
      if not result['Value']:
        failed[path] = "Directory not found"
        continue
      dirID = result['Value']

      req = "SELECT S.SEID, S.SEName, D.SESize, D.SEFiles FROM FC_DirectoryUsage as D, FC_StorageElements as S"
      req += "  WHERE S.SEID=D.SEID AND D.DirID=%d" % dirID
      result = self.db._query( req, connection )
      if not result['OK']:
        failed[path] = result['Message']
      elif not result['Value']:
        successful[path] = {}
      elif result['Value'][0][0]:
        seDict = {}
        totalSize = 0
        totalFiles = 0
        for seID, seName, seSize, seFiles in result['Value']:
          if seSize or seFiles:
            seDict[seName] = {'Size':seSize, 'Files':seFiles}
            totalSize += seSize
            totalFiles += seFiles
          else:
            req = 'DELETE FROM FC_DirectoryUsage WHERE SEID=%d AND DirID=%d' % ( seID, dirID )
            result = self.db._update( req )
            if not result['OK']:
              gLogger.error( 'Failed to delete entry from FC_DirectoryUsage', result['Message'] )
        seDict['TotalSize'] = int( totalSize )
        seDict['TotalFiles'] = int( totalFiles )
        successful[path] = seDict
      else:
        successful[path] = {}

    return S_OK( {'Successful':successful, 'Failed':failed} )


  def _getDirectoryPhysicalSizeFromUsage_old( self, lfns, connection ):
    """ Get the total size of the requested directories
    """
    paths = lfns.keys()
    successful = {}
    failed = {}
    for path in paths:

      if path == '/':
        req = "SELECT S.SEName, D.SESize, D.SEFiles FROM FC_DirectoryUsage as D, FC_StorageElements as S"
        req += "  WHERE S.SEID=D.SEID"
      else:
        result = self.findDir( path )
        if not result['OK']:
          failed[path] = "Directory not found"
          continue
        if not result['Value']:
          failed[path] = "Directory not found"
          continue
        dirID = result['Value']
        result = self.getSubdirectoriesByID( dirID, requestString = True, includeParent = True )
        if not result['OK']:
          return result
        subDirString = result['Value']
        req = "SELECT S.SEName, D.SESize, D.SEFiles FROM FC_DirectoryUsage as D, FC_StorageElements as S"
        req += " JOIN (%s) AS F" % subDirString
        req += " WHERE S.SEID=D.SEID AND D.DirID=F.DirID"

      result = self.db._query( req, connection )
      if not result['OK']:
        failed[path] = result['Message']
      elif not result['Value']:
        successful[path] = {}
      elif result['Value'][0][0]:
        seDict = {}
        totalSize = 0
        totalFiles = 0
        for seName, seSize, seFiles in result['Value']:
          sfDict = seDict.get( seName, {'Size':0, 'Files':0} )
          sfDict['Size'] += seSize
          sfDict['Files'] += seFiles
          seDict[seName] = sfDict
          totalSize += seSize
          totalFiles += seFiles
        seDict['TotalSize'] = int( totalSize )
        seDict['TotalFiles'] = int( totalFiles )
        successful[path] = seDict
      else:
        successful[path] = {}

    return S_OK( {'Successful':successful, 'Failed':failed} )

  def _getDirectoryPhysicalSize( self, lfns, connection ):
    """ Get the total size of the requested directories
    """
    paths = lfns.keys()
    successful = {}
    failed = {}
    for path in paths:
      if path == '/':
        req = "SELECT SUM(F.Size),COUNT(F.Size),S.SEName from FC_Files as F, FC_Replicas as R, FC_StorageElements as S "
        req += "WHERE R.SEID=S.SEID AND F.FileID=R.FileID "
        req += "GROUP BY S.SEID"
      else:
        result = self.findDir( path )
        if not result['OK']:
          failed[path] = "Directory not found"
          continue
        if not result['Value']:
          failed[path] = "Directory not found"
          continue
        dirID = result['Value']
        result = self.getSubdirectoriesByID( dirID, requestString = True, includeParent = True )
        if not result['OK']:
          failed[path] = result['Message']
          continue
        else:
          dirString = result['Value']

          req = "SELECT SUM(F.Size),COUNT(F.Size),S.SEName from FC_Files as F, FC_Replicas as R, FC_StorageElements as S JOIN (%s) as T " % dirString
          req += "WHERE R.SEID=S.SEID AND F.FileID=R.FileID AND F.DirID=T.DirID "
          req += "GROUP BY S.SEID"

      result = self.db._query( req, connection )
      if not result['OK']:
        failed[path] = result['Message']
      elif not result['Value']:
        successful[path] = {}
      elif result['Value'][0][0]:
        seDict = {}
        totalSize = 0
        totalFiles = 0
        for size, files, seName in result['Value']:
          seDict[seName] = {"Size":int( size ), "Files":int( files )}
          totalSize += size
          totalFiles += files
        seDict['TotalSize'] = int( totalSize )
        seDict['TotalFiles'] = int( totalFiles )
        successful[path] = seDict
      else:
        successful[path] = {}

    return S_OK( {'Successful':successful, 'Failed':failed} )

  def _rebuildDirectoryUsage( self ):
    """ Recreate and replenish the Storage Usage tables
    """

    req = "DROP TABLE IF EXISTS FC_DirectoryUsage_backup"
    self.db._update( req )
    req = "RENAME TABLE FC_DirectoryUsage TO FC_DirectoryUsage_backup"
    self.db._update( req )
    
    tableDict = { "FC_DirectoryUsage": self._base_tables['FC_DirectoryUsage'] }
    result = self.db._createTables( tableDict )
    if not result['OK']:
      return result

    result = self.__rebuildDirectoryUsageLeaves()
    if not result['OK']:
      return result

    result = self.db.dtree.findDir( '/' )
    if not result['OK']:
      return result
    if not result['Value']:
      return S_ERROR( 'Directory / not found' )
    dirID = result['Value']
    result = self.__rebuildDirectoryUsage( dirID )
    gLogger.verbose( 'Finished rebuilding Directory Usage' )
    return result

  def __rebuildDirectoryUsageLeaves( self ):
    """ Rebuild DirectoryUsage entries for directories having files
    """
    req = 'SELECT DISTINCT(DirID) FROM FC_Files'
    result = self.db._query( req )
    if not result['OK']:
      return result

    dirIDs = [ x[0] for x in result['Value'] ]
    gLogger.verbose( 'Starting rebuilding Directory Usage, number of visible directories %d' % len( dirIDs ) )

    insertFields = ['DirID', 'SEID', 'SESize', 'SEFiles', 'LastUpdate']
    insertValues = []

    count = 0
    empty = 0

    for dirID in dirIDs:

      count += 1

      # Get the physical size
      req = "SELECT SUM(F.Size),COUNT(F.Size),R.SEID from FC_Files as F, FC_Replicas as R "
      req += "WHERE F.FileID=R.FileID AND F.DirID=%d GROUP BY R.SEID" % int( dirID )
      result = self.db._query( req )
      if not result['OK']:
        return result
      if not result['Value']:
        empty += 1

      for seSize, seFiles, seID in result['Value']:
        insertValues = [dirID, seID, seSize, seFiles, 'UTC_TIMESTAMP()']
        result = self.db.insertFields( 'FC_DirectoryUsage', insertFields, insertValues )
        if not result['OK']:
          if "Duplicate" in result['Message']:
            req = "UPDATE FC_DirectoryUsage SET SESize=%d, SEFiles=%d, LastUpdate=UTC_TIMESTAMP()" % ( seSize, seFiles )
            req += " WHERE DirID=%s AND SEID=%s" % ( dirID, seID )
            result = self.db._update( req )
            if not result['OK']:
              return result
          return result

      # Get the logical size
      req = "SELECT SUM(Size),COUNT(Size) from FC_Files WHERE DirID=%d " % int( dirID )
      result = self.db._query( req )
      if not result['OK']:
        return result
      if not result['Value']:
        return S_ERROR( 'Empty directory' )
      seSize, seFiles = result['Value'][0]
      insertValues = [dirID, 0, seSize, seFiles, 'UTC_TIMESTAMP()']
      result = self.db.insertFields( 'FC_DirectoryUsage', insertFields, insertValues )
      if not result['OK']:
        if "Duplicate" in result['Message']:
          req = "UPDATE FC_DirectoryUsage SET SESize=%d, SEFiles=%d, LastUpdate=UTC_TIMESTAMP()" % ( seSize, seFiles )
          req += " WHERE DirID=%s AND SEID=0" % dirID
          result = self.db._update( req )
          if not result['OK']:
            return result
        else:
          return result

    gLogger.verbose( "Processed %d directories, %d empty " % ( count, empty ) )

    return S_OK()

  def __rebuildDirectoryUsage( self, directoryID ):
    """ Rebuild DirectoryUsage entries recursively for the given path
    """
    result = self.getChildren( directoryID )
    if not result['OK']:
      return result
    dirIDs = result['Value']
    resultDict = {}
    for dirID in dirIDs:
      result = self.__rebuildDirectoryUsage( dirID )
      if not result['OK']:
        return result
      dirDict = result['Value']
      for seID in dirDict:
        resultDict.setdefault( seID, {'Size':0, 'Files':0} )
        resultDict[seID]['Size'] += dirDict[seID]['Size']
        resultDict[seID]['Files'] += dirDict[seID]['Files']

    insertFields = ['DirID', 'SEID', 'SESize', 'SEFiles', 'LastUpdate']
    insertValues = []
    for seID in resultDict:
      size = resultDict[seID]['Size']
      files = resultDict[seID]['Files']
      req = "UPDATE FC_DirectoryUsage SET SESize=SESize+%d, SEFiles=SEFiles+%d WHERE DirID=%d AND SEID=%d"
      req = req % ( size, files, directoryID, seID )
      result = self.db._update( req )
      if not result['OK']:
        return result
      if not result['Value']:
        insertValues = [directoryID, seID, size, files, 'UTC_TIMESTAMP()']
        result = self.db.insertFields( 'FC_DirectoryUsage', insertFields, insertValues )
        if not result['OK']:
          return result

    req = "SELECT SEID,SESize,SEFiles from FC_DirectoryUsage WHERE DirID=%d" % directoryID
    result = self.db._query( req )
    if not result['OK']:
      return result

    resultDict = {}
    for seid, size, files in result['Value']:
      resultDict[seid] = {'Size':size, 'Files':files}

    return S_OK( resultDict )

  def getDirectoryCounters( self, connection = False ):
    """ Get the total number of directories
    """
    connection = self._getConnection( connection )
    resultDict = {}
    req = "SELECT COUNT(*) from FC_DirectoryInfo"
    res = self.db._query( req, connection )
    if not res['OK']:
      return res
    resultDict['Directories'] = res['Value'][0][0]

    treeTable = self.getTreeTable()

    req = "SELECT COUNT(DirID) FROM %s WHERE Parent NOT IN ( SELECT DirID from %s )" % ( treeTable, treeTable )
    req += " AND DirID <> 1"
    res = self.db._query( req, connection )
    if not res['OK']:
      return res
    resultDict['Orphan Directories'] = res['Value'][0][0]

    req = "SELECT COUNT(DirID) FROM %s WHERE DirID NOT IN ( SELECT Parent from %s )" % ( treeTable, treeTable )
    req += " AND DirID NOT IN ( SELECT DirID from FC_Files ) "
    res = self.db._query( req, connection )
    if not res['OK']:
      return res
    resultDict['Empty Directories'] = res['Value'][0][0]

    req = "SELECT COUNT(DirID) FROM %s WHERE DirID NOT IN ( SELECT DirID FROM FC_DirectoryInfo )" % treeTable
    res = self.db._query( req, connection )
    if not res['OK']:
      return res
    resultDict['DirTree w/o DirInfo'] = res['Value'][0][0]

    req = "SELECT COUNT(DirID) FROM FC_DirectoryInfo WHERE DirID NOT IN ( SELECT DirID FROM %s )" % treeTable
    res = self.db._query( req, connection )
    if not res['OK']:
      return res
    resultDict['DirInfo w/o DirTree'] = res['Value'][0][0]

    return S_OK( resultDict )
