from DIRAC import S_OK, S_ERROR
from DIRAC.ConfigurationSystem.Client.Helpers.Registry import getAllGroups, getGroupOption
from DIRAC.DataManagementSystem.DB.FileCatalogComponents.SecurityManager import SecurityManagerBase, __readMethods, __writeMethods

import os

import datetime


class VOMSPolicy( SecurityManagerBase ):
  """ This class implements a 3-level POSIX permission, wrapping up
      the DIRAC group into VOMS roles
  """

  def __init__( self, database = False ):
    super( VOMSPolicy, self ).__init__( database = database )

    # voms role : [dirac groups that have it]
    self.vomsRoles = {}
    # dirac group : voms role it has
    self.diracGroups = {}
    
    # Lifetime of the info in the two dictionaries
    self.CACHE_TIME = datetime.timedelta(seconds = 600)
    self.__buildRolesAndGroups()


  def __buildRolesAndGroups( self ):
    """ Rebuild the cache dictionary for VOMS roles and DIRAC Groups"""

    self.lastBuild = datetime.datetime.now()

    print 'ET LA ??'
    allGroups = getAllGroups()
    

    for grpName in allGroups:
      vomsRole = getGroupOption( grpName, 'VOMSRole' )
      if vomsRole:
        self.diracGroups[grpName] = vomsRole
        self.vomsRoles.setdefault( vomsRole, [] ).append( grpName )

  
  def __getVomsRole(self, grpName):
    """ Returns the VOMS role of a given DIRAC group
        :param grpName

        :returns VOMS role, or None
    """
    if ( datetime.datetime.now() - self.lastBuild ) > self.CACHE_TIME:
      self.__buildRolesAndGroups()

    return self.diracGroups.get( grpName )

  def __getDiracGroups( self, vomsRole ):
    """ Returns all the DIRAC groups that have a given VOMS role
        :param vomsRole
        
        :returns list of groups, empty if not exist
    """

    if ( datetime.datetime.now - self.lastBuild ) > self.CACHE_TIME:
      self.__buildRolesAndGroups()

    return self.vomsRoles.get( vomsRole, [] )

  def __shareVomsRole( self, grpName, otherGrpName ):
    """ Returns True if the two DIRAC groups have the same VOMS role"""

    vomsGrp = self.__getVomsRole( grpName )
    vomsOtherGrp = self.__getVomsRole( otherGrpName )
    # The voms group cannot be none
    return  vomsGrp and vomsOtherGrp and ( vomsGrp == vomsOtherGrp )



  def __isNotExistError( self, errorMsg ):
    """ Returns true if the errorMsg means that the file/directory does not exist """
    return ( ( 'not found' in errorMsg ) or ( 'No such file or directory' in errorMsg ) )





  def __getFilePermission( self, path, credDict, noExistStrategy = None ):
    """ Checks POSIX permission for a file using the VOMS roles.
    """

    # We check what is the group stored in the DB for the given path
    res = self.db.fileManager.getFileMetadata( path )
    if not res['OK']:
      # If the error is not due to the directory not existing, we return
      if not self.__isNotExistError( res['Message'] ):
        return res

      # From now on, we know that the error is due to the file not existing

      # If we have no strategy regarding non existing files, then just return the error
      if noExistStrategy is None:
        return res

      # Finally, follow the strategy
      return dict.fromkeys( ['Read', 'Write', 'Execute'], noExistStrategy )

    origGrp = 'unknown'
    res = self.db.ugManager.getGroupName( res['Value']['GID'] )
    if res['OK']:
      origGrp = res['Value']

    # If the two group share the same voms role, we do the query like if we were
    # the group stored in the DB
    if self.__shareVomsRole( credDict.get( 'group', 'anon' ), origGrp ):
      credDict = { 'username' : credDict.get( 'username', 'anon' ), 'group' : origGrp}

    return  self.db.fileManager.getPathPermissions( path, credDict )


  def __testPermissionOnFile( self, paths, permission, credDict, noExistStrategy = None ):
    successful = {}
    failed = {}

    for filename in paths:
      res = self.__getFilePermission( filename, credDict, noExistStrategy = noExistStrategy )
      if not res['OK']:
        failed[filename] = res['Message']
      else:
        successful[filename] = res['Value'].get( permission, False )

    return S_OK( { 'Successful' : successful, 'Failed' : failed } )



  def __getDirectoryPermission( self, path, credDict, recursive = True, noExistStrategy = None ):
    """ Checks POSIX permission for a directory using the VOMS roles.
    """

    # We check what is the group stored in the DB for the given path
    res = self.db.dtree.getDirectoryParameters( path )
    if not res['OK']:
      # If the error is not due to the directory not existing, we return
      if not self.__isNotExistError( res['Message'] ):
        return res

      # Very special case to allow creation of very first entry
      if path == '/':
        return S_OK( {'Read':True, 'Write':True, 'Execute':True} )

      # From now on, we know that the error is due to the directory not existing

      # If recursive, we try the parent directory
      if recursive:
        return self.__getDirectoryPermission( os.path.dirname( path ), credDict,
                                             recursive = recursive, noExistStrategy = noExistStrategy )
      # From now on, we know we don't run recursive

      # If we have no strategy regarding non existing files, then just return the error
      if noExistStrategy is None:
        return res

      # Finally, follow the strategy
      return dict.fromkeys( ['Read', 'Write', 'Execute'], noExistStrategy )

    # The directory exists.
    origGrp = res['Value']['OwnerGroup']

    # If the two group share the same voms role, we do the query like if we were
    # the group stored in the DB
    if self.__shareVomsRole( credDict.get( 'group', 'anon' ), origGrp ):
      credDict = { 'username' : credDict.get( 'username', 'anon' ), 'group' : origGrp}

    return  self.db.dtree.getDirectoryPermissions( path, credDict )


#   def __getRecursiveDirectoryPermissions( self, path, credDict ):
#     """ Get path permissions according to the policy
#         If the directory does not exist, then we check the parent directory
#
#     """
#
#     res = self.__getDirectoryPermission( path, credDict )
#     if res['OK']:
#       return res
#
#     if self.__isNotExistError( res['Message'] ):
#       if path == '/':
#         return S_OK( {'Read':True, 'Write':True, 'Execute':True} )
#
#       return self.__getRecursiveDirectoryPermissions( os.path.dirname( path ), credDict )
#
#     return res



  def __testPermissionOnDirectory( self, paths, permission, credDict, recursive = True, noExistStrategy = None ):
    successful = {}
    failed = {}

    for dirName in paths:
      res = self.__getDirectoryPermission( dirName, credDict, recursive = recursive, noExistStrategy = noExistStrategy )
      if not res['OK']:
        failed[dirName] = res['Message']
      else:
        successful[dirName] = res['Value'].get( permission, False )
        
    return S_OK( { 'Successful' : successful, 'Failed' : failed } )


  def __testPermissionOnParentDirectory( self, paths, permission, credDict, recursive = True, noExistStrategy = None ):
    """ For a list of paths, checks whether we have write permissions on the parents
        directories
    """


    parentDirs = {}
    for dirName in paths:
      parentDirs.setdefault( os.path.dirname( dirName ), [] ).append( dirName )

    res = self.__testPermissionOnDirectory( parentDirs, permission, credDict,
                                            recursive = recursive, noExistStrategy = noExistStrategy )
    if not res['OK']:
      return res
    
    failed = res['Value']['Failed']
    successful = {}

    parentAllowed = res['Value']['Successful']
    
    for parentName in parentAllowed:
      isParentAllowed = parentAllowed[parentName]
      for dirName in parentDirs[parentName]:
        successful[dirName] = isParentAllowed

    return S_OK( { 'Successful' : successful, 'Failed' : failed } )



  def __getFileOrDirectoryPermission( self, path, credDict, recursive = False, noExistStrategy = None ):
    
    #First consider it as File
    # We want to know whether the file does not exist, so we force noExistStrategy to None
    res = self.__getFilePermission( path, credDict, noExistStrategy = None )
    if not res['OK']:
      # If the error is not due to the directory not existing, we return
      if not self.__isNotExistError( res['Message'] ):
        return res

      # From now on, we know that the error is due to the File not existing
      # We Try then the directory method, since path can be a directory
      # The noExistStrategy will be applied by __getDirectoryPermission, so we don't need to do it ourselves
      res = self.__getDirectoryPermission( path, credDict, recursive = recursive, noExistStrategy = noExistStrategy )

    return res


  
  
  def __policyDefaultModifyDirectory( self, paths, credDict ):
    """ Test write permission on the directory. If it does not
        exist, go up the hierarchy
    """

    return self.__testPermissionOnDirectory( paths, 'Write', credDict,
                                             recursive = False, noExistStrategy = False )
  
  def __policyCreateDirectory(self, paths, credDict):
    """ Tests whether the creation operation on directories
        is permitted.
        For that, we need the Write permission on the parent directory

        :param paths : list/dict of path
        :credDict : credential of the user
    """

    return self.__testPermissionOnParentDirectory( paths, 'Write', credDict, recursive = True )

  def __policyRemoveDirectory( self, paths, credDict ):
    """ Tests whether the remove operation on directories
        is permitted.
        Removal of non existing directory is always allowed.

        :param paths : list/dict of path
        :credDict : credential of the user
    """

    successful = {}

    # We allow removal of all the non existing directories
    res = self.db.dtree.exists( paths )
    if not res['OK']:
      return res

    nonExistingDirectories = set( path for path in res['Value']['Successful'] if not res['Value']['Successful'][path] )

    for dirName in nonExistingDirectories:
      successful[dirName] = True
      try:
        paths.remove( dirName )
      except Exception, _e:
        print _e
        pass
      
    res = self.__testPermissionOnParentDirectory( paths, 'Write', credDict, recursive = False )
    if not res['OK']:
      return res
    
    failed = res['Value']['Failed'] 
    successful.update(res['Value']['Successful'])

    return S_OK( { 'Successful' : successful, 'Failed' : failed } )




# __readMethods = ['exists','isFile','getFileSize','getFileMetadata',
#                'getReplicas','getReplicaStatus','getFileAncestors',
#                'getFileDescendents','listDirectory','isDirectory',
#                'getDirectoryReplicas','getDirectorySize']
# 
# __writeMethods = ['changePathOwner','changePathGroup','changePathMode',
#                 'addFile','setFileStatus','removeFile','addReplica',
#                 'removeReplica','setReplicaStatus','setReplicaHost',
#                 'setFileOwner','setFileGroup','setFileMode',
#                 'addFileAncestors',
#                 'setMetadata','__removeMetadata']

  def hasAccess( self, opType, paths, credDict ):

    # Check if admin access is granted first
    result = self.hasAdminAccess( credDict )
    if not result['OK']:
      return result
    if result['Value']:
      # We are admins, allow everything
      permissions = {}
      for path in paths:
        permissions[path] = True
      return S_OK( {'Successful':permissions, 'Failed':{}} )

    successful = {}


#     if not opType.lower() in ['read', 'write', 'execute']:
#       return S_ERROR( "Operation type not known" )


    if self.db.globalReadAccess and ( opType in __readMethods ):
      for path in paths:
        successful[path] = True
      resDict = {'Successful':successful, 'Failed':{}}
      return S_OK( resDict )

    policyToExecute = None

    if opType == 'removeDirectory':
      policyToExecute = self.__policyRemoveDirectory
    elif opType == 'createDirectory':
      policyToExecute = self.__policyCreateDirectory

    res = policyToExecute( paths, credDict )

    return res
