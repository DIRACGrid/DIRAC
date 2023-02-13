""" VOMSSecurityManager class implements access permissions based on VOMS roles
"""
import os
import datetime

from DIRAC import S_OK, S_ERROR
from DIRAC.ConfigurationSystem.Client.Helpers.Registry import getAllGroups, getGroupOption
from DIRAC.DataManagementSystem.DB.FileCatalogComponents.SecurityManager.SecurityManagerBase import (
    SecurityManagerBase,
    _readMethods,
    _writeMethods,
)
from DIRAC.Core.Utilities.ReturnValues import returnSingleResult


class VOMSSecurityManager(SecurityManagerBase):
    """This class implements a 3-level POSIX permission, wrapping up
    the DIRAC group into VOMS roles
    """

    def __init__(self, database=False):
        super().__init__(database=database)

        # voms role : [dirac groups that have it]
        self.vomsRoles = {}
        # dirac group : voms role it has
        self.diracGroups = {}

        # Lifetime of the info in the two dictionaries
        self.CACHE_TIME = datetime.timedelta(seconds=600)
        self.__buildRolesAndGroups()

    def __buildRolesAndGroups(self):
        """Rebuild the cache dictionary for VOMS roles and DIRAC Groups"""

        self.lastBuild = datetime.datetime.now()

        allGroups = getAllGroups()

        for grpName in allGroups:
            vomsRole = getGroupOption(grpName, "VOMSRole")
            if vomsRole:
                self.diracGroups[grpName] = vomsRole
                self.vomsRoles.setdefault(vomsRole, []).append(grpName)

    def __getVomsRole(self, grpName):
        """Returns the VOMS role of a given DIRAC group

        :param grpName:

        :returns: VOMS role, or None
        """
        if (datetime.datetime.now() - self.lastBuild) > self.CACHE_TIME:
            self.__buildRolesAndGroups()

        return self.diracGroups.get(grpName)

    def __getDiracGroups(self, vomsRole):
        """Returns all the DIRAC groups that have a given VOMS role

        :param vomsRole:

        :returns: list of groups, empty if not exist
        """

        if (datetime.datetime.now - self.lastBuild) > self.CACHE_TIME:
            self.__buildRolesAndGroups()

        return self.vomsRoles.get(vomsRole, [])

    def __shareVomsRole(self, grpName, otherGrpName):
        """Returns True if the two DIRAC groups have the same VOMS role"""

        vomsGrp = self.__getVomsRole(grpName)
        vomsOtherGrp = self.__getVomsRole(otherGrpName)
        # The voms group cannot be None
        return vomsGrp and vomsOtherGrp and (vomsGrp == vomsOtherGrp)

    def __isNotExistError(self, errorMsg):
        """Returns true if the errorMsg means that the file/directory does not exist"""

        for possibleMsg in ["not exist", "not found", "No such file or directory"]:
            if possibleMsg in errorMsg:
                return True

        return False

    def __getFilePermission(self, path, credDict, noExistStrategy=None):
        """Checks POSIX permission for a file using the VOMS roles.
        That is, if the owner group of the file shares the same vomsRole as the requesting user,
        we check the permission as if the request was done with the real owner group.

        :param path : file path (string)
        :param credDict : credential of the user
        :param noExistStrategy : If the directory does not exist, we can
                                 * True : allow the access
                                 * False : forbid the access
                                 * None : return the error as is

        :returns S_OK structure with a dictionary ( Read/Write/Execute : True/False)
        """

        if not path:
            return S_ERROR("Empty path")

        # We check what is the group stored in the DB for the given path
        res = returnSingleResult(self.db.fileManager.getFileMetadata([path]))
        if not res["OK"]:
            # If the error is not due to the directory not existing, we return
            if not self.__isNotExistError(res["Message"]):
                return res

            # From now on, we know that the error is due to the file not existing

            # If we have no strategy regarding non existing files, then just return the error
            if noExistStrategy is None:
                return res

            # Finally, follow the strategy
            return S_OK(dict.fromkeys(["Read", "Write", "Execute"], noExistStrategy))

        # ===========================================================================
        # # This does not seem necessary since we add the OwnerGroup in the query behind the scene
        # origGrp = 'unknown'
        # res = self.db.ugManager.getGroupName( res['Value']['GID'] )
        # if res['OK']:
        #   origGrp = res['Value']
        # ===========================================================================

        origGrp = res["Value"].get("OwnerGroup", "unknown")

        # If the two group share the same voms role, we do the query like if we were
        # the group stored in the DB
        if self.__shareVomsRole(credDict.get("group", "anon"), origGrp):
            credDict = {"username": credDict.get("username", "anon"), "group": origGrp}

        return returnSingleResult(self.db.fileManager.getPathPermissions([path], credDict))

    def __testPermissionOnFile(self, paths, permission, credDict, noExistStrategy=None):
        """Tests a permission on a list of files

        :param path : list/dict of file paths
        :param permission : Read/Write/Execute string
        :param credDict : credential of the user
        :param noExistStrategy : If the directory does not exist, we can
                                 * True : allow the access
                                 * False : forbid the access
                                 * None : return the error as is

        :returns: Successful dictionary with True of False, and Failed.
        """

        successful = {}
        failed = {}

        for filename in paths:
            res = self.__getFilePermission(filename, credDict, noExistStrategy=noExistStrategy)
            if not res["OK"]:
                failed[filename] = res["Message"]
            else:
                successful[filename] = res["Value"].get(permission, False)

        return S_OK({"Successful": successful, "Failed": failed})

    def __getDirectoryPermission(self, path, credDict, recursive=True, noExistStrategy=None):
        """Checks POSIX permission for a directory using the VOMS roles.
        That is, if the owner group of the directory share the same vomsRole as the requesting user,
        we check the permission as if the request was done with the real owner group.

        :param path : directory path (string)
        :param credDict : credential of the user
        :param recursive : if that directory does not exist, checks the parent one
        :param noExistStrategy : If the directory does not exist, we can
                                 * True : allow the access
                                 * False : forbid the access
                                 * None : return the error as is

               noExistStrategy makes sense only if recursive is False

        :returns S_OK structure with a dictionary ( Read/Write/Execute : True/False)
        """

        if not path:
            return S_ERROR("Empty path")

        # We check what is the group stored in the DB for the given path
        res = self.db.dtree.getDirectoryParameters(path)
        if not res["OK"]:
            # If the error is not due to the directory not existing, we return

            if not self.__isNotExistError(res["Message"]):
                return res

            # Very special case to allow creation of very first entry
            if path == "/":
                return S_OK({"Read": True, "Write": True, "Execute": True})

            # From now on, we know that the error is due to the directory not existing

            # If recursive, we try the parent directory
            if recursive:
                return self.__getDirectoryPermission(
                    os.path.dirname(path), credDict, recursive=recursive, noExistStrategy=noExistStrategy
                )
            # From now on, we know we don't run recursive

            # If we have no strategy regarding non existing directories, then just return the error
            if noExistStrategy is None:
                return res

            # Finally, follow the strategy
            return S_OK(dict.fromkeys(["Read", "Write", "Execute"], noExistStrategy))

        # The directory exists.
        origGrp = res["Value"]["OwnerGroup"]

        # If the two group share the same voms role, we do the query like if we were
        # the group stored in the DB
        if self.__shareVomsRole(credDict.get("group", "anon"), origGrp):
            credDict = {"username": credDict.get("username", "anon"), "group": origGrp}

        return self.db.dtree.getDirectoryPermissions(path, credDict)

    def __testPermissionOnDirectory(self, paths, permission, credDict, recursive=True, noExistStrategy=None):
        """Tests a permission on a list of directories

        :param path : list/dict of directory paths
        :param permission : Read/Write/Execute string
        :param credDict : credential of the user
        :param recursive : if that directory does not exist, checks the parent one
        :param noExistStrategy : If the directory does not exist, we can
                                 * True : allow the access
                                 * False : forbid the access
                                 * None : return the error as is

               noExistStrategy makes sense only if recursive is False

        :returns: Successful dictionary with True of False, and Failed.
        """

        successful = {}
        failed = {}

        for dirName in paths:
            res = self.__getDirectoryPermission(dirName, credDict, recursive=recursive, noExistStrategy=noExistStrategy)
            if not res["OK"]:
                failed[dirName] = res["Message"]
            else:
                successful[dirName] = res["Value"].get(permission, False)

        return S_OK({"Successful": successful, "Failed": failed})

    def __testPermissionOnParentDirectory(self, paths, permission, credDict, recursive=True, noExistStrategy=None):
        """Tests a permission on the parents of a list of directories

        :param path : directory path (string)
        :param permission : Read/Write/Execute string
        :param credDict : credential of the user
        :param recursive : if that directory does not exist, checks the parent one
        :param noExistStrategy : If the directory does not exist, we can
                                 * True : allow the access
                                 * False : forbid the access
                                 * None : return the error as is

               noExistStrategy makes sense only if recursive is False

        :returns: Successful dictionary with True of False, and Failed.
        """
        parentDirs = {}
        for path in paths:
            parentDirs.setdefault(os.path.dirname(path), []).append(path)

        res = self.__testPermissionOnDirectory(
            parentDirs, permission, credDict, recursive=recursive, noExistStrategy=noExistStrategy
        )

        if not res["OK"]:
            return res

        failed = res["Value"]["Failed"]
        successful = {}

        parentAllowed = res["Value"]["Successful"]

        for parentName in parentAllowed:
            isParentAllowed = parentAllowed[parentName]
            for path in parentDirs[parentName]:
                successful[path] = isParentAllowed

        return S_OK({"Successful": successful, "Failed": failed})

    def __getFileOrDirectoryPermission(self, path, credDict, recursive=False, noExistStrategy=None):
        """Checks POSIX permission for a directory or file using the VOMS roles.
        That is, if the owner group of the directory or file shares the same vomsRole as the requesting user,
        we check the permission as if the request was done with the real owner group.

        We first consider the path as a file, and if it does not exist, we consider it as a directory.

        :param path : directory or file path (string)
        :param credDict : credential of the user
        :param recursive : if that directory does not exist, checks the parent one
        :param noExistStrategy : If the directory does not exist, we can
                                 * True : allow the access
                                 * False : forbid the access
                                 * None : return the error as is

               noExistStrategy makes sense only if recursive is False

        :returns S_OK structure with a dictionary ( Read/Write/Execute : True/False)
        """
        # First consider it as File
        # We want to know whether the file does not exist, so we force noExistStrategy to None
        res = self.__getFilePermission(path, credDict, noExistStrategy=None)
        if not res["OK"]:
            # If the error is not due to the directory not existing, we return
            if not self.__isNotExistError(res["Message"]):
                return res

            # From now on, we know that the error is due to the File not existing
            # We Try then the directory method, since path can be a directory
            # The noExistStrategy will be applied by __getDirectoryPermission, so we don't need to do it ourselves
            res = self.__getDirectoryPermission(path, credDict, recursive=recursive, noExistStrategy=noExistStrategy)

        return res

    def __testPermissionOnFileOrDirectory(self, paths, permission, credDict, recursive=False, noExistStrategy=None):
        """Tests a permission on a list of files or directories.

        :param path : list/dict of directory or files paths
        :param permission : Read/Write/Execute string
        :param credDict : credential of the user
        :param recursive : if that directory does not exist, checks the parent one
        :param noExistStrategy : If the directory does not exist, we can
                                 * True : allow the access
                                 * False : forbid the access
                                 * None : return the error as is

               noExistStrategy makes sense only if recursive is False

        :returns: Successful dictionary with True of False, and Failed.
        """

        successful = {}
        failed = {}

        for path in paths:
            res = self.__getFileOrDirectoryPermission(
                path, credDict, recursive=recursive, noExistStrategy=noExistStrategy
            )
            if not res["OK"]:
                failed[path] = res["Message"]
            else:
                successful[path] = res["Value"].get(permission, False)

        return S_OK({"Successful": successful, "Failed": failed})

    def __policyRemoveDirectory(self, paths, credDict):
        """Tests whether the remove operation on directories
        is permitted.
        Removal of non existing directory is always allowed.
        For existing directories, we must have the write permission
        on the parent

        :param paths: list/dict of path
        :param credDict: credential of the user
        :returns: Successful with True of False, and Failed.
        """

        successful = {}

        # We allow removal of all the non existing directories
        res = self.db.dtree.exists(paths)
        if not res["OK"]:
            return res

        nonExistingDirectories = {path for path in res["Value"]["Successful"] if not res["Value"]["Successful"][path]}

        existingDirs = set(paths) - set(nonExistingDirectories)
        for dirName in nonExistingDirectories:
            successful[dirName] = True

        res = self.__testPermissionOnParentDirectory(existingDirs, "Write", credDict, recursive=False)
        if not res["OK"]:
            return res

        failed = res["Value"]["Failed"]
        successful.update(res["Value"]["Successful"])

        return S_OK({"Successful": successful, "Failed": failed})

    def __policyRemoveFile(self, paths, credDict):
        """Tests whether the remove operation on files
        is permitted.
        Removal of non existing file is always allowed.
        For existing files, we must have the write permission
        on the parent

        :param paths: list/dict of path
        :param credDict: credential of the user
        :returns: Successful with True of False, and Failed.
        """

        successful = {}

        # We allow removal of all the non existing files
        res = self.db.fileManager.exists(paths)
        if not res["OK"]:
            return res

        nonExistingFiles = {path for path in res["Value"]["Successful"] if not res["Value"]["Successful"][path]}

        existingFiles = set(paths) - set(nonExistingFiles)
        for dirName in nonExistingFiles:
            successful[dirName] = True

        res = self.__testPermissionOnParentDirectory(existingFiles, "Write", credDict, recursive=False)
        if not res["OK"]:
            return res

        failed = res["Value"]["Failed"]
        successful.update(res["Value"]["Successful"])

        return S_OK({"Successful": successful, "Failed": failed})

    def __policyListDirectory(self, paths, credDict):
        """Test Read permission on the directory.
        If the directory does not exist, we do not allow.

        :param paths: list/dict of path
        :param credDict: credential of the user
        :returns: Successful with True of False, and Failed.
        """

        return self.__testPermissionOnDirectory(paths, "Read", credDict, recursive=True)

    def __policyReadForFileAndDirectory(self, paths, credDict):
        """Testing the read bit on the parent directory,
        be it a file or a directory.
        So it reads permissions from a directory

        :param paths: list/dict of path
        :param credDict: credential of the user
        :returns: Successful with True of False, and Failed.
        """

        return self.__testPermissionOnParentDirectory(paths, "Read", credDict, recursive=True)

    def __policyWriteForFileAndDirectory(self, paths, credDict):
        """Testing the read bit on the parent directory (recursively),
        be it a file or a directory.
        So it reads permissions from a directory

        :param paths: list/dict of path
        :param credDict: credential of the user
        :returns: Successful with True of False, and Failed.
        """

        return self.__testPermissionOnParentDirectory(paths, "Write", credDict, recursive=True)

    def __policyReadForReplica(self, paths, credDict):
        """Test Read permission on the file associated to the replica.
        If the file does not exist, we allow.

        :param paths: list/dict of path
        :param credDict: credential of the user
        :returns: Successful with True of False, and Failed.
        """
        return self.__testPermissionOnFile(paths, "Read", credDict, noExistStrategy=True)

    def __policyWriteForReplica(self, paths, credDict):
        """Test Write permission on the file associated to the replica.
        If the file does not exist, we allow.

        :param paths: list/dict of path
        :param credDict: credential of the user
        :returns: Successful with True of False, and Failed.
        """
        return self.__testPermissionOnFile(paths, "Write", credDict, noExistStrategy=True)

    def __policyWriteOnFile(self, paths, credDict):
        """Test Write permission on the file.
        If the file does not exist, we allow.

        :param paths: list/dict of path
        :param credDict: credential of the user
        :returns: Successful with True of False, and Failed.
        """
        return self.__testPermissionOnFile(paths, "Write", credDict, noExistStrategy=True)

    def __policyChangePathMode(self, paths, credDict):
        """Test Write permission on the directory/file.
        If the directory/file does not exist, we allow.

        :param paths: list/dict of path
        :param credDict: credential of the user
        :returns: Successful with True of False, and Failed.
        """

        return self.__testPermissionOnFileOrDirectory(paths, "Write", credDict, recursive=False, noExistStrategy=True)

    def __policyDeny(self, paths, credDict):
        """Denies the access to all the paths given

        :param paths: list/dict of path
        :param credDict: credential of the user
        :returns: Successful with True of False, and Failed.
        """

        return S_OK({"Successful": dict.fromkeys(paths, False), "Failed": {}})

    def hasAccess(self, opType, paths, credDict):
        """Checks whether a given operation on given paths is permitted

        :param opType: name of the operation (the FileCatalog methods in fact...)
        :param paths: list/dictionary of path on which we want to apply the operation
        :param credDict: credential of the users (with at least username, group and properties)

        :returns: Successful dict with True or False, and Failed dict. In fact, it is not neccesarily
                a boolean, rather an int (binary operation results)
        """

        # Check if admin access is granted first
        result = self.hasAdminAccess(credDict)
        if not result["OK"]:
            return result

        if result["Value"]:
            # We are admin, allow everything
            return S_OK({"Successful": dict.fromkeys(paths, True), "Failed": {}})

        if opType not in _readMethods + _writeMethods:
            return S_ERROR("Operation type not known %s" % opType)

        if self.db.globalReadAccess and (opType in _readMethods):
            return S_OK({"Successful": dict.fromkeys(paths, True), "Failed": {}})

        policyToExecute = None

        if opType == "removeDirectory":
            policyToExecute = self.__policyRemoveDirectory

        elif opType in ["createDirectory", "addFile"]:
            policyToExecute = self.__policyWriteForFileAndDirectory

        elif opType == "removeFile":
            policyToExecute = self.__policyRemoveFile

        elif opType in [
            "addFileAncestors",
            "setFileStatus",
            "addReplica",
            "removeReplica",
            "setReplicaStatus",
            "setReplicaHost",
        ]:
            policyToExecute = self.__policyWriteOnFile

        elif opType == "listDirectory":
            policyToExecute = self.__policyListDirectory

        elif opType in [
            "isDirectory",
            "getDirectoryReplicas",
            "getDirectoryMetadata",
            "getDirectorySize",
            "isFile",
            "getFileSize",
            "getFileMetadata",
            "exists",
            "getFileAncestors",
            "getFileDescendents",
            "getDirectoryDump",
        ]:
            policyToExecute = self.__policyReadForFileAndDirectory

        elif opType in ["getReplicas", "getReplicaStatus"]:
            policyToExecute = self.__policyReadForReplica

        # Only admin can do that, and if we are here, we are not admin
        elif opType in ["changePathOwner", "changePathGroup"]:
            policyToExecute = self.__policyDeny

        elif opType == "changePathMode":
            policyToExecute = self.__policyChangePathMode

        if not policyToExecute:
            return S_ERROR("No policy matching operation %s" % opType)

        res = policyToExecute(paths, credDict)

        return res

    def getPathPermissions(self, paths, credDict):
        """This method is meant to disappear, hopefully soon,
        but as long as we have clients from versions < v6r14,
        we need a getPathPermissions method. Since it does not make
        sense with that kind of fine grain policy, we return what used to
        be returned...
        """
        return super().getPathPermissions(paths, credDict)
