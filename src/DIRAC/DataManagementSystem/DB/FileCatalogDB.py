""" DIRAC FileCatalog Database """
import errno

from DIRAC import gLogger, S_OK, S_ERROR
from DIRAC.Core.Base.DB import DB
from DIRAC.Resources.Catalog.Utilities import checkArgumentFormat
from DIRAC.Core.Utilities.ObjectLoader import ObjectLoader

#############################################################################


class FileCatalogDB(DB):
    def __init__(self, databaseLocation="DataManagement/FileCatalogDB", parentLogger=None):
        # The database location can be specified in System/Database form or in just the Database name
        # in the DataManagement system
        db = databaseLocation
        if "/" not in db:
            db = "DataManagement/" + db

        super().__init__("FileCatalogDB", db, parentLogger=parentLogger)

        self.ugManager = None
        self.seManager = None
        self.securityManager = None
        self.dtree = None
        self.fileManager = None
        self.dmeta = None
        self.fmeta = None
        self.datasetManager = None

    def setConfig(self, databaseConfig):
        self.directories = {}
        # In memory storage of the various parameters
        self.users = {}
        self.uids = {}
        self.groups = {}
        self.gids = {}
        self.seNames = {}
        self.seids = {}

        # Obtain some general configuration of the database
        self.uniqueGUID = databaseConfig["UniqueGUID"]
        self.globalReadAccess = databaseConfig["GlobalReadAccess"]
        self.lfnPfnConvention = databaseConfig["LFNPFNConvention"]
        if self.lfnPfnConvention == "None":
            self.lfnPfnConvention = False
        self.resolvePfn = databaseConfig["ResolvePFN"]
        self.umask = databaseConfig["DefaultUmask"]
        self.validFileStatus = databaseConfig["ValidFileStatus"]
        self.validReplicaStatus = databaseConfig["ValidReplicaStatus"]
        self.visibleFileStatus = databaseConfig["VisibleFileStatus"]
        self.visibleReplicaStatus = databaseConfig["VisibleReplicaStatus"]

        # Load the configured components
        for compAttribute, componentType in [
            ("ugManager", "UserGroupManager"),
            ("seManager", "SEManager"),
            ("securityManager", "SecurityManager"),
            ("dtree", "DirectoryManager"),
            ("fileManager", "FileManager"),
            ("datasetManager", "DatasetManager"),
            ("dmeta", "DirectoryMetadata"),
            ("fmeta", "FileMetadata"),
        ]:

            result = self.__loadCatalogComponent(componentType, databaseConfig[componentType])
            if not result["OK"]:
                return result
            self.__setattr__(compAttribute, result["Value"])

        return S_OK()

    def __loadCatalogComponent(self, componentType, componentName):
        """Create an object of a given catalog component"""
        componentModule = f"DataManagementSystem.DB.FileCatalogComponents.{componentType}.{componentName}"
        result = ObjectLoader().loadObject(componentModule)
        if not result["OK"]:
            gLogger.error("Failed to load catalog component", "{}: {}".format(componentName, result["Message"]))
            return result
        componentClass = result["Value"]
        component = componentClass(self)
        return S_OK(component)

    def setUmask(self, umask):
        self.umask = umask

    ########################################################################
    #
    #  SE based write methods
    #

    def addSE(self, seName, credDict):
        """
        Add a new StorageElement

        :param str seName: Name of the StorageElement
        :param credDict: credential
        """
        res = self._checkAdminPermission(credDict)
        if not res["OK"]:
            return res
        if not res["Value"]:
            return S_ERROR(errno.EACCES, "Permission denied")
        return self.seManager.addSE(seName)

    def deleteSE(self, seName, credDict):
        """
        Delete a StorageElement

        :param str seName: Name of the StorageElement
        :param credDict: credential
        """
        res = self._checkAdminPermission(credDict)
        if not res["OK"]:
            return res
        if not res["Value"]:
            return S_ERROR(errno.EACCES, "Permission denied")
        return self.seManager.deleteSE(seName)

    ########################################################################
    #
    #  User/groups based write methods
    #

    def addUser(self, userName, credDict):
        """
        Add a new user

        :param str userName: Name of the User
        :param credDict: credential
        """
        res = self._checkAdminPermission(credDict)
        if not res["OK"]:
            return res
        if not res["Value"]:
            return S_ERROR(errno.EACCES, "Permission denied")
        return self.ugManager.addUser(userName)

    def deleteUser(self, userName, credDict):
        """
        Delete a user

        :param str userName: Name of the User
        :param credDict: credential
        """
        res = self._checkAdminPermission(credDict)
        if not res["OK"]:
            return res
        if not res["Value"]:
            return S_ERROR(errno.EACCES, "Permission denied")
        return self.ugManager.deleteUser(userName)

    def addGroup(self, groupName, credDict):
        """
        Add a new group

        :param str groupName: Name of the group
        :param credDict: credential
        """
        res = self._checkAdminPermission(credDict)
        if not res["OK"]:
            return res
        if not res["Value"]:
            return S_ERROR(errno.EACCES, "Permission denied")
        return self.ugManager.addGroup(groupName)

    def deleteGroup(self, groupName, credDict):
        """
        Delete a group

        :param str groupName: Name of the group
        :param credDict: credential
        """
        res = self._checkAdminPermission(credDict)
        if not res["OK"]:
            return res
        if not res["Value"]:
            return S_ERROR(errno.EACCES, "Permission denied")
        return self.ugManager.deleteGroup(groupName)

    ########################################################################
    #
    #  User/groups based read methods
    #

    def getUsers(self, credDict):
        """
        Returns the list of users

        :param credDict: credential
        :return: dictionary indexed on the user name
        """
        res = self._checkAdminPermission(credDict)
        if not res["OK"]:
            return res
        if not res["Value"]:
            return S_ERROR(errno.EACCES, "Permission denied")
        return self.ugManager.getUsers()

    def getGroups(self, credDict):
        """
        Returns the list of groups

        :param credDict: credential
        :return: dictionary indexed on the group name
        """

        res = self._checkAdminPermission(credDict)
        if not res["OK"]:
            return res
        if not res["Value"]:
            return S_ERROR(errno.EACCES, "Permission denied")
        return self.ugManager.getGroups()

    ########################################################################
    #
    #  Path based read methods
    #

    def exists(self, lfns, credDict):
        res = self._checkPathPermissions("exists", lfns, credDict)
        if not res["OK"]:
            return res
        failed = res["Value"]["Failed"]
        successful = {}
        if res["Value"]["Successful"]:
            res = self.fileManager.exists(res["Value"]["Successful"])
            if not res["OK"]:
                return res
            failed.update(res["Value"]["Failed"])
            successful = res["Value"]["Successful"]

            notExist = []
            for lfn in list(res["Value"]["Successful"]):
                if not successful[lfn]:
                    notExist.append(lfn)
                    successful.pop(lfn)
            if notExist:
                res = self.dtree.exists(notExist)
                if not res["OK"]:
                    return res
                failed.update(res["Value"]["Failed"])
                successful.update(res["Value"]["Successful"])

        return S_OK({"Successful": successful, "Failed": failed})

    def getPathPermissions(self, lfns, credDict):
        """Get permissions for the given user/group to manipulate the given lfns"""
        res = checkArgumentFormat(lfns)
        if not res["OK"]:
            return res
        lfns = res["Value"]

        return self.securityManager.getPathPermissions(list(lfns), credDict)

    def hasAccess(self, opType, paths, credDict):
        """Get permissions for the given user/group to execute the given operation
        on the given paths

        returns Successful dict with True/False
        """
        res = checkArgumentFormat(paths)
        if not res["OK"]:
            return res
        paths = res["Value"]

        return self.securityManager.hasAccess(opType, paths, credDict)

    ########################################################################
    #
    #  Path based read methods
    #

    def changePathOwner(self, paths, credDict, recursive=False):
        """Bulk method to change Owner for the given paths

        :param dict paths: dictionary < lfn : owner >
        :param dict credDict: dictionary of the caller credentials
        :param boolean recursive: flag to apply the operation recursively
        """
        res = self._checkPathPermissions("changePathOwner", paths, credDict)
        if not res["OK"]:
            return res
        failed = res["Value"]["Failed"]
        successful = {}
        if res["Value"]["Successful"]:
            result = self.__changePathFunction(
                res["Value"]["Successful"],
                credDict,
                self.dtree.changeDirectoryOwner,
                self.fileManager.changeFileOwner,
                recursive=recursive,
            )
            failed.update(result["Value"]["Failed"])
            successful = result["Value"]["Successful"]
        return S_OK({"Successful": successful, "Failed": failed})

    def changePathGroup(self, paths, credDict, recursive=False):
        """Bulk method to change Group for the given paths

        :param dict paths: dictionary < lfn : group >
        :param dict credDict: dictionary of the caller credentials
        :param boolean recursive: flag to apply the operation recursively
        """
        res = self._checkPathPermissions("changePathGroup", paths, credDict)
        if not res["OK"]:
            return res
        failed = res["Value"]["Failed"]
        successful = {}
        if res["Value"]["Successful"]:
            result = self.__changePathFunction(
                res["Value"]["Successful"],
                credDict,
                self.dtree.changeDirectoryGroup,
                self.fileManager.changeFileGroup,
                recursive=recursive,
            )
            failed.update(result["Value"]["Failed"])
            successful = result["Value"]["Successful"]
        return S_OK({"Successful": successful, "Failed": failed})

    def changePathMode(self, paths, credDict, recursive=False):
        """Bulk method to change Mode for the given paths

        :param dict paths: dictionary < lfn : mode >
        :param dict credDict: dictionary of the caller credentials
        :param boolean recursive: flag to apply the operation recursively
        """

        res = self._checkPathPermissions("changePathMode", paths, credDict)
        if not res["OK"]:
            return res
        failed = res["Value"]["Failed"]
        successful = {}
        if res["Value"]["Successful"]:
            result = self.__changePathFunction(
                res["Value"]["Successful"],
                credDict,
                self.dtree.changeDirectoryMode,
                self.fileManager.changeFileMode,
                recursive=recursive,
            )
            failed.update(result["Value"]["Failed"])
            successful = result["Value"]["Successful"]
        return S_OK({"Successful": successful, "Failed": failed})

    def __changePathFunction(self, paths, credDict, change_function_directory, change_function_file, recursive=False):
        """A generic function to change Owner, Group or Mode for the given paths

        :param dict paths: dictionary < lfn : parameter_value >
        :param dict credDict: dictionary of the caller credentials
        :param function change_function_directory: function to change directory parameters
        :param function change_function_file: function to change file parameters
        :param boolean recursive: flag to apply the operation recursively
        """
        dirList = []
        result = self.isDirectory(paths, credDict)
        if not result["OK"]:
            return result
        for di in result["Value"]["Successful"]:
            if result["Value"]["Successful"][di]:
                dirList.append(di)
        fileList = []
        if len(dirList) < len(paths):
            result = self.isFile(paths, credDict)
            if not result["OK"]:
                return result
            for fi in result["Value"]["Successful"]:
                if result["Value"]["Successful"][fi]:
                    fileList.append(fi)

        successful = {}
        failed = {}

        dirArgs = {}
        fileArgs = {}

        for path in paths:
            if (path not in dirList) and (path not in fileList):
                failed[path] = "No such file or directory"
            if path in dirList:
                dirArgs[path] = paths[path]
            elif path in fileList:
                fileArgs[path] = paths[path]
        if dirArgs:
            result = change_function_directory(dirArgs, recursive=recursive)
            if not result["OK"]:
                return result
            successful.update(result["Value"]["Successful"])
            failed.update(result["Value"]["Failed"])
        if fileArgs:
            result = change_function_file(fileArgs)
            if not result["OK"]:
                return result
            successful.update(result["Value"]["Successful"])
            failed.update(result["Value"]["Failed"])
        return S_OK({"Successful": successful, "Failed": failed})

    ########################################################################
    #
    #  File based write methods
    #

    def addFile(self, lfns, credDict):
        """
        Add a new File

        :param dict lfns: indexed on file's LFN, the values are dictionaries which contains
                          the attributes of the files (PFN, SE, Size, GUID, Checksum)
        :param creDict: credential

        :return: Successful/Failed dict.
        """
        res = self._checkPathPermissions("addFile", lfns, credDict)
        if not res["OK"]:
            return res
        failed = res["Value"]["Failed"]
        # if no successful, just return
        if not res["Value"]["Successful"]:
            return S_OK({"Successful": {}, "Failed": failed})

        res = self.fileManager.addFile(res["Value"]["Successful"], credDict)
        if not res["OK"]:
            return res
        failed.update(res["Value"]["Failed"])
        successful = res["Value"]["Successful"]
        return S_OK({"Successful": successful, "Failed": failed})

    def setFileStatus(self, lfns, credDict):
        """
        Set the status of a File

        :param dict lfns: dict indexed on the LFNs. The values are the status (should be in config['ValidFileStatus'])
        :param creDict: credential

        :return: Successful/Failed dict.
        """

        res = self._checkPathPermissions("setFileStatus", lfns, credDict)
        if not res["OK"]:
            return res
        failed = res["Value"]["Failed"]

        # if no successful, just return
        if not res["Value"]["Successful"]:
            return S_OK({"Successful": {}, "Failed": failed})

        res = self.fileManager.setFileStatus(res["Value"]["Successful"])
        if not res["OK"]:
            return res
        failed.update(res["Value"]["Failed"])
        successful = res["Value"]["Successful"]
        return S_OK({"Successful": successful, "Failed": failed})

    def removeFile(self, lfns, credDict):
        """
         Remove files

        :param lfns: list of LFNs to remove
        :type lfns: python:list
        :param creDict: credential
        :return: Successful/Failed dict.
        """

        res = self._checkPathPermissions("removeFile", lfns, credDict)
        if not res["OK"]:
            return res
        failed = res["Value"]["Failed"]

        # if no successful, just return
        if not res["Value"]["Successful"]:
            return S_OK({"Successful": {}, "Failed": failed})

        res = self.fileManager.removeFile(res["Value"]["Successful"])
        if not res["OK"]:
            return res
        failed.update(res["Value"]["Failed"])
        successful = res["Value"]["Successful"]
        return S_OK({"Successful": successful, "Failed": failed})

    def addReplica(self, lfns, credDict):
        """
         Add a replica to a File

        :param dict lfns: keys are LFN. The values are dict with key PFN and SE
                          (e.g. {myLfn : {"PFN" : "myPfn", "SE" : "mySE"}})
        :param creDict: credential

        :return: Successful/Failed dict.
        """

        res = self._checkPathPermissions("addReplica", lfns, credDict)
        if not res["OK"]:
            return res
        failed = res["Value"]["Failed"]

        # if no successful, just return
        if not res["Value"]["Successful"]:
            return S_OK({"Successful": {}, "Failed": failed})

        res = self.fileManager.addReplica(res["Value"]["Successful"])
        if not res["OK"]:
            return res
        failed.update(res["Value"]["Failed"])
        successful = res["Value"]["Successful"]
        return S_OK({"Successful": successful, "Failed": failed})

    def removeReplica(self, lfns, credDict):
        """
         Remove replicas

        :param dict lfns: keys are LFN. The values are dict with key PFN and SE
                          (e.g. {myLfn : {"PFN" : "myPfn", "SE" : "mySE"}})
        :param creDict: credential

        :return: Successful/Failed dict.
        """

        res = self._checkPathPermissions("removeReplica", lfns, credDict)
        if not res["OK"]:
            return res
        failed = res["Value"]["Failed"]

        # if no successful, just return
        if not res["Value"]["Successful"]:
            return S_OK({"Successful": {}, "Failed": failed})

        res = self.fileManager.removeReplica(res["Value"]["Successful"])
        if not res["OK"]:
            return res
        failed.update(res["Value"]["Failed"])
        successful = res["Value"]["Successful"]
        return S_OK({"Successful": successful, "Failed": failed})

    def setReplicaStatus(self, lfns, credDict):
        """
        Set the status of a Replicas

        :param dict lfns: dict indexed on the LFNs. The values are dict with keys
                          "SE" and "Status" (that has to be in config['ValidReplicaStatus'])
        :param creDict: credential

        :return: Successful/Failed dict.
        """
        res = self._checkPathPermissions("setReplicaStatus", lfns, credDict)
        if not res["OK"]:
            return res
        failed = res["Value"]["Failed"]

        # if no successful, just return
        if not res["Value"]["Successful"]:
            return S_OK({"Successful": {}, "Failed": failed})

        res = self.fileManager.setReplicaStatus(res["Value"]["Successful"])
        if not res["OK"]:
            return res
        failed.update(res["Value"]["Failed"])
        successful = res["Value"]["Successful"]
        return S_OK({"Successful": successful, "Failed": failed})

    def setReplicaHost(self, lfns, credDict):
        res = self._checkPathPermissions("setReplicaHost", lfns, credDict)
        if not res["OK"]:
            return res
        failed = res["Value"]["Failed"]

        # if no successful, just return
        if not res["Value"]["Successful"]:
            return S_OK({"Successful": {}, "Failed": failed})

        res = self.fileManager.setReplicaHost(res["Value"]["Successful"])
        if not res["OK"]:
            return res
        failed.update(res["Value"]["Failed"])
        successful = res["Value"]["Successful"]
        return S_OK({"Successful": successful, "Failed": failed})

    def addFileAncestors(self, lfns, credDict):
        """Add ancestor information for the given LFNs"""
        res = self._checkPathPermissions("addFileAncestors", lfns, credDict)
        if not res["OK"]:
            return res
        failed = res["Value"]["Failed"]

        # if no successful, just return
        if not res["Value"]["Successful"]:
            return S_OK({"Successful": {}, "Failed": failed})

        res = self.fileManager.addFileAncestors(res["Value"]["Successful"])
        if not res["OK"]:
            return res
        failed.update(res["Value"]["Failed"])
        successful = res["Value"]["Successful"]
        return S_OK({"Successful": successful, "Failed": failed})

    ########################################################################
    #
    #  File based read methods
    #

    def isFile(self, lfns, credDict):
        """
        Checks whether a list of LFNS are files or not

        :param lfns: list of LFN to check
        :type lfns: python:list
        :param creDict: credential

        :return: Successful/Failed dict.
                The values of the successful dict are True or False whether it's a file or not
        """

        res = self._checkPathPermissions("isFile", lfns, credDict)
        if not res["OK"]:
            return res
        failed = res["Value"]["Failed"]

        # if no successful, just return
        if not res["Value"]["Successful"]:
            return S_OK({"Successful": {}, "Failed": failed})

        res = self.fileManager.isFile(res["Value"]["Successful"])
        if not res["OK"]:
            return res
        failed.update(res["Value"]["Failed"])
        successful = res["Value"]["Successful"]
        return S_OK({"Successful": successful, "Failed": failed})

    def getFileSize(self, lfns, credDict):
        """
        Gets the size of a list of lfns

        :param lfns: list of LFN to check
        :type lfns: python:list
        :param creDict: credential

        :return: Successful/Failed dict.
        """

        res = self._checkPathPermissions("getFileSize", lfns, credDict)
        if not res["OK"]:
            return res
        failed = res["Value"]["Failed"]

        # if no successful, just return
        if not res["Value"]["Successful"]:
            return S_OK({"Successful": {}, "Failed": failed})

        res = self.fileManager.getFileSize(res["Value"]["Successful"])
        if not res["OK"]:
            return res
        failed.update(res["Value"]["Failed"])
        successful = res["Value"]["Successful"]
        return S_OK({"Successful": successful, "Failed": failed})

    def getFileMetadata(self, lfns, credDict):
        """
        Gets the metadata of a list of lfns

        :param lfns: list of LFN to check
        :type lfns: python:list
        :param creDict: credential

        :return: Successful/Failed dict.
        """

        res = self._checkPathPermissions("getFileMetadata", lfns, credDict)
        if not res["OK"]:
            return res
        failed = res["Value"]["Failed"]

        # if no successful, just return
        if not res["Value"]["Successful"]:
            return S_OK({"Successful": {}, "Failed": failed})

        res = self.fileManager.getFileMetadata(res["Value"]["Successful"])

        if not res["OK"]:
            return res
        failed.update(res["Value"]["Failed"])
        successful = res["Value"]["Successful"]
        return S_OK({"Successful": successful, "Failed": failed})

    def getReplicas(self, lfns, allStatus, credDict):
        """
        Gets the list of replicas of a list of lfns

        :param lfns: list of LFN to check
        :type lfns: python:list
        :param allStatus: if all the status are visible, or only those defined in config['ValidReplicaStatus']
        :param creDict: credential

        :return: Successful/Failed dict. Successful is indexed on the LFN, and the values are dictionary
                 with the SEName as keys
        """

        res = self._checkPathPermissions("getReplicas", lfns, credDict)
        if not res["OK"]:
            return res
        failed = res["Value"]["Failed"]

        # if no successful, just return
        if not res["Value"]["Successful"]:
            return S_OK({"Successful": {}, "Failed": failed})

        res = self.fileManager.getReplicas(res["Value"]["Successful"], allStatus=allStatus)
        if not res["OK"]:
            return res
        failed.update(res["Value"]["Failed"])
        successful = res["Value"]["Successful"]
        return S_OK({"Successful": successful, "Failed": failed})

    def getReplicaStatus(self, lfns, credDict):
        """
        Gets the status of a list of replicas

        :param dict lfns: <lfn, se name>
        :param creDict: credential

        :return: Successful/Failed dict.
        """

        res = self._checkPathPermissions("getReplicaStatus", lfns, credDict)
        if not res["OK"]:
            return res
        failed = res["Value"]["Failed"]

        # if no successful, just return
        if not res["Value"]["Successful"]:
            return S_OK({"Successful": {}, "Failed": failed})

        res = self.fileManager.getReplicaStatus(res["Value"]["Successful"])
        if not res["OK"]:
            return res
        failed.update(res["Value"]["Failed"])
        successful = res["Value"]["Successful"]
        return S_OK({"Successful": successful, "Failed": failed})

    def getFileAncestors(self, lfns, depths, credDict):
        res = self._checkPathPermissions("getFileAncestors", lfns, credDict)
        if not res["OK"]:
            return res
        failed = res["Value"]["Failed"]

        # if no successful, just return
        if not res["Value"]["Successful"]:
            return S_OK({"Successful": {}, "Failed": failed})

        res = self.fileManager.getFileAncestors(res["Value"]["Successful"], depths)
        if not res["OK"]:
            return res
        failed.update(res["Value"]["Failed"])
        successful = res["Value"]["Successful"]
        return S_OK({"Successful": successful, "Failed": failed})

    def getFileDescendents(self, lfns, depths, credDict):
        res = self._checkPathPermissions("getFileDescendents", lfns, credDict)
        if not res["OK"]:
            return res
        failed = res["Value"]["Failed"]

        # if no successful, just return
        if not res["Value"]["Successful"]:
            return S_OK({"Successful": {}, "Failed": failed})

        res = self.fileManager.getFileDescendents(res["Value"]["Successful"], depths)
        if not res["OK"]:
            return res
        failed.update(res["Value"]["Failed"])
        successful = res["Value"]["Successful"]
        return S_OK({"Successful": successful, "Failed": failed})

    def getFileDetails(self, lfnList, credDict):
        """Get all the metadata for the given files"""
        connection = False
        result = self.fileManager._findFiles(lfnList, connection=connection)
        if not result["OK"]:
            return result
        resultDict = {}
        fileIDDict = {}
        lfnDict = result["Value"]["Successful"]
        for lfn in lfnDict:
            fileIDDict[lfnDict[lfn]["FileID"]] = lfn

        result = self.fileManager._getFileMetadataByID(list(fileIDDict), connection=connection)
        if not result["OK"]:
            return result
        for fileID in result["Value"]:
            resultDict[fileIDDict[fileID]] = result["Value"][fileID]

        result = self.fmeta._getFileUserMetadataByID(list(fileIDDict), credDict, connection=connection)
        if not result["OK"]:
            return result
        for fileID in fileIDDict:
            resultDict[fileIDDict[fileID]].setdefault("Metadata", {})
            if fileID in result["Value"]:
                resultDict[fileIDDict[fileID]]["Metadata"] = result["Value"][fileID]

        return S_OK(resultDict)

    def getLFNForGUID(self, guids, credDict):
        """
        Gets the lfns that match a list of guids

        :param lfns: list of guid to look for
        :type lfns: python:list
        :param creDict: credential

        :return: S_OK({guid:lfn}) dict.
        """
        return self.fileManager.getLFNForGUID(guids)

    ########################################################################
    #
    #  Directory based Write methods
    #

    def createDirectory(self, lfns, credDict):
        """
        Create new directories

        :param lfns: list of directories
        :type lfns: python:list
        :param creDict: credential

        :return: Successful/Failed dict.
        """
        res = self._checkPathPermissions("createDirectory", lfns, credDict)
        if not res["OK"]:
            return res
        failed = res["Value"]["Failed"]

        # if no successful, just return
        if not res["Value"]["Successful"]:
            return S_OK({"Successful": {}, "Failed": failed})

        res = self.dtree.createDirectory(res["Value"]["Successful"], credDict)
        if not res["OK"]:
            return res
        failed.update(res["Value"]["Failed"])
        successful = res["Value"]["Successful"]
        return S_OK({"Successful": successful, "Failed": failed})

    def removeDirectory(self, lfns, credDict):
        """
        Remove directories

        :param lfns: list of directories
        :type lfns: python:list
        :param creDict: credential

        :return: Successful/Failed dict.
        """
        res = self._checkPathPermissions("removeDirectory", lfns, credDict)
        if not res["OK"]:
            return res
        failed = res["Value"]["Failed"]

        # if no successful, just return
        if not res["Value"]["Successful"]:
            return S_OK({"Successful": {}, "Failed": failed})

        res = self.dtree.removeDirectory(res["Value"]["Successful"], credDict)
        if not res["OK"]:
            return res
        failed.update(res["Value"]["Failed"])
        successful = res["Value"]["Successful"]
        if not successful:
            return S_OK({"Successful": successful, "Failed": failed})

        # Remove the directory metadata now
        dirIdList = [successful[p]["DirID"] for p in successful if "DirID" in successful[p]]
        result = self.dmeta.removeMetadataForDirectory(dirIdList, credDict)
        if not result["OK"]:
            return result
        failed.update(result["Value"]["Failed"])
        # We remove from The successful those that failed in the metadata removal
        map(lambda x: successful.pop(x) if x in successful else None, failed)
        # We update the successful
        successful.update(result["Value"]["Successful"])
        return S_OK({"Successful": successful, "Failed": failed})

    ########################################################################
    #
    #  Directory based read methods
    #

    def listDirectory(self, lfns, credDict, verbose=False):
        """
        List directories

        :param lfns: list of directories
        :type lfns: python:list
        :param creDict: credential

        :return: Successful/Failed dict.
           The successful values are dictionaries indexed "Files", "Datasets", "Subdirs" and "Links"
        """

        res = self._checkPathPermissions("listDirectory", lfns, credDict)
        if not res["OK"]:
            return res
        failed = res["Value"]["Failed"]

        # if no successful, just return
        if not res["Value"]["Successful"]:
            return S_OK({"Successful": {}, "Failed": failed})

        res = self.dtree.listDirectory(res["Value"]["Successful"], verbose=verbose)
        if not res["OK"]:
            return res
        failed.update(res["Value"]["Failed"])
        successful = res["Value"]["Successful"]
        return S_OK({"Successful": successful, "Failed": failed})

    def isDirectory(self, lfns, credDict):
        """
        Checks whether a list of LFNS are directories or not

        :param lfns: list of LFN to check
        :type lfns: python:list
        :param creDict: credential

        :return: Successful/Failed dict.
                The values of the successful dict are True or False whether it's a dir or not
        """

        res = self._checkPathPermissions("isDirectory", lfns, credDict)
        if not res["OK"]:
            return res
        failed = res["Value"]["Failed"]

        # if no successful, just return
        if not res["Value"]["Successful"]:
            return S_OK({"Successful": {}, "Failed": failed})

        res = self.dtree.isDirectory(res["Value"]["Successful"])
        if not res["OK"]:
            return res
        failed.update(res["Value"]["Failed"])
        successful = res["Value"]["Successful"]
        return S_OK({"Successful": successful, "Failed": failed})

    def getDirectoryReplicas(self, lfns, allStatus, credDict):
        res = self._checkPathPermissions("getDirectoryReplicas", lfns, credDict)
        if not res["OK"]:
            return res
        failed = res["Value"]["Failed"]

        # if no successful, just return
        if not res["Value"]["Successful"]:
            return S_OK({"Successful": {}, "Failed": failed})

        res = self.dtree.getDirectoryReplicas(res["Value"]["Successful"], allStatus)
        if not res["OK"]:
            return res
        failed.update(res["Value"]["Failed"])
        successful = res["Value"]["Successful"]
        return S_OK({"Successful": successful, "Failed": failed})

    def getDirectorySize(self, lfns, longOutput, fromFiles, recursiveSum, credDict):
        """
        Get the sizes of a list of directories

        :param lfns: list of LFN to check
        :type lfns: python:list
        :param longOutput: if True, get also the physical size per SE (takes longer)
        :param fromFiles: if True, recompute the size from the file tables instead of the
                         precomputed values (takes longer)
        :param recursiveSum: if True (default), takes into account the subdirectories
        :param creDict: credential

        :return: Successful/Failed dict.
            The successful values are dictionaries indexed "LogicalFiles" (nb of files),
            "LogicalDirectories" (nb of dir) and "LogicalSize" (sum of File's sizes)
        """

        res = self._checkPathPermissions("getDirectorySize", lfns, credDict)
        if not res["OK"]:
            return res
        failed = res["Value"]["Failed"]

        # if no successful, just return
        if not res["Value"]["Successful"]:
            return S_OK({"Successful": {}, "Failed": failed})

        res = self.dtree.getDirectorySize(res["Value"]["Successful"], longOutput, fromFiles, recursiveSum)
        if not res["OK"]:
            return res
        failed.update(res["Value"]["Failed"])
        successful = res["Value"]["Successful"]
        queryTime = res["Value"].get("QueryTime", -1.0)
        return S_OK({"Successful": successful, "Failed": failed, "QueryTime": queryTime})

    def getDirectoryMetadata(self, lfns, credDict):
        """Get standard directory metadata

        :param lfns: list of directory paths
        :type lfns: python:list
        :param dict credDict: credentials
        :return: Successful/Failed dict.
        """
        res = self._checkPathPermissions("getDirectoryMetadata", lfns, credDict)
        if not res["OK"]:
            return res
        failed = res["Value"]["Failed"]
        successful = {}
        for lfn in res["Value"]["Successful"]:
            result = self.dtree.getDirectoryParameters(lfn)
            if result["OK"]:
                successful[lfn] = result["Value"]
            else:
                failed[lfn] = result["Message"]

        return S_OK({"Successful": successful, "Failed": failed})

    def rebuildDirectoryUsage(self):
        """Rebuild DirectoryUsage table from scratch"""

        result = self.dtree._rebuildDirectoryUsage()
        return result

    def repairCatalog(self, credDict={}):
        """Repair catalog inconsistencies"""

        result = self._checkAdminPermission(credDict)
        if not result["OK"]:
            return result
        if not result["Value"]:
            return S_ERROR(errno.EACCES, "Not authorized to perform catalog repairs")

        resultDict = {}
        resultDict["RecoverOrphanDirectories"] = self.dtree.recoverOrphanDirectories(credDict)
        resultDict["RepairFileTables"] = self.fileManager.repairFileTables()

        return S_OK(resultDict)

    #######################################################################
    #
    #  Catalog metadata methods
    #

    def setMetadata(self, path, metadataDict, credDict):
        """Add metadata to the given path"""
        res = self._checkPathPermissions("setMetadata", path, credDict)
        if not res["OK"]:
            return res
        if not res["Value"]["Successful"]:
            return S_ERROR(errno.EACCES, "Permission denied")
        if not res["Value"]["Successful"][path]:
            return S_ERROR(errno.EACCES, "Permission denied")

        result = self.dtree.isDirectory({path: True})
        if not result["OK"]:
            return result
        if not result["Value"]["Successful"]:
            return S_ERROR("Failed to determine the path type")
        if result["Value"]["Successful"][path]:
            # This is a directory
            return self.dmeta.setMetadata(path, metadataDict, credDict)
        else:
            # This is a file
            return self.fmeta.setMetadata(path, metadataDict, credDict)

    def setMetadataBulk(self, pathMetadataDict, credDict):
        """Add metadata for the given paths"""
        successful = {}
        failed = {}
        for path, metadataDict in pathMetadataDict.items():
            result = self.setMetadata(path, metadataDict, credDict)
            if result["OK"]:
                successful[path] = True
            else:
                failed[path] = result["Message"]

        return S_OK({"Successful": successful, "Failed": failed})

    def removeMetadata(self, pathMetadataDict, credDict):
        """Remove metadata for the given paths"""
        successful = {}
        failed = {}
        for path, metadataDict in pathMetadataDict.items():
            result = self.__removeMetadata(path, metadataDict, credDict)
            if result["OK"]:
                successful[path] = True
            else:
                failed[path] = result["Message"]

        return S_OK({"Successful": successful, "Failed": failed})

    def __removeMetadata(self, path, metadata, credDict):
        """Remove metadata from the given path"""
        res = self._checkPathPermissions("__removeMetadata", path, credDict)
        if not res["OK"]:
            return res
        if not res["Value"]["Successful"]:
            return S_ERROR("Permission denied")
        if not res["Value"]["Successful"][path]:
            return S_ERROR("Permission denied")

        result = self.dtree.isDirectory({path: True})
        if not result["OK"]:
            return result
        if not result["Value"]["Successful"]:
            return S_ERROR("Failed to determine the path type")
        if result["Value"]["Successful"][path]:
            # This is a directory
            return self.dmeta.removeMetadata(path, metadata, credDict)
        else:
            # This is a file
            return self.fmeta.removeMetadata(path, metadata, credDict)

    #######################################################################
    #
    #  Catalog admin methods
    #

    def getCatalogCounters(self, credDict):
        counterDict = {}
        res = self._checkAdminPermission(credDict)
        if not res["OK"]:
            return res
        if not res["Value"]:
            return S_ERROR(errno.EACCES, "Permission denied")
        # res = self.dtree.getDirectoryCounters()
        # if not res['OK']:
        #  return res
        # counterDict.update(res['Value'])
        res = self.fileManager.getFileCounters()
        if not res["OK"]:
            return res
        counterDict.update(res["Value"])
        res = self.fileManager.getReplicaCounters()
        if not res["OK"]:
            return res
        counterDict.update(res["Value"])
        res = self.dtree.getDirectoryCounters()
        if not res["OK"]:
            return res
        counterDict.update(res["Value"])
        return S_OK(counterDict)

    ########################################################################
    #
    #  Security based methods
    #

    def _checkAdminPermission(self, credDict):
        return self.securityManager.hasAdminAccess(credDict)

    def _checkPathPermissions(self, operation, lfns, credDict):

        res = checkArgumentFormat(lfns)
        if not res["OK"]:
            return res
        lfns = res["Value"]

        res = self.securityManager.hasAccess(operation, list(lfns), credDict)
        if not res["OK"]:
            return res
        # Do not consider those paths for which we failed to determine access
        failed = res["Value"]["Failed"]
        for lfn in failed:
            lfns.pop(lfn)
        # Do not consider those paths for which access is denied
        successful = {}
        for lfn, access in res["Value"]["Successful"].items():
            if not access:
                failed[lfn] = "Permission denied"
            else:
                successful[lfn] = lfns[lfn]
        return S_OK({"Successful": successful, "Failed": failed})

    def getSEDump(self, seNames):
        """
         Return all the files at given SEs, together with checksum and size

        :param seName: list of StorageElement names

        :returns: S_OK with list of tuples (SEName, lfn, checksum, size)
        """
        return self.fileManager.getSEDump(seNames)
