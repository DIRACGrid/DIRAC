""" The FileCatalogClient is a class representing the client of the DIRAC File Catalog
"""
import json
import os

from DIRAC import S_OK, S_ERROR
from DIRAC.Core.Tornado.Client.ClientSelector import TransferClientSelector as TransferClient

from DIRAC.ConfigurationSystem.Client.Helpers.Registry import getVOMSAttributeForGroup, getDNForUsername
from DIRAC.Resources.Catalog.Utilities import checkCatalogArguments
from DIRAC.Resources.Catalog.FileCatalogClientBase import FileCatalogClientBase


class FileCatalogClient(FileCatalogClientBase):
    """Client code to the DIRAC File Catalogue"""

    # The list of methods below is defining the client interface
    READ_METHODS = FileCatalogClientBase.READ_METHODS + [
        "isFile",
        "getFileMetadata",
        "getFileDetails",
        "getReplicas",
        "getReplicaStatus",
        "getFileSize",
        "isDirectory",
        "getDirectoryReplicas",
        "listDirectory",
        "getDirectoryMetadata",
        "getDirectorySize",
        "getDirectoryContents",
        "getLFNForPFN",
        "getLFNForGUID",
        "findFilesByMetadata",
        "getMetadataFields",
        "findDirectoriesByMetadata",
        "getReplicasByMetadata",
        "findFilesByMetadataDetailed",
        "findFilesByMetadataWeb",
        "getCompatibleMetadata",
        "getMetadataSet",
        "getDatasets",
        "getFileDescendents",
        "getFileAncestors",
        "getDirectoryUserMetadata",
        "getFileUserMetadata",
        "checkDataset",
        "getDatasetParameters",
        "getDatasetFiles",
        "getDatasetAnnotation",
        "getSEDump",
        "getDirectoryDump",
    ]

    WRITE_METHODS = [
        "createLink",
        "removeLink",
        "addFile",
        "addFileAncestors",
        "setFileStatus",
        "addReplica",
        "removeReplica",
        "removeFile",
        "setReplicaStatus",
        "setReplicaHost",
        "setReplicaProblematic",
        "createDirectory",
        "setDirectoryStatus",
        "removeDirectory",
        "changePathMode",
        "changePathOwner",
        "changePathGroup",
        "addMetadataField",
        "deleteMetadataField",
        "setMetadata",
        "setMetadataBulk",
        "removeMetadata",
        "addMetadataSet",
        "addDataset",
        "addDatasetAnnotation",
        "removeDataset",
        "updateDataset",
        "freezeDataset",
        "releaseDataset",
        "addUser",
        "deleteUser",
        "addGroup",
        "deleteGroup",
        "repairCatalog",
        "rebuildDirectoryUsage",
    ]

    NO_LFN_METHODS = [
        "findFilesByMetadata",
        "addMetadataField",
        "deleteMetadataField",
        "getMetadataFields",
        "setMetadata",
        "setMetadataBulk",
        "removeMetadata",
        "getDirectoryUserMetadata",
        "findDirectoriesByMetadata",
        "getReplicasByMetadata",
        "findFilesByMetadataDetailed",
        "findFilesByMetadataWeb",
        "getCompatibleMetadata",
        "addMetadataSet",
        "getMetadataSet",
        "getFileUserMetadata",
        "getLFNForGUID",
        "addUser",
        "deleteUser",
        "addGroup",
        "deleteGroup",
        "repairCatalog",
        "rebuildDirectoryUsage",
    ]

    ADMIN_METHODS = [
        "addUser",
        "deleteUser",
        "addGroup",
        "deleteGroup",
        "getUsers",
        "getGroups",
        "getCatalogCounters",
        "repairCatalog",
        "rebuildDirectoryUsage",
    ]

    def __init__(self, url=None, **kwargs):
        """Constructor function."""
        self.serverURL = "DataManagement/FileCatalog" if not url else url
        super().__init__(self.serverURL, **kwargs)

    @checkCatalogArguments
    def getReplicas(self, lfns, allStatus=False, timeout=120):
        """Get the replicas of the given files"""
        rpcClient = self._getRPC(timeout=timeout)
        result = rpcClient.getReplicas(lfns, allStatus)

        if not result["OK"]:
            return result

        # If there is no PFN returned, just set the LFN instead
        lfnDict = result["Value"]
        for lfn in lfnDict["Successful"]:
            for se in lfnDict["Successful"][lfn]:
                if not lfnDict["Successful"][lfn][se]:
                    lfnDict["Successful"][lfn][se] = lfn

        return S_OK(lfnDict)

    @checkCatalogArguments
    def setReplicaProblematic(self, lfns, revert=False):
        """
        Set replicas to problematic.

        :param lfn lfns: has to be formated this way :
                    { lfn : { se1 : pfn1, se2 : pfn2, ...}, ...}
        :param revert: If True, remove the problematic flag

        :return: { successful : { lfn : [ ses ] } : failed : { lfn : { se : msg } } }
        """

        # This method does a batch treatment because the setReplicaStatus can only take one replica per lfn at once
        #
        # Illustration :
        #
        # lfns {'L2': {'S1': 'P3'}, 'L3': {'S3': 'P5', 'S2': 'P4', 'S4': 'P6'}, 'L1': {'S2': 'P2', 'S1': 'P1'}}
        #
        # loop1: lfnSEs {'L2': ['S1'], 'L3': ['S3', 'S2', 'S4'], 'L1': ['S2', 'S1']}
        # loop1 : batch {'L2': {'Status': 'P', 'SE': 'S1', 'PFN': 'P3'},
        #                'L3': {'Status': 'P', 'SE': 'S4', 'PFN': 'P6'},
        #                'L1': {'Status': 'P', 'SE': 'S1', 'PFN': 'P1'}}
        #
        # loop2: lfnSEs {'L2': [], 'L3': ['S3', 'S2'], 'L1': ['S2']}
        # loop2 : batch {'L3': {'Status': 'P', 'SE': 'S2', 'PFN': 'P4'}, 'L1': {'Status': 'P', 'SE': 'S2', 'PFN': 'P2'}}
        #
        # loop3: lfnSEs {'L3': ['S3'], 'L1': []}
        # loop3 : batch {'L3': {'Status': 'P', 'SE': 'S3', 'PFN': 'P5'}}
        #
        # loop4: lfnSEs {'L3': []}
        # loop4 : batch {}

        successful = {}
        failed = {}

        status = "AprioriGood" if revert else "Trash"

        # { lfn : [ se1, se2, ...], ...}
        lfnsSEs = {lfn: [se for se in lfns[lfn]] for lfn in lfns}

        while lfnsSEs:
            # { lfn : { 'SE' : se1, 'PFN' : pfn1, 'Status' : status }, ... }
            batch = {}

            for lfn in list(lfnsSEs):
                # If there are still some Replicas (SE) for the given LFN, we put it in the next batch
                # else we remove the entry from the lfnsSEs dict
                if lfnsSEs[lfn]:
                    se = lfnsSEs[lfn].pop()
                    batch[lfn] = {"SE": se, "PFN": lfns[lfn][se], "Status": status}
                else:
                    del lfnsSEs[lfn]

            # Happens when there is nothing to treat anymore
            if not batch:
                break

            res = self.setReplicaStatus(batch)
            if not res["OK"]:
                for lfn in batch:
                    failed.setdefault(lfn, {})[batch[lfn]["SE"]] = res["Message"]
                continue

            for lfn in res["Value"]["Failed"]:
                failed.setdefault(lfn, {})[batch[lfn]["SE"]] = res["Value"]["Failed"][lfn]

            for lfn in res["Value"]["Successful"]:
                successful.setdefault(lfn, []).append(batch[lfn]["SE"])

        return S_OK({"Successful": successful, "Failed": failed})

    @checkCatalogArguments
    def listDirectory(self, lfn, verbose=False, timeout=120):
        """List the given directory's contents"""
        rpcClient = self._getRPC(timeout=timeout)
        result = rpcClient.listDirectory(lfn, verbose)
        if not result["OK"]:
            return result
        # Force returned directory entries to be LFNs
        for entryType in ["Files", "SubDirs", "Links"]:
            for path in result["Value"]["Successful"]:
                entryDict = result["Value"]["Successful"][path][entryType]
                for fname in list(entryDict):
                    detailsDict = entryDict.pop(fname)
                    lfn = os.path.join(path, os.path.basename(fname))
                    entryDict[lfn] = detailsDict
        return result

    @checkCatalogArguments
    def getDirectoryMetadata(self, lfns, timeout=120):
        """Get standard directory metadata"""
        rpcClient = self._getRPC(timeout=timeout)
        result = rpcClient.getDirectoryMetadata(lfns)
        if not result["OK"]:
            return result
        # Add some useful fields
        for path in result["Value"]["Successful"]:
            owner = result["Value"]["Successful"][path]["Owner"]
            group = result["Value"]["Successful"][path]["OwnerGroup"]
            res = getDNForUsername(owner)
            if res["OK"]:
                result["Value"]["Successful"][path]["OwnerDN"] = res["Value"][0]
            else:
                result["Value"]["Successful"][path]["OwnerDN"] = ""
            result["Value"]["Successful"][path]["OwnerRole"] = getVOMSAttributeForGroup(group)
        return result

    @checkCatalogArguments
    def removeDirectory(self, lfn, recursive=False, timeout=120):
        """Remove the directory from the File Catalog. The recursive keyword is for the ineterface."""
        rpcClient = self._getRPC(timeout=timeout)
        return rpcClient.removeDirectory(lfn)

    @checkCatalogArguments
    def getDirectoryReplicas(self, lfns, allStatus=False, timeout=120):
        """Find all the given directories' replicas"""
        rpcClient = self._getRPC(timeout=timeout)
        result = rpcClient.getDirectoryReplicas(lfns, allStatus)

        if not result["OK"]:
            return result

        # Replace just the filename with the full LFN
        for path in result["Value"]["Successful"]:
            pathDict = result["Value"]["Successful"][path]
            for fname in list(pathDict):
                # Remove the file name from the directory dict
                detailsDict = pathDict.pop(fname)
                # Build the lfn
                lfn = f"{path}/{os.path.basename(fname)}"
                # Add the LFN as value for each SE which does not have a PFN
                for se in detailsDict:
                    if not detailsDict[se]:
                        detailsDict[se] = lfn
                # Add the lfn entry to the directory dict
                pathDict[lfn] = detailsDict

        return result

    def findFilesByMetadata(self, metaDict, path="/", timeout=120):
        """Find files given the meta data query and the path"""
        rpcClient = self._getRPC(timeout=timeout)
        result = rpcClient.findFilesByMetadata(metaDict, path)
        if not result["OK"]:
            return result
        if isinstance(result["Value"], list):
            return result
        elif isinstance(result["Value"], dict):
            # Process into the lfn list
            fileList = []
            for dir_, fList in result["Value"].items():
                for fi in fList:
                    fileList.append(dir_ + "/" + fi)
            result["Value"] = fileList
            return result
        else:
            return S_ERROR(f"Illegal return value type {type(result['Value'])}")

    def getFileUserMetadata(self, path, timeout=120):
        """Get the meta data attached to a file, but also to
        the its corresponding directory
        """
        directory = "/".join(path.split("/")[:-1])
        rpcClient = self._getRPC(timeout=timeout)
        result = rpcClient.getFileUserMetadata(path)
        if not result["OK"]:
            return result
        fmeta = result["Value"]
        result = rpcClient.getDirectoryUserMetadata(directory)
        if not result["OK"]:
            return result
        fmeta.update(result["Value"])

        return S_OK(fmeta)

    ########################################################################
    # Path operations (not updated)
    #

    @checkCatalogArguments
    def changePathOwner(self, lfns, recursive=False, timeout=120):
        """Get replica info for the given list of LFNs"""
        return self._getRPC(timeout=timeout).changePathOwner(lfns, recursive)

    @checkCatalogArguments
    def changePathGroup(self, lfns, recursive=False, timeout=120):
        """Get replica info for the given list of LFNs"""
        return self._getRPC(timeout=timeout).changePathGroup(lfns, recursive)

    @checkCatalogArguments
    def changePathMode(self, lfns, recursive=False, timeout=120):
        """Get replica info for the given list of LFNs"""
        return self._getRPC(timeout=timeout).changePathMode(lfns, recursive)

    ########################################################################
    # ACL Operations
    #

    @checkCatalogArguments
    def getPathPermissions(self, lfns, timeout=120):
        """Determine the ACL information for a supplied path"""
        return self._getRPC(timeout=timeout).getPathPermissions(lfns)

    @checkCatalogArguments
    def hasAccess(self, paths, opType, timeout=120):
        """Determine if the given op can be performed on the paths
        The OpType is all the operations exported
        """
        return self._getRPC(timeout=timeout).hasAccess(paths, opType)

    ###################################################################
    #
    #  User/Group write operations
    #

    def addUser(self, userName, timeout=120):
        """Add a new user to the File Catalog"""
        return self._getRPC(timeout=timeout).addUser(userName)

    def deleteUser(self, userName, timeout=120):
        """Delete user from the File Catalog"""
        return self._getRPC(timeout=timeout).deleteUser(userName)

    def addGroup(self, groupName, timeout=120):
        """Add a new group to the File Catalog"""
        return self._getRPC(timeout=timeout).addGroup(groupName)

    def deleteGroup(self, groupName, timeout=120):
        """Delete group from the File Catalog"""
        return self._getRPC(timeout=timeout).deleteGroup(groupName)

    ###################################################################
    #
    #  User/Group read operations
    #

    def getUsers(self, timeout=120):
        """Get all the users defined in the File Catalog"""
        return self._getRPC(timeout=timeout).getUsers()

    def getGroups(self, timeout=120):
        """Get all the groups defined in the File Catalog"""
        return self._getRPC(timeout=timeout).getGroups()

    ########################################################################
    #
    # Path read operations
    #

    @checkCatalogArguments
    def exists(self, lfns, timeout=120):
        """Check whether the supplied paths exists"""
        return self._getRPC(timeout=timeout).exists(lfns)

    ########################################################################
    #
    # File write operations
    #

    @checkCatalogArguments
    def addFile(self, lfns, timeout=120):
        """Register supplied files"""

        return self._getRPC(timeout=timeout).addFile(lfns)

    @checkCatalogArguments
    def removeFile(self, lfns, timeout=120):
        """Remove the supplied lfns"""
        return self._getRPC(timeout=timeout).removeFile(lfns)

    @checkCatalogArguments
    def setFileStatus(self, lfns, timeout=120):
        """Remove the supplied lfns"""
        return self._getRPC(timeout=timeout).setFileStatus(lfns)

    @checkCatalogArguments
    def addReplica(self, lfns, timeout=120):
        """Register supplied replicas"""
        return self._getRPC(timeout=timeout).addReplica(lfns)

    @checkCatalogArguments
    def removeReplica(self, lfns, timeout=120):
        """Remove the supplied replicas"""
        return self._getRPC(timeout=timeout).removeReplica(lfns)

    @checkCatalogArguments
    def setReplicaStatus(self, lfns, timeout=120):
        """Set the status for the supplied replicas"""
        return self._getRPC(timeout=timeout).setReplicaStatus(lfns)

    @checkCatalogArguments
    def setReplicaHost(self, lfns, timeout=120):
        """Change the registered SE for the supplied replicas"""
        return self._getRPC(timeout=timeout).setReplicaHost(lfns)

    @checkCatalogArguments
    def addFileAncestors(self, lfns, timeout=120):
        """Add file ancestor information for the given dict of LFNs.

        :param dict lfns: {lfn1: {'Ancestor': [ancestorLFNs]}, lfn2: {'Ancestors': ...}}
        """
        return self._getRPC(timeout=timeout).addFileAncestors(lfns)

    ########################################################################
    #
    # File read operations
    #

    @checkCatalogArguments
    def isFile(self, lfns, timeout=120):
        """Check whether the supplied lfns are files"""
        return self._getRPC(timeout=timeout).isFile(lfns)

    @checkCatalogArguments
    def getFileSize(self, lfns, timeout=120):
        """Get the size associated to supplied lfns"""
        return self._getRPC(timeout=timeout).getFileSize(lfns)

    @checkCatalogArguments
    def getFileMetadata(self, lfns, timeout=120):
        """Get the metadata associated to supplied lfns"""
        return self._getRPC(timeout=timeout).getFileMetadata(lfns)

    @checkCatalogArguments
    def getFileDetails(self, lfns, timeout=120):
        """Get the (user) metadata associated to supplied lfns"""
        return self._getRPC(timeout=timeout).getFileDetails(lfns)

    @checkCatalogArguments
    def getReplicaStatus(self, lfns, timeout=120):
        """Get the status for the supplied replicas"""
        return self._getRPC(timeout=timeout).getReplicaStatus(lfns)

    @checkCatalogArguments
    def getFileAncestors(self, lfns, depths, timeout=120):
        """Get the status for the supplied replicas"""
        return self._getRPC(timeout=timeout).getFileAncestors(lfns, depths)

    @checkCatalogArguments
    def getFileDescendents(self, lfns, depths, timeout=120):
        """Get the status for the supplied replicas"""
        return self._getRPC(timeout=timeout).getFileDescendents(lfns, depths)

    def getLFNForGUID(self, guids, timeout=120):
        """Get the matching lfns for given guids"""
        return self._getRPC(timeout=timeout).getLFNForGUID(guids)

    ########################################################################
    #
    # Directory write operations
    #

    @checkCatalogArguments
    def createDirectory(self, lfns, timeout=120):
        """Create the supplied directories"""
        return self._getRPC(timeout=timeout).createDirectory(lfns)

    ########################################################################
    #
    # Directory read operations
    #

    @checkCatalogArguments
    def isDirectory(self, lfns, timeout=120):
        """Determine whether supplied path is a directory"""
        return self._getRPC(timeout=timeout).isDirectory(lfns)

    @checkCatalogArguments
    def getDirectorySize(self, lfns, longOut=False, fromFiles=False, timeout=120, recursiveSum=True):
        """Get the size of the supplied directory"""
        return self._getRPC(timeout=timeout).getDirectorySize(lfns, longOut, fromFiles, recursiveSum)

    ########################################################################
    #
    # Administrative database operations
    #

    def getCatalogCounters(self, timeout=120):
        """Get the number of registered directories, files and replicas in various tables"""
        return self._getRPC(timeout=timeout).getCatalogCounters()

    def rebuildDirectoryUsage(self, timeout=120):
        """Rebuild DirectoryUsage table from scratch"""
        return self._getRPC(timeout=timeout).rebuildDirectoryUsage()

    def repairCatalog(self, timeout=120):
        """Repair the catalog inconsistencies"""
        return self._getRPC(timeout=timeout).repairCatalog()

    ########################################################################
    # Metadata Catalog Operations
    #

    def addMetadataField(self, fieldName, fieldType, metaType="-d", timeout=120):
        """Add a new metadata field of the given type"""
        return self._getRPC(timeout=timeout).addMetadataField(fieldName, fieldType, metaType)

    def deleteMetadataField(self, fieldName, timeout=120):
        """Delete the metadata field"""
        return self._getRPC(timeout=timeout).deleteMetadataField(fieldName)

    def getMetadataFields(self, timeout=120):
        """Get all the metadata fields"""
        return self._getRPC(timeout=timeout).getMetadataFields()

    def setMetadata(self, path, metadatadict, timeout=120):
        """Set metadata parameter for the given path"""
        return self._getRPC(timeout=timeout).setMetadata(path, metadatadict)

    def setMetadataBulk(self, pathMetadataDict, timeout=120):
        """Set metadata parameter for the given path"""
        return self._getRPC(timeout=timeout).setMetadataBulk(pathMetadataDict)

    def removeMetadata(self, pathMetadataDict, timeout=120):
        """Remove the specified metadata for the given path"""
        return self._getRPC(timeout=timeout).removeMetadata(pathMetadataDict)

    def getDirectoryUserMetadata(self, path, timeout=120):
        """Get all the metadata valid for the given directory path"""
        return self._getRPC(timeout=timeout).getDirectoryUserMetadata(path)

    def findDirectoriesByMetadata(self, metaDict, path="/", timeout=120):
        """Find all the directories satisfying the given metadata set"""
        return self._getRPC(timeout=timeout).findDirectoriesByMetadata(metaDict, path)

    def getReplicasByMetadata(self, metaDict, path="/", allStatus=False, timeout=120):
        """Find all the files satisfying the given metadata set"""
        return self._getRPC(timeout=timeout).getReplicasByMetadata(metaDict, path, allStatus)

    def findFilesByMetadataDetailed(self, metaDict, path="/", timeout=120):
        """Find all the files satisfying the given metadata set"""
        return self._getRPC(timeout=timeout).findFilesByMetadataDetailed(metaDict, path)

    def findFilesByMetadataWeb(self, metaDict, path, startItem, maxItems, timeout=120):
        """Find files satisfying the given metadata set"""
        return self._getRPC(timeout=timeout).findFilesByMetadataWeb(metaDict, path, startItem, maxItems)

    def getCompatibleMetadata(self, metaDict, path="/", timeout=120):
        """Get metadata values compatible with the given metadata subset"""
        return self._getRPC(timeout=timeout).getCompatibleMetadata(metaDict, path)

    def addMetadataSet(self, setName, setDict, timeout=120):
        """Add a new metadata set"""
        return self._getRPC(timeout=timeout).addMetadataSet(setName, setDict)

    def getMetadataSet(self, setName, expandFlag, timeout=120):
        """Add a new metadata set"""
        return self._getRPC(timeout=timeout).getMetadataSet(setName, expandFlag)

    #########################################################################################
    #
    #  Dataset manipulation methods
    #

    @checkCatalogArguments
    def addDataset(self, datasets, timeout=120):
        """Add a new dynamic dataset defined by its meta query"""
        return self._getRPC(timeout=timeout).addDataset(datasets)

    @checkCatalogArguments
    def addDatasetAnnotation(self, datasetDict, timeout=120):
        """Add annotation to an already created dataset"""
        return self._getRPC(timeout=timeout).addDatasetAnnotation(datasetDict)

    @checkCatalogArguments
    def removeDataset(self, datasets, timeout=120):
        """Check the given dynamic dataset for changes since its definition"""
        return self._getRPC(timeout=timeout).removeDataset(datasets)

    @checkCatalogArguments
    def checkDataset(self, datasets, timeout=120):
        """Check the given dynamic dataset for changes since its definition"""
        return self._getRPC(timeout=timeout).checkDataset(datasets)

    @checkCatalogArguments
    def updateDataset(self, datasets, timeout=120):
        """Update the given dynamic dataset for changes since its definition"""
        return self._getRPC(timeout=timeout).updateDataset(datasets)

    @checkCatalogArguments
    def getDatasets(self, datasets, timeout=120):
        """Get parameters of the given dynamic dataset as they are stored in the database"""
        return self._getRPC(timeout=timeout).getDatasets(datasets)

    @checkCatalogArguments
    def getDatasetParameters(self, datasets, timeout=120):
        """Get parameters of the given dynamic dataset as they are stored in the database"""
        return self._getRPC(timeout=timeout).getDatasetParameters(datasets)

    @checkCatalogArguments
    def getDatasetAnnotation(self, datasets, timeout=120):
        """Get annotation of the given datasets"""
        return self._getRPC(timeout=timeout).getDatasetAnnotation(datasets)

    @checkCatalogArguments
    def freezeDataset(self, datasets, timeout=120):
        """Freeze the contents of the dataset making it effectively static"""
        return self._getRPC(timeout=timeout).freezeDataset(datasets)

    @checkCatalogArguments
    def releaseDataset(self, datasets, timeout=120):
        """Release the contents of the frozen dataset allowing changes in its contents"""
        return self._getRPC(timeout=timeout).releaseDataset(datasets)

    @checkCatalogArguments
    def getDatasetFiles(self, datasets, timeout=120):
        """Get lfns in the given dataset
        two lines !
        """
        return self._getRPC(timeout=timeout).getDatasetFiles(datasets)

    #############################################################################

    def getSEDump(self, seNames, outputFilename):
        """
        Dump the content of SEs in the given file.
        The file contains a list of [SEName, lfn,checksum,size] dumped as csv,
        separated by '|'

        :param seName: list of StorageElement names
        :param outputFilename: path to the file where to dump it

        :returns: result from the TransferClient
        """
        if isinstance(seNames, str):
            seNames = seNames.split(",")

        seNames = json.dumps(seNames)

        dfc = TransferClient(self.serverURL, timeout=20000)
        return dfc.receiveFile(outputFilename, seNames)

    @checkCatalogArguments
    def getDirectoryDump(self, lfns, timeout=120):
        """Get the content of a directory recursively"""
        return self._getRPC(timeout=timeout).getDirectoryDump(lfns)
