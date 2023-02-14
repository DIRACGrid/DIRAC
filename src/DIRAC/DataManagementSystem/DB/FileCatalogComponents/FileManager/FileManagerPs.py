""" FileManager for ... ?
"""
import os
import datetime

from DIRAC import S_OK, S_ERROR
from DIRAC.DataManagementSystem.DB.FileCatalogComponents.FileManager.FileManagerBase import FileManagerBase
from DIRAC.Core.Utilities.List import stringListToString, intListToString, breakListIntoChunks

# The logic of some methods is basically a copy/paste from the FileManager class,
# so I could have inherited from it. However, I did not want to depend on it


class FileManagerPs(FileManagerBase):
    def __init__(self, database=None):
        super().__init__(database)

    ######################################################
    #
    # The all important _findFiles and _getDirectoryFiles methods
    #

    def _findFiles(self, lfns, metadata=["FileID"], allStatus=False, connection=False):
        """Returns the information for the given lfns
        The logic works nicely in the FileManager, so I pretty much copied it.
        :param lfns: list of lfns
        :param metadata: list of params that we want to get for each lfn
        :param allStatus: consider all file status or only those defined in db.visibleFileStatus

        :return successful/failed convention. successful is a dict < lfn : dict of metadata >

        """
        connection = self._getConnection(connection)
        dirDict = self._getFileDirectories(lfns)

        result = self.db.dtree.findDirs(list(dirDict))
        if not result["OK"]:
            return result

        directoryIDs = result["Value"]

        failed = {}
        successful = {}
        for dirPath in directoryIDs:
            fileNames = dirDict[dirPath]
            res = self._getDirectoryFiles(
                directoryIDs[dirPath], fileNames, metadata, allStatus=allStatus, connection=connection
            )

            for fileName, fileDict in res.get("Value", {}).items():
                fname = os.path.join(dirPath, fileName)
                successful[fname] = fileDict

        # The lfns that are not in successful nor failed don't exist
        for failedLfn in set(lfns) - set(successful):
            failed.setdefault(failedLfn, "No such file or directory")

        return S_OK({"Successful": successful, "Failed": failed})

    def _findFileIDs(self, lfns, connection=False):
        """Find lfn <-> FileID correspondence"""
        connection = self._getConnection(connection)
        failed = {}
        successful = {}

        # If there is only one lfn, we might as well make a direct query
        if len(lfns) == 1:
            lfn = list(lfns)[0]  # if lfns is a dict, list(lfns) returns lfns.keys()
            pathPart, filePart = os.path.split(lfn)
            result = self.db.executeStoredProcedure(
                "ps_get_file_id_from_lfn", (pathPart, filePart, "ret1"), outputIds=[2]
            )
            if not result["OK"]:
                return result

            fileId = result["Value"][0]

            if not fileId:
                failed[lfn] = "No such file or directory"
            else:
                successful[lfn] = fileId

        else:
            # We separate the files by directory
            filesInDirDict = self._getFileDirectories(lfns)

            # We get the directory ids
            result = self.db.dtree.findDirs(list(filesInDirDict))
            if not result["OK"]:
                return result
            directoryPathToIds = result["Value"]

            # For each directory, we get the file ids of the files we want
            for dirPath in directoryPathToIds:
                fileNames = filesInDirDict[dirPath]
                dirID = directoryPathToIds[dirPath]

                formatedFileNames = stringListToString(fileNames)

                result = self.db.executeStoredProcedureWithCursor(
                    "ps_get_file_ids_from_dir_id", (dirID, formatedFileNames)
                )
                if not result["OK"]:
                    return result
                for fileID, fileName in result["Value"]:
                    fname = os.path.join(dirPath, fileName)
                    successful[fname] = fileID

            # The lfns that are not in successful dont exist
            for failedLfn in set(lfns) - set(successful):
                failed[failedLfn] = "No such file or directory"

        return S_OK({"Successful": successful, "Failed": failed})

    def _getDirectoryFiles(self, dirID, fileNames, metadata_input, allStatus=False, connection=False):
        """For a given directory, and eventually given file, returns all the desired metadata

        :param int dirID: directory ID
        :param fileNames: the list of filenames, or []
        :param metadata_input: list of desired metadata.
                   It can be anything from (FileName, DirID, FileID, Size, UID, Owner,
                   GID, OwnerGroup, Status, GUID, Checksum, ChecksumType, Type, CreationDate, ModificationDate, Mode)
        :param bool allStatus: if False, only displays the files whose status is in db.visibleFileStatus

        :returns: S_OK(files), where files is a dictionary indexed on filename, and values are dictionary of metadata
        """

        connection = self._getConnection(connection)

        metadata = list(metadata_input)
        if "UID" in metadata:
            metadata.append("Owner")
        if "GID" in metadata:
            metadata.append("OwnerGroup")
        if "FileID" not in metadata:
            metadata.append("FileID")

        # Format the filenames and status to be used in a IN clause in the sotred procedure
        formatedFileNames = stringListToString(fileNames)
        fStatus = stringListToString(self.db.visibleFileStatus)

        specificFiles = True if len(fileNames) else False
        result = self.db.executeStoredProcedureWithCursor(
            "ps_get_all_info_for_files_in_dir", (dirID, specificFiles, formatedFileNames, allStatus, fStatus)
        )

        if not result["OK"]:
            return result

        fieldNames = [
            "FileName",
            "DirID",
            "FileID",
            "Size",
            "UID",
            "Owner",
            "GID",
            "OwnerGroup",
            "Status",
            "GUID",
            "Checksum",
            "ChecksumType",
            "Type",
            "CreationDate",
            "ModificationDate",
            "Mode",
        ]

        rows = result["Value"]
        files = {}

        for row in rows:
            rowDict = dict(zip(fieldNames, row))
            fileName = rowDict["FileName"]
            # Returns only the required metadata
            files[fileName] = {key: rowDict.get(key, "Unknown metadata field") for key in metadata}

        return S_OK(files)

    def _getFileMetadataByID(self, fileIDs, connection=False):
        """Get standard file metadata for a list of files specified by FileID

        :param fileIDS : list of file Ids

        :returns: S_OK(files), where files is a dictionary indexed on fileID
                            and the values dictionaries containing the following info:
                            ["FileID", "Size", "UID", "GID", "s.Status", "GUID", "CreationDate"]
        """

        # Format the filenames and status to be used in a IN clause in the sotred procedure
        formatedFileIds = intListToString(fileIDs)
        result = self.db.executeStoredProcedureWithCursor("ps_get_all_info_for_file_ids", (formatedFileIds,))
        if not result["OK"]:
            return result

        rows = result["Value"]

        fieldNames = ["FileID", "Size", "UID", "GID", "s.Status", "GUID", "CreationDate"]

        resultDict = {}

        for row in rows:
            rowDict = dict(zip(fieldNames, row))
            rowDict["Size"] = int(rowDict["Size"])
            rowDict["UID"] = int(rowDict["UID"])
            rowDict["GID"] = int(rowDict["GID"])
            resultDict[rowDict["FileID"]] = rowDict

        return S_OK(resultDict)

    def __insertMultipleFiles(self, allFileValues, wantedLfns):
        """Insert multiple files in one query. However, if there is a problem
            with one file, all the query is rolled back.
        :param allFileValues : dictionary of tuple with all the information about possibly more
                              files than we want to insert
        :param wantedLfns : list of lfn that we want to insert
        """

        fileValuesStrings = []
        fileDescStrings = []

        for lfn in wantedLfns:
            dirID, size, s_uid, s_gid, statusID, fileName, guid, checksum, checksumtype, mode = allFileValues[lfn]
            utcNow = datetime.datetime.utcnow().replace(microsecond=0)
            fileValuesStrings.append(
                "(%s, %s, %s, %s, %s, '%s', '%s', '%s', '%s', '%s', '%s', %s)"
                % (dirID, size, s_uid, s_gid, statusID, fileName, guid, checksum, checksumtype, utcNow, utcNow, mode)
            )
            fileDescStrings.append(f"(DirID = {dirID} AND FileName = '{fileName}')")

        fileValuesStr = ",".join(fileValuesStrings)
        fileDescStr = " OR ".join(fileDescStrings)

        result = self.db.executeStoredProcedureWithCursor("ps_insert_multiple_file", (fileValuesStr, fileDescStr))

        return result

    def __chunks(self, l, n):
        """Yield successive n-sized chunks from l."""
        for i in range(0, len(l), n):
            yield l[i : i + n]

    def _insertFiles(self, lfns, uid, gid, connection=False):
        """Insert new files. lfns is a dictionary indexed on lfn, the values are
        mandatory: DirID, Size, Checksum, GUID
        optional : Owner (dict with username and group), ChecksumType (Adler32 by default), Mode (db.umask by default)

        :param lfns : lfns and info to insert
        :param uid : user id, overwriten by Owner['username'] if defined
        :param gid : user id, overwriten by Owner['group'] if defined

        """

        connection = self._getConnection(connection)

        failed = {}
        successful = {}
        res = self._getStatusInt("AprioriGood", connection=connection)

        if res["OK"]:
            statusID = res["Value"]
        else:
            return res

        lfnsToRetry = []

        fileValues = {}
        fileDesc = {}

        # Prepare each file separately
        for lfn in lfns:
            # Get all the info
            fileInfo = lfns[lfn]

            dirID = fileInfo["DirID"]
            fileName = os.path.basename(lfn)
            size = fileInfo["Size"]
            ownerDict = fileInfo.get("Owner", None)
            checksum = fileInfo["Checksum"]
            checksumtype = fileInfo.get("ChecksumType", "Adler32")
            guid = fileInfo["GUID"]
            mode = fileInfo.get("Mode", self.db.umask)

            s_uid = uid
            s_gid = gid

            # overwrite the s_uid and s_gid if defined in the lfn info
            if ownerDict:
                result = self.db.ugManager.getUserAndGroupID(ownerDict)
                if result["OK"]:
                    s_uid, s_gid = result["Value"]

            fileValues[lfn] = (dirID, size, s_uid, s_gid, statusID, fileName, guid, checksum, checksumtype, mode)
            fileDesc[(dirID, fileName)] = lfn

        chunkSize = 200
        allChunks = list(self.__chunks(list(lfns), chunkSize))

        for lfnChunk in allChunks:
            result = self.__insertMultipleFiles(fileValues, lfnChunk)

            if result["OK"]:
                allIds = result["Value"]
                for dirId, fileName, fileID in allIds:
                    lfn = fileDesc[(dirId, fileName)]
                    successful[lfn] = lfns[lfn]
                    successful[lfn]["FileID"] = fileID
            else:
                lfnsToRetry.extend(lfnChunk)

        # If we are here, that means that the multiple insert failed, so we do one by one

        for lfn in lfnsToRetry:
            dirID, size, s_uid, s_gid, statusID, fileName, guid, checksum, checksumtype, mode = fileValues[lfn]
            # insert
            result = self.db.executeStoredProcedureWithCursor(
                "ps_insert_file", (dirID, size, s_uid, s_gid, statusID, fileName, guid, checksum, checksumtype, mode)
            )

            if not result["OK"]:
                failed[lfn] = result["Message"]
            else:
                fileID = result["Value"][0][0]

                successful[lfn] = lfns[lfn]
                successful[lfn]["FileID"] = fileID

        return S_OK({"Successful": successful, "Failed": failed})

    def _getFileIDFromGUID(self, guids, connection=False):
        """Returns the file ids from list of guids
        :param guids : list of guid

        :returns dictionary  < guid : fileId >

        """

        connection = self._getConnection(connection)
        if not guids:
            return S_OK({})

        if not isinstance(guids, (list, tuple)):
            guids = [guids]

        #     formatedGuids = ','.join( [ '"%s"' % guid for guid in guids ] )
        formatedGuids = stringListToString(guids)
        result = self.db.executeStoredProcedureWithCursor("ps_get_file_ids_from_guids", (formatedGuids,))

        if not result["OK"]:
            return result

        guidDict = {guid: fileID for guid, fileID in result["Value"]}

        return S_OK(guidDict)

    def getLFNForGUID(self, guids, connection=False):
        """Returns the lfns matching given guids"""
        connection = self._getConnection(connection)
        if not guids:
            return S_OK({})

        if not isinstance(guids, (list, tuple)):
            guids = [guids]

        formatedGuids = stringListToString(guids)
        result = self.db.executeStoredProcedureWithCursor("ps_get_lfns_from_guids", (formatedGuids,))

        if not result["OK"]:
            return result

        guidDict = {guid: lfn for guid, lfn in result["Value"]}
        failedGuid = set(guids) - set(guidDict)
        failed = dict.fromkeys(failedGuid, "GUID does not exist") if failedGuid else {}
        return S_OK({"Successful": guidDict, "Failed": failed})

    ######################################################
    #
    # _deleteFiles related methods
    #

    def _deleteFiles(self, fileIDs, connection=False):
        """Delete a list of files and the associated replicas

        :param fileIDS : list of fileID

        :returns: S_OK() or S_ERROR(msg)
        """

        connection = self._getConnection(connection)

        replicaPurge = self.__deleteFileReplicas(fileIDs)
        filePurge = self.__deleteFiles(fileIDs, connection=connection)

        if not replicaPurge["OK"]:
            return replicaPurge

        if not filePurge["OK"]:
            return filePurge

        return S_OK()

    def __deleteFileReplicas(self, fileIDs, connection=False):
        """Delete all the replicas from the file ids

        :param fileIDs: list of file ids

        :returns: S_OK() or S_ERROR(msg)
        """

        connection = self._getConnection(connection)

        if not fileIDs:
            return S_OK()

        formatedFileIds = intListToString(fileIDs)

        result = self.db.executeStoredProcedureWithCursor("ps_delete_replicas_from_file_ids", (formatedFileIds,))
        if not result["OK"]:
            return result

        errno, msg = result["Value"][0]

        if errno:
            return S_ERROR(msg)

        return S_OK()

    def __deleteFiles(self, fileIDs, connection=False):
        """Delete the files from their ids

        :param fileIDs: list of file ids

        :returns: S_OK() or S_ERROR(msg)
        """

        connection = self._getConnection(connection)

        formatedFileIds = intListToString(fileIDs)

        result = self.db.executeStoredProcedureWithCursor("ps_delete_files", (formatedFileIds,))
        if not result["OK"]:
            return result

        errno, msg = result["Value"][0]

        if errno:
            return S_ERROR(msg)

        return S_OK()

    def __insertMultipleReplicas(self, allReplicaValues, lfnsChunk):
        """Insert multiple replicas in one query. However, if there is a problem
            with one replica, all the query is rolled back.
        :param allReplicaValues : dictionary of tuple with all the information about possibly more
                              replica than we want to insert
        :param lfnsChunk : list of lfn that we want to insert
        """

        repValuesStrings = []
        repDescStrings = []

        for lfn in lfnsChunk:
            fileID, seID, statusID, replicaType, pfn = allReplicaValues[lfn]
            utcNow = datetime.datetime.utcnow().replace(microsecond=0)
            repValuesStrings.append(f"({fileID},{seID},'{statusID}','{replicaType}','{utcNow}','{utcNow}','{pfn}')")
            repDescStrings.append(f"(r.FileID = {fileID} AND SEID = {seID})")

        repValuesStr = ",".join(repValuesStrings)
        repDescStr = " OR ".join(repDescStrings)

        result = self.db.executeStoredProcedureWithCursor("ps_insert_multiple_replica", (repValuesStr, repDescStr))

        return result

    def _insertReplicas(self, lfns, master=False, connection=False):
        """Insert new replicas. lfns is a dictionary with one entry for each file. The keys are lfns, and values are dict
        with mandatory attributes : FileID, SE (the name), PFN

        :param lfns: lfns and info to insert
        :param master: true if they are master replica, otherwise they will be just 'Replica'

        :return: successful/failed convention, with successful[lfn] = true
        """
        chunkSize = 200

        connection = self._getConnection(connection)

        # Add the files
        failed = {}
        successful = {}

        # Get the status id of AprioriGood
        res = self._getStatusInt("AprioriGood", connection=connection)
        if not res["OK"]:
            return res
        statusID = res["Value"]

        lfnsToRetry = []

        repValues = {}
        repDesc = {}

        # treat each file after each other
        for lfn in lfns.keys():
            fileID = lfns[lfn]["FileID"]

            seName = lfns[lfn]["SE"]
            if isinstance(seName, str):
                seList = [seName]
            elif isinstance(seName, list):
                seList = seName
            else:
                return S_ERROR(f"Illegal type of SE list: {str(type(seName))}")

            replicaType = "Master" if master else "Replica"
            pfn = lfns[lfn]["PFN"]

            # treat each replica of a file after the other
            # (THIS CANNOT WORK... WE ARE ONLY CAPABLE OF DOING ONE REPLICA PER FILE AT THE TIME)
            for seName in seList:
                # get the SE id
                res = self.db.seManager.findSE(seName)
                if not res["OK"]:
                    failed[lfn] = res["Message"]
                    continue
                seID = res["Value"]

                # This is incompatible with adding multiple replica at the time for a given file
                repValues[lfn] = (fileID, seID, statusID, replicaType, pfn)
                repDesc[(fileID, seID)] = lfn

        allChunks = list(self.__chunks(list(lfns), chunkSize))

        for lfnChunk in allChunks:
            result = self.__insertMultipleReplicas(repValues, lfnChunk)

            if result["OK"]:
                allIds = result["Value"]
                for fileId, seId, repId in allIds:
                    lfn = repDesc[(fileId, seId)]
                    successful[lfn] = True
                    lfns[lfn]["RepID"] = repId
            else:
                lfnsToRetry.extend(lfnChunk)

        for lfn in lfnsToRetry:
            fileID, seID, statusID, replicaType, pfn = repValues[lfn]
            # insert the replica and its info
            result = self.db.executeStoredProcedureWithCursor(
                "ps_insert_replica", (fileID, seID, statusID, replicaType, pfn)
            )

            if not result["OK"]:
                failed[lfn] = result["Message"]
            else:
                replicaID = result["Value"][0][0]
                lfns[lfn]["RepID"] = replicaID
                successful[lfn] = True

        return S_OK({"Successful": successful, "Failed": failed})

    def _getRepIDsForReplica(self, replicaTuples, connection=False):
        """Get the Replica IDs for (fileId, SEID) couples

        :param repliacTuples : list of (fileId, SEID) couple

        :returns { fileID : { seID : RepID } }
        """
        connection = self._getConnection(connection)

        replicaDict = {}

        for fileID, seID in replicaTuples:
            result = self.db.executeStoredProcedure("ps_get_replica_id", (fileID, seID, "repIdOut"), outputIds=[2])
            if not result["OK"]:
                return result

            repID = result["Value"][0]

            # if the replica exists, we add it to the dict
            if repID:
                replicaDict.setdefault(fileID, {}).setdefault(seID, repID)

        return S_OK(replicaDict)

    ######################################################
    #
    # _deleteReplicas related methods
    #

    def _deleteReplicas(self, lfns, connection=False):
        """Deletes replicas. The deletion of replicas that do not exist is successful

        :param lfns : dictinary with lfns as key, and the value is a dict with a mandatory "SE" key,
                      corresponding to the SE name or SE ID

        :returns: successful/failed convention, with successful[lfn] = True
        """
        connection = self._getConnection(connection)
        failed = {}
        successful = {}
        # First we get the fileIds from our lfns
        res = self._findFiles(list(lfns), ["FileID"], connection=connection)
        if not res["OK"]:
            return res

        # If the file does not exist we consider the deletion successful
        for lfn, error in res["Value"]["Failed"].items():
            if error == "No such file or directory":
                successful[lfn] = True
            else:
                failed[lfn] = error

        lfnFileIDDict = res["Value"]["Successful"]
        for lfn, fileDict in lfnFileIDDict.items():
            fileID = fileDict["FileID"]

            # Then we get our StorageElement Id (cached in seManager)
            se = lfns[lfn]["SE"]
            # if se is already the se id, findSE will return it
            res = self.db.seManager.findSE(se)
            if not res["OK"]:
                return res
            seID = res["Value"]

            # Finally remove the replica
            result = self.db.executeStoredProcedureWithCursor("ps_delete_replica_from_file_and_se_ids", (fileID, seID))
            if not result["OK"]:
                failed[lfn] = result["Message"]
                continue

            errno, errMsg = result["Value"][0]
            if errno:
                failed[lfn] = errMsg
            else:
                successful[lfn] = True

        return S_OK({"Successful": successful, "Failed": failed})

    ######################################################
    #
    # _setReplicaStatus _setReplicaHost _setReplicaParameter methods
    # _setFileParameter method
    #

    def _setReplicaStatus(self, fileID, se, status, connection=False):
        """Set the status of a replica

        :param fileID : file id
        :param se : se name or se id
        :param status : status to be applied

        :returns: S_OK() or S_ERROR(msg)
        """
        if status not in self.db.validReplicaStatus:
            return S_ERROR(f"Invalid replica status {status}")
        connection = self._getConnection(connection)
        res = self._getStatusInt(status, connection=connection)
        if not res["OK"]:
            return res
        statusID = res["Value"]

        # Then we get our StorageElement Id (cached in seManager)
        res = self.db.seManager.findSE(se)
        if not res["OK"]:
            return res
        seID = res["Value"]

        result = self.db.executeStoredProcedureWithCursor("ps_set_replica_status", (fileID, seID, statusID))
        if not result["OK"]:
            return result

        affected = result["Value"][0][0]  # Affected is the number of raws updated

        if not affected:
            return S_ERROR("Replica does not exist")
        return S_OK()

    def _setReplicaHost(self, fileID, se, newSE, connection=False):
        """Move a replica from one SE to another (I don't think this should be called

        :param fileID : file id
        :param se : se name or se id of the previous se
        :param newSE : se name or se id of the new se

        :returns: S_OK() or S_ERROR(msg)
        """
        connection = self._getConnection(connection)

        # Get the new se id
        res = self.db.seManager.findSE(newSE)
        if not res["OK"]:
            return res
        newSEID = res["Value"]

        # Get the old se id
        res = self.db.seManager.findSE(se)
        if not res["OK"]:
            return res
        oldSEID = res["Value"]

        # update
        result = self.db.executeStoredProcedureWithCursor("ps_set_replica_host", (fileID, oldSEID, newSEID))
        if not result["OK"]:
            return result

        affected = result["Value"][0][0]
        if not affected:
            return S_ERROR("Replica does not exist")
        else:
            return S_OK()

    def _setFileParameter(self, fileID, paramName, paramValue, connection=False):
        """Generic method to set a file parameter


        :param fileID : id of the file
        :param paramName : the file parameter you want to change
              It should be one of [ UID, GID, Status, Mode]. However, in case of
              unexpected parameter, and to stay compatible with the other Manager,
              there is a manual request done.
        :param paramValue : the value (raw, or id) to insert

        :returns: S_OK() or S_ERROR

        """
        connection = self._getConnection(connection)

        # The PS associated with a given parameter
        psNames = {
            "UID": "ps_set_file_uid",
            "GID": "ps_set_file_gid",
            "Status": "ps_set_file_status",
            "Mode": "ps_set_file_mode",
        }

        psName = psNames.get(paramName, None)

        # If there is an associated procedure, we go for it
        if psName:
            result = self.db.executeStoredProcedureWithCursor(psName, (fileID, paramValue))
            if not result["OK"]:
                return result

            _affected = result["Value"][0][0]
            # If affected = 0, the file does not exist, but who cares...

        # In case this is a 'new' parameter, we have a failback solution, but we
        # should add a specific ps for it
        else:
            req = "UPDATE FC_Files SET {}='{}', ModificationDate=UTC_TIMESTAMP() WHERE FileID IN ({})".format(
                paramName,
                paramValue,
                intListToString(fileID),
            )
            return self.db._update(req, conn=connection)

        return S_OK()

    ######################################################
    #
    # _getFileReplicas related methods
    #

    def _getFileReplicas(self, fileIDs, fields_input=None, allStatus=False, connection=False):
        """Get replicas for the given list of files specified by their fileIDs
        :param fileIDs : list of file ids
        :param fields_input : metadata of the Replicas we are interested in (default to PFN)
        :param allStatus : if True, all the Replica statuses will be considered,
                           otherwise, only the db.visibleReplicaStatus

        :returns S_OK with a dict { fileID : { SE name : dict of metadata } }
        """

        if fields_input is None:
            fields_input = ["PFN"]

        fields = list(fields_input)

        # always add Status in the list of required fields
        if "Status" not in fields:
            fields.append("Status")

        # We initialize the dictionary with empty dict
        # as default value, because this is what we want for
        # non existing replicas
        replicas = {fileID: {} for fileID in fileIDs}

        # Format the status to be used in a IN clause in the stored procedure
        fStatus = stringListToString(self.db.visibleReplicaStatus)

        fieldNames = ["FileID", "SE", "Status", "RepType", "CreationDate", "ModificationDate", "PFN"]

        for chunks in breakListIntoChunks(fileIDs, 1000):
            # Format the FileIDs to be used in a IN clause in the stored procedure
            formatedFileIds = intListToString(chunks)
            result = self.db.executeStoredProcedureWithCursor(
                "ps_get_all_info_of_replicas_bulk", (formatedFileIds, allStatus, fStatus)
            )

            if not result["OK"]:
                return result

            rows = result["Value"]

            for row in rows:
                rowDict = dict(zip(fieldNames, row))
                se = rowDict["SE"]
                fileID = rowDict["FileID"]
                replicas[fileID][se] = {key: rowDict.get(key, "Unknown metadata field") for key in fields}

        return S_OK(replicas)

    def countFilesInDir(self, dirId):
        """Count how many files there is in a given Directory

        :param dirID: directory id

        :returns: S_OK(value) or S_ERROR
        """

        result = self.db.executeStoredProcedure("ps_count_files_in_dir", (dirId, "ret1"), outputIds=[1])
        if not result["OK"]:
            return result

        res = S_OK(result["Value"][0])
        return res

    ##########################################################################
    #
    #  We overwrite some methods from the base class because of the new DB constraints or perf reasons
    #
    #  Some methods could be inherited in the future if we have perf problems. For example
    #  * setFileGroup
    #  * setFileOwner
    #  * setFileMode
    #  * changePath*
    #
    ##########################################################################

    def _updateDirectoryUsage(self, directorySEDict, change, connection=False):
        """This updates the directory usage, but is now done by triggers in the DB"""
        return S_OK()

    def _computeStorageUsageOnRemoveFile(self, lfns, connection=False):
        """Again nothing to compute, all done by the triggers"""
        directorySESizeDict = {}
        return S_OK(directorySESizeDict)

    #   "REMARQUE : THIS IS STILL TRUE, BUT YOU MIGHT WANT TO CHECK FOR A GIVEN GUID ANYWAY
    #   def _checkUniqueGUID( self, lfns, connection = False ):
    #     """ The GUID unicity is ensured at the DB level, so we will have similar message if the insertion fails"""
    #
    #     failed = {}
    #     return failed

    def getDirectoryReplicas(self, dirID, path, allStatus=False, connection=False):
        """
        This is defined in the FileManagerBase but it relies on the SEManager to get the SE names.
        It is good practice in software, but since the SE and Replica tables are bound together in the DB,
        I might as well resolve the name in the query


        Get the replicas for all the Files in the given Directory

        :param int dirID: ID of the directory
        :param unused path: useless
        :param bool allStatus: whether all replicas and file status are considered
                               If False, take the visibleFileStatus and visibleReplicaStatus
                               values from the configuration
        """

        # We format the visible file/replica satus so we can give it as argument to the ps
        # It is used in an IN clause, so it looks like --'"AprioriGood","Trash"'--
        #     fStatus = ','.join( [ '"%s"' % status for status in self.db.visibleFileStatus ] )
        #     rStatus = ','.join( [ '"%s"' % status for status in self.db.visibleReplicaStatus ] )
        fStatus = stringListToString(self.db.visibleFileStatus)
        rStatus = stringListToString(self.db.visibleReplicaStatus)

        result = self.db.executeStoredProcedureWithCursor(
            "ps_get_replicas_for_files_in_dir", (dirID, allStatus, fStatus, rStatus)
        )
        if not result["OK"]:
            return result

        resultDict = {}
        for fileName, _fileID, seName, pfn in result["Value"]:
            resultDict.setdefault(fileName, {}).setdefault(seName, []).append(pfn)

        return S_OK(resultDict)

    def _getFileLFNs(self, fileIDs):
        """Get the file LFNs for a given list of file IDs
        We need to override this method because the base class hard codes the column names
        """

        successful = {}
        for chunks in breakListIntoChunks(fileIDs, 1000):
            # Format the filenames and status to be used in a IN clause in the sotred procedure
            formatedFileIds = intListToString(chunks)
            result = self.db.executeStoredProcedureWithCursor("ps_get_full_lfn_for_file_ids", (formatedFileIds,))
            if not result["OK"]:
                return result

            # The result contains FileID, LFN
            for row in result["Value"]:
                successful[row[0]] = row[1]

        missingIds = set(fileIDs) - set(successful)
        failed = dict.fromkeys(missingIds, "File ID not found")

        return S_OK({"Successful": successful, "Failed": failed})

    def getSEDump(self, seNames):
        """
         Return all the files at a given SE, together with checksum and size

        :param seName: list of StorageElement names

        :returns: S_OK with list of tuples (SEName, lfn, checksum, size)
        """

        seIDs = []

        for seName in seNames:
            res = self.db.seManager.findSE(seName)
            if not res["OK"]:
                return res
            seIDs.append(res["Value"])

        formatedSEIds = intListToString(seIDs)

        return self.db.executeStoredProcedureWithCursor("ps_get_se_dump", (formatedSEIds,))
