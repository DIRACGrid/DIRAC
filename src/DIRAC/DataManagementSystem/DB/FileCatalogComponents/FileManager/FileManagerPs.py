"""FileManager for ... ?"""

import os

import MySQLdb

from DIRAC import S_OK, S_ERROR
from DIRAC.DataManagementSystem.DB.FileCatalogComponents.FileManager.FileManagerBase import FileManagerBase
from DIRAC.DataManagementSystem.DB.FileCatalogComponents.Utilities import executeInTransaction
from DIRAC.Core.Utilities.List import breakListIntoChunks

# MySQL error code for duplicate entry
ER_DUP_ENTRY = 1062

# The logic of some methods is basically a copy/paste from the FileManager class,
# so I could have inherited from it. However, I did not want to depend on it


def _placeholders(values):
    """Returns a string of comma separated %s placeholders, one per value, to be used in an IN clause"""
    return ",".join(["%s"] * len(values))


class FileManagerPs(FileManagerBase):
    def __init__(self, database=None):
        super().__init__(database)

    @staticmethod
    def __validatedIntList(values):
        """Helper function to ensure list arguments are all int-only."""
        return [int(v) for v in values]

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
            result = self.db._query(
                "SELECT f.FileID FROM FC_Files f JOIN FC_DirectoryList d ON d.DirID = f.DirID "
                "WHERE d.Name = %s AND f.FileName = %s",
                args=(pathPart, filePart),
                conn=connection,
            )
            if not result["OK"]:
                return result

            fileId = result["Value"][0][0] if result["Value"] else 0

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
                fileNames = [str(fileName) for fileName in filesInDirDict[dirPath]]
                dirID = directoryPathToIds[dirPath]

                result = self.db._query(
                    "SELECT FileID, FileName FROM FC_Files "  # nosec B608
                    f"WHERE DirID = %s AND FileName IN ({_placeholders(fileNames)})",
                    args=[dirID] + fileNames,
                    conn=connection,
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

        req = (
            "SELECT f.FileName, f.DirID, f.FileID, f.Size, f.UID, u.UserName, f.GID, g.GroupName, s.Status, "
            "f.GUID, f.Checksum, f.ChecksumType, f.Type, f.CreationDate, f.ModificationDate, f.Mode "
            "FROM FC_Files f "
            "JOIN FC_Users u ON f.UID = u.UID "
            "JOIN FC_Groups g ON f.GID = g.GID "
            "JOIN FC_Statuses s ON f.Status = s.StatusID "
            "WHERE f.DirID = %s"
        )
        args = [dirID]

        if not allStatus:
            fStatus = list(self.db.visibleFileStatus)
            req += f" AND s.Status IN ({_placeholders(fStatus)})"
            args.extend(fStatus)

        if fileNames:
            fileNames = [str(fileName) for fileName in fileNames]
            req += f" AND f.FileName IN ({_placeholders(fileNames)})"
            args.extend(fileNames)

        result = self.db._query(req, args=args, conn=connection)

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

        if not fileIDs:
            return S_OK({})

        fileIDs = self.__validatedIntList(fileIDs)
        result = self.db._query(
            "SELECT f.FileID, f.Size, f.UID, f.GID, s.Status, f.GUID, f.CreationDate "  # nosec B608
            "FROM FC_Files f JOIN FC_Statuses s ON f.Status = s.StatusID "
            f"WHERE f.FileID IN ({_placeholders(fileIDs)})",
            args=fileIDs,
        )
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

        :returns: S_OK with a list of tuples (DirID, FileName, FileID)
        """

        fileValuesStrings = []
        fileValuesArgs = []
        fileDescStrings = []
        fileDescArgs = []

        for lfn in wantedLfns:
            dirID, size, s_uid, s_gid, statusID, fileName, guid, checksum, checksumtype, mode = allFileValues[lfn]
            # A missing checksum is bound as None, so it is NULL and not the string "None",
            # to stay consistent with the FC_FileInfo inserts made by FileManager
            fileValuesStrings.append("(%s, %s, %s, %s, %s, %s, %s, %s, %s, UTC_TIMESTAMP(), UTC_TIMESTAMP(), %s)")
            fileValuesArgs.extend(
                [dirID, size, s_uid, s_gid, statusID, str(fileName), str(guid), checksum, checksumtype, mode]
            )
            fileDescStrings.append("(f.DirID = %s AND f.FileName = %s)")
            fileDescArgs.extend([dirID, str(fileName)])

        fileValuesStr = ",".join(fileValuesStrings)
        fileDescStr = " OR ".join(fileDescStrings)

        def _insertFiles(cursor):
            """Insert the files and update the FC_DirectoryUsage of the FakeSE (SEID 1)"""
            cursor.execute(
                "INSERT INTO FC_Files (DirID, Size, UID, GID, Status, FileName, GUID, Checksum, ChecksumType, "  # nosec B608
                f"CreationDate, ModificationDate, Mode) VALUES {fileValuesStr}",
                fileValuesArgs,
            )
            cursor.execute(
                "INSERT INTO FC_DirectoryUsage (DirID, SEID, SESize, SEFiles) "  # nosec B608
                f"SELECT f.DirID, 1, SUM(f.Size), COUNT(*) FROM FC_Files f WHERE {fileDescStr} GROUP BY f.DirID "
                "ON DUPLICATE KEY UPDATE SESize = SESize + VALUES(SESize), SEFiles = SEFiles + VALUES(SEFiles)",
                fileDescArgs,
            )

        result = executeInTransaction(self.db, _insertFiles)
        if not result["OK"]:
            return result

        return self.db._query(
            f"SELECT f.DirID, f.FileName, f.FileID FROM FC_Files f WHERE {fileDescStr}", args=fileDescArgs  # nosec B608
        )

    def __insertFile(self, dirID, size, s_uid, s_gid, statusID, fileName, guid, checksum, checksumtype, mode):
        """Insert a single file and update the FC_DirectoryUsage of the FakeSE (SEID 1)

        :returns: S_OK(fileID)
        """

        def _insertFile(cursor):
            cursor.execute(
                "INSERT INTO FC_Files (DirID, Size, UID, GID, Status, FileName, GUID, Checksum, ChecksumType, "
                "CreationDate, ModificationDate, Mode) "
                "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, UTC_TIMESTAMP(), UTC_TIMESTAMP(), %s)",
                (dirID, size, s_uid, s_gid, statusID, str(fileName), str(guid), checksum, checksumtype, mode),
            )
            fileID = cursor.lastrowid
            cursor.execute(
                "INSERT INTO FC_DirectoryUsage (DirID, SEID, SESize, SEFiles) VALUES (%s, 1, %s, 1) "
                "ON DUPLICATE KEY UPDATE SESize = SESize + %s, SEFiles = SEFiles + 1",
                (dirID, size, size),
            )
            return fileID

        return executeInTransaction(self.db, _insertFile)

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
        if len(lfns) == 1:
            allChunks = []
            lfnsToRetry = lfns
        else:
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
            # insert
            result = self.__insertFile(*fileValues[lfn])

            if not result["OK"]:
                failed[lfn] = result["Message"]
            else:
                fileID = result["Value"]

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

        guids = [str(guid) for guid in guids]
        result = self.db._query(
            f"SELECT GUID, FileID FROM FC_Files WHERE GUID IN ({_placeholders(guids)})",  # nosec B608
            args=guids,
            conn=connection,
        )

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

        escapedGuids = [str(guid) for guid in guids]
        result = self.db._query(
            "SELECT f.GUID, CONCAT(d.Name, '/', f.FileName) FROM FC_Files f "  # nosec B608
            "JOIN FC_DirectoryList d ON f.DirID = d.DirID "
            f"WHERE f.GUID IN ({_placeholders(escapedGuids)})",
            args=escapedGuids,
            conn=connection,
        )

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
        """Delete all the replicas from the file ids and update the FC_DirectoryUsage

        :param fileIDs: list of file ids

        :returns: S_OK() or S_ERROR(msg)
        """

        if not fileIDs:
            return S_OK()

        fileIDs = self.__validatedIntList(fileIDs)
        inClause = _placeholders(fileIDs)

        def _deleteReplicas(cursor):
            # The sub query aggregates per directory and SE, so that removing two files
            # having a replica on the same SE is correctly accounted for
            cursor.execute(
                "UPDATE FC_DirectoryUsage d, "  # nosec B608
                "(SELECT d1.DirID, d1.SEID, SUM(f.Size) AS t_size, COUNT(*) AS t_file "
                "FROM FC_DirectoryUsage d1, FC_Files f, FC_Replicas r "
                "WHERE r.FileID = f.FileID AND f.DirID = d1.DirID AND r.SEID = d1.SEID "
                f"AND f.FileID IN ({inClause}) "
                "GROUP BY d1.DirID, d1.SEID) t "
                "SET d.SESize = d.SESize - t.t_size, d.SEFiles = d.SEFiles - t.t_file "
                "WHERE d.DirID = t.DirID AND d.SEID = t.SEID",
                fileIDs,
            )
            cursor.execute(f"DELETE FROM FC_Replicas WHERE FileID IN ({inClause})", fileIDs)  # nosec B608

        result = executeInTransaction(self.db, _deleteReplicas)
        if not result["OK"]:
            return result

        return S_OK()

    def __deleteFiles(self, fileIDs, connection=False):
        """Delete the files from their ids and update the FC_DirectoryUsage of the FakeSE (SEID 1).
        CAREFUL : the cascade delete also removes the replicas but will not update FC_DirectoryUsage

        :param fileIDs: list of file ids

        :returns: S_OK() or S_ERROR(msg)
        """

        if not fileIDs:
            return S_OK()

        fileIDs = self.__validatedIntList(fileIDs)
        inClause = _placeholders(fileIDs)

        def _deleteFiles(cursor):
            cursor.execute(
                "UPDATE FC_DirectoryUsage d, "  # nosec B608
                "(SELECT d1.DirID, SUM(f.Size) AS t_size, COUNT(*) AS t_file "
                "FROM FC_DirectoryList d1, FC_Files f "
                f"WHERE f.DirID = d1.DirID AND f.FileID IN ({inClause}) "
                "GROUP BY d1.DirID) t "
                "SET d.SESize = d.SESize - t.t_size, d.SEFiles = d.SEFiles - t.t_file "
                "WHERE d.DirID = t.DirID AND d.SEID = 1",
                fileIDs,
            )
            cursor.execute(f"DELETE FROM FC_Files WHERE FileID IN ({inClause})", fileIDs)  # nosec B608

        result = executeInTransaction(self.db, _deleteFiles)
        if not result["OK"]:
            return result

        return S_OK()

    def __insertMultipleReplicas(self, allReplicaValues, lfnsChunk):
        """Insert multiple replicas in one query. However, if there is a problem
            with one replica, all the query is rolled back.
        :param allReplicaValues : dictionary of tuple with all the information about possibly more
                              replica than we want to insert
        :param lfnsChunk : list of lfn that we want to insert

        :returns: S_OK with a list of tuples (FileID, SEID, RepID)
        """

        repValuesStrings = []
        repValuesArgs = []
        repDescStrings = []
        repDescArgs = []

        for lfn in lfnsChunk:
            fileID, seID, statusID, replicaType, pfn = allReplicaValues[lfn]
            repValuesStrings.append("(%s, %s, %s, %s, UTC_TIMESTAMP(), UTC_TIMESTAMP(), %s)")
            repValuesArgs.extend([fileID, seID, statusID, replicaType, str(pfn)])
            repDescStrings.append("(r.FileID = %s AND r.SEID = %s)")
            repDescArgs.extend([fileID, seID])

        repValuesStr = ",".join(repValuesStrings)
        repDescStr = " OR ".join(repDescStrings)

        def _insertReplicas(cursor):
            """Insert the replicas and update the FC_DirectoryUsage"""
            cursor.execute(
                "INSERT INTO FC_Replicas (FileID, SEID, Status, RepType, CreationDate, ModificationDate, PFN) "  # nosec B608
                f"VALUES {repValuesStr}",
                repValuesArgs,
            )
            cursor.execute(
                "INSERT INTO FC_DirectoryUsage (DirID, SEID, SESize, SEFiles) "  # nosec B608
                "SELECT f.DirID, r.SEID, SUM(f.Size), COUNT(*) "
                f"FROM FC_Files f JOIN FC_Replicas r ON f.FileID = r.FileID WHERE ({repDescStr}) "
                "GROUP BY f.DirID, r.SEID "
                "ON DUPLICATE KEY UPDATE SESize = SESize + VALUES(SESize), SEFiles = SEFiles + VALUES(SEFiles)",
                repDescArgs,
            )

        result = executeInTransaction(self.db, _insertReplicas)
        if not result["OK"]:
            return result

        return self.db._query(
            f"SELECT r.FileID, r.SEID, r.RepID FROM FC_Replicas r WHERE {repDescStr}", args=repDescArgs  # nosec B608
        )

    def __insertReplica(self, fileID, seID, statusID, replicaType, pfn):
        """Insert a single replica and update the FC_DirectoryUsage.
        If the replica already exists, its ID is returned

        :returns: S_OK(replicaID)
        """

        def _insertReplica(cursor):
            try:
                cursor.execute(
                    "INSERT INTO FC_Replicas (FileID, SEID, Status, RepType, CreationDate, ModificationDate, PFN) "
                    "VALUES (%s, %s, %s, %s, UTC_TIMESTAMP(), UTC_TIMESTAMP(), %s)",
                    (fileID, seID, statusID, replicaType, str(pfn)),
                )
            except MySQLdb.IntegrityError as e:
                # The replica already exists. Nothing was done in the transaction yet
                if e.args[0] != ER_DUP_ENTRY:
                    raise
                cursor.execute("SELECT RepID FROM FC_Replicas WHERE FileID = %s AND SEID = %s", (fileID, seID))
                return cursor.fetchone()[0]

            replicaID = cursor.lastrowid
            cursor.execute(
                "INSERT INTO FC_DirectoryUsage (DirID, SEID, SESize, SEFiles) "
                "SELECT f.DirID, %s, f.Size, 1 FROM FC_Files f WHERE f.FileID = %s "
                "ON DUPLICATE KEY UPDATE SESize = SESize + Size, SEFiles = SEFiles + 1",
                (seID, fileID),
            )
            return replicaID

        return executeInTransaction(self.db, _insertReplica)

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

        if len(lfns) == 1:
            allChunks = []
            lfnsToRetry = lfns
        else:
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
            # insert the replica and its info
            result = self.__insertReplica(*repValues[lfn])

            if not result["OK"]:
                failed[lfn] = result["Message"]
            else:
                replicaID = result["Value"]
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
            result = self.db._query(
                "SELECT RepID FROM FC_Replicas WHERE FileID = %s AND SEID = %s", args=(fileID, seID), conn=connection
            )
            if not result["OK"]:
                return result

            # if the replica exists, we add it to the dict
            if result["Value"]:
                repID = result["Value"][0][0]
                replicaDict.setdefault(fileID, {}).setdefault(seID, repID)

        return S_OK(replicaDict)

    ######################################################
    #
    # _deleteReplicas related methods
    #

    def __deleteReplica(self, fileID, seID):
        """Delete a given replica and update the FC_DirectoryUsage

        :returns: S_OK() or S_ERROR(msg)
        """

        def _deleteReplica(cursor):
            # We need to join on the replicas to make sure that there is a replica at the given se
            # otherwise the FC_DirectoryUsage would be updated for no good reason
            cursor.execute(
                "SELECT f.Size, f.DirID FROM FC_Files f JOIN FC_Replicas r ON f.FileID = r.FileID "
                "WHERE f.FileID = %s AND r.SEID = %s",
                (fileID, seID),
            )
            row = cursor.fetchone()
            if not row:
                return
            fileSize, dirID = row

            cursor.execute(
                "UPDATE FC_DirectoryUsage SET SESize = SESize - %s, SEFiles = SEFiles - 1 "
                "WHERE DirID = %s AND SEID = %s",
                (fileSize, dirID, seID),
            )
            cursor.execute("DELETE FROM FC_Replicas WHERE FileID = %s AND SEID = %s", (fileID, seID))

        return executeInTransaction(self.db, _deleteReplica)

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
            result = self.__deleteReplica(fileID, seID)
            if not result["OK"]:
                failed[lfn] = result["Message"]
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

        result = self.db._update(
            "UPDATE FC_Replicas SET Status = %s WHERE FileID = %s AND SEID = %s",
            args=(statusID, fileID, seID),
            conn=connection,
        )
        if not result["OK"]:
            return result

        affected = result["Value"]  # Affected is the number of raws updated

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

        def _moveReplica(cursor):
            """Move the replica and update the FC_DirectoryUsage

            :returns: the number of replicas moved
            """
            cursor.execute("SELECT Size, DirID FROM FC_Files WHERE FileID = %s", (fileID,))
            row = cursor.fetchone()
            if not row:
                return 0
            fileSize, dirID = row

            affected = cursor.execute(
                "UPDATE FC_Replicas SET SEID = %s WHERE FileID = %s AND SEID = %s", (newSEID, fileID, oldSEID)
            )
            # Only touch the FC_DirectoryUsage if the replica was actually moved
            if not affected:
                return 0

            cursor.execute(
                "UPDATE FC_DirectoryUsage SET SESize = SESize - %s, SEFiles = SEFiles - 1 "
                "WHERE DirID = %s AND SEID = %s",
                (fileSize, dirID, oldSEID),
            )
            cursor.execute(
                "INSERT INTO FC_DirectoryUsage (DirID, SEID, SESize, SEFiles) VALUES (%s, %s, %s, 1) "
                "ON DUPLICATE KEY UPDATE SESize = SESize + %s, SEFiles = SEFiles + 1",
                (dirID, newSEID, fileSize, fileSize),
            )
            return affected

        result = executeInTransaction(self.db, _moveReplica)
        if not result["OK"]:
            return result

        if not result["Value"]:
            return S_ERROR("Replica does not exist")
        return S_OK()

    def _setFileParameter(self, fileID, paramName, paramValue, connection=False):
        """Generic method to set a file parameter


        :param fileID : id of the file, or list of ids
        :param paramName : the file parameter you want to change
              It should be one of [ UID, GID, Status, Mode]. However, in case of
              unexpected parameter, and to stay compatible with the other Manager,
              there is a manual request done.
        :param paramValue : the value (raw, or id) to insert

        :returns: S_OK() or S_ERROR

        """
        connection = self._getConnection(connection)

        if not self.db._checkIdentifier(paramName)["OK"]:
            return S_ERROR(f"ParamName is invalid: {paramName}")

        fileIDs = list(fileID) if isinstance(fileID, (list, tuple)) else [fileID]
        if not fileIDs:
            return S_OK()

        req = (
            f"UPDATE FC_Files SET {paramName} = %s, ModificationDate = UTC_TIMESTAMP() "  # nosec B608
            f"WHERE FileID IN ({_placeholders(fileIDs)})"
        )
        result = self.db._update(req, args=[paramValue] + fileIDs, conn=connection)
        if not result["OK"]:
            return result

        # If nothing was affected, the file does not exist, but who cares...
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

        rStatus = list(self.db.visibleReplicaStatus)

        fieldNames = ["FileID", "SE", "Status", "RepType", "CreationDate", "ModificationDate", "PFN"]

        for chunks in breakListIntoChunks(fileIDs, 1000):
            chunkIDs = self.__validatedIntList(chunks)
            req = (
                "SELECT r.FileID, se.SEName, st.Status, r.RepType, r.CreationDate, r.ModificationDate, r.PFN "  # nosec B608
                "FROM FC_Replicas r "
                "JOIN FC_StorageElements se ON r.SEID = se.SEID "
                "JOIN FC_Statuses st ON r.Status = st.StatusID "
                f"WHERE r.FileID IN ({_placeholders(chunkIDs)})"
            )
            args = list(chunkIDs)
            if not allStatus:
                req += f" AND st.Status IN ({_placeholders(rStatus)})"
                args.extend(rStatus)

            result = self.db._query(req, args=args)

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

        result = self.db._query("SELECT COUNT(FileID) FROM FC_Files WHERE DirID = %s", args=(dirId,))
        if not result["OK"]:
            return result

        res = S_OK(result["Value"][0][0])
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

        req = (
            "SELECT f.FileName, f.FileID, s.SEName, r.PFN FROM FC_Replicas r "
            "JOIN FC_Files f ON f.FileID = r.FileID "
            "JOIN FC_StorageElements s ON s.SEID = r.SEID "
        )
        args = [dirID]

        if not allStatus:
            fStatus = list(self.db.visibleFileStatus)
            rStatus = list(self.db.visibleReplicaStatus)
            req += (
                "JOIN FC_Statuses fst ON f.Status = fst.StatusID "
                "JOIN FC_Statuses rst ON r.Status = rst.StatusID "
                f"WHERE f.DirID = %s AND fst.Status IN ({_placeholders(fStatus)}) "
                f"AND rst.Status IN ({_placeholders(rStatus)})"
            )
            args.extend(fStatus)
            args.extend(rStatus)
        else:
            req += "WHERE f.DirID = %s"

        result = self.db._query(req, args=args)
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
            chunkIDs = self.__validatedIntList(chunks)
            result = self.db._query(
                "SELECT f.FileID, CONCAT(d.Name, '/', f.FileName) FROM FC_Files f "  # nosec B608
                "JOIN FC_DirectoryList d ON f.DirID = d.DirID "
                f"WHERE f.FileID IN ({_placeholders(chunkIDs)})",
                args=chunkIDs,
            )
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

        if not seIDs:
            return S_OK(())

        seIDs = self.__validatedIntList(seIDs)

        return self.db._query(
            "SELECT s.SEName, CONCAT(d.Name, '/', f.FileName), f.Checksum, f.Size FROM FC_Files f "  # nosec B608
            "JOIN FC_Replicas r ON f.FileID = r.FileID "
            "JOIN FC_DirectoryList d ON d.DirID = f.DirID "
            "JOIN FC_StorageElements s ON r.SEID = s.SEID "
            f"WHERE s.SEID IN ({_placeholders(seIDs)})",
            args=seIDs,
        )
