""" FileManager (add doc here)
"""
import os

from DIRAC import S_OK, S_ERROR, gLogger
from DIRAC.DataManagementSystem.DB.FileCatalogComponents.FileManager.FileManagerBase import FileManagerBase
from DIRAC.Core.Utilities.List import stringListToString, intListToString, breakListIntoChunks
from DIRAC.Core.Utilities.File import makeGuid
from DIRAC.DataManagementSystem.DB.FileCatalogComponents.Utilities import getIDSelectString

DEBUG = 0


class FileManager(FileManagerBase):
    ######################################################
    #
    # The all important _findFiles and _getDirectoryFiles methods
    #

    def _findFiles(self, lfns, metadata=["FileID"], allStatus=False, connection=False):
        """Find file ID if it exists for the given list of LFNs"""

        connection = self._getConnection(connection)
        dirDict = self._getFileDirectories(lfns)
        failed = {}
        result = self.db.dtree.findDirs(list(dirDict))
        if not result["OK"]:
            return result
        directoryIDs = result["Value"]

        for dirPath in dirDict:
            if dirPath not in directoryIDs:
                for fileName in dirDict[dirPath]:
                    fname = f"{dirPath}/{fileName}"
                    fname = fname.replace("//", "/")
                    failed[fname] = "No such file or directory"

        successful = {}
        for dirPath in directoryIDs:
            fileNames = dirDict[dirPath]
            res = self._getDirectoryFiles(
                directoryIDs[dirPath], fileNames, metadata, allStatus=allStatus, connection=connection
            )
            if (not res["OK"]) or (not res["Value"]):
                error = res.get("Message", "No such file or directory")
                for fileName in fileNames:
                    fname = f"{dirPath}/{fileName}"
                    fname = fname.replace("//", "/")
                    failed[fname] = error
            else:
                for fileName, fileDict in res["Value"].items():
                    fname = f"{dirPath}/{fileName}"
                    fname = fname.replace("//", "/")
                    successful[fname] = fileDict
            for fileName in fileNames:
                if fileName not in res["Value"]:
                    fname = f"{dirPath}/{fileName}"
                    fname = fname.replace("//", "/")
                    failed[fname] = "No such file or directory"
        return S_OK({"Successful": successful, "Failed": failed})

    def _findFileIDs(self, lfns, connection=False):
        """Find lfn <-> FileID correspondence"""
        connection = self._getConnection(connection)
        dirDict = self._getFileDirectories(lfns)
        failed = {}
        successful = {}
        result = self.db.dtree.findDirs(list(dirDict))
        if not result["OK"]:
            return result
        directoryIDs = result["Value"]
        directoryPaths = {}

        for dirPath in dirDict:
            if dirPath not in directoryIDs:
                for fileName in dirDict[dirPath]:
                    fname = f"{dirPath}/{fileName}"
                    fname = fname.replace("//", "/")
                    failed[fname] = "No such file or directory"
            else:
                directoryPaths[directoryIDs[dirPath]] = dirPath

        for dirIDs in breakListIntoChunks(list(directoryIDs), 1000):
            args = []
            wheres = []
            for dirPath in dirIDs:
                fileNames = dirDict[dirPath]
                dirID = directoryIDs[dirPath]
                whereStr = "( DirID=%s AND FileName IN ("
                whereStr += ",".join(["%s"] * len(fileNames))
                whereStr += ") )"
                args.append(dirID)
                args.extend(fileNames)
                wheres.append(whereStr)

            req = "SELECT FileName,DirID,FileID FROM FC_Files WHERE "
            req += " OR ".join(wheres)
            result = self.db._query(req, args=args, conn=connection)
            if not result["OK"]:
                return result
            for fileName, dirID, fileID in result["Value"]:
                fname = f"{directoryPaths[dirID]}/{fileName}"
                fname = fname.replace("//", "/")
                successful[fname] = fileID

        for lfn in lfns:
            if lfn not in successful:
                failed[lfn] = "No such file or directory"

        return S_OK({"Successful": successful, "Failed": failed})

    def _getDirectoryFiles(self, dirID, fileNames, metadata_input, allStatus=False, connection=False):
        """Get the metadata for files in the same directory"""
        metadata = list(metadata_input)

        connection = self._getConnection(connection)
        # metadata can be any of ['FileID','Size','UID','GID','Status','Checksum','ChecksumType',
        # 'Type','CreationDate','ModificationDate','Mode']
        req = "SELECT FileName,DirID,FileID,Size,UID,GID,Status FROM FC_Files WHERE DirID=%s"
        args = [str(dirID)]
        if not allStatus:
            statusIDs = []
            for status in self.db.visibleFileStatus:
                res = self._getStatusInt(status, connection=connection)
                if res["OK"]:
                    statusIDs.append(res["Value"])
            if statusIDs:
                req += " AND Status IN ("
                req += ",".join(["%s"] * len(statusIDs))
                req += ")"
                args.extend(statusIDs)
        if fileNames:
            req += " AND FileName IN ("
            req += ",".join(["%s"] * len(fileNames))
            req += ")"
            args.extend(fileNames)
        res = self.db._query(req, args=args, conn=connection)
        if not res["OK"]:
            return res
        fileNameIDs = res["Value"]
        if not fileNameIDs:
            return S_OK({})
        filesDict = {}
        # If we only requested the FileIDs then there is no need to do anything else
        if metadata == ["FileID"]:
            for fileName, dirID, fileID, size, uid, gid, status in fileNameIDs:
                filesDict[fileName] = {"FileID": fileID}
            return S_OK(filesDict)
        # Otherwise get the additionally requested metadata from the FC_FileInfo table
        files = {}
        userDict = {}
        groupDict = {}
        for fileName, dirID, fileID, size, uid, gid, status in fileNameIDs:
            filesDict[fileID] = fileName
            files[fileName] = {}
            if "Size" in metadata:
                files[fileName]["Size"] = size
            if "DirID" in metadata:
                files[fileName]["DirID"] = dirID
            if "UID" in metadata:
                files[fileName]["UID"] = uid
                if uid in userDict:
                    owner = userDict[uid]
                else:
                    owner = "unknown"
                    result = self.db.ugManager.getUserName(uid)
                    if result["OK"]:
                        owner = result["Value"]
                    userDict[uid] = owner
                files[fileName]["Owner"] = owner
            if "GID" in metadata:
                files[fileName]["GID"] = gid
                if gid in groupDict:
                    group = groupDict[gid]
                else:
                    group = "unknown"
                    result = self.db.ugManager.getGroupName(gid)
                    if result["OK"]:
                        group = result["Value"]
                    groupDict[gid] = group
                files[fileName]["OwnerGroup"] = group
            if "Status" in metadata:
                files[fileName]["Status"] = self._getIntStatus(status).get("Value", status)
        for element in ["FileID", "Size", "DirID", "UID", "GID", "Status"]:
            if element in metadata:
                metadata.remove(element)
        metadata.append("FileID")
        metadata.reverse()
        for field in metadata:
            if not self.db._checkIdentifier(field)["OK"]:
                return S_ERROR(f"Bad metadata field name: {field}")
        req = "SELECT "
        req += ",".join(metadata)
        req += " FROM FC_FileInfo WHERE FileID IN ("
        req += ",".join(["%s"] * len(filesDict))
        req += ")"
        res = self.db._query(req, args=filesDict.keys(), conn=connection)
        if not res["OK"]:
            return res
        for tuple_ in res["Value"]:
            fileID = tuple_[0]
            rowDict = dict(zip(metadata, tuple_))
            files[filesDict[fileID]].update(rowDict)
        return S_OK(files)

    def _getDirectoryFileIDs(self, dirID, requestString=False):
        """Get a list of IDs for all the files stored in given directories or their
            subdirectories
        :param mixt dirID: single directory ID or a list of directory IDs
        :param boolean requestString: if True return result as a SQL SELECT string
        :return: list of file IDs or SELECT string
        """

        result = getIDSelectString(dirID)
        if not result["OK"]:
            return result
        dirListString = result["Value"]

        if requestString:
            # TODO: Mogrify this instead of using string construction!
            req = "SELECT FileID FROM FC_Files WHERE DirID IN ("
            req += dirListString
            req += ")"
            return S_OK(req)

        if not isinstance(dirID, (list, tuple)):
            dirList = [dirID]
        else:
            dirList = dirID
        req = "SELECT FileID,DirID,FileName FROM FC_Files WHERE DirID IN ("
        req += ",".join(["%s"] * len(dirList))
        req += ")"
        result = self.db._query(req, args=dirList)
        return result

    def _getFileMetadataByID(self, fileIDs, connection=False):
        """Get standard file metadata for a list of files specified by FileID"""
        req = "SELECT FileID,Size,UID,GID,Status FROM FC_Files WHERE FileID in ("
        req += ",".join(["%s"] * len(fileIDs))
        req += ")"
        result = self.db._query(req, args=fileIDs, conn=connection)
        if not result["OK"]:
            return result
        resultDict = {}
        for fileID, size, uid, gid, status in result["Value"]:
            resultDict[fileID] = {
                "Size": int(size),
                "UID": int(uid),
                "GID": int(gid),
                "Status": self._getIntStatus(status).get("Value", status),
            }

        req = "SELECT FileID,GUID,CreationDate from FC_FileInfo WHERE FileID in ("
        req += ",".join(["%s"] * len(fileIDs))
        req += ")"
        result = self.db._query(req, args=fileIDs, conn=connection)
        if not result["OK"]:
            return result
        for fileID, guid, date in result["Value"]:
            resultDict.setdefault(fileID, {})
            resultDict[fileID].update({"GUID": guid, "CreationDate": date})

        return S_OK(resultDict)

    ######################################################
    #
    # _addFiles related methods
    #

    def _insertFiles(self, lfns, uid, gid, connection=False):
        connection = self._getConnection(connection)
        # Add the files
        failed = {}
        insertTuples = []
        res = self._getStatusInt("AprioriGood", connection=connection)
        statusID = 0
        if res["OK"]:
            statusID = res["Value"]

        directorySESizeDict = {}
        for lfn in lfns.keys():
            dirID = lfns[lfn]["DirID"]
            fileName = os.path.basename(lfn)
            size = lfns[lfn]["Size"]
            ownerDict = lfns[lfn].get("Owner", None)
            s_uid = uid
            s_gid = gid
            if ownerDict:
                result = self.db.ugManager.getUserAndGroupID(ownerDict)
                if result["OK"]:
                    s_uid, s_gid = result["Value"]
            insertTuples.append((dirID, size, s_uid, s_gid, statusID, fileName))
            directorySESizeDict.setdefault(dirID, {})
            directorySESizeDict[dirID].setdefault(0, {"Files": 0, "Size": 0})
            directorySESizeDict[dirID][0]["Size"] += lfns[lfn]["Size"]
            directorySESizeDict[dirID][0]["Files"] += 1

        req = "INSERT INTO FC_Files (DirID,Size,UID,GID,Status,FileName) VALUES (%s,%s,%s,%s,%s,%s)"
        res = self.db._updatemany(req, data=insertTuples, conn=connection)
        if not res["OK"]:
            return res
        # Get the fileIDs for the inserted files
        res = self._findFiles(list(lfns), ["FileID"], connection=connection)
        if not res["OK"]:
            for lfn in list(lfns):
                failed[lfn] = "Failed post insert check"
                lfns.pop(lfn)
        else:
            failed.update(res["Value"]["Failed"])
            for lfn in res["Value"]["Failed"]:
                lfns.pop(lfn)
            for lfn, fileDict in res["Value"]["Successful"].items():
                lfns[lfn]["FileID"] = fileDict["FileID"]
        insertTuples = []
        toDelete = []
        for lfn in lfns:
            fileInfo = lfns[lfn]
            fileID = fileInfo["FileID"]
            dirID = fileInfo["DirID"]
            checksum = fileInfo["Checksum"]
            checksumtype = fileInfo.get("ChecksumType", "Adler32")
            guid = fileInfo.get("GUID", "")
            mode = fileInfo.get("Mode", self.db.umask)
            toDelete.append(fileID)
            insertTuples.append((fileID, guid, checksum, checksumtype, mode))
        if insertTuples:
            req = (
                "INSERT INTO FC_FileInfo ("
                "FileID,GUID,Checksum,ChecksumType,CreationDate,ModificationDate,Mode"
                ") VALUES (%s, %s, %s, %s, UTC_TIMESTAMP(), UTC_TIMESTAMP(), %s)"
            )
            res = self.db._updatemany(req, data=insertTuples, conn=connection)
            if not res["OK"]:
                self._deleteFiles(toDelete, connection=connection)
                for lfn in list(lfns):
                    failed[lfn] = res["Message"]
                    lfns.pop(lfn)
            else:
                # Update the directory usage
                result = self._updateDirectoryUsage(directorySESizeDict, "+", connection=connection)
                if not result["OK"]:
                    gLogger.warn("Failed to insert FC_DirectoryUsage", result["Message"])

        return S_OK({"Successful": lfns, "Failed": failed})

    def _getFileIDFromGUID(self, guid, connection=False):
        connection = self._getConnection(connection)
        if not guid:
            return S_OK({})
        if not isinstance(guid, (list, tuple)):
            guid = [guid]
        req = "SELECT FileID,GUID FROM FC_FileInfo WHERE GUID IN ("
        req += ",".join(["%s"] * len(guid))
        req += ")"
        res = self.db._query(req, args=guid, conn=connection)
        if not res["OK"]:
            return res
        guidDict = {}
        for fileID, guid in res["Value"]:
            guidDict[guid] = fileID
        return S_OK(guidDict)

    def getLFNForGUID(self, guids, connection=False):
        """Returns the lfns matching given guids"""

        connection = self._getConnection(connection)
        if not guids:
            return S_OK({})
        if not isinstance(guids, (list, tuple)):
            guids = [guids]
        req = "SELECT f.FileID, f.FileName, fi.GUID, f.DirID FROM FC_FileInfo fi"
        req += " JOIN FC_Files f on fi.FileID = f.FileID WHERE GUID IN ("
        req += ",".join(["%s"] * len(guids))
        req += ")"
        res = self.db._query(req, args=guids, conn=connection)
        if not res["OK"]:
            return res

        fileIDguid = {}
        dirIDFileIDs = {}
        fileIDName = {}
        for fileID, fileName, guid, dirID in res["Value"]:
            fileIDguid[fileID] = guid
            dirIDFileIDs.setdefault(dirID, []).append(fileID)
            fileIDName[fileID] = fileName

        res = self.db.dtree.getDirectoryPaths(list(dirIDFileIDs))
        if not res["OK"]:
            return res

        dirIDName = res["Value"]

        guidLFN = {
            fileIDguid[fId]: os.path.join(dirIDName[dId], fileIDName[fId])
            for dId in dirIDName
            for fId in dirIDFileIDs[dId]
        }
        failedGuid = set(guids) - set(guidLFN)
        failed = dict.fromkeys(failedGuid, "GUID does not exist") if failedGuid else {}

        return S_OK({"Successful": guidLFN, "Failed": failed})

    ######################################################
    #
    # _deleteFiles related methods
    #

    def _deleteFiles(self, fileIDs, connection=False):
        connection = self._getConnection(connection)
        replicaPurge = self.__deleteFileReplicas(fileIDs)
        filePurge = self.__deleteFiles(fileIDs, connection=connection)
        if not replicaPurge["OK"]:
            return replicaPurge
        if not filePurge["OK"]:
            return filePurge
        return S_OK()

    def __deleteFileReplicas(self, fileIDs, connection=False):
        connection = self._getConnection(connection)
        res = self.__getFileIDReplicas(fileIDs, connection=connection)
        if not res["OK"]:
            return res
        return self.__deleteReplicas(list(res["Value"]), connection=connection)

    def __deleteFiles(self, fileIDs, connection=False):
        connection = self._getConnection(connection)
        if not isinstance(fileIDs, (list, tuple)):
            fileIDs = [fileIDs]
        if not fileIDs:
            return S_OK()
        failed = []
        for table in ["FC_Files", "FC_FileInfo"]:
            req = "DELETE FROM "
            req += table
            req += " WHERE FileID in ("
            req += ",".join(["%s"] * len(fileIDs))
            req += ")"
            res = self.db._update(req, args=fileIDs, conn=connection)
            if not res["OK"]:
                gLogger.error(f"Failed to remove files from table {table}", res["Message"])
                failed.append(table)
        if failed:
            return S_ERROR(f"Failed to remove files from {stringListToString(failed)}")
        return S_OK()

    ######################################################
    #
    # _addReplicas related methods
    #

    def _insertReplicas(self, lfns, master=False, connection=False):
        connection = self._getConnection(connection)
        # Add the files
        failed = {}
        successful = {}
        insertTuples = []
        fileIDLFNs = {}
        res = self._getStatusInt("AprioriGood")
        statusID = 0
        if res["OK"]:
            statusID = res["Value"]
        for lfn in list(lfns):
            fileID = lfns[lfn]["FileID"]
            fileIDLFNs[fileID] = lfn
            seName = lfns[lfn]["SE"]
            if isinstance(seName, str):
                seList = [seName]
            elif isinstance(seName, list):
                seList = seName
            else:
                return S_ERROR(f"Illegal type of SE list: {str(type(seName))}")
            for seName in seList:
                res = self.db.seManager.findSE(seName)
                if not res["OK"]:
                    failed[lfn] = res["Message"]
                    lfns.pop(lfn)
                    continue
                seID = res["Value"]
                insertTuples.append((fileID, seID))
        if not master:
            res = self._getRepIDsForReplica(insertTuples, connection=connection)
            if not res["OK"]:
                return res
            for fileID, repDict in res["Value"].items():
                for seID, repID in repDict.items():
                    successful[fileIDLFNs[fileID]] = True
                    insertTuples.remove((fileID, seID))

        if not insertTuples:
            return S_OK({"Successful": successful, "Failed": failed})

        req = "INSERT INTO FC_Replicas "
        req += "(FileID,SEID,Status) VALUES (%s,%s,%s)"
        args = tuple((x[0], x[1], statusID) for x in insertTuples)
        res = self.db._updatemany(req, data=args, conn=connection)
        if not res["OK"]:
            return res
        res = self._getRepIDsForReplica(insertTuples, connection=connection)
        if not res["OK"]:
            return res
        replicaDict = res["Value"]
        directorySESizeDict = {}
        for fileID, repDict in replicaDict.items():
            lfn = fileIDLFNs[fileID]
            dirID = lfns[lfn]["DirID"]
            directorySESizeDict.setdefault(dirID, {})
            for seID, repID in repDict.items():
                lfns[lfn]["RepID"] = repID
                directorySESizeDict[dirID].setdefault(seID, {"Files": 0, "Size": 0})
                directorySESizeDict[dirID][seID]["Size"] += lfns[lfn]["Size"]
                directorySESizeDict[dirID][seID]["Files"] += 1

        replicaType = "Replica"
        if master:
            replicaType = "Master"
        insertReplicas = []
        toDelete = []
        for lfn in lfns.keys():
            fileDict = lfns[lfn]
            repID = fileDict.get("RepID", 0)
            if repID:
                pfn = fileDict["PFN"]
                toDelete.append(repID)
                insertReplicas.append((str(repID), replicaType, pfn))
        if insertReplicas:
            req = "INSERT INTO FC_ReplicaInfo "
            req += "(RepID,RepType,CreationDate,ModificationDate,PFN) "
            req += "VALUES (%s, %s, UTC_TIMESTAMP(), UTC_TIMESTAMP(), %s)"
            res = self.db._updatemany(req, data=insertReplicas, conn=connection)
            if not res["OK"]:
                for lfn in lfns.keys():
                    failed[lfn] = res["Message"]
                self.__deleteReplicas(toDelete, connection=connection)
            else:
                # Update the directory usage
                self._updateDirectoryUsage(directorySESizeDict, "+", connection=connection)
                for lfn in lfns.keys():
                    successful[lfn] = True
        return S_OK({"Successful": successful, "Failed": failed})

    def _getRepIDsForReplica(self, replicaTuples, connection=False):
        connection = self._getConnection(connection)
        queryTuples = []
        for fileID, seID in replicaTuples:
            queryTuples.append((fileID, seID))
        if not queryTuples:
            return S_OK({})
        req = "SELECT RepID,FileID,SEID FROM FC_Replicas WHERE (FileID,SEID) IN ("
        req += ",".join(["(%s,%s)"] * len(queryTuples))
        req += ")"
        # Flatten queryTuples into args
        args = tuple(f for t in queryTuples for f in t)
        res = self.db._query(req, args=args, conn=connection)
        if not res["OK"]:
            return res
        replicaDict = {}
        for repID, fileID, seID in res["Value"]:
            replicaDict.setdefault(fileID, {})
            replicaDict[fileID][seID] = repID

        return S_OK(replicaDict)

    ######################################################
    #
    # _deleteReplicas related methods
    #

    def _deleteReplicas(self, lfns, connection=False):
        connection = self._getConnection(connection)
        failed = {}
        successful = {}
        res = self._findFiles(list(lfns), ["DirID", "FileID", "Size"], connection=connection)

        # If the file does not exist we consider the deletion successful
        for lfn, error in res["Value"]["Failed"].items():
            if error == "No such file or directory":
                successful[lfn] = True
            else:
                failed[lfn] = error

        lfnFileIDDict = res["Value"]["Successful"]
        toRemove = []
        directorySESizeDict = {}
        for lfn, fileDict in lfnFileIDDict.items():
            fileID = fileDict["FileID"]
            se = lfns[lfn]["SE"]
            if isinstance(se, str):
                res = self.db.seManager.findSE(se)
                if not res["OK"]:
                    return res
            seID = res["Value"]
            toRemove.append((fileID, seID))
            # Now prepare the storage usage update
            dirID = fileDict["DirID"]
            directorySESizeDict.setdefault(dirID, {})
            directorySESizeDict[dirID].setdefault(seID, {"Files": 0, "Size": 0})
            directorySESizeDict[dirID][seID]["Size"] += fileDict["Size"]
            directorySESizeDict[dirID][seID]["Files"] += 1
        res = self._getRepIDsForReplica(toRemove, connection=connection)
        if not res["OK"]:
            for lfn in lfnFileIDDict.keys():
                failed[lfn] = res["Message"]
        else:
            repIDs = []
            for fileID, seDict in res["Value"].items():
                for seID, repID in seDict.items():
                    repIDs.append(repID)
            res = self.__deleteReplicas(repIDs, connection=connection)
            if not res["OK"]:
                for lfn in lfnFileIDDict.keys():
                    failed[lfn] = res["Message"]
            else:
                # Update the directory usage
                self._updateDirectoryUsage(directorySESizeDict, "-", connection=connection)
                for lfn in lfnFileIDDict.keys():
                    successful[lfn] = True
        return S_OK({"Successful": successful, "Failed": failed})

    def __deleteReplicas(self, repIDs, connection=False):
        connection = self._getConnection(connection)
        if not isinstance(repIDs, (list, tuple)):
            repIDs = [repIDs]
        if not repIDs:
            return S_OK()
        failed = []
        for table in ["FC_Replicas", "FC_ReplicaInfo"]:
            req = "DELETE FROM "
            req += table
            req += " WHERE RepID in ("
            req += ",".join(["%s"] * len(repIDs))
            req += ")"
            res = self.db._update(req, args=repIDs, conn=connection)
            if not res["OK"]:
                gLogger.error(f"Failed to remove replicas from table {table}", res["Message"])
                failed.append(table)
        if failed:
            return S_ERROR(f"Failed to remove replicas from {stringListToString(failed)}")
        return S_OK()

    ######################################################
    #
    # _setReplicaStatus _setReplicaHost _setReplicaParameter methods
    # _setFileParameter method
    #

    def _setReplicaStatus(self, fileID, se, status, connection=False):
        if status not in self.db.validReplicaStatus:
            return S_ERROR(f"Invalid replica status {status}")
        connection = self._getConnection(connection)
        res = self._getStatusInt(status, connection=connection)
        if not res["OK"]:
            return res
        statusID = res["Value"]
        res = self.__getRepIDForReplica(fileID, se, connection=connection)
        if not res["OK"]:
            return res
        if not res["Exists"]:
            return S_ERROR("Replica does not exist")
        if not res["Value"]:
            return res
        repID = res["Value"]
        req = "UPDATE FC_Replicas SET Status=%s WHERE RepID=%s"
        return self.db._update(req, args=(statusID, repID), conn=connection)

    def _setReplicaHost(self, fileID, se, newSE, connection=False):
        connection = self._getConnection(connection)
        res = self.db.seManager.findSE(newSE)
        if not res["OK"]:
            return res
        newSE = res["Value"]
        res = self.__getRepIDForReplica(fileID, se, connection=connection)
        if not res["OK"]:
            return res
        if not res["Value"]:
            return res
        repID = res["Value"]
        req = "UPDATE FC_Replicas SET SEID=%s WHERE RepID = %s"
        return self.db._update(req, args=(newSE, repID), conn=connection)

    def _setReplicaParameter(self, fileID, se, paramName, paramValue, connection=False):
        connection = self._getConnection(connection)
        res = self.__getRepIDForReplica(fileID, se, connection=connection)
        if not res["OK"]:
            return res
        if not res["Value"]:
            return res
        repID = res["Value"]
        if not self.db._checkIdentifier(paramName)["OK"]:
            return S_ERROR(f"Invalid paramName: {paramName}")
        req = "UPDATE FC_ReplicaInfo "
        req += f"SET {paramName}=%s, ModificationDate=UTC_TIMESTAMP() "
        req += "WHERE RepID = %s"
        args = (paramValue, repID)
        return self.db._update(req, args=args, conn=connection)

    def _setFileParameter(self, fileID, paramName, paramValue, connection=False):
        # TODO: This function should really be split into a version that takes a fileID list
        #       and a version that can take an SQL select statement
        connection = self._getConnection(connection)

        result = getIDSelectString(fileID)
        if not result["OK"]:
            return result
        fileIDString = result["Value"]

        args = []
        if paramName in ["UID", "GID", "Status", "Size"]:
            # Treat primary file attributes specially
            # Different statement for the fileIDString with SELECT is for performance optimization
            # since in this case the MySQL engine manages to use index on FileID.
            if "select" in fileIDString.lower():
                tmpreq = f"UPDATE FC_Files as FF1, ( {fileIDString} ) as FF2 "
                tmpreq += f"SET {paramName}=%s WHERE FF1.FileID=FF2.FileID"
                tmpargs = [paramValue]
            else:
                tmpreq = "UPDATE FC_Files "
                tmpreq += f"SET {paramName}=%s "
                tmpreq += f"WHERE FileID IN ({fileIDString})"
                tmpargs = [paramValue]
            result = self.db._update(tmpreq, args=tmpargs, conn=connection)
            if not result["OK"]:
                return result
            if "select" in fileIDString.lower():
                req = f"UPDATE FC_FileInfo as FF1, ( {fileIDString} ) as FF2"
                req += " SET ModificationDate=UTC_TIMESTAMP() WHERE FF1.FileID=FF2.FileID"
            else:
                req = "UPDATE FC_FileInfo SET ModificationDate=UTC_TIMESTAMP() WHERE FileID IN ("
                req += fileIDString
                req += ")"
        else:
            # Different statement for the fileIDString with SELECT is for performance optimization
            # since in this case the MySQL engine manages to use index on FileID.
            if "select" in fileIDString.lower():
                req = f"UPDATE FC_FileInfo as FF1, ( {fileIDString} ) as FF2 "
                req += f"SET {paramName}=%s, ModificationDate=UTC_TIMESTAMP() "
                req += "WHERE FF1.FileID=FF2.FileID"
                args.append(paramValue)
            else:
                req = "UPDATE FC_FileInfo "
                req += f"SET {paramName}=%s, ModificationDate=UTC_TIMESTAMP() "
                req += f"WHERE FileID IN ({fileIDString})"
                args.append(paramValue)
        return self.db._update(req, args=args, conn=connection)

    def __getRepIDForReplica(self, fileID, seID, connection=False):
        connection = self._getConnection(connection)
        if isinstance(seID, str):
            res = self.db.seManager.findSE(seID)
            if not res["OK"]:
                return res
            seID = res["Value"]
        res = self._getRepIDsForReplica([(fileID, seID)], connection=connection)
        if not res["OK"]:
            return res
        if not res["Value"]:
            result = S_OK()
            result["Exists"] = False
        else:
            result = S_OK(res["Value"][fileID][seID])
            result["Exists"] = True
        return result

    ######################################################
    #
    # _getFileReplicas related methods
    #

    def _getFileReplicas(self, fileIDs, fields_input=["PFN"], allStatus=False, connection=False):
        """Get replicas for the given list of files specified by their fileIDs"""
        fields = list(fields_input)
        connection = self._getConnection(connection)
        res = self.__getFileIDReplicas(fileIDs, allStatus=allStatus, connection=connection)
        if not res["OK"]:
            return res
        fileIDDict = res["Value"]
        if fileIDDict:
            if "Status" in fields:
                fields.remove("Status")
            repIDDict = {}
            if fields:
                for field in fields:
                    if not self.db._checkIdentifier(field)["OK"]:
                        return S_ERROR(f"Invalid field name: {field}")
                req = "SELECT "
                req += ",".join(["RepID"] + fields)
                req += " FROM FC_ReplicaInfo WHERE RepID IN ("
                req += ",".join(["%s"] * len(fileIDDict))
                req += ")"
                res = self.db._query(req, args=fileIDDict.keys(), conn=connection)
                if not res["OK"]:
                    return res
                for tuple_ in res["Value"]:
                    repID = tuple_[0]
                    repIDDict[repID] = dict(zip(fields, tuple_[1:]))
                    statusID = fileIDDict[repID][2]
                    res = self._getIntStatus(statusID, connection=connection)
                    if not res["OK"]:
                        continue
                    repIDDict[repID]["Status"] = res["Value"]
            else:
                for repID in fileIDDict:
                    statusID = fileIDDict[repID][2]
                    res = self._getIntStatus(statusID, connection=connection)
                    if not res["OK"]:
                        continue
                    repIDDict[repID] = {"Status": res["Value"]}
        seDict = {}
        replicas = {}
        for repID in fileIDDict.keys():
            fileID, seID, statusID = fileIDDict[repID]
            replicas.setdefault(fileID, {})
            if seID not in seDict:
                res = self.db.seManager.getSEName(seID)
                if not res["OK"]:
                    continue
                seDict[seID] = res["Value"]
            seName = seDict[seID]
            replicas[fileID][seName] = repIDDict.get(repID, {})

        if len(replicas) != len(fileIDs):
            for fileID in fileIDs:
                if fileID not in replicas:
                    replicas[fileID] = {}

        return S_OK(replicas)

    def __getFileIDReplicas(self, fileIDs, allStatus=False, connection=False):
        connection = self._getConnection(connection)
        if not fileIDs:
            return S_ERROR("No such file or directory")
        req = "SELECT FileID,SEID,RepID,Status FROM FC_Replicas WHERE FileID IN ("
        req += ",".join(["%s"] * len(fileIDs))
        req += ")"
        args = list(fileIDs)
        if not allStatus:
            statusIDs = []
            for status in self.db.visibleReplicaStatus:
                result = self._getStatusInt(status, connection=connection)
                if result["OK"]:
                    statusIDs.append(result["Value"])
            req += " AND Status IN ("
            req += ",".join(["%s"] * len(statusIDs))
            req += ")"
            args.extend(statusIDs)
        res = self.db._query(req, args=args, conn=connection)
        if not res["OK"]:
            return res
        fileIDDict = {}
        for fileID, seID, repID, statusID in res["Value"]:
            fileIDDict[repID] = (fileID, seID, statusID)
        return S_OK(fileIDDict)

    def _getDirectoryReplicas(self, dirID, allStatus=False, connection=False):
        """Get replicas for files in a given directory"""
        replicaStatusIDs = []
        if not allStatus:
            for status in self.db.visibleReplicaStatus:
                result = self._getStatusInt(status, connection=connection)
                if result["OK"]:
                    replicaStatusIDs.append(result["Value"])
        fileStatusIDs = []
        if not allStatus:
            for status in self.db.visibleFileStatus:
                result = self._getStatusInt(status, connection=connection)
                if result["OK"]:
                    fileStatusIDs.append(result["Value"])

        if not self.db.lfnPfnConvention or self.db.lfnPfnConvention == "Weak":
            req = "SELECT FF.FileName,FR.FileID,FR.SEID,FI.PFN FROM FC_Files as FF,"
            req += " FC_Replicas as FR, FC_ReplicaInfo as FI"
            req += " WHERE FF.FileID=FR.FileID AND FR.RepID=FI.RepID AND FF.DirID=%s "
            args = [str(dirID)]
            if replicaStatusIDs:
                req += " AND FR.Status in ("
                req += ",".join(["%s"] * len(replicaStatusIDs))
                req += ")"
                args.extend(replicaStatusIDs)
            if fileStatusIDs:
                req += " AND FF.Status in ("
                req += ",".join(["%s"] * len(fileStatusIDs))
                req += ")"
                args.extend(fileStatusIDs)
        else:
            req = "SELECT FF.FileName,FR.FileID,FR.SEID,'' FROM FC_Files as FF,"
            req += " FC_Replicas as FR"
            req += " WHERE FF.FileID=FR.FileID AND FF.DirID=%s "
            args = [str(dirID)]
            if replicaStatusIDs:
                req += " AND FR.Status in ("
                req += ",".join(["%s"] * len(replicaStatusIDs))
                req += ")"
                args.extend(replicaStatusIDs)
            if fileStatusIDs:
                req += " AND FF.Status in ("
                req += ",".join(["%s"] * len(fileStatusIDs))
                req += ")"
                args.extend(fileStatusIDs)

        result = self.db._query(req, args=args, conn=connection)
        return result

    def repairFileTables(self, connection=False):
        """Repair FC_FileInfo table by adding missing records as compaired to the FC_Files table"""

        req = "SELECT F1.FileID, F2.FileID from FC_Files as F1 LEFT JOIN FC_FileInfo as F2 "
        req += "ON F1.FileID=F2.FileID WHERE F2.FileID IS NULL"
        result = self.db._query(req, conn=connection)
        if not result["OK"]:
            return result

        fileIDsToAdd = []
        for f1, _ in result["Value"]:
            fileIDsToAdd.append(f1)

        if not fileIDsToAdd:
            return S_OK(0)

        nFiles = len(fileIDsToAdd)
        insertTuples = []
        for fileID in fileIDsToAdd:
            guid = makeGuid()
            insertTuples.append((str(fileID), guid, self.db.umask))

        req = "INSERT INTO FC_FileInfo "
        req += "(FileID,GUID,CreationDate,ModificationDate,Mode) "
        req += "VALUES (%s, %s, UTC_TIMESTAMP(), UTC_TIMESTAMP(), %s)"
        result = self.db._updatemany(req, data=insertTuples, conn=connection)
        if not result["OK"]:
            return result

        return S_OK(nFiles)
