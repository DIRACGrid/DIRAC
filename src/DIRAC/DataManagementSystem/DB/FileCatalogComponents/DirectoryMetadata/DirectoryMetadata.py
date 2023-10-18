""" DIRAC FileCatalog mix-in class to manage directory metadata
"""
# pylint: disable=protected-access
import os
from DIRAC.ConfigurationSystem.Client.Helpers import Registry
from DIRAC.ConfigurationSystem.Client.Helpers.Operations import Operations
from DIRAC import S_OK, S_ERROR
from DIRAC.Core.Utilities.TimeUtilities import queryTime


class DirectoryMetadata:
    def __init__(self, database=None):
        self.db = database

    def setDatabase(self, database):
        self.db = database

    ##############################################################################
    #
    #  Manage Metadata fields
    #

    def addMetadataField(self, pName, pType, credDict):
        """Add a new metadata parameter to the Metadata Database.

        :param str pName: parameter name
        :param str pType: parameter type in the MySQL notation

        :return: S_OK/S_ERROR, Value - comment on a positive result
        """

        result = self.db.fmeta.getFileMetadataFields(credDict)
        if not result["OK"]:
            return result
        if pName in result["Value"]:
            return S_ERROR(f"The metadata {pName} is already defined for Files")

        result = self._getMetadataFields(credDict)
        if not result["OK"]:
            return result
        if pName in result["Value"]:
            if pType.lower() == result["Value"][pName].lower():
                return S_OK("Already exists")
            return S_ERROR(f"Attempt to add an existing metadata with different type: {pType}/{result['Value'][pName]}")

        valueType = pType
        if pType.lower()[:3] == "int":
            valueType = "INT"
        elif pType.lower() == "string":
            valueType = "VARCHAR(128)"
        elif pType.lower() == "float":
            valueType = "FLOAT"
        elif pType.lower() == "date":
            valueType = "DATETIME"
        elif pType == "MetaSet":
            valueType = "VARCHAR(64)"

        req = "CREATE TABLE FC_Meta_{} ( DirID INTEGER NOT NULL, Value {}, PRIMARY KEY (DirID), INDEX (Value) )".format(
            pName,
            valueType,
        )
        result = self.db._query(req)
        if not result["OK"]:
            return result

        result = self.db.insertFields("FC_MetaFields", ["MetaName", "MetaType"], [pName, pType])
        if not result["OK"]:
            return result

        metadataID = result["lastRowId"]
        result = self.__transformMetaParameterToData(pName)
        if not result["OK"]:
            return result

        return S_OK("Added new metadata: %d" % metadataID)

    def deleteMetadataField(self, pName, credDict):
        """Remove metadata field

        :param str pName: meta parameter name
        :param dict credDict: client credential dictionary

        :return: S_OK/S_ERROR
        """

        req = f"DROP TABLE FC_Meta_{pName}"
        result = self.db._update(req)
        error = ""
        if not result["OK"]:
            error = result["Message"]
        req = f"DELETE FROM FC_MetaFields WHERE MetaName='{pName}'"
        result = self.db._update(req)
        if not result["OK"]:
            if error:
                result["Message"] = error + "; " + result["Message"]
        return result

    def getMetadataFields(self, credDict):
        """Get all the defined metadata fields

        :param dict credDict: client credential dictionary
        :return: S_OK/S_ERROR, Value is the metadata:metadata type dictionary
        """

        return self._getMetadataFields(credDict)

    def _getMetadataFields(self, credDict):
        """Get all the defined metadata fields as they are defined in the database

        :param dict credDict: client credential dictionary
        :return: S_OK/S_ERROR, Value is the metadata:metadata type dictionary
        """

        req = "SELECT MetaName,MetaType FROM FC_MetaFields"
        result = self.db._query(req)
        if not result["OK"]:
            return result

        metaDict = {}
        for row in result["Value"]:
            metaDict[row[0]] = row[1]

        return S_OK(metaDict)

    def addMetadataSet(self, metaSetName, metaSetDict, credDict):
        """Add a new metadata set with the contents from metaSetDict

        :param str metaSetName: metaSet name
        :param dict metaSetDict: contents of the meta set definition
        :param dict credDict: client credential dictionary

        :return: S_OK/S_ERROR
        """
        result = self._getMetadataFields(credDict)
        if not result["OK"]:
            return result
        metaTypeDict = result["Value"]
        # Check the sanity of the metadata set contents
        for key in metaSetDict:
            if key not in metaTypeDict:
                return S_ERROR(f"Unknown key {key}")

        result = self.db.insertFields("FC_MetaSetNames", ["MetaSetName"], [metaSetName])
        if not result["OK"]:
            return result

        metaSetID = result["lastRowId"]

        req = "INSERT INTO FC_MetaSets (MetaSetID,MetaKey,MetaValue) VALUES %s"
        vList = []
        for key, value in metaSetDict.items():
            vList.append("(%d,'%s','%s')" % (metaSetID, key, str(value)))
        vString = ",".join(vList)
        result = self.db._update(req % vString)
        return result

    def getMetadataSet(self, metaSetName, expandFlag, credDict):
        """Get fully expanded contents of the metadata set

        :param str metaSetName: metaSet name
        :param bool expandFlag: flag to whether to expand the metaset recursively
        :param dict credDict: client credential dictionary

        :return: S_OK/S_ERROR, Value dictionary of the meta set definition contents
        """
        result = self._getMetadataFields(credDict)
        if not result["OK"]:
            return result
        metaTypeDict = result["Value"]

        req = "SELECT S.MetaKey,S.MetaValue FROM FC_MetaSets as S, FC_MetaSetNames as N "
        req += f"WHERE N.MetaSetName='{metaSetName}' AND N.MetaSetID=S.MetaSetID"
        result = self.db._query(req)
        if not result["OK"]:
            return result

        if not result["Value"]:
            return S_OK({})

        resultDict = {}
        for key, value in result["Value"]:
            if key not in metaTypeDict:
                return S_ERROR(f"Unknown key {key}")
            if expandFlag:
                if metaTypeDict[key] == "MetaSet":
                    result = self.getMetadataSet(value, expandFlag, credDict)
                    if not result["OK"]:
                        return result
                    resultDict.update(result["Value"])
                else:
                    resultDict[key] = value
            else:
                resultDict[key] = value
        return S_OK(resultDict)

    #############################################################################################
    #
    # Set and get directory metadata
    #
    #############################################################################################

    def setMetadata(self, dPath, metaDict, credDict):
        """Set the value of a given metadata field for the the given directory path

        :param str dPath: directory path
        :param dict metaDict: dictionary with metadata
        :param dict credDict: client credential dictionary

        :return: S_OK/S_ERROR
        """
        result = self._getMetadataFields(credDict)
        if not result["OK"]:
            return result
        metaFields = result["Value"]

        result = self.db.dtree.findDir(dPath)
        if not result["OK"]:
            return result
        if not result["Value"]:
            return S_ERROR(f"Path not found: {dPath}")
        dirID = result["Value"]

        dirmeta = self.getDirectoryMetadata(dPath, credDict, ownData=False)
        if not dirmeta["OK"]:
            return dirmeta

        voName = Registry.getGroupOption(credDict["group"], "VO")
        forceIndex = Operations(vo=voName).getValue("DataManagement/ForceIndexedMetadata", False)
        for metaName, metaValue in metaDict.items():
            if metaName not in metaFields:
                if forceIndex:
                    return S_ERROR(f"Field {metaName} not indexed, but ForceIndexedMetadata is set", callStack=[])
                result = self.setMetaParameter(dPath, metaName, metaValue, credDict)
                if not result["OK"]:
                    return result
                continue
            # Check that the metadata is not defined for the parent directories
            if metaName in dirmeta["Value"]:
                return S_ERROR(f"Metadata conflict detected for {metaName} for directory {dPath}")
            result = self.db.insertFields(f"FC_Meta_{metaName}", ["DirID", "Value"], [dirID, metaValue])
            if not result["OK"]:
                if result["Message"].find("Duplicate") != -1:
                    req = "UPDATE FC_Meta_%s SET Value='%s' WHERE DirID=%d" % (metaName, metaValue, dirID)
                    result = self.db._update(req)
                    if not result["OK"]:
                        return result
                else:
                    return result

        return S_OK()

    def removeMetadata(self, dPath, metaData, credDict):
        """Remove the specified metadata for the given directory

        :param str dPath: directory path
        :param dict metaData: metadata dictionary
        :param dict credDict: client credential dictionary
        :return: standard Dirac result object
        """

        result = self._getMetadataFields(credDict)
        if not result["OK"]:
            return result
        metaFields = result["Value"]

        result = self.db.dtree.findDir(dPath)
        if not result["OK"]:
            return result
        if not result["Value"]:
            return S_ERROR(f"Path not found: {dPath}")
        dirID = result["Value"]

        failedMeta = {}
        for meta in metaData:
            if meta in metaFields:
                # Indexed meta case
                req = "DELETE FROM FC_Meta_%s WHERE DirID=%d" % (meta, dirID)
                result = self.db._update(req)
                if not result["OK"]:
                    failedMeta[meta] = result["Value"]
            else:
                # Meta parameter case
                req = "DELETE FROM FC_DirMeta WHERE MetaKey='%s' AND DirID=%d" % (meta, dirID)
                result = self.db._update(req)
                if not result["OK"]:
                    failedMeta[meta] = result["Value"]

        if failedMeta:
            metaExample = list(failedMeta)[0]
            result = S_ERROR(f"Failed to remove {len(failedMeta)} metadata, e.g. {failedMeta[metaExample]}")
            result["FailedMetadata"] = failedMeta
        else:
            return S_OK()

    def setMetaParameter(self, dPath, metaName, metaValue, credDict):
        """Set an meta parameter - metadata which is not used in the the data
        search operations

        :param str dPath: directory name
        :param str metaName: meta parameter name
        :param str metaValue: meta parameter value
        :param dict credDict: client credential dictionary

        :return: S_OK/S_ERROR
        """
        result = self.db.dtree.findDir(dPath)
        if not result["OK"]:
            return result
        if not result["Value"]:
            return S_ERROR(f"Path not found: {dPath}")
        dirID = result["Value"]

        result = self.db.insertFields(
            "FC_DirMeta", ["DirID", "MetaKey", "MetaValue"], [dirID, metaName, str(metaValue)]
        )
        return result

    def getDirectoryMetaParameters(self, dpath, credDict, inherited=True):
        """Get meta parameters for the given directory

        :param str dPath: directory name
        :param dict credDict: client credential dictionary

        :return: S_OK/S_ERROR, Value dictionary of meta parameters
        """
        if inherited:
            result = self.db.dtree.getPathIDs(dpath)
            if not result["OK"]:
                return result
            pathIDs = result["Value"]
            dirID = pathIDs[-1]
        else:
            result = self.db.dtree.findDir(dpath)
            if not result["OK"]:
                return result
            if not result["Value"]:
                return S_ERROR(f"Path not found: {dpath}")
            dirID = result["Value"]
            pathIDs = [dirID]

        if len(pathIDs) > 1:
            pathString = ",".join([str(x) for x in pathIDs])
            req = f"SELECT DirID,MetaKey,MetaValue from FC_DirMeta where DirID in ({pathString})"
        else:
            req = "SELECT DirID,MetaKey,MetaValue from FC_DirMeta where DirID=%d " % dirID
        result = self.db._query(req)
        if not result["OK"]:
            return result
        if not result["Value"]:
            return S_OK({})
        metaDict = {}
        for _dID, key, value in result["Value"]:
            if isinstance(metaDict.get(key), list):
                metaDict[key].append(value)
            else:
                metaDict[key] = value

        return S_OK(metaDict)

    def getDirectoryMetadata(self, path, credDict, inherited=True, ownData=True):
        """Get metadata for the given directory aggregating metadata for the directory itself
        and for all the parent directories if inherited flag is True. Get also the non-indexed
        metadata parameters.

        :param str path: directory name
        :param dict credDict: client credential dictionary
        :param bool inherited: flag to include metadata from the parent directories
        :param bool ownData: flag to include metadata for the directory itself

        :return: S_OK/S_ERROR, Value dictionary of metadata
        """

        result = self.db.dtree.getPathIDs(path)
        if not result["OK"]:
            return result
        pathIDs = result["Value"]

        result = self._getMetadataFields(credDict)
        if not result["OK"]:
            return result
        metaFields = result["Value"]

        metaDict = {}
        metaOwnerDict = {}
        metaTypeDict = {}
        dirID = pathIDs[-1]
        if not inherited:
            pathIDs = pathIDs[-1:]
        if not ownData:
            pathIDs = pathIDs[:-1]
        pathString = ",".join([str(x) for x in pathIDs])

        for meta in metaFields:
            req = f"SELECT Value,DirID FROM FC_Meta_{meta} WHERE DirID in ({pathString})"
            result = self.db._query(req)
            if not result["OK"]:
                return result
            if len(result["Value"]) > 1:
                return S_ERROR(f"Metadata conflict for {meta} for directory {path}")
            if result["Value"]:
                metaDict[meta] = result["Value"][0][0]
                if int(result["Value"][0][1]) == dirID:
                    metaOwnerDict[meta] = "OwnMetadata"
                else:
                    metaOwnerDict[meta] = "ParentMetadata"
            metaTypeDict[meta] = metaFields[meta]

        # Get also non-searchable data
        result = self.getDirectoryMetaParameters(path, credDict, inherited)
        if result["OK"]:
            metaDict.update(result["Value"])
            for meta in result["Value"]:
                metaOwnerDict[meta] = "OwnParameter"

        result = S_OK(metaDict)
        result["MetadataOwner"] = metaOwnerDict
        result["MetadataType"] = metaTypeDict
        return result

    def __transformMetaParameterToData(self, metaName):
        """Relocate the meta parameters of all the directories to the corresponding
        indexed metadata table

        :param str metaName: name of the parameter to transform

        :return: S_OK/S_ERROR
        """

        req = f"SELECT DirID,MetaValue from FC_DirMeta WHERE MetaKey='{metaName}'"
        result = self.db._query(req)
        if not result["OK"]:
            return result
        if not result["Value"]:
            return S_OK()

        dirDict = {}
        for dirID, meta in result["Value"]:
            dirDict[dirID] = meta
        dirList = list(dirDict)

        # Exclude child directories from the list
        for dirID in dirList:
            result = self.db.dtree.getSubdirectoriesByID(dirID)
            if not result["OK"]:
                return result
            if not result["Value"]:
                continue
            childIDs = list(result["Value"])
            for childID in childIDs:
                if childID in dirList:
                    del dirList[dirList.index(childID)]

        insertValueList = []
        for dirID in dirList:
            insertValueList.append("( %d,'%s' )" % (dirID, dirDict[dirID]))

        req = f"INSERT INTO FC_Meta_{metaName} (DirID,Value) VALUES {', '.join(insertValueList)}"
        result = self.db._update(req)
        if not result["OK"]:
            return result

        req = f"DELETE FROM FC_DirMeta WHERE MetaKey='{metaName}'"
        result = self.db._update(req)
        return result

    ############################################################################################
    #
    # Find directories corresponding to the metadata
    #

    def __createMetaSelection(self, value, table=""):
        """Create an SQL selection element for the given meta value

        :param dict value: dictionary with selection instructions suitable for the database search
        :param str table: table name

        :return: selection string
        """

        if isinstance(value, dict):
            selectList = []
            for operation, operand in value.items():
                if operation in [">", "<", ">=", "<="]:
                    if isinstance(operand, list):
                        return S_ERROR("Illegal query: list of values for comparison operation")
                    if isinstance(operand, int):
                        selectList.append("%sValue%s%d" % (table, operation, operand))
                    elif isinstance(operand, float):
                        selectList.append(f"{table}Value{operation}{operand:f}")
                    else:
                        selectList.append(f"{table}Value{operation}'{operand}'")
                elif operation == "in" or operation == "=":
                    if isinstance(operand, list):
                        vString = ",".join(["'" + str(x) + "'" for x in operand])
                        selectList.append(f"{table}Value IN ({vString})")
                    else:
                        selectList.append(f"{table}Value='{operand}'")
                elif operation == "nin" or operation == "!=":
                    if isinstance(operand, list):
                        vString = ",".join(["'" + str(x) + "'" for x in operand])
                        selectList.append(f"{table}Value NOT IN ({vString})")
                    else:
                        selectList.append(f"{table}Value!='{operand}'")
                selectString = " AND ".join(selectList)
        elif isinstance(value, list):
            vString = ",".join(["'" + str(x) + "'" for x in value])
            selectString = f"{table}Value in ({vString})"
        else:
            if value == "Any":
                selectString = ""
            else:
                selectString = f"{table}Value='{value}' "

        return S_OK(selectString)

    def __findSubdirByMeta(self, metaName, value, pathSelection="", subdirFlag=True):
        """Find directories for the given metaName datum. If the the metaName datum type is a list,
        combine values in OR. In case the metaName datum is 'Any', finds all the subdirectories
        for which the metaName datum is defined at all.

        :param str metaName: metadata name
        :param dict,list value: dictionary with selection instructions suitable for the database search
        :param str pathSelection: directory path selection string
        :param bool subdirFlag: fla to include subdirectories

        :return: S_OK/S_ERROR, Value list of found directories
        """

        result = self.__createMetaSelection(value, "M.")
        if not result["OK"]:
            return result
        selectString = result["Value"]

        req = f" SELECT M.DirID FROM FC_Meta_{metaName} AS M"
        if pathSelection:
            req += f" JOIN ( {pathSelection} ) AS P WHERE M.DirID=P.DirID"
        if selectString:
            if pathSelection:
                req += f" AND {selectString}"
            else:
                req += f" WHERE {selectString}"

        result = self.db._query(req)
        if not result["OK"]:
            return result
        if not result["Value"]:
            return S_OK([])

        dirList = []
        for row in result["Value"]:
            dirID = row[0]
            dirList.append(dirID)
            # if subdirFlag:
            #  result = self.db.dtree.getSubdirectoriesByID( dirID )
            #  if not result['OK']:
            #    return result
            #  dirList += result['Value']
        if subdirFlag:
            result = self.db.dtree.getAllSubdirectoriesByID(dirList)
            if not result["OK"]:
                return result
            dirList += result["Value"]

        return S_OK(dirList)

    def __findSubdirMissingMeta(self, metaName, pathSelection):
        """Find directories not having the given meta datum defined

        :param str metaName: metadata name
        :param str pathSelection: directory path selection string

        :return: S_OK,S_ERROR , Value list of directories
        """
        result = self.__findSubdirByMeta(metaName, "Any", pathSelection)
        if not result["OK"]:
            return result
        dirList = result["Value"]
        table = self.db.dtree.getTreeTable()
        dirString = ",".join([str(x) for x in dirList])
        if dirList:
            req = f"SELECT DirID FROM {table} WHERE DirID NOT IN ( {dirString} )"
        else:
            req = f"SELECT DirID FROM {table}"
        result = self.db._query(req)
        if not result["OK"]:
            return result
        if not result["Value"]:
            return S_OK([])

        dirList = [x[0] for x in result["Value"]]
        return S_OK(dirList)

    def __expandMetaDictionary(self, metaDict, credDict):
        """Update the dictionary with metadata query by expand metaSet type metadata

        :param dict metaDict: metaDict to be expanded
        :param dict credDict: client credential dictionary

        :return: S_OK/S_ERROR , Value dictionary of metadata
        """
        result = self._getMetadataFields(credDict)
        if not result["OK"]:
            return result
        metaTypeDict = result["Value"]
        resultDict = {}
        extraDict = {}
        for key, value in metaDict.items():
            if key not in metaTypeDict:
                # return S_ERROR( 'Unknown metadata field %s' % key )
                extraDict[key] = value
                continue
            keyType = metaTypeDict[key]
            if keyType != "MetaSet":
                resultDict[key] = value
            else:
                result = self.getMetadataSet(value, True, credDict)
                if not result["OK"]:
                    return result
                mDict = result["Value"]
                for mk, mv in mDict.items():
                    if mk in resultDict:
                        return S_ERROR(f"Contradictory query for key {mk}")
                    else:
                        resultDict[mk] = mv

        result = S_OK(resultDict)
        result["ExtraMetadata"] = extraDict
        return result

    def __checkDirsForMetadata(self, metaName, value, pathString):
        """Check if any of the given directories conform to the given metadata

        :param str metaName: matadata name
        :param dict,list value: dictionary with selection instructions suitable for the database search
        :param str pathString: string of comma separated directory names

        :return: S_OK/S_ERROR, Value directory ID
        """
        result = self.__createMetaSelection(value, "M.")
        if not result["OK"]:
            return result
        selectString = result["Value"]

        if selectString:
            req = "SELECT M.DirID FROM FC_Meta_{} AS M WHERE {} AND M.DirID IN ({})".format(
                metaName,
                selectString,
                pathString,
            )
        else:
            req = f"SELECT M.DirID FROM FC_Meta_{metaName} AS M WHERE M.DirID IN ({pathString})"
        result = self.db._query(req)
        if not result["OK"]:
            return result
        elif not result["Value"]:
            return S_OK(None)
        elif len(result["Value"]) > 1:
            return S_ERROR("Conflict in the directory metadata hierarchy")
        else:
            return S_OK(result["Value"][0][0])

    @queryTime
    def findDirIDsByMetadata(self, queryDict, path, credDict):
        """Find Directories satisfying the given metadata and being subdirectories of
        the given path

        :param dict queryDict: dictionary containing query data
        :param str path: starting directory path
        :param dict credDict: client credential dictionary

        :return: S_OK/S_ERROR, Value list of selected directory IDs
        """

        pathDirList = []
        pathDirID = 0
        pathString = "0"
        if path != "/":
            result = self.db.dtree.getPathIDs(path)
            if not result["OK"]:
                # as result[Value] is already checked in getPathIDs
                return result
            pathIDs = result["Value"]
            pathDirID = pathIDs[-1]
            pathString = ",".join([str(x) for x in pathIDs])

        result = self.__expandMetaDictionary(queryDict, credDict)
        if not result["OK"]:
            return result
        metaDict = result["Value"]

        # Now check the meta data for the requested directory and its parents
        finalMetaDict = dict(metaDict)
        for meta in metaDict:
            result = self.__checkDirsForMetadata(meta, metaDict[meta], pathString)
            if not result["OK"]:
                return result
            elif result["Value"] is not None:
                # Some directory in the parent hierarchy is already conforming with the
                # given metadata, no need to check it further
                del finalMetaDict[meta]

        if finalMetaDict:
            pathSelection = ""
            if pathDirID:
                result = self.db.dtree.getSubdirectoriesByID(pathDirID, includeParent=True, requestString=True)
                if not result["OK"]:
                    return result
                pathSelection = result["Value"]
            dirList = []
            first = True
            for meta, value in finalMetaDict.items():
                if value == "Missing":
                    result = self.__findSubdirMissingMeta(meta, pathSelection)
                else:
                    result = self.__findSubdirByMeta(meta, value, pathSelection)
                if not result["OK"]:
                    return result
                mList = result["Value"]
                if first:
                    dirList = mList
                    first = False
                else:
                    newList = []
                    for d in dirList:
                        if d in mList:
                            newList.append(d)
                    dirList = newList
        else:
            if pathDirID:
                result = self.db.dtree.getSubdirectoriesByID(pathDirID, includeParent=True)
                if not result["OK"]:
                    return result
                pathDirList = list(result["Value"])

        finalList = []
        dirSelect = False
        if finalMetaDict:
            dirSelect = True
            finalList = dirList
            if pathDirList:
                finalList = list(set(dirList) & set(pathDirList))
        else:
            if pathDirList:
                dirSelect = True
                finalList = pathDirList
        result = S_OK(finalList)

        if finalList:
            result["Selection"] = "Done"
        elif dirSelect:
            result["Selection"] = "None"
        else:
            result["Selection"] = "All"

        return result

    @queryTime
    def findDirectoriesByMetadata(self, queryDict, path, credDict):
        """Find Directory names satisfying the given metadata and being subdirectories of
        the given path

        :param dict queryDict: dictionary containing query data
        :param str path: starting directory path
        :param dict credDict: client credential dictionary

        :return: S_OK/S_ERROR, Value list of selected directory paths
        """

        result = self.findDirIDsByMetadata(queryDict, path, credDict)
        if not result["OK"]:
            return result

        dirIDList = result["Value"]

        dirNameDict = {}
        if dirIDList:
            result = self.db.dtree.getDirectoryPaths(dirIDList)
            if not result["OK"]:
                return result
            dirNameDict = result["Value"]
        elif result["Selection"] == "None":
            dirNameDict = {0: "None"}
        elif result["Selection"] == "All":
            dirNameDict = {0: "All"}

        return S_OK(dirNameDict)

    def findFilesByMetadata(self, metaDict, path, credDict):
        """Find Files satisfying the given metadata

        :param dict metaDict: dictionary with the selection metadata
        :param str path: starting directory path
        :param dict credDict: client credential dictionary

        :return: S_OK/S_ERROR, Value list files in selected directories
        """

        result = self.findDirectoriesByMetadata(metaDict, path, credDict)
        if not result["OK"]:
            return result

        dirDict = result["Value"]
        dirList = list(dirDict)
        fileList = []
        result = self.db.dtree.getFilesInDirectory(dirList, credDict)
        if not result["OK"]:
            return result
        for _fileID, dirID, fname in result["Value"]:
            fileList.append(dirDict[dirID] + "/" + os.path.basename(fname))

        return S_OK(fileList)

    def findFileIDsByMetadata(self, metaDict, path, credDict, startItem=0, maxItems=25):
        """Find Files satisfying the given metadata

        :param dict metaDict: dictionary with the selection metadata
        :param str path: starting directory path
        :param dict credDict: client credential dictionary
        :param int startItem: offset in the file list
        :param int maxItems: max number of files to rteurn

        :return: S_OK/S_ERROR, Value list file IDs in selected directories
        """
        result = self.findDirIDsByMetadata(metaDict, path, credDict)
        if not result["OK"]:
            return result

        dirList = result["Value"]
        return self.db.dtree.getFileIDsInDirectoryWithLimits(dirList, credDict, startItem, maxItems)

    ################################################################################################
    #
    # Find metadata compatible with other metadata in order to organize dynamically updated metadata selectors

    def __findCompatibleDirectories(self, metaName, value, fromDirs):
        """Find directories compatible with the given metaName datum.
        Optionally limit the list of compatible directories to only those in the
        fromDirs list

        :param str metaName: metadata name
        :param dict,list value: dictionary with selection instructions suitable for the database search
        :param list fromDirs: list of directories to choose from

        :return: S_OK/S_ERROR, Value list of selected directories
        """

        # The directories compatible with the given metaName datum are:
        # - directory for which the datum is defined
        # - all the subdirectories of the above directory
        # - all the directories in the parent hierarchy of the above directory

        # Find directories defining the metaName datum and their subdirectories
        result = self.__findSubdirByMeta(metaName, value, subdirFlag=False)
        if not result["OK"]:
            return result
        selectedDirs = result["Value"]
        if not selectedDirs:
            return S_OK([])

        result = self.db.dtree.getAllSubdirectoriesByID(selectedDirs)
        if not result["OK"]:
            return result
        subDirs = result["Value"]

        # Find parent directories of the directories defining the metaName datum
        parentDirs = []
        for psub in selectedDirs:
            result = self.db.dtree.getPathIDsByID(psub)
            if not result["OK"]:
                return result
            parentDirs += result["Value"]

        # Constrain the output to only those that are present in the input list
        resDirs = parentDirs + subDirs + selectedDirs
        if fromDirs:
            resDirs = list(set(resDirs) & set(fromDirs))

        return S_OK(resDirs)

    def __findDistinctMetadata(self, metaList, dList):
        """Find distinct metadata values defined for the list of the input directories.
        Limit the search for only metadata in the input list

        :param list metaList: list of metadata names
        :param list dList: list of directories to limit the selection

        :return: S_OK/S_ERROR, Value dictionary of metadata
        """

        if dList:
            dString = ",".join([str(x) for x in dList])
        else:
            dString = None
        metaDict = {}
        for meta in metaList:
            req = f"SELECT DISTINCT(Value) FROM FC_Meta_{meta}"
            if dString:
                req += f" WHERE DirID in ({dString})"
            result = self.db._query(req)
            if not result["OK"]:
                return result
            if result["Value"]:
                metaDict[meta] = []
                for row in result["Value"]:
                    metaDict[meta].append(row[0])

        return S_OK(metaDict)

    def getCompatibleMetadata(self, queryDict, path, credDict):
        """Get distinct metadata values compatible with the given already defined metadata

        :param dict queryDict: dictionary containing query data
        :param str path: starting directory path
        :param dict credDict: client credential dictionary

        :return: S_OK/S_ERROR, Value dictionary of metadata
        """

        pathDirID = 0
        if path != "/":
            result = self.db.dtree.findDir(path)
            if not result["OK"]:
                return result
            if not result["Value"]:
                return S_ERROR(f"Path not found: {path}")
            pathDirID = int(result["Value"])
        pathDirs = []
        if pathDirID:
            result = self.db.dtree.getSubdirectoriesByID(pathDirID, includeParent=True)
            if not result["OK"]:
                return result
            if result["Value"]:
                pathDirs = list(result["Value"])
            result = self.db.dtree.getPathIDsByID(pathDirID)
            if not result["OK"]:
                return result
            if result["Value"]:
                pathDirs += result["Value"]

        # Get the list of metadata fields to inspect
        result = self._getMetadataFields(credDict)
        if not result["OK"]:
            return result
        metaFields = result["Value"]
        comFields = list(metaFields)

        # Commented out to return compatible data also for selection metadata
        # for m in metaDict:
        #  if m in comFields:
        #    del comFields[comFields.index( m )]

        result = self.__expandMetaDictionary(queryDict, credDict)
        if not result["OK"]:
            return result
        metaDict = result["Value"]

        fromList = pathDirs
        anyMeta = True
        if metaDict:
            anyMeta = False
            for meta, value in metaDict.items():
                result = self.__findCompatibleDirectories(meta, value, fromList)
                if not result["OK"]:
                    return result
                cdirList = result["Value"]
                if cdirList:
                    fromList = cdirList
                else:
                    fromList = []
                    break

        if anyMeta or fromList:
            result = self.__findDistinctMetadata(comFields, fromList)
        else:
            result = S_OK({})
        return result

    def removeMetadataForDirectory(self, dirList, credDict):
        """Remove all the metadata for the given directory list

        :param list dirList: list of directory paths
        :param dict credDict: client credential dictionary

        :return: S_OK/S_ERROR, Value Successful/Failed dictionaries
        """
        if not dirList:
            return S_OK({"Successful": {}, "Failed": {}})

        failed = {}
        successful = {}
        dirs = dirList
        if not isinstance(dirList, list):
            dirs = [dirList]

        dirListString = ",".join([str(d) for d in dirs])

        # Get the list of metadata fields to inspect
        result = self._getMetadataFields(credDict)
        if not result["OK"]:
            return result
        metaFields = result["Value"]

        for meta in metaFields:
            req = f"DELETE FROM FC_Meta_{meta} WHERE DirID in ( {dirListString} )"
            result = self.db._query(req)
            if not result["OK"]:
                failed[meta] = result["Message"]
            else:
                successful[meta] = "OK"

        return S_OK({"Successful": successful, "Failed": failed})
