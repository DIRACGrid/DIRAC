########################################################################
# File: MetaQuery.py
# Author: A.T.
# Date: 24.02.2015
# $HeadID$
########################################################################

""" Utilities for managing metadata based queries
"""
from DIRAC import S_OK, S_ERROR
import DIRAC.Core.Utilities.TimeUtilities as TimeUtilities

import json

FILE_STANDARD_METAKEYS = {
    "SE": "VARCHAR",
    "CreationDate": "DATETIME",
    "ModificationDate": "DATETIME",
    "LastAccessDate": "DATETIME",
    "User": "VARCHAR",
    "Group": "VARCHAR",
    "Path": "VARCHAR",
    "Name": "VARCHAR",
    "FileName": "VARCHAR",
    "CheckSum": "VARCHAR",
    "GUID": "VARCHAR",
    "UID": "INTEGER",
    "GID": "INTEGER",
    "Size": "INTEGER",
    "Status": "VARCHAR",
}

FILES_TABLE_METAKEYS = {
    "Name": "FileName",
    "FileName": "FileName",
    "Size": "Size",
    "User": "UID",
    "Group": "GID",
    "UID": "UID",
    "GID": "GID",
    "Status": "Status",
}

FILEINFO_TABLE_METAKEYS = {
    "GUID": "GUID",
    "CheckSum": "CheckSum",
    "CreationDate": "CreationDate",
    "ModificationDate": "ModificationDate",
    "LastAccessDate": "LastAccessDate",
}


class MetaQuery:
    def __init__(self, queryDict=None, typeDict=None):
        self.__metaQueryDict = {}
        if queryDict is not None:
            self.__metaQueryDict = queryDict
        self.__metaTypeDict = {}
        if typeDict is not None:
            self.__metaTypeDict = typeDict

    def setMetaQuery(self, queryList, metaTypeDict=None):
        """Create the metadata query out of the command line arguments"""
        if metaTypeDict is not None:
            self.__metaTypeDict = metaTypeDict
        metaDict = {}
        contMode = False
        value = ""
        for arg in queryList:
            if not contMode:
                operation = ""
                for op in [">=", "<=", ">", "<", "!=", "="]:
                    if op in arg:
                        operation = op
                        break
                if not operation:
                    return S_ERROR(f"Illegal query element {arg}")

                name, value = arg.split(operation)
                if name not in self.__metaTypeDict:
                    return S_ERROR(f"Metadata field {name} not defined")

                mtype = self.__metaTypeDict[name]
            else:
                value += " " + arg
                value = value.replace(contMode, "")
                contMode = False

            if value[0] in ['"', "'"] and value[-1] not in ['"', "'"]:
                contMode = value[0]
                continue

            if "," in value:
                valueList = [x.replace("'", "").replace('"', "") for x in value.split(",")]
                mvalue = valueList
                if mtype[0:3].lower() == "int":
                    mvalue = [int(x) for x in valueList if x not in ["Missing", "Any"]]
                    mvalue += [x for x in valueList if x in ["Missing", "Any"]]
                if mtype[0:5].lower() == "float":
                    mvalue = [float(x) for x in valueList if x not in ["Missing", "Any"]]
                    mvalue += [x for x in valueList if x in ["Missing", "Any"]]
                if operation == "=":
                    operation = "in"
                if operation == "!=":
                    operation = "nin"
                mvalue = {operation: mvalue}
            else:
                mvalue = value.replace("'", "").replace('"', "")
                if value not in ["Missing", "Any"]:
                    if mtype[0:3].lower() == "int":
                        mvalue = int(value)
                    if mtype[0:5].lower() == "float":
                        mvalue = float(value)
                if operation != "=":
                    mvalue = {operation: mvalue}

            if name in metaDict:
                if isinstance(metaDict[name], dict):
                    if isinstance(mvalue, dict):
                        op, value = list(mvalue.items())[0]
                        if op in metaDict[name]:
                            if isinstance(metaDict[name][op], list):
                                if isinstance(value, list):
                                    metaDict[name][op] = list(set(metaDict[name][op] + value))
                                else:
                                    metaDict[name][op] = list(set(metaDict[name][op].append(value)))
                            else:
                                if isinstance(value, list):
                                    metaDict[name][op] = list(set([metaDict[name][op]] + value))
                                else:
                                    metaDict[name][op] = list({metaDict[name][op], value})
                        else:
                            metaDict[name].update(mvalue)
                    else:
                        if isinstance(mvalue, list):
                            metaDict[name].update({"in": mvalue})
                        else:
                            metaDict[name].update({"=": mvalue})
                elif isinstance(metaDict[name], list):
                    if isinstance(mvalue, dict):
                        metaDict[name] = {"in": metaDict[name]}
                        metaDict[name].update(mvalue)
                    elif isinstance(mvalue, list):
                        metaDict[name] = list(set(metaDict[name] + mvalue))
                    else:
                        metaDict[name] = list(set(metaDict[name].append(mvalue)))
                else:
                    if isinstance(mvalue, dict):
                        metaDict[name] = {"=": metaDict[name]}
                        metaDict[name].update(mvalue)
                    elif isinstance(mvalue, list):
                        metaDict[name] = list(set([metaDict[name]] + mvalue))
                    else:
                        metaDict[name] = list({metaDict[name], mvalue})
            else:
                metaDict[name] = mvalue

        self.__metaQueryDict = metaDict
        return S_OK(metaDict)

    def getMetaQuery(self):
        return self.__metaQueryDict

    def getMetaQueryAsJson(self):
        return json.dumps(self.__metaQueryDict)

    def applyQuery(self, userMetaDict):
        """Return a list of tuples with tables and conditions to locate files for a given user Metadata"""

        def getOperands(value):
            if isinstance(value, list):
                return [("in", value)]
            elif isinstance(value, dict):
                resultList = []
                for operation, operand in value.items():
                    resultList.append((operation, operand))
                return resultList
            else:
                return [("=", value)]

        def getTypedValue(value, mtype):
            if mtype[0:3].lower() == "int":
                return int(value)
            elif mtype[0:5].lower() == "float":
                return float(value)
            elif mtype[0:4].lower() == "date":
                return TimeUtilities.fromString(value)
            else:
                return value

        for meta, value in self.__metaQueryDict.items():
            # Check if user dict contains all the requested meta data
            userValue = userMetaDict.get(meta, None)
            if userValue is None:
                if str(value).lower() == "missing":
                    continue
                else:
                    return S_OK(False)
            elif str(value).lower() == "any":
                continue

            mtype = self.__metaTypeDict[meta]
            try:
                userValue = getTypedValue(userValue, mtype)
            except ValueError:
                return S_ERROR(f"Illegal type for metadata {meta}: {str(userValue)} in user data")

            # Check operations
            for operation, operand in getOperands(value):
                try:
                    if isinstance(operand, list):
                        typedValue = [getTypedValue(x, mtype) for x in operand]
                    else:
                        typedValue = getTypedValue(operand, mtype)
                except ValueError:
                    return S_ERROR(f"Illegal type for metadata {meta}: {str(operand)} in filter")

                # Apply query operation
                if operation in [">", "<", ">=", "<="]:
                    if isinstance(typedValue, list):
                        return S_ERROR("Illegal query: list of values for comparison operation")
                    elif operation == ">" and typedValue >= userValue:
                        return S_OK(False)
                    elif operation == "<" and typedValue <= userValue:
                        return S_OK(False)
                    elif operation == ">=" and typedValue > userValue:
                        return S_OK(False)
                    elif operation == "<=" and typedValue < userValue:
                        return S_OK(False)
                elif operation == "in" or operation == "=":
                    if isinstance(typedValue, list) and userValue not in typedValue:
                        return S_OK(False)
                    elif not isinstance(typedValue, list) and userValue != typedValue:
                        return S_OK(False)
                elif operation == "nin" or operation == "!=":
                    if isinstance(typedValue, list) and userValue in typedValue:
                        return S_OK(False)
                    elif not isinstance(typedValue, list) and userValue == typedValue:
                        return S_OK(False)

        return S_OK(True)
