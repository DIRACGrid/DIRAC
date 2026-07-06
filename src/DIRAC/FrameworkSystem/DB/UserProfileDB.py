""" UserProfileDB class is a front-end to the User Profile Database
"""

import cachetools

from DIRAC import S_OK, S_ERROR
from DIRAC.ConfigurationSystem.Client.Helpers import Registry
from DIRAC.Core.Base.DB import DB


class UserProfileDB(DB):
    """UserProfileDB class is a front-end to the User Profile Database"""

    tableDict = {
        "up_Users": {
            "Fields": {
                "Id": "INTEGER AUTO_INCREMENT NOT NULL",
                "UserName": "VARCHAR(64) NOT NULL",
                "LastAccess": "DATETIME",
            },
            "PrimaryKey": "Id",
            "UniqueIndexes": {"U": ["UserName"]},
            "Engine": "InnoDB",
        },
        "up_Groups": {
            "Fields": {
                "Id": "INTEGER AUTO_INCREMENT NOT NULL",
                "UserGroup": "VARCHAR(32) NOT NULL",
                "LastAccess": "DATETIME",
            },
            "PrimaryKey": "Id",
            "UniqueIndexes": {"G": ["UserGroup"]},
            "Engine": "InnoDB",
        },
        "up_VOs": {
            "Fields": {
                "Id": "INTEGER AUTO_INCREMENT NOT NULL",
                "VO": "VARCHAR(32) NOT NULL",
                "LastAccess": "DATETIME",
            },
            "PrimaryKey": "Id",
            "UniqueIndexes": {"VO": ["VO"]},
            "Engine": "InnoDB",
        },
        "up_ProfilesData": {
            "Fields": {
                "UserId": "INTEGER",
                "GroupId": "INTEGER",
                "VOId": "INTEGER",
                "Profile": "VARCHAR(255) NOT NULL",
                "VarName": "VARCHAR(255) NOT NULL",
                "Data": "TEXT",
                "ReadAccess": 'VARCHAR(10) DEFAULT "USER"',
                "PublishAccess": 'VARCHAR(10) DEFAULT "USER"',
            },
            "PrimaryKey": ["UserId", "GroupId", "Profile", "VarName"],
            "Indexes": {
                "ProfileKey": ["UserId", "GroupId", "Profile"],
                "UserKey": ["UserId"],
            },
            "Engine": "InnoDB",
        },
    }

    def __init__(self, parentLogger=None):
        """Constructor"""
        self.__permValues = ["USER", "GROUP", "VO", "ALL"]
        self.__permAttrs = ["ReadAccess", "PublishAccess"]
        self.__cache = cachetools.TTLCache(1024, 15)
        DB.__init__(self, "UserProfileDB", "Framework/UserProfileDB", parentLogger=parentLogger)
        retVal = self.__initializeDB()
        if not retVal["OK"]:
            raise Exception(f"Can't create tables: {retVal['Message']}")

    def _checkTable(self):
        """Make sure the tables are created"""
        return self.__initializeDB()

    def __initializeDB(self):
        """
        Create the tables
        """
        retVal = self._query("show tables")
        if not retVal["OK"]:
            return retVal

        tablesInDB = [t[0] for t in retVal["Value"]]
        tablesD = {}

        if "up_Users" not in tablesInDB:
            tablesD["up_Users"] = self.tableDict["up_Users"]

        if "up_Groups" not in tablesInDB:
            tablesD["up_Groups"] = self.tableDict["up_Groups"]

        if "up_VOs" not in tablesInDB:
            tablesD["up_VOs"] = self.tableDict["up_VOs"]

        if "up_ProfilesData" not in tablesInDB:
            tablesD["up_ProfilesData"] = self.tableDict["up_ProfilesData"]

        return self._createTables(tablesD)

    def __getUserId(self, userName, insertIfMissing=True):
        return self.__getObjId(userName, "UserName", "up_Users", insertIfMissing)

    def __getGroupId(self, groupName, insertIfMissing=True):
        return self.__getObjId(groupName, "UserGroup", "up_Groups", insertIfMissing)

    def __getVOId(self, voName, insertIfMissing=True):
        return self.__getObjId(voName, "VO", "up_VOs", insertIfMissing)

    def __getFieldsCached(self, tableName, outFields, condDict):
        """Call getFields with a TTL cache

        The UserProfileDB is written in such a way that repeatedly makes the same
        DB queries thousands of times. To workaround this, use a simple short-lived
        TTL cache to dramatically improve performance.
        """
        key = (tableName, tuple(outFields), tuple(sorted(condDict.items())))
        if key in self.__cache:
            return self.__cache[key]
        result = self.getFields(tableName, outFields, condDict)
        if not result["OK"]:
            return result
        data = result["Value"]
        if len(data) > 0:
            objId = data[0][0]
            self.updateFields(tableName, ["LastAccess"], ["UTC_TIMESTAMP()"], {"Id": objId})
            self.__cache[key] = result
        return result

    def __getObjId(self, objValue, varName, tableName, insertIfMissing=True):
        result = self.__getFieldsCached(tableName, ["Id"], {varName: objValue})
        if not result["OK"]:
            return result
        data = result["Value"]
        if len(data) > 0:
            objId = data[0][0]
            return S_OK(objId)
        if not insertIfMissing:
            return S_ERROR(f"No entry {objValue} for {varName} defined in the DB")
        result = self.insertFields(tableName, [varName, "LastAccess"], [objValue, "UTC_TIMESTAMP()"])
        if not result["OK"]:
            return result
        return S_OK(result["lastRowId"])

    def getUserGroupIds(self, userName, userGroup, insertIfMissing=True):
        result = self.__getUserId(userName, insertIfMissing)
        if not result["OK"]:
            return result
        userId = result["Value"]
        result = self.__getGroupId(userGroup, insertIfMissing)
        if not result["OK"]:
            return result
        groupId = result["Value"]
        userVO = Registry.getVOForGroup(userGroup)
        if not userVO:
            userVO = "undefined"
        result = self.__getVOId(userVO, insertIfMissing)
        if not result["OK"]:
            return result
        voId = result["Value"]
        return S_OK((userId, groupId, voId))

    def deleteUserProfile(self, userName, userGroup=False):
        """
        Delete the profiles for a user
        """
        result = self.__getUserId(userName)
        if not result["OK"]:
            return result
        userId = result["Value"]
        condDict = {"UserId": userId}
        if userGroup:
            result = self.__getGroupId(userGroup)
            if not result["OK"]:
                return result
            groupId = result["Value"]
            condDict["GroupId"] = groupId
        result = self.deleteEntries("up_ProfilesData", condDict)
        if not result["OK"] or not userGroup:
            return result
        return self.deleteEntries("up_Users", {"Id": userId})

    def __webProfileUserDataCond(self, userIds, profileName=False, varName=False):
        """Returns (sqlText, args): Input parameters can be untrusted."""
        condSQL = [
            "up_ProfilesData.UserId=%s",
            "up_ProfilesData.GroupId=%s",
            "up_ProfilesData.VOId=%s",
        ]
        args = list(userIds[0:3])
        if profileName:
            condSQL.append("up_ProfilesData.Profile=%s")
            args.append(profileName)
        if varName:
            condSQL.append("up_ProfilesData.VarName=%s")
            args.append(varName)
        sql = " AND ".join(condSQL)
        return (sql, args)

    def __webProfileReadAccessDataCond(self, userIds, ownerIds, profileName, varName=False, match=False):
        """Returns (sqlText, args): Input parameters can be untrusted."""
        # Keep the args for each list seperately so we can ensure ordering is correct
        permCondSQL = []
        permCondArgs = []
        sqlCond = []
        sqlCondArgs = []

        if match:
            sqlCond.append("up_ProfilesData.UserId=%s AND up_ProfilesData.GroupId = %s")
            sqlCondArgs.extend((ownerIds[0], ownerIds[1]))
        else:
            permCondSQL.append("up_ProfilesData.UserId=%s AND up_ProfilesData.GroupId = %s")
            permCondArgs.extend((ownerIds[0], ownerIds[1]))

        permCondSQL.append("up_ProfilesData.GroupId=%s AND up_ProfilesData.ReadAccess=%s")
        permCondArgs.extend((userIds[1], "GROUP"))
        permCondSQL.append("up_ProfilesData.VOId=%s AND up_ProfilesData.ReadAccess=%s")
        permCondArgs.extend((userIds[2], "VO"))
        permCondSQL.append("up_ProfilesData.ReadAccess=%s")
        permCondArgs.append("ALL")

        sqlCond.append("up_ProfilesData.Profile=%s")
        sqlCondArgs.append(profileName)
        if varName:
            sqlCond.append("up_ProfilesData.VarName=%s")
            sqlCondArgs.append(varName)
        # Perms
        sqlCond.append(f"( ( {' ) OR ( '.join(permCondSQL)} ) )")
        sqlCondArgs.extend(permCondArgs)
        req = " AND ".join(sqlCond)
        return (req, sqlCondArgs)

    def __parsePerms(self, perms, addMissing=True):
        normPerms = {}
        for pName in self.__permAttrs:
            if not perms or pName not in perms:
                if addMissing:
                    normPerms[pName] = self.__permValues[0]
                continue
            permVal = perms[pName].upper()
            for nV in self.__permValues:
                if nV == permVal:
                    normPerms[pName] = nV
                    break
            if pName not in normPerms and addMissing:
                normPerms[pName] = self.__permValues[0]

        return normPerms

    def retrieveVarById(self, userIds, ownerIds, profileName, varName):
        """
        Get a data entry for a profile
        """
        sqlCond, args = self.__webProfileReadAccessDataCond(userIds, ownerIds, profileName, varName, True)
        # when we retrieve the user profile we have to take into account the user.
        req = "SELECT data FROM up_ProfilesData "
        req += f"WHERE {sqlCond}"
        print(req)
        print(args)
        result = self._query(req, args=args)
        if not result["OK"]:
            return result
        data = result["Value"]
        if len(data) > 0:
            # TODO: The decode is only needed in DIRAC v8.0.x while moving from BLOB -> TEXT
            return S_OK(data[0][0].decode() if isinstance(data[0][0], bytes) else data[0][0])
        return S_ERROR(f"No data for userIds {userIds} profileName {profileName} varName {varName}")

    def retrieveAllUserVarsById(self, userIds, profileName):
        """
        Get a data entry for a profile
        """
        sqlCond, args = self.__webProfileUserDataCond(userIds, profileName)
        req = "SELECT varName, data FROM up_ProfilesData "
        req += f"WHERE {sqlCond}"
        result = self._query(req, args=args)
        if not result["OK"]:
            return result
        data = result["Value"]
        try:
            # TODO: This is only needed in DIRAC v8.0.x while moving from BLOB -> TEXT
            allUserDataDict = {k: v.decode() for k, v in data}
        except AttributeError:
            allUserDataDict = {k: v for k, v in data}
        return S_OK(allUserDataDict)

    def retrieveUserProfilesById(self, userIds):
        """
        Get all profiles and data for a user
        """
        sqlCond, args = self.__webProfileUserDataCond(userIds)
        req = "SELECT Profile, varName, data FROM up_ProfilesData "
        req += f"WHERE {sqlCond}"
        result = self._query(req, args=args)
        if not result["OK"]:
            return result
        dataDict = {}
        for profile, varName, data in result["Value"]:
            if profile not in dataDict:
                dataDict[profile] = {}
            try:
                # TODO: This is only needed in DIRAC v8.0.x while moving from BLOB -> TEXT
                dataDict[profile][varName] = data.decode()
            except AttributeError:
                dataDict[profile][varName] = data
        return S_OK(dataDict)

    def retrieveVarPermsById(self, userIds, ownerIds, profileName, varName):
        """
        Get a data entry for a profile
        """
        sqlCond, args = self.__webProfileReadAccessDataCond(userIds, ownerIds, profileName, varName)
        req = "SELECT "
        req += ",".join(self.__permAttrs)
        req += " FROM up_ProfilesData WHERE "
        req += sqlCond
        result = self._query(req, args=args)
        if not result["OK"]:
            return result
        data = result["Value"]
        if len(data) > 0:
            permDict = {self.__permAttrs[i]: data[0][i] for i in range(len(self.__permAttrs))}
            return S_OK(permDict)
        return S_ERROR(f"No data for userIds {userIds} profileName {profileName} varName {varName}")

    def deleteVarByUserId(self, userIds, profileName, varName):
        """
        Remove a data entry for a profile
        """
        sqlCond, args = self.__webProfileUserDataCond(userIds, profileName, varName)
        req = "DELETE FROM up_ProfilesData "
        req += f"WHERE {sqlCond}"
        return self._update(req, args=args)

    def storeVarByUserId(self, userIds, profileName, varName, data, perms):
        """
        Set a data entry for a profile
        """
        sqlInsertValues = []
        sqlInsertKeys = []

        sqlInsertKeys.append(("UserId", userIds[0]))
        sqlInsertKeys.append(("GroupId", userIds[1]))
        sqlInsertKeys.append(("VOId", userIds[2]))

        sqlInsertKeys.append(("Profile", profileName))
        sqlInsertKeys.append(("VarName", varName))
        sqlInsertValues.append(("Data", data))

        normPerms = self.__parsePerms(perms)
        for k in normPerms:
            sqlInsertValues.append((k, f"{normPerms[k]}"))

        sqlInsert = sqlInsertKeys + sqlInsertValues
        req = "INSERT INTO up_ProfilesData ("
        req += ",".join([x[0] for x in sqlInsert])
        req += ") VALUES ("
        req += ",".join(["%s"] * len(sqlInsert))
        req += ")"
        args = [x[1] for x in sqlInsert]
        result = self._update(req, args=args, debug=False)
        if result["OK"]:
            return result
        # If error and not duplicate -> real error
        if "Duplicate entry" not in result["Message"]:
            return result
        sqlCond, condArgs = self.__webProfileUserDataCond(userIds, profileName, varName)
        args = []
        req = "UPDATE up_ProfilesData "
        req += "SET "
        req += ",".join([f"{x[0]}=%s" for x in sqlInsertValues])
        args.extend([x[1] for x in sqlInsertValues])
        req += f" WHERE {sqlCond}"
        args.extend(condArgs)
        return self._update(req, args=args)

    def setUserVarPermsById(self, userIds, profileName, varName, perms):
        nPerms = self.__parsePerms(perms, False)
        if not nPerms:
            return S_OK()
        condSql, condArgs = self.__webProfileUserDataCond(userIds, profileName, varName)
        req = "UPDATE up_ProfilesData SET "
        req += ",".join(f"{k}=%s" for k in nPerms)
        args = list(nPerms.values())
        req += " WHERE "
        req += condSql
        args.extend(condArgs)
        return self._update(req, args=args)

    def retrieveVar(self, userName, userGroup, ownerName, ownerGroup, profileName, varName):
        """
        Get a data entry for a profile
        """
        result = self.getUserGroupIds(userName, userGroup)
        if not result["OK"]:
            return result
        userIds = result["Value"]

        result = self.getUserGroupIds(ownerName, ownerGroup)
        if not result["OK"]:
            return result
        ownerIds = result["Value"]

        return self.retrieveVarById(userIds, ownerIds, profileName, varName)

    def retrieveUserProfiles(self, userName, userGroup):
        """
        Helper for getting data
        """
        result = self.getUserGroupIds(userName, userGroup)
        if not result["OK"]:
            return result
        userIds = result["Value"]
        return self.retrieveUserProfilesById(userIds)

    def retrieveAllUserVars(self, userName, userGroup, profileName):
        """
        Helper for getting data
        """
        result = self.getUserGroupIds(userName, userGroup)
        if not result["OK"]:
            return result
        userIds = result["Value"]
        return self.retrieveAllUserVarsById(userIds, profileName)

    def retrieveVarPerms(self, userName, userGroup, ownerName, ownerGroup, profileName, varName):
        result = self.getUserGroupIds(userName, userGroup)
        if not result["OK"]:
            return result
        userIds = result["Value"]

        result = self.getUserGroupIds(ownerName, ownerGroup, False)
        if not result["OK"]:
            return result
        ownerIds = result["Value"]

        return self.retrieveVarPermsById(userIds, ownerIds, profileName, varName)

    def setUserVarPerms(self, userName, userGroup, profileName, varName, perms):
        result = self.getUserGroupIds(userName, userGroup)
        if not result["OK"]:
            return result
        userIds = result["Value"]
        return self.setUserVarPermsById(userIds, profileName, varName, perms)

    def storeVar(self, userName, userGroup, profileName, varName, data, perms=None):
        """
        Helper for setting data
        """
        result = self.getUserGroupIds(userName, userGroup)
        if not result["OK"]:
            return result
        userIds = result["Value"]
        return self.storeVarByUserId(userIds, profileName, varName, data, perms=perms)

    def deleteVar(self, userName, userGroup, profileName, varName):
        """
        Helper for deleting data
        """
        result = self.getUserGroupIds(userName, userGroup)
        if not result["OK"]:
            return result
        userIds = result["Value"]
        return self.deleteVarByUserId(userIds, profileName, varName)

    def __profilesCondGenerator(self, value, varType, initialValue=False):
        if isinstance(value, str):
            value = [value]
        ids = []
        if initialValue:
            ids.append(initialValue)
        for val in value:
            if varType == "user":
                result = self.__getUserId(val, insertIfMissing=False)
            elif varType == "group":
                result = self.__getGroupId(val, insertIfMissing=False)
            else:
                result = self.__getVOId(val, insertIfMissing=False)
            if not result["OK"]:
                continue
            ids.append(result["Value"])
        if varType == "user":
            fieldName = "UserId"
        elif varType == "group":
            fieldName = "GroupId"
        else:
            fieldName = "VOId"
        sql = f"up_ProfilesData.{fieldName} IN ("
        sql += ",".join(["%s"] * len(ids))
        sql += ")"
        return (sql, ids)

    def listVarsById(self, userIds, profileName, filterDict=None):
        extConds, args = self.__webProfileReadAccessDataCond(userIds, userIds, profileName)
        sqlCond = [
            "up_Users.Id = up_ProfilesData.UserId",
            "up_Groups.Id = up_ProfilesData.GroupId",
            "up_VOs.Id = up_ProfilesData.VOId",
            extConds,
        ]
        if filterDict:
            filterDict = {k.lower(): filterDict[k] for k in filterDict}
            for k in ("user", "group", "vo"):
                if k in filterDict:
                    filterCond, filterArgs = self.__profilesCondGenerator(filterDict[k], k)
                    sqlCond.append(filterCond)
                    args.extend(filterArgs)

        req = "SELECT up_Users.UserName, up_Groups.UserGroup, up_VOs.VO, up_ProfilesData.VarName "
        req += "FROM up_Users, up_Groups, up_VOs, up_ProfilesData WHERE "
        req += " AND ".join(sqlCond)
        result = self._query(req, args=args)
        if result["OK"]:
            # Convert returned tuples to lists to appease JEncode
            result = S_OK([list(x) for x in result["Value"]])
        return result

    def listVars(self, userName, userGroup, profileName, filterDict=None):
        result = self.getUserGroupIds(userName, userGroup)
        if not result["OK"]:
            return result
        userIds = result["Value"]
        return self.listVarsById(userIds, profileName, filterDict)

    def getUserProfileNames(self, permission):
        """
        it returns the available profile names by not taking account the permission: ReadAccess and PublishAccess
        """
        permissions = self.__parsePerms(permission, False)
        if not permissions:
            return S_OK([])

        query = "SELECT DISTINCT Profile FROM up_ProfilesData WHERE "
        query += " AND ".join([f"{x}=%s" for x in permissions])
        retVal = self._query(query, args=permissions.values())
        if not retVal["OK"]:
            return retVal
        return S_OK([i[0] for i in retVal["Value"]])
