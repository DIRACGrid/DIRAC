""" SandboxMetadataDB class is a front-end to the metadata for sandboxes
"""
from DIRAC import S_ERROR, S_OK
from DIRAC.ConfigurationSystem.Client.Helpers import Registry
from DIRAC.Core.Base.DB import DB
from DIRAC.Core.Security import Properties
from DIRAC.Core.Utilities import List
from DIRAC.Core.Utilities.ReturnValues import convertToReturnValue, returnValueOrRaise


class SandboxMetadataDB(DB):
    def __init__(self, parentLogger=None):
        DB.__init__(self, "SandboxMetadataDB", "WorkloadManagement/SandboxMetadataDB", parentLogger=parentLogger)
        result = self.__initializeDB()
        if not result["OK"]:
            raise RuntimeError(f"Can't create tables: {result['Message']}")
        self.__assignedSBGraceDays = 0
        self.__unassignedSBGraceDays = 15

    def __initializeDB(self):
        """
        Create the tables
        """
        result = self._query("show tables")
        if not result["OK"]:
            return result

        tablesInDB = [t[0] for t in result["Value"]]
        tablesToCreate = {}
        self.__tablesDesc = {}

        self.__tablesDesc["sb_Owners"] = {
            "Fields": {
                "OwnerId": "INTEGER(10) UNSIGNED AUTO_INCREMENT NOT NULL",
                "Owner": "VARCHAR(32) NOT NULL",
                "OwnerGroup": "VARCHAR(32) NOT NULL",
                "VO": "VARCHAR(64) NOT NULL",
            },
            "PrimaryKey": "OwnerId",
        }

        self.__tablesDesc["sb_SandBoxes"] = {
            "Fields": {
                "SBId": "INTEGER(10) UNSIGNED AUTO_INCREMENT NOT NULL",
                "OwnerId": "INTEGER(10) UNSIGNED NOT NULL",
                "SEName": "VARCHAR(64) NOT NULL",
                "SEPFN": "VARCHAR(512) NOT NULL",
                "Bytes": "BIGINT(20) NOT NULL DEFAULT 0",
                "RegistrationTime": "DATETIME NOT NULL",
                "LastAccessTime": "DATETIME NOT NULL",
                "Assigned": "TINYINT NOT NULL DEFAULT 0",
            },
            "PrimaryKey": "SBId",
            "Indexes": {
                "SBOwner": ["OwnerId"],
            },
            "UniqueIndexes": {"Location": ["SEName", "SEPFN"]},
        }

        self.__tablesDesc["sb_EntityMapping"] = {
            "Fields": {
                "SBId": "INTEGER(10) UNSIGNED NOT NULL",
                "EntityId": "VARCHAR(128) NOT NULL",
                "Type": "VARCHAR(64) NOT NULL",
            },
            "Indexes": {"Entity": ["EntityId"], "SBIndex": ["SBId"]},
            "PrimaryKey": ["SBId", "EntityId", "Type"],
        }

        for tableName in self.__tablesDesc:
            if tableName not in tablesInDB:
                tablesToCreate[tableName] = self.__tablesDesc[tableName]

        return self._createTables(tablesToCreate)

    def __registerAndGetOwnerId(self, owner, ownerGroup, VO):
        """
        Get the owner ID and register it if it's not there
        """
        if not VO:
            return S_ERROR("VO is not specified")
        sqlCmd = "SELECT OwnerId FROM `sb_Owners` WHERE Owner = %s AND OwnerGroup = %s AND VO = %s"
        result = self._query(sqlCmd, args=(owner, ownerGroup, VO))
        if not result["OK"]:
            return result
        data = result["Value"]
        if data:
            return S_OK(data[0][0])
        # Its not there, insert it
        sqlCmd = "INSERT INTO `sb_Owners` ( OwnerId, Owner, OwnerGroup, VO ) VALUES ( 0, %s, %s, %s )"
        result = self._update(sqlCmd, args=(owner, ownerGroup, VO))
        if not result["OK"]:
            return result
        if "lastRowId" in result:
            return S_OK(result["lastRowId"])
        result = self._query("SELECT LAST_INSERT_ID()")
        if not result["OK"]:
            return S_ERROR("Can't determine owner id after insertion")
        return S_OK(result["Value"][0][0])

    def registerAndGetSandbox(self, owner, ownerGroup, VO, sbSE, sbPFN, size=0):
        """
        Register a new sandbox in the metadata catalog
        Returns ( sbid, newSandbox )
        """
        result = self.__registerAndGetOwnerId(owner, ownerGroup, VO)
        if not result["OK"]:
            return result
        ownerId = result["Value"]
        sqlCmd = "INSERT INTO `sb_SandBoxes` ( SBId, OwnerId, SEName, SEPFN, Bytes, RegistrationTime, LastAccessTime )"
        sqlCmd += " VALUES ( 0, %s, %s, %s, %s, UTC_TIMESTAMP(), UTC_TIMESTAMP() )"
        args = (ownerId, sbSE, sbPFN, size)
        result = self._update(sqlCmd, args=args)
        if not result["OK"]:
            if result["Message"].find("Duplicate entry") == -1:
                return result
            # It's a duplicate, try to retrieve sbid
            sqlCmd = "SELECT SBId FROM `sb_SandBoxes` WHERE SEPFN=%s AND SEName=%s and OwnerID=%s"
            args = (sbPFN, sbSE, ownerId)
            result = self._query(sqlCmd, args=args)
            if not result["OK"]:
                return result
            if not result["Value"]:
                return S_ERROR("SandBox already exists but doesn't belong to the user")
            sbId = result["Value"][0][0]
            if not (
                ret := self._update(
                    "UPDATE `sb_SandBoxes` SET LastAccessTime=UTC_TIMESTAMP() WHERE SBId = %s", args=(sbId,)
                )
            )["OK"]:
                return ret
            return S_OK((sbId, False))
        # Inserted, time to get the id
        if "lastRowId" in result:
            return S_OK((result["lastRowId"], True))
        result = self._query("SELECT LAST_INSERT_ID()")
        if not result["OK"]:
            return S_ERROR("Can't determine sandbox id after insertion")
        return S_OK((result["Value"][0][0], True))

    def accessedSandboxById(self, sbId):
        """
        Update last access time for sb id
        """
        return self._update("UPDATE `sb_SandBoxes` SET LastAccessTime=UTC_TIMESTAMP() WHERE SBId = %s", args=(sbId,))

    def assignSandboxesToEntities(self, enDict, requesterName, requesterGroup, ownerName="", ownerGroup=""):
        """
        Assign jobs to entities
        """

        if ownerName or ownerGroup:
            requesterProps = Registry.getPropertiesForEntity(requesterGroup, name=requesterName)
            if Properties.JOB_ADMINISTRATOR in requesterProps:
                if ownerName:
                    requesterName = ownerName
                if ownerGroup:
                    requesterGroup = ownerGroup

        entitiesToSandboxList = []
        for entityId in enDict:
            for sbTuple in enDict[entityId]:
                if not isinstance(sbTuple, (tuple, list)):
                    return S_ERROR(f"Entry for entity {entityId} is not an iterable of tuples/lists")
                if len(sbTuple) != 2:
                    return S_ERROR(f"SB definition is not ( SBLocation, Type )! It's {sbTuple}")
                SBLocation = sbTuple[0]
                if SBLocation.find("SB:") != 0:
                    return S_ERROR(f"{SBLocation} doesn't seem to be a sandbox")
                SBLocation = SBLocation[3:]
                splitted = List.fromChar(SBLocation, "|")
                if len(splitted) < 2:
                    return S_ERROR("SB Location has to have SEName|SEPFN form")
                SEName = splitted[0]
                SEPFN = ":".join(splitted[1:])
                entitiesToSandboxList.append((entityId, sbTuple[1], SEName, SEPFN))
        if not entitiesToSandboxList:
            return S_OK()

        sbIds = []
        assigned = 0
        for entityId, SBType, SEName, SEPFN in entitiesToSandboxList:
            result = self.getSandboxId(SEName, SEPFN, requesterName, requesterGroup)
            if not result["OK"]:
                self.log.warn(
                    f"Cannot find id for {SEName}:",
                    f"{SEPFN} with requester {requesterName}@{requesterGroup}: {result['Message']}",
                )
                return S_ERROR(
                    f"Sandbox does not exist or you are not authorized to assign it being {requesterName}@{requesterGroup}"
                )
            sbId = result["Value"]
            sbIds.append(str(sbId))
            sqlCmd = "INSERT INTO `sb_EntityMapping` ( entityId, Type, SBId ) VALUES (%s, %s, %s)"
            result = self._update(sqlCmd, args=(entityId, SBType, sbId))
            if not result["OK"]:
                if result["Message"].find("Duplicate entry") == -1:
                    return result
            assigned += 1
        sqlCmd = "UPDATE `sb_SandBoxes` SET Assigned=1 WHERE SBId in ("
        sqlCmd += ",".join(["%s"] * len(sbIds))
        sqlCmd += ")"
        if not (result := self._update(sqlCmd, args=sbIds))["OK"]:
            return result
        return S_OK(assigned)

    @convertToReturnValue
    def unassignEntities(self, entities: list):
        """
        Unassign entities to sandboxes. Entities are a list of strings, e.g. ['job:1234', 'job:5678'].

        :param list entities: list of entities to unassign
        """
        if not entities:
            return None

        sqlCmd = "CREATE TEMPORARY TABLE to_delete_EntityId (EntityId VARCHAR(128) NOT NULL, PRIMARY KEY (EntityId)) ENGINE=MEMORY;"
        returnValueOrRaise(self._update(sqlCmd))
        try:
            sqlCmd = "INSERT INTO to_delete_EntityId (EntityId) VALUES ( %s )"
            returnValueOrRaise(self._updatemany(sqlCmd, [(e,) for e in entities]))
            sqlCmd = "DELETE m from `sb_EntityMapping` m JOIN to_delete_EntityId t USING (EntityId)"
            returnValueOrRaise(self._update(sqlCmd))
        finally:
            sqlCmd = "DROP TEMPORARY TABLE to_delete_EntityId"
            returnValueOrRaise(self._update(sqlCmd))
        return 1

    def getSandboxesAssignedToEntity(self, entityId, requesterName, requesterGroup, requestedVO):
        """
        Get the sandboxes and the type of assignation to the jobId
        """
        sqlTables = ["`sb_SandBoxes` s", "`sb_EntityMapping` e"]
        sqlCond = [
            "s.SBId = e.SBId",
            "e.EntityId = %s",
        ]
        args = [entityId]
        requesterProps = Registry.getPropertiesForEntity(requesterGroup, name=requesterName)
        if Properties.JOB_ADMINISTRATOR in requesterProps or Properties.JOB_MONITOR in requesterProps:
            # Do nothing, just ensure it doesn't fit in the other cases
            pass
        elif Properties.JOB_SHARING in requesterProps:
            sqlTables.append("`sb_Owners` o")
            sqlCond.append("o.OwnerGroup=%s")
            sqlCond.append("s.OwnerId=o.OwnerId")
            sqlCond.append("o.VO=%s")
            args.extend([requesterGroup, requestedVO])
        elif Properties.NORMAL_USER in requesterProps:
            sqlTables.append("`sb_Owners` o")
            sqlCond.append("o.OwnerGroup=%s")
            sqlCond.append("o.Owner=%s")
            sqlCond.append("s.OwnerId=o.OwnerId")
            sqlCond.append("o.VO=%s")
            args.extend([requesterGroup, requesterName, requestedVO])
        else:
            return S_ERROR("Not authorized to access sandbox")
        sqlCmd = "SELECT DISTINCT s.SEName, s.SEPFN, e.Type FROM "
        sqlCmd += ",".join(sqlTables)
        sqlCmd += " WHERE "
        sqlCmd += " AND ".join(sqlCond)
        return self._query(sqlCmd, args=args)

    def getUnusedSandboxes(self):
        """
        Get sandboxes that have been assigned but the job is no longer there
        """
        sqlCond = [
            "Assigned AND SBId NOT IN ( SELECT SBId FROM `sb_EntityMapping` ) AND "
            "TIMESTAMPDIFF( DAY, LastAccessTime, UTC_TIMESTAMP() ) >= %s",
            "! Assigned AND TIMESTAMPDIFF( DAY, LastAccessTime, UTC_TIMESTAMP() ) >= %s",
        ]
        args = [self.__assignedSBGraceDays, self.__unassignedSBGraceDays]
        # Exclude sandboxes that are in S3 as those are handled by DiracX
        sqlCmd = "SELECT SBId, SEName, SEPFN FROM `sb_SandBoxes` WHERE SEPFN NOT LIKE '/S3/%%' AND (( "
        sqlCmd += " ) OR ( ".join(sqlCond)
        sqlCmd += "))"
        return self._query(sqlCmd, args=args)

    @convertToReturnValue
    def deleteSandboxes(self, SBIdList):
        """
        Delete sandboxes using a temporary table for efficiency and consistency.
        """
        if not SBIdList:
            return S_OK()
        # Create temporary table
        sqlCmd = "CREATE TEMPORARY TABLE to_delete_SBId (SBId INTEGER(10) UNSIGNED NOT NULL, PRIMARY KEY (SBId)) ENGINE=MEMORY;"
        returnValueOrRaise(self._update(sqlCmd))
        try:
            # Insert SBIds into temporary table
            sqlCmd = "INSERT INTO to_delete_SBId (SBId) VALUES (%s)"
            returnValueOrRaise(self._updatemany(sqlCmd, [(sbid,) for sbid in SBIdList]))
            # Delete from sb_EntityMapping first (to respect FK constraints if any)
            sqlCmd = "DELETE FROM `sb_EntityMapping` WHERE SBId IN (SELECT SBId FROM to_delete_SBId)"
            returnValueOrRaise(self._update(sqlCmd))
            # Delete from sb_SandBoxes
            sqlCmd = "DELETE FROM `sb_SandBoxes` WHERE SBId IN (SELECT SBId FROM to_delete_SBId)"
            returnValueOrRaise(self._update(sqlCmd))
        finally:
            # Drop temporary table
            sqlCmd = "DROP TEMPORARY TABLE to_delete_SBId"
            returnValueOrRaise(self._update(sqlCmd))
        return S_OK()

    def getSandboxId(self, SEName, SEPFN, requesterName, requesterGroup, field="SBId"):
        """
        Get the sandboxId if it exists

        :param SEName: name of the StorageElement
        :param SEPFN: PFN of the Sandbox
        :param requesterName: name (host or user) to use as credentials
        :param requesterGroup: user group used to use as credentials, or 'hosts'
        :param field: field we want to look for (default SBId)
        :param requestDN: host DN used as credentials

        :returns: S_OK with sandbox ID

        """
        allowedFields = [x.lower() for x in self.__tablesDesc["sb_SandBoxes"]["Fields"]]
        if not field.lower() in allowedFields:
            return S_ERROR(f"Unknown field '{field}' in getSandboxID")
        sqlCond = [
            "s.SEPFN=%s",
            "s.SEName=%s",
            "s.OwnerId=o.OwnerId",
        ]
        args = [SEPFN, SEName]
        sqlCmd = "SELECT s." + field
        sqlCmd += " FROM `sb_SandBoxes` s, `sb_Owners` o WHERE "
        requesterProps = Registry.getPropertiesForEntity(requesterGroup, name=requesterName)
        if Properties.JOB_ADMINISTRATOR in requesterProps or Properties.JOB_MONITOR in requesterProps:
            # Do nothing, just ensure it doesn't fit in the other cases
            pass
        elif Properties.JOB_SHARING in requesterProps:
            sqlCond.append("o.OwnerGroup=%s")
            args.append(requesterGroup)
        elif Properties.NORMAL_USER in requesterProps:
            sqlCond.append("o.OwnerGroup=%s")
            sqlCond.append("o.Owner=%s")
            args.extend([requesterGroup, requesterName])
        else:
            return S_ERROR("Not authorized to access sandbox")
        sqlCmd += " AND ".join(sqlCond)
        result = self._query(sqlCmd, args=args)
        if not result["OK"]:
            return result
        data = result["Value"]
        if len(data) > 1:
            self.log.error("More than one sandbox registered with the same Id!", data)
        if not data:
            return S_ERROR("No sandbox matches the requirements")
        return S_OK(data[0][0])
