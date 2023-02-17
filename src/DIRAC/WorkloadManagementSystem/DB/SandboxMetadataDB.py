""" SandboxMetadataDB class is a front-end to the metadata for sandboxes
"""
from DIRAC import gLogger, S_OK, S_ERROR
from DIRAC.Core.Base.DB import DB
from DIRAC.Core.Utilities import List
from DIRAC.Core.Security import Properties
from DIRAC.ConfigurationSystem.Client.Helpers import Registry


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
                "OwnerDN": "VARCHAR(255) NOT NULL",
                "OwnerGroup": "VARCHAR(32) NOT NULL",
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
                "EntitySetup": "VARCHAR(64) NOT NULL",
                "EntityId": "VARCHAR(128) NOT NULL",
                "Type": "VARCHAR(64) NOT NULL",
            },
            "Indexes": {"Entity": ["EntityId", "EntitySetup"], "SBIndex": ["SBId"]},
            "UniqueIndexes": {"Mapping": ["SBId", "EntitySetup", "EntityId", "Type"]},
        }

        for tableName in self.__tablesDesc:
            if tableName not in tablesInDB:
                tablesToCreate[tableName] = self.__tablesDesc[tableName]

        return self._createTables(tablesToCreate)

    def __registerAndGetOwnerId(self, owner, ownerDN, ownerGroup):
        """
        Get the owner ID and register it if it's not there
        """
        ownerEscaped = self._escapeString(owner)["Value"]
        ownerDNEscaped = self._escapeString(ownerDN)["Value"]
        ownerGroupEscaped = self._escapeString(ownerGroup)["Value"]
        sqlCmd = "SELECT OwnerId FROM `sb_Owners` WHERE Owner = {} AND OwnerDN = {} AND OwnerGroup = {}".format(
            ownerEscaped,
            ownerDNEscaped,
            ownerGroupEscaped,
        )
        result = self._query(sqlCmd)
        if not result["OK"]:
            return result
        data = result["Value"]
        if data:
            return S_OK(data[0][0])
        # Its not there, insert it
        sqlCmd = "INSERT INTO `sb_Owners` ( OwnerId, Owner, OwnerDN, OwnerGroup ) VALUES ( 0, {}, {}, {} )".format(
            ownerEscaped,
            ownerDNEscaped,
            ownerGroupEscaped,
        )
        result = self._update(sqlCmd)
        if not result["OK"]:
            return result
        if "lastRowId" in result:
            return S_OK(result["lastRowId"])
        result = self._query("SELECT LAST_INSERT_ID()")
        if not result["OK"]:
            return S_ERROR("Can't determine owner id after insertion")
        return S_OK(result["Value"][0][0])

    def registerAndGetSandbox(self, owner, ownerDN, ownerGroup, sbSE, sbPFN, size=0):
        """
        Register a new sandbox in the metadata catalog
        Returns ( sbid, newSandbox )
        """
        result = self.__registerAndGetOwnerId(owner, ownerDN, ownerGroup)
        if not result["OK"]:
            return result
        ownerId = result["Value"]
        sqlCmd = "INSERT INTO `sb_SandBoxes` ( SBId, OwnerId, SEName, SEPFN, Bytes, RegistrationTime, LastAccessTime )"
        sqlCmd = "%s VALUES ( 0, '%s', '%s', '%s', %d, UTC_TIMESTAMP(), UTC_TIMESTAMP() )" % (
            sqlCmd,
            ownerId,
            sbSE,
            sbPFN,
            size,
        )
        result = self._update(sqlCmd)
        if not result["OK"]:
            if result["Message"].find("Duplicate entry") == -1:
                return result
            # It's a duplicate, try to retrieve sbid
            sqlCond = [f"SEPFN='{sbPFN}'", f"SEName='{sbSE}'", f"OwnerId='{ownerId}'"]
            sqlCmd = f"SELECT SBId FROM `sb_SandBoxes` WHERE {' AND '.join(sqlCond)}"
            result = self._query(sqlCmd)
            if not result["OK"]:
                return result
            if not result["Value"]:
                return S_ERROR("SandBox already exists but doesn't belong to the user or setup")
            sbId = result["Value"][0][0]
            self.accessedSandboxById(sbId)
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
        return self.__accessedSandboxByCond({"SBId": sbId})

    def __accessedSandboxByCond(self, condDict):
        sqlCond = [f"{key}={condDict[key]}" for key in condDict]
        return self._update(f"UPDATE `sb_SandBoxes` SET LastAccessTime=UTC_TIMESTAMP() WHERE {' AND '.join(sqlCond)}")

    def assignSandboxesToEntities(self, enDict, requesterName, requesterGroup, enSetup, ownerName="", ownerGroup=""):
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
                entitiesToSandboxList.append((entityId, enSetup, sbTuple[1], SEName, SEPFN))
        if not entitiesToSandboxList:
            return S_OK()

        sbIds = []
        assigned = 0
        for entityId, entitySetup, SBType, SEName, SEPFN in entitiesToSandboxList:
            result = self.getSandboxId(SEName, SEPFN, requesterName, requesterGroup)
            insertValues = []
            if not result["OK"]:
                self.log.warn(
                    f"Cannot find id for {SEName}:",
                    f"{SEPFN} with requester {requesterName}@{requesterGroup}: {result['Message']}",
                )
            else:
                sbId = result["Value"]
                sbIds.append(str(sbId))
                insertValues.append(
                    "( %s, %s, %s, %d )"
                    % (
                        self._escapeString(entityId)["Value"],
                        self._escapeString(entitySetup)["Value"],
                        self._escapeString(SBType)["Value"],
                        sbId,
                    )
                )

            if not insertValues:
                return S_ERROR(
                    "Sandbox does not exist or you're not authorized to assign it being %s@%s"
                    % (requesterName, requesterGroup)
                )
            sqlCmd = "INSERT INTO `sb_EntityMapping` ( entityId, entitySetup, Type, SBId ) VALUES %s" % ", ".join(
                insertValues
            )
            result = self._update(sqlCmd)
            if not result["OK"]:
                if result["Message"].find("Duplicate entry") == -1:
                    return result
            assigned += 1
        sqlCmd = f"UPDATE `sb_SandBoxes` SET Assigned=1 WHERE SBId in ( {', '.join(sbIds)} )"
        result = self._update(sqlCmd)
        if not result["OK"]:
            return result
        return S_OK(assigned)

    def __filterEntitiesByRequester(self, entitiesList, entitiesSetup, requesterName, requesterGroup):
        """
        Given a list of entities and a requester, return the ones that the requester is allowed to modify
        """
        sqlCond = ["s.OwnerId=o.OwnerId", "s.SBId=e.SBId", f"e.EntitySetup={entitiesSetup}"]
        requesterProps = Registry.getPropertiesForEntity(requesterGroup, name=requesterName)
        if Properties.JOB_ADMINISTRATOR in requesterProps:
            # Do nothing, just ensure it doesn't fit in the other cases
            pass
        elif Properties.JOB_SHARING in requesterProps:
            sqlCond.append(f"o.OwnerGroup='{requesterGroup}'")
        elif Properties.NORMAL_USER in requesterProps:
            sqlCond.append(f"o.OwnerGroup='{requesterGroup}'")
            sqlCond.append(f"o.Owner='{requesterName}'")
        else:
            return S_ERROR("Not authorized to access sandbox")
        for i in range(len(entitiesList)):
            entitiesList[i] = self._escapeString(entitiesList[i])["Value"]
        if len(entitiesList) == 1:
            sqlCond.append(f"e.EntityId = {entitiesList[0]}")
        else:
            sqlCond.append(f"e.EntityId in ( {', '.join(entitiesList)} )")
        sqlCmd = "SELECT DISTINCT e.EntityId FROM `sb_EntityMapping` e, `sb_SandBoxes` s, `sb_Owners` o WHERE"
        sqlCmd = f"{sqlCmd} {' AND '.join(sqlCond)}"
        result = self._query(sqlCmd)
        if not result["OK"]:
            return result
        return S_OK([row[0] for row in result["Value"]])

    def unassignEntities(self, entitiesDict, requesterName, requesterGroup):
        """
        Unassign jobs to sandboxes
        entitiesDict = { 'setup' : [ 'entityId', 'entityId' ] }
        """
        updated = 0
        for entitySetup in entitiesDict:
            entitiesIds = entitiesDict[entitySetup]
            if not entitiesIds:
                continue
            escapedSetup = self._escapeString(entitySetup)["Value"]
            result = self.__filterEntitiesByRequester(entitiesIds, escapedSetup, requesterName, requesterGroup)
            if not result["OK"]:
                gLogger.error("Cannot filter entities", result["Message"])
                continue
            ids = result["Value"]
            if not ids:
                return S_OK(0)
            sqlCond = [f"EntitySetup = {escapedSetup}"]
            sqlCond.append("EntityId in ( %s )" % ", ".join(["'%s'" % str(eid) for eid in ids]))
            sqlCmd = f"DELETE FROM `sb_EntityMapping` WHERE {' AND '.join(sqlCond)}"
            result = self._update(sqlCmd)
            if not result["OK"]:
                gLogger.error("Cannot unassign entities", result["Message"])
            else:
                updated += 1
        return S_OK(updated)

    def getSandboxesAssignedToEntity(self, entityId, entitySetup, requesterName, requesterGroup):
        """
        Get the sandboxes and the type of assignation to the jobId
        """
        sqlTables = ["`sb_SandBoxes` s", "`sb_EntityMapping` e"]
        sqlCond = [
            "s.SBId = e.SBId",
            f"e.EntityId = {self._escapeString(entityId)['Value']}",
            f"e.EntitySetup = {self._escapeString(entitySetup)['Value']}",
        ]
        requesterProps = Registry.getPropertiesForEntity(requesterGroup, name=requesterName)
        if Properties.JOB_ADMINISTRATOR in requesterProps or Properties.JOB_MONITOR in requesterProps:
            # Do nothing, just ensure it doesn't fit in the other cases
            pass
        elif Properties.JOB_SHARING in requesterProps:
            sqlTables.append("`sb_Owners` o")
            sqlCond.append(f"o.OwnerGroup='{requesterGroup}'")
            sqlCond.append("s.OwnerId=o.OwnerId")
        elif Properties.NORMAL_USER in requesterProps:
            sqlTables.append("`sb_Owners` o")
            sqlCond.append(f"o.OwnerGroup='{requesterGroup}'")
            sqlCond.append(f"o.Owner='{requesterName}'")
            sqlCond.append("s.OwnerId=o.OwnerId")
        else:
            return S_ERROR("Not authorized to access sandbox")
        sqlCmd = "SELECT DISTINCT s.SEName, s.SEPFN, e.Type FROM  {} WHERE {}".format(
            ", ".join(sqlTables),
            " AND ".join(sqlCond),
        )
        return self._query(sqlCmd)

    def getUnusedSandboxes(self):
        """
        Get sandboxes that have been assigned but the job is no longer there
        """
        sqlCond = [
            "Assigned AND SBId NOT IN ( SELECT SBId FROM `sb_EntityMapping` ) AND "
            "TIMESTAMPDIFF( DAY, LastAccessTime, UTC_TIMESTAMP() ) >= %d" % self.__assignedSBGraceDays,
            f"! Assigned AND TIMESTAMPDIFF( DAY, LastAccessTime, UTC_TIMESTAMP() ) >= {self.__unassignedSBGraceDays}",
        ]
        sqlCmd = f"SELECT SBId, SEName, SEPFN FROM `sb_SandBoxes` WHERE ( {' ) OR ( '.join(sqlCond)} )"
        return self._query(sqlCmd)

    def deleteSandboxes(self, SBIdList):
        """
        Delete sandboxes
        """
        sqlSBList = ", ".join([str(sbid) for sbid in SBIdList])
        for table in ("sb_SandBoxes", "sb_EntityMapping"):
            sqlCmd = f"DELETE FROM `{table}` WHERE SBId IN ( {sqlSBList} )"
            result = self._update(sqlCmd)
            if not result["OK"]:
                return result
        return S_OK()

    def getSandboxId(self, SEName, SEPFN, requesterName, requesterGroup, field="SBId", requesterDN=None):
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
        sqlCond = [
            f"s.SEPFN={self._escapeString(SEPFN)['Value']}",
            f"s.SEName={self._escapeString(SEName)['Value']}",
            "s.OwnerId=o.OwnerId",
        ]
        sqlCmd = f"SELECT s.{field} FROM `sb_SandBoxes` s, `sb_Owners` o WHERE"
        requesterProps = Registry.getPropertiesForEntity(requesterGroup, name=requesterName, dn=requesterDN)
        if Properties.JOB_ADMINISTRATOR in requesterProps or Properties.JOB_MONITOR in requesterProps:
            # Do nothing, just ensure it doesn't fit in the other cases
            pass
        elif Properties.JOB_SHARING in requesterProps:
            sqlCond.append(f"o.OwnerGroup='{requesterGroup}'")
        elif Properties.NORMAL_USER in requesterProps:
            sqlCond.append(f"o.OwnerGroup='{requesterGroup}'")
            sqlCond.append(f"o.Owner='{requesterName}'")
        else:
            return S_ERROR("Not authorized to access sandbox")
        result = self._query(f"{sqlCmd} {' AND '.join(sqlCond)}")
        if not result["OK"]:
            return result
        data = result["Value"]
        if len(data) > 1:
            self.log.error("More than one sandbox registered with the same Id!", data)
        if not data:
            return S_ERROR("No sandbox matches the requirements")
        return S_OK(data[0][0])

    def getSandboxOwner(self, SEName, SEPFN, requesterDN, requesterGroup):
        """get the sandbox owner, if such sandbox exists

        :param SEName: name of the StorageElement
        :param SEPFN: PFN of the Sandbox
        :param requestDN: host DN used as credentials
        :param requesterGroup: group used to use as credentials (should be 'hosts')

        :returns: S_OK with tuple (owner, ownerDN, ownerGroup)
        """
        res = self.getSandboxId(SEName, SEPFN, None, requesterGroup, "OwnerId", requesterDN=requesterDN)
        if not res["OK"]:
            return res

        sqlCmd = "SELECT `Owner`, `OwnerDN`, `OwnerGroup` FROM `sb_Owners` WHERE `OwnerId` = %d" % res["Value"]
        res = self._query(sqlCmd)
        if not res["OK"]:
            return res
        return S_OK(res["Value"][0])
