""" SandboxMetadataDB class is a front-end to the metadata for sandboxes
"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

__RCSID__ = "$Id$"

from DIRAC import gLogger, S_OK, S_ERROR
from DIRAC.Core.Base.DB import DB
from DIRAC.Core.Utilities import List
from DIRAC.Core.Security import Properties
from DIRAC.ConfigurationSystem.Client.Helpers import Registry


class SandboxMetadataDB(DB):

  def __init__(self):
    DB.__init__(self, 'SandboxMetadataDB', 'WorkloadManagement/SandboxMetadataDB')
    result = self.__initializeDB()
    if not result['OK']:
      raise RuntimeError("Can't create tables: %s" % result['Message'])
    self.__assignedSBGraceDays = 0
    self.__unassignedSBGraceDays = 15

  def __initializeDB(self):
    """
    Create the tables
    """
    result = self._query("show tables")
    if not result['OK']:
      return result

    tablesInDB = [t[0] for t in result['Value']]
    tablesToCreate = {}
    self.__tablesDesc = {}

    self.__tablesDesc['sb_Owners'] = {'Fields': {'OwnerId': 'INTEGER(10) UNSIGNED AUTO_INCREMENT NOT NULL',
                                                 'Owner': 'VARCHAR(32) NOT NULL',
                                                 'OwnerDN': 'VARCHAR(255) NOT NULL',
                                                 'OwnerGroup': 'VARCHAR(32) NOT NULL',
                                                 },
                                      'PrimaryKey': 'OwnerId',
                                      }

    self.__tablesDesc['sb_SandBoxes'] = {'Fields': {'SBId': 'INTEGER(10) UNSIGNED AUTO_INCREMENT NOT NULL',
                                                    'OwnerId': 'INTEGER(10) UNSIGNED NOT NULL',
                                                    'SEName': 'VARCHAR(64) NOT NULL',
                                                    'SEPFN': 'VARCHAR(512) NOT NULL',
                                                    'Bytes': 'BIGINT(20) NOT NULL DEFAULT 0',
                                                    'RegistrationTime': 'DATETIME NOT NULL',
                                                    'LastAccessTime': 'DATETIME NOT NULL',
                                                    'Assigned': 'TINYINT NOT NULL DEFAULT 0',
                                                    },
                                         'PrimaryKey': 'SBId',
                                         'Indexes': {'SBOwner': ['OwnerId'],
                                                     },
                                         'UniqueIndexes': {'Location': ['SEName', 'SEPFN']}

                                         }

    self.__tablesDesc['sb_EntityMapping'] = {'Fields': {'SBId': 'INTEGER(10) UNSIGNED NOT NULL',
                                                        'EntitySetup': 'VARCHAR(64) NOT NULL',
                                                        'EntityId': 'VARCHAR(128) NOT NULL',
                                                        'Type': 'VARCHAR(64) NOT NULL',
                                                        },
                                             'Indexes': {'Entity': ['EntityId', 'EntitySetup'],
                                                         'SBIndex': ['SBId']
                                                         },
                                             'UniqueIndexes': {'Mapping': ['SBId', 'EntitySetup', 'EntityId', 'Type']}
                                             }

    for tableName in self.__tablesDesc:
      if tableName not in tablesInDB:
        tablesToCreate[tableName] = self.__tablesDesc[tableName]

    return self._createTables(tablesToCreate)

  def registerAndGetOwnerId(self, owner, ownerDN, ownerGroup):
    """
    Get the owner ID and register it if it's not there
    """
    ownerEscaped = self._escapeString(owner)['Value']
    ownerDNEscaped = self._escapeString(ownerDN)['Value']
    ownerGroupEscaped = self._escapeString(ownerGroup)['Value']
    sqlCmd = "SELECT OwnerId FROM `sb_Owners` WHERE Owner = %s AND OwnerDN = %s AND OwnerGroup = %s" % (
        ownerEscaped, ownerDNEscaped, ownerGroupEscaped)
    result = self._query(sqlCmd)
    if not result['OK']:
      return result
    data = result['Value']
    if data:
      return S_OK(data[0][0])
    # Its not there, insert it
    sqlCmd = "INSERT INTO `sb_Owners` ( OwnerId, Owner, OwnerDN, OwnerGroup ) VALUES ( 0, %s, %s, %s )" % (
        ownerEscaped, ownerDNEscaped, ownerGroupEscaped)
    result = self._update(sqlCmd)
    if not result['OK']:
      return result
    if 'lastRowId' in result:
      return S_OK(result['lastRowId'])
    result = self._query("SELECT LAST_INSERT_ID()")
    if not result['OK']:
      return S_ERROR("Can't determine owner id after insertion")
    return S_OK(result['Value'][0][0])

  def registerAndGetSandbox(self, owner, ownerDN, ownerGroup, sbSE, sbPFN, size=0):
    """
    Register a new sandbox in the metadata catalog
    Returns ( sbid, newSandbox )
    """
    result = self.registerAndGetOwnerId(owner, ownerDN, ownerGroup)
    if not result['OK']:
      return result
    ownerId = result['Value']
    sqlCmd = "INSERT INTO `sb_SandBoxes` ( SBId, OwnerId, SEName, SEPFN, Bytes, RegistrationTime, LastAccessTime )"
    sqlCmd = "%s VALUES ( 0, '%s', '%s', '%s', %d, UTC_TIMESTAMP(), UTC_TIMESTAMP() )" % (sqlCmd, ownerId, sbSE,
                                                                                          sbPFN, size)
    result = self._update(sqlCmd)
    if not result['OK']:
      if result['Message'].find("Duplicate entry") == -1:
        return result
      # It's a duplicate, try to retrieve sbid
      sqlCond = ["SEPFN='%s'" % sbPFN, "SEName='%s'" % sbSE, "OwnerId='%s'" % ownerId]
      sqlCmd = "SELECT SBId FROM `sb_SandBoxes` WHERE %s" % " AND ".join(sqlCond)
      result = self._query(sqlCmd)
      if not result['OK']:
        return result
      if not result['Value']:
        return S_ERROR("Location %s already exists but doesn't belong to the user or setup")
      sbId = result['Value'][0][0]
      self.accessedSandboxById(sbId)
      return S_OK((sbId, False))
    # Inserted, time to get the id
    if 'lastRowId' in result:
      return S_OK((result['lastRowId'], True))
    result = self._query("SELECT LAST_INSERT_ID()")
    if not result['OK']:
      return S_ERROR("Can't determine sand box id after insertion")
    return S_OK((result['Value'][0][0], True))

  def accessedSandboxById(self, sbId):
    """
    Update last access time for sb id
    """
    return self.__accessedSandboxByCond({'SBId': sbId})

  def __accessedSandboxByCond(self, condDict):
    sqlCond = ["%s=%s" % (key, condDict[key]) for key in condDict]
    return self._update("UPDATE `sb_SandBoxes` SET LastAccessTime=UTC_TIMESTAMP() WHERE %s" % " AND ".join(sqlCond))

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
          return S_ERROR("Entry for entity %s is not a itterable of tuples/lists" % entityId)
        if len(sbTuple) != 2:
          return S_ERROR("SB definition is not ( SBLocation, Type )! It's '%s'" % str(sbTuple))
        SBLocation = sbTuple[0]
        if SBLocation.find("SB:") != 0:
          return S_ERROR("%s doesn't seem to be a sandbox" % SBLocation)
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
      if not result['OK']:
        self.log.warn("Cannot find id for %s:%s with requester %s@%s" % (SEName, SEPFN, requesterName, requesterGroup))
      else:
        sbId = result['Value']
        sbIds.append(str(sbId))
        insertValues.append("( %s, %s, %s, %d )" % (self._escapeString(entityId)['Value'],
                                                    self._escapeString(entitySetup)['Value'],
                                                    self._escapeString(SBType)['Value'],
                                                    sbId))

      if not insertValues:
        return S_ERROR(
            "Sandbox does not exist or you're not authorized to assign it being %s@%s" %
            (requesterName, requesterGroup))
      sqlCmd = "INSERT INTO `sb_EntityMapping` ( entityId, entitySetup, Type, SBId ) VALUES %s" % ", ".join(
          insertValues)
      result = self._update(sqlCmd)
      if not result['OK']:
        if result['Message'].find("Duplicate entry") == -1:
          return result
      assigned += 1
    sqlCmd = "UPDATE `sb_SandBoxes` SET Assigned=1 WHERE SBId in ( %s )" % ", ".join(sbIds)
    result = self._update(sqlCmd)
    if not result['OK']:
      return result
    return S_OK(assigned)

  def __filterEntitiesByRequester(self, entitiesList, entitiesSetup, requesterName, requesterGroup):
    """
    Given a list of entities and a requester, return the ones that the requester is allowed to modify
    """
    sqlCond = ["s.OwnerId=o.OwnerId", "s.SBId=e.SBId", "e.EntitySetup=%s" % entitiesSetup]
    requesterProps = Registry.getPropertiesForEntity(requesterGroup, name=requesterName)
    if Properties.JOB_ADMINISTRATOR in requesterProps:
      # Do nothing, just ensure it doesn't fit in the other cases
      pass
    elif Properties.JOB_SHARING in requesterProps:
      sqlCond.append("o.OwnerGroup='%s'" % requesterGroup)
    elif Properties.NORMAL_USER in requesterProps:
      sqlCond.append("o.OwnerGroup='%s'" % requesterGroup)
      sqlCond.append("o.Owner='%s'" % requesterName)
    else:
      return S_ERROR("Not authorized to access sandbox")
    for i in range(len(entitiesList)):
      entitiesList[i] = self._escapeString(entitiesList[i])['Value']
    if len(entitiesList) == 1:
      sqlCond.append("e.EntityId = %s" % entitiesList[0])
    else:
      sqlCond.append("e.EntityId in ( %s )" % ", ".join(entitiesList))
    sqlCmd = "SELECT DISTINCT e.EntityId FROM `sb_EntityMapping` e, `sb_SandBoxes` s, `sb_Owners` o WHERE"
    sqlCmd = "%s %s" % (sqlCmd, " AND ".join(sqlCond))
    result = self._query(sqlCmd)
    if not result['OK']:
      return result
    return S_OK([row[0] for row in result['Value']])

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
      escapedSetup = self._escapeString(entitySetup)['Value']
      result = self.__filterEntitiesByRequester(entitiesIds, escapedSetup, requesterName, requesterGroup)
      if not result['OK']:
        gLogger.error("Cannot filter entities: %s" % result['Message'])
        continue
      ids = result['Value']
      if not ids:
        return S_OK(0)
      sqlCond = ["EntitySetup = %s" % escapedSetup]
      sqlCond.append("EntityId in ( %s )" % ", ".join(["'%s'" % str(eid) for eid in ids]))
      sqlCmd = "DELETE FROM `sb_EntityMapping` WHERE %s" % " AND ".join(sqlCond)
      result = self._update(sqlCmd)
      if not result['OK']:
        gLogger.error("Cannot unassign entities: %s" % result['Message'])
      else:
        updated += 1
    return S_OK(updated)

  def getSandboxesAssignedToEntity(self, entityId, entitySetup, requesterName, requesterGroup):
    """
    Get the sandboxes and the type of assignation to the jobId
    """
    sqlTables = ["`sb_SandBoxes` s", "`sb_EntityMapping` e"]
    sqlCond = ["s.SBId = e.SBId",
               "e.EntityId = %s" % self._escapeString(entityId)['Value'],
               "e.EntitySetup = %s" % self._escapeString(entitySetup)['Value']]
    requesterProps = Registry.getPropertiesForEntity(requesterGroup, name=requesterName)
    if Properties.JOB_ADMINISTRATOR in requesterProps or Properties.JOB_MONITOR in requesterProps:
      # Do nothing, just ensure it doesn't fit in the other cases
      pass
    elif Properties.JOB_SHARING in requesterProps:
      sqlTables.append("`sb_Owners` o")
      sqlCond.append("o.OwnerGroup='%s'" % requesterGroup)
      sqlCond.append("s.OwnerId=o.OwnerId")
    elif Properties.NORMAL_USER in requesterProps:
      sqlTables.append("`sb_Owners` o")
      sqlCond.append("o.OwnerGroup='%s'" % requesterGroup)
      sqlCond.append("o.Owner='%s'" % requesterName)
      sqlCond.append("s.OwnerId=o.OwnerId")
    else:
      return S_ERROR("Not authorized to access sandbox")
    sqlCmd = "SELECT DISTINCT s.SEName, s.SEPFN, e.Type FROM  %s WHERE %s" % (", ".join(sqlTables),
                                                                              " AND ".join(sqlCond))
    return self._query(sqlCmd)

  def getUnusedSandboxes(self):
    """
    Get sandboxes that have been assigned but the job is no longer there
    """
    sqlCond = [
        "Assigned AND SBId NOT IN ( SELECT SBId FROM `sb_EntityMapping` ) AND "
        "TIMESTAMPDIFF( DAY, LastAccessTime, UTC_TIMESTAMP() ) >= %d" % self.__assignedSBGraceDays,
        "! Assigned AND TIMESTAMPDIFF( DAY, LastAccessTime, UTC_TIMESTAMP() ) >= %s" %
        self.__unassignedSBGraceDays]
    sqlCmd = "SELECT SBId, SEName, SEPFN FROM `sb_SandBoxes` WHERE ( %s )" % " ) OR ( ".join(sqlCond)
    return self._query(sqlCmd)

  def deleteSandboxes(self, SBIdList):
    """
    Delete sandboxes
    """
    sqlSBList = ", ".join([str(sbid) for sbid in SBIdList])
    for table in ('sb_SandBoxes', 'sb_EntityMapping'):
      sqlCmd = "DELETE FROM `%s` WHERE SBId IN ( %s )" % (table, sqlSBList)
      result = self._update(sqlCmd)
      if not result['OK']:
        return result
    return S_OK()

  def getSandboxId(self, SEName, SEPFN, requesterName, requesterGroup, field='SBId', requesterDN=None):
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
    sqlCond = ["s.SEPFN=%s" % self._escapeString(SEPFN)['Value'],
               "s.SEName=%s" % self._escapeString(SEName)['Value'],
               's.OwnerId=o.OwnerId']
    sqlCmd = "SELECT s.%s FROM `sb_SandBoxes` s, `sb_Owners` o WHERE" % field
    requesterProps = Registry.getPropertiesForEntity(requesterGroup, name=requesterName, dn=requesterDN)
    if Properties.JOB_ADMINISTRATOR in requesterProps or Properties.JOB_MONITOR in requesterProps:
      # Do nothing, just ensure it doesn't fit in the other cases
      pass
    elif Properties.JOB_SHARING in requesterProps:
      sqlCond.append("o.OwnerGroup='%s'" % requesterGroup)
    elif Properties.NORMAL_USER in requesterProps:
      sqlCond.append("o.OwnerGroup='%s'" % requesterGroup)
      sqlCond.append("o.Owner='%s'" % requesterName)
    else:
      return S_ERROR("Not authorized to access sandbox")
    result = self._query("%s %s" % (sqlCmd, " AND ".join(sqlCond)))
    if not result['OK']:
      return result
    data = result['Value']
    if len(data) > 1:
      self.log.error("More than one sandbox registered with the same Id!", data)
    if not data:
      return S_ERROR("No sandbox matches the requirements")
    return S_OK(data[0][0])

  def getSandboxOwner(self, SEName, SEPFN, requesterDN, requesterGroup):
    """ get the sandbox owner, if such sandbox exists

        :param SEName: name of the StorageElement
        :param SEPFN: PFN of the Sandbox
        :param requestDN: host DN used as credentials
        :param requesterGroup: group used to use as credentials (should be 'hosts')

        :returns: S_OK with tuple (owner, ownerDN, ownerGroup)
    """
    res = self.getSandboxId(SEName, SEPFN, None, requesterGroup, 'OwnerId', requesterDN=requesterDN)
    if not res['OK']:
      return res

    sqlCmd = "SELECT `Owner`, `OwnerDN`, `OwnerGroup` FROM `sb_Owners` WHERE `OwnerId` = %d" % res['Value']
    res = self._query(sqlCmd)
    if not res['OK']:
      return res
    return S_OK(res['Value'][0])
