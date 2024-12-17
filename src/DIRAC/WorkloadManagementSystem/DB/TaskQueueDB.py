""" TaskQueueDB class is a front-end to the task queues db
"""
import random
import string
from collections import defaultdict
from typing import Any

from DIRAC import S_ERROR, S_OK, gConfig
from DIRAC.ConfigurationSystem.Client.Helpers import Registry
from DIRAC.ConfigurationSystem.Client.Helpers.Operations import Operations
from DIRAC.Core.Base.DB import DB
from DIRAC.Core.Security import Properties
from DIRAC.Core.Utilities import List
from DIRAC.Core.Utilities.DictCache import DictCache
from DIRAC.Core.Utilities.PrettyPrint import printDict
from DIRAC.FrameworkSystem.Client.Logger import contextLogger
from DIRAC.WorkloadManagementSystem.private.SharesCorrector import SharesCorrector

DEFAULT_GROUP_SHARE = 1000
TQ_MIN_SHARE = 0.001

# For checks at insertion time, and not only
singleValueDefFields = ("Owner", "OwnerGroup", "CPUTime")
multiValueDefFields = ("Sites", "GridCEs", "BannedSites", "Platforms", "JobTypes", "Tags")

# Used for matching
multiValueMatchFields = ("GridCE", "Site", "Platform", "JobType", "Tag")
bannedJobMatchFields = ("Site",)
mandatoryMatchFields = ("CPUTime",)
priorityIgnoredFields = ("Sites", "BannedSites")


def _lowerAndRemovePunctuation(s):
    table = str.maketrans("", "", string.punctuation)
    return s.lower().translate(table)


class TaskQueueDB(DB):
    """MySQL DB of "Task Queues" """

    def __init__(self, parentLogger=None):
        DB.__init__(self, "TaskQueueDB", "WorkloadManagement/TaskQueueDB", parentLogger=parentLogger)
        self._defaultLogger = self.log
        self.__maxJobsInTQ = 5000
        self.__defaultCPUSegments = [
            6 * 60,
            30 * 60,
            1 * 3600,
            6 * 3600,
            12 * 3600,
            1 * 86400,
            2 * 86400,
            3 * 86400,
            4 * 86400,
            6 * 86400,
            8 * 86400,
            10 * 86400,
            int(12.5 * 86400),
        ]
        self.__maxMatchRetry = 3
        self.__jobPriorityBoundaries = (0.001, 10)
        self.__groupShares = {}
        self.__deleteTQWithDelay = DictCache(self.__deleteTQIfEmpty)
        self.__opsHelper = Operations()
        self.__sharesCorrector = SharesCorrector(self.__opsHelper)
        result = self.__initializeDB()
        if not result["OK"]:
            raise Exception(f"Can't create tables: {result['Message']}")

    @property
    def log(self):
        return contextLogger.get() or self._defaultLogger

    @log.setter
    def log(self, value):
        self._defaultLogger = value

    def enableAllTaskQueues(self):
        """Enable all Task queues"""
        return self.updateFields("tq_TaskQueues", updateDict={"Enabled": "1"})

    def findOrphanJobs(self):
        """Find jobs that are not in any task queue"""
        result = self._query("select JobID from tq_Jobs WHERE TQId not in (SELECT TQId from tq_TaskQueues)")
        if not result["OK"]:
            return result
        return S_OK([row[0] for row in result["Value"]])

    def isSharesCorrectionEnabled(self):
        return self.__getCSOption("EnableSharesCorrection", False)

    def __getCSOption(self, optionName, defValue):
        return self.__opsHelper.getValue(f"JobScheduling/{optionName}", defValue)

    def __initializeDB(self):
        """
        Create the tables
        """
        result = self._query("show tables")
        if not result["OK"]:
            return result

        tablesInDB = [t[0] for t in result["Value"]]
        self.__tablesDesc = {}

        self.__tablesDesc["tq_TaskQueues"] = {
            "Fields": {
                "TQId": "INTEGER(11) UNSIGNED AUTO_INCREMENT NOT NULL",
                "Owner": "VARCHAR(255) NOT NULL",
                "OwnerGroup": "VARCHAR(32) NOT NULL",
                "VO": "VARCHAR(32) NOT NULL",
                "CPUTime": "BIGINT(20) UNSIGNED NOT NULL",
                "Priority": "FLOAT NOT NULL",
                "Enabled": "TINYINT(1) NOT NULL DEFAULT 0",
            },
            "PrimaryKey": "TQId",
            "Indexes": {"TQOwner": ["Owner", "OwnerGroup", "CPUTime"]},
        }

        self.__tablesDesc["tq_Jobs"] = {
            "Fields": {
                "TQId": "INTEGER(11) UNSIGNED NOT NULL",
                "JobId": "INTEGER(11) UNSIGNED NOT NULL",
                "Priority": "INTEGER UNSIGNED NOT NULL",
                "RealPriority": "FLOAT NOT NULL",
            },
            "PrimaryKey": "JobId",
            "Indexes": {"TaskIndex": ["TQId"]},
            "ForeignKeys": {"TQId": "tq_TaskQueues.TQId"},
        }

        for multiField in multiValueDefFields:
            tableName = f"tq_TQTo{multiField}"
            self.__tablesDesc[tableName] = {
                "Fields": {"TQId": "INTEGER(11) UNSIGNED NOT NULL", "Value": "VARCHAR(64) NOT NULL"},
                "PrimaryKey": ["TQId", "Value"],
                "Indexes": {"TaskIndex": ["TQId"], f"{multiField}Index": ["Value"]},
                "ForeignKeys": {"TQId": "tq_TaskQueues.TQId"},
            }

        tablesToCreate = {}
        for tableName, tableDef in self.__tablesDesc.items():
            if tableName not in tablesInDB:
                tablesToCreate[tableName] = tableDef

        return self._createTables(tablesToCreate)

    def getGroupsInTQs(self):
        cmdSQL = "SELECT DISTINCT( OwnerGroup ) FROM `tq_TaskQueues`"
        result = self._query(cmdSQL)
        if not result["OK"]:
            return result
        return S_OK([row[0] for row in result["Value"]])

    def fitCPUTimeToSegments(self, cpuTime):
        """
        Fit the CPU time to the valid segments
        """
        maxCPUSegments = self.__getCSOption("taskQueueCPUTimeIntervals", self.__defaultCPUSegments)
        try:
            maxCPUSegments = [int(seg) for seg in maxCPUSegments]
            # Check segments in the CS
            last = 0
            for cpuS in maxCPUSegments:
                if cpuS <= last:
                    maxCPUSegments = self.__defaultCPUSegments
                    break
                last = cpuS
        except Exception:
            maxCPUSegments = self.__defaultCPUSegments
        # Map to a segment
        for cpuSegment in maxCPUSegments:
            if cpuTime <= cpuSegment:
                return cpuSegment
        return maxCPUSegments[-1]

    def _checkTaskQueueDefinition(self, tqDefDict):
        """
        Check a task queue definition dict is valid
        """

        if "OwnerGroup" in tqDefDict:
            result = self._escapeString(Registry.getVOForGroup(tqDefDict["OwnerGroup"]))
            if not result["OK"]:
                return result
            tqDefDict["VO"] = result["Value"]

        for field in singleValueDefFields:
            if field == "CPUTime":
                if not isinstance(tqDefDict[field], int):
                    return S_ERROR(f"Mandatory field 'CPUTime' value type is not valid: {type(tqDefDict['CPUTime'])}")
            else:
                if not isinstance(tqDefDict[field], str):
                    return S_ERROR(f"Mandatory field {field} value type is not valid: {type(tqDefDict[field])}")
                result = self._escapeString(tqDefDict[field])
                if not result["OK"]:
                    return result
                tqDefDict[field] = result["Value"]
        for field in multiValueDefFields:
            if field not in tqDefDict:
                continue
            if not isinstance(tqDefDict[field], (list, tuple)):
                return S_ERROR(f"Multi value field {field} value type is not valid: {type(tqDefDict[field])}")
            result = self._escapeValues(tqDefDict[field])
            if not result["OK"]:
                return result
            tqDefDict[field] = result["Value"]

        return S_OK(tqDefDict)

    def _checkMatchDefinition(self, tqMatchDict):
        """
        Check a task queue match dict is valid
        """

        def travelAndCheckType(value, validTypes, escapeValues=True):
            if isinstance(value, (list, tuple)):
                for subValue in value:
                    if not isinstance(subValue, validTypes):
                        return S_ERROR(f"List contained type {type(subValue)} is not valid -> {validTypes}")
                if escapeValues:
                    return self._escapeValues(value)
                return S_OK(value)
            else:
                if not isinstance(value, validTypes):
                    return S_ERROR(f"Type {type(value)} is not valid -> {validTypes}")
                if escapeValues:
                    return self._escapeString(value)
                return S_OK(value)

        for field in singleValueDefFields:
            if field not in tqMatchDict:
                if field in mandatoryMatchFields:
                    return S_ERROR(f"Missing mandatory field '{field}' in match request definition")
                continue
            fieldValue = tqMatchDict[field]
            if field in ["CPUTime"]:
                result = travelAndCheckType(fieldValue, int, escapeValues=False)
            else:
                result = travelAndCheckType(fieldValue, str)
            if not result["OK"]:
                return S_ERROR(f"Match definition field {field} failed : {result['Message']}")
            tqMatchDict[field] = result["Value"]
        # Check multivalue
        for multiField in multiValueMatchFields:
            for field in (multiField, f"Banned{multiField}", f"Required{multiField}"):
                if field in tqMatchDict:
                    fieldValue = tqMatchDict[field]
                    result = travelAndCheckType(fieldValue, str)
                    if not result["OK"]:
                        return S_ERROR(f"Match definition field {field} failed : {result['Message']}")
                    tqMatchDict[field] = result["Value"]

        return S_OK(tqMatchDict)

    def __createTaskQueue(self, tqDefDict, priority=1, connObj=False):
        """
        Create a task queue
          :returns: S_OK( tqId ) / S_ERROR
        """
        if not connObj:
            result = self._getConnection()
            if not result["OK"]:
                return S_ERROR(f"Can't create task queue: {result['Message']}")
            connObj = result["Value"]
        tqDefDict["CPUTime"] = self.fitCPUTimeToSegments(tqDefDict["CPUTime"])
        sqlSingleFields = ["TQId", "Priority"]
        sqlValues = ["0", str(priority)]
        for field in singleValueDefFields:
            sqlSingleFields.append(field)
            sqlValues.append(tqDefDict[field])
        sqlSingleFields.append("VO")
        sqlValues.append(tqDefDict["VO"])
        # Insert the TQ Disabled
        sqlSingleFields.append("Enabled")
        sqlValues.append("0")
        cmd = "INSERT INTO tq_TaskQueues ( {} ) VALUES ( {} )".format(
            ", ".join(sqlSingleFields),
            ", ".join([str(v) for v in sqlValues]),
        )
        result = self._update(cmd, conn=connObj)
        if not result["OK"]:
            self.log.error("Can't insert TQ in DB", result["Message"])
            return result
        if "lastRowId" in result:
            tqId = result["lastRowId"]
        else:
            result = self._query("SELECT LAST_INSERT_ID()", conn=connObj)
            if not result["OK"]:
                self.cleanOrphanedTaskQueues(connObj=connObj)
                return S_ERROR("Can't determine task queue id after insertion")
            tqId = result["Value"][0][0]
        for field in multiValueDefFields:
            if field not in tqDefDict:
                continue
            values = List.uniqueElements([value for value in tqDefDict[field] if value.strip()])
            if not values:
                continue
            cmd = f"INSERT INTO `tq_TQTo{field}` ( TQId, Value ) VALUES "
            cmd += ", ".join([f"( {tqId}, {str(value)} )" for value in values])
            result = self._update(cmd, conn=connObj)
            if not result["OK"]:
                self.log.error("Failed to insert condition", f"{field} : {result['Message']}")
                self.cleanOrphanedTaskQueues(connObj=connObj)
                return S_ERROR(f"Can't insert values {values} for field {field}: {result['Message']}")
        self.log.info("Created TQ", tqId)
        return S_OK(tqId)

    def cleanOrphanedTaskQueues(self, connObj=False):
        """
        Delete all empty task queues
        """
        self.log.info("Cleaning orphaned TQs")
        sq = "SELECT TQId FROM `tq_TaskQueues` WHERE Enabled >= 1 AND TQId not in ( SELECT DISTINCT TQId from `tq_Jobs` )"
        result = self._query(sq, conn=connObj)
        if not result["OK"]:
            return result
        orphanedTQs = result["Value"]
        if not orphanedTQs:
            return S_OK()
        orphanedTQs = [str(otq[0]) for otq in orphanedTQs]

        for mvField in multiValueDefFields:
            result = self._update(
                f"DELETE FROM `tq_TQTo{mvField}` WHERE TQId in ( {','.join(orphanedTQs)} )", conn=connObj
            )
            if not result["OK"]:
                return result

        result = self._update(f"DELETE FROM `tq_TaskQueues` WHERE TQId in ( {','.join(orphanedTQs)} )", conn=connObj)
        if not result["OK"]:
            return result
        return S_OK()

    def __setTaskQueueEnabled(self, tqId, enabled=True, connObj=False):
        if enabled:
            enabled = "+ 1"
        else:
            enabled = "- 1"
        upSQL = "UPDATE `tq_TaskQueues` SET Enabled = Enabled %s WHERE TQId=%d" % (enabled, tqId)
        result = self._update(upSQL, conn=connObj)
        if not result["OK"]:
            self.log.error("Error setting TQ state", f"TQ {tqId} State {enabled}: {result['Message']}")
            return result
        updated = result["Value"] > 0
        if updated:
            self.log.verbose("Set enabled for TQ", f"({enabled} for TQ {tqId})")
        return S_OK(updated)

    def __hackJobPriority(self, jobPriority):
        jobPriority = min(max(int(jobPriority), self.__jobPriorityBoundaries[0]), self.__jobPriorityBoundaries[1])
        if jobPriority == self.__jobPriorityBoundaries[0]:
            return 10 ** (-5)
        if jobPriority == self.__jobPriorityBoundaries[1]:
            return 10**6
        return jobPriority

    def insertJob(self, jobId, tqDefDict, jobPriority, skipTQDefCheck=False):
        """Insert a job in a task queue (creating one if it doesn't exit)

        :param int jobId: job ID
        :param dict tqDefDict: dict for TQ definition
        :param int jobPriority: integer that defines the job priority

        :returns: S_OK() / S_ERROR
        """
        try:
            int(jobId)
        except ValueError:
            return S_ERROR("JobId is not a number!")
        retVal = self._getConnection()
        if not retVal["OK"]:
            return S_ERROR(f"Can't insert job: {retVal['Message']}")
        connObj = retVal["Value"]
        if not skipTQDefCheck:
            tqDefDict = dict(tqDefDict)
            retVal = self._checkTaskQueueDefinition(tqDefDict)
            if not retVal["OK"]:
                self.log.error("TQ definition check failed", retVal["Message"])
                return retVal
            tqDefDict = retVal["Value"]
        tqDefDict["CPUTime"] = self.fitCPUTimeToSegments(tqDefDict["CPUTime"])
        self.log.info("Inserting job with requirements", f"({jobId} : {printDict(tqDefDict)})")
        retVal = self.__findAndDisableTaskQueue(tqDefDict, connObj=connObj)
        if not retVal["OK"]:
            return retVal
        tqInfo = retVal["Value"]
        newTQ = False
        if not tqInfo["found"]:
            self.log.info("Creating a TQ for job", jobId)
            retVal = self.__createTaskQueue(tqDefDict, 1, connObj=connObj)
            if not retVal["OK"]:
                return retVal
            tqId = retVal["Value"]
            newTQ = True
        else:
            tqId = tqInfo["tqId"]
            self.log.info("Found TQ for job requirements", f"({tqId} : {jobId})")
        try:
            result = self.__insertJobInTaskQueue(jobId, tqId, int(jobPriority), checkTQExists=False, connObj=connObj)
            if not result["OK"]:
                self.log.error("Error inserting job in TQ", f"Job {jobId} TQ {tqId}: {result['Message']}")
                return result
            if newTQ:
                self.recalculateTQSharesForEntity(tqDefDict["Owner"], tqDefDict["OwnerGroup"], connObj=connObj)
        finally:
            self.__setTaskQueueEnabled(tqId, True)
        return S_OK()

    def __insertJobInTaskQueue(self, jobId, tqId, jobPriority, checkTQExists=True, connObj=False):
        """Insert a job in a given task queue

        :param int jobId: job ID
        :param dict tqDefDict: dict for TQ definition
        :param int jobPriority: integer that defines the job priority

        :returns: S_OK() / S_ERROR
        """
        self.log.info("Inserting job in TQ with priority", f"({jobId} : {tqId} : {jobPriority})")
        if not connObj:
            result = self._getConnection()
            if not result["OK"]:
                return S_ERROR(f"Can't insert job: {result['Message']}")
            connObj = result["Value"]
        if checkTQExists:
            result = self._query(f"SELECT tqId FROM `tq_TaskQueues` WHERE TQId = {tqId}", conn=connObj)
            if not result["OK"] or not result["Value"]:
                return S_OK(f"Can't find task queue with id {tqId}: {result['Message']}")
        hackedPriority = self.__hackJobPriority(jobPriority)
        result = self._update(
            "INSERT INTO tq_Jobs ( TQId, JobId, Priority, RealPriority ) \
                            VALUES ( %s, %s, %s, %f ) ON DUPLICATE KEY UPDATE TQId = %s, \
                            Priority = %s, RealPriority = %f"
            % (tqId, jobId, jobPriority, hackedPriority, tqId, jobPriority, hackedPriority),
            conn=connObj,
        )
        if not result["OK"]:
            return result
        return S_OK()

    def __generateTQFindSQL(
        self,
        tqDefDict,
    ):
        """
        Generate the SQL to find a task queue that has exactly the given requirements

        :param dict tqDefDict: dict for TQ definition
        :returns: S_OK() / S_ERROR
        """
        sqlCondList = []
        for field in singleValueDefFields:
            sqlCondList.append(f"`tq_TaskQueues`.{field} = {tqDefDict[field]}")
        # MAGIC SUBQUERIES TO ENSURE STRICT MATCH
        for field in multiValueDefFields:
            tableName = f"`tq_TQTo{field}`"
            if field in tqDefDict and tqDefDict[field]:
                firstQuery = (
                    "SELECT COUNT(%s.Value) \
                      FROM %s \
                      WHERE %s.TQId = `tq_TaskQueues`.TQId"
                    % (tableName, tableName, tableName)
                )
                grouping = f"GROUP BY {tableName}.TQId"
                valuesList = List.uniqueElements([value.strip() for value in tqDefDict[field] if value.strip()])
                numValues = len(valuesList)
                secondQuery = "{} AND {}.Value in ({})".format(
                    firstQuery,
                    tableName,
                    ",".join(["%s" % str(value) for value in valuesList]),
                )
                sqlCondList.append(f"{numValues} = ({firstQuery} {grouping})")
                sqlCondList.append(f"{numValues} = ({secondQuery} {grouping})")
            else:
                sqlCondList.append(f"`tq_TaskQueues`.TQId not in ( SELECT DISTINCT {tableName}.TQId from {tableName} )")
        # END MAGIC: That was easy ;)
        return S_OK(" AND ".join(sqlCondList))

    def __findAndDisableTaskQueue(self, tqDefDict, retries=10, connObj=False):
        """Disable and find TQ

        :param dict tqDefDict: dict for TQ definition
        :returns: S_OK() / S_ERROR
        """
        for _ in range(retries):
            result = self.__findSmallestTaskQueue(tqDefDict, connObj=connObj)
            if not result["OK"]:
                return result
            data = result["Value"]
            if not data["found"]:
                return result
            if data["enabled"] < 1:
                self.log.debug("TaskQueue {tqId} seems to be already disabled ({enabled})".format(**data))
            result = self.__setTaskQueueEnabled(data["tqId"], False)
            if result["OK"]:
                return S_OK(data)
        return S_ERROR("Could not disable TQ")

    def __findSmallestTaskQueue(self, tqDefDict, connObj=False):
        """
        Find a task queue that has at least the given requirements

        :param dict tqDefDict: dict for TQ definition
        :returns: S_OK() / S_ERROR
        """
        result = self.__generateTQFindSQL(tqDefDict)
        if not result["OK"]:
            return result

        sqlCmd = "SELECT COUNT( `tq_Jobs`.JobID ), `tq_TaskQueues`.TQId, `tq_TaskQueues`.Enabled \
FROM `tq_TaskQueues`, `tq_Jobs`"
        sqlCmd = (
            "%s WHERE `tq_TaskQueues`.TQId = `tq_Jobs`.TQId AND %s GROUP BY `tq_Jobs`.TQId \
ORDER BY COUNT( `tq_Jobs`.JobID ) ASC"
            % (sqlCmd, result["Value"])
        )
        result = self._query(sqlCmd, conn=connObj)
        if not result["OK"]:
            self.log.error("Can't find task queue", result["Message"])
            return result
        data = result["Value"]
        if not data or data[0][0] >= self.__maxJobsInTQ:
            return S_OK({"found": False})
        return S_OK({"found": True, "tqId": data[0][1], "enabled": data[0][2], "jobs": data[0][0]})

    def matchAndGetJob(self, tqMatchDict, numJobsPerTry=50, numQueuesPerTry=10, negativeCond=None):
        """Match a job based on requirements

        :param dict tqDefDict: dict for TQ definition
        :returns: S_OK() / S_ERROR
        """
        if negativeCond is None:
            negativeCond = {}
        # Make a copy to avoid modification of original if escaping needs to be done
        tqMatchDict = dict(tqMatchDict)
        retVal = self._checkMatchDefinition(tqMatchDict)
        if not retVal["OK"]:
            self.log.error("TQ match request check failed", retVal["Message"])
            return retVal
        retVal = self._getConnection()
        if not retVal["OK"]:
            return S_ERROR(f"Can't connect to DB: {retVal['Message']}")
        connObj = retVal["Value"]
        preJobSQL = "SELECT `tq_Jobs`.JobId, `tq_Jobs`.TQId \
FROM `tq_Jobs` WHERE `tq_Jobs`.TQId = %s AND `tq_Jobs`.Priority = %s"
        prioSQL = "SELECT `tq_Jobs`.Priority FROM `tq_Jobs` \
WHERE `tq_Jobs`.TQId = %s ORDER BY RAND() / `tq_Jobs`.RealPriority ASC LIMIT 1"
        postJobSQL = f" ORDER BY `tq_Jobs`.JobId ASC LIMIT {numJobsPerTry}"
        for _ in range(self.__maxMatchRetry):
            noJobsFound = False
            if "JobID" in tqMatchDict:
                # A certain JobID is required by the resource, so all TQ are to be considered
                retVal = self.matchAndGetTaskQueue(
                    tqMatchDict, numQueuesToGet=0, skipMatchDictDef=True, connObj=connObj
                )
                preJobSQL = f"{preJobSQL} AND `tq_Jobs`.JobId = {tqMatchDict['JobID']} "
            else:
                retVal = self.matchAndGetTaskQueue(
                    tqMatchDict,
                    numQueuesToGet=numQueuesPerTry,
                    skipMatchDictDef=True,
                    negativeCond=negativeCond,
                    connObj=connObj,
                )
            if not retVal["OK"]:
                return retVal
            tqList = retVal["Value"]
            if not tqList:
                self.log.info("No TQ matches requirements")
                return S_OK({"matchFound": False, "tqMatch": tqMatchDict})
            for tqId, tqOwner, tqOwnerGroup in tqList:
                self.log.verbose("Trying to extract jobs from TQ", tqId)
                retVal = self._query(prioSQL % tqId, conn=connObj)
                if not retVal["OK"]:
                    return S_ERROR(f"Can't retrieve winning priority for matching job: {retVal['Message']}")
                if not retVal["Value"]:
                    noJobsFound = True
                    continue
                prio = retVal["Value"][0][0]
                retVal = self._query(f"{preJobSQL % (tqId, prio)} {postJobSQL}", conn=connObj)
                if not retVal["OK"]:
                    return S_ERROR(f"Can't begin transaction for matching job: {retVal['Message']}")
                jobTQList = [(row[0], row[1]) for row in retVal["Value"]]
                if not jobTQList:
                    self.log.info("Task queue seems to be empty, triggering a cleaning of", tqId)
                    self.__deleteTQWithDelay.add(tqId, 300, (tqId, tqOwner, tqOwnerGroup))
                while jobTQList:
                    jobId, tqId = jobTQList.pop(random.randint(0, len(jobTQList) - 1))
                    self.log.verbose("Trying to extract job from TQ", f"{jobId} : {tqId}")
                    retVal = self.deleteJob(jobId, connObj=connObj)
                    if not retVal["OK"]:
                        msgFix = "Could not take job"
                        msgVar = f" {jobId} out from the TQ {tqId}: {retVal['Message']}"
                        self.log.error(msgFix, msgVar)
                        return S_ERROR(msgFix + msgVar)
                    if retVal["Value"]:
                        self.log.info("Extracted job with prio from TQ", f"({jobId} : {prio} : {tqId})")
                        return S_OK({"matchFound": True, "jobId": jobId, "taskQueueId": tqId, "tqMatch": tqMatchDict})
                self.log.info("No jobs could be extracted from TQ", tqId)
        if noJobsFound:
            return S_OK({"matchFound": False, "tqMatch": tqMatchDict})

        self.log.info(f"Could not find a match after {self.__maxMatchRetry} match retries")
        return S_ERROR(f"Could not find a match after {self.__maxMatchRetry} match retries")

    def matchAndGetTaskQueue(
        self, tqMatchDict, numQueuesToGet=1, skipMatchDictDef=False, negativeCond=None, connObj=False
    ):
        """Get a queue that matches the requirements"""
        if negativeCond is None:
            negativeCond = {}
        # Make a copy to avoid modification of original if escaping needs to be done
        tqMatchDict = dict(tqMatchDict)
        if not skipMatchDictDef:
            retVal = self._checkMatchDefinition(tqMatchDict)
            if not retVal["OK"]:
                return retVal
        retVal = self.__generateTQMatchSQL(tqMatchDict, numQueuesToGet=numQueuesToGet, negativeCond=negativeCond)
        if not retVal["OK"]:
            return retVal
        matchSQL = retVal["Value"]
        retVal = self._query(matchSQL, conn=connObj)
        if not retVal["OK"]:
            return retVal
        return S_OK([(row[0], row[1], row[2]) for row in retVal["Value"]])

    @staticmethod
    def __generateSQLSubCond(sqlString, value, boolOp="OR"):
        if not isinstance(value, (list, tuple)):
            return sqlString % str(value).strip()
        sqlORList = []
        for v in value:
            sqlORList.append(sqlString % str(v).strip())
        return "( %s )" % (" %s " % boolOp).join(sqlORList)

    def __generateNotSQL(self, negativeCond):
        """Generate negative conditions
        Can be a list of dicts or a dict:
         - list of dicts will be  OR of conditional dicts
         - dicts will be normal conditional dict ( kay1 in ( v1, v2, ... ) AND key2 in ( v3, v4, ... ) )
        """
        if isinstance(negativeCond, (list, tuple)):
            sqlCond = []
            for cD in negativeCond:
                sqlCond.append(self.__generateNotDictSQL(cD))
            return f" ( {' OR  '.join(sqlCond)} )"
        elif isinstance(negativeCond, dict):
            return self.__generateNotDictSQL(negativeCond)
        raise RuntimeError(f"negativeCond has to be either a list or a dict or a tuple, and it's {type(negativeCond)}")

    def __generateNotDictSQL(self, negativeCond):
        """Generate the negative sql condition from a standard condition dict
        not ( cond1 and cond2 ) = ( not cond1 or not cond 2 )
        For instance: { 'Site': 'S1', 'JobType': [ 'T1', 'T2' ] }
          ( not 'S1' in Sites or ( not 'T1' in JobType and not 'T2' in JobType ) )
          S2 T1 -> not False or ( not True and not False ) -> True or ... -> True -> Eligible
          S1 T3 -> not True or ( not False and not False ) -> False or (True and True ) -> True -> Eligible
          S1 T1 -> not True or ( not True and not False ) -> False or ( False and True ) -> False -> Nop
        """
        condList = []
        for field in negativeCond:
            if field in multiValueMatchFields:
                fullTableN = f"`tq_TQTo{field}s`"
                valList = negativeCond[field]
                if not isinstance(valList, (list, tuple)):
                    valList = (valList,)
                subList = []
                for value in valList:
                    value = self._escapeString(value)["Value"]
                    sql = f"{value} NOT IN ( SELECT {fullTableN}.Value FROM {fullTableN} WHERE {fullTableN}.TQId = tq.TQId )"
                    subList.append(sql)
                condList.append(f"( {' AND '.join(subList)} )")
            elif field in singleValueDefFields:
                for value in negativeCond[field]:
                    value = self._escapeString(value)["Value"]
                    sql = f"{value} != tq.{field} "
                    condList.append(sql)
        return f"( {' OR '.join(condList)} )"

    @staticmethod
    def __generateTablesName(sqlTables, field):
        fullTableName = f"tq_TQTo{field}s"
        if fullTableName not in sqlTables:
            tableN = field.lower()
            sqlTables[fullTableName] = tableN
            return (
                tableN,
                f"`{fullTableName}`",
            )
        return sqlTables[fullTableName], f"`{fullTableName}`"

    def __generateTQMatchSQL(self, tqMatchDict, numQueuesToGet=1, negativeCond=None):
        """
        Generate the SQL needed to match a task queue
        """
        self.log.debug(tqMatchDict)

        if negativeCond is None:
            negativeCond = {}
        # Only enabled TQs
        sqlCondList = []
        sqlTables = {"tq_TaskQueues": "tq"}
        # If Owner and OwnerGroup are defined only use those combinations that make sense
        if "Owner" in tqMatchDict and "OwnerGroup" in tqMatchDict:
            groups = tqMatchDict["OwnerGroup"]
            if not isinstance(groups, (list, tuple)):
                groups = [groups]
            owner = tqMatchDict["Owner"]
            ownerConds = []
            for group in groups:
                if Properties.JOB_SHARING in Registry.getPropertiesForGroup(group.replace('"', "")):
                    ownerConds.append(f"tq.OwnerGroup = {group}")
                else:
                    ownerConds.append(f"( tq.Owner = {owner} AND tq.OwnerGroup = {group} )")
            sqlCondList.append(" OR ".join(ownerConds))
        else:
            # If not both are defined, just add the ones that are defined
            for field in ("OwnerGroup", "Owner"):
                if field in tqMatchDict:
                    sqlCondList.append(self.__generateSQLSubCond("tq.%s = %%s" % field, tqMatchDict[field]))
        # Type of single value conditions
        if "CPUTime" in tqMatchDict:
            sqlCondList.append(self.__generateSQLSubCond("tq.%s <= %%s" % "CPUTime", tqMatchDict["CPUTime"]))

        tag_fv = []

        # Match multi value fields
        for field in multiValueMatchFields:
            self.log.debug(f"Evaluating field {field}")
            # It has to be %ss , with an 's' at the end because the columns names
            # are plural and match options are singular

            # Just treating the (not so) special case of no Tag, No RequiredTag
            if "Tag" not in tqMatchDict and "RequiredTag" not in tqMatchDict:
                tqMatchDict["Tag"] = []

            if field in tqMatchDict:
                self.log.debug(f"Evaluating {field} with value {tqMatchDict[field]}")

                _, fullTableN = self.__generateTablesName(sqlTables, field)

                sqlMultiCondList = []
                csql = None

                # Now evaluating Tags
                if field == "Tag":
                    tag_fv = tqMatchDict.get("Tag")
                    self.log.debug(f"Evaluating tag {tag_fv} of type {type(tag_fv)}")
                    if isinstance(tag_fv, str):
                        tag_fv = [tag_fv]

                    # Is there something to consider?
                    if any(_lowerAndRemovePunctuation(fvx) == "any" for fvx in tag_fv):
                        continue
                    else:
                        sqlMultiCondList.append(self.__generateTagSQLSubCond(fullTableN, tag_fv))

                # Now evaluating everything that is not tags
                else:
                    fv = tqMatchDict.get(field)
                    self.log.debug(f"Evaluating field {field} of type {type(fv)}")

                    # Is there something to consider?
                    if not fv:
                        continue
                    if isinstance(fv, str) and _lowerAndRemovePunctuation(fv) == "any":
                        continue
                    if isinstance(fv, list) and any(_lowerAndRemovePunctuation(fvx) == "any" for fvx in fv):
                        continue
                    # if field != 'GridCE' or 'Site' in tqMatchDict:
                    # Jobs for masked sites can be matched if they specified a GridCE
                    # Site is removed from tqMatchDict if the Site is mask. In this case we want
                    # that the GridCE matches explicitly so the COUNT can not be 0. In this case we skip this
                    # condition
                    sqlMultiCondList.append(
                        "( SELECT COUNT(%s.Value) FROM %s WHERE %s.TQId = tq.TQId ) = 0"
                        % (fullTableN, fullTableN, fullTableN)
                    )
                    sqlMultiCondList.append(
                        self.__generateSQLSubCond(
                            "%%s IN ( SELECT %s.Value \
                                                            FROM %s \
                                                            WHERE %s.TQId = tq.TQId )"
                            % (fullTableN, fullTableN, fullTableN),
                            tqMatchDict.get(field),
                        )
                    )

                sqlCondList.append(f"( {' OR '.join(sqlMultiCondList)} )")

                # In case of Site, check it's not in job banned sites
                if field in bannedJobMatchFields:
                    fullTableN = f"`tq_TQToBanned{field}s`"
                    csql = self.__generateSQLSubCond(
                        "%%s not in ( SELECT %s.Value \
                                                          FROM %s \
                                                          WHERE %s.TQId = tq.TQId )"
                        % (fullTableN, fullTableN, fullTableN),
                        tqMatchDict[field],
                        boolOp="OR",
                    )
                    sqlCondList.append(csql)

        # Add possibly RequiredTag conditions
        rtag_fv = tqMatchDict.get("RequiredTag", [])
        if isinstance(rtag_fv, str):
            rtag_fv = [rtag_fv]

        # Is there something to consider?
        if not rtag_fv or any(_lowerAndRemovePunctuation(fv) == "any" for fv in rtag_fv):
            pass
        elif not set(rtag_fv).issubset(set(tag_fv)):
            return S_ERROR("Wrong conditions")
        else:
            self.log.debug(f"Evaluating RequiredTag {rtag_fv}")
            sqlCondList.append(self.__generateRequiredTagSQLSubCond("`tq_TQToTags`", rtag_fv))

        # Add possibly Resource banning conditions
        for field in multiValueMatchFields:
            bannedField = f"Banned{field}"

            # Is there something to consider?
            b_fv = tqMatchDict.get(bannedField)
            if (
                not b_fv
                or isinstance(b_fv, str)
                and _lowerAndRemovePunctuation(b_fv) == "any"
                or isinstance(b_fv, list)
                and any(_lowerAndRemovePunctuation(fvx) == "any" for fvx in b_fv)
            ):
                continue

            fullTableN = f"`tq_TQTo{field}s`"

            sqlCondList.append(
                self.__generateSQLSubCond(
                    f"%%s not in ( SELECT {fullTableN}.Value FROM {fullTableN} WHERE {fullTableN}.TQId = tq.TQId )",
                    b_fv,
                    boolOp="OR",
                )
            )

        # Add extra negative conditions
        if negativeCond:
            sqlCondList.append(self.__generateNotSQL(negativeCond))

        # Generate the final query string
        tqSqlCmd = "SELECT tq.TQId, tq.Owner, tq.OwnerGroup FROM `tq_TaskQueues` tq WHERE %s" % (
            " AND ".join(sqlCondList)
        )

        # Apply priorities
        tqSqlCmd = f"{tqSqlCmd} ORDER BY RAND() / tq.Priority ASC"

        # Do we want a limit?
        if numQueuesToGet:
            tqSqlCmd = f"{tqSqlCmd} LIMIT {numQueuesToGet}"
        return S_OK(tqSqlCmd)

    @staticmethod
    def __generateTagSQLSubCond(tableName, tagMatchList):
        """Generate SQL condition where ALL the specified multiValue requirements must be
        present in the matching resource list
        """
        sql1 = f"SELECT COUNT({tableName}.Value) FROM {tableName} WHERE {tableName}.TQId=tq.TQId"
        if not tagMatchList:
            sql2 = sql1 + f" AND {tableName}.Value=''"
        else:
            if isinstance(tagMatchList, (list, tuple)):
                sql2 = sql1 + f" AND {tableName}.Value in ( {','.join([('%s' % v) for v in tagMatchList])} )"
            else:
                sql2 = sql1 + f" AND {tableName}.Value={tagMatchList}"
        sql = "( " + sql1 + " ) = (" + sql2 + " )"
        return sql

    @staticmethod
    def __generateRequiredTagSQLSubCond(tableName, tagMatchList):
        """Generate SQL condition where the TQ corresponds to the requirements
        of the resource
        """
        sql = f"SELECT COUNT({tableName}.Value) FROM {tableName} WHERE {tableName}.TQId=tq.TQId"
        if isinstance(tagMatchList, (list, tuple)):
            sql = sql + f" AND {tableName}.Value in ( {','.join([('%s' % v) for v in tagMatchList])} )"
            nTags = len(tagMatchList)
        else:
            sql = sql + f" AND {tableName}.Value={tagMatchList}"
            nTags = 1
        sql = f"( {sql} ) = {nTags}"
        return sql

    def deleteJob(self, jobId, connObj=False):
        """
        Delete a job from the task queues
        Return S_OK( True/False ) / S_ERROR
        """
        if not connObj:
            retVal = self._getConnection()
            if not retVal["OK"]:
                return S_ERROR(f"Can't delete job: {retVal['Message']}")
            connObj = retVal["Value"]
        retVal = self._query(
            "SELECT t.TQId, t.Owner, t.OwnerGroup \
FROM `tq_TaskQueues` t, `tq_Jobs` j \
WHERE j.JobId = %s AND t.TQId = j.TQId"
            % jobId,
            conn=connObj,
        )
        if not retVal["OK"]:
            return S_ERROR(f"Could not get job from task queue {jobId}: {retVal['Message']}")
        data = retVal["Value"]
        if not data:
            return S_OK(False)
        tqId, tqOwner, tqOwnerGroup = data[0]
        self.log.verbose("Deleting job", jobId)
        retVal = self._update(f"DELETE FROM `tq_Jobs` WHERE JobId = {jobId}", conn=connObj)
        if not retVal["OK"]:
            return S_ERROR(f"Could not delete job from task queue {jobId}: {retVal['Message']}")
        if retVal["Value"] == 0:
            # No job deleted
            return S_OK(False)
        # Always return S_OK() because job has already been taken out from the TQ
        self.__deleteTQWithDelay.add(tqId, 300, (tqId, tqOwner, tqOwnerGroup))
        return S_OK(True)

    def getTaskQueueForJob(self, jobId, connObj=False):
        """
        Return TaskQueue for a given Job
        Return S_OK( [TaskQueueID] ) / S_ERROR
        """
        if not connObj:
            retVal = self._getConnection()
            if not retVal["OK"]:
                return S_ERROR(f"Can't get TQ for job: {retVal['Message']}")
            connObj = retVal["Value"]

        retVal = self._query(f"SELECT TQId FROM `tq_Jobs` WHERE JobId = {jobId}", conn=connObj)

        if not retVal["OK"]:
            return retVal

        if not retVal["Value"]:
            return S_ERROR("Not in TaskQueues")

        return S_OK(retVal["Value"][0][0])

    def __getOwnerForTaskQueue(self, tqId, connObj=False):
        retVal = self._query(f"SELECT Owner, OwnerGroup from `tq_TaskQueues` WHERE TQId={tqId}", conn=connObj)
        if not retVal["OK"]:
            return retVal
        data = retVal["Value"]
        if not data:
            return S_OK(False)
        return S_OK(retVal["Value"][0])

    def __deleteTQIfEmpty(self, args):
        (tqId, tqOwner, tqOwnerGroup) = args
        retries = 3
        while retries:
            retries -= 1
            result = self.deleteTaskQueueIfEmpty(tqId, tqOwner, tqOwnerGroup)
            if result["OK"]:
                return
        self.log.error("Could not delete TQ", f"{tqId}: {result['Message']}")

    def deleteTaskQueueIfEmpty(self, tqId, tqOwner=False, tqOwnerGroup=False, connObj=False):
        """
        Try to delete a task queue if its empty
        """
        if not connObj:
            retVal = self._getConnection()
            if not retVal["OK"]:
                self.log.error("Can't insert job", retVal["Message"])
                return retVal
            connObj = retVal["Value"]
        if not tqOwner or not tqOwnerGroup:
            retVal = self.__getOwnerForTaskQueue(tqId, connObj=connObj)
            if not retVal["OK"]:
                return retVal
            data = retVal["Value"]
            if not data:
                return S_OK(False)
            tqOwner, tqOwnerGroup = data

        sqlCmd = f"SELECT TQId FROM `tq_TaskQueues` WHERE Enabled >= 1 AND `tq_TaskQueues`.TQId = {tqId} "
        sqlCmd += "AND `tq_TaskQueues`.TQId not in ( SELECT DISTINCT TQId from `tq_Jobs` )"
        retVal = self._query(sqlCmd, conn=connObj)
        if not retVal["OK"]:
            self.log.error("Could not select task queue", "%s : %s" % tqId, retVal["Message"])
            return retVal
        tqToDel = retVal["Value"]

        if tqToDel:
            for mvField in multiValueDefFields:
                retVal = self._update(f"DELETE FROM `tq_TQTo{mvField}` WHERE TQId = {tqId}", conn=connObj)
                if not retVal["OK"]:
                    return retVal
            retVal = self._update(f"DELETE FROM `tq_TaskQueues` WHERE TQId = {tqId}", conn=connObj)
            if not retVal["OK"]:
                return retVal
            self.recalculateTQSharesForEntity(tqOwner, tqOwnerGroup, connObj=connObj)
            self.log.info("Deleted empty and enabled TQ", tqId)
            return S_OK()
        return S_OK(False)

    def getMatchingTaskQueues(self, tqMatchDict, negativeCond=False):
        """Get the info of the task queues that match a resource"""
        result = self.matchAndGetTaskQueue(tqMatchDict, numQueuesToGet=0, negativeCond=negativeCond)
        if not result["OK"]:
            return result
        return self.retrieveTaskQueues([tqTuple[0] for tqTuple in result["Value"]])

    def retrieveTaskQueues(self, tqIdList=None):
        """
        Get all the task queues
        """
        sqlSelectEntries = ["`tq_TaskQueues`.TQId", "`tq_TaskQueues`.Priority", "COUNT( `tq_Jobs`.TQId )"]
        sqlGroupEntries = ["`tq_TaskQueues`.TQId", "`tq_TaskQueues`.Priority"]
        for field in singleValueDefFields:
            sqlSelectEntries.append(f"`tq_TaskQueues`.{field}")
            sqlGroupEntries.append(f"`tq_TaskQueues`.{field}")
        sqlCmd = f"SELECT {', '.join(sqlSelectEntries)} FROM `tq_TaskQueues`, `tq_Jobs`"
        sqlTQCond = ""
        if tqIdList is not None:
            if not tqIdList:
                # Empty list => Fast-track no matches
                return S_OK({})
            else:
                sqlTQCond += f" AND `tq_TaskQueues`.TQId in ( {', '.join([str(id_) for id_ in tqIdList])} )"
        sqlCmd = "{} WHERE `tq_TaskQueues`.TQId = `tq_Jobs`.TQId {} GROUP BY {}".format(
            sqlCmd,
            sqlTQCond,
            ", ".join(sqlGroupEntries),
        )
        retVal = self._query(sqlCmd)
        if not retVal["OK"]:
            self.log.error("Can't retrieve task queues info", retVal["Message"])
            return retVal
        tqData = {}
        for record in retVal["Value"]:
            tqId = record[0]
            tqData[tqId] = {"Priority": record[1], "Jobs": record[2]}
            record = record[3:]
            for iP, _ in enumerate(singleValueDefFields):
                tqData[tqId][singleValueDefFields[iP]] = record[iP]

        tqNeedCleaning = False
        for field in multiValueDefFields:
            table = f"`tq_TQTo{field}`"
            sqlCmd = f"SELECT {table}.TQId, {table}.Value FROM {table}"
            retVal = self._query(sqlCmd)
            if not retVal["OK"]:
                self.log.error("Can't retrieve task queues field", f"{field} info: {retVal['Message']}")
                return retVal
            for record in retVal["Value"]:
                tqId = record[0]
                value = record[1]
                if tqId not in tqData:
                    if tqIdList is None or tqId in tqIdList:
                        self.log.verbose(
                            "Task Queue is defined for a field, but does not exist: triggering a cleaning",
                            f"TQID: {tqId}, field: {field}",
                        )
                        tqNeedCleaning = True
                else:
                    if field not in tqData[tqId]:
                        tqData[tqId][field] = []
                    tqData[tqId][field].append(value)
        if tqNeedCleaning:
            self.cleanOrphanedTaskQueues()
        return S_OK(tqData)

    def __updateGlobalShares(self):
        """
        Update internal structure for shares
        """
        # Update group shares
        self.__groupShares = self.getGroupShares()
        # Apply corrections if enabled
        if self.isSharesCorrectionEnabled():
            result = self.getGroupsInTQs()
            if not result["OK"]:
                self.log.error("Could not get groups in the TQs", result["Message"])
            activeGroups = result["Value"]
            newShares = {}
            for group in activeGroups:
                if group in self.__groupShares:
                    newShares[group] = self.__groupShares[group]
            newShares = self.__sharesCorrector.correctShares(newShares)
            for group in self.__groupShares:
                if group in newShares:
                    self.__groupShares[group] = newShares[group]

    def recalculateTQSharesForAll(self):
        """
        Recalculate all priorities for TQ's
        """
        if self.isSharesCorrectionEnabled():
            self.log.info("Updating correctors state")
            self.__sharesCorrector.update()
        self.__updateGlobalShares()
        self.log.info("Recalculating shares for all TQs")
        retVal = self._getConnection()
        if not retVal["OK"]:
            return S_ERROR(f"Can't insert job: {retVal['Message']}")
        result = self._query("SELECT DISTINCT( OwnerGroup ) FROM `tq_TaskQueues`")
        if not result["OK"]:
            return result
        for group in [r[0] for r in result["Value"]]:
            self.recalculateTQSharesForEntity("all", group)
        return S_OK()

    def recalculateTQSharesForEntity(self, user, userGroup, connObj=False):
        """
        Recalculate the shares for a user/userGroup combo
        """
        self.log.info("Recalculating shares", f"for {user}@{userGroup} TQs")
        if userGroup in self.__groupShares:
            share = self.__groupShares[userGroup]
        else:
            share = float(DEFAULT_GROUP_SHARE)
        if Properties.JOB_SHARING in Registry.getPropertiesForGroup(userGroup):
            # If group has JobSharing just set prio for that entry, user is irrelevant
            return self.__setPrioritiesForEntity(user, userGroup, share, connObj=connObj)

        selSQL = f"SELECT Owner, COUNT(Owner) FROM `tq_TaskQueues` WHERE OwnerGroup='{userGroup}' GROUP BY Owner"
        result = self._query(selSQL, conn=connObj)
        if not result["OK"]:
            return result
        # Get owners in this group and the amount of times they appear
        data = [(r[0], r[1]) for r in result["Value"] if r]
        numOwners = len(data)
        # If there are no owners do now
        if numOwners == 0:
            return S_OK()
        # Split the share amongst the number of owners
        share /= numOwners
        entitiesShares = {row[0]: share for row in data}
        # If corrector is enabled let it work it's magic
        if self.isSharesCorrectionEnabled():
            entitiesShares = self.__sharesCorrector.correctShares(entitiesShares, group=userGroup)
        # Keep updating
        owners = dict(data)
        # IF the user is already known and has more than 1 tq, the rest of the users don't need to be modified
        # (The number of owners didn't change)
        if user in owners and owners[user] > 1:
            return self.__setPrioritiesForEntity(user, userGroup, entitiesShares[user], connObj=connObj)
        # Oops the number of owners may have changed so we recalculate the prio for all owners in the group
        for user in owners:
            self.__setPrioritiesForEntity(user, userGroup, entitiesShares[user], connObj=connObj)
        return S_OK()

    def __setPrioritiesForEntity(self, user, userGroup, share, connObj=False, consolidationFunc="AVG"):
        """
        Set the priority for a user/userGroup combo given a splitted share
        """
        self.log.info("Setting priorities", f"to {user}@{userGroup} TQs")
        tqCond = [f"t.OwnerGroup='{userGroup}'"]
        allowBgTQs = gConfig.getValue(f"/Registry/Groups/{userGroup}/AllowBackgroundTQs", False)
        if Properties.JOB_SHARING not in Registry.getPropertiesForGroup(userGroup):
            res = self._escapeString(user)
            if not res["OK"]:
                return res
            user = res["Value"]
            tqCond.append(f"t.Owner= {user} ")
        tqCond.append("t.TQId = j.TQId")
        if consolidationFunc == "AVG":
            selectSQL = "SELECT j.TQId, SUM( j.RealPriority )/COUNT(j.RealPriority) \
FROM `tq_TaskQueues` t, `tq_Jobs` j WHERE "
        elif consolidationFunc == "SUM":
            selectSQL = "SELECT j.TQId, SUM( j.RealPriority ) FROM `tq_TaskQueues` t, `tq_Jobs` j WHERE "
        else:
            return S_ERROR(f"Unknown consolidation func {consolidationFunc} for setting priorities")
        selectSQL += " AND ".join(tqCond)
        selectSQL += " GROUP BY t.TQId"
        result = self._query(selectSQL, conn=connObj)
        if not result["OK"]:
            return result

        tqDict = dict(result["Value"])
        if not tqDict:
            return S_OK()

        result = self.retrieveTaskQueues(list(tqDict))
        if not result["OK"]:
            return result
        allTQsData = result["Value"]

        prioDict = calculate_priority(tqDict, allTQsData, share, allowBgTQs)

        # Execute updates
        for prio, tqs in prioDict.items():
            tqList = ", ".join([str(tqId) for tqId in tqs])
            updateSQL = f"UPDATE `tq_TaskQueues` SET Priority={prio:.4f} WHERE TQId in ( {tqList} )"
            self._update(updateSQL, conn=connObj)
        return S_OK()

    @staticmethod
    def getGroupShares():
        """
        Get all the shares as a DICT
        """
        result = gConfig.getSections("/Registry/Groups")
        if result["OK"]:
            groups = result["Value"]
        else:
            groups = []
        shares = {}
        for group in groups:
            shares[group] = gConfig.getValue(f"/Registry/Groups/{group}/JobShare", DEFAULT_GROUP_SHARE)
        return shares


def calculate_priority(
    tq_dict: dict[int, float], all_tqs_data: dict[int, dict[str, Any]], share: float, allow_bg_tqs: bool
) -> dict[float, list[int]]:
    """
    Calculate the priority for each TQ given a share

    :param tq_dict: dict of {tq_id: prio}
    :param all_tqs_data: dict of {tq_id: {tq_data}}, where tq_data is a dict of {field: value}
    :param share: share to be distributed among TQs
    :param allow_bg_tqs: allow background TQs to be used
    :return: dict of {priority: [tq_ids]}
    """

    def is_background(tq_priority: float, allow_bg_tqs: bool) -> bool:
        """
        A TQ is background if its priority is below a threshold and background TQs are allowed
        """
        return tq_priority <= 0.1 and allow_bg_tqs

    # Calculate Sum of priorities of non background TQs
    total_prio = sum([prio for prio in tq_dict.values() if not is_background(prio, allow_bg_tqs)])

    # Update prio for each TQ
    for tq_id, tq_priority in tq_dict.items():
        if is_background(tq_priority, allow_bg_tqs):
            prio = TQ_MIN_SHARE
        else:
            prio = max((share / total_prio) * tq_priority, TQ_MIN_SHARE)
        tq_dict[tq_id] = prio

    # Generate groups of TQs that will have the same prio=sum(prios) maomenos
    tq_groups: dict[str, list[int]] = defaultdict(list)
    for tq_id, tq_data in all_tqs_data.items():
        for field in ("Jobs", "Priority") + priorityIgnoredFields:
            if field in tq_data:
                tq_data.pop(field)
        tq_hash = []
        for f in sorted(tq_data):
            tq_hash.append(f"{f}:{tq_data[f]}")
        tq_hash = "|".join(tq_hash)
        # if tq_hash not in tq_groups:
        #     tq_groups[tq_hash] = []
        tq_groups[tq_hash].append(tq_id)

    # Do the grouping
    for tq_group in tq_groups.values():
        total_prio = sum(tq_dict[tq_id] for tq_id in tq_group)
        for tq_id in tq_group:
            tq_dict[tq_id] = total_prio

    # Group by priorities
    result: dict[float, list[int]] = defaultdict(list)
    for tq_id, tq_priority in tq_dict.items():
        result[tq_priority].append(tq_id)

    return result
