""" Frontend to MySQL DB AccountingDB
"""
import datetime
import random
import threading
import time

from DIRAC import S_ERROR, S_OK
from DIRAC.Core.Base.DB import DB
from DIRAC.Core.Utilities import DEncode, List, ThreadSafe, TimeUtilities
from DIRAC.Core.Utilities.Plotting.TypeLoader import TypeLoader
from DIRAC.Core.Utilities.ThreadPool import ThreadPool

gSynchro = ThreadSafe.Synchronizer()


class AccountingDB(DB):
    def __init__(self, name="Accounting/AccountingDB", readOnly=False, parentLogger=None):
        DB.__init__(self, "AccountingDB", name, parentLogger=parentLogger)
        self.maxBucketTime = 604800  # 1 w
        self.autoCompact = False
        self.__readOnly = readOnly
        self.__doingCompaction = False
        self.__doingPendingLockTime = 0
        self.__deadLockRetries = 2
        self.dbCatalog = {}
        self.dbBucketsLength = {}
        self.__keysCache = {}
        maxParallelInsertions = self.getCSOption("ParallelRecordInsertions", 10)
        self.__threadPool = ThreadPool(1, maxParallelInsertions)
        self.__threadPool.daemonize()
        self.catalogTableName = _getTableName("catalog", "Types")
        self._createTables(
            {
                self.catalogTableName: {
                    "Fields": {
                        "name": "VARCHAR(64) UNIQUE NOT NULL",
                        "keyFields": "VARCHAR(255) NOT NULL",
                        "valueFields": "VARCHAR(255) NOT NULL",
                        "bucketsLength": "VARCHAR(255) NOT NULL",
                    },
                    "PrimaryKey": "name",
                }
            }
        )
        self.__loadCatalogFromDB()

        self.__compactTime = datetime.time(hour=2, minute=random.randint(0, 59), second=random.randint(0, 59))
        lcd = datetime.datetime.utcnow()
        lcd.replace(hour=self.__compactTime.hour + 1, minute=0, second=0)
        self.__lastCompactionEpoch = TimeUtilities.toEpoch(lcd)
        self.__registerTypes()

    def __loadTablesCreated(self):
        result = self._query("show tables")
        if not result["OK"]:  # pylint: disable=invalid-sequence-index
            return result
        return S_OK([f[0] for f in result["Value"]])  # pylint: disable=invalid-sequence-index

    def autoCompactDB(self):
        self.autoCompact = True
        th = threading.Thread(target=self.__periodicAutoCompactDB)
        th.daemon = True
        th.start()

    def __periodicAutoCompactDB(self):
        while self.autoCompact:
            nct = datetime.datetime.utcnow()
            if nct.hour >= self.__compactTime.hour:
                nct = nct + datetime.timedelta(days=1)
            nct = nct.replace(
                hour=self.__compactTime.hour, minute=self.__compactTime.minute, second=self.__compactTime.second
            )
            self.log.info("Next db compaction", f"will be at {nct}")
            sleepTime = TimeUtilities.toEpoch(nct) - TimeUtilities.toEpoch()
            time.sleep(sleepTime)
            self.compactBuckets()

    def __registerTypes(self):
        """
        Register all types
        """
        objectsLoaded = TypeLoader().getTypes()

        # Load the files
        for typeName in sorted(objectsLoaded):
            typeClass = objectsLoaded[typeName]

            typeDef = typeClass().getDefinition()
            definitionKeyFields, definitionAccountingFields, bucketsLength = typeDef[1:]
            # If already defined check the similarities
            if typeName in self.dbCatalog:
                bucketsLength.sort()
                if bucketsLength != self.dbBucketsLength[typeName]:
                    bucketsLength = self.dbBucketsLength[typeName]
                    self.log.warn("Bucket length has changed", f"for type {typeName}")
                keyFields = [f[0] for f in definitionKeyFields]
                if keyFields != self.dbCatalog[typeName]["keys"]:
                    keyFields = self.dbCatalog[typeName]["keys"]
                    self.log.error("Definition fields have changed", f"Type {typeName}")
                valueFields = [f[0] for f in definitionAccountingFields]
                if valueFields != self.dbCatalog[typeName]["values"]:
                    valueFields = self.dbCatalog[typeName]["values"]
                    self.log.error("Accountable fields have changed", f"Type {typeName}")
            # Try to re register to check all the tables are there
            retVal = self.registerType(typeName, definitionKeyFields, definitionAccountingFields, bucketsLength)
            if not retVal["OK"]:
                self.log.error("Can't register type", f"{typeName}: {retVal['Message']}")
            # If it has been properly registered, update info
            elif retVal["Value"]:
                # Set the timespan
                self.dbCatalog[typeName]["dataTimespan"] = typeClass().getDataTimespan()
                self.dbCatalog[typeName]["definition"] = {
                    "keys": definitionKeyFields,
                    "values": definitionAccountingFields,
                }
        return S_OK()

    def __loadCatalogFromDB(self):
        retVal = self._query(
            f"SELECT `name`, `keyFields`, `valueFields`, `bucketsLength` FROM `{self.catalogTableName}`"
        )
        if not retVal["OK"]:
            raise Exception(retVal["Message"])
        for typesEntry in retVal["Value"]:
            typeName = typesEntry[0]
            keyFields = List.fromChar(typesEntry[1], ",")
            valueFields = List.fromChar(typesEntry[2], ",")
            bucketsLength = DEncode.decode(typesEntry[3].encode())[0]
            self.__addToCatalog(typeName, keyFields, valueFields, bucketsLength)

    def getWaitingRecordsLifeTime(self):
        """
        Get the time records can live in the IN tables without no retry
        """
        return self.getCSOption("RecordMaxWaitingTime", 86400)

    def markAllPendingRecordsAsNotTaken(self):
        """
        Mark all records to be processed as not taken
        NOTE: ONLY EXECUTE THIS AT THE BEGINNING OF THE DATASTORE SERVICE!
        """
        self.log.always("Marking all records to be processed as not taken")
        for typeName in self.dbCatalog:
            sqlTableName = _getTableName("in", typeName)
            result = self._update(f"UPDATE `{sqlTableName}` SET taken=0")
            if not result["OK"]:
                return result
        return S_OK()

    def loadPendingRecords(self):
        """
        Load all records pending to insertion and generate threaded jobs
        """
        gSynchro.lock()
        try:
            now = time.time()
            if now - self.__doingPendingLockTime <= 3600:
                return S_OK()
            self.__doingPendingLockTime = now
        finally:
            gSynchro.unlock()
        self.log.info("[PENDING] Loading pending records for insertion")
        pending = 0
        now = TimeUtilities.toEpoch()
        recordsPerSlot = self.getCSOption("RecordsPerSlot", 100)
        for typeName, typeDef in self.dbCatalog.items():
            self.log.info(f"[PENDING] Checking {typeName}")
            pendingInQueue = self.__threadPool.pendingJobs()
            emptySlots = max(0, 3000 - pendingInQueue)
            self.log.info("[PENDING] %s in the queue, %d empty slots" % (pendingInQueue, emptySlots))
            if emptySlots < 1:
                continue
            emptySlots = min(100, emptySlots)
            sqlTableName = _getTableName("in", typeName)
            sqlFields = ["id"] + typeDef["typeFields"]
            sqlCond = (
                "WHERE taken = 0 or TIMESTAMPDIFF( SECOND, takenSince, UTC_TIMESTAMP() ) > %s"
                % self.getWaitingRecordsLifeTime()
            )
            result = self._query(
                "SELECT %s FROM `%s` %s ORDER BY id ASC LIMIT %d"
                % (", ".join(["`%s`" % f for f in sqlFields]), sqlTableName, sqlCond, emptySlots * recordsPerSlot)
            )
            if not result["OK"]:
                self.log.error(
                    "[PENDING] Error when trying to get pending records",
                    f"for {typeName} : {result['Message']}",
                )
                return result
            self.log.info(f"[PENDING] Got {len(result['Value'])} pending records for type {typeName}")
            dbData = result["Value"]
            idList = [str(r[0]) for r in dbData]
            # If nothing to do, continue
            if not idList:
                continue
            result = self._update(
                "UPDATE `%s` SET taken=1, takenSince=UTC_TIMESTAMP() WHERE id in (%s)"
                % (sqlTableName, ", ".join(idList))
            )
            if not result["OK"]:
                self.log.error(
                    "[PENDING] Error when trying set state to waiting records",
                    f"for {typeName} : {result['Message']}",
                )
                self.__doingPendingLockTime = 0
                return result
            # Group them in groups of 10
            recordsToProcess = []
            for record in dbData:
                pending += 1
                iD = record[0]
                startTime = record[-2]
                endTime = record[-1]
                valuesList = list(record[1:-2])
                recordsToProcess.append((iD, typeName, startTime, endTime, valuesList, now))
                if len(recordsToProcess) % recordsPerSlot == 0:
                    self.__threadPool.generateJobAndQueueIt(self.__insertFromINTable, args=(recordsToProcess,))
                    recordsToProcess = []
            if recordsToProcess:
                self.__threadPool.generateJobAndQueueIt(self.__insertFromINTable, args=(recordsToProcess,))
        self.log.info(f"[PENDING] Got {pending} records requests for all types")
        self.__doingPendingLockTime = 0
        return S_OK()

    def __addToCatalog(self, typeName, keyFields, valueFields, bucketsLength):
        """
        Add type to catalog
        """
        self.log.verbose(f"Adding to catalog type {typeName}", f"with length {str(bucketsLength)}")
        self.dbCatalog[typeName] = {
            "keys": keyFields,
            "values": valueFields,
            "typeFields": [],
            "bucketFields": [],
            "dataTimespan": 0,
        }
        self.dbCatalog[typeName]["typeFields"].extend(keyFields)
        self.dbCatalog[typeName]["typeFields"].extend(valueFields)
        self.dbCatalog[typeName]["bucketFields"] = list(self.dbCatalog[typeName]["typeFields"])
        self.dbCatalog[typeName]["typeFields"].extend(["startTime", "endTime"])
        self.dbCatalog[typeName]["bucketFields"].extend(["entriesInBucket", "startTime", "bucketLength"])
        self.dbBucketsLength[typeName] = bucketsLength

    def changeBucketsLength(self, typeName, bucketsLength):
        gSynchro.lock()

        try:
            if typeName not in self.dbCatalog:
                return S_ERROR(f"{typeName} is not a valid type name")
            bucketsLength.sort()
            bucketsEncoding = DEncode.encode(bucketsLength)
            retVal = self._update(
                "UPDATE `%s` set bucketsLength = '%s' where name = '%s'"
                % (self.catalogTableName, bucketsEncoding, typeName)
            )
            if not retVal["OK"]:
                return retVal
            self.dbBucketsLength[typeName] = bucketsLength
        finally:
            gSynchro.unlock()
        return self.regenerateBuckets(typeName)

    @gSynchro
    def registerType(self, name, definitionKeyFields, definitionAccountingFields, bucketsLength):
        """
        Register a new type
        """

        result = self.__loadTablesCreated()
        if not result["OK"]:
            return result
        tablesInThere = result["Value"]
        keyFieldsList = []
        valueFieldsList = []
        for key in definitionKeyFields:
            keyFieldsList.append(key[0])
        for value in definitionAccountingFields:
            valueFieldsList.append(value[0])
        for field in definitionKeyFields:
            if field in valueFieldsList:
                return S_ERROR(f"Key field {field} is also in the list of value fields")
        for field in definitionAccountingFields:
            if field in keyFieldsList:
                return S_ERROR(f"Value field {field} is also in the list of key fields")
        for bucket in bucketsLength:
            if not isinstance(bucket, tuple):
                return S_ERROR("Length of buckets should be a list of tuples")
            if len(bucket) != 2:
                return S_ERROR("Length of buckets should have 2d tuples")
        updateDBCatalog = True
        if name in self.dbCatalog:
            updateDBCatalog = False
        tables = {}
        for key in definitionKeyFields:
            keyTableName = _getTableName("key", name, key[0])
            if keyTableName not in tablesInThere:
                self.log.info(f"Table for key {key[0]} has to be created")
                tables[keyTableName] = {
                    "Fields": {"id": "INTEGER NOT NULL AUTO_INCREMENT", "value": f"{key[1]} NOT NULL"},
                    "UniqueIndexes": {"valueindex": ["value"]},
                    "PrimaryKey": "id",
                }
        # Registering type
        fieldsDict = {}
        bucketFieldsDict = {}
        inbufferDict = {"id": "BIGINT NOT NULL AUTO_INCREMENT"}
        bucketIndexes = {"startTimeIndex": ["startTime"], "bucketLengthIndex": ["bucketLength"]}
        uniqueIndexFields = ["startTime"]
        for field in definitionKeyFields:
            bucketIndexes[f"{field[0]}Index"] = [field[0]]
            uniqueIndexFields.append(field[0])
            fieldsDict[field[0]] = "INTEGER NOT NULL"
            bucketFieldsDict[field[0]] = "INTEGER NOT NULL"
            inbufferDict[field[0]] = field[1] + " NOT NULL"
        for field in definitionAccountingFields:
            fieldsDict[field[0]] = field[1] + " NOT NULL"
            bucketFieldsDict[field[0]] = "DECIMAL(30,10) NOT NULL"
            inbufferDict[field[0]] = field[1] + " NOT NULL"
        fieldsDict["startTime"] = "INT UNSIGNED NOT NULL"
        fieldsDict["endTime"] = "INT UNSIGNED NOT NULL"
        bucketFieldsDict["entriesInBucket"] = "DECIMAL(30,10) NOT NULL"
        bucketFieldsDict["startTime"] = "INT UNSIGNED NOT NULL"
        inbufferDict["startTime"] = "INT UNSIGNED NOT NULL"
        inbufferDict["endTime"] = "INT UNSIGNED NOT NULL"
        inbufferDict["taken"] = "TINYINT(1) DEFAULT 1 NOT NULL"
        inbufferDict["takenSince"] = "DATETIME NOT NULL"
        bucketFieldsDict["bucketLength"] = "MEDIUMINT UNSIGNED NOT NULL"
        uniqueIndexFields.append("bucketLength")
        bucketTableName = _getTableName("bucket", name)
        if bucketTableName not in tablesInThere:
            tables[bucketTableName] = {
                "Fields": bucketFieldsDict,
                "UniqueIndexes": {"UniqueConstraint": uniqueIndexFields},
            }
        typeTableName = _getTableName("type", name)
        if typeTableName not in tablesInThere:
            tables[typeTableName] = {"Fields": fieldsDict}
        inTableName = _getTableName("in", name)
        if inTableName not in tablesInThere:
            tables[inTableName] = {"Fields": inbufferDict, "PrimaryKey": "id"}
        if self.__readOnly:
            if tables:
                self.log.notice(f"ReadOnly mode: Skipping create of tables for {name}. Removing from memory catalog")
                self.log.verbose(f"Skipping creation of tables {', '.join([tn for tn in tables])}")
                try:
                    self.dbCatalog.pop(name)
                except KeyError:
                    pass
            else:
                self.log.notice(f"ReadOnly mode: {name} is OK")
            return S_OK(not updateDBCatalog)

        if tables:
            retVal = self._createTables(tables)
            if not retVal["OK"]:
                self.log.error("Can't create type", f"{name}: {retVal['Message']}")
                return S_ERROR(f"Can't create type {name}: {retVal['Message']}")
        if updateDBCatalog:
            bucketsLength.sort()
            bucketsEncoding = DEncode.encode(bucketsLength)
            self.insertFields(
                self.catalogTableName,
                ["name", "keyFields", "valueFields", "bucketsLength"],
                [name, ",".join(keyFieldsList), ",".join(valueFieldsList), bucketsEncoding],
            )
            self.__addToCatalog(name, keyFieldsList, valueFieldsList, bucketsLength)
        self.log.info(f"Registered type {name}")
        return S_OK(True)

    def getRegisteredTypes(self):
        """
        Get list of registered types
        """
        retVal = self._query(
            f"SELECT `name`, `keyFields`, `valueFields`, `bucketsLength` FROM `{self.catalogTableName}`"
        )
        if not retVal["OK"]:
            return retVal
        typesList = []
        for name, keyFields, valueFields, bucketsLength in retVal["Value"]:
            keyFields = List.fromChar(keyFields)
            valueFields = List.fromChar(valueFields)
            bucketsLength = DEncode.decode(bucketsLength.encode())
            typesList.append([name, keyFields, valueFields, bucketsLength])
        return S_OK(typesList)

    def getKeyValues(self, typeName, condDict, connObj=False):
        """
        Get all values for a given key field in a type
        """
        keyValuesDict = {}

        keyTables = []
        sqlCond = []
        mainTable = f"`{_getTableName('bucket', typeName)}`"
        try:
            typeKeysList = self.dbCatalog[typeName]["keys"]
        except KeyError:
            return S_ERROR("Please select a category")

        for keyName in condDict:
            if keyName in typeKeysList:
                keyTable = f"`{_getTableName('key', typeName, keyName)}`"
                if keyTable not in keyTables:
                    keyTables.append(keyTable)
                sqlCond.append(f"{keyTable}.id = {mainTable}.`{keyName}`")
                for value in condDict[keyName]:
                    sqlCond.append(f"{keyTable}.value = {self._escapeString(value)['Value']}")

        for keyName in typeKeysList:
            keyTable = f"`{_getTableName('key', typeName, keyName)}`"
            allKeyTables = keyTables
            if keyTable not in allKeyTables:
                allKeyTables = list(keyTables)
                allKeyTables.append(keyTable)
            cmd = f"SELECT DISTINCT {keyTable}.value FROM {', '.join(allKeyTables)}"
            if sqlCond:
                sqlValueLink = f"{keyTable}.id = {mainTable}.`{keyName}`"
                cmd += f", {mainTable} WHERE {sqlValueLink} AND {' AND '.join(sqlCond)}"
            retVal = self._query(cmd, conn=connObj)
            if not retVal["OK"]:
                return retVal
            keyValuesDict[keyName] = [r[0] for r in retVal["Value"]]

        return S_OK(keyValuesDict)

    @gSynchro
    def deleteType(self, typeName):
        """
        Deletes a type
        """
        if self.__readOnly:
            return S_ERROR("ReadOnly mode enabled. No modification allowed")
        if typeName not in self.dbCatalog:
            return S_ERROR(f"Type {typeName} does not exist")
        self.log.info("Deleting type", typeName)
        tablesToDelete = []
        for keyField in self.dbCatalog[typeName]["keys"]:
            tablesToDelete.append(f"`{_getTableName('key', typeName, keyField)}`")
        tablesToDelete.insert(0, f"`{_getTableName('type', typeName)}`")
        tablesToDelete.insert(0, f"`{_getTableName('bucket', typeName)}`")
        tablesToDelete.insert(0, f"`{_getTableName('in', typeName)}`")
        retVal = self._query(f"DROP TABLE {', '.join(tablesToDelete)}")
        if not retVal["OK"]:
            return retVal
        retVal = self._update(f"DELETE FROM `{_getTableName('catalog', 'Types')}` WHERE name='{typeName}'")
        del self.dbCatalog[typeName]
        return S_OK()

    def __getIdForKeyValue(self, typeName, keyName, keyValue, conn=False):
        """
        Finds id number for value in a key table
        """
        retVal = self._escapeString(keyValue)
        if not retVal["OK"]:
            return retVal
        keyValue = retVal["Value"]
        retVal = self._query(
            f"SELECT `id` FROM `{_getTableName('key', typeName, keyName)}` WHERE `value`={keyValue}",
            conn=conn,
        )
        if not retVal["OK"]:
            return retVal
        if len(retVal["Value"]) > 0:
            return S_OK(retVal["Value"][0][0])
        return S_ERROR(f"Key id {keyName} for value {keyValue} does not exist although it shoud")

    def __addKeyValue(self, typeName, keyName, keyValue):
        """
        Adds a key value to a key table if not existant
        """
        # Cast to string just in case
        if not isinstance(keyValue, str):
            keyValue = str(keyValue)
        # No more than 64 chars for keys
        if len(keyValue) > 64:
            keyValue = keyValue[:64]

        # Look into the cache
        if typeName not in self.__keysCache:
            self.__keysCache[typeName] = {}
        typeCache = self.__keysCache[typeName]
        if keyName not in typeCache:
            typeCache[keyName] = {}
        keyCache = typeCache[keyName]
        if keyValue in keyCache:
            return S_OK(keyCache[keyValue])
        # Retrieve key
        keyTable = _getTableName("key", typeName, keyName)
        retVal = self.__getIdForKeyValue(typeName, keyName, keyValue)
        if retVal["OK"]:
            keyCache[keyValue] = retVal["Value"]
            return retVal
        # Key is not in there
        retVal = self._getConnection()
        if not retVal["OK"]:
            return retVal
        connection = retVal["Value"]
        self.log.info(f"Value {keyValue} for key {keyName} didn't exist, inserting")
        retVal = self.insertFields(keyTable, ["value"], [keyValue], conn=connection)
        if not retVal["OK"] and retVal["Message"].find("Duplicate key") == -1:
            return retVal
        result = self.__getIdForKeyValue(typeName, keyName, keyValue, conn=connection)
        if not result["OK"]:
            return result
        keyCache[keyValue] = result["Value"]
        return result

    def calculateBucketLengthForTime(self, typeName, now, when):
        """
        Get the expected bucket time for a moment in time
        """
        for granuT in self.dbBucketsLength[typeName]:
            nowBucketed = now - now % granuT[1]
            dif = max(0, nowBucketed - when)
            if dif <= granuT[0]:
                return granuT[1]
        return self.maxBucketTime

    def calculateBuckets(self, typeName, startTime, endTime, nowEpoch=False):
        """
        Magic function for calculating buckets between two times and
        the proportional part for each bucket
        """
        if not nowEpoch:
            nowEpoch = int(TimeUtilities.toEpoch())
        bucketTimeLength = self.calculateBucketLengthForTime(typeName, nowEpoch, startTime)
        currentBucketStart = startTime - startTime % bucketTimeLength
        if startTime == endTime:
            return [(currentBucketStart, 1, bucketTimeLength)]
        buckets = []
        totalLength = endTime - startTime
        while currentBucketStart < endTime:
            start = max(currentBucketStart, startTime)
            end = min(currentBucketStart + bucketTimeLength, endTime)
            proportion = float(end - start) / totalLength
            buckets.append((currentBucketStart, proportion, bucketTimeLength))
            currentBucketStart += bucketTimeLength
            bucketTimeLength = self.calculateBucketLengthForTime(typeName, nowEpoch, currentBucketStart)
        return buckets

    def __insertInQueueTable(self, typeName, startTime, endTime, valuesList):
        sqlFields = ["taken", "takenSince"] + self.dbCatalog[typeName]["typeFields"]
        sqlValues = ["0", "UTC_TIMESTAMP()"] + valuesList + [startTime, endTime]
        if len(sqlFields) != len(sqlValues):
            numRcv = len(valuesList) + 2
            numExp = len(self.dbCatalog[typeName]["typeFields"])
            return S_ERROR(f"Fields mismatch for record {typeName}. {numRcv} fields and {numExp} expected")
        retVal = self.insertFields(_getTableName("in", typeName), sqlFields, sqlValues)
        if not retVal["OK"]:
            return retVal
        return S_OK(retVal["lastRowId"])

    def insertRecordBundleThroughQueue(self, recordsToQueue):
        if self.__readOnly:
            return S_ERROR("ReadOnly mode enabled. No modification allowed")
        recordsToProcess = []
        now = TimeUtilities.toEpoch()
        for record in recordsToQueue:
            typeName, startTime, endTime, valuesList = record
            result = self.__insertInQueueTable(typeName, startTime, endTime, valuesList)
            if not result["OK"]:
                return result
            iD = result["Value"]
            recordsToProcess.append((iD, typeName, startTime, endTime, valuesList, now))

        return S_OK()

    def insertRecordThroughQueue(self, typeName, startTime, endTime, valuesList):
        """
        Insert a record in the intable to be really insterted afterwards
        """
        if self.__readOnly:
            return S_ERROR("ReadOnly mode enabled. No modification allowed")
        self.log.info(
            "Adding record to queue",
            "for type %s\n [%s -> %s]"
            % (typeName, TimeUtilities.fromEpoch(startTime), TimeUtilities.fromEpoch(endTime)),
        )
        if typeName not in self.dbCatalog:
            return S_ERROR(f"Type {typeName} has not been defined in the db")
        result = self.__insertInQueueTable(typeName, startTime, endTime, valuesList)
        if not result["OK"]:
            return result

        return S_OK()

    def __insertFromINTable(self, recordTuples):
        """
        Do the real insert and delete from the in buffer table
        """
        self.log.verbose("Received bundle to process", f"of {len(recordTuples)} elements")
        for record in recordTuples:
            iD, typeName, startTime, endTime, valuesList, insertionEpoch = record
            result = self.insertRecordDirectly(typeName, startTime, endTime, valuesList)
            if not result["OK"]:
                self._update(f"UPDATE `{_getTableName('in', typeName)}` SET taken=0 WHERE id={iD}")
                self.log.error("Can't insert row", result["Message"])
                continue
            result = self._update(f"DELETE FROM `{_getTableName('in', typeName)}` WHERE id={iD}")
            if not result["OK"]:
                self.log.error("Can't delete row from the IN table", result["Message"])

    def insertRecordDirectly(self, typeName, startTime, endTime, valuesList):
        """
        Add an entry to the type contents
        """
        if self.__readOnly:
            return S_ERROR("ReadOnly mode enabled. No modification allowed")
        self.log.info(
            "Adding record",
            "for type %s\n [%s -> %s]"
            % (typeName, TimeUtilities.fromEpoch(startTime), TimeUtilities.fromEpoch(endTime)),
        )
        if typeName not in self.dbCatalog:
            return S_ERROR(f"Type {typeName} has not been defined in the db")
        # Discover key indexes
        for keyPos, keyName in enumerate(self.dbCatalog[typeName]["keys"]):
            keyValue = valuesList[keyPos]
            retVal = self.__addKeyValue(typeName, keyName, keyValue)
            if not retVal["OK"]:
                return retVal
            self.log.debug(f"Value {keyValue} for key {keyName} has id {retVal['Value']}")
            valuesList[keyPos] = retVal["Value"]
        insertList = list(valuesList)
        insertList.append(startTime)
        insertList.append(endTime)
        retVal = self._getConnection()
        if not retVal["OK"]:
            return retVal
        connObj = retVal["Value"]
        try:
            retVal = self.insertFields(
                _getTableName("type", typeName), self.dbCatalog[typeName]["typeFields"], insertList, conn=connObj
            )
            if not retVal["OK"]:
                return retVal
            # HACK: One more record to split in the buckets to be able to count total entries
            valuesList.append(1)
            retVal = self.__startTransaction(connObj)
            if not retVal["OK"]:
                return retVal
            retVal = self.__splitInBuckets(typeName, startTime, endTime, valuesList, connObj=connObj)
            if not retVal["OK"]:
                self.__rollbackTransaction(connObj)
                return retVal
            return self.__commitTransaction(connObj)
        finally:
            connObj.close()

    def deleteRecord(self, typeName, startTime, endTime, valuesList):
        """
        Delete an entry
        """
        if self.__readOnly:
            return S_ERROR("ReadOnly mode enabled. No modification allowed")
        if typeName not in self.dbCatalog:
            return S_ERROR(f"Type {typeName} has not been defined in the db")

        self.log.info(
            "Deleting record",
            "for type %s\n [%s -> %s]"
            % (typeName, TimeUtilities.fromEpoch(startTime), TimeUtilities.fromEpoch(endTime)),
        )
        sqlValues = []
        sqlValues.extend(valuesList)
        # Discover key indexes
        for keyPos, keyName in enumerate(self.dbCatalog[typeName]["keys"]):
            keyValue = sqlValues[keyPos]
            retVal = self.__addKeyValue(typeName, keyName, keyValue)
            if not retVal["OK"]:
                return retVal
            self.log.verbose(f"Value {keyValue} for key {keyName} has id {retVal['Value']}")
            sqlValues[keyPos] = retVal["Value"]
        sqlCond = []
        mainTable = _getTableName("type", typeName)
        sqlValues.extend([startTime, endTime])
        numKeyFields = len(self.dbCatalog[typeName]["keys"])
        numValueFields = len(self.dbCatalog[typeName]["values"])
        for i, value in enumerate(sqlValues):
            needToRound = False
            if i >= numKeyFields and i - numKeyFields < numValueFields:
                vIndex = i - numKeyFields
                if self.dbCatalog[typeName]["definition"]["values"][vIndex][1].find("FLOAT") > -1:
                    needToRound = True
            if needToRound:
                compVal = [f"`{mainTable}`.`{self.dbCatalog[typeName]['typeFields'][i]}`", f"{value:f}"]
                compVal = [f"CEIL( {v} * 1000 )" for v in compVal]
                compVal = f"ABS( {' - '.join(compVal)} ) <= 1 "
            else:
                sqlCond.append(f"`{mainTable}`.`{self.dbCatalog[typeName]['typeFields'][i]}`={value}")
        retVal = self._getConnection()
        if not retVal["OK"]:
            return retVal
        connObj = retVal["Value"]
        retVal = self.__startTransaction(connObj)
        if not retVal["OK"]:
            return retVal
        retVal = self._update(f"DELETE FROM `{mainTable}` WHERE {' AND '.join(sqlCond)}", conn=connObj)
        if not retVal["OK"]:
            return retVal
        numInsertions = retVal["Value"]
        # Deleted from type, now the buckets
        # HACK: One more record to split in the buckets to be able to count total entries
        if numInsertions == 0:
            return S_OK(0)
        sqlValues.append(1)
        retVal = self.__deleteFromBuckets(typeName, startTime, endTime, sqlValues, numInsertions, connObj=connObj)
        if not retVal["OK"]:
            self.__rollbackTransaction(connObj)
            return retVal
        retVal = self.__commitTransaction(connObj)
        if not retVal["OK"]:
            self.__rollbackTransaction(connObj)
            return retVal
        return S_OK(numInsertions)

    def __splitInBuckets(self, typeName, startTime, endTime, valuesList, connObj=False):
        """
        Bucketize a record
        """
        # Calculate amount of buckets
        buckets = self.calculateBuckets(typeName, startTime, endTime)
        if not buckets:
            return S_OK()
        # Separate key values from normal values
        numKeys = len(self.dbCatalog[typeName]["keys"])
        keyValues = valuesList[:numKeys]
        valuesList = valuesList[numKeys:]
        self.log.debug("Splitting entry", f" in {len(buckets)} buckets")
        return self.__writeBuckets(typeName, buckets, keyValues, valuesList, connObj=connObj)

    def __deleteFromBuckets(self, typeName, startTime, endTime, valuesList, numInsertions, connObj=False):
        """
        DeBucketize a record
        """
        # Calculate amount of buckets
        buckets = self.calculateBuckets(typeName, startTime, endTime, self.__lastCompactionEpoch)
        # Separate key values from normal values
        numKeys = len(self.dbCatalog[typeName]["keys"])
        keyValues = valuesList[:numKeys]
        valuesList = valuesList[numKeys:]
        self.log.verbose("Deleting bucketed entry", f"from {len(buckets)} buckets")
        for bucketInfo in buckets:
            bucketStartTime = bucketInfo[0]
            bucketProportion = bucketInfo[1]
            bucketLength = bucketInfo[2]
            for _i in range(max(1, self.__deadLockRetries)):
                retVal = self.__extractFromBucket(
                    typeName,
                    bucketStartTime,
                    bucketLength,
                    keyValues,
                    valuesList,
                    bucketProportion * numInsertions,
                    connObj=connObj,
                )
                if not retVal["OK"]:
                    # If failed because of dead lock try restarting
                    if retVal["Message"].find("try restarting transaction"):
                        continue
                    return retVal
                # If OK, break loop
                if retVal["OK"]:
                    break
        return S_OK()

    def getBucketsDef(self, typeName):
        return self.dbBucketsLength[typeName]

    def __generateSQLConditionForKeys(self, typeName, keyValues):
        """
        Generate sql condition for buckets, values are indexes to real values
        """
        realCondList = []
        for keyPos, keyField in enumerate(self.dbCatalog[typeName]["keys"]):
            keyValue = keyValues[keyPos]
            retVal = self._escapeString(keyValue)
            if not retVal["OK"]:
                return retVal
            keyValue = retVal["Value"]
            realCondList.append(f"`{_getTableName('bucket', typeName)}`.`{keyField}` = {keyValue}")
        return " AND ".join(realCondList)

    def __extractFromBucket(
        self, typeName, startTime, bucketLength, keyValues, bucketValues, proportion, connObj=False
    ):
        """
        Update a bucket when coming from the raw insert
        """
        tableName = _getTableName("bucket", typeName)
        cmd = f"UPDATE `{tableName}` SET "
        sqlValList = []
        for pos, valueField in enumerate(self.dbCatalog[typeName]["values"]):
            value = bucketValues[pos]
            fullFieldName = f"`{tableName}`.`{valueField}`"
            sqlValList.append(f"{fullFieldName}=GREATEST(0,{fullFieldName}-({value}*{proportion}))")
        sqlValList.append(
            "`%s`.`entriesInBucket`=GREATEST(0,`%s`.`entriesInBucket`-(%s*%s))"
            % (tableName, tableName, bucketValues[-1], proportion)
        )
        cmd += ", ".join(sqlValList)
        cmd += " WHERE `{}`.`startTime`='{}' AND `{}`.`bucketLength`='{}' AND ".format(
            tableName,
            startTime,
            tableName,
            bucketLength,
        )
        cmd += self.__generateSQLConditionForKeys(typeName, keyValues)
        return self._update(cmd, conn=connObj)

    def __writeBuckets(self, typeName, buckets, keyValues, valuesList, connObj=False):
        """Insert or update a bucket"""
        #     tableName = _getTableName( "bucket", typeName )
        # INSERT PART OF THE QUERY
        sqlFields = ["`startTime`", "`bucketLength`", "`entriesInBucket`"]
        for keyPos in range(len(self.dbCatalog[typeName]["keys"])):
            sqlFields.append(f"`{self.dbCatalog[typeName]['keys'][keyPos]}`")
        sqlUpData = ["`entriesInBucket`=`entriesInBucket`+VALUES(`entriesInBucket`)"]
        for valPos in range(len(self.dbCatalog[typeName]["values"])):
            valueField = f"`{self.dbCatalog[typeName]['values'][valPos]}`"
            sqlFields.append(valueField)
            sqlUpData.append(f"{valueField}={valueField}+VALUES({valueField})")
        valuesGroups = []
        for bucketInfo in buckets:
            bStartTime = bucketInfo[0]
            bProportion = bucketInfo[1]
            bLength = bucketInfo[2]
            sqlValues = [bStartTime, bLength, f"({valuesList[-1]}*{bProportion})"]
            for keyPos in range(len(self.dbCatalog[typeName]["keys"])):
                sqlValues.append(keyValues[keyPos])
            for valPos in range(len(self.dbCatalog[typeName]["values"])):
                #         value = valuesList[ valPos ]
                sqlValues.append(f"({valuesList[valPos]}*{bProportion})")
            valuesGroups.append(f"( {','.join(str(val) for val in sqlValues)} )")

        cmd = f"INSERT INTO `{_getTableName('bucket', typeName)}` ( {', '.join(sqlFields)} ) "
        cmd += f"VALUES {', '.join(valuesGroups)} "
        cmd += f"ON DUPLICATE KEY UPDATE {', '.join(sqlUpData)}"

        for _i in range(max(1, self.__deadLockRetries)):
            result = self._update(cmd, conn=connObj)
            if not result["OK"]:
                # If failed because of dead lock try restarting
                if result["Message"].find("try restarting transaction"):
                    continue
                return result
            # If OK, break loopo
            if result["OK"]:
                return result

        return S_ERROR(f"Cannot update bucket: {result['Message']}")

    def __checkFieldsExistsInType(self, typeName, fields, tableType):
        """
        Check wether a list of fields exist for a given typeName
        """
        missing = []
        tableFields = self.dbCatalog[typeName][f"{tableType}Fields"]
        for key in fields:
            if key not in tableFields:
                missing.append(key)
        return missing

    def __checkIncomingFieldsForQuery(self, typeName, selectFields, condDict, groupFields, orderFields, tableType):
        missing = self.__checkFieldsExistsInType(typeName, selectFields[1], tableType)
        if missing:
            return S_ERROR(f"Value keys {', '.join(missing)} are not defined")
        missing = self.__checkFieldsExistsInType(typeName, condDict, tableType)
        if missing:
            return S_ERROR(f"Condition keys {', '.join(missing)} are not defined")
        if groupFields:
            missing = self.__checkFieldsExistsInType(typeName, groupFields[1], tableType)
            if missing:
                return S_ERROR(f"Group fields {', '.join(missing)} are not defined")
        if orderFields:
            missing = self.__checkFieldsExistsInType(typeName, orderFields[1], tableType)
            if missing:
                return S_ERROR(f"Order fields {', '.join(missing)} are not defined")
        return S_OK()

    def retrieveRawRecords(self, typeName, startTime, endTime, condDict, orderFields):
        """
        Get RAW data from the DB
        """
        if typeName not in self.dbCatalog:
            return S_ERROR(f"Type {typeName} not defined")
        selectFields = [["%s", "%s"], ["startTime", "endTime"]]
        for tK in ("keys", "values"):
            for key in self.dbCatalog[typeName][tK]:
                selectFields[0].append("%s")
                selectFields[1].append(key)
        selectFields[0] = ", ".join(selectFields[0])
        return self.__queryType(typeName, startTime, endTime, selectFields, condDict, False, orderFields, "type")

    def retrieveBucketedData(
        self, typeName, startTime, endTime, selectFields, condDict, groupFields, orderFields, connObj=False
    ):
        """
        Get data from the DB

        Parameters:
         - typeName -> typeName
         - startTime & endTime -> int
             epoch objects. Do I need to explain the meaning?
         - selectFields: list containing a string and a list of fields:
                           ["SUM(%s), %s/%s", ["field1name", "field2name", "field3name"]]
         - condDict -> conditions for the query
                       key -> name of the field
                       value -> list of possible values
         - groupFields -> list of fields to group by
                          ( "%s, %s, %s", ( "field1name", "field2name", "field3name" ) )
         - orderFields -> list of fields to order by
                          ( "%s, %s, %s", ( "field1name", "field2name", "field3name" ) )

        """
        if typeName not in self.dbCatalog:
            return S_ERROR(f"Type {typeName} is not defined")
        if len(selectFields) < 2:
            return S_ERROR("selectFields has to be a list containing a string and a list of fields")
        retVal = self.__checkIncomingFieldsForQuery(
            typeName, selectFields, condDict, groupFields, orderFields, "bucket"
        )
        if not retVal["OK"]:
            return retVal
        nowEpoch = TimeUtilities.toEpoch()
        bucketTimeLength = self.calculateBucketLengthForTime(typeName, nowEpoch, startTime)
        startTime = startTime - startTime % bucketTimeLength
        result = self.__queryType(
            typeName, startTime, endTime, selectFields, condDict, groupFields, orderFields, "bucket", connObj=connObj
        )
        return result

    def __queryType(
        self, typeName, startTime, endTime, selectFields, condDict, groupFields, orderFields, tableType, connObj=False
    ):
        """
        Execute a query over a main table
        """

        tableName = _getTableName(tableType, typeName)
        cmd = "SELECT"
        sqlLinkList = []
        # Check if groupFields and orderFields are in ( "%s", ( field1, ) ) form
        if groupFields:
            try:
                groupFields[0] % tuple(groupFields[1])
                # We can have the case when we have multiple grouping and the fields in the select
                # does not much the group by conditions
                # for example: selectFields = ('%s, %s, %s, SUM(%s)', ['Site', 'startTime', 'bucketLength', 'entriesInBucket'])
                #             groupFields = ('%s, %s', ['startTime', 'Site'])
                #             in this case the correct query must be: select Site, startTime, bucketlength,
                #                               sum(entriesInBucket) from xxxx where yyy Group by Site, startTime, bucketlength
                #
                # When we have multiple grouping then we must have all the fields in Group by. This is from mysql 5.7.
                # We have fields which are not in the groupFields and it is in selectFields

                if "bucketLength" in selectFields[1]:
                    groupFields = list(groupFields)
                    groupFields[0] = f"{groupFields[0]}, %s"
                    groupFields[1].append("bucketlength")
                    groupFields = tuple(groupFields)

            except TypeError as e:
                return S_ERROR(f"Cannot format properly group string: {repr(e)}")
        if orderFields:
            try:
                orderFields[0] % tuple(orderFields[1])
            except TypeError as e:
                return S_ERROR(f"Cannot format properly order string: {repr(e)}")
        # Calculate fields to retrieve
        realFieldList = []
        for rawFieldName in selectFields[1]:
            keyTable = _getTableName("key", typeName, rawFieldName)
            if rawFieldName in self.dbCatalog[typeName]["keys"]:
                realFieldList.append(f"`{keyTable}`.`value`")
                List.appendUnique(sqlLinkList, f"`{tableName}`.`{rawFieldName}` = `{keyTable}`.`id`")
            else:
                realFieldList.append(f"`{tableName}`.`{rawFieldName}`")
        try:
            cmd += f" {selectFields[0]}" % tuple(realFieldList)
        except TypeError as e:
            return S_ERROR(f"Error generating select fields string: {repr(e)}")
        # Calculate tables needed
        sqlFromList = [f"`{tableName}`"]
        for key in self.dbCatalog[typeName]["keys"]:
            if (
                key in condDict
                or key in selectFields[1]
                or (groupFields and key in groupFields[1])
                or (orderFields and key in orderFields[1])
            ):
                sqlFromList.append(f"`{_getTableName('key', typeName, key)}`")
        cmd += f" FROM {', '.join(sqlFromList)}"
        # Calculate time conditions
        sqlTimeCond = []
        if startTime:
            if tableType == "bucket":
                # HACK because MySQL and UNIX do not start epoch at the same time
                startTime = startTime + 3600
                startTime = self.calculateBuckets(typeName, startTime, startTime)[0][0]
            sqlTimeCond.append(f"`{tableName}`.`startTime` >= {startTime}")
        if endTime:
            if tableType == "bucket":
                endTimeSQLVar = "startTime"
                endTime = endTime + 3600
                endTime = self.calculateBuckets(typeName, endTime, endTime)[0][0]
            else:
                endTimeSQLVar = "endTime"
            sqlTimeCond.append(f"`{tableName}`.`{endTimeSQLVar}` <= {endTime}")
        cmd += f" WHERE {' AND '.join(sqlTimeCond)}"
        # Calculate conditions
        sqlCondList = []
        for keyName in condDict:
            sqlORList = []
            if keyName in self.dbCatalog[typeName]["keys"]:
                List.appendUnique(
                    sqlLinkList,
                    f"`{tableName}`.`{keyName}` = `{_getTableName('key', typeName, keyName)}`.`id`",
                )
            if not isinstance(condDict[keyName], (list, tuple)):
                condDict[keyName] = [condDict[keyName]]
            for keyValue in condDict[keyName]:
                retVal = self._escapeString(keyValue)
                if not retVal["OK"]:
                    return retVal
                keyValue = retVal["Value"]
                if keyName in self.dbCatalog[typeName]["keys"]:
                    sqlORList.append(f"`{_getTableName('key', typeName, keyName)}`.`value` = {keyValue}")
                else:
                    sqlORList.append(f"`{tableName}`.`{keyName}` = {keyValue}")
            if sqlORList:
                sqlCondList.append(f"( {' OR '.join(sqlORList)} )")
        if sqlCondList:
            cmd += f" AND {' AND '.join(sqlCondList)}"
        # Calculate grouping and sorting
        for preGenFields in (groupFields, orderFields):
            if preGenFields:
                for i, field in enumerate(preGenFields[1]):
                    if field in self.dbCatalog[typeName]["keys"]:
                        List.appendUnique(
                            sqlLinkList,
                            f"`{tableName}`.`{field}` = `{_getTableName('key', typeName, field)}`.`id`",
                        )
                        if preGenFields[0] != "%s":
                            # The default grouping was changed
                            preGenFields[1][i] = f"`{_getTableName('key', typeName, field)}`.Value"
                        else:
                            # The default grouping is maintained
                            preGenFields[1][i] = f"`{tableName}`.`{field}`"
                    elif field in ["bucketLength", "entriesInBucket"]:  # these are not in the dbCatalog
                        preGenFields[1][i] = f"`{tableName}`.`{field}`"

        if sqlLinkList:
            cmd += f" AND {' AND '.join(sqlLinkList)}"
        if groupFields:
            if len(groupFields[1]) == 1:
                testGroupFields = f" {selectFields[0]}" % tuple(realFieldList)
                testGroupFieldsList = testGroupFields.split(",")
                realGroupFields = ()
                for testGroupFields in testGroupFieldsList:
                    if "sum" not in testGroupFields.lower():
                        realGroupFields += (testGroupFields.strip(),)
                cmd += " GROUP BY " + ",".join(realGroupFields)
            else:
                cmd += f" GROUP BY {groupFields[0] % tuple(groupFields[1])}"
        if orderFields:
            cmd += f" ORDER BY {orderFields[0] % tuple(orderFields[1])}"
        return self._query(cmd, conn=connObj)

    def compactBuckets(self, typeFilter=False):
        """
        Compact buckets for all defined types
        """
        if self.__readOnly:
            return S_ERROR("ReadOnly mode enabled. No modification allowed")
        gSynchro.lock()
        try:
            if self.__doingCompaction:
                return S_OK()
            self.__doingCompaction = True
        finally:
            gSynchro.unlock()
        slow = True
        for typeName in self.dbCatalog:
            if typeFilter and typeName.find(typeFilter) == -1:
                self.log.info(f"[COMPACT] Skipping {typeName}")
                continue
            if self.dbCatalog[typeName]["dataTimespan"] > 0:
                self.log.info(f"[COMPACT] Deleting records older that timespan for type {typeName}")
                self.__deleteRecordsOlderThanDataTimespan(typeName)
            self.log.info(f"[COMPACT] Compacting {typeName}")
            if slow:
                self.__slowCompactBucketsForType(typeName)
            else:
                self.__compactBucketsForType(typeName)
        self.log.info("[COMPACT] Compaction finished")
        self.__lastCompactionEpoch = int(TimeUtilities.toEpoch())
        gSynchro.lock()
        try:
            if self.__doingCompaction:
                self.__doingCompaction = False
        finally:
            gSynchro.unlock()
        return S_OK()

    def __selectForCompactBuckets(self, typeName, timeLimit, bucketLength, nextBucketLength, connObj=False):
        """
        Nasty SQL query to get ideal buckets using grouping by date calculations and adding value contents
        """
        tableName = _getTableName("bucket", typeName)
        selectSQL = "SELECT "
        sqlSelectList = []
        for field in self.dbCatalog[typeName]["keys"]:
            sqlSelectList.append(f"`{tableName}`.`{field}`")
        for field in self.dbCatalog[typeName]["values"]:
            sqlSelectList.append(f"SUM( `{tableName}`.`{field}` )")
        sqlSelectList.append(f"SUM( `{tableName}`.`entriesInBucket` )")
        sqlSelectList.append(f"MIN( `{tableName}`.`startTime` )")
        sqlSelectList.append(f"MAX( `{tableName}`.`startTime` )")
        selectSQL += ", ".join(sqlSelectList)
        selectSQL += f" FROM `{tableName}`"
        selectSQL += f" WHERE `{tableName}`.`startTime` < '{timeLimit}' AND"
        selectSQL += f" `{tableName}`.`bucketLength` = {bucketLength}"
        # MAGIC bucketing
        sqlGroupList = [_bucketizeDataField(f"`{tableName}`.`startTime`", nextBucketLength)]
        for field in self.dbCatalog[typeName]["keys"]:
            sqlGroupList.append(f"`{tableName}`.`{field}`")
        selectSQL += f" GROUP BY {', '.join(sqlGroupList)}"
        return self._query(selectSQL, conn=connObj)

    def __deleteForCompactBuckets(self, typeName, timeLimit, bucketLength, connObj=False):
        """
        Delete compacted buckets
        """
        tableName = _getTableName("bucket", typeName)
        deleteSQL = f"DELETE FROM `{tableName}` WHERE "
        deleteSQL += f"`{tableName}`.`startTime` < '{timeLimit}' AND "
        deleteSQL += f"`{tableName}`.`bucketLength` = {bucketLength}"
        return self._update(deleteSQL, conn=connObj)

    def __compactBucketsForType(self, typeName):
        """
        Compact all buckets for a given type
        """
        nowEpoch = TimeUtilities.toEpoch()
        # retVal = self.__startTransaction(connObj)
        # if not retVal[ 'OK' ]:
        #  return retVal
        for bPos in range(len(self.dbBucketsLength[typeName]) - 1):
            self.log.info("[COMPACT] Query %d of %d" % (bPos + 1, len(self.dbBucketsLength[typeName]) - 1))
            secondsLimit = self.dbBucketsLength[typeName][bPos][0]
            bucketLength = self.dbBucketsLength[typeName][bPos][1]
            timeLimit = (nowEpoch - nowEpoch % bucketLength) - secondsLimit
            nextBucketLength = self.dbBucketsLength[typeName][bPos + 1][1]
            self.log.info(
                "[COMPACT] Compacting data newer that %s with bucket size %s"
                % (TimeUtilities.fromEpoch(timeLimit), bucketLength)
            )
            # Retrieve the data
            retVal = self.__selectForCompactBuckets(typeName, timeLimit, bucketLength, nextBucketLength)
            if not retVal["OK"]:
                # self.__rollbackTransaction(connObj)
                return retVal
            bucketsData = retVal["Value"]
            self.log.info(f"[COMPACT] Got {len(bucketsData)} records to compact")
            if len(bucketsData) == 0:
                continue
            retVal = self.__deleteForCompactBuckets(typeName, timeLimit, bucketLength)
            if not retVal["OK"]:
                # self.__rollbackTransaction( connObj )
                return retVal
            self.log.info(f"[COMPACT] Compacting {len(bucketsData)} records {bucketLength} seconds size for {typeName}")
            # Add data
            for record in bucketsData:
                startTime = record[-2]
                endTime = record[-1]
                valuesList = record[:-2]
                retVal = self.__splitInBuckets(typeName, startTime, endTime, valuesList)
                if not retVal["OK"]:
                    # self.__rollbackTransaction( connObj )
                    self.log.error("[COMPACT] Error while compacting data for record", f"{typeName}: {retVal['Value']}")
            self.log.info("[COMPACT] Finished compaction %d of %d" % (bPos, len(self.dbBucketsLength[typeName]) - 1))
        # return self.__commitTransaction( connObj )
        return S_OK()

    def __slowCompactBucketsForType(self, typeName):
        """
        Compact all buckets for a given type
        """
        nowEpoch = TimeUtilities.toEpoch()
        for bPos in range(len(self.dbBucketsLength[typeName]) - 1):
            self.log.info("[COMPACT] Query %d of %d" % (bPos, len(self.dbBucketsLength[typeName]) - 1))
            secondsLimit = self.dbBucketsLength[typeName][bPos][0]
            bucketLength = self.dbBucketsLength[typeName][bPos][1]
            timeLimit = (nowEpoch - nowEpoch % bucketLength) - secondsLimit
            self.log.info(
                "[COMPACT] Compacting data newer that %s with bucket size %s for %s"
                % (TimeUtilities.fromEpoch(timeLimit), bucketLength, typeName)
            )
            querySize = 10000
            previousRecordsSelected = querySize
            totalCompacted = 0
            while previousRecordsSelected == querySize:
                # Retrieve the data
                self.log.info(
                    "[COMPACT] Retrieving buckets to compact newer that %s with size %s"
                    % (TimeUtilities.fromEpoch(timeLimit), bucketLength)
                )
                roundStartTime = time.time()
                result = self.__selectIndividualForCompactBuckets(typeName, timeLimit, bucketLength, querySize)
                if not result["OK"]:
                    # self.__rollbackTransaction( connObj )
                    return result
                bucketsData = result["Value"]
                previousRecordsSelected = len(bucketsData)
                selectEndTime = time.time()
                self.log.info(
                    "[COMPACT] Got %d buckets (%d done) (took %.2f secs)"
                    % (previousRecordsSelected, totalCompacted, selectEndTime - roundStartTime)
                )
                if len(bucketsData) == 0:
                    break

                result = self.__deleteIndividualForCompactBuckets(typeName, bucketsData)
                if not result["OK"]:
                    # self.__rollbackTransaction(connObj)
                    return result
                bucketsData = result["Value"]
                deleteEndTime = time.time()
                self.log.info(
                    "[COMPACT] Deleted %s out-of-bounds buckets (took %.2f secs)"
                    % (len(bucketsData), deleteEndTime - selectEndTime)
                )
                # Add data
                for record in bucketsData:
                    startTime = record[-2]
                    endTime = record[-2] + record[-1]
                    valuesList = record[:-2]
                    retVal = self.__splitInBuckets(typeName, startTime, endTime, valuesList)
                    if not retVal["OK"]:
                        self.log.error(
                            "[COMPACT] Error while compacting data for buckets",
                            f"{typeName}: {retVal['Value']}",
                        )
                totalCompacted += len(bucketsData)
                insertElapsedTime = time.time() - deleteEndTime
                self.log.info(
                    "[COMPACT] Records compacted (took %.2f secs, %.2f secs/bucket)"
                    % (insertElapsedTime, insertElapsedTime / len(bucketsData))
                )
            self.log.info("[COMPACT] Finised compaction %d of %d" % (bPos, len(self.dbBucketsLength[typeName]) - 1))
        # return self.__commitTransaction(connObj)
        return S_OK()

    def __selectIndividualForCompactBuckets(self, typeName, timeLimit, bucketLength, querySize, connObj=False):
        """
        Nasty SQL query to get ideal buckets using grouping by date calculations and adding value contents
        """
        tableName = _getTableName("bucket", typeName)
        selectSQL = "SELECT "
        sqlSelectList = []
        for field in self.dbCatalog[typeName]["keys"]:
            sqlSelectList.append(f"`{tableName}`.`{field}`")
        for field in self.dbCatalog[typeName]["values"]:
            sqlSelectList.append(f"`{tableName}`.`{field}`")
        sqlSelectList.append(f"`{tableName}`.`entriesInBucket`")
        sqlSelectList.append(f"`{tableName}`.`startTime`")
        sqlSelectList.append(f"`{tableName}`.bucketLength")
        selectSQL += ", ".join(sqlSelectList)
        selectSQL += f" FROM `{tableName}`"
        selectSQL += f" WHERE `{tableName}`.`startTime` < '{timeLimit}' AND"
        selectSQL += f" `{tableName}`.`bucketLength` = {bucketLength}"
        # MAGIC bucketing
        selectSQL += " LIMIT %d" % querySize
        return self._query(selectSQL, conn=connObj)

    def __deleteIndividualForCompactBuckets(self, typeName, bucketsData, connObj=False):
        """
        Delete compacted buckets
        """
        tableName = _getTableName("bucket", typeName)
        keyFields = self.dbCatalog[typeName]["keys"]
        deleteQueryLimit = 50
        deletedBuckets = []
        for bLimit in range(0, len(bucketsData), deleteQueryLimit):
            delCondsSQL = []
            for record in bucketsData[bLimit : bLimit + deleteQueryLimit]:
                condSQL = []
                for iPos, field in enumerate(keyFields):
                    condSQL.append(f"`{tableName}`.`{field}` = {record[iPos]}")
                condSQL.append("`%s`.`startTime` = %d" % (tableName, record[-2]))
                condSQL.append("`%s`.`bucketLength` = %d" % (tableName, record[-1]))
                delCondsSQL.append(f"({' AND '.join(condSQL)})")
            delSQL = f"DELETE FROM `{tableName}` WHERE {' OR '.join(delCondsSQL)}"
            result = self._update(delSQL, conn=connObj)
            if not result["OK"]:
                self.log.error("Cannot delete individual records for compaction", result["Message"])
            else:
                deletedBuckets.extend(bucketsData[bLimit : bLimit + deleteQueryLimit])
        return S_OK(deletedBuckets)

    def __deleteRecordsOlderThanDataTimespan(self, typeName):
        """
        IF types define dataTimespan, then records older than datatimespan seconds will be deleted
        automatically
        """
        dataTimespan = self.dbCatalog[typeName]["dataTimespan"] + self.dbBucketsLength[typeName][-1][1]
        if dataTimespan < 86400 * 30:
            return
        for table, field in (
            (_getTableName("type", typeName), "endTime"),
            (_getTableName("bucket", typeName), "startTime"),
        ):
            self.log.info(f"[COMPACT] Deleting old records for table {table}")
            deleteLimit = 100000
            deleted = deleteLimit
            while deleted >= deleteLimit:
                sqlCmd = "DELETE FROM `%s` WHERE %s < UNIX_TIMESTAMP()-%d LIMIT %d" % (
                    table,
                    field,
                    dataTimespan,
                    deleteLimit,
                )
                result = self._update(sqlCmd)
                if not result["OK"]:
                    self.log.error(
                        "[COMPACT] Cannot delete old records",
                        f"Table: {table} Timespan: {dataTimespan} Error: {result['Message']}",
                    )
                    break
                self.log.info("[COMPACT] Deleted %d records for %s table" % (result["Value"], table))
                deleted = result["Value"]
                time.sleep(1)

    def regenerateBuckets(self, typeName):
        if self.__readOnly:
            return S_ERROR("ReadOnly mode enabled. No modification allowed")
        # Delete old entries if any
        if self.dbCatalog[typeName]["dataTimespan"] > 0:
            self.log.info(f"[REBUCKET] Deleting records older that timespan for type {typeName}")
            self.__deleteRecordsOlderThanDataTimespan(typeName)
            self.log.info("[REBUCKET] Done deleting old records")
        rawTableName = _getTableName("type", typeName)
        # retVal = self.__startTransaction(connObj)
        # if not retVal[ 'OK' ]:
        #  return retVal
        self.log.info(f"[REBUCKET] Deleting buckets for {typeName}")
        retVal = self._update(f"DELETE FROM `{_getTableName('bucket', typeName)}`")
        if not retVal["OK"]:
            return retVal
        # Generate the common part of the query
        # SELECT fields
        startTimeTableField = f"`{rawTableName}`.startTime"
        endTimeTableField = f"`{rawTableName}`.endTime"
        # Select strings and sum select strings
        sqlSUMSelectList = []
        sqlSelectList = []
        for field in self.dbCatalog[typeName]["keys"]:
            sqlSUMSelectList.append(f"`{rawTableName}`.`{field}`")
            sqlSelectList.append(f"`{rawTableName}`.`{field}`")
        for field in self.dbCatalog[typeName]["values"]:
            sqlSUMSelectList.append(f"SUM( `{rawTableName}`.`{field}` )")
            sqlSelectList.append(f"`{rawTableName}`.`{field}`")
        sumSelectString = ", ".join(sqlSUMSelectList)
        selectString = ", ".join(sqlSelectList)
        # Grouping fields
        sqlGroupList = []
        for field in self.dbCatalog[typeName]["keys"]:
            sqlGroupList.append(f"`{rawTableName}`.`{field}`")
        groupingString = ", ".join(sqlGroupList)
        # List to contain all queries
        sqlQueries = []
        dateInclusiveConditions = []
        countedField = f"`{rawTableName}`.`{self.dbCatalog[typeName]['keys'][0]}`"
        lastTime = TimeUtilities.toEpoch()
        # Iterate for all ranges
        for iRange, iValue in enumerate(self.dbBucketsLength[typeName]):
            bucketTimeSpan = iValue[0]
            bucketLength = iValue[1]
            startRangeTime = lastTime - bucketTimeSpan
            endRangeTime = lastTime
            lastTime -= bucketTimeSpan
            bucketizedStart = _bucketizeDataField(startTimeTableField, bucketLength)
            bucketizedEnd = _bucketizeDataField(endTimeTableField, bucketLength)

            timeSelectString = f"MIN({startTimeTableField}), MAX({endTimeTableField})"
            # Is the last bucket?
            if iRange == len(self.dbBucketsLength[typeName]) - 1:
                whereString = "%s <= %d" % (endTimeTableField, endRangeTime)
            else:
                whereString = "%s > %d AND %s <= %d" % (
                    startTimeTableField,
                    startRangeTime,
                    endTimeTableField,
                    endRangeTime,
                )
            sameBucketCondition = f"({bucketizedStart}) = ({bucketizedEnd})"
            # Records that fit in a bucket
            sqlQuery = "SELECT {}, {}, COUNT({}) FROM `{}` WHERE {} AND {} GROUP BY {}, {}".format(
                timeSelectString,
                sumSelectString,
                countedField,
                rawTableName,
                whereString,
                sameBucketCondition,
                groupingString,
                bucketizedStart,
            )
            sqlQueries.append(sqlQuery)
            # Records that fit in more than one bucket
            sqlQuery = "SELECT {}, {}, {}, 1 FROM `{}` WHERE {} AND NOT {}".format(
                startTimeTableField,
                endTimeTableField,
                selectString,
                rawTableName,
                whereString,
                sameBucketCondition,
            )
            sqlQueries.append(sqlQuery)
            dateInclusiveConditions.append(f"( {whereString} )")
        # Query for records that are in between two ranges
        sqlQuery = "SELECT {}, {}, {}, 1 FROM `{}` WHERE NOT {}".format(
            startTimeTableField,
            endTimeTableField,
            selectString,
            rawTableName,
            " AND NOT ".join(dateInclusiveConditions),
        )
        sqlQueries.append(sqlQuery)
        self.log.info(f"[REBUCKET] Retrieving data for rebuilding buckets for type {typeName}...")
        queryNum = 0
        for sqlQuery in sqlQueries:
            self.log.info(f"[REBUCKET] Executing query #{queryNum}...")
            queryNum += 1
            retVal = self._query(sqlQuery)
            if not retVal["OK"]:
                self.log.error("[REBUCKET] Can't retrieve data for rebucketing", retVal["Message"])
                # self.__rollbackTransaction(connObj)
                return retVal
            rawData = retVal["Value"]
            self.log.info(f"[REBUCKET] Retrieved {len(rawData)} records")
            rebucketedRecords = 0
            startQuery = time.time()
            startBlock = time.time()
            numRecords = len(rawData)
            for entry in rawData:
                startT = entry[0]
                endT = entry[1]
                values = entry[2:]
                retVal = self.__splitInBuckets(typeName, startT, endT, values)
                if not retVal["OK"]:
                    # self.__rollbackTransaction(connObj)
                    return retVal
                rebucketedRecords += 1
                if rebucketedRecords % 1000 == 0:
                    queryAvg = rebucketedRecords / float(time.time() - startQuery)
                    blockAvg = 1000 / float(time.time() - startBlock)
                    startBlock = time.time()
                    perDone = 100 * rebucketedRecords / float(numRecords)
                    expectedEnd = str(datetime.timedelta(seconds=int((numRecords - rebucketedRecords) / blockAvg)))
                    self.log.info(
                        "[REBUCKET] Rebucketed %.2f%% %s (%.2f r/s block %.2f r/s query | ETA %s )..."
                        % (perDone, typeName, blockAvg, queryAvg, expectedEnd)
                    )
        # return self.__commitTransaction(connObj)
        return S_OK()

    def __startTransaction(self, connObj):
        return self._query("START TRANSACTION", conn=connObj)

    def __commitTransaction(self, connObj):
        return self._query("COMMIT", conn=connObj)

    def __rollbackTransaction(self, connObj):
        return self._query("ROLLBACK", conn=connObj)


def _bucketizeDataField(dataField, bucketLength):
    return f"{dataField} - ( {dataField} % {bucketLength} )"


def _getTableName(tableType, typeName, keyName=None):
    """
    Generate table name
    """
    if not keyName:
        return f"ac_{tableType}_{typeName}"
    elif tableType == "key":
        return f"ac_{tableType}_{typeName}_{keyName}"
    else:
        raise Exception("Call to _getTableName with tableType as key but with no keyName")
