""" Module for handling AccountingDB tables on multiple DBs (e.g. 2 MySQL servers)
"""
from DIRAC import gConfig, S_OK, gLogger
from DIRAC.AccountingSystem.DB.AccountingDB import AccountingDB
from DIRAC.Core.Utilities.Plotting.TypeLoader import TypeLoader


class MultiAccountingDB:
    def __init__(self, csPath, readOnly=False):
        self.__csPath = csPath
        self.__readOnly = readOnly
        self.__dbByType = {}
        self.__defaultDB = "AccountingDB/AccountingDB"
        self.__log = gLogger.getSubLogger(self.__class__.__name__)
        self.__generateDBs()
        self.__registerMethods()

    def __generateDBs(self):
        self.__log.notice("Creating default AccountingDB...")
        self.__allDBs = {self.__defaultDB: AccountingDB(readOnly=self.__readOnly)}
        result = gConfig.getOptionsDict(self.__csPath)
        if not result["OK"]:
            gLogger.verbose("No extra databases defined", f"in {self.__csPath}")
            return
        validTypes = TypeLoader().getTypes()
        opts = result["Value"]
        for acType in opts:
            if acType not in validTypes:
                msg = f"({acType} defined in {self.__csPath})"
                self.__log.fatal("Not a known accounting type", msg)
                raise RuntimeError(msg)
            dbName = opts[acType]
            gLogger.notice("Type will be assigned", f"({acType} to {dbName})")
            if dbName not in self.__allDBs:
                fields = dbName.split("/")
                if len(fields) == 1:
                    dbName = f"Accounting/{dbName}"
                gLogger.notice("Creating DB", dbName)
                self.__allDBs[dbName] = AccountingDB(dbName, readOnly=self.__readOnly)
            self.__dbByType[acType] = dbName

    def __registerMethods(self):
        for methodName in (
            "registerType",
            "changeBucketsLength",
            "regenerateBuckets",
            "deleteType",
            "insertRecordThroughQueue",
            "deleteRecord",
            "getKeyValues",
            "retrieveBucketedData",
            "calculateBuckets",
            "calculateBucketLengthForTime",
        ):
            (
                lambda closure: setattr(
                    self,
                    closure,
                    lambda *x: self.__mimeTypeMethod(closure, *x),  # pylint: disable=no-value-for-parameter
                )
            )(methodName)
        for methodName in (
            "autoCompactDB",
            "compactBuckets",
            "markAllPendingRecordsAsNotTaken",
            "loadPendingRecords",
            "getRegisteredTypes",
        ):
            (lambda closure: setattr(self, closure, lambda *x: self.__mimeMethod(closure, *x)))(methodName)

    def __mimeTypeMethod(self, methodName, acType, *args):
        return getattr(self.__db(acType), methodName)(acType, *args)

    def __mimeMethod(self, methodName, *args):
        end = S_OK()
        for DB in self.__allDBs.values():
            res = getattr(DB, methodName)(*args)
            if res and not res["OK"]:
                end = res
        return end

    def __db(self, acType):
        return self.__allDBs[self.__dbByType.get(acType, self.__defaultDB)]

    def insertRecordBundleThroughQueue(self, records):
        recByType = {}
        for record in records:
            acType = record[1]
            if acType not in recByType:
                recByType[acType] = []
            recByType[acType].append((f"{record[0]}", record[1], record[2], record[3]))
        end = S_OK()
        for acType, records in recByType.items():
            res = self.__db(acType).insertRecordBundleThroughQueue(records)
            if not res["OK"]:
                end = res
        return end
