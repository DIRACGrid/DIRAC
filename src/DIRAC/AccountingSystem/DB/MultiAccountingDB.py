""" Module for handling AccountingDB tables on multiple DBs (e.g. 2 MySQL servers)
"""
from DIRAC import gConfig, S_OK, gLogger
from DIRAC.Core.Utilities.Plotting.TypeLoader import TypeLoader
from DIRAC.AccountingSystem.DB.AccountingDB import AccountingDB


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
        types = self.__allDBs[self.__defaultDB].getRegisteredTypes()
        result = gConfig.getOptionsDict(self.__csPath)
        if not result["OK"]:
            gLogger.verbose("No extra databases defined", "in %s" % self.__csPath)
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
                    dbName = "Accounting/%s" % dbName
                gLogger.notice("Creating DB", "%s" % dbName)
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

    def __mimeTypeMethod(self, methodName, setup, acType, *args):
        return getattr(self.__db(acType), methodName)(f"{setup}_{acType}", *args)

    def __mimeMethod(self, methodName, *args):
        end = S_OK()
        for dbName in self.__allDBs:
            res = getattr(self.__allDBs[dbName], methodName)(*args)
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
            recByType[acType].append((f"{record[0]}_{record[1]}", record[2], record[3], record[4]))
        end = S_OK()
        for acType in recByType:
            res = self.__db(acType).insertRecordBundleThroughQueue(recByType[acType])
            if not res["OK"]:
                end = res
        return end
