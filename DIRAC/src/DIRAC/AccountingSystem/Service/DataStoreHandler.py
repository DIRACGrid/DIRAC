""" DataStore is the service for inserting accounting reports (rows) in the Accounting DB

    This service CAN be duplicated iff the first is a "master" and all the others are slaves.
    See the information about :ref:`datastorehelpers`.

.. literalinclude:: ../ConfigTemplate.cfg
  :start-after: ##BEGIN DataStore
  :end-before: ##END
  :dedent: 2
  :caption: DataStore options
"""
import datetime

from DIRAC import S_ERROR, S_OK
from DIRAC.AccountingSystem.DB.MultiAccountingDB import MultiAccountingDB
from DIRAC.ConfigurationSystem.Client import PathFinder
from DIRAC.Core.Base.Client import Client
from DIRAC.Core.DISET.RequestHandler import RequestHandler, getServiceOption
from DIRAC.Core.Utilities import TimeUtilities
from DIRAC.Core.Utilities.ThreadScheduler import gThreadScheduler


class DataStoreHandler(RequestHandler):
    """DISET implementation of service for inserting records in accountingDB."""

    __acDB = None

    @classmethod
    def initializeHandler(cls, svcInfoDict):
        multiPath = PathFinder.getDatabaseSection("Accounting/MultiDB")
        cls.__acDB = MultiAccountingDB(multiPath)
        # we can run multiple services in read only mode. In that case we do not bucket
        cls.runBucketing = getServiceOption(svcInfoDict, "RunBucketing", True)
        if cls.runBucketing:
            cls.__acDB.autoCompactDB()
            result = cls.__acDB.markAllPendingRecordsAsNotTaken()
            if not result["OK"]:
                return result
            gThreadScheduler.addPeriodicTask(60, cls.__acDB.loadPendingRecords)
        return S_OK()

    types_registerType = [str, list, list, list]

    def export_registerType(self, typeName, definitionKeyFields, definitionAccountingFields, bucketsLength):
        """
        Register a new type. (Only for all powerful admins)
        """
        return self.__acDB.registerType(typeName, definitionKeyFields, definitionAccountingFields, bucketsLength)

    types_setBucketsLength = [str, list]

    def export_setBucketsLength(self, typeName, bucketsLength):
        """
        Change the buckets Length. (Only for all powerful admins)
        """
        return self.__acDB.changeBucketsLength(typeName, bucketsLength)

    types_regenerateBuckets = [str]

    def export_regenerateBuckets(self, typeName):
        """
        Recalculate buckets. (Only for all powerful admins)
        """
        return self.__acDB.regenerateBuckets(typeName)

    types_getRegisteredTypes = []

    def export_getRegisteredTypes(self):
        """
        Get a list of registered types (Only for all powerful admins)
        """
        return self.__acDB.getRegisteredTypes()

    types_commit = [str, datetime.datetime, datetime.datetime, list]

    def export_commit(self, typeName, startTime, endTime, valuesList):
        """
        Add a record for a type
        """
        startTime = int(TimeUtilities.toEpoch(startTime))
        endTime = int(TimeUtilities.toEpoch(endTime))
        return self.__acDB.insertRecordThroughQueue(typeName, startTime, endTime, valuesList)

    types_commitRegisters = [list]

    def export_commitRegisters(self, entriesList):
        """
        Add a record for a type
        """
        expectedTypes = [str, datetime.datetime, datetime.datetime, list]
        for entry in entriesList:
            if len(entry) != 4:
                return S_ERROR("Invalid records")
            for i, _ in enumerate(entry):
                if not isinstance(entry[i], expectedTypes[i]):
                    self.log.error(
                        "Unexpected type in report",
                        ": field %d in the records should be %s (and it is %s)" % (i, expectedTypes[i], type(entry[i])),
                    )
                    return S_ERROR("Unexpected type in report")
        records = []
        for entry in entriesList:
            startTime = int(TimeUtilities.toEpoch(entry[1]))
            endTime = int(TimeUtilities.toEpoch(entry[2]))
            self.log.debug("inserting", entry)
            records.append((entry[0], startTime, endTime, entry[3]))
        return self.__acDB.insertRecordBundleThroughQueue(records)

    types_compactDB = []

    def export_compactDB(self):
        """
        Compact the db by grouping buckets
        """
        # if we are running slaves (not only one service) we can redirect the request to the master
        # For more information please read the Administrative guide Accounting part!
        # ADVICE: If you want to trigger the bucketing, please make sure the bucketing is not running!!!!
        if self.runBucketing:
            return self.__acDB.compactBuckets()

        return Client(url="Accounting/DataStoreMaster").compactDB()

    types_remove = [str, datetime.datetime, datetime.datetime, list]

    def export_remove(self, typeName, startTime, endTime, valuesList):
        """
        Remove a record for a type
        """
        startTime = int(TimeUtilities.toEpoch(startTime))
        endTime = int(TimeUtilities.toEpoch(endTime))
        return self.__acDB.deleteRecord(typeName, startTime, endTime, valuesList)

    types_removeRegisters = [list]

    def export_removeRegisters(self, entriesList):
        """
        Remove a record for a type
        """
        expectedTypes = [str, datetime.datetime, datetime.datetime, list]
        for entry in entriesList:
            if len(entry) != 4:
                return S_ERROR("Invalid records")
            for i, en in enumerate(entry):
                if not isinstance(en, expectedTypes[i]):
                    return S_ERROR(f"{i} field in the records should be {expectedTypes[i]}")
        ok = 0
        for entry in entriesList:
            startTime = int(TimeUtilities.toEpoch(entry[1]))
            endTime = int(TimeUtilities.toEpoch(entry[2]))
            record = entry[3]
            result = self.__acDB.deleteRecord(entry[0], startTime, endTime, record)
            if not result["OK"]:
                return S_OK(ok)
            ok += 1

        return S_OK(ok)
