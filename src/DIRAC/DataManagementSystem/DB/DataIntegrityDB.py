""" DataIntegrityDB class is a front-end to the Data Integrity Database.
"""
from DIRAC import S_OK
from DIRAC.Core.Base.DB import DB

#############################################################################


class DataIntegrityDB(DB):
    """Only 1 table, that can be created with:

    .. code-block:: sql

        CREATE TABLE Problematics(
          FileID INTEGER NOT NULL AUTO_INCREMENT,
          Prognosis VARCHAR(32) NOT NULL,
          LFN VARCHAR(255) NOT NULL,
          PFN VARCHAR(255),
          Size BIGINT(20),
          SE VARCHAR(32),
          GUID VARCHAR(255),
          Status VARCHAR(32) DEFAULT 'New',
          Retries INTEGER DEFAULT 0,
          InsertDate DATETIME NOT NULL,
          LastUpdate DATETIME NOT NULL,
          Source VARCHAR(127) NOT NULL DEFAULT 'Unknown',
          PRIMARY KEY(FileID),
          INDEX (Prognosis,Status)
        );

    """

    def __init__(self, parentLogger=None):
        """Standard Constructor"""
        DB.__init__(self, "DataIntegrityDB", "DataManagement/DataIntegrityDB", parentLogger=parentLogger)

        self.tableName = "Problematics"
        self.fieldList = ["FileID", "LFN", "PFN", "Size", "SE", "GUID", "Prognosis"]

        retVal = self.__initializeDB()
        if not retVal["OK"]:
            raise Exception("Can't create tables: %s" % retVal["Message"])

    def __initializeDB(self):
        """Make sure the table is created"""
        result = self._query("show tables")
        if not result["OK"]:
            return result

        tablesInDB = [t[0] for t in result["Value"]]
        if self.tableName in tablesInDB:
            return S_OK()
        tablesDesc = {}
        tablesDesc[self.tableName] = {
            "Fields": {
                "FileID": "INTEGER NOT NULL AUTO_INCREMENT",
                "Prognosis": "VARCHAR(32) NOT NULL",
                "LFN": "VARCHAR(255) NOT NULL",
                "PFN": "VARCHAR(255)",
                "Size": "BIGINT(20)",
                "SE": "VARCHAR(32)",
                "GUID": "VARCHAR(255)",
                "Status": 'VARCHAR(32) DEFAULT "New"',
                "Retries": "INTEGER DEFAULT 0",
                "InsertDate": "DATETIME NOT NULL",
                "LastUpdate": "DATETIME NOT NULL",
                "Source": 'VARCHAR(127) NOT NULL DEFAULT "Unknown"',
            },
            "PrimaryKey": "FileID",
            "Indexes": {"PS": ["Prognosis", "Status"]},
            "Engine": "InnoDB",
        }

        return self._createTables(tablesDesc)

    #############################################################################
    def insertProblematic(self, source, fileMetadata):
        """Insert the supplied file metadata into the problematics table"""
        failed = {}
        successful = {}
        for lfn, metadata in fileMetadata.items():
            condDict = {key: metadata[key] for key in ["Prognosis", "PFN", "SE"]}
            condDict["LFN"] = lfn
            res = self.getFields(self.tableName, ["FileID"], condDict=condDict)
            if not res["OK"]:
                failed[lfn] = res["Message"]
            elif res["Value"]:
                successful[lfn] = "Already exists"
            else:
                metadata["LFN"] = lfn
                metadata["Source"] = source
                metadata["InsertDate"] = "UTC_TIMESTAMP()"
                metadata["LastUpdate"] = "UTC_TIMESTAMP()"
                res = self.insertFields(self.tableName, inDict=metadata)
                if res["OK"]:
                    successful[lfn] = True
                else:
                    failed[lfn] = res["Message"]
        resDict = {"Successful": successful, "Failed": failed}
        return S_OK(resDict)

    #############################################################################
    def getProblematicsSummary(self):
        """Get a summary of the current problematics table"""
        res = self.getCounters(self.tableName, ["Prognosis", "Status"], {})
        if not res["OK"]:
            return res
        resDict = {}
        for counterDict, count in res["Value"]:
            resDict.setdefault(counterDict["Prognosis"], {})
            resDict[counterDict["Prognosis"]][counterDict["Status"]] = int(count)
        return S_OK(resDict)

    #############################################################################
    def getDistinctPrognosis(self):
        """Get a list of all the current problematic types"""
        return self.getDistinctAttributeValues(self.tableName, "Prognosis")

    #############################################################################
    def getProblematic(self):
        """Get the next file to resolve"""
        res = self.getFields(
            self.tableName, self.fieldList, condDict={"Status": "New"}, limit=1, orderAttribute="LastUpdate:ASC"
        )
        if not res["OK"]:
            return res
        if not res["Value"][0]:
            return S_OK()
        valueList = list(res["Value"][0])
        return S_OK({key: valueList.pop(0) for key in self.fieldList})

    def getPrognosisProblematics(self, prognosis):
        """Get all the active files with the given problematic"""
        res = self.getFields(
            self.tableName,
            self.fieldList,
            condDict={"Prognosis": prognosis, "Status": "New"},
            orderAttribute=["Retries", "LastUpdate"],
        )
        if not res["OK"]:
            return res
        problematics = []
        for valueTuple in res["Value"]:
            valueList = list(valueTuple)
            problematics.append({key: valueList.pop(0) for key in self.fieldList})
        return S_OK(problematics)

    def getTransformationProblematics(self, transID):
        """Get problematic files matching a given production"""
        req = "SELECT LFN,FileID FROM Problematics WHERE Status = 'New' AND LFN LIKE '%%/%08d/%%';" % transID
        res = self._query(req)
        if not res["OK"]:
            return res
        problematics = {}
        for lfn, fileID in res["Value"]:
            problematics[lfn] = fileID
        return S_OK(problematics)

    def incrementProblematicRetry(self, fileID):
        """Increment retry count"""
        req = "UPDATE Problematics SET Retries=Retries+1, LastUpdate=UTC_TIMESTAMP() WHERE FileID = %s;" % (fileID)
        res = self._update(req)
        return res

    def removeProblematic(self, fileID):
        """Remove Problematic file by FileID"""
        return self.deleteEntries(self.tableName, condDict={"FileID": fileID})

    def setProblematicStatus(self, fileID, status):
        """Set Status for problematic file by FileID"""
        return self.updateFields(
            self.tableName, condDict={"FileID": fileID}, updateDict={"Status": status, "LastUpdate": "UTC_TIMESTAMP()"}
        )

    def changeProblematicPrognosis(self, fileID, newPrognosis):
        """Change prognisis for file by FileID"""
        return self.updateFields(
            self.tableName,
            condDict={"FileID": fileID},
            updateDict={"Prognosis": newPrognosis, "LastUpdate": "UTC_TIMESTAMP()"},
        )
