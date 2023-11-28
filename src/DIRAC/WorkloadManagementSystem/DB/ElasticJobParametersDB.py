""" Module containing a front-end to the ElasticSearch-based ElasticJobParametersDB.
    This is a drop-in replacement for MySQL-based table JobDB.JobParameters.

    The reason for switching to a ES-based JobParameters lies in the extended searching
    capabilities of ES.
    This results in higher traceability for DIRAC jobs.

    The following class methods are provided for public usage
      - getJobParameters()
      - setJobParameter()
      - deleteJobParameters()
"""
from DIRAC import S_ERROR, S_OK
from DIRAC.ConfigurationSystem.Client.Helpers import CSGlobals
from DIRAC.Core.Base.ElasticDB import ElasticDB
from DIRAC.Core.Utilities import TimeUtilities


mapping = {
    "properties": {
        "JobID": {"type": "long"},
        "timestamp": {"type": "date"},
        "CPUNormalizationFactor": {"type": "long"},
        "NormCPUTime(s)": {"type": "long"},
        "Memory(kB)": {"type": "long"},
        "TotalCPUTime(s)": {"type": "long"},
        "MemoryUsed(kb)": {"type": "long"},
        "HostName": {"type": "keyword"},
        "GridCE": {"type": "keyword"},
        "ModelName": {"type": "keyword"},
        "Status": {"type": "keyword"},
        "JobType": {"type": "keyword"},
    }
}


class ElasticJobParametersDB(ElasticDB):
    def __init__(self, parentLogger=None):
        """Standard Constructor"""

        try:
            indexPrefix = CSGlobals.getSetup().lower()

            # Connecting to the ES cluster
            super().__init__("WorkloadManagement/ElasticJobParametersDB", indexPrefix, parentLogger=parentLogger)
        except Exception as ex:
            self.log.error("Can't connect to ElasticJobParametersDB", repr(ex))
            raise RuntimeError("Can't connect to ElasticJobParametersDB") from ex

        self.indexName_base = f"{self.getIndexPrefix()}_elasticjobparameters_index"

    def _indexName(self, jobID: int) -> str:
        """construct the index name

        :param jobID: Job ID
        """
        indexSplit = int(jobID) // 1e6
        return f"{self.indexName_base}_{indexSplit}m"

    def _createIndex(self, indexName: str) -> None:
        """Create a new index if needed

        :param indexName: index name
        """
        # Verifying if the index is there, and if not create it
        res = self.existingIndex(indexName)
        if not res["OK"] or not res["Value"]:
            result = self.createIndex(indexName, mapping, period=None)
            if not result["OK"]:
                self.log.error(result["Message"])
                raise RuntimeError(result["Message"])
            self.log.always("Index created:", indexName)

    def getJobParameters(self, jobID: int, paramList=None) -> dict:
        """Get Job Parameters defined for jobID.
          Returns a dictionary with the Job Parameters.
          If paramList is empty - all the parameters are returned.

        :param self: self reference
        :param jobID: Job ID
        :param paramList: list of parameters to be returned (also a string is treated)
        :return: dict with all Job Parameter values
        """
        if isinstance(paramList, str):
            paramList = paramList.replace(" ", "").split(",")
        self.log.debug(f"JobDB.getParameters: Getting Parameters for job {jobID}")

        res = self.getDoc(self._indexName(jobID), str(jobID))
        if not res["OK"]:
            return res
        resultDict = res["Value"]
        if paramList:
            for k in list(resultDict):
                if k not in paramList:
                    resultDict.pop(k)

        return S_OK({jobID: resultDict})

    def setJobParameter(self, jobID: int, key: str, value: str) -> dict:
        """
        Inserts data into ElasticJobParametersDB index

        :param self: self reference
        :param jobID: Job ID
        :param key: parameter key
        :param value: parameter value
        :returns: S_OK/S_ERROR as result of indexing
        """
        data = {"JobID": jobID, key: value, "timestamp": TimeUtilities.toEpochMilliSeconds()}

        self.log.debug("Inserting data in {self.indexName}:{data}")

        # The _id in ES can't exceed 512 bytes, this is a ES hard-coded limitation.

        # If a record with this jobID update and add parameter, otherwise create a new record
        if self.existsDoc(self._indexName(jobID), docID=str(jobID)):
            self.log.debug("A document for this job already exists, it will now be updated")
            result = self.updateDoc(index=self._indexName(jobID), docID=str(jobID), body={"doc": data})
        else:
            self.log.debug("No document has this job id, creating a new document for this job")
            self._createIndex(self._indexName(jobID))
            result = self.index(indexName=self._indexName(jobID), body=data, docID=str(jobID))
        if not result["OK"]:
            self.log.error("Couldn't insert or update data", result["Message"])
        return result

    def setJobParameters(self, jobID: int, parameters: list) -> dict:
        """
        Inserts data into ElasticJobParametersDB index using bulk indexing

        :param self: self reference
        :param jobID: Job ID
        :param parameters: list of tuples (name, value) pairs
        :returns: S_OK/S_ERROR as result of indexing
        """
        self.log.debug("Inserting parameters", f"in {self._indexName(jobID)}: for job {jobID}: {parameters}")

        parametersDict = dict(parameters)
        parametersDict["JobID"] = jobID
        parametersDict["timestamp"] = int(TimeUtilities.toEpochMilliSeconds())

        if self.existsDoc(self._indexName(jobID), docID=str(jobID)):
            self.log.debug("A document for this job already exists, it will now be updated")
            result = self.updateDoc(index=self._indexName(jobID), docID=str(jobID), body={"doc": parametersDict})
        else:
            self.log.debug("Creating a new document for this job")
            self._createIndex(self._indexName(jobID))
            result = self.index(self._indexName(jobID), body=parametersDict, docID=str(jobID))
        if not result["OK"]:
            self.log.error("Couldn't insert or update data", result["Message"])
        return result

    def deleteJobParameters(self, jobID: int, paramList=None) -> dict:
        """Deletes Job Parameters defined for jobID.
          Returns a dictionary with the Job Parameters.
          If paramList is empty - all the parameters for the job are removed

        :param self: self reference
        :param jobID: Job ID
        :param paramList: list of parameters to be returned (also a string is treated)
        :return: S_OK()/S_ERROR()
        """

        if isinstance(paramList, str):
            paramList = paramList.replace(" ", "").split(",")

        if not paramList:
            # Deleting the whole record
            self.log.debug("Deleting record of job {jobID}")
            result = self.deleteDoc(self._indexName(jobID), docID=str(jobID))
        else:
            # Deleting the specific parameters
            self.log.debug(f"JobDB.getParameters: Deleting Parameters {paramList} for job {jobID}")
            for paramName in paramList:
                result = self.updateDoc(
                    index=self._indexName(jobID),
                    docID=str(jobID),
                    body={"script": "ctx._source.remove('" + paramName + "')"},
                )
                self.log.debug(f"Deleted parameter {paramName}")
        if not result["OK"]:
            return S_ERROR(result)
        self.log.debug("Parameters successfully deleted.")
        return S_OK()

    # TODO: Add query by value (e.g. query which values are in a certain pattern)
