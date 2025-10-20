"""Module containing a front-end to the OpenSearch-based JobParametersDB.
This is a drop-in replacement for MySQL-based table JobDB.JobParameters.

The following class methods are provided for public usage
  - getJobParameters()
  - setJobParameter()
  - deleteJobParameters()
"""

from DIRAC import S_ERROR, S_OK
from DIRAC.Core.Base.ElasticDB import ElasticDB
from DIRAC.Core.Utilities import TimeUtilities

mapping = {
    "JobID": {"type": "long"},
    "timestamp": {"type": "date"},
    "PilotAgent": {"type": "keyword"},
    "Pilot_Reference": {"type": "keyword"},
    "JobGroup": {"type": "keyword"},
    "CPUNormalizationFactor": {"type": "long"},
    "NormCPUTime(s)": {"type": "long"},
    "Memory(MB)": {"type": "long"},
    "LocalAccount": {"type": "keyword"},
    "TotalCPUTime(s)": {"type": "long"},
    "PayloadPID": {"type": "long"},
    "HostName": {"type": "text"},
    "GridCE": {"type": "keyword"},
    "CEQueue": {"type": "keyword"},
    "BatchSystem": {"type": "keyword"},
    "ModelName": {"type": "keyword"},
    "Status": {"type": "keyword"},
    "JobType": {"type": "keyword"},
}


class JobParametersDB(ElasticDB):
    def __init__(self, parentLogger=None):
        """Standard Constructor"""

        self.fullname = "WorkloadManagement/JobParametersDB"
        self.index_name = self.getCSOption("index_name", "job_parameters")

        try:
            # Connecting to the OpenSearch cluster
            super().__init__(self.fullname, self.index_name, parentLogger=parentLogger)
        except Exception:
            RuntimeError("Can't connect to JobParameters index")
        self.addIndexTemplate("elasticjobparametersdb", index_patterns=[f"{self.index_name}_*"], mapping=mapping)

    def _indexName(self, jobID: int, vo: str) -> str:
        """construct the index name

        :param jobID: Job ID
        """
        indexSplit = int(int(jobID) // 1e6)
        return f"{self.index_name}_{vo}_{indexSplit}m"

    def _createIndex(self, indexName: str) -> None:
        """Create a new index if needed

        :param indexName: index name
        """
        # Verifying if the index is there, and if not create it
        res = self.existingIndex(indexName)
        if not res["OK"] or not res["Value"]:
            result = self.createIndex(indexName, period=None)
            if not result["OK"]:
                self.log.error(result["Message"])
                raise RuntimeError(result["Message"])
            self.log.always("Index created:", indexName)

    def getJobParameters(self, jobIDs: int | list[int], vo: str, paramList: list[str] | None = None) -> dict:
        """Get Job Parameters defined for jobID.
          Returns a dictionary with the Job Parameters.
          If paramList is empty - all the parameters are returned.

        :param self: self reference
        :param jobID: Job ID
        :param paramList: list of parameters to be returned (also a string is treated)
        :return: dict with all Job Parameter values
        """
        if isinstance(jobIDs, int):
            jobIDs = [jobIDs]
        if isinstance(paramList, str):
            paramList = paramList.replace(" ", "").split(",")
        self.log.debug(f"JobDB.getParameters: Getting Parameters for jobs {jobIDs}")

        res = self.getDocs(self._indexName, jobIDs, vo)
        if not res["OK"]:
            return res
        result = {}
        for job_id, doc in res["Value"].items():
            if paramList:
                result[job_id] = {k: v for k, v in doc.items() if k in paramList}
            else:
                result[job_id] = doc

        return S_OK(result)

    def setJobParameter(self, jobID: int, key: str, value: str, vo: str) -> dict:
        """
        Inserts data into JobParametersDB index

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
        if self.existsDoc(self._indexName(jobID, vo), docID=str(jobID)):
            self.log.debug("A document for this job already exists, it will now be updated")
            result = self.updateDoc(index=self._indexName(jobID, vo), docID=str(jobID), body={"doc": data})
        else:
            self.log.debug("No document has this job id, creating a new document for this job")
            self._createIndex(self._indexName(jobID, vo))
            result = self.index(indexName=self._indexName(jobID, vo), body=data, docID=str(jobID))
        if not result["OK"]:
            self.log.error("Couldn't insert or update data", result["Message"])
        return result

    def setJobParameters(self, jobID: int, parameters: list, vo: str) -> dict:
        """
        Inserts data into JobParametersDB index using bulk indexing

        :param self: self reference
        :param jobID: Job ID
        :param parameters: list of tuples (name, value) pairs
        :returns: S_OK/S_ERROR as result of indexing
        """
        self.log.debug("Inserting parameters", f"in {self._indexName(jobID, vo)}: for job {jobID}: {parameters}")

        parametersDict = dict(parameters)
        parametersDict["JobID"] = jobID
        parametersDict["timestamp"] = int(TimeUtilities.toEpochMilliSeconds())

        if self.existsDoc(self._indexName(jobID, vo), docID=str(jobID)):
            self.log.debug("A document for this job already exists, it will now be updated")
            result = self.updateDoc(index=self._indexName(jobID, vo), docID=str(jobID), body={"doc": parametersDict})
        else:
            self.log.debug("Creating a new document for this job")
            self._createIndex(self._indexName(jobID, vo))
            result = self.index(self._indexName(jobID, vo), body=parametersDict, docID=str(jobID))
        if not result["OK"]:
            self.log.error("Couldn't insert or update data", result["Message"])
        return result

    def deleteJobParameters(self, jobID: int, paramList=None, vo: str = "") -> dict:
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
            result = self.deleteDoc(self._indexName(jobID, vo), docID=str(jobID))
        else:
            # Deleting the specific parameters
            self.log.debug(f"JobDB.getParameters: Deleting Parameters {paramList} for job {jobID}")
            for paramName in paramList:
                result = self.updateDoc(
                    index=self._indexName(jobID, vo),
                    docID=str(jobID),
                    body={"script": "ctx._source.remove('" + paramName + "')"},
                )
                self.log.debug(f"Deleted parameter {paramName}")
        if not result["OK"]:
            return S_ERROR(result)
        self.log.debug("Parameters successfully deleted.")
        return S_OK()

    # TODO: Add query by value (e.g. query which values are in a certain pattern)
