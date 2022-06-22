""" Module containing a front-end to the ElasticSearch-based ElasticJobParametersDB.
    This module interacts with one ES index: "ElasticJobParametersDB",
    which is a drop-in replacement for MySQL-based table JobDB.JobParameters.
    While JobDB.JobParameters in MySQL is defined as::

      CREATE TABLE `JobParameters` (
        `JobID` INT(11) UNSIGNED NOT NULL,
        `Name` VARCHAR(100) NOT NULL,
        `Value` TEXT NOT NULL,
        PRIMARY KEY (`JobID`,`Name`),
        FOREIGN KEY (`JobID`) REFERENCES `Jobs`(`JobID`)
      ) ENGINE=InnoDB DEFAULT CHARSET=latin1;

    Here we define a dynamic mapping with the constant fields::

    "JobID": {"type": "long"},
    "timestamp": {"type": "date"},

    and all other custom fields added dynamically.

    The reason for switching to a ES-based JobParameters lies in the extended searching
    capabilities of ES..
    This results in higher traceability for DIRAC jobs.

    The following class methods are provided for public usage
      - getJobParameters()
      - setJobParameter()
      - deleteJobParameters()
"""
from DIRAC import S_OK, S_ERROR, gConfig
from DIRAC.Core.Utilities import TimeUtilities
from DIRAC.ConfigurationSystem.Client.PathFinder import getDatabaseSection
from DIRAC.ConfigurationSystem.Client.Helpers import CSGlobals
from DIRAC.Core.Base.ElasticDB import ElasticDB

name = "ElasticJobParametersDB"

mapping = {
    "properties": {
        "JobID": {"type": "long"},
        "timestamp": {"type": "date"},
        "CPUNormalizationFactor": {"type": "long"},
        "NormCPUTime(s)": {"type": "long"},
        "Memory(kB)": {"type": "long"},
        "CPU(MHz)": {"type": "long"},
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
            section = getDatabaseSection("WorkloadManagement/ElasticJobParametersDB")
            indexPrefix = gConfig.getValue(f"{section}/IndexPrefix", CSGlobals.getSetup()).lower()

            # Connecting to the ES cluster
            super().__init__(name, "WorkloadManagement/ElasticJobParametersDB", indexPrefix, parentLogger=parentLogger)
        except Exception as ex:
            self.log.error("Can't connect to ElasticJobParametersDB", repr(ex))
            raise RuntimeError("Can't connect to ElasticJobParametersDB")

        self.indexName = f"{self.getIndexPrefix()}_elasticjobparameters_index"
        # Verifying if the index is there, and if not create it
        res = self.existingIndex(self.indexName)
        if not res["OK"] or not res["Value"]:
            result = self.createIndex(self.indexName, mapping, period=None)
            if not result["OK"]:
                self.log.error(result["Message"])
                raise RuntimeError(result["Message"])
            self.log.always("Index created:", self.indexName)

    def getJobParameters(self, jobID: int, paramList=None) -> dict:
        """Get Job Parameters defined for jobID.
        Returns a dictionary with the Job Parameters.
        If paramList is empty - all the parameters are returned.


        :param self: self reference
        :param jobID: Job ID
        :param paramList: List or string of parameters to return
        :return: dict with Job Parameter values
        """
        if isinstance(paramList, str):
            paramList = paramList.replace(" ", "").split(",")
        self.log.debug(f"JobDB.getParameters: Getting Parameters for job {jobID}")
        resultDict = {}
        self.log.debug(f"The searched parameters with JobID {jobID} exists in the new index {self.indexName}")
        res = self.getDoc(self.indexName, str(jobID))
        if res["OK"]:
            if paramList:
                for par in paramList:
                    try:
                        resultDict[par] = res["Value"][par]
                    except Exception as ex:
                        self.log.error("Could not find the searched parameters")
            else:
                # if parameters are not specified return all of them
                resultDict = res["Value"]
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

        # If a record with this jobID update and add parameter, otherwise create a new record
        if self.existsDoc(self.indexName, id=str(jobID)):
            self.log.debug("A document for this job already exists, it will now be updated")
            result = self.updateDoc(index=self.indexName, id=str(jobID), body={"doc": data})
        else:
            self.log.debug("No document has this job id, creating a new document for this job")
            result = self.index(self.indexName, body=data, docID=str(jobID))
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
        self.log.debug(f"Inserting parameters", "in {self.indexName}: for job {jobID}: {parameters}")

        if isinstance(parameters, list):
            parametersDict = dict(parameters)

        parametersDict["JobID"] = jobID
        parametersDict["timestamp"] = int(TimeUtilities.toEpochMilliSeconds())

        if self.existsDoc(self.indexName, id=str(jobID)):
            self.log.debug("A document for this job already exists, it will now be updated")
            result = self.updateDoc(index=self.indexName, id=str(jobID), body={"doc": parametersDict})
        else:
            self.log.debug("Creating a new document for this job")
            result = self.index(self.indexName, body=parametersDict, docID=str(jobID))
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
        self.log.debug(f"Deleting parameters with JobID {jobID} from the  index {self.indexName}")
        if not paramList:
            # Deleting the whole record
            self.log.debug("Deleting record of job {jobID}")
            result = self.deleteDoc(self.indexName, id=str(jobID))
            if not result["OK"]:
                self.log.error("Could not delete the record")
                return S_ERROR(result)
        else:
            # Deleting the specific parameters
            self.log.debug(f"Deleting Parameters {paramList} for job {jobID}")
            for paramName in paramList:
                result = self.updateDoc(
                    index=self.indexName, id=str(jobID), body={"script": "ctx._source.remove('" + paramName + "')"}
                )
                if not result["OK"]:
                    self.log.error("Could not delete the prameters")
                    return S_ERROR(result)
        self.log.debug("Parameters successfully deleted.")
        return S_OK()
