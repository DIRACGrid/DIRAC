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
from DIRAC import S_OK, gConfig
from DIRAC.Core.Utilities import TimeUtilities
from DIRAC.ConfigurationSystem.Client.PathFinder import getDatabaseSection
from DIRAC.ConfigurationSystem.Client.Helpers import CSGlobals
from DIRAC.Core.Base.ElasticDB import ElasticDB
from DIRAC.Core.Utilities.ReturnValues import S_ERROR

try:
    from opensearchpy.exceptions import NotFoundError, RequestError
except ImportError:
    from elasticsearch.exceptions import NotFoundError, RequestError

name = "ElasticJobParametersDB"

mapping = {"properties": {"JobID": {"type": "long"}, "Name": {"type": "keyword"}, "Value": {"type": "text"}}}


class ElasticJobParametersDB(ElasticDB):
    def __init__(self, parentLogger=None):
        """Standard Constructor"""

        try:
            section = getDatabaseSection("WorkloadManagement/ElasticJobParametersDB")
            indexPrefix = gConfig.getValue("%s/IndexPrefix" % section, CSGlobals.getSetup()).lower()

            # Connecting to the ES cluster
            super().__init__(name, "WorkloadManagement/ElasticJobParametersDB", indexPrefix, parentLogger=parentLogger)
        except Exception as ex:
            self.log.error("Can't connect to ElasticJobParametersDB", repr(ex))
            raise RuntimeError("Can't connect to ElasticJobParametersDB")

        self.oldIndexName = "%s_%s" % (self.getIndexPrefix(), name.lower())
        self.indexName = "%s_%s" % (self.getIndexPrefix(), "es_jobparameters_records")
        # Verifying if the index is there, and if not create it
        res = self.existingIndex(self.indexName)
        if not res["OK"] or not res["Value"]:
            result = self.createIndex(self.indexName, mapping, period=None)
            if not result["OK"]:
                self.log.error(result["Message"])
                raise RuntimeError(result["Message"])
            self.log.always("Index created:", self.indexName)

        self.dslSearch = self._Search(self.oldIndexName)
        self.dslSearch.extra(track_total_hits=True)

    def getJobParameters(self, jobID: int, paramList=None) -> dict:
        """Get Job Parameters defined for jobID.
          Returns a dictionary with the Job Parameters.
          If paramList is empty - all the parameters are returned.

        :return: dict with all Job Parameter values
        """
        self.log.debug("JobDB.getParameters: Getting Parameters for job %s" % jobID)

        if self._isOldIndex(self.oldIndexName, jobID):
            self.log.debug("A document with JobID %s was found in the old index %s" % (jobID, self.oldIndexName))
            if paramList:
                if isinstance(paramList, str):
                    paramList = paramList.replace(" ", "").split(",")
            else:
                paramList = []

            resultDict = {}

            # the following should be equivalent to
            # {
            #   "query": {
            #     "bool": {
            #       "filter": {  # no scoring
            #         "term": {"JobID": jobID}  # term level query, does not pass through the analyzer
            #       }
            #     }
            #   }
            # }

            s = self.dslSearch.query("bool", filter=self._Q("term", JobID=jobID))

            res = s.scan()

            for hit in res:
                name = hit.Name
                if paramList and name not in paramList:
                    continue
                resultDict[name] = hit.Value
        else:
            self.log.debug("The searched parameters with JobID %s exists in the new index %s" % (jobID, self.indexName))
            resultDict = self.getDoc(self.indexName, str(jobID))
        return S_OK({jobID: resultDict})

    def setJobParameter(self, jobID: int, key: str, value: str) -> dict:
        """
        Inserts data into ElasticJobParametersDB index

        :returns: S_OK/S_ERROR as result of indexing
        """
        data = {"JobID": jobID, key: value, "timestamp": TimeUtilities.toEpochMilliSeconds()}

        self.log.debug("Inserting data in %s:%s" % (self.indexName, data))

        # The _id in ES can't exceed 512 bytes, this is a ES hard-coded limitation.

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

        :returns: S_OK/S_ERROR as result of indexing
        """
        self.log.debug("Inserting parameters", "in %s: for job %s : %s" % (self.indexName, jobID, parameters))

        parametersListDict = dict(parameters)
        parametersListDict["JobID"] = jobID
        parametersListDict["timestamp"] = int(TimeUtilities.toEpochMilliSeconds())
        if self.existsDoc(self.indexName, id=str(jobID)):
            self.log.debug("A document for this job already exists, it will now be updated")
            result = self.updateDoc(index=self.indexName, id=str(jobID), body={"doc": parametersListDict})
        else:
            self.log.debug("Creating a new document for this job")
            result = self.bulk_index(self.indexName, data=parametersListDict, period=None, withTimeStamp=True)
        if not result["OK"]:
            self.log.error("Couldn't insert or update data", result["Message"])
        return result

    def deleteJobParameters(self, jobID: int, paramList=None) -> dict:
        """delete Job Parameters defined for jobID.
          Returns a dictionary with the Job Parameters.
          If paramList is empty - all the parameters for the job are removed

        :return: dict with all Job Parameter values
        """

        if isinstance(paramList, str):
            paramList = paramList.replace(" ", "").split(",")

        # Old structure of the parameter indices
        if self._isOldIndex(self.oldIndexName, jobID):
            self.log.debug("A document with JobID %s was found in the old index %s" % (jobID, self.oldIndexName))
            jobFilter = self._Q("term", JobID=jobID)

            if not paramList:
                s = self.dslSearch.query("bool", filter=jobFilter)
                s.delete()
                return S_OK()

            # the following should be equivalent to
            # {
            #   "query": {
            #     "bool": {
            #       "filter": [  # no scoring
            #         {"term": {"JobID": jobID}},  # term level query, does not pass through the analyzer
            #         {"term": {"Name": param}},  # term level query, does not pass through the analyzer
            #       ]
            #     }
            #   }
            # }

            for param in paramList:
                paramFilter = self._Q("term", Name=param)
                combinedFilter = jobFilter & paramFilter

                s = self.dslSearch.query("bool", filter=combinedFilter)
                s.delete()
            return S_OK()

        else:
            self.log.debug("The searched parameters with JobID %s exists in the new index %s" % (jobID, self.indexName))
            if not paramList:
                # Deleting the whole record
                self.log.debug("Deleting record of job %s" % jobID)
                result = self.deleteDoc(self.indexName, id=str(jobID))
            else:
                # Deleting the specific parameters
                self.log.debug("JobDB.getParameters: Deleting Parameters %s for job %s" % (paramList, jobID))
                for paramName in paramList:
                    result = self.updateDoc(
                        index=self.indexName, id=str(jobID), body={"script": "ctx._source.remove('" + paramName + "')"}
                    )
                    self.log.debug("Deleted parameter %s" % (paramName))
            if not result["OK"]:
                return S_ERROR(result)
            self.log.debug("Parameters successfully deleted.")
            return S_OK(result)

    def _isOldIndex(self, old_index, jobID):
        query = {
            "query": {
                "bool": {
                    "filter": {  # no scoring
                        "term": {"JobID": jobID}  # term level query, does not pass through the analyzer
                    }
                }
            }
        }
        try:
            # See a document with this jobID is stored in the old index
            self.query(old_index, query)
            return True
        except (RequestError, NotFoundError):
            return False

    # TODO: Add query by value (e.g. query which values are in a certain pattern)
