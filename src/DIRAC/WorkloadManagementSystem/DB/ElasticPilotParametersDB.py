""" Module containing a front-end to the ElasticSearch-based ElasticPilotParametersDB.

    The following class methods are provided for public usage
      - getPilotParameters()
      - setPilotParameter()
      - deletePilotParameters()
"""
from DIRAC import S_OK, S_ERROR, gConfig
from DIRAC.Core.Utilities import TimeUtilities
from DIRAC.ConfigurationSystem.Client.PathFinder import getDatabaseSection
from DIRAC.ConfigurationSystem.Client.Helpers import CSGlobals
from DIRAC.Core.Base.ElasticDB import ElasticDB


mapping = {
    "properties": {
        "PilotID": {"type": "long"},
        "timestamp": {"type": "date"},
        "CPUNormalizationFactor": {"type": "long"},
        "Wallclock": {"type": "long"},
        "HostName": {"type": "keyword"},
        "GridCE": {"type": "keyword"},
        "ModelName": {"type": "keyword"},
        "Status": {"type": "keyword"},
    }
}


class ElasticPilotParametersDB(ElasticDB):
    def __init__(self, parentLogger=None):
        """Standard Constructor"""

        try:
            section = getDatabaseSection("WorkloadManagement/ElasticPilotParametersDB")
            indexPrefix = gConfig.getValue(f"{section}/IndexPrefix", CSGlobals.getSetup()).lower()

            # Connecting to the ES cluster
            super().__init__("WorkloadManagement/ElasticPilotParametersDB", indexPrefix, parentLogger=parentLogger)
        except Exception as ex:
            self.log.error("Can't connect to ElasticPilotParametersDB", repr(ex))
            raise RuntimeError("Can't connect to ElasticPilotParametersDB") from ex

        self.indexName_base = f"{self.getIndexPrefix()}_elasticpilotparameters_index"

    def _indexName(self, pilotID: int) -> str:
        """construct the index name

        :param pilotID: Pilot ID
        """
        indexSplit = int(pilotID) // 1e6
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

    def getPilotParameters(self, pilotID: int, paramList=None) -> dict:
        """Get Pilot Parameters defined for pilotID.
          Returns a dictionary with the Pilot Parameters.
          If paramList is empty - all the parameters are returned.

        :param self: self reference
        :param pilotID: Pilot ID
        :param paramList: list of parameters to be returned (also a string is treated)
        :return: dict with all Pilot Parameter values
        """
        if isinstance(paramList, str):
            paramList = paramList.replace(" ", "").split(",")
        self.log.debug(f"ElasticPilotParametersDB.getParameters: Getting Parameters for pilot {pilotID}")

        res = self.getDoc(self._indexName(pilotID), str(pilotID))
        if not res["OK"]:
            return res
        resultDict = res["Value"]
        if paramList:
            for k in list(resultDict):
                if k not in paramList:
                    resultDict.pop(k)

        return S_OK({pilotID: resultDict})

    def setPilotParameter(self, pilotID: int, key: str, value: str) -> dict:
        """
        Inserts data into ElasticPilotParametersDB index

        :param self: self reference
        :param pilotID: Pilot ID
        :param key: parameter key
        :param value: parameter value
        :returns: S_OK/S_ERROR as result of indexing
        """
        data = {"PilotID": pilotID, key: value, "timestamp": TimeUtilities.toEpochMilliSeconds()}

        self.log.debug("Inserting data in {self.indexName}:{data}")

        # The _id in ES can't exceed 512 bytes, this is a ES hard-coded limitation.

        # If a record with this pilotID update and add parameter, otherwise create a new record
        if self.existsDoc(self._indexName(pilotID), docID=str(pilotID)):
            self.log.debug("A document for this pilot already exists, it will now be updated")
            result = self.updateDoc(index=self._indexName(pilotID), docID=str(pilotID), body={"doc": data})
        else:
            self.log.debug("No document has this pilot id, creating a new document for this pilot")
            self._createIndex(self._indexName(pilotID))
            result = self.index(indexName=self._indexName(pilotID), body=data, docID=str(pilotID))
        if not result["OK"]:
            self.log.error("Couldn't insert or update data", result["Message"])
        return result

    def setPilotParameters(self, pilotID: int, parameters: list) -> dict:
        """
        Inserts data into ElasticPilotParametersDB index using bulk indexing

        :param self: self reference
        :param pilotID: Pilot ID
        :param parameters: list of tuples (name, value) pairs
        :returns: S_OK/S_ERROR as result of indexing
        """
        self.log.debug("Inserting parameters", f"in {self._indexName(pilotID)}: for pilot {pilotID}: {parameters}")

        parametersDict = dict(parameters)
        parametersDict["PilotID"] = pilotID
        parametersDict["timestamp"] = int(TimeUtilities.toEpochMilliSeconds())

        if self.existsDoc(self._indexName(pilotID), docID=str(pilotID)):
            self.log.debug("A document for this pilot already exists, it will now be updated")
            result = self.updateDoc(index=self._indexName(pilotID), docID=str(pilotID), body={"doc": parametersDict})
        else:
            self.log.debug("Creating a new document for this pilot")
            self._createIndex(self._indexName(pilotID))
            result = self.index(self._indexName(pilotID), body=parametersDict, docID=str(pilotID))
        if not result["OK"]:
            self.log.error("Couldn't insert or update data", result["Message"])
        return result

    def deletePilotParameters(self, pilotID: int, paramList=None) -> dict:
        """Deletes Pilot Parameters defined for pilotID.
          Returns a dictionary with the Pilot Parameters.
          If paramList is empty - all the parameters for the pilot are removed

        :param self: self reference
        :param pilotID: Pilot ID
        :param paramList: list of parameters to be returned (also a string is treated)
        :return: S_OK()/S_ERROR()
        """

        if isinstance(paramList, str):
            paramList = paramList.replace(" ", "").split(",")

        if not paramList:
            # Deleting the whole record
            self.log.debug("Deleting record of pilot {pilotID}")
            result = self.deleteDoc(self._indexName(pilotID), docID=str(pilotID))
        else:
            # Deleting the specific parameters
            self.log.debug(f"PilotDB.getParameters: Deleting Parameters {paramList} for pilot {pilotID}")
            for paramName in paramList:
                result = self.updateDoc(
                    index=self._indexName(pilotID),
                    docID=str(pilotID),
                    body={"script": "ctx._source.remove('" + paramName + "')"},
                )
                self.log.debug(f"Deleted parameter {paramName}")
        if not result["OK"]:
            return S_ERROR(result)
        self.log.debug("Parameters successfully deleted.")
        return S_OK()

    # TODO: Add query by value (e.g. query which values are in a certain pattern)
