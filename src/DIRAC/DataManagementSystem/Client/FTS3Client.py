from DIRAC.Core.Base.Client import Client, createClient
from DIRAC import S_OK, S_ERROR
from DIRAC.Core.Utilities.JEncode import encode, decode


@createClient("DataManagement/FTS3Manager")
class FTS3Client(Client):
    """Client code to the FTS3 service"""

    def __init__(self, url=None, **kwargs):
        """Constructor function."""
        super().__init__(**kwargs)
        self.setServer("DataManagement/FTS3Manager")
        if url:
            self.setServer(url)

    def persistOperation(self, opObj, **kwargs):
        """Persist (insert/update) an FTS3Operation object into the db

        :param opObj: instance of FTS3Operation
        """

        # In case someone manually set sourceSEs as a list:
        if isinstance(opObj.sourceSEs, list):
            opObj.sourceSEs = ",".join(opObj.sourceSEs)

        opJSON = encode(opObj)
        return self._getRPC(**kwargs).persistOperation(opJSON)

    def getOperation(self, operationID, **kwargs):
        """Get the FTS3Operation from the database

        :param operationID: id of the operation
        :return: FTS3Operation object
        """
        res = self._getRPC(**kwargs).getOperation(operationID)
        if not res["OK"]:
            return res

        opJSON = res["Value"]

        try:
            opObj, _size = decode(opJSON)
            return S_OK(opObj)
        except Exception as e:
            return S_ERROR("Exception when decoding the FTS3Operation object %s" % e)

    def getActiveJobs(self, limit=20, lastMonitor=None, jobAssignmentTag="Assigned", **kwargs):
        """Get all the FTSJobs that are not in a final state

        :param limit: max number of jobs to retrieve
        :return: list of FTS3Jobs
        """
        res = self._getRPC(**kwargs).getActiveJobs(limit, lastMonitor, jobAssignmentTag)
        if not res["OK"]:
            return res

        activeJobsJSON = res["Value"]

        try:
            activeJobs, _size = decode(activeJobsJSON)
            return S_OK(activeJobs)
        except Exception as e:
            return S_ERROR("Exception when decoding the active jobs json %s" % e)

    def getNonFinishedOperations(self, limit=20, operationAssignmentTag="Assigned", **kwargs):
        """Get all the FTS3Operations that have files in New or Failed state
        (reminder: Failed is NOT terminal for files. Failed is when fts failed, but we
        can retry)

        :param limit: max number of jobs to retrieve
        :return: json list of FTS3Operation
        """

        res = self._getRPC(**kwargs).getNonFinishedOperations(limit, operationAssignmentTag)
        if not res["OK"]:
            return res

        operationsJSON = res["Value"]

        try:
            operations, _size = decode(operationsJSON)
            return S_OK(operations)
        except Exception as e:
            return S_ERROR(0, "Exception when decoding the non finished operations json %s" % e)

    def getOperationsFromRMSOpID(self, rmsOpID, **kwargs):
        """Get the FTS3Operations matching a given RMS Operation

        :param rmsOpID: id of the operation in the RMS
        :return: list of FTS3Operation objects
        """
        res = self._getRPC(**kwargs).getOperationsFromRMSOpID(rmsOpID)
        if not res["OK"]:
            return res

        operationsJSON = res["Value"]
        try:
            operations, _size = decode(operationsJSON)
            return S_OK(operations)
        except Exception as e:
            return S_ERROR(0, "Exception when decoding the operations json %s" % e)
