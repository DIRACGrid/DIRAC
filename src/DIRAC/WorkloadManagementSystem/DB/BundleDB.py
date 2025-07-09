""" BundleDB class is a front-end to the bundle db
"""
import uuid
from ast import literal_eval

from DIRAC import S_ERROR, S_OK
from DIRAC.Core.Base.DB import DB
from DIRAC.FrameworkSystem.Client.Logger import contextLogger
from DIRAC.WorkloadManagementSystem.Client import PilotStatus

STATUS_MAP = {
    "Storing": PilotStatus.WAITING,
    "Sent": PilotStatus.RUNNING,
    "Finalized": PilotStatus.DONE,
    "Failed": PilotStatus.FAILED,
}

BUNDLES_INFO_COLUMNS = [
    "BundleID",
    "ProcessorSum",
    "MaxProcessors",
    "Site",
    "CE",
    "Queue",
    "CEDict",
    "ExecTemplate",
    "TaskID",
    "Status",
    "ProxyPath",
]

JOB_TO_BUNDLE_COLUMNS = [
    "JobID",
    "BundleID",
    "ExecutablePath",
    "Inputs",
    "Outputs",
    "Processors",
]


def formatSelectOutput(listOfResults, keys):
    retVal = []

    for kvTuple in listOfResults:
        inner = {}
        for k, v in zip(keys, list(kvTuple)):
            inner[k] = v
        retVal.append(inner)

    return retVal


class BundleDB(DB):
    """BundleDB MySQL Database Manager"""

    def __init__(self, parentLogger=None):
        super().__init__("BundleDB", "WorkloadManagement/BundleDB", parentLogger=parentLogger)
        self._defaultLogger = self.log

    @property
    def log(self):
        return contextLogger.get() or self._defaultLogger

    @log.setter
    def log(self, value):
        self._defaultLogger = value

    #############################################################################

    def insertJobToBundle(self, jobId, executable, inputs, outputs, processors, ceDict, proxyPath):
        result = self.__getBundlesFromCEDict(ceDict)

        if not result["OK"]:
            return result

        bundles = result["Value"]

        # No bundles matching ceDict, so create a new one
        if not bundles:
            result = self.__createNewBundle(ceDict, proxyPath)

            if not result["OK"]:
                return result

            bundleId = result["Value"]
            result = self.__insertJobInBundle(jobId, bundleId, executable, inputs, outputs, processors, proxyPath)

            if not result["OK"]:
                return result

            return S_OK({"BundleId": bundleId, "Ready": result["Value"]["Ready"]})

        # Check the best possible bundle to insert the job
        bundleId = self.__selectBestBundle(bundles, processors)

        # If it does not fit in an already created bundle, create a new one
        if not bundleId:
            result = self.__createNewBundle(ceDict, proxyPath)

            if not result["OK"]:
                return result

            bundleId = result["Value"]

        # TODO: CHECK IF THE JOB IS ALREADY IN THE BUNDLE

        # Insert it and obtain if it is ready to be submitted
        result = self.__insertJobInBundle(jobId, bundleId, executable, inputs, outputs, processors, proxyPath)

        if not result["OK"]:
            return result

        return S_OK({"BundleId": bundleId, "Ready": result["Value"]["Ready"]})

    def removeJobFromBundle(self, jobId):
        result = self.getFields("JobToBundle", ["BundleID", "Processors"], {"JobID": jobId})

        if not result["OK"]:
            return result

        jobInfo = result["Value"][0]
        bundleId, procs = jobInfo[0], jobInfo[1]

        result = self.__reduceProcessorSum(bundleId, procs)

        if not result["OK"]:
            return result

        result = self.deleteEntries("JobToBundle", {"JobID": jobId})

        # Rollback on error?? Can this Fail??
        return result

    #############################################################################

    def getBundleIdFromJobId(self, jobId):
        result = self.getFields("JobToBundle", ["BundleID"], {"JobID": jobId})

        if not result["OK"]:
            return result

        if not result["Value"]:
            return S_ERROR("JobId not present in any bundle")

        return S_OK(result["Value"][0][0])

    def getBundleStatus(self, bundleId):
        result = self.getFields("BundlesInfo", ["Status"], {"BundleID": bundleId})

        if not result["Value"]:
            return S_ERROR("Failed to get bundle Status")

        return S_OK(STATUS_MAP[result["Value"][0][0]])

    def getJobsOfBundle(self, bundleId):
        fields = ["JobID", "ExecutablePath", "Inputs", "Outputs"]

        result = self.getFields("JobToBundle", fields, {"BundleID": bundleId})

        if not result["OK"]:
            return result

        retVal = formatSelectOutput(result["Value"], fields)

        for i in range(len(retVal)):
            retVal[i]["Inputs"] = literal_eval(retVal[i]["Inputs"])
            retVal[i]["Outputs"] = literal_eval(retVal[i]["Outputs"])

        return S_OK(retVal)

    #############################################################################

    def setTaskId(self, bundleId, taskId):
        result = self.updateFields("BundlesInfo", ["TaskID", "Status"], [taskId, "Sent"], {"BundleID": bundleId})
        return result

    def getTaskId(self, bundleId):
        result = self.getFields("BundlesInfo", ["TaskID"], {"BundleID": bundleId})

        if not result["OK"]:
            return result

        return S_OK(result["Value"][0][0])

    #############################################################################

    def setBundleAsFinalized(self, bundleId):
        result = self.__updateBundleStatus(bundleId, "Finalized")
        return result

    def setBundleAsFailed(self, bundleId):
        result = self.__updateBundleStatus(bundleId, "Failed")
        return result

    #############################################################################

    def getWholeBundle(self, bundleId):
        result = self.getFields("BundlesInfo", [], {"BundleID": bundleId})

        if not result["OK"]:
            return result

        if not result["Value"]:
            return S_ERROR(f"No bundle with id {bundleId}")

        bundleDict = formatSelectOutput(result["Value"], BUNDLES_INFO_COLUMNS)[0]
        bundleDict["Status"] = STATUS_MAP[bundleDict["Status"]]

        self.log.debug(f"Look at this cool bundle: {bundleDict}")

        return S_OK(bundleDict)

    def getBundleCE(self, bundleId):
        result = self.getFields("BundlesInfo", ["CEDict", "ProxyPath"], {"BundleID": bundleId})

        if not result["OK"]:
            return result

        return S_OK(formatSelectOutput(result["Value"], ["CEDict", "ProxyPath"])[0])

    #############################################################################

    def __reduceProcessorSum(self, bundleId, nProcessors):
        cmd = 'UPDATE BundlesInfo SET ProcessorSum = ProcessorSum - {} WHERE BundleID = "{}";'.format(
            nProcessors, bundleId
        )
        return self._query(cmd)

    def __createNewBundle(self, ceDict, proxyPath):
        if "ExecTemplate" not in ceDict:
            return S_ERROR("CE must have a properly formatted ExecTemplate")

        bundleId = uuid.uuid4().hex
        insertInfo = {
            "BundleID": bundleId,
            "ProcessorSum": 0,
            "MaxProcessors": ceDict["NumberOfProcessors"],
            "ExecTemplate": ceDict["ExecTemplate"],
            "Site": ceDict["Site"],
            "CE": ceDict["GridCE"],
            "Queue": ceDict["Queue"],
            "CEDict": str(ceDict),
            "ProxyPath": proxyPath,
        }

        result = self.insertFields("BundlesInfo", list(insertInfo.keys()), list(insertInfo.values()))

        if not result["OK"]:
            return result

        return S_OK(bundleId)

    def __insertJobInBundle(self, jobId, bundleId, executable, inputs, outputs, nProcessors, proxyPath):
        # Insert the job into the bundle
        insertInfo = {
            "JobID": jobId,
            "BundleID": bundleId,
            "ExecutablePath": executable,
            "Inputs": str(inputs),
            "Outputs": str(outputs),
            "Processors": nProcessors,
        }

        result = self.insertFields("JobToBundle", list(insertInfo.keys()), list(insertInfo.values()))

        if not result["OK"]:
            return result

        # Modify the number of processors that will be used by the bundle
        cmd = 'UPDATE BundlesInfo SET ProcessorSum = ProcessorSum + {} WHERE BundleID = "{}";'.format(
            nProcessors, bundleId
        )
        result = self._query(cmd)

        if not result["OK"]:
            return result

        # Obtain the current Sum and the Max available
        result = self.getFields("BundlesInfo", ["ProcessorSum", "MaxProcessors", "Status"], {"BundleID": bundleId})

        if not result["OK"]:
            return result

        retVal = formatSelectOutput(result["Value"], ["ProcessorSum", "MaxProcessors", "Status"])
        selection = retVal[0]
        selection["Ready"] = selection["ProcessorSum"] == selection["MaxProcessors"]

        selection.pop("ProcessorSum")
        selection.pop("MaxProcessors")

        selection["Status"] = STATUS_MAP[selection["Status"]]

        # TODO: Change this to a strategy based selection and remove self.__selectBestBundle(...)
        return S_OK(selection)

    def __getBundlesFromCEDict(self, ceDict):
        # conditions = {
        #     "Site": ceDict["Site"],
        #     "CE": ceDict["GridCE"],
        #     "Queue": ceDict["Queue"],
        # }

        cmd = 'SELECT * FROM BundlesInfo WHERE Site = "{Site}" AND CE = "{CE}" AND Queue = "{Queue}";'.format(
            Site=ceDict["Site"],
            CE=ceDict["GridCE"],
            Queue=ceDict["Queue"],
        )
        result = self._query(cmd)
        # result = self.getFields("BundlesInfo", [], conditions)

        if not result["OK"]:
            return result

        if not result["Value"]:
            return S_OK([])

        retVal = formatSelectOutput(
            result["Value"],
            BUNDLES_INFO_COLUMNS,
        )
        return S_OK(retVal)

    def __updateBundleStatus(self, bundleId, newStatus):
        if newStatus not in STATUS_MAP.keys():
            msg = f"The new status '{newStatus}' does not correspond with the possible statuses:"
            return S_ERROR(msg, STATUS_MAP.keys())

        cmd = f'UPDATE BundlesInfo SET Status = "{newStatus}" WHERE BundleID = "{bundleId}";'
        result = self._query(cmd)

        if not result["OK"]:
            return result

        return S_OK()

    # This is function quite dumb, and should not work like this, but for a fist
    #  aproximation is fine (I guess).
    #
    # The best way (in my opinion) of approching this is by taking advantage of
    #  dynamic programming.
    # We could approach this by considering the bundles as sacks and selecting
    #  the bundle to insert the same way it is done in the Knapsack Problem.
    #
    #  REF: https://en.wikipedia.org/wiki/Knapsack_problem
    #
    # Each bundle that relates to the same CE would be a Knapsack and each item
    #  would be a different job. The job would have its 'weight' and 'price' set
    #  to the number of processors it needs, and the algorithm would optimize
    #  how they are distributed around the bundles.
    #
    # By having multiple bundles, this would relate more to the Bin Packing Problem,
    #  which is an abstaction of the Knapsack Problem.
    #
    #  REF: https://en.wikipedia.org/wiki/Bin_packing_problem
    #
    def __selectBestBundle(self, bundles, nProcessors):
        bestBundleId = None
        currentBestProcs = 0

        for bundle in bundles:
            bundleId = bundle["BundleID"]
            procs = bundle["ProcessorSum"]
            maxProcs = bundle["MaxProcessors"]

            newProcSum = procs + nProcessors

            if newProcSum == maxProcs:
                return bundleId

            elif newProcSum > maxProcs:
                continue

            elif newProcSum > currentBestProcs:
                currentBestProcs = newProcSum
                bestBundleId = bundleId

        return bestBundleId
