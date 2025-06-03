""" BundleDB class is a front-end to the bundle db
"""
from DIRAC import S_ERROR, S_OK
from DIRAC.Core.Base.DB import DB
from DIRAC.FrameworkSystem.Client.Logger import contextLogger

BUNDLE_STATUS = ("Storing", "Sent", "Finalized", "Failed")


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

    def getBundleIdFromJobId(self, jobID):
        result = self.getFields("JobToBundle", ["BundleID"], {"JobID": jobID})

        if not result["OK"]:
            return result

        if not result["Value"]:
            return S_ERROR("JobId not present in any bundle")

        return S_OK(result["Value"][0][0])

    def insertJobToBundle(self, jobId, executable, inputs, processors, ceDict):
        result = self.__getBundlesFromCEDict(ceDict)

        if not result["OK"]:
            return result

        bundles = result["Value"]

        # No bundles matching ceDict, so create a new one
        if not bundles:
            result = self.__createNewBundle(ceDict)

            if not result["OK"]:
                return result

            bundleId = result["Value"]
            result = self.__insertJobInBundle(jobId, bundleId, executable, inputs, processors)

            if not result["OK"]:
                return result

            return S_OK({"BundleId": bundleId, "Ready": result["Value"]})

        # Check the best possible bundle to insert the job
        bundleId = self.__selectBestBundle(bundles, processors)

        # If it does not fit in an already created bundle, create a new one
        if not bundleId:
            result = self.__createNewBundle(ceDict)

            if not result["OK"]:
                return result

            bundleId = result["Value"]

        # Insert it and obtain if it is ready to be submitted
        result = self.__insertJobInBundle(jobId, bundleId, executable, inputs, processors)

        if not result["OK"]:
            return result

        return S_OK({"BundleId": bundleId, "Ready": result["Value"]})

    def getBundle(self, bundleId):
        result = self.getFields("BundlesInfo", [], {"BundleID": bundleId})

        if not result["OK"]:
            return result

        retVal = formatSelectOutput(
            result["Value"],
            [
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
            ],
        )
        return S_OK(retVal[0])

    def getBundleStatus(self, bundleId):
        result = self.getFields("BundlesInfo", ["Status"], {"BundleID": bundleId})
        
        if not result["Value"]:
            return S_ERROR("Failed to get bundle Status")

        return S_OK(result["Value"][0][0])

    def getJobsOfBundle(self, bundleId):
        result = self.getFields("JobToBundle", ["JobID", "ExecutablePath", "Inputs"], {"BundleID": bundleId})

        if not result["OK"]:
            return result
        retVal = formatSelectOutput(result["Value"], ["JobID", "ExecutablePath", "Inputs"])
        for i in range(len(retVal)):
            retVal[i]["Inputs"] = retVal[i]["Inputs"].split(" ")
            
        return S_OK(retVal)

    def setTaskId(self, bundleId, taskId):
        result = self.updateFields("BundlesInfo", ["TaskID"], [taskId], {"BundleID": bundleId})

        if not result["OK"]:
            return result

        return S_OK()
    
    def getTaskId(self, bundleId):
        result = self.getFields("BundlesInfo", ["TaskID"], {"BundleID": bundleId})

        if not result["OK"]:
            return result

        return S_OK(result["Value"][0][0])

    def setBundleAsFinalized(self, bundleId):
        result = self.__updateBundleStatus(bundleId, "Finalized")
        return result
    
    def setBundleAsFailed(self, bundleId):
        result = self.__updateBundleStatus(bundleId, "Failed")
        return result

    def __createNewBundle(self, ceDict):
        if "ExecTemplate" not in ceDict:
            return S_ERROR("CE must have a properly formatted ExecTemplate")

        insertInfo = {
            "ProcessorSum": 0,
            "MaxProcessors": ceDict["NumberOfProcessors"],
            "ExecTemplate": ceDict["ExecTemplate"],
            "Site": ceDict["Site"],
            "CE": ceDict["GridCE"],
            "Queue": ceDict["Queue"],
            "CEDict": str(ceDict),
        }

        result = self.insertFields("BundlesInfo", list(insertInfo.keys()), list(insertInfo.values()))

        if not result["OK"]:
            return result

        # Returns the ID of the Bundle (which is automatically incremented)
        return S_OK(result["lastRowId"])

    def __insertJobInBundle(self, jobId, bundleId, executable, inputs, nProcessors):
        # Insert the job into the bundle
        insertInfo = {"JobID": jobId, "BundleID": bundleId, "ExecutablePath": executable, "Inputs": " ".join(inputs)}

        result = self.insertFields("JobToBundle", list(insertInfo.keys()), list(insertInfo.values()))

        if not result["OK"]:
            return result

        # Modify the number of processors that will be used by the bundle
        cmd = "UPDATE BundlesInfo SET ProcessorSum = ProcessorSum + {} WHERE BundleID = {};".format(
            nProcessors, bundleId
        )
        result = self._query(cmd)

        if not result["OK"]:
            return result

        # Obtain the current Sum and the Max available
        result = self.getFields("BundlesInfo", ["ProcessorSum", "MaxProcessors"], {"BundleID": bundleId})

        if not result["OK"]:
            return result

        retVal = formatSelectOutput(result["Value"], ["ProcessorSum", "MaxProcessors"])
        selection = retVal[0]

        # TODO: Change this to a strategy based selection and remove self.__selectBestBundle(...)
        return S_OK(selection["ProcessorSum"] == selection["MaxProcessors"])

    def __getBundlesFromCEDict(self, ceDict):
        conditions = {
            "Site": ceDict["Site"],
            "CE": ceDict["GridCE"],
            "Queue": ceDict["Queue"],
        }

        result = self.getFields("BundlesInfo", [], conditions)

        if not result["OK"]:
            return result

        if not result["Value"]:
            return S_OK()

        # TODO: This line is awful, should change to something easier to scale
        retVal = formatSelectOutput(
            result["Value"],
            [
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
            ],
        )
        return S_OK(retVal)

    def __updateBundleStatus(self, bundleId, newStatus):
        if newStatus not in BUNDLE_STATUS:
            msg = f"The new status '{newStatus}' does not correspond with the possible statuses:"
            return S_ERROR(msg, BUNDLE_STATUS)

        cmd = f"UPDATE BundlesInfo SET Status = {newStatus} WHERE BundleID = {bundleId};"
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
