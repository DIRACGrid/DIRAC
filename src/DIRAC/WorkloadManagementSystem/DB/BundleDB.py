""" BundleDB class is a front-end to the bundle db
"""
from DIRAC import S_ERROR, S_OK
from DIRAC.ConfigurationSystem.Client.Helpers.Operations import Operations
from DIRAC.Core.Base.DB import DB
from DIRAC.FrameworkSystem.Client.Logger import contextLogger

# NOTE:
# THIS BLOCK SHOULD BE ITS OWN FUNCTION:
#
# result = self._query(cmd)
# if not result["OK"]:
#     return result
# return S_OK(result["Value"][0])

BUNDLE_STATUS = ('Storing', 'Full', 'Sent','Finalized')

def formatSelectOutput(listOfResults):
    retVal = []

    for kvTuple in listOfResults:
        inner = {}
        for k, v in kvTuple:
            inner[k] = v
        retVal.append(inner)

    return retVal

class BundleDB(DB):
    """BundleDB MySQL Database Manager"""

    def __init__(self, parentLogger=None):
        DB.__init__(self, "BundleDB", "WorkloadManagement/BundleDB", parentLogger=parentLogger)
        self._defaultLogger = self.log
        self.__opsHelper = Operations()

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
        
        return S_OK(result["Value"][0])

    def insertJobToBundle(self, jobId, executable, inputs, processors, ceDict): 
        result = self.__getBundlesFromCEDict(ceDict)
        
        if not result["OK"]:
            return result
        
        bundles = result["Value"]

        # No bundles matching ceDict, so create a new one
        if not bundles:
            bundleId = self.__createNewBundle(ceDict)
            return S_OK(bundleId)

        # Check the best possible bundle to insert the job
        bundleId = self.__selectBestBundle(bundles, processors)
        
        # If it does not fit in an already created bundle, create a new one
        if not bundleId:
            bundleId = self.__createNewBundle(ceDict)
            
        # Insert it and obtain if it is ready to be submitted
        readyForSubmission = self.__insertJobInBundle(jobId, bundleId, executable, inputs, processors)

        return S_OK({"BundleId": bundleId, "Ready": readyForSubmission})

    def getBundle(self, bundleId):
        result = self.getFields("BundlesInfo", [], {"BundleID": bundleId})

        if not result["OK"]:
            return result

        retVal = formatSelectOutput(result["Value"])
        return S_OK(retVal[0])

    def getJobsOfBundle(self, bundleId):
        result = self.getFields("JobToBundle", ["JobID", "ExecutablePath", "Inputs"], {"BundleID": bundleId})

        if not result["OK"]:
            return result

        retVal = formatSelectOutput(result["Value"])
        return S_OK(retVal)

    def setTaskId(self, bundleId, taskId):
        result = self.updateFields("BundlesInfo", ["TaskID"], [taskId], {"BundleID": bundleId})

        if not result["OK"]:
            return result

        return S_OK()

    def __createNewBundle(self, ceDict):
        insertInfo = {
            "ProcessorSum": 0,
            "MaxProcessors": ceDict["NumberOfProcessors"],
            "ExecTemplate": ceDict["ExecTemplate"],
            "Site": ceDict['Site'],
            "CE": ceDict['GridCE'],
            "Queue": ceDict['Queue'],
            "CEDict": str(ceDict)
        }

        result = self.insertFields(
            "BundlesInfo",
            list(insertInfo.keys()),
            list(insertInfo.values())
        )

        if not result["OK"]:
            return result

        #! WILL THIS WORK??
        result = self.getFields("BundlesInfo", ["BundleID"], {"lastRowId": result["lastRowId"]})
        retVal = formatSelectOutput(result["Value"])

        return S_OK(retVal[0]) #! IT SHOULD RETURN THE ID OF THE BUNDLE
    
    def __insertJobInBundle(self, jobId, bundleId, executable, inputs, nProcessors):
        # Insert the job into the bundle
        insertInfo = {
            "JobID": jobId,
            "BundleID": bundleId,
            "ExecutablePath": executable,
            "Inputs": inputs
        }

        result = self.insertFields(
            "JobToBundle",
            list(insertInfo.keys()),
            list(insertInfo.values())
        )

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

        retVal = formatSelectOutput(result["Value"])
        selection = retVal[0]

        # TODO: Change this to a strategy based selection and remove self.__selectBestBundle(...)
        return S_OK(selection["ProcessorSum"] == selection["MaxProcessors"])

    def __getBundlesFromCEDict(self, ceDict):
        conditions = {
            "Site": ceDict['Site'],
            "CE": ceDict['GridCE'],
            "Queue": ceDict['Queue'],
        }

        result = self.getFields("BundlesInfo", [], conditions)

        if not result["OK"]:
            return result
        
        if not result["Value"]:
            return S_OK()

        retVal = formatSelectOutput(result["Value"])
        return S_OK(retVal) 

    def __updateBundleStatus(self, bundleId, newStatus):
        if newStatus not in BUNDLE_STATUS:
            msg = "The new status '{}' does not correspond with the possible statuses:".format(newStatus)
            return S_ERROR(msg, BUNDLE_STATUS)
        
        cmd = "UPDATE BundlesInfo SET Status = {} WHERE BundleID = {};".format(
            newStatus, bundleId
        )
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
                bestBundleId = bundleId
        
        return bestBundleId
            
