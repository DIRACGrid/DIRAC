"""BundleDB class is a front-end to the bundle db"""

import uuid
from ast import literal_eval
from datetime import datetime, timezone

from DIRAC import S_ERROR, S_OK
from DIRAC.Core.Base.DB import DB
from DIRAC.FrameworkSystem.Client.Logger import contextLogger
from DIRAC.WorkloadManagementSystem.Client import PilotStatus


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

        self.BUNDLES_INFO_TABLE = "BundlesInfo"
        self.JOB_TO_BUNDLE_TABLE = "JobToBundle"
        self.JOB_INPUTS_TABLE = "JobInputs"

        self.BUNDLES_INFO_COLUMNS = [
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
            "Flags",
            "FirstTimestamp",
            "LastTimestamp"
        ]

        self.JOB_TO_BUNDLE_COLUMNS = [
            "JobID",
            "BundleID",
            "DiracID",
            "ExecutablePath",
            "Outputs",
            "Processors",
        ]

        self.JOB_INPUTS_COLUMNS = [
            "InputID",
            "JobID",
            "InputPath",
        ]

        self.STATUS_MAP = {
            "Storing": PilotStatus.WAITING,
            "Sent": PilotStatus.RUNNING,
            "Finalized": PilotStatus.DONE,
            "Failed": PilotStatus.FAILED,
        }

        self.MYSQL_DATETIME_FORMAT = '%Y-%m-%d %H:%M:%S'

        self.BUNDLE_FLAGS = {
            "Cleaned":  1,
            "Purged":   1 << 1,
        }


    @property
    def log(self):
        return contextLogger.get() or self._defaultLogger

    @log.setter
    def log(self, value):
        self._defaultLogger = value

    #############################################################################

    def insertJobToBundle(self, jobId, executable, inputs, outputs, processors, ceDict, proxyPath, diracId):
        result = self._getBundlesFromCEDict(ceDict)

        if not result["OK"]:
            return result

        bundles = result["Value"]

        # No bundles matching ceDict, so create a new one
        if not bundles:
            result = self._createNewBundle(ceDict, proxyPath)

            if not result["OK"]:
                return result

            bundleId = result["Value"]
            result = self._insertJobInBundle(jobId, bundleId, executable, inputs, outputs, processors, proxyPath, diracId)

            if not result["OK"]:
                return result

            return S_OK({"BundleId": bundleId, "Ready": result["Value"]["Ready"]})

        # Check the best possible bundle to insert the job
        bundleId = self.__selectBestBundle(bundles, processors)

        # If it does not fit in an already created bundle, create a new one
        if not bundleId:
            result = self._createNewBundle(ceDict, proxyPath)

            if not result["OK"]:
                return result

            bundleId = result["Value"]

        # Insert it and obtain if it is ready to be submitted
        result = self._insertJobInBundle(jobId, bundleId, executable, inputs, outputs, processors, proxyPath, diracId)

        if not result["OK"]:
            return result

        return S_OK({"BundleId": bundleId, "Ready": result["Value"]["Ready"]})

    def removeJobsFromBundle(self, jobIds):
        for jobId in jobIds:
            result = self.getFields(self.JOB_TO_BUNDLE_TABLE, ["BundleID", "Processors"], {"JobID": jobId})

            if not result["OK"]:
                return result

            jobInfo = result["Value"][0]
            bundleId, procs = jobInfo[0], jobInfo[1]

            result = self._reduceProcessorSum(bundleId, procs)

            if not result["OK"]:
                return result

        result = self.deleteEntries(self.JOB_TO_BUNDLE_TABLE, {"JobID": jobIds})
        return S_OK(result)

    #############################################################################

    def getUnpurgedBundles(self):
        cmd = 'SELECT BundleID FROM BundlesInfo WHERE Status = "Finalized" AND Flags & {flag} != {flag};'.format(
            flag=self.BUNDLE_FLAGS["Purged"]
        )

        result = self._query(cmd)

        if not result["OK"]:
            return result

        return S_OK([entry[0] for entry in result["Value"]])

    def getWaitingBundles(self):
        result = self.getFields(self.BUNDLES_INFO_TABLE, self.BUNDLES_INFO_COLUMNS, {"Status": "Storing"})

        if not result["OK"]:
            return result

        bundlesDict = formatSelectOutput(result["Value"], self.BUNDLES_INFO_COLUMNS)
        return S_OK(bundlesDict)
    
    #############################################################################

    def getBundleIdFromJobId(self, jobId):
        result = self.getFields(self.JOB_TO_BUNDLE_TABLE, ["BundleID"], {"JobID": jobId})

        if not result["OK"]:
            return result

        if not result["Value"]:
            return S_ERROR("JobId not present in any bundle")

        return S_OK(result["Value"][0][0])

    def getBundleStatus(self, bundleId):
        result = self.getFields(self.BUNDLES_INFO_TABLE, ["Status"], {"BundleID": bundleId})

        if not result["Value"]:
            return S_ERROR("Failed to get bundle Status")

        return S_OK(self.STATUS_MAP[result["Value"][0][0]])

    def getJobsOfBundle(self, bundleId):
        cmd = """\
        SELECT JobToBundle.JobID, DiracID, ExecutablePath, Outputs, InputPath
        FROM JobToBundle 
        LEFT JOIN JobInputs 
        ON JobToBundle.JobID = JobInputs.JobID 
        WHERE BundleID = "{bundleId}";""".format(
            bundleId=bundleId
        )

        result = self._query(cmd)

        if not result["OK"]:
            return result

        rows = list(result["Value"])
        retVal = {}

        # For each row (JobID, ExecutablePath, Outputs, [InputPath])
        for row in rows:
            # The job has no input
            if len(row) == 4:
                jobID, diracId, jobExecutablePath, jobOutputs = row
                jobInputPath = ""
            else:
                jobID, diracId, jobExecutablePath, jobOutputs, jobInputPath = row

            if jobID not in retVal:
                retVal[jobID] = {
                    "ExecutablePath": jobExecutablePath,
                    "DiracID": diracId,
                    "Inputs": [],
                    "Outputs": [],
                }

            retVal[jobID]["Outputs"].extend(literal_eval(jobOutputs))

            if jobInputPath:
                retVal[jobID]["Inputs"].append(jobInputPath)

        return S_OK(retVal)

    def getJobIDsOfBundle(self, bundleId):
        result = self.getFields(self.JOB_TO_BUNDLE_TABLE, ["JobID"], {"BundleID": bundleId})

        if not result["OK"]:
            return result

        return S_OK([entry[0] for entry in result["Value"]])

    def removeJobInputs(self, jobIds):
        if not isinstance(jobIds, list):
            jobIds = [jobIds]
        
        return self.deleteEntries(self.JOB_INPUTS_TABLE, {"JobID": jobIds})

    #############################################################################

    def setTaskId(self, bundleId, taskId):
        result = self.updateFields(
            self.BUNDLES_INFO_TABLE, ["TaskID", "Status"], [taskId, "Sent"], {"BundleID": bundleId}
        )
        return result

    def getTaskId(self, bundleId):
        result = self.getFields(self.BUNDLES_INFO_TABLE, ["TaskID"], {"BundleID": bundleId})

        if not result["OK"]:
            return result

        return S_OK(result["Value"][0][0])

    #############################################################################

    def setBundleAsFinalized(self, bundleId):
        result = self._updateBundleStatus(bundleId, "Finalized")
        return result

    def setBundleAsFailed(self, bundleId):
        result = self._updateBundleStatus(bundleId, "Failed")
        return result

    def setBundleAsPurged(self, bundleId):
        cmd = 'UPDATE BundlesInfo SET Flags = Flags | {flag} WHERE BundleID = "{bundleId}";'.format(
            bundleId=bundleId, flag=self.BUNDLE_FLAGS["Purged"] 
        )

        return self._query(cmd)

    def setBundleAsCleaned(self, bundleId):
        cmd = 'UPDATE BundlesInfo SET Flags = Flags | {flag} WHERE BundleID = "{bundleId}";'.format(
            bundleId=bundleId, flag=self.BUNDLE_FLAGS["Cleaned"] 
        )

        return self._query(cmd)

    def isBundleCleaned(self, bundleId):
        cmd = 'SELECT BundleID FROM BundlesInfo WHERE BundleID = "{bundleId}" AND Flags & {flag} = {flag};'.format(
            bundleId=bundleId, flag=self.BUNDLE_FLAGS["Cleaned"] 
        )

        result = self._query(cmd)

        if not result["OK"]:
            return result
        
        cleaned = result["Value"] != []

        return S_OK(cleaned) 

    #############################################################################

    def getWholeBundle(self, bundleId):
        result = self.getFields(self.BUNDLES_INFO_TABLE, [], {"BundleID": bundleId})

        if not result["OK"]:
            return result

        if not result["Value"]:
            return S_ERROR(f"No bundle with id {bundleId}")

        bundleDict = formatSelectOutput(result["Value"], self.BUNDLES_INFO_COLUMNS)[0]
        bundleDict["Status"] = self.STATUS_MAP[bundleDict["Status"]]

        self.log.debug(f"Look at this cool bundle: {bundleDict}")

        return S_OK(bundleDict)

    def getBundleCE(self, bundleId):
        result = self.getFields(self.BUNDLES_INFO_TABLE, ["CEDict", "ProxyPath"], {"BundleID": bundleId})

        if not result["OK"]:
            return result

        return S_OK(formatSelectOutput(result["Value"], ["CEDict", "ProxyPath"])[0])

    #############################################################################

    def _reduceProcessorSum(self, bundleId, nProcessors):
        cmd = 'UPDATE BundlesInfo SET ProcessorSum = ProcessorSum - {nProcs} WHERE BundleID = "{bundleId}";'.format(
            bundleId=bundleId, nProcs=nProcessors
        )
        return self._query(cmd)

    def _createNewBundle(self, ceDict, proxyPath):
        if "ExecTemplate" not in ceDict:
            return S_ERROR("CE must have a properly formatted ExecTemplate")

        timestamp = datetime.now(tz=timezone.utc).strftime(self.MYSQL_DATETIME_FORMAT)
        
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
            "FirstTimestamp": timestamp,
            "LastTimestamp": timestamp,
        }

        result = self.insertFields(self.BUNDLES_INFO_TABLE, list(insertInfo.keys()), list(insertInfo.values()))

        if not result["OK"]:
            return result

        return S_OK(bundleId)

    def _insertJobInBundle(self, jobId, bundleId, executable, inputs, outputs, nProcessors, proxyPath, diracId):
        timestamp = datetime.now(tz=timezone.utc).strftime(self.MYSQL_DATETIME_FORMAT)

        # Insert the job into the bundle
        insertInfo = {
            "JobID": jobId,
            "BundleID": bundleId,
            "ExecutablePath": executable,
            "Outputs": str(outputs),
            "Processors": nProcessors,
        }

        if diracId:
            insertInfo["DiracID"] = diracId

        result = self.insertFields(self.JOB_TO_BUNDLE_TABLE, list(insertInfo.keys()), list(insertInfo.values()))

        if not result["OK"]:
            return result

        # Insert the Inputs
        for _input in inputs:
            insertInfo = {
                "JobID": jobId,
                "InputPath": _input,
            }

            result = self.insertFields(self.JOB_INPUTS_TABLE, list(insertInfo.keys()), list(insertInfo.values()))

            if not result["OK"]:
                return result

        # Modify the number of processors that will be used by the bundle
        cmd = """\
        UPDATE BundlesInfo 
        SET ProcessorSum = ProcessorSum + {nProcs}, LastTimestamp = "{timestamp}" 
        WHERE BundleID = "{bundleId}";
        """.format(
            bundleId=bundleId, nProcs=nProcessors, timestamp=timestamp
        )
        result = self._query(cmd)

        if not result["OK"]:
            return result
            
        # Obtain the info to be returned to the Service
        result = self.getFields(
            self.BUNDLES_INFO_TABLE, 
            ["ProcessorSum", "MaxProcessors", "Status", "FirstTimestamp", "LastTimestamp"], 
            {"BundleID": bundleId}
        )

        if not result["OK"]:
            return result

        selection = formatSelectOutput(
            result["Value"], 
            ["ProcessorSum", "MaxProcessors", "Status", "FirstTimestamp", "LastTimestamp"]
        )
        selection = selection[0]
        
        ready = selection["ProcessorSum"] == selection["MaxProcessors"]

        return S_OK({"BundleId": bundleId, "Ready": ready})

    def _getBundlesFromCEDict(self, ceDict):
        cmd = 'SELECT * FROM BundlesInfo WHERE Site = "{Site}" AND CE = "{CE}" AND Queue = "{Queue}";'.format(
            Site=ceDict["Site"],
            CE=ceDict["GridCE"],
            Queue=ceDict["Queue"],
        )
        result = self._query(cmd)

        if not result["OK"]:
            return result

        if not result["Value"]:
            return S_OK([])

        retVal = formatSelectOutput(
            result["Value"],
            self.BUNDLES_INFO_COLUMNS,
        )
        return S_OK(retVal)

    def _updateBundleStatus(self, bundleId, newStatus):
        if newStatus not in self.STATUS_MAP.keys():
            msg = f"The new status '{newStatus}' does not correspond with the possible statuses:"
            return S_ERROR(msg, self.STATUS_MAP.keys())

        cmd = 'UPDATE BundlesInfo SET Status = "{status}" WHERE BundleID = "{bundleId}";'.format(
            bundleId=bundleId, status=newStatus
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
            status = bundle["Status"]

            newProcSum = procs + nProcessors

            if status != "Storing":
                continue

            if newProcSum == maxProcs:
                return bundleId

            if newProcSum > currentBestProcs:
                currentBestProcs = newProcSum
                bestBundleId = bundleId

        return bestBundleId
