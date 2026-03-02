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
            "TaskID",
            "Status",
            "ProxyPath",
            "Flags",
            "FirstTimestamp",
            "LastTimestamp",
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

        self.MYSQL_DATETIME_FORMAT = "%Y-%m-%d %H:%M:%S"

        self.BUNDLE_FLAGS = {
            "Cleaned": 1,
            "Purged": 1 << 1,
        }

    @property
    def log(self):
        return contextLogger.get() or self._defaultLogger

    @log.setter
    def log(self, value):
        self._defaultLogger = value

    #############################################################################

    def insertJobToBundle(self, jobId, executable, inputs, outputs, processors, ceDict, proxyPath, diracId):
        """Inserts a new job in a new or existing Bundle depending of the CE to be submitted."""
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
            result = self._insertJobInBundle(jobId, bundleId, executable, inputs, outputs, processors, diracId)

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
        result = self._insertJobInBundle(jobId, bundleId, executable, inputs, outputs, processors, diracId)

        if not result["OK"]:
            return result

        return S_OK({"BundleId": bundleId, "Ready": result["Value"]["Ready"]})

    def removeJobsFromBundle(self, jobIds):
        """Receives a list of DIRAC JobIds, matches them to their corresponding bundle and removes them."""
        if not isinstance(jobIds, list):
            jobIds = list(jobIds)

        for jobId in jobIds:
            result = self.getFields(self.JOB_TO_BUNDLE_TABLE, ["BundleID", "Processors"], {"JobID": jobId})

            if not result["OK"]:
                return result

            jobInfo = result["Value"][0]
            bundleId = jobInfo[0]
            nProcs = jobInfo[1]

            result = self._reduceProcessorSum(bundleId, nProcs)

            if not result["OK"]:
                return result

        result = self.deleteEntries(self.JOB_TO_BUNDLE_TABLE, {"JobID": jobIds})
        return S_OK(result)

    #############################################################################

    def getUnpurgedBundles(self):
        """Obtains the list of Bundles that inputs haven't been removed locally."""
        cmd = 'SELECT BundleID FROM BundlesInfo WHERE Status = "{status}" AND Flags & {flag} != {flag};'.format(
            status=PilotStatus.DONE, flag=self.BUNDLE_FLAGS["Purged"]
        )

        result = self._query(cmd)

        if not result["OK"]:
            return result

        return S_OK([entry[0] for entry in result["Value"]])

    def isBundleCleaned(self, bundleId):
        """Check if ce.cleanJob has been performed properly."""
        cmd = 'SELECT BundleID FROM BundlesInfo WHERE BundleID = "{bundleId}" AND Flags & {flag} = {flag};'.format(
            bundleId=bundleId, flag=self.BUNDLE_FLAGS["Cleaned"]
        )

        result = self._query(cmd)

        if not result["OK"]:
            return result

        cleaned = result["Value"] != []

        return S_OK(cleaned)

    def getWaitingBundles(self):
        return self._getBundlesWithStatus(PilotStatus.WAITING)

    def getRunningBundles(self):
        return self._getBundlesWithStatus(PilotStatus.RUNNING)

    def _getBundlesWithStatus(self, status):
        """Get Bundles that match certain status."""
        result = self.getFields(self.BUNDLES_INFO_TABLE, self.BUNDLES_INFO_COLUMNS, {"Status": status})

        if not result["OK"]:
            return result

        bundlesDict = formatSelectOutput(result["Value"], self.BUNDLES_INFO_COLUMNS)
        return S_OK(bundlesDict)

    #############################################################################

    def getBundleIdFromJobId(self, jobId):
        """Returns the BundleId that corresponds to a DIRAC JobId."""
        result = self.getFields(self.JOB_TO_BUNDLE_TABLE, ["BundleID"], {"JobID": jobId})

        if not result["OK"]:
            return result

        if not result["Value"]:
            return S_ERROR("JobId not present in any bundle")

        return S_OK(result["Value"][0][0])

    def getBundleStatus(self, bundleId):
        """Obtain the status of the Bundle."""
        result = self.getFields(self.BUNDLES_INFO_TABLE, ["Status"], {"BundleID": bundleId})

        if not result["Value"]:
            return S_ERROR("Failed to get bundle Status")

        return S_OK(result["Value"][0][0])

    # TODO: This whole function is incomprehensible, needs to be split in 2
    def getJobsOfBundle(self, bundleId, noInputs=False):
        """Get every Job that comprise a Bundle."""
        if noInputs:
            cmd = """\
            SELECT JobID, DiracID, ExecutablePath, Outputs, Processors
            FROM JobToBundle
            WHERE BundleID = "{bundleId}";""".format(
                bundleId=bundleId
            )
        else:
            cmd = """\
            SELECT JobToBundle.JobID, DiracID, ExecutablePath, Outputs, Processors, InputPath
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

        # For each row (JobID, ExecutablePath, Outputs, Processors, [InputPath])
        for row in rows:
            # The job has no input
            if len(row) == len(self.JOB_TO_BUNDLE_COLUMNS) - 1:  # All columns except BundleID
                jobID, diracId, jobExecutablePath, jobOutputs, processors = row
                jobInputPath = ""
            else:  # All columns except BundleID but with the inputs
                jobID, diracId, jobExecutablePath, jobOutputs, processors, jobInputPath = row

            if jobID not in retVal:
                retVal[jobID] = {
                    "ExecutablePath": jobExecutablePath,
                    "DiracID": diracId,
                    "Outputs": [],
                    "Processors": processors,
                }

                if not noInputs:
                    retVal[jobID]["Inputs"] = []

            retVal[jobID]["Outputs"].extend(literal_eval(jobOutputs))

            if jobInputPath:
                retVal[jobID]["Inputs"].append(jobInputPath)

        return S_OK(retVal)

    def getJobIDsOfBundle(self, bundleId):
        """Returns the list of JobIds that are contained in a bundle"""
        result = self.getFields(self.JOB_TO_BUNDLE_TABLE, ["JobID"], {"BundleID": bundleId})

        if not result["OK"]:
            return result

        return S_OK([entry[0] for entry in result["Value"]])

    def removeJobInputs(self, jobIds):
        """Removes the contents of the JobInputs table for each corresponding JobID."""
        if not isinstance(jobIds, list):
            jobIds = [jobIds]

        return self.deleteEntries(self.JOB_INPUTS_TABLE, {"JobID": jobIds})

    #############################################################################

    def setTaskId(self, bundleId, taskId):
        """Sets the value of the TaskID generetad by the real CE during Bundle submission."""
        result = self.updateFields(
            self.BUNDLES_INFO_TABLE, ["TaskID", "Status"], [taskId, PilotStatus.RUNNING], {"BundleID": bundleId}
        )
        return result

    def getTaskId(self, bundleId):
        """Returns the value of the TaskId stored."""
        result = self.getFields(self.BUNDLES_INFO_TABLE, ["TaskID"], {"BundleID": bundleId})

        if not result["OK"]:
            return result

        return S_OK(result["Value"][0][0])

    #############################################################################

    def setBundleAsDone(self, bundleId):
        result = self._updateBundleStatus(bundleId, PilotStatus.DONE)
        return result

    def setBundleAsFailed(self, bundleId):
        result = self._updateBundleStatus(bundleId, PilotStatus.FAILED)
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

    #############################################################################

    def getWholeBundle(self, bundleId):
        result = self.getFields(self.BUNDLES_INFO_TABLE, [], {"BundleID": bundleId})

        if not result["OK"]:
            return result

        if not result["Value"]:
            return S_ERROR(f"No bundle with id {bundleId}")

        bundleDict = formatSelectOutput(result["Value"], self.BUNDLES_INFO_COLUMNS)[0]

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
        """Initialize a new Bundle."""
        timestamp = datetime.now(tz=timezone.utc).strftime(self.MYSQL_DATETIME_FORMAT)

        bundleId = uuid.uuid4().hex
        insertInfo = {
            "BundleID": bundleId,
            "ProcessorSum": 0,
            "MaxProcessors": ceDict["NumberOfProcessors"],
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

    def _insertJobInBundle(self, jobId, bundleId, executable, inputs, outputs, nProcessors, diracId):
        """Add the info of a Job to a Bundle."""
        timestamp = datetime.now(tz=timezone.utc).strftime(self.MYSQL_DATETIME_FORMAT)

        # Job Insertion
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

        # TODO: Move all of this out of the function
        # Obtain the info to be returned to the Service
        result = self.getFields(
            self.BUNDLES_INFO_TABLE,
            ["ProcessorSum", "MaxProcessors", "Status", "FirstTimestamp", "LastTimestamp"],
            {"BundleID": bundleId},
        )

        if not result["OK"]:
            return result

        selection = formatSelectOutput(
            result["Value"], ["ProcessorSum", "MaxProcessors", "Status", "FirstTimestamp", "LastTimestamp"]
        )[0]

        ready = selection["ProcessorSum"] == selection["MaxProcessors"]

        return S_OK({"BundleId": bundleId, "Ready": ready})

    def _getBundlesFromCEDict(self, ceDict):
        """Returns the bundles that match a CE (Site, CE and Queue)."""
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
        """Changes the status of a Bundle."""
        cmd = 'UPDATE BundlesInfo SET Status = "{status}" WHERE BundleID = "{bundleId}";'.format(
            bundleId=bundleId, status=newStatus
        )
        result = self._query(cmd)

        if not result["OK"]:
            return result

        return S_OK()

    def __selectBestBundle(self, bundles, nProcessors):
        """Return the BundleID of the best match from a list of bundles and the number of processors requested."""
        bestBundleId = None
        currentBestProcs = 0

        for bundle in bundles:
            bundleId = bundle["BundleID"]
            procs = bundle["ProcessorSum"]
            maxProcs = bundle["MaxProcessors"]
            status = bundle["Status"]

            newProcSum = procs + nProcessors

            if status != PilotStatus.WAITING:
                continue

            if newProcSum == maxProcs:
                return bundleId

            if newProcSum > currentBestProcs:
                currentBestProcs = newProcSum
                bestBundleId = bundleId

        return bestBundleId
