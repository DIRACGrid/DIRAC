""" This object is a wrapper for setting and getting jobs states
"""
from DIRAC import S_ERROR, S_OK, gLogger
from DIRAC.WorkloadManagementSystem.Client import JobStatus
from DIRAC.WorkloadManagementSystem.Client.JobState.JobManifest import JobManifest
from DIRAC.WorkloadManagementSystem.DB.JobDB import JobDB
from DIRAC.WorkloadManagementSystem.DB.JobLoggingDB import JobLoggingDB
from DIRAC.WorkloadManagementSystem.DB.TaskQueueDB import TaskQueueDB, multiValueDefFields, singleValueDefFields
from DIRAC.WorkloadManagementSystem.Service.JobPolicy import (
    RIGHT_CHANGE_STATUS,
    RIGHT_GET_INFO,
    RIGHT_RESCHEDULE,
    RIGHT_RESET,
    RIGHT_SUBMIT,
)
from DIRAC.WorkloadManagementSystem.Utilities.JobStatusUtility import JobStatusUtility


class JobState:
    class DBHold:
        def __init__(self):
            self.checked = False
            self.reset()

        def reset(self):
            self.jobDB = None
            self.logDB = None
            self.tqDB = None

    __db = DBHold()

    @classmethod
    def checkDBAccess(cls):
        # Init DB if there
        if not JobState.__db.checked:
            JobState.__db.checked = True
            JobState.__db.jobDB = JobDB()
            JobState.__db.logDB = JobLoggingDB()
            JobState.__db.tqDB = TaskQueueDB()

    def __init__(self, jid):
        self.__jid = jid
        self.checkDBAccess()

    @property
    def jid(self):
        return self.__jid

    def getManifest(self, rawData=False):
        result = JobState.__db.jobDB.getJobJDL(self.__jid)
        if not result["OK"] or rawData:
            return result
        if not result["Value"]:
            return S_ERROR(f"No manifest for job {self.__jid}")
        manifest = JobManifest()
        result = manifest.loadJDL(result["Value"])
        if not result["OK"]:
            return result
        return S_OK(manifest)

    def setManifest(self, manifest):
        if not isinstance(manifest, JobManifest):
            manifestStr = manifest
            manifest = JobManifest()
            result = manifest.load(manifestStr)
            if not result["OK"]:
                return result
        manifestJDL = manifest.dumpAsJDL()
        return self.__retryFunction(5, JobState.__db.jobDB.setJobJDL, (self.__jid, manifestJDL))

    # Execute traces

    def __retryFunction(self, retries, functor, args=False, kwargs=False):
        retries = max(1, retries)
        if not args:
            args = tuple()
        if not kwargs:
            kwargs = {}
        while retries:
            retries -= 1
            result = functor(*args, **kwargs)
            if result["OK"]:
                return result
            if retries == 0:
                return result
        return S_ERROR("No more retries")

    right_commitCache = RIGHT_GET_INFO

    def commitCache(self, initialState, cache, jobLog):
        try:
            self.__checkType(initialState, dict)
            self.__checkType(cache, dict)
            self.__checkType(jobLog, (list, tuple))
        except TypeError as excp:
            return S_ERROR(str(excp))
        result = self.getAttributes(list(initialState))
        if not result["OK"]:
            return result
        if not result["Value"] == initialState:
            return S_OK(False)
        gLogger.verbose(f"Job {self.__jid}: About to execute trace. Current state {initialState}")

        data = {"att": [], "optp": []}
        for key in cache:
            for dk in data:
                if key.find(f"{dk}.") == 0:
                    data[dk].append((key[len(dk) + 1 :], cache[key]))

        if data["att"]:
            attN = [t[0] for t in data["att"]]
            attV = [t[1] for t in data["att"]]
            result = self.__retryFunction(
                5, JobState.__db.jobDB.setJobAttributes, (self.__jid, attN, attV), {"update": True}
            )
            if not result["OK"]:
                return result

        for k, v in data["optp"]:
            result = self.__retryFunction(5, JobState.__db.jobDB.setJobOptParameter, (self.__jid, k, v))
            if not result["OK"]:
                return result

        if "inputData" in cache:
            result = self.__retryFunction(5, JobState.__db.jobDB.setInputData, (self.__jid, cache["inputData"]))
            if not result["OK"]:
                return result

        gLogger.verbose("Adding logging records", f" for {self.__jid}")
        for record, updateTime, source in jobLog:
            gLogger.verbose("", f"Logging records for {self.__jid}: {record} {updateTime} {source}")
            record["date"] = updateTime
            record["source"] = source
            result = self.__retryFunction(5, JobState.__db.logDB.addLoggingRecord, (self.__jid,), record)
            if not result["OK"]:
                return result

        gLogger.info(f"Job {self.__jid}: Ended trace execution")
        # We return a new initial state
        return self.getAttributes(list(initialState))

    #
    # Status
    #

    def __checkType(self, value, tList, canBeNone=False):
        """Raise TypeError if the value does not have one of the expected types

        :param value: the value to test
        :param tList: type or tuple of types
        :param canBeNone: boolean, since there is no type for None to be used with isinstance

        """
        if canBeNone:
            if value is None:
                return
        if not isinstance(value, tList):
            raise TypeError(f"{value} has wrong type. Has to be one of {tList}")

    right_setStatus = RIGHT_GET_INFO

    def setStatus(self, majorStatus=None, minorStatus=None, appStatus=None, source=None):
        return JobStatusUtility(self.__db.jobDB, self.__db.logDB).setJobStatus(
            self.jid, status=majorStatus, minorStatus=minorStatus, appStatus=appStatus, source=source
        )

    def getStatus(self):
        result = JobState.__db.jobDB.getJobAttributes(self.__jid, ["Status", "MinorStatus"])
        if not result["OK"]:
            return result
        data = result["Value"]
        if data:
            return S_OK((data["Status"], data["MinorStatus"]))
        return S_ERROR("Job %d not found in the JobDB" % int(self.__jid))

    right_getAppStatus = RIGHT_GET_INFO

    def getAppStatus(self):
        result = JobState.__db.jobDB.getJobAttributes(self.__jid, ["ApplicationStatus"])
        if result["OK"]:
            result["Value"] = result["Value"]["ApplicationStatus"]
        return result

    # Attributes

    right_setAttribute = RIGHT_GET_INFO

    def setAttribute(self, name, value):
        try:
            self.__checkType(name, str)
            self.__checkType(value, str)
        except TypeError as excp:
            return S_ERROR(str(excp))
        return JobState.__db.jobDB.setJobAttribute(self.__jid, name, value)

    right_setAttributes = RIGHT_GET_INFO

    def setAttributes(self, attDict):
        try:
            self.__checkType(attDict, dict)
        except TypeError as excp:
            return S_ERROR(str(excp))
        keys = [key for key in attDict]
        values = [attDict[key] for key in keys]
        return JobState.__db.jobDB.setJobAttributes(self.__jid, keys, values)

    right_getAttribute = RIGHT_GET_INFO

    def getAttribute(self, name):
        try:
            self.__checkType(name, str)
        except TypeError as excp:
            return S_ERROR(str(excp))
        return JobState.__db.jobDB.getJobAttribute(self.__jid, name)

    right_getAttributes = RIGHT_GET_INFO

    def getAttributes(self, nameList=None):
        try:
            self.__checkType(nameList, (list, tuple), canBeNone=True)
        except TypeError as excp:
            return S_ERROR(str(excp))
        return JobState.__db.jobDB.getJobAttributes(self.__jid, nameList)

    # OptimizerParameters

    right_setOptParameter = RIGHT_GET_INFO

    def setOptParameter(self, name, value):
        try:
            self.__checkType(name, str)
            self.__checkType(value, str)
        except TypeError as excp:
            return S_ERROR(str(excp))
        return JobState.__db.jobDB.setJobOptParameter(self.__jid, name, value)

    right_setOptParameters = RIGHT_GET_INFO

    def setOptParameters(self, pDict):
        try:
            self.__checkType(pDict, dict)
        except TypeError as excp:
            return S_ERROR(str(excp))
        for name in pDict:
            result = JobState.__db.jobDB.setJobOptParameter(self.__jid, name, pDict[name])
            if not result["OK"]:
                return result
        return S_OK()

    right_removeOptParameters = RIGHT_GET_INFO

    def removeOptParameters(self, nameList):
        if isinstance(nameList, str):
            nameList = [nameList]
        try:
            self.__checkType(nameList, (list, tuple))
        except TypeError as excp:
            return S_ERROR(str(excp))
        for name in nameList:
            result = JobState.__db.jobDB.removeJobOptParameter(self.__jid, name)
            if not result["OK"]:
                return result
        return S_OK()

    right_getOptParameter = RIGHT_GET_INFO

    def getOptParameter(self, name):
        try:
            self.__checkType(name, str)
        except TypeError as excp:
            return S_ERROR(str(excp))
        return JobState.__db.jobDB.getJobOptParameter(self.__jid, name)

    right_getOptParameters = RIGHT_GET_INFO

    def getOptParameters(self, nameList=None):
        try:
            self.__checkType(nameList, (list, tuple), canBeNone=True)
        except TypeError as excp:
            return S_ERROR(str(excp))
        return JobState.__db.jobDB.getJobOptParameters(self.__jid, nameList)

    # Other

    right_resetJob = RIGHT_RESCHEDULE

    def rescheduleJob(self, source=""):
        result = JobState.__db.tqDB.deleteJob(self.__jid)
        if not result["OK"]:
            return S_ERROR(f"Cannot delete from TQ job {self.__jid}: {result['Message']}")
        result = JobState.__db.jobDB.rescheduleJob(self.__jid)
        if not result["OK"]:
            return S_ERROR(f"Cannot reschedule in JobDB job {self.__jid}: {result['Message']}")
        JobState.__db.logDB.addLoggingRecord(
            self.__jid, status=JobStatus.RECEIVED, minorStatus="", applicationStatus="", source=source
        )
        return S_OK()

    right_resetJob = RIGHT_RESET

    def resetJob(self, source=""):
        result = JobState.__db.jobDB.setJobAttribute(self.__jid, "RescheduleCounter", -1)
        if not result["OK"]:
            return S_ERROR(f"Cannot set the RescheduleCounter for job {self.__jid}: {result['Message']}")
        result = JobState.__db.tqDB.deleteJob(self.__jid)
        if not result["OK"]:
            return S_ERROR(f"Cannot delete from TQ job {self.__jid}: {result['Message']}")
        result = JobState.__db.jobDB.rescheduleJob(self.__jid)
        if not result["OK"]:
            return S_ERROR(f"Cannot reschedule in JobDB job {self.__jid}: {result['Message']}")
        JobState.__db.logDB.addLoggingRecord(
            self.__jid, status=JobStatus.RECEIVED, minorStatus="", applicationStatus="", source=source
        )
        return S_OK()

    right_getInputData = RIGHT_GET_INFO

    def getInputData(self):
        return JobState.__db.jobDB.getInputData(self.__jid)

    right_setInputData = RIGHT_SUBMIT

    def setInputData(self, inputData):
        return JobState.__db.jobDB.setInputData(self.__jid, inputData)

    right_insertIntoTQ = RIGHT_CHANGE_STATUS

    def insertIntoTQ(self, manifest=None):
        if not manifest:
            result = self.getManifest()
            if not result["OK"]:
                return result
            manifest = result["Value"]

        reqSection = "JobRequirements"

        result = manifest.getSection(reqSection)
        if not result["OK"]:
            return S_ERROR(f"No {reqSection} section in the job manifest")
        reqCfg = result["Value"]

        jobReqDict = {}
        for name in singleValueDefFields:
            if name in reqCfg:
                if name == "CPUTime":
                    jobReqDict[name] = int(reqCfg[name])
                else:
                    jobReqDict[name] = reqCfg[name]

        for name in multiValueDefFields:
            if name in reqCfg:
                jobReqDict[name] = reqCfg.getOption(name, [])

        jobPriority = reqCfg.getOption("UserPriority", 1)

        result = self.__retryFunction(2, JobState.__db.tqDB.insertJob, (self.__jid, jobReqDict, jobPriority))
        if not result["OK"]:
            errMsg = result["Message"]
            # Force removing the job from the TQ if it was actually inserted
            result = JobState.__db.tqDB.deleteJob(self.__jid)
            if result["OK"]:
                if result["Value"]:
                    gLogger.info(f"Job {self.__jid} removed from the TQ")
            return S_ERROR(f"Cannot insert in task queue: {errMsg}")
        return S_OK()
