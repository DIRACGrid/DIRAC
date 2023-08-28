""" Base class for all the executor modules for Jobs Optimization
"""
import threading
import datetime  # Because eval(valenc) might require it
from DIRAC import S_OK, S_ERROR
from DIRAC.Core.Utilities import DEncode, List
from DIRAC.Core.Base.ExecutorModule import ExecutorModule
from DIRAC.WorkloadManagementSystem.Client.JobState.CachedJobState import CachedJobState
from DIRAC.WorkloadManagementSystem.Client import JobStatus, JobMinorStatus


class OptimizerExecutor(ExecutorModule):
    @classmethod
    def initialize(cls):
        opName = cls.ex_getProperty("fullName")
        opName = "/".join(opName.split("/")[1:])
        if opName.find("Agent") == len(opName) - 5:
            opName = opName[:-5]
        cls.__optimizerName = opName
        maxTasks = cls.ex_getOption("Tasks", 1)
        cls.__jobData = threading.local()

        cls.__jobData.jobState = None
        cls.__jobData.jobLog = None
        cls.ex_setProperty("optimizerName", cls.__optimizerName)
        try:
            result = cls.initializeOptimizer()
            if not result["OK"]:
                return result
        except Exception as excp:
            cls.log.exception("Error while initializing optimizer")
            return S_ERROR(f"Error initializing: {str(excp)}")
        cls.ex_setMind("WorkloadManagement/OptimizationMind")

        return S_OK()

    @classmethod
    def ex_optimizerName(cls):
        return cls.__optimizerName

    @classmethod
    def initializeOptimizer(cls):
        return S_OK()

    def processTask(self, jid, jobState):
        self.__jobData.jobState = jobState
        self.__jobData.jobLog = self.log.getSubLogger(f"{self.ex_optimizerName()}/JID_{jid}")
        try:
            self.jobLog.info("Processing")
            optResult = self.optimizeJob(jid, jobState)
            # If the manifest is dirty, update it!
            result = jobState.getManifest()
            if not result["OK"]:
                self.jobLog.error("Failed to get job manifest", result["Message"])
                return result
            manifest = result["Value"]
            if manifest.isDirty():
                jobState.setManifest(manifest)
            # Did it go as expected? If not Failed!
            if not optResult["OK"]:
                self.jobLog.info(f"Set to Failed/{optResult['Message']}")
                minorStatus = f"{self.ex_optimizerName()} optimizer"
                return jobState.setStatus("Failed", minorStatus, optResult["Message"], source=self.ex_optimizerName())

            return S_OK()
        finally:
            self.__jobData.jobState = None
            self.__jobData.jobLog = None

    def optimizeJob(self, jid, jobState):
        raise Exception("You need to overwrite this method to optimize the job!")

    def setNextOptimizer(self, jobState=None):
        if not jobState:
            jobState = self.__jobData.jobState
        result = jobState.getOptParameter("OptimizerChain")
        if not result["OK"]:
            return result
        opChain = List.fromChar(result["Value"], ",")
        opName = self.__optimizerName
        try:
            opIndex = opChain.index(opName)
        except ValueError:
            return S_ERROR(f"Optimizer {opName} is not in the chain!")
        chainLength = len(opChain)
        if chainLength - 1 == opIndex:
            # This is the last optimizer in the chain!
            result = jobState.setStatus(
                JobStatus.WAITING, minorStatus=JobMinorStatus.PILOT_AGENT_SUBMISSION, appStatus="Unknown", source=opName
            )
            if not result["OK"]:
                return result

            result = jobState.insertIntoTQ()
            if not result["OK"]:
                return result

            return S_OK()
        # Keep optimizing!
        nextOp = opChain[opIndex + 1]
        self.jobLog.info(f"Set to Checking/{nextOp}")
        return jobState.setStatus(JobStatus.CHECKING, nextOp, source=opName)

    def storeOptimizerParam(self, name, value):
        if not self.__jobData.jobState:
            return S_ERROR("This function can only be called inside the optimizeJob function")
        valenc = DEncode.encode(value)
        return self.__jobData.jobState.setOptParameter(name, valenc)

    def retrieveOptimizerParam(self, name):
        if not self.__jobData.jobState:
            return S_ERROR("This function can only be called inside the optimizeJob function")
        result = self.__jobData.jobState.getOptParameter(name)
        if not result["OK"]:
            return result
        valenc = result["Value"]
        try:
            if not isinstance(valenc, bytes):
                valenc = valenc.encode()
            value, encLength = DEncode.decode(valenc)
            if encLength == len(valenc):
                return S_OK(value)
        except Exception:
            self.jobLog.exception(f"Opt param {name} doesn't seem to be dencoded {valenc!r}")
        return S_OK(eval(valenc))

    @property
    def jobLog(self):
        if not self.__jobData.jobLog:
            raise RuntimeError("jobLog can only be invoked inside the optimizeJob function")
        return self.__jobData.jobLog

    def deserializeTask(self, taskStub):
        return CachedJobState.deserialize(taskStub)

    def serializeTask(self, cjs):
        return S_OK(cjs.serialize())

    def fastTrackDispatch(self, jid, jobState):
        result = jobState.getStatus()
        if not result["OK"]:
            return S_ERROR(f"Could not retrieve job status for {jid}: {result['Message']}")
        status, minorStatus = result["Value"]
        if status != JobStatus.CHECKING:
            self.log.info(f"[JID {jid}] Not in checking state. Avoid fast track")
            return S_OK()
        result = jobState.getOptParameter("OptimizerChain")
        if not result["OK"]:
            return S_ERROR(f"Could not retrieve OptimizerChain for job {jid}: {result['Message']}")
        optChain = result["Value"]
        if minorStatus not in optChain:
            self.log.info(f"[JID {jid}] End of chain for job")
            return S_OK()
        self.log.info(f"[JID {jid}] Fast track possible to {minorStatus}")
        return S_OK(f"WorkloadManagement/{minorStatus}")
