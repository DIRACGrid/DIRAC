""" The Matcher service provides an interface for matching jobs to pilots

    It uses a Matcher and a Limiter object that encapsulates the matching logic.
    It connects to JobDB, TaskQueueDB, JobLoggingDB, and PilotAgentsDB.
"""
from DIRAC import S_ERROR, S_OK
from DIRAC.ConfigurationSystem.Client.Helpers.Operations import Operations
from DIRAC.Core.DISET.RequestHandler import RequestHandler
from DIRAC.Core.Utilities import DErrno
from DIRAC.Core.Utilities.DEncode import ignoreEncodeWarning
from DIRAC.Core.Utilities.ObjectLoader import ObjectLoader
from DIRAC.WorkloadManagementSystem.Client.Limiter import Limiter
from DIRAC.WorkloadManagementSystem.Client.Matcher import Matcher, PilotVersionError


class MatcherHandlerMixin:
    @classmethod
    def initializeHandler(cls, serviceInfoDict):
        try:
            result = ObjectLoader().loadObject("WorkloadManagementSystem.DB.JobDB", "JobDB")
            if not result["OK"]:
                return result
            cls.jobDB = result["Value"](parentLogger=cls.log)

            result = ObjectLoader().loadObject("WorkloadManagementSystem.DB.JobLoggingDB", "JobLoggingDB")
            if not result["OK"]:
                return result
            cls.jobLoggingDB = result["Value"](parentLogger=cls.log)

            result = ObjectLoader().loadObject("WorkloadManagementSystem.DB.TaskQueueDB", "TaskQueueDB")
            if not result["OK"]:
                return result
            cls.taskQueueDB = result["Value"](parentLogger=cls.log)

            result = ObjectLoader().loadObject("WorkloadManagementSystem.DB.PilotAgentsDB", "PilotAgentsDB")
            if not result["OK"]:
                return result
            cls.pilotAgentsDB = result["Value"](parentLogger=cls.log)

        except RuntimeError as excp:
            return S_ERROR(f"Can't connect to DB: {excp}")

        cls.limiter = Limiter(jobDB=cls.jobDB)

        return S_OK()

    ##############################################################################
    types_requestJob = [[str, dict]]

    def export_requestJob(self, resourceDescription):
        """Serve a job to the request of an agent which is the highest priority
        one matching the agent's site capacity
        """

        credDict = self.getRemoteCredentials()
        pilotRef = resourceDescription.get("PilotReference", "Unknown")

        try:
            opsHelper = Operations(group=credDict["group"])
            matcher = Matcher(
                pilotAgentsDB=self.pilotAgentsDB,
                jobDB=self.jobDB,
                tqDB=self.taskQueueDB,
                jlDB=self.jobLoggingDB,
                opsHelper=opsHelper,
                pilotRef=pilotRef,
            )
            result = matcher.selectJob(resourceDescription, credDict)
        except RuntimeError as rte:
            self.log.error("Error requesting job for pilot", f"[{pilotRef}] {rte}")
            return S_ERROR("Error requesting job")
        except PilotVersionError as pve:
            self.log.warn("Pilot version error for pilot", f"[{pilotRef}] {pve}")
            return S_ERROR(DErrno.EWMSPLTVER, callStack=[])

        # result can be empty, meaning that no job matched
        if result:
            return S_OK(result)
        return S_ERROR(DErrno.EWMSNOMATCH, callStack=[])

    ##############################################################################
    types_getActiveTaskQueues = []

    @classmethod
    @ignoreEncodeWarning
    def export_getActiveTaskQueues(cls):
        """Return all task queues"""
        return cls.taskQueueDB.retrieveTaskQueues()

    ##############################################################################
    types_getMatchingTaskQueues = [dict]

    @classmethod
    @ignoreEncodeWarning
    def export_getMatchingTaskQueues(cls, resourceDict):
        """Return all task queues that match the resourceDict"""
        if "Site" in resourceDict and isinstance(resourceDict["Site"], str):
            gridCE = resourceDict.get("GridCE")
            negativeCond = cls.limiter.getNegativeCondForSite(resourceDict["Site"], gridCE)
        else:
            negativeCond = cls.limiter.getNegativeCond()
        matcher = Matcher(pilotAgentsDB=cls.pilotAgentsDB, jobDB=cls.jobDB, tqDB=cls.taskQueueDB, jlDB=cls.jobLoggingDB)
        resourceDescriptionDict = matcher._processResourceDescription(resourceDict)
        return cls.taskQueueDB.getMatchingTaskQueues(resourceDescriptionDict, negativeCond=negativeCond)


class MatcherHandler(MatcherHandlerMixin, RequestHandler):
    pass
