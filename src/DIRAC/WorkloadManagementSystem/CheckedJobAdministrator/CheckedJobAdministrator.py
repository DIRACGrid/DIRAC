from DIRAC import S_OK, gLogger
from DIRAC.Core.Utilities.ObjectLoader import ObjectLoader
from DIRAC.WorkloadManagementSystem.Client import JobStatus
from DIRAC.WorkloadManagementSystem.Client.JobState.JobState import JobState


class CheckedAdministrator:
    def __init__(self, jobState: JobState) -> None:
        self.jobState = jobState
        self.log = gLogger.getSubLogger(f"[jid={jobState.jid}]{__name__}")
        self.objectLoader = ObjectLoader()

    def handle(self):
        """
        Gets the handler agents to launch from getCheckedJobHandlers to
        determine the job status after the CHECKED state.

        :return: S_OK() / S_ERROR()
        """

        # Getting the optimizers in an ordered array
        checkedJobHandlers = self.getCheckedJobHandlers()

        # Launching the first handler of the chain
        checkedJobHandlers[0].handle()

        if not result["OK"]:
            self.log.error("An error has occured while trying to determine and set the job status after checked")
            result = self.jobState.setStatus(
                JobStatus.FAILED,
                minorStatus=result["Message"],
                appStatus="Unknown",
                source=self.__class__.__name__,
            )
            if not result["OK"]:
                return result

        return S_OK()

    def getCheckedJobHandlers(self):
        """
        Gets the staging and waiting handers, instanciates them and return them.

        :returns: S_OK(list[Handlers]) / S_ERROR()
        """

        moduleName = "DIRAC.WorkloadManagementSystem.CheckedAdministrator"

        result = self.objectLoader.loadObject(moduleName, "StagingHandler")
        if not result["OK"]:
            return result
        stagingHandler = result["Value"](self.jobState)

        result = self.objectLoader.loadObject(moduleName, "WaitingHandler")
        if not result["OK"]:
            return result
        waitingHandler = result["Value"](self.jobState)

        stagingHandler.setNext(waitingHandler)

        return S_OK([stagingHandler, waitingHandler])
