"""
The OptimizerAdministrator determines the chain of Optimizing Agents
that must work on the job prior to the scheduling decision and
launches thoses agents

Initially this takes jobs in the received state and starts the jobs on the
optimizer chain.
It ends by setting the job in the Waiting state
"""

from DIRAC import gLogger
from DIRAC.Core.Utilities import List
from DIRAC.Core.Utilities.ObjectLoader import ObjectLoader
from DIRAC.WorkloadManagementSystem.CheckerAdminstrator.Checker import Checker
from DIRAC.WorkloadManagementSystem.Client import JobStatus
from DIRAC.WorkloadManagementSystem.Client.JobState.JobState import JobState


class CheckerAdministrator:
    """
    CheckerAdministratorf gets the names of the checkers to run, load them and run them.
    """

    def __init__(self, jobState: JobState):
        self.jobState = jobState
        self.log = gLogger.getSubLogger(f"[jid={jobState.jid}]{self.__class__.__name__}")

    def check(self):
        """
        Gets the optimizers agents to launch from getCheckers, launches then
        and set the JobStatus to CHECKED if no error has occured during optimizing.
        It then launch the CheckedAdministrator to determine if some file needs staging
        or if the job can be put to the waiting status.

        :return: S_OK() / S_ERROR() if the status couldn't be changed (should always return S_OK())
        """

        # Getting the optimizers in an ordered array
        checkers = self.getCheckers()

        # Launching every checker until all are checked or one returns S_ERROR
        for checker in checkers:
            result = self.jobState.setStatus(JobStatus.CHECKING, source=checker.__class__.__name__)
            if not result["OK"]:
                return result

            result = checker.check()
            if not result["OK"]:
                self.log.error("An error has occured while checking the job")
                return self.jobState.setStatus(
                    JobStatus.FAILED,
                    minorStatus=result["Message"],
                    appStatus="Unknown",
                    source=self.__class__.__name__,
                )

        self.log.error("Checking completed successfully")
        return self.jobState.setStatus(
            JobStatus.CHECKING,
            source=self.__class__.__name__,
        )

    def getCheckers(self) -> list[Checker]:
        """
        Gets a list of the checker names to launch from getCheckPath,
        loads each one in a list and return them.

        :return: list[Checker]
        """
        objectLoader = ObjectLoader()
        checkers = []

        checkPath = self.getCheckPath()
        for optimizerName in checkPath:
            result = objectLoader.loadObject(f"DIRAC.WorkloadManagementSystem.Checker.{optimizerName}")
            if not result["OK"]:
                self.log.error(f"No optimizer named {optimizerName} was found")
                continue
            checkerClass = result["Value"]
            checkers.append(checkerClass(self.jobState))

        return checkers

    def getCheckPath(self) -> list[str]:
        """
        Tries to get the optimizer chain from the JobPath attribute of the manifest
        returns it as a list if founded.
        If not, the function will return the standard list of optimizers.


        :param jobState: JobState in the waiting state
        :return: list[str] (all the strings are unique in the list)
        """
        result = self.jobState.getManifest()
        if result["OK"]:
            manifest = result["Value"]
            if manifest.isOption("CheckPath"):
                checkPath = manifest.getOption("CheckPath", [])
                self.log.info("Job defines its own optimizer chain: ", checkPath)
                return List.fromChar(checkPath, ",")

        self.log.info("Using default jobPath")
        return ["", "InputData", "JobScheduling"]
