"""
The OptimizerAdministrator determines the chain of Optimizing Agents
that must work on the job prior to the scheduling decision and
launches thoses agents

Initially this takes jobs in the received state and starts the jobs on the
optimizer chain.
It ends by setting the job in the Waiting state
"""
from DIRAC import S_OK, gLogger
from DIRAC.Core.Utilities import List
from DIRAC.Core.Utilities.ObjectLoader import ObjectLoader
from DIRAC.WorkloadManagementSystem.Client import JobMinorStatus, JobStatus
from DIRAC.WorkloadManagementSystem.Client.JobState.JobState import JobState
from DIRAC.WorkloadManagementSystem.OptimizerAdministrator.Optimizer import Optimizer


class OptimizerAdministrator:
    """
    OptimizerAdministrator gets the job optimization path, load the optimizers and run them.
    """

    def __init__(self, jobState: JobState):
        """Constructor"""
        self.jobState = jobState
        self.log = gLogger.getSubLogger(f"[jid={jobState.jid}]{__name__}")
        self.objectLoader = ObjectLoader()

    def optimize(self):
        """
        Gets the optimizers agents to launch from getOptimizers, launches then
        and set the JobStatus to Waiting if no error has occured during optimizing.

        :return: S_OK() / S_ERROR()
        """

        optimizers = self.getOptimizers()

        for optimizer in optimizers:
            result = optimizer.optimize()
            if not result["OK"]:
                self.log.error("An error has occured while optimizing")
                return result

        result = self.jobState.setStatus(
            JobStatus.WAITING,
            minorStatus=JobMinorStatus.PILOT_AGENT_SUBMISSION,
            appStatus="Unknown",
            source="OptimizerClient",
        )
        if not result["OK"]:
            self.log.error("Error while updating status")
            return result
        return S_OK()

    def getOptimizers(self) -> list[Optimizer]:
        """
        Gets a list of the optimizer names to launch from getOptimizersChain,
        loads each one in a list and return them.

        :return: list[Optimizers]
        """
        optimizers = []

        jobPath = self.getJobPath()
        for optimizerName in jobPath:
            result = self.objectLoader.loadObject(f"DIRAC.WorkloadManagementSystem.Optimizer.{optimizerName}")
            if not result["OK"]:
                self.log.error(f"No optimizer called {optimizerName} was found")
                continue
            optimizerClass = result["Value"]
            optimizers.append(optimizerClass(self.jobState))

        return optimizers

    def getJobPath(self) -> list[str]:
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
            if manifest.isOption("JobPath"):
                jobPath = manifest.getOption("JobPath", [])
                self.log.info("Job defines its own optimizer chain: ", jobPath)
                return List.fromChar(jobPath, ",")

        self.log.info("Using default jobPath")
        return ["JobSanity", "InputData", "JobScheduling"]
