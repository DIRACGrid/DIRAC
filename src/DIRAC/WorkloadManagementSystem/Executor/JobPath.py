"""
  The Job Path executor determines the chain of Optimizing Agents that must
  work on the job prior to the scheduling decision.

  Initially this takes jobs in the received state and starts the jobs on the
  optimizer chain.
"""
from DIRAC import S_OK, S_ERROR
from DIRAC.Core.Utilities import List
from DIRAC.WorkloadManagementSystem.Executor.Base.OptimizerExecutor import OptimizerExecutor


class JobPath(OptimizerExecutor):
    """
    The specific Optimizer must provide the following methods:
    - optimizeJob() - the main method called for each job
    and it can provide:
    - initializeOptimizer() before each execution cycle
    """

    @classmethod
    def initializeOptimizer(cls):
        return S_OK()

    def __setOptimizerChain(self, jobState, opChain):
        if not isinstance(opChain, str):
            opChain = ",".join(opChain)
        return jobState.setOptParameter("OptimizerChain", opChain)

    def optimizeJob(self, jid, jobState):
        result = jobState.getManifest()
        if not result["OK"]:
            self.jobLog.error("Failed to get job manifest", result["Message"])
            return result
        jobManifest = result["Value"]
        opChain = jobManifest.getOption("JobPath", [])
        if opChain:
            self.jobLog.info("Job defines its own optimizer chain", opChain)
        else:
            # Construct path
            opChain = self.ex_getOption("BasePath", ["JobPath", "JobSanity"])

            result = jobState.getInputData()
            if not result["OK"]:
                self.jobLog.error("Failed to get input data", result["Message"])
                return result
            if result["Value"]:
                # if the returned tuple is not empty it will evaluate true
                self.jobLog.info("Input data requirement found")
                opChain.extend(self.ex_getOption("InputData", ["InputData"]))
            else:
                self.jobLog.info("No input data requirement")

            # End of path
            opChain.extend(self.ex_getOption("EndPath", ["JobScheduling"]))
            uPath = []
            for opN in opChain:
                if opN not in uPath:
                    uPath.append(opN)
            opChain = uPath
            self.jobLog.info("Constructed path is", "%s" % "->".join(opChain))

        result = self.__setOptimizerChain(jobState, opChain)
        if not result["OK"]:
            self.jobLog.error("Failed to set optimizer chain", result["Message"])
            return result
        return self.setNextOptimizer(jobState)
