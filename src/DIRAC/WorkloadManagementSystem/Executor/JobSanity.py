""" The Job Sanity executor assigns sandboxes to the job """
from DIRAC import S_OK
from DIRAC.WorkloadManagementSystem.DB.SandboxMetadataDB import SandboxMetadataDB
from DIRAC.WorkloadManagementSystem.Executor.Base.OptimizerExecutor import OptimizerExecutor


class JobSanity(OptimizerExecutor):
    """
    The specific Optimizer must provide the following methods:
      - optimizeJob() - the main method called for each job
    and it can provide:
      - initializeOptimizer() before each execution cycle
    """

    @classmethod
    def initializeOptimizer(cls):
        """Initialize specific parameters for JobSanityAgent."""
        return S_OK()

    def optimizeJob(self, jid, jobState):
        """This method controls the order and presence of
        each sanity check for submitted jobs. This should
        be easily extended in the future to accommodate
        any other potential checks.
        """

        result = jobState.getManifest()
        if not result["OK"]:
            self.jobLog.error("Failed to get job manifest", result["Message"])
            return result
        manifest = result["Value"]

        # Input Sandbox check
        if self.ex_getOption("InputSandboxCheck", True):
            result = self.checkInputSandbox(jobState, manifest)
            if not result["OK"]:
                self.jobLog.error("Failed to check input sandbox", result["Message"])
                return result
            self.jobLog.info("Assigned ISBs", result["Value"])

        return self.setNextOptimizer(jobState)

    def checkInputSandbox(self, jobState, manifest):
        """The number of input sandbox files, as specified in the job
        JDL are checked in the JobDB.
        """
        ownerName = manifest.getOption("Owner")
        ownerGroup = manifest.getOption("OwnerGroup")
        isbList = manifest.getOption("InputSandbox", [])
        vo = manifest.getOption("VirtualOrganization")
        sbsToAssign = []
        for isb in isbList:
            if isb.startswith("SB:"):
                sbsToAssign.append((isb, "Input"))
        numSBsToAssign = len(sbsToAssign)
        if not numSBsToAssign:
            return S_OK(0)
        self.jobLog.info("Assigning sandboxes", f"({numSBsToAssign} on behalf of {ownerName}@{ownerGroup}@{vo})")
        eId = f"Job:{jobState.jid}"
        result = SandboxMetadataDB().assignSandboxesToEntities({eId: sbsToAssign}, ownerName, ownerGroup)
        if not result["OK"]:
            self.jobLog.error("Could not assign sandboxes in the SandboxStore")
            return result
        assigned = result["Value"]
        if assigned != numSBsToAssign:
            self.jobLog.error("Could not assign all sandboxes", f"({numSBsToAssign}). Only assigned {assigned}")
        return S_OK(numSBsToAssign)
