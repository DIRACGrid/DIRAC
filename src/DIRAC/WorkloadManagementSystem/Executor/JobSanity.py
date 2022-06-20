"""
  The Job Sanity executor screens jobs for the following problems:

     - Output data already exists
     - Problematic JDL
     - Jobs with too much input data e.g. > 100 files
     - Jobs with input data incorrectly specified e.g. castor:/
     - Input sandbox not correctly uploaded.
"""

from DIRAC import S_OK, S_ERROR
from DIRAC.Core.Utilities.ClassAd import ClassAd
from DIRAC.WorkloadManagementSystem.Client.SandboxStoreClient import SandboxStoreClient
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
        cls.sandboxClient = SandboxStoreClient(useCertificates=True, smdb=True)
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
        jobDescription = result["Value"]

        # Input Sandbox check
        if self.ex_getOption("InputSandboxCheck", True):
            result = self.assignSandboxes(jid, jobDescription)
            if not result["OK"]:
                return result

        return self.setNextOptimizer(jobState)

    def assignSandboxes(self, jid, jobDescription: ClassAd):
        """The number of input sandbox files, as specified in the job
        JDL are checked in the JobDB.
        """

        ownerName = jobDescription.getAttributeString("Owner")
        ownerGroup = jobDescription.getAttributeString("OwnerGroup")
        jobSetup = jobDescription.getAttributeString("DIRACSetup")
        inputSandboxes = jobDescription.getListFromExpression("InputSandbox")
        sbsToAssign = []
        for isb in inputSandboxes:
            if isb.startswith("SB:"):
                self.jobLog.debug("Found a sandbox", isb)
                sbsToAssign.append((isb, "Input"))
        numSBsToAssign = len(sbsToAssign)
        if not numSBsToAssign:
            return S_OK(0)
        self.jobLog.info("Assigning sandboxes", f"({numSBsToAssign} on behalf of {ownerName}@{ownerGroup})")
        result = self.sandboxClient.assignSandboxesToJob(jobState.jid, sbsToAssign, ownerName, ownerGroup)
        if not result["OK"]:
            self.jobLog.error("Could not assign sandboxes in the SandboxStore")
            return S_ERROR("Cannot assign sandbox to job")
        assigned = result["Value"]
        if assigned != numSBsToAssign:
            self.jobLog.error("Could not assign all sandboxes", f"({numSBsToAssign}). Only assigned {assigned}")
        return S_OK(numSBsToAssign)
