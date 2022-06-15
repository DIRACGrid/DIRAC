"""
  The Job Sanity executor screens jobs for the following problems:

     - Output data already exists
     - Problematic JDL
     - Jobs with too much input data e.g. > 100 files
     - Jobs with input data incorrectly specified e.g. castor:/
     - Input sandbox not correctly uploaded.
"""

from DIRAC import S_OK, S_ERROR

from DIRAC.ConfigurationSystem.Client.Helpers import Registry
from DIRAC.WorkloadManagementSystem.Client.JobState.JobManifest import JobManifest
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
        manifest = result["Value"]

        # Input Sandbox check
        if self.ex_getOption("InputSandboxCheck", True):
            result = self.checkInputSandbox(jobState, manifest)
            if not result["OK"]:
                self.jobLog.error("Failed to check input sandbox", result["Message"])
                return result
            self.jobLog.info("Assigned ISBs", result["Value"])

        return self.setNextOptimizer(jobState)

    def checkInputSandbox(self, jobState, manifest: JobManifest):
        """The number of input sandbox files, as specified in the job
        JDL are checked in the JobDB.
        """
        result = jobState.getAttributes(["Owner", "OwnerDN", "OwnerGroup"])
        if not result["OK"]:
            self.jobLog.error("Failed to get job attributes", result["Message"])
            return result
        attDict = result["Value"]
        ownerName = attDict["Owner"]
        if not ownerName:
            ownerDN = attDict["OwnerDN"]
            if not ownerDN:
                return S_ERROR("Missing OwnerDN")
            result = Registry.getUsernameForDN(ownerDN)
            if not result["OK"]:
                self.jobLog.error("Failed to get user name from DN", result["Message"])
                return result
            ownerName = result["Value"]
        ownerGroup = attDict["OwnerGroup"]
        if not ownerGroup:
            return S_ERROR("Missing OwnerGroup")

        isbList = manifest.getOption("InputSandbox", [])
        sbsToAssign = []
        for isb in isbList:
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
