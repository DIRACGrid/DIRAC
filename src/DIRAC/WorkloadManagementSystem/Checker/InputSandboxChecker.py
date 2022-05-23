"""
The Input Sandbox Checker screens jobs for the following problems:
    - Missing OwnerDN
    - Missing OwnerGroup
    - LFN sandboxes that starts with a /

The Input Sandbox Checker also assigns the sandboxes to the job
"""

from DIRAC import S_OK, gLogger
from DIRAC.ConfigurationSystem.Client.Helpers import Registry
from DIRAC.ConfigurationSystem.Client.Helpers.Operations import Operations
from DIRAC.Core.Utilities.ReturnValues import S_ERROR
from DIRAC.WorkloadManagementSystem.CheckerAdminstrator.Checker import Checker
from DIRAC.WorkloadManagementSystem.Client.JobState.JobState import JobState
from DIRAC.WorkloadManagementSystem.Client.SandboxStoreClient import SandboxStoreClient


class InputSandboxChecker(Checker):
    """Check input sandbox"""

    def __init__(self, jobState: JobState) -> None:
        super().__init__(jobState)
        self.log = gLogger.getSubLogger(f"[jid={jobState.jid}]{__name__}")
        self.operations = Operations()
        self.sandboxClient = SandboxStoreClient(useCertificates=True, smdb=True)

    def check(self):
        """
        This method is the method that will be launched by the CheckerAdministrator
        """

        # Check if input sandboxes should be checked
        if not self.operations.getValue("InputSandboxCheck", True):
            self.log.info("InputSandboxCheck option set to false. Skipping")
            return S_OK()

        # Get job manifest
        result = self.jobState.getManifest()
        if not result["OK"]:
            self.log.error("Failed to get job manifest", result["Message"])
            return result
        jobManifest = result["Value"]

        # Get ownerName, ownerGroup and jobSetup
        result = self.jobState.getAttributes(["Owner", "OwnerDN", "OwnerGroup", "DIRACSetup"])
        if not result["OK"]:
            self.log.error("Failed to get job attributes", result["Message"])
            return result
        attDict = result["Value"]
        ownerName = attDict["Owner"]
        if not ownerName:
            ownerDN = attDict["OwnerDN"]
            if not ownerDN:
                return S_ERROR("Missing OwnerDN")
            result = Registry.getUsernameForDN(ownerDN)
            if not result["OK"]:
                self.log.error("Failed to get user name from DN", result["Message"])
                return result
            ownerName = result["Value"]
        ownerGroup = attDict["OwnerGroup"]
        if not ownerGroup:
            return S_ERROR("Missing OwnerGroup")
        jobSetup = attDict["DIRACSetup"]
        if not jobSetup:
            return S_ERROR("Missing DIRACSetup")

        # Get input sandboxes
        isbList = jobManifest.getOption("InputSandbox", [])
        sbsToAssign = []
        for isb in isbList:

            # Get input sandboxes to assign
            if isb.startswith("SB:"):
                self.log.debug("Found a sandbox", isb)
                sbsToAssign.append((isb, "Input"))

            # Check that LFN sandboxes are correctly set
            if isb.startswith("LFN:"):
                self.log.debug("Found a LFN sandbox", isb)
                if len(isb) < 5 or isb[4] != "/":  # the LFN does not start with /
                    return S_ERROR("LFNs should always start with '/'")

        numSBsToAssign = len(sbsToAssign)
        if not numSBsToAssign:
            self.log.info("No sandbox to assign")
            return S_OK()
        self.log.info("Assigned ISBs", numSBsToAssign)

        # Assigning sandboxes
        self.log.info("Assigning sandboxes", f"({numSBsToAssign} on behalf of {ownerName}@{ownerGroup})")
        result = self.sandboxClient.assignSandboxesToJob(
            self.jobState.jid, sbsToAssign, ownerName, ownerGroup, jobSetup
        )
        if not result["OK"]:
            self.log.error("Could not assign sandboxes in the SandboxStore")
            return S_ERROR("Cannot assign sandbox to job")
        assigned = result["Value"]
        if assigned != numSBsToAssign:
            self.log.error("Could not assign all sandboxes", f"({numSBsToAssign}). Only assigned {assigned}")

        return S_OK()
