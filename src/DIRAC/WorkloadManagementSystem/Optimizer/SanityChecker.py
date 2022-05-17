"""
The Job Sanity optimizer screens jobs for the following problems:
    - Output data already exists
    - Problematic JDL
    - Jobs with too much input data e.g. > 100 files
    - Jobs with input data incorrectly specified e.g. castor:/
    - Input sandbox not correctly uploaded.
"""
import re

from DIRAC import S_OK, S_ERROR, gLogger
from DIRAC.ConfigurationSystem.Client.Helpers import Registry
from DIRAC.ConfigurationSystem.Client.Helpers.Operations import Operations
from DIRAC.WorkloadManagementSystem.Client.JobState.JobManifest import JobManifest
from DIRAC.WorkloadManagementSystem.Client.JobState.JobState import JobState
from DIRAC.WorkloadManagementSystem.Client.SandboxStoreClient import SandboxStoreClient
from DIRAC.WorkloadManagementSystem.Client import JobMinorStatus
from DIRAC.WorkloadManagementSystem.OptimizerAdministrator.Optimizer import Optimizer


class SanityChecker(Optimizer):
    """
    The specific Optimizer must provide the following methods:
      - optimize() - the main method called for each job
    """

    def __init__(self, jobState: JobState):
        """Constructor"""
        super().__init__(jobState)
        self.log = gLogger.getSubLogger(f"[jid={jobState.jid}]{__name__}")
        self.__operations = Operations()
        self.sandboxClient = SandboxStoreClient(useCertificates=True, smdb=True)

    def optimize(self):
        """
        This method controls the order and presence of
        each sanity check for submitted jobs. This should
        be easily extended in the future to accommodate
        any other potential checks.
        """
        # Job JDL check
        result = self.jobState.getAttribute("JobType")
        if not result["OK"]:
            self.log.error("Failed to get job type attribute", result["Message"])
            return result
        jobType = result["Value"].lower()

        result = self.jobState.getManifest()
        if not result["OK"]:
            self.log.error("Failed to get job manifest", result["Message"])
            return result
        manifest = result["Value"]

        # Input data check
        if self.__operations.getValue("InputDataCheck", True):
            voName = manifest.getOption("VirtualOrganization", "")
            if not voName:
                return S_ERROR("No VirtualOrganization defined in manifest")
            result = self.checkInputData(jobType, voName)
            if not result["OK"]:
                self.log.error("Failed to check input data", result["Message"])
                return result
            self.log.info("Found LFNs", result["Value"])

        # Input Sandbox check
        if self.__operations.getValue("InputSandboxCheck", True):
            result = self.checkInputSandbox(manifest)
            if not result["OK"]:
                self.log.error("Failed to check input sandbox", result["Message"])
                return result
            self.log.info("Assigned ISBs", result["Value"])

        return S_OK()

    def checkInputData(self, jobType: str, voName: str):
        """
        This method checks both the amount of input
        datasets for the job and whether the LFN conventions
        are correct.
        """

        result = self.jobState.getInputData()
        if not result["OK"]:
            self.log.warn("Failed to get input data from JobDB", result["Message"])
            return S_ERROR("Input Data Specification")

        data = result["Value"]  # seems to be [''] when null, which isn't an empty list ;)
        data = [lfn.strip() for lfn in data if lfn.strip()]
        if not data:
            return S_OK(0)

        self.log.debug("Input data requirement will be checked")
        self.log.debug("Data is:\n\t%s" % "\n\t".join(data))

        voRE = re.compile("^(LFN:)?/%s/" % voName)

        for lfn in data:
            if not voRE.match(lfn):
                return S_ERROR(JobMinorStatus.INPUT_INCORRECT)
            if lfn.find("//") > -1:
                return S_ERROR(JobMinorStatus.INPUT_CONTAINS_SLASHES)

        # only check limit for user jobs
        if jobType == "user":
            maxLFNs = self.__operations.getValue("MaxInputDataPerJob", 100)
            if len(data) > maxLFNs:
                return S_ERROR("Exceeded Maximum Dataset Limit (%s)" % maxLFNs)
        return S_OK(len(data))

    def checkInputSandbox(self, manifest: JobManifest):
        """
        The number of input sandbox files, as specified in the job
        JDL are checked in the JobDB.
        """
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

        isbList = manifest.getOption("InputSandbox", [])
        sbsToAssign = []
        for isb in isbList:
            if isb.startswith("SB:"):
                self.log.debug("Found a sandbox", isb)
                sbsToAssign.append((isb, "Input"))
            if isb.startswith("LFN:"):
                self.log.debug("Found a LFN sandbox", isb)
                if len(isb) < 5 or isb[4] != "/":  # the LFN does not start with /
                    return S_ERROR("LFNs should always start with '/'")
        numSBsToAssign = len(sbsToAssign)
        if not numSBsToAssign:
            return S_OK(0)
        self.log.info("Assigning sandboxes", "(%s on behalf of %s@%s)" % (numSBsToAssign, ownerName, ownerGroup))
        result = self.sandboxClient.assignSandboxesToJob(
            self.jobState.jid, sbsToAssign, ownerName, ownerGroup, jobSetup
        )
        if not result["OK"]:
            self.log.error("Could not assign sandboxes in the SandboxStore")
            return S_ERROR("Cannot assign sandbox to job")
        assigned = result["Value"]
        if assigned != numSBsToAssign:
            self.log.error("Could not assign all sandboxes", "(%s). Only assigned %s" % (numSBsToAssign, assigned))
        return S_OK(numSBsToAssign)
