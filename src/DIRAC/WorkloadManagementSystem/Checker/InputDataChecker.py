"""
The Input Data Checker screens jobs for the following problems:
    - Virtual Organization not defined
    - User jobs with too much input data e.g. > 100 files
    - Jobs with input data incorrectly specified e.g. castor:/
"""

import re
from DIRAC import gLogger
from DIRAC.ConfigurationSystem.Client.Helpers.Operations import Operations
from DIRAC.Core.Utilities.ReturnValues import S_ERROR, S_OK
from DIRAC.WorkloadManagementSystem.CheckerAdminstrator.Checker import Checker
from DIRAC.WorkloadManagementSystem.Client import JobMinorStatus
from DIRAC.WorkloadManagementSystem.Client.JobState.JobState import JobState


class InputDataChecker(Checker):
    """Check input data"""

    def __init__(self, jobState: JobState) -> None:
        super().__init__(jobState)
        self.log = gLogger.getSubLogger(f"[jid={jobState.jid}]{self.__class__.__name__}")
        self.operations = Operations()

    def check(self):
        """
        This method is the method that will be launched by the CheckerAdministrator
        """

        # Check if input data should be checked
        if not self.operations.getValue("InputDataCheck", True):
            return S_OK()

        # Get job manifest
        result = self.jobState.getManifest()
        if not result["OK"]:
            self.log.error("Failed to get job manifest", result["Message"])
            return result
        jobManifest = result["Value"]

        # Get VO name
        voName = jobManifest.getOption("VirtualOrganization", "")
        if not voName:
            return S_ERROR("No VirtualOrganization defined in manifest")

        # Set VO regex
        voRE = re.compile(f"^(LFN:)?/{voName}/")

        # Get input data
        result = self.jobState.getInputData()
        if not result["OK"]:
            self.log.warn("Failed to get input data from JobDB", result["Message"])
            return S_ERROR("Input Data Specification")
        data = result["Value"]  # seems to be [''] when null, which isn't an empty list ;)
        data = [lfn.strip() for lfn in data if lfn.strip()]
        if not data:
            self.log.info("No LFNs found")
            return S_OK()

        numberOfLFNs = len(data)
        self.log.info("Found LFNs", numberOfLFNs)
        self.log.debug("Input data requirement will be checked")
        self.log.debug("Data is:\n\t%s" % "\n\t".join(data))

        # Check if the LFNs are correctly set
        for lfn in data:
            if not voRE.match(lfn):
                return S_ERROR(JobMinorStatus.INPUT_INCORRECT)
            if lfn.find("//") > -1:
                return S_ERROR(JobMinorStatus.INPUT_CONTAINS_SLASHES)

        # Get job type
        result = self.jobState.getAttribute("JobType")
        if not result["OK"]:
            self.log.error("Failed to get job type attribute", result["Message"])
            return result
        jobType = result["Value"].lower()

        # Check input data limit for user jobs
        if jobType == "user":
            maxLFNs = self.operations.getValue("MaxInputDataPerJob", 100)
            if numberOfLFNs > maxLFNs:
                return S_ERROR(f"Exceeded Maximum Dataset Limit ({maxLFNs})")

        return S_OK()
