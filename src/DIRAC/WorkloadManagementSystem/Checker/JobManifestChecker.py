"""
The Input Data Checker configures the job in 2 ways:
    - Adds useful tags regarding machine configuration for assigning later
    - Adds plurals keys in the manifest for backward compatibility
"""

from DIRAC import S_OK, gLogger
from DIRAC.WorkloadManagementSystem.Client.JobState.JobManifest import JobManifest
from DIRAC.WorkloadManagementSystem.Client.JobState.JobState import JobState
from DIRAC.WorkloadManagementSystem.CheckerAdminstrator.Checker import Checker


class JobManifestChecker(Checker):
    """Check job manifest"""

    def __init__(self, jobState: JobState) -> None:
        super().__init__(jobState)
        self.log = gLogger.getSubLogger(f"[jid={jobState.jid}]{self.__class__.__name__}")

    def check(self):
        """
        This method is the method that will be launched by the CheckerAdministrator
        """

        # Get job manifest
        result = self.jobState.getManifest()
        if not result["OK"]:
            self.log.error("Could not retrieve job manifest", result["Message"])
            return result
        jobManifest = result["Value"]

        tagList = getTagsFromManifest(jobManifest)
        if tagList:
            jobManifest.setOption("Tags", ", ".join(tagList))

        jobRequirements = "JobRequirements"
        if jobRequirements in jobManifest:
            result = jobManifest.getSection(jobRequirements)
        else:
            result = jobManifest.createSection(jobRequirements)
        if not result["OK"]:
            self.log.error("Cannot create jobManifest section", f"({jobRequirements}: {result['Message']})")
            return result
        reqCfg = result["Value"]

        # Job multivalue requirement keys are specified as singles in the job descriptions
        # but for backward compatibility can be also plurals
        translationDict = {"JobType": "JobTypes", "GridCE": "GridCEs"}
        for oldName, newName in translationDict.items():
            if oldName in jobManifest:
                reqCfg.setOption(newName, ", ".join(jobManifest.getOption(oldName, [])))

        return S_OK()


def getTagsFromManifest(jobManifest: JobManifest) -> set[str]:
    """Helper method to add a list of tags to the TQ from the job manifest content"""

    tagList = set()

    # CPU cores
    if "NumberOfProcessors" in jobManifest:
        minProcessors = maxProcessors = int(jobManifest.getOption("NumberOfProcessors"))
    else:
        minProcessors = jobManifest.getOption("MinNumberOfProcessors", 1)
        maxProcessors = jobManifest.getOption("MaxNumberOfProcessors", minProcessors)
    if minProcessors > 1:
        tagList.add(f"{minProcessors}Processors")
    if maxProcessors > 1:
        tagList.add("MultiProcessor")

    # Whole node
    if jobManifest.getOption("WholeNode", "").lower() in ["1", "yes", "true", "y"]:
        tagList.add("WholeNode")
        tagList.add("MultiProcessor")

    # RAM
    maxRAM = jobManifest.getOption("MaxRAM", 0)
    if maxRAM:
        tagList.add(f"{maxRAM}GB")

    # Other tags? Just add them
    if "Tags" in jobManifest:
        tagList |= set(jobManifest.getOption("Tags", []))
    if "Tag" in jobManifest:
        tagList |= set(jobManifest.getOption("Tag", []))

    return tagList
