"""Transition from CHECKING to WAITING if STAGING is not required"""

from DIRAC import S_OK, S_ERROR
from DIRAC.Core.Utilities.ClassAd import ClassAd
from DIRAC.Core.Utilities.ObjectLoader import ObjectLoader
from DIRAC.WorkloadManagementSystem.Client import JobMinorStatus, JobStatus
from DIRAC.WorkloadManagementSystem.Client.JobState.JobState import JobState
from DIRAC.WorkloadManagementSystem.Executor.Base.OptimizerExecutor import OptimizerExecutor


class CheckingToWaitingTransitioner(OptimizerExecutor):
    """Transition from CHECKING to WAITING"""

    @classmethod
    def initializeOptimizer(cls):
        """Initialization of the optimizer."""
        return S_OK()

    def optimizeJob(self, jid, jobState: JobState):
        """Insert the jobState into the task queue and set the status to WAITING"""

        result = jobState.getManifest()
        if not result["OK"]:
            return result
        jobDescription = result["Value"]

        userSites = set(jobDescription.getListFromExpression("Sites"))
        userBannedSites = set(jobDescription.getListFromExpression("BannedSites"))
        onlineSites = set(jobDescription.getListFromExpression("OnlineSites"))

        # Resolve sites
        result = resolveSites(userSites, userBannedSites, onlineSites)
        if not result["OK"]:
            return result
        sites = result["Value"]

        # Set the site field in the job db
        siteName = resolveJobSiteName(sites, onlineSites)
        result = jobState.setAttribute("Site", siteName)
        if not result["OK"]:
            return result

        # Set the job requirements in the job description
        result = setJobRequirements(jobDescription, sites, userBannedSites)
        if not result["OK"]:
            return result

        # Insert the job in the task queue db
        result = jobState.insertIntoTQ(jobDescription)
        if not result["OK"]:
            return result

        # Change the status to WAITING
        return jobState.setStatus(
            JobStatus.WAITING,
            minorStatus=JobMinorStatus.PILOT_AGENT_SUBMISSION,
            appStatus="Unknown",
            source=self.__class__.__name__,
        )


def resolveSites(userSites: set[str], userBannedSites: set[str], onlineSites: set[str]):
    """Resolve the site for the jobRequirements"""

    # Removing user banned sites from user sites
    if userSites and userBannedSites:
        userSites -= userBannedSites
        if not userSites:
            return S_ERROR("All user sites are in the user banned site list")

    # Removing online sites that are not part of the user site list
    if onlineSites and userSites:
        onlineSites &= userSites
        if not onlineSites:
            return S_ERROR("The online sites are not part of the selected user sites")

    # Removing online sites that are part of the user banned sites list
    if onlineSites and userBannedSites:
        onlineSites -= userBannedSites
        if not onlineSites:
            return S_ERROR("All the online sites have been banned")

    # Determining the list of sites
    if onlineSites:
        sites = list(onlineSites)
    elif userSites:
        sites = list(userSites)
    else:
        sites = []

    return S_OK(sites)


def resolveJobSiteName(sites: list[str], onlineSites: list[str]) -> str:
    """Resolve the site field of jobDB"""

    if len(sites) == 0:
        siteName = "ANY"
    elif len(sites) == 1:
        siteName = sites[0]
    else:
        if onlineSites:
            siteName = "MultipleInput"
        else:
            siteName = "Multiple"
    return siteName


def setJobRequirements(jobDescription: ClassAd, sites: list[str], userBannedSites: list[str]):
    """
    This method resolves the JobRequirements subsection of the JDL
    """

    if jobDescription.lookupAttribute("JobRequirements"):
        jobRequirements = jobDescription.getAttributeSubsection("JobRequirements")
    else:
        jobRequirements = ClassAd("[]")

    jobRequirements.insertAttributeString("Setup", jobDescription.getAttributeString("DIRACSetup"))
    jobRequirements.insertAttributeString("OwnerDN", jobDescription.getAttributeString("OwnerDN"))
    jobRequirements.insertAttributeString("OwnerGroup", jobDescription.getAttributeString("OwnerGroup"))

    if jobDescription.lookupAttribute("VirtualOrganization"):
        jobRequirements.insertAttributeString(
            "VirtualOrganization", jobDescription.getAttributeString("VirtualOrganization")
        )

    jobRequirements.insertAttributeInt("UserPriority", jobDescription.getAttributeInt("Priority"))
    jobRequirements.insertAttributeInt("CPUTime", jobDescription.getAttributeInt("CPUTime"))
    jobRequirements.insertAttributeVectorString("Tags", jobDescription.getListFromExpression("Tags"))

    platformList = jobDescription.getListFromExpression("Platform")
    if platformList:
        result = getDIRACPlatform(platformList)
        if not result["OK"]:
            return result
        if not result["Value"]:
            return S_ERROR("OS compatibility info not found")
        jobRequirements.insertAttributeVectorString("Platforms", result["Value"])

    if sites:
        jobRequirements.insertAttributeVectorString("Sites", sites)
    if userBannedSites:
        jobRequirements.insertAttributeVectorString("BannedSites", userBannedSites)

    jobDescription.insertAttributeSubsection("JobRequirements", jobRequirements)

    return S_OK()


def getDIRACPlatform(platformList):
    """Loading the function that will be used to determine the platform (it can be VO specific)"""
    result = ObjectLoader().loadObject("ConfigurationSystem.Client.Helpers.Resources", "getDIRACPlatform")
    if not result["OK"]:
        return result
    getDIRACPlatformMethod = result["Value"]
    return getDIRACPlatformMethod(platformList)
