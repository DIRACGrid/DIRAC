""" Collection of utility functions for Job descriptions
"""

import re

from DIRAC import gConfig, S_OK, S_ERROR
from DIRAC.ConfigurationSystem.Client import PathFinder
from DIRAC.ConfigurationSystem.Client.Helpers.Operations import Operations
from DIRAC.ConfigurationSystem.Client.Helpers.Registry import getVOForGroup
from DIRAC.Core.Utilities import List
from DIRAC.Core.Utilities.ClassAd import ClassAd
from DIRAC.Core.Utilities.DErrno import EWMSSUBM
from DIRAC.Core.Utilities.ObjectLoader import ObjectLoader
from DIRAC.WorkloadManagementSystem.Client import JobMinorStatus


def resolveJobDescription(jobDescription: ClassAd, owner: str, ownerDN: str, ownerGroup: str):
    """Method that launch all the job description resolvers"""

    opsHelper = Operations(group=ownerGroup)

    # Resolve owner
    jobDescription.insertAttributeString("Owner", owner)
    jobDescription.insertAttributeString("OwnerDN", ownerDN)
    jobDescription.insertAttributeString("OwnerGroup", ownerGroup)

    # Resolve VO
    vo = getVOForGroup(ownerGroup)
    if vo:
        jobDescription.insertAttributeString("VirtualOrganization", vo)

    # Resolve single elements into a list
    resolveSingularNamesToPlurals(jobDescription)

    # Resolve Input data module
    inputDataPolicy = Operations(vo=vo).getValue("InputDataPolicy/InputDataModule")
    if inputDataPolicy and not jobDescription.lookupAttribute("InputDataModule"):
        jobDescription.insertAttributeString("InputDataModule", inputDataPolicy)

    # Resolve priority
    resolvePriority(jobDescription, opsHelper)

    # Resolve CPU time
    resolveCpuTime(jobDescription, opsHelper)

    # Resolve sites
    resolveSites(jobDescription)

    # Resolve tags
    resolveTags(jobDescription)

    # Resolve job path
    resolveJobPath(jobDescription)

    return S_OK()


def resolveSingularNamesToPlurals(jobDescription: ClassAd) -> None:
    """Resolve the old singular naming convention to plurals"""
    translationalDictionary = {"BannedSite": "BannedSites", "GridCE": "GridCEs", "Site": "Sites", "Tag": "Tags"}
    for oldName, newName in translationalDictionary.items():
        if jobDescription.lookupAttribute(oldName):
            jobDescription.insertAttributeVectorString(
                newName, List.fromChar(jobDescription.getAttributeString(oldName))
            )
            # TODO: replace old name by new names everywhere in the code
            # so we can delete the old attributes from the job description
            # jobDescription.deleteAttribute(oldName)


def resolveTags(jobDescription: ClassAd) -> None:
    """Resolve a list of tagsTQ from the job manifest content for the TQ"""

    tags = set()

    # CPU cores
    if jobDescription.lookupAttribute("NumberOfProcessors"):
        minProcessors = maxProcessors = jobDescription.getAttributeInt("NumberOfProcessors")
    else:
        if jobDescription.lookupAttribute("MinNumberOfProcessors"):
            minProcessors = jobDescription.getAttributeInt("MinNumberOfProcessors")
        else:
            minProcessors = 1
        if jobDescription.lookupAttribute("MaxNumberOfProcessors"):
            maxProcessors = jobDescription.getAttributeInt("MaxNumberOfProcessors")
        else:
            maxProcessors = minProcessors

    if minProcessors > 1:
        tags.add(f"{minProcessors}Processors")
    if maxProcessors > 1:
        tags.add("MultiProcessor")

    # Whole node
    if jobDescription.lookupAttribute("WholeNode"):
        if jobDescription.getAttributeString("WholeNode").lower() in ["1", "yes", "true", "y"]:
            tags.add("WholeNode")
            tags.add("MultiProcessor")

    # RAM
    if jobDescription.lookupAttribute("MaxRAM"):
        maxRAM = jobDescription.getAttributeInt("MaxRAM")
        if maxRAM:
            tags.add(f"{maxRAM}GB")

    # Other tags? Just add them
    if jobDescription.lookupAttribute("Tags"):
        tags |= set(jobDescription.getListFromExpression("Tags"))

    # Store in the job description the tags if any
    if tags:
        jobDescription.insertAttributeVectorString("Tags", tags)


def resolvePriority(jobDescription: ClassAd, operations: Operations) -> None:
    """Resolve the job priority and stores it in the job description"""
    if jobDescription.lookupAttribute("Priority"):
        minPriority = operations.getValue("JobDescription/MinPriority", 0)
        maxPriority = operations.getValue("JobDescription/MaxPriority", 10)
        priority = max(minPriority, min(jobDescription.getAttributeInt("Priority"), maxPriority))
    else:
        priority = operations.getValue("JobDescription/DefaultPriority", 1)

    jobDescription.insertAttributeInt("Priority", int(priority))


def resolveCpuTime(jobDescription: ClassAd, operations: Operations) -> None:
    """Resolve the CPU time and stores it in the job description"""
    if jobDescription.lookupAttribute("CPUTime"):
        minCpuTime = operations.getValue("JobDescription/MinCPUTime", 100)
        maxCpuTime = operations.getValue("JobDescription/MaxCPUTime", 500000)
        resolvedCpuTime = max(minCpuTime, min(jobDescription.getAttributeInt("CPUTime"), maxCpuTime))
    else:
        resolvedCpuTime = operations.getValue("JobDescription/DefaultCPUTime", 86400)

    jobDescription.insertAttributeInt("CPUTime", int(resolvedCpuTime))


def resolveSites(jobDescription: ClassAd):
    """Remove the Site attribute if set to ANY in the original manifest"""
    if jobDescription.lookupAttribute("Sites"):
        sites = jobDescription.getListFromExpression("Sites")
        if not sites or "ANY" in sites or "Any" in sites or "any" in sites:
            jobDescription.deleteAttribute("Sites")


def resolveJobPath(jobDescription: ClassAd):
    """Resolve the job path (i.e which optimizers need to be run for a job"""
    if not jobDescription.lookupAttribute("JobPath"):
        jobPathExecutorSection = PathFinder.getExecutorSection("WorkloadManagement/JobPath")

        result = gConfig.getOption(f"{jobPathExecutorSection}/BasePath")
        if result["OK"]:
            opPath = result["Value"]
        else:
            opPath = ["JobSanity"]

        if jobDescription.lookupAttribute("InputData"):

            jobType = jobDescription.getAttributeString("JobType")
            # if it's a production job
            if jobType in Operations().getValue("Transformations/DataProcessing", ["MCSimulation", "Merge"]):
                opPath.append(["InputData"])
            else:
                # Resolve files to stage or the online site(s) if there is no file to stage for user job
                opPath.append(["JobScheduling"])
            opPath.append("CheckingToStagingTransitioner")

        opPath.append("CheckingToWaitingTransitioner")

        jobDescription.insertAttributeVectorString("JobPath", opPath)


def checkJobDescription(jobDescription: ClassAd):
    """Check that the job description is correctly set"""

    # Check that the job description contains some mandatory fields
    mandatoryFields = {"JobType", "Owner", "OwnerDN", "OwnerGroup", "Priority", "JobPath"}
    for field in mandatoryFields:
        if not jobDescription.lookupAttribute(field):
            return S_ERROR(EWMSSUBM, f"The JDL needs to contain the {field} field")

    # Check that job type is correct
    opsHelper = Operations(group=jobDescription.getAttributeString("OwnerGroup"))
    allowedJobTypes = opsHelper.getValue("JobDescription/AllowedJobTypes", ["User", "Test", "Hospital"])
    transformationTypes = opsHelper.getValue("Transformations/DataProcessing", [])
    jobTypes = opsHelper.getValue("JobDescription/ChoicesJobType", allowedJobTypes + transformationTypes)
    jobType = jobDescription.getAttributeString("JobType")
    if jobType not in jobTypes:
        return S_ERROR(EWMSSUBM, f"{jobType} is not a valid value for JobType")

    # Check input data
    inputData = jobDescription.getListFromExpression("InputData")
    if inputData:

        # Check that the VO is set up
        vo = jobDescription.getAttributeString("VirtualOrganization")
        if not vo:
            return S_ERROR(EWMSSUBM, "Input data listed but no VO is set")

        # Check that LFNs are well formated
        voRE = re.compile(f"^(LFN:)?/{vo}/")
        for lfn in inputData:
            if not voRE.match(lfn):
                return S_ERROR(EWMSSUBM, JobMinorStatus.INPUT_INCORRECT)
            if lfn.find("//") > -1:
                return S_ERROR(EWMSSUBM, JobMinorStatus.INPUT_CONTAINS_SLASHES)

        # Check max input data
        maxInputData = Operations().getValue("JobDescription/MaxInputData", 500)
        if len(inputData) > maxInputData:
            return S_ERROR(
                EWMSSUBM,
                f"Number of Input Data Files ({len(inputData)}) greater than current limit: {maxInputData}",
            )

    # Check LFN input sandboxes
    inputSandboxes = jobDescription.getListFromExpression("InputSandbox")
    for inputSandbox in inputSandboxes:
        if inputSandbox.startswith("LFN:") and not inputSandbox.startswith("LFN:/"):
            return S_ERROR(EWMSSUBM, "LFNs should always start with '/'")

    # Check JobPath
    jobPath = jobDescription.getListFromExpression("JobPath")
    for optimizer in jobPath:
        result = ObjectLoader().loadObject(f"WorkloadManagementSystem.Executor.{optimizer}")
        if not result["OK"]:
            return result

    return S_OK()


def getDIRACPlatform(platformList):
    """Loading the function that will be used to determine the platform (it can be VO specific)"""
    result = ObjectLoader().loadObject("ConfigurationSystem.Client.Helpers.Resources", "getDIRACPlatform")
    if not result["OK"]:
        return result
    getDIRACPlatformMethod = result["Value"]
    return getDIRACPlatformMethod(platformList)
