"""
The Job Scheduling Executor makes job scheduling decisions based on the input data.

If the job has input data on tape, a staging request will be sent to the StorageManager.
Otherwise, the job will be sent to the Task Queue to be picked up by pilots.

All issues preventing the successful resolution of a site candidate are discovered
here where all information is available.

This Executor will fail affected jobs meaningfully.
"""

import random

from celery import Task

from DIRAC import gLogger, S_OK, S_ERROR
from DIRAC.ConfigurationSystem.Client.Helpers import Registry
from DIRAC.Core.Celery.CeleryApp import celery
from DIRAC.Core.Security import Properties
from DIRAC.Core.Utilities.Proxy import executeWithUserProxy
from DIRAC.ConfigurationSystem.Client.Helpers.Operations import Operations
from DIRAC.Core.Utilities.SiteSEMapping import getSEsForSite
from DIRAC.FrameworkSystem.private.standardLogging.LoggingRoot import LoggingRoot
from DIRAC.ResourceStatusSystem.Client.SiteStatus import SiteStatus
from DIRAC.Resources.Storage.StorageElement import StorageElement
from DIRAC.Resources.Catalog.FileCatalog import FileCatalog
from DIRAC.DataManagementSystem.Utilities.DMSHelpers import DMSHelpers
from DIRAC.DataManagementSystem.Client.DataManager import DataManager
from DIRAC.StorageManagementSystem.Client.StorageManagerClient import StorageManagerClient, getFilesToStage
from DIRAC.WorkloadManagementSystem.Client import JobMinorStatus, JobStatus
from DIRAC.WorkloadManagementSystem.Client.JobStateUpdateClient import JobStateUpdateClient
from DIRAC.WorkloadManagementSystem.DB.TaskQueueDB import TaskQueueDB
from DIRAC.WorkloadManagementSystem.Utilities.JobModel import JobDescriptionModel


@celery.task(bind=True, name="resolveInputData", retry_backoff=60, retry_backoff_max=600)
def resolveInputData(self: Task, jsonJobDescription: str):
    """Send the job to the TQ, launch the staging request, wait until a site is available or fail the job"""
    jobDescription = JobDescriptionModel.parse_raw(jsonJobDescription)
    jobId = self.request.id
    log = gLogger.getSubLogger(f"resolveInputData/{jobId}")

    # Get banned sites from DIRAC
    res = SiteStatus().getSites("Banned")
    if not res["OK"]:
        log.warn("Cannot retrieve banned sites", res["Message"])
        self.retry()
    wmsBannedSites = res["Value"]

    # If the user has selected any site, filter them and hold the job if not able to run
    if jobDescription.sites:
        res = SiteStatus().getUsableSites(list(jobDescription.sites))
    else:
        res = SiteStatus().getUsableSites()
    if not res["OK"]:
        log.error("Problem checking userSites for tuple of active/banned/invalid sites", res["Message"])
        JobStateUpdateClient().setJobStatus(jobId, JobStatus.FAILED, res["Message"], "ResolveInputData")
        return

    usableSites = set(res["Value"])
    bannedSites = []
    invalidSites = []

    if jobDescription.sites:
        for site in jobDescription.sites:
            if site in wmsBannedSites:
                bannedSites.append(site)
            elif site not in usableSites:
                invalidSites.append(site)

        if invalidSites:
            log.debug(f"Invalid site(s) requested: {','.join(invalidSites)}")
            if not Operations().getValue("AllowInvalidSites", True):
                log.warn("Invalid site(s) requested, job held")
                self.retry()

        if bannedSites:
            log.debug(f"Banned site(s) {','.join(bannedSites)} ignored")

    if not usableSites:
        log.warn("No usable sites for the job, job held")
        self.retry()

    # Resolve the online sites
    offlineLFNs = {}
    onlineSites = set()
    if jobDescription.inputData:
        if jobDescription.jobType in Operations().getValue("Transformations/DataProcessing", []):
            log.info("Production job: sending to TQ, but first checking if staging is requested")

            res = getFilesToStage(
                jobDescription.inputData,
                jobDescription.owner,
                jobDescription.ownerGroup,
                Operations().getValue("CheckOnlyTapeSEs", True),
                jobLog=log,
            )
            if not res["OK"]:
                log.warn("Could not get files to stage", res["Message"])
                self.retry()

            if res["Value"]["absentLFNs"]:
                # Some files do not exist at all... set the job Failed
                # Reverse errors
                reasons = {}
                for lfn, reason in res["Value"]["absentLFNs"].items():
                    reasons.setdefault(reason, []).append(lfn)
                for reason, lfns in reasons.items():
                    # Some files are missing in the FC or in SEs, fail the job
                    log.error(reason, ",".join(lfns))
                error = ",".join(reasons)
                log.warn(error)
                JobStateUpdateClient().setJobStatus(jobId, JobStatus.FAILED, error, "ResolveInputData")
                return

            if res["Value"]["failedLFNs"]:
                log.info("Couldn't get storage metadata of some files, retrying")
                self.retry()

            offlineLFNs = res["Value"]["offlineLFNs"]
            if not offlineLFNs:
                # No staging required
                onlineSites = set(res["Value"]["onlineSites"])
                if onlineSites:
                    # Set the online site(s) first
                    onlineSites &= set(usableSites)
                    usableSites = list(onlineSites) + list(set(usableSites) - onlineSites)
                else:
                    usableSites = list(usableSites)

        else:
            # If it's a user job with input data
            if Operations().getValue("CheckFileMetadata", True):
                res = checkFileMetadata(jobDescription, log)
                if not res["OK"]:
                    log.error(res["Message"])
                    JobStateUpdateClient().setJobStatus(jobId, JobStatus.FAILED, res["Message"], "ResolveInputData")
                    return

            if Operations().getValue("CheckWithUserProxy", False):
                res = checkInputDataReplicas(  # pylint: disable=unexpected-keyword-arg
                    jobDescription,
                    proxyUserName=jobDescription.owner,
                    proxyUserGroup=jobDescription.ownerGroup,
                    executionLock=True,
                )
            else:
                res = checkInputDataReplicas(jobDescription)

            if not res["OK"]:
                log.error(res["Message"])
                JobStateUpdateClient().setJobStatus(
                    jobId, JobStatus.FAILED, "No replica Info available", "ResolveInputData"
                )
                return
            opData = res["Value"]
            siteCandidates = set(opData["SiteCandidates"])
            log.info("Site candidates are", siteCandidates)

            if jobDescription.sites:
                siteCandidates &= jobDescription.sites

            if jobDescription.bannedSites:
                siteCandidates -= jobDescription.bannedSites

            if not siteCandidates:
                JobStateUpdateClient().setJobStatus(
                    jobId, JobStatus.FAILED, "Impossible InputData * Site requirements", "ResolveInputData"
                )
                return

            idSites = {}
            for site in siteCandidates:
                idSites[site] = siteCandidates[site]

            # Check if sites have correct count of disk+tape replicas
            numData = len(jobDescription.inputData)
            errorSites = set()
            for site in idSites:
                if numData != idSites[site]["disk"] + idSites[site]["tape"]:
                    log.error("Site candidate does not have all the input data", f"({site})")
                    errorSites.add(site)
            for site in errorSites:
                idSites.pop(site)
            if not idSites:
                error = "Site candidates do not have all the input data"
                log.error(error)
                JobStateUpdateClient().setJobStatus(jobId, JobStatus.FAILED, error, "ResolveInputData")
                return

            # Check if staging is required
            stageRequired, siteCandidates = resolveStaging(jobDescription.inputData, idSites)
            if not siteCandidates:
                error = "No destination sites available"
                log.error(error)
                JobStateUpdateClient().setJobStatus(jobId, JobStatus.FAILED, error, "ResolveInputData")
                return

            # Is any site active?
            usableSites = siteCandidates - set(wmsBannedSites)
            if not usableSites:
                log.info("No active site candidates available")
                self.retry()

            res = setJobSite(jobId, list(usableSites))
            if not res["OK"]:
                log.warn(res["Message"])
                self.retry()

            if stageRequired:
                # Check if the user is allowed to stage
                if Operations().getValue(
                    "RestrictDataStage", False
                ) and Properties.STAGE_ALLOWED not in Registry.getPropertiesForGroup(jobDescription.ownerGroup):
                    error = "Stage not allowed"
                    log.error(error)
                    JobStateUpdateClient().setJobStatus(jobId, JobStatus.FAILED, error, "ResolveInputData")
                    return

                # We select randomly one of the best stage sites
                stageSite = random.choice(usableSites)
                log.verbose("Staging site will be", stageSite)

                stageData = idSites[stageSite]
                # Set as if everything has already been staged
                stageData["disk"] += stageData["tape"]
                stageData["tape"] = 0
                # Set the site info back to the original dict to save afterwards
                opData["SiteCandidates"][stageSite] = stageData

                res = preRequestStaging(jobDescription, stageSite, opData, log)
                if not res["OK"]:
                    log.error(res["Message"])
                    JobStateUpdateClient().setJobStatus(jobId, JobStatus.FAILED, res["Message"], "ResolveInputData")
                    return
                offlineLFNs = res["Value"]

    if offlineLFNs:
        # If staging is required
        # We send a request to the StorageManager and set the job status to STAGING
        log.debug("Stage request will be \n\t%s" % "\n\t".join([f"{lfn}:{offlineLFNs[lfn]}" for lfn in offlineLFNs]))
        res = StorageManagerClient().setRequest(
            offlineLFNs, "WorkloadManagement", "updateJobFromStager@WorkloadManagement/JobStateUpdate", jobId
        )
        if not res["OK"]:
            log.warn("Could not send stage request", res["Message"])
            self.retry()

        res = JobStateUpdateClient().setJobStatus(jobId, JobStatus.STAGING, "Request Sent", "ResolveInputData")
        if not res["OK"]:
            log.warn(res["Message"])
            self.retry()

        if jobDescription.jobType not in Operations().getValue("Transformations/DataProcessing", []):
            updateSharedSESites(jobDescription, stageSite, offlineLFNs, opData, log)

    else:
        # If we're here, no staging is required
        # We can therefore send the job to the TQ
        jobReqDict = {}
        # Signe value definition fields
        jobReqDict["OwnerDN"] = jobDescription.ownerDN
        jobReqDict["OwnerGroup"] = jobDescription.ownerGroup
        jobReqDict["CPUTime"] = jobDescription.cpuTime

        # Multi value definition fields
        if jobDescription.sites:
            jobReqDict["Sites"] = jobDescription.sites
        if jobDescription.gridCE:
            jobReqDict["GridCEs"] = [jobDescription.gridCE]
        if jobDescription.bannedSites:
            jobReqDict["BannedSites"] = jobDescription.bannedSites
        if jobDescription.platform:
            jobReqDict["Platforms"] = [jobDescription.platform]
        if jobDescription.tags:
            jobReqDict["Tags"] = jobDescription.tags

        taskQueueDB = TaskQueueDB()
        res = taskQueueDB.insertJob(jobId, jobReqDict, jobDescription.priority)
        if not res["OK"]:
            log.warn("Could not insert job into the TQ", res["Message"])
            # Force removing the job from the TQ if it was actually inserted
            res = taskQueueDB.deleteJob(jobId)
            if res["OK"]:
                if res["Value"]:
                    log.info(f"Job {jobId} removed from the TQ")

            # If the insertion failed, we retry
            self.retry()

        res = JobStateUpdateClient().setJobStatus(
            jobId, JobStatus.WAITING, JobMinorStatus.PILOT_AGENT_SUBMISSION, "ResolveInputData"
        )
        if not res["OK"]:
            log.warn(res["Message"])
            self.retry()

    # Finally, we set the job site in the job db
    res = setJobSite(jobId, list(usableSites), onlineSites)
    if not res["OK"]:
        log.warn(res["Message"])
        self.retry()


@executeWithUserProxy
def checkInputDataReplicas(jobDescription: JobDescriptionModel):
    """This method checks the file catalog for replica information.

    :param JobState jobState: JobState object
    :param list inputData: list of LFNs

    :returns: S_OK/S_ERROR structure with resolved input data info
    """

    res = DataManager(vo=jobDescription.vo).getReplicasForJobs(jobDescription.inputData)
    if not res["OK"]:
        return res

    if "Successful" not in res["Value"]:
        return S_ERROR("No replica Info available")

    replicaDict = res["Value"]
    okReplicas = replicaDict["Successful"]

    badLFNs = []
    for lfn in okReplicas:
        if not okReplicas[lfn]:
            badLFNs.append(f"LFN:{lfn} -> No replicas available")

    if "Failed" in replicaDict:
        errorReplicas = replicaDict["Failed"]
        for lfn in errorReplicas:
            badLFNs.append(f"LFN:{lfn} -> {errorReplicas[lfn]}")

    if badLFNs:
        errorMsg = "\n".join(badLFNs)
        return S_ERROR(f"Input data not available: {errorMsg}")

    res = getSiteCandidates(okReplicas, jobDescription.vo)
    if not res["OK"]:
        return res

    siteCandidates = res["Value"]

    if Operations().getValue("CheckFileMetadata", True):
        guidDict = FileCatalog(vo=jobDescription.vo).getFileMetadata(jobDescription.inputData)
        if not guidDict["OK"]:
            return guidDict

        failed = guidDict["Value"]["Failed"]
        if failed:
            pass
            # log.warn("Failed to establish some GUIDs")
            # log.warn(failed)

        for lfn in replicaDict["Successful"]:
            replicas = replicaDict["Successful"][lfn]
            guidDict["Value"]["Successful"][lfn].update(replicas)

    resolvedData = {}
    resolvedData["Value"] = guidDict
    resolvedData["SiteCandidates"] = siteCandidates

    return S_OK(resolvedData)


def getSiteCandidates(okReplicas, vo):
    """This method returns a list of possible site candidates based on the job input data requirement.

    For each site candidate, the number of files on disk and tape is resolved.
    """

    lfnSEs = {}
    for lfn in okReplicas:
        replicas = okReplicas[lfn]
        siteSet = set()
        for seName in replicas:
            res = DMSHelpers().getSitesForSE(seName)
            if not res["OK"]:
                return res
            siteSet.update(res["Value"])
        lfnSEs[lfn] = siteSet

    if not lfnSEs:
        return S_ERROR(JobMinorStatus.NO_CANDIDATE_SITE_FOUND)

    # This makes an intersection of all sets in the dictionary and returns a set with it
    siteCandidates = set.intersection(*[lfnSEs[lfn] for lfn in lfnSEs])

    if not siteCandidates:
        return S_ERROR(JobMinorStatus.NO_CANDIDATE_SITE_FOUND)

    # In addition, check number of files on tape and disk for each site
    # for optimizations during scheduling
    sitesData = {}
    for siteName in siteCandidates:
        sitesData[siteName] = {"disk": set(), "tape": set()}

    # Loop time!
    seDict = {}
    for lfn in okReplicas:
        replicas = okReplicas[lfn]
        # Check each SE in the replicas
        for seName in replicas:
            # If not already "loaded" the add it to the dict
            if seName not in seDict:
                res = DMSHelpers().getSitesForSE(seName)
                if not res["OK"]:
                    continue
                siteList = res["Value"]
                seObj = StorageElement(seName, vo=vo)
                res = seObj.getStatus()
                if not res["OK"]:
                    return res

                seDict[seName] = {"Sites": siteList, "Status": res["Value"]}
            # Get SE info from the dict
            seData = seDict[seName]
            siteList = seData["Sites"]
            seStatus = seData["Status"]
            for siteName in siteList:
                # If not a candidate site then skip it
                if siteName not in siteCandidates:
                    continue
                # Add the LFNs to the disk/tape lists
                diskLFNs = sitesData[siteName]["disk"]
                tapeLFNs = sitesData[siteName]["tape"]
                if seStatus["DiskSE"]:
                    # Sets contain only unique elements, no need to check if it's there
                    diskLFNs.add(lfn)
                if seStatus["TapeSE"]:
                    tapeLFNs.add(lfn)

    for _, siteData in sitesData.items():
        siteData["disk"] = len(siteData["disk"])
        siteData["tape"] = len(siteData["tape"])
    return S_OK(sitesData)


def checkFileMetadata(jobDescription: JobDescriptionModel, log):
    """Check file metadata for input data"""

    res = FileCatalog(vo=jobDescription.vo).getFileMetadata(jobDescription.inputData)
    if not res["OK"]:
        return res

    if res["Value"]["Failed"]:
        log.warn(f"Failed to establish some GUIDs: {res['Value']['Failed']}")

    return S_OK()


def resolveStaging(inputData, idSites):
    diskSites = []
    maxOnDisk = 0
    bestSites = []

    for site in idSites:
        nDisk = idSites[site]["disk"]
        if nDisk == len(inputData):
            diskSites.append(site)
        if nDisk > maxOnDisk:
            maxOnDisk = nDisk
            bestSites = [site]
        elif nDisk == maxOnDisk:
            bestSites.append(site)

    # If there are selected sites, those are disk only sites
    if diskSites:
        return (False, diskSites)

    return (True, bestSites)


def preRequestStaging(jobDescription: JobDescriptionModel, stageSite, opData, log: LoggingRoot):
    tapeSEs = []
    diskSEs = []

    # Get the connection level
    if jobDescription.inputDataPolicy and "download" in jobDescription.inputDataPolicy.lower():
        connectionLevel = "DOWNLOAD"
    else:
        connectionLevel = "PROTOCOL"

    # Allow staging from SEs accessible by protocol
    res = DMSHelpers(vo=jobDescription.vo).getSEsForSite(stageSite, connectionLevel=connectionLevel)
    if not res["OK"]:
        return S_ERROR(f"Could not determine SEs for site {stageSite}")
    siteSEs = res["Value"]

    for seName in siteSEs:
        se = StorageElement(seName, vo=jobDescription.vo)
        seStatus = se.getStatus()
        if not seStatus["OK"]:
            return seStatus
        seStatus = seStatus["Value"]
        if seStatus["Read"] and seStatus["TapeSE"]:
            tapeSEs.append(seName)
        if seStatus["Read"] and seStatus["DiskSE"]:
            diskSEs.append(seName)

    if not tapeSEs:
        return S_ERROR(f"No Local SEs for site {stageSite}")

    log.debug(f"Tape SEs are {', '.join(tapeSEs)}")

    # I swear this is horrible DM code it's not mine.
    # Eternity of hell to the inventor of the Value of Value of Success of...
    inputData = opData["Value"]["Value"]["Successful"]
    stageLFNs = {}
    lfnToStage = []
    for lfn in inputData:
        replicas = inputData[lfn]
        # Check SEs
        seStage = []
        for seName in replicas:
            if seName in diskSEs:
                # This lfn is in disk. Skip it
                seStage = []
                break
            if seName not in tapeSEs:
                # This lfn is not in this tape SE. Check next SE
                continue
            seStage.append(seName)
        for seName in seStage:
            if seName not in stageLFNs:
                stageLFNs[seName] = []
            stageLFNs[seName].append(lfn)
            if lfn not in lfnToStage:
                lfnToStage.append(lfn)

    if not stageLFNs:
        return S_ERROR("Cannot find tape replicas")

    # Check if any LFN is in more than one SE
    # If that's the case, try to stage from the SE that has more LFNs to stage to group the request
    # 1.- Get the SEs ordered by ascending replicas
    sortedSEs = reversed(sorted((len(stageLFNs[seName]), seName) for seName in stageLFNs))
    for lfn in lfnToStage:
        found = False
        # 2.- Traverse the SEs
        for _stageCount, seName in sortedSEs:
            if lfn in stageLFNs[seName]:
                # 3.- If first time found, just mark as found. Next time delete the replica from the request
                if found:
                    stageLFNs[seName].remove(lfn)
                else:
                    found = True
            # 4.-If empty SE, remove
            if not stageLFNs[seName]:
                stageLFNs.pop(seName)

    return S_OK(stageLFNs)


def updateSharedSESites(jobDescription: JobDescriptionModel, stageSite, stagedLFNs, opData, log: LoggingRoot):
    siteCandidates = opData["SiteCandidates"]

    seStatus = {}
    for siteName in siteCandidates:
        if siteName == stageSite:
            continue
        log.debug(f"Checking {siteName} for shared SEs")
        siteData = siteCandidates[siteName]
        res = getSEsForSite(siteName)
        if not res["OK"]:
            continue
        closeSEs = res["Value"]
        diskSEs = []
        for seName in closeSEs:
            # If we don't have the SE status get it and store it
            if seName not in seStatus:
                seStatus[seName] = StorageElement(seName, vo=jobDescription.vo).status()
            # get the SE status from mem and add it if its disk
            status = seStatus[seName]
            if status["Read"] and status["DiskSE"]:
                diskSEs.append(seName)
        log.debug(f"Disk SEs for {siteName} are {', '.join(diskSEs)}")

        # Hell again to the dev of this crappy value of value of successful of ...
        lfnData = opData["Value"]["Value"]["Successful"]
        for seName in stagedLFNs:
            # If the SE is not close then skip it
            if seName not in closeSEs:
                continue
            for lfn in stagedLFNs[seName]:
                log.debug(f"Checking {seName} for {lfn}")
                # I'm pretty sure that this cannot happen :P
                if lfn not in lfnData:
                    continue
                # Check if it's already on disk at the site
                onDisk = False
                for siteSE in lfnData[lfn]:
                    if siteSE in diskSEs:
                        log.verbose("lfn on disk", f": {lfn} at {siteSE}")
                        onDisk = True
                # If not on disk, then update!
                if not onDisk:
                    log.verbose("Setting LFN to disk", f"for {seName}")
                    siteData["disk"] += 1
                    siteData["tape"] -= 1


def setJobSite(jobId: int, siteList: list[str], onlineSites=None):
    """Set the site attribute"""
    if onlineSites is None:
        onlineSites = []

    numSites = len(siteList)
    if numSites == 0:
        siteName = "ANY"
    elif numSites == 1:
        siteName = siteList[0]
    else:
        # If the job has input data, the online sites are hosting the data
        if not onlineSites:
            # No input site reported (could be a user job)
            siteName = "Multiple"
        if len(onlineSites) == 1:
            siteName = f"Group.{'.'.join(list(onlineSites)[0].split('.')[1:])}"
        else:
            # More than one site with input
            siteName = "MultipleInput"

    return JobStateUpdateClient().setJobSite(jobId, siteName)
