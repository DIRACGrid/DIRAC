from DIRAC import S_OK, gLogger
from DIRAC.ConfigurationSystem.Client.Helpers.Operations import Operations
from DIRAC.Core.Utilities.ReturnValues import S_ERROR
from DIRAC.DataManagementSystem.Utilities.DMSHelpers import DMSHelpers
from DIRAC.Resources.Storage.StorageElement import StorageElement
from DIRAC.StorageManagementSystem.Client.StorageManagerClient import StorageManagerClient
from DIRAC.WorkloadManagementSystem.CheckedJobAdministrator.CheckedJobHandler import CheckedJobHandler
from DIRAC.WorkloadManagementSystem.Client import JobStatus
from DIRAC.WorkloadManagementSystem.Client.JobState.JobState import JobState


class CheckedToStagingHandler(CheckedJobHandler):
    """
    The CheckedToStagingHandler will get the list of job sites available, and if it
    founds out that all the job sites have the input data on tape, it will select
    a job site where the input data will be put on disk and it will set the
    job to the staged status. Otherwise, it will go to the next CheckedJobHandler
    """

    def __init__(self, jobState: JobState):
        super().__init__(jobState)
        self.__log = gLogger.getSubLogger(f"[jid={jobState.jid}]{__name__}")
        self.__operations = Operations()
        self.storageManagerClient = StorageManagerClient()

    def handle(self):

        # Get input data
        result = self.jobState.getInputData()
        if not result["OK"]:
            self.__log.error("Failed to get input data from JobDB", result["Message"])
            return result

        # No input data? No staging required
        if not result["Value"]:
            self.__log.info("No input data. Skipping")
            return super().optimize()

        # Get input data LFNs state
        idAgent = self.__operations.getValue("OnlineSiteHandlerAgent", "OnlineSiteHandler")
        result = self.retrieveOptimizerParam(idAgent)
        if not result["OK"]:
            self.__log.error("Could not retrieve online site handler info", result["Message"])
            return result
        LFNs = result["Value"]

        # No offline LFNs? No staging required
        if "offlineLFNs" not in LFNs:
            return super().handle()

        offlineLFNs = LFNs["offlineLFNs"]

        self.__log.debug(
            "Stage request will be \n\t%s" % "\n\t".join(["%s:%s" % (lfn, offlineLFNs[lfn]) for lfn in offlineLFNs])
        )

        result = self.jobState.setStatus(
            JobStatus.STAGING,
            self.__operations.getValue("StagingMinorStatus", "Request To Be Sent"),
            appStatus="",
            source=self.__class__.__name__,
        )
        if not result["OK"]:
            return result

        # Send the actual storage request
        result = StorageManagerClient().setRequest(
            offlineLFNs,
            "WorkloadManagement",
            "updateJobFromStager@WorkloadManagement/JobStateUpdate",
            int(self.jobState.jid),
        )
        if not result["OK"]:
            self.__log.error("Could not send stage request", result["Message"])
            return result

        rid = str(result["Value"])
        self.__log.info("Stage request sent", rid)

        # Setting the job status to STAGING
        return self.jobState.setStatus(
            JobStatus.STAGING,
            self.__operations.getValue("StagingMinorStatus", "Request Sent"),
            appStatus="",
            source=self.__class__.__name__,
        )


def __preRequestStaging(self, jobManifest, stageSite, opData):

    tapeSEs = []
    diskSEs = []
    vo = jobManifest.getOption("VirtualOrganization")
    inputDataPolicy = jobManifest.getOption("InputDataPolicy", "Protocol")
    connectionLevel = "DOWNLOAD" if "download" in inputDataPolicy.lower() else "PROTOCOL"
    # Allow staging from SEs accessible by protocol
    result = DMSHelpers(vo=vo).getSEsForSite(stageSite, connectionLevel=connectionLevel)
    if not result["OK"]:
        return S_ERROR("Could not determine SEs for site %s" % stageSite)
    siteSEs = result["Value"]

    for storageElements in siteSEs:
        se = StorageElement(storageElements, vo=vo)
        result = se.getStatus()
        if not result["OK"]:
            return result
        seStatus = result["Value"]
        if seStatus["Read"] and seStatus["TapeSE"]:
            tapeSEs.append(storageElements)
        if seStatus["Read"] and seStatus["DiskSE"]:
            diskSEs.append(storageElements)

    if not tapeSEs:
        return S_ERROR("No Local SEs for site %s" % stageSite)

    self.jobLog.debug("Tape SEs are %s" % (", ".join(tapeSEs)))

    # I swear this is horrible DM code it's not mine.
    # Eternity of hell to the inventor of the Value of Value of Success of...
    inputData = opData["Value"]["Value"]["Successful"]
    stageLFNs = {}
    lfnToStage = []
    for lfn in inputData:
        replicas = inputData[lfn]
        # Check SEs
        storageElementsToStage = []
        for storageElements in replicas:
            if storageElements in diskSEs:
                # This lfn is in disk. Skip it
                storageElementsToStage = []
                break
            if storageElements not in tapeSEs:
                # This lfn is not in this tape SE. Check next SE
                continue
            storageElementsToStage.append(storageElements)
        for storageElements in storageElementsToStage:
            if storageElements not in stageLFNs:
                stageLFNs[storageElements] = []
            stageLFNs[storageElements].append(lfn)
            if lfn not in lfnToStage:
                lfnToStage.append(lfn)

    if not stageLFNs:
        return S_ERROR("Cannot find tape replicas")

    # Check if any LFN is in more than one SE
    # If that's the case, try to stage from the SE that has more LFNs to stage to group the request
    # 1.- Get the SEs ordered by ascending replicas
    sortedSEs = reversed(sorted([(len(stageLFNs[seName]), seName) for seName in stageLFNs]))
    for lfn in lfnToStage:
        found = False
        # 2.- Traverse the SEs
        for _stageCount, storageElements in sortedSEs:
            if lfn in stageLFNs[storageElements]:
                # 3.- If first time found, just mark as found. Next time delete the replica from the request
                if found:
                    stageLFNs[storageElements].remove(lfn)
                else:
                    found = True
            # 4.-If empty SE, remove
            if not stageLFNs[storageElements]:
                stageLFNs.pop(storageElements)

    return S_OK(stageLFNs)
