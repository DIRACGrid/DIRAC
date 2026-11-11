"""FTS3Job module containing only the FTS3Job class"""

import errno
import os
import requests

from cachetools import LRUCache, TTLCache, cached
from threading import Lock
from typing import Optional

from DIRAC.Resources.Storage.StorageElement import StorageElement

from DIRAC.FrameworkSystem.Client.Logger import gLogger
from DIRAC.FrameworkSystem.Utilities.TokenManagementUtilities import getIdProviderClient

from DIRAC.Core.Utilities.ReturnValues import S_OK, S_ERROR, returnValueOrRaise
from DIRAC.Core.Utilities.DErrno import cmpError

from DIRAC.Core.Utilities.JEncode import JSerializable
from DIRAC.Core.Utilities.TimeUtilities import DiracTime
from DIRAC.DataManagementSystem.Client.FTS3File import FTS3File
from DIRAC.DataManagementSystem.private import FTSRestLibs

# 3 days in seconds
BRING_ONLINE_TIMEOUT = 259200

# Number of IdP to keep in cache. Should correspond roughly
# to the number of groups performing transfers
IDP_CACHE_SIZE = 8


_scitag_cache = TTLCache(maxsize=10, ttl=3600)
_scitag_lock = Lock()
_scitag_json_cache = TTLCache(maxsize=1, ttl=86400)
_scitag_json_lock = Lock()


@cached(_scitag_cache, lock=_scitag_lock)
def get_scitag(vo: str, activity: Optional[str] = None) -> int:
    """
    Get the scitag based on the VO and activity.
    If the VO is not found in the scitag.json, it defaults to 1.
    If no specific activity is provided, it defaults to the "default" activityName.

    :param vo: The VO for which to get the scitag
    :param activity: The activity for which to get the scitag
    :return: The scitag value
    """

    @cached(_scitag_json_cache, lock=_scitag_json_lock)
    def get_remote_json():
        gLogger.verbose("Fetching https://scitags.org/api.json from the network")
        response = requests.get("https://scitags.org/api.json", timeout=30)
        response.raise_for_status()
        return response.json()

    vo_id = 1  # Default VO ID
    activity_id = 1  # Default activity ID

    try:
        # Load the JSON data from the cache or network
        sj = get_remote_json()

        for experiment in sj.get("experiments", []):
            if experiment.get("expName") == vo.lower():
                vo_id = experiment.get("expId")
                for act in experiment.get("activities", []):
                    if act.get("activityName") == activity:
                        activity_id = act.get("activityId")
    except Exception as e:
        gLogger.error(f"Error fetching or parsing scitag.json. Using default scitag values.", repr(e))
    # Logic to determine the scitag based on vo and activity (this is what FTS wants)
    return vo_id << 6 | activity_id  # Example logic, replace with actual implementation


class FTS3Job(JSerializable):
    """Abstract class to represent a job to be executed by FTS. It belongs
    to an FTS3Operation
    """

    # START states

    # States from FTS doc https://fts3-docs.web.cern.ch/fts3-docs/docs/state_machine.html
    ALL_STATES = [
        "Submitted",  # Initial state of a job as soon it's dropped into the database
        "Ready",  # One of the files within a job went to Ready state
        "Active",  # One of the files within a job went to Active state
        "Finished",  # All files Finished gracefully
        "Canceled",  # Job canceled
        "Failed",  # All files Failed
        "Finisheddirty",  # Some files Failed
        "Staging",  # One of the files within a job went to Staging state
        "Archiving",  # From FTS: one of the files within a job went to Archiving state
    ]

    FINAL_STATES = ["Canceled", "Failed", "Finished", "Finisheddirty"]

    # This field is only used for optimizing sql queries (`in`` instead of `not in`)
    NON_FINAL_STATES = list(set(ALL_STATES) - set(FINAL_STATES))
    INIT_STATE = "Submitted"

    # END states
    _attrToSerialize = [
        "jobID",
        "operationID",
        "status",
        "error",
        "submitTime",
        "lastUpdate",
        "ftsServer",
        "ftsGUID",
        "completeness",
        "username",
        "userGroup",
    ]

    _idp_cache = LRUCache(maxsize=IDP_CACHE_SIZE)

    def __init__(self):
        self.submitTime = None
        self.lastUpdate = None
        self.lastMonitor = None

        self.ftsGUID = None
        self.ftsServer = None

        self.error = None
        self.status = FTS3Job.INIT_STATE

        self.completeness = None
        self.operationID = None

        self.username = None
        self.userGroup = None

        # temporary used only for submission
        # Set by FTS Operation when preparing
        self.type = None  # Transfer

        self.sourceSE = None
        self.multiHopSE = None
        self.targetSE = None
        self.filesToSubmit = []
        self.activity = None
        self.priority = None
        self.vo = None
        self.rmsReqID = None

        # temporary used only for submission
        # Set by prepareSubmission, and used by FTSRestLibs.submit
        self.transfers = []
        self.fileIDsInTheJob = set()
        self.multihop = False
        self.bringOnline = None
        self.copyPinLifetime = None
        self.archiveTimeout = None
        self.sourceSpaceToken = None
        self.targetSpaceToken = None
        self.jobMetadata = {}
        self.useTokens = False

        # temporary used only for accounting
        # it is set by the monitor method
        # when a job is in a final state
        self.accountingDicts = None

    @staticmethod
    @cached(_idp_cache)
    def _getIdpClient(group_name: str):
        return returnValueOrRaise(getIdProviderClient(group_name, None, client_name_prefix="fts"))

    def monitor(self, context):
        """Queries the fts server to monitor the job.
        The internal state of the object is updated depending on the
        monitoring result.

        In case the job is not found on the server, the status is set to 'Failed'

        Within a job, only the transfers having a `fileID` metadata are considered.
        This is to allow for multihop jobs doing a staging

        This method assumes that the attribute self.ftsGUID is set

        :param context: context to the server the job was submitted to
                        (see :py:class:`~DIRAC.DataManagementSystem.private.FTSRestLibs.FTSContextCache`)

        :returns: {FileID: { status, error } }

                  Possible error numbers

                  * errno.ESRCH: If the job does not exist on the server
                  * errno.EDEADLK: In case the job and file status are inconsistent (see comments inside the code)


        """

        if not self.ftsGUID:
            return S_ERROR("FTSGUID not set, FTS job not submitted?")

        res = FTSRestLibs.getJobStatus(context, self.ftsGUID)
        if not res["OK"]:
            # The job is not found
            # Set its status to Failed and return
            if cmpError(res, errno.ESRCH):
                self.status = "Failed"
            return res
        jobStatusDict = res["Value"]

        now = DiracTime.utcnow().replace(microsecond=0)
        self.lastMonitor = now

        newStatus = jobStatusDict["job_state"].capitalize()
        if newStatus != self.status:
            self.status = newStatus
            self.lastUpdate = now
            self.error = jobStatusDict["reason"]

        if newStatus in self.FINAL_STATES:
            self._fillAccountingDicts(jobStatusDict)

        filesInfoList = jobStatusDict["files"]
        filesStatus = {}
        statusSummary = {}

        # Make a copy, since we are potentially

        # deleting objects
        for fileDict in list(filesInfoList):
            file_state = fileDict["file_state"].capitalize()
            file_metadata = fileDict["file_metadata"]

            # previous version of the code did not have dictionary as
            # file_metadata
            if isinstance(file_metadata, dict):
                file_id = file_metadata.get("fileID")
            else:
                file_id = file_metadata

            # The transfer does not have a fileID attached to it
            # so it does not correspond to a file in our DB: skip it
            # (typical of jobs with different staging protocol == CTA)
            # We also remove it from the fileInfoList, such that it is
            # not considered for accounting
            if not file_id:
                filesInfoList.remove(fileDict)
                continue

            file_error = fileDict["reason"]
            filesStatus[file_id] = {"status": file_state, "error": file_error}

            # If the state of the file is final for FTS, set ftsGUID of the file to None,
            # such that it is "released" from this job and not updated anymore in future
            # monitoring calls
            if file_state in FTS3File.FTS_FINAL_STATES:
                filesStatus[file_id]["ftsGUID"] = None

                # If the file is failed, check if it is recoverable
                # recoverable can be True/False for disk <-> disk transfers
                # but would be None for tape or stalled transfers,
                # so it is safer to retry.
                if file_state in FTS3File.FTS_FAILED_STATES:
                    if fileDict.get("recoverable", True) is False:
                        filesStatus[file_id]["status"] = "Defunct"

            # If the file is not in a final state, but the job is, we return an error
            # FTS can have inconsistencies where the FTS Job is in a final state
            # but not all the files.
            # The inconsistencies are cleaned every hour on the FTS side.
            # https://its.cern.ch/jira/browse/FTS-1482
            elif self.status in self.FINAL_STATES:
                return S_ERROR(
                    errno.EDEADLK,
                    "Job %s in a final state (%s) while File %s is not (%s)"
                    % (self.ftsGUID, self.status, file_id, file_state),
                )

            statusSummary[file_state] = statusSummary.get(file_state, 0) + 1

        # We've removed all the intermediate transfers that we are not interested in
        # so we put this back into the monitoring data such that the accounting is done properly
        jobStatusDict["files"] = filesInfoList
        if newStatus in self.FINAL_STATES:
            self._fillAccountingDicts(jobStatusDict)

        total = len(filesInfoList)
        completed = sum(statusSummary.get(state, 0) for state in FTS3File.FTS_FINAL_STATES)
        self.completeness = int(100 * completed / total)

        return S_OK(filesStatus)

    def cancel(self, context):
        """Cancel the job on the FTS server. Note that it will cancel all the files.
        See https://fts3-docs.web.cern.ch/fts3-docs/fts-rest/docs/api.html#delete-jobsjobidlist
        for behavior details

        :param context: context to the server the job was submitted to

        :returns: S_OK()/S_ERROR() (errno.ESRCH if the job is not found)
        """

        res = FTSRestLibs.cancel(context, self.ftsGUID)
        if not res["OK"]:
            return res
        newStatus = res["Value"]["job_state"].capitalize()
        # If the status is already canceled
        # (for a reason or another, don't change the error message)
        # If the new status is Canceled, set it, and update the reason
        if newStatus == "Canceled" and self.status != "Canceled":
            self.status = "Canceled"
            self.error = "Matching RMS Request was canceled"
        return S_OK()

    @staticmethod
    def __fetchSpaceToken(seName, vo):
        """Fetch the space token of storage element

        :param seName: name of the storageElement
        :param vo: vo of the job
        :returns: space token. If there is no SpaceToken defined, returns None
        """
        seToken = None
        if seName:
            seObj = StorageElement(seName, vo=vo)

            res = seObj.getStorageParameters(protocol="srm")
            if not res["OK"]:
                # If there is no SRM protocol, we do not specify
                # the space token
                if cmpError(res, errno.ENOPROTOOPT):
                    return S_OK(None)

                return res

            seToken = res["Value"].get("SpaceToken")

        return S_OK(seToken)

    @staticmethod
    def __isTapeSE(seName, vo):
        """Check whether a given SE is a tape storage

        :param seName: name of the storageElement
        :param vo: VO of the job
        :returns: True/False
                  In case of error, returns True.
                  It is better to lose a bit of time on the FTS
                  side, rather than failing jobs because the FTS default
                  pin time is too short
        """
        isTape = StorageElement(seName, vo=vo).getStatus().get("Value", {}).get("TapeSE", True)

        return isTape

    @staticmethod
    def __seTokenSupport(seObj):
        """Check whether a given SE supports token

        :param seObj: StorageElement object

        :returns: True/False
                  In case of error, returns False
        """
        return seObj.options.get("TokenSupport", "").lower() in ("true", "yes")

    def _constructTransferJob(self, pinTime, allLFNs, target_spacetoken, protocols=None, tokensEnabled=False):
        """Build a job for transfer

        Some attributes of the job are expected to be set
          * sourceSE
          * targetSE
          * multiHopSE (optional)
          * activity (optional)
          * priority (optional)
          * filesToSubmit
          * operationID (optional, used as metadata for the job)

        Note that, because of FTS limitations (and also because it anyway would be "not very smart"),
        multiHop can only use non-SRM disk storage as hops.


        :param pinTime: pining time in case staging is needed
        :param allLFNs: list of LFNs to transfer
        :param failedLFNs: set of LFNs in filesToSubmit for which there was a problem
        :param target_spacetoken: the space token of the target
        :param protocols: list of protocols to restrict the protocol choice for the transfer
        :param tokensEnabled: whether tokens can be used for the transfers

        The result is stored in the job attributes used by
        :py:func:`~DIRAC.DataManagementSystem.private.FTSRestLibs.buildJob`
        (``transfers``, ``multihop``, ``bringOnline``, etc)

        :return: S_OK(set of ftsFileIDs in the job)
        """

        log = gLogger.getLocalSubLogger(f"constructTransferJob/{self.operationID}/{self.sourceSE}_{self.targetSE}")

        isMultiHop = False
        useTokens = False

        # Check if it is a multiHop transfer
        if self.multiHopSE:
            if len(allLFNs) != 1:
                log.debug(f"Multihop job has {len(allLFNs)} files while only 1 allowed")
                return S_ERROR(errno.E2BIG, "Trying multihop job with more than one file !")
            allHops = [(self.sourceSE, self.multiHopSE), (self.multiHopSE, self.targetSE)]
            if tokensEnabled:
                tokensEnabled = all(
                    [
                        self.__seTokenSupport(StorageElement(seName))
                        for seName in (self.sourceSE, self.multiHopSE, self.targetSE)
                    ]
                )
                if not tokensEnabled:
                    log.warn("Not using token because not all hop supports it")
            isMultiHop = True
        else:
            allHops = [(self.sourceSE, self.targetSE)]

        nbOfHops = len(allHops)

        res = self.__fetchSpaceToken(self.sourceSE, self.vo)
        if not res["OK"]:
            return res
        source_spacetoken = res["Value"]

        failedLFNs = set()

        copy_pin_lifetime = None
        bring_online = None
        archive_timeout = None

        transfers = []

        fileIDsInTheJob = set()

        for hopId, (hopSrcSEName, hopDstSEName) in enumerate(allHops, start=1):
            # Again, this is relevant only for the very initial source
            # but code factorization is more important
            hopSrcIsTape = self.__isTapeSE(hopSrcSEName, self.vo)

            dstSE = StorageElement(hopDstSEName, vo=self.vo)
            srcSE = StorageElement(hopSrcSEName, vo=self.vo)

            # getting all the (source, dest) surls
            res = dstSE.generateTransferURLsBetweenSEs(allLFNs, srcSE, protocols=protocols)
            if not res["OK"]:
                return res

            for lfn, reason in res["Value"]["Failed"].items():
                failedLFNs.add(lfn)
                log.error("Could not get source SURL", f"{lfn} {reason}")

            allSrcDstSURLs = res["Value"]["Successful"]
            srcProto, destProto = res["Value"]["Protocols"]

            # If the source is a tape SE, we should set the
            # copy_pin_lifetime and bring_online params
            # In case of multihop, this is relevant only for the
            # original source, but again, code factorization is more important
            if hopSrcIsTape:
                copy_pin_lifetime = pinTime
                bring_online = srcSE.options.get("BringOnlineTimeout", BRING_ONLINE_TIMEOUT)

            # If the destination is a tape, and the protocol supports it,
            # check if we want to have an archive timeout
            # In case of multihop, this is relevant only for the
            # final target, but again, code factorization is more important
            dstIsTape = self.__isTapeSE(hopDstSEName, self.vo)
            if dstIsTape and destProto in dstSE.localStageProtocolList:
                archive_timeout = dstSE.options.get("ArchiveTimeout")

            # This contains the staging URLs if they are different from the transfer URLs
            # (CTA...)
            allStageURLs = dict()

            # In case we are transfering from a tape system, and the stage protocol
            # is not the same as the transfer protocol, we generate the staging URLs
            # to do a multihop transfer. See below.
            if hopSrcIsTape and srcProto not in srcSE.localStageProtocolList:
                isMultiHop = True
                # As of version 3.10, FTS can only handle one file per multi hop
                # job. If we are here, that means that we need one, so make sure that
                # we only have a single file to transfer (this should have been checked
                # at the job construction step in FTS3Operation).
                # This test is important, because multiple files would result in the source
                # being deleted !
                if len(allLFNs) != 1:
                    log.debug(f"Multihop job has {len(allLFNs)} files while only 1 allowed")
                    return S_ERROR(errno.E2BIG, "Trying multihop job with more than one file !")

                res = srcSE.getURL(allSrcDstSURLs, protocol=srcSE.localStageProtocolList)

                if not res["OK"]:
                    return res

                for lfn, reason in res["Value"]["Failed"].items():
                    failedLFNs.add(lfn)
                    log.error("Could not get stage SURL", f"{lfn} {reason}")
                    allSrcDstSURLs.pop(lfn)

                allStageURLs = res["Value"]["Successful"]

            for ftsFile in self.filesToSubmit:
                if ftsFile.lfn in failedLFNs:
                    log.debug(f"Not preparing transfer for file {ftsFile.lfn}")
                    continue

                srcToken = None
                dstToken = None

                sourceSURL, targetSURL = allSrcDstSURLs[ftsFile.lfn]
                stageURL = allStageURLs.get(ftsFile.lfn)

                if sourceSURL == targetSURL:
                    log.error("sourceSURL equals to targetSURL", f"{ftsFile.lfn}")
                    ftsFile.error = "sourceSURL equals to targetSURL"
                    ftsFile.status = "Defunct"
                    continue

                ftsFileID = getattr(ftsFile, "fileID")

                # scitag 65 is 1 << 6 | 1 (default experiment, default activity)
                scitag = get_scitag(vo=self.vo, activity=self.activity)

                # Under normal circumstances, we simply submit an fts transfer as such:
                # * srcProto://myFile -> destProto://myFile
                #
                # Even in case of the source storage being a tape system, it works fine.
                # However, if the staging and transfer protocols are different (which might be the case for CTA),
                #  we use the multihop machinery to submit two sequential fts transfers:
                # one to stage, one to transfer.
                # It looks like such
                # * stageProto://myFile -> stageProto://myFile
                # * srcProto://myFile -> destProto://myFile

                if stageURL:
                    # We do not set a fileID in the metadata
                    # such that we do not update the DB when monitoring
                    stageTrans_metadata = {"desc": f"PreStage {ftsFileID}"}

                    # If we use an activity, also set it as file metadata
                    # for WLCG monitoring purposes
                    # https://its.cern.ch/jira/projects/DOMATPC/issues/DOMATPC-14?
                    if self.activity:
                        stageTrans_metadata["activity"] = self.activity

                    stageTrans = {
                        "source": stageURL,
                        "destination": stageURL,
                        "checksum": f"ADLER32:{ftsFile.checksum}",
                        "filesize": ftsFile.size,
                        "metadata": stageTrans_metadata,
                        "activity": self.activity,
                        "scitag": scitag,
                    }
                    transfers.append(stageTrans)

                # If it is the last hop only, we set the fileID metadata
                # for monitoring
                if hopId == nbOfHops:
                    trans_metadata = {"desc": f"Transfer {ftsFileID}", "fileID": ftsFileID}
                else:
                    trans_metadata = {"desc": f"MultiHop {ftsFileID}"}

                # If we use an activity, also set it as file metadata
                # for WLCG monitoring purposes
                # https://its.cern.ch/jira/projects/DOMATPC/issues/DOMATPC-14?
                if self.activity:
                    trans_metadata["activity"] = self.activity

                # Add tokens if both storages support it and if the requested
                if tokensEnabled and self.__seTokenSupport(srcSE) and self.__seTokenSupport(dstSE):
                    # We get a read token for the source
                    # offline_access is to allow FTS to refresh it
                    res = srcSE.getWLCGTokenPath(ftsFile.lfn)
                    if not res["OK"]:
                        return res
                    srcTokenPath = res["Value"]
                    res = self._getIdpClient(self.userGroup).fetchToken(
                        grant_type="client_credentials",
                        scope=[f"storage.read:/{srcTokenPath}", "offline_access"],
                        # TODO: add a specific audience
                    )
                    if not res["OK"]:
                        return res
                    srcToken = res["Value"]["access_token"]

                    # We get a token with modify and read for the destination
                    # We need the read to be able to stat
                    # CAUTION: only works with dcache for now, other storages
                    # interpret permissions differently
                    # offline_access is to allow FTS to refresh it
                    res = dstSE.getWLCGTokenPath(ftsFile.lfn)
                    if not res["OK"]:
                        return res
                    dstTokenPath = res["Value"]
                    res = self._getIdpClient(self.userGroup).fetchToken(
                        grant_type="client_credentials",
                        scope=[
                            f"storage.modify:/{dstTokenPath}",
                            f"storage.read:/{dstTokenPath}",
                            # Needed because CNAF
                            # https://ggus.eu/index.php?mode=ticket_info&ticket_id=165048
                            f"storage.read:/{os.path.dirname(dstTokenPath)}",
                            "offline_access",
                        ],
                        # TODO: add a specific audience
                    )
                    if not res["OK"]:
                        return res
                    dstToken = res["Value"]["access_token"]
                    useTokens = True

                # because of an xroot bug (https://github.com/xrootd/xrootd/issues/1433)
                # the checksum needs to be lowercase. It does not impact the other
                # protocol, so it's fine to put it here.
                # I only add it in this transfer and not the "staging" one above because it
                # impacts only root -> root transfers
                trans = {
                    "source": sourceSURL,
                    "destination": targetSURL,
                    "checksum": f"ADLER32:{ftsFile.checksum.lower()}",
                    "filesize": ftsFile.size,
                    "metadata": trans_metadata,
                    "activity": self.activity,
                    "source_token": srcToken,
                    "destination_token": dstToken,
                    "scitag": scitag,
                }

                transfers.append(trans)
                fileIDsInTheJob.add(ftsFileID)

        if not transfers:
            log.error("No transfer possible!")
            return S_ERROR(errno.ENODATA, "No transfer possible")

        # We add a few metadata to the fts job so that we can reuse them later on without
        # querying our DB.
        # source and target SE are just used for accounting purpose
        job_metadata = {
            "operationID": self.operationID,
            "rmsReqID": self.rmsReqID,
            "sourceSE": self.sourceSE,
            "targetSE": self.targetSE,
            "useTokens": useTokens,
        }

        if self.activity:
            job_metadata["activity"] = self.activity

        self.transfers = transfers
        self.fileIDsInTheJob = fileIDsInTheJob
        self.multihop = isMultiHop
        self.bringOnline = bring_online
        self.copyPinLifetime = copy_pin_lifetime
        self.archiveTimeout = archive_timeout
        self.sourceSpaceToken = source_spacetoken
        self.targetSpaceToken = target_spacetoken
        self.jobMetadata = job_metadata
        self.useTokens = useTokens

        return S_OK(fileIDsInTheJob)

    def prepareSubmission(self, pinTime=36000, protocols=None, tokensEnabled=False):
        """Prepare everything needed to submit the job, independently of the FTS server.
        This chooses all the storage related parameters (URLs, protocols, tokens, staging, ...).

        Some attributes are expected to be defined for this to work:
          * type (set by FTS3Operation)
          * sourceSE
          * targetSE
          * multiHopSE (optional)
          * activity (optional)
          * priority (optional)
          * username
          * userGroup
          * filesToSubmit
          * operationID (optional, used as metadata for the job)

        We also expect the FTSFiles have an ID defined, as it is given as transfer metadata

        The result is stored in the attributes used by
        :py:func:`~DIRAC.DataManagementSystem.private.FTSRestLibs.buildJob`

        :param pinTime: Time the file should be pinned on disk
                        Used only if he source SE is a tape storage
        :param protocols: list of protocols from which we should choose the protocol to use
        :param tokensEnabled: whether tokens can be used for the transfers

        :returns: S_OK(set of FTSFiles ids that will be submitted)
        """

        if self.type != "Transfer":
            return S_ERROR(f"Unsupported job type {self.type}")

        res = self.__fetchSpaceToken(self.targetSE, self.vo)
        if not res["OK"]:
            return res
        target_spacetoken = res["Value"]

        allLFNs = [ftsFile.lfn for ftsFile in self.filesToSubmit]

        return self._constructTransferJob(
            pinTime, allLFNs, target_spacetoken, protocols=protocols, tokensEnabled=tokensEnabled
        )

    def submit(self, context):
        """Submit the job prepared by :py:meth:`prepareSubmission` to the FTS server

        :param context: context to the server to submit to. It has to be token based if the job uses tokens

        :returns: S_OK(set of FTSFiles ids submitted)
        """

        log = gLogger.getLocalSubLogger(f"submit/{self.operationID}/{self.sourceSE}_{self.targetSE}")

        if not self.transfers:
            return S_ERROR(errno.ENODATA, "Job not prepared for submission")

        if self.useTokens and not FTSRestLibs.usesTokens(context):
            return S_ERROR("Job needs token support but no FTS token was supplied")

        res = FTSRestLibs.submit(context, self)
        if not res["OK"]:
            log.error("Error at submission", res["Message"])
            return res
        self.ftsGUID = res["Value"]

        log.info(f"Got GUID {self.ftsGUID}")

        # Only increase the amount of attempt
        # if we succeeded in submitting -> no ! Why did I do that ??
        for ftsFile in self.filesToSubmit:
            ftsFile.attempt += 1

            # This should never happen because a file should be "released"
            # first by the previous job.
            # But we just print a warning
            if ftsFile.ftsGUID is not None:
                log.warn(
                    "FTSFile has a non NULL ftsGUID at job submission time",
                    f"FileID: {ftsFile.fileID} existing ftsGUID: {ftsFile.ftsGUID}",
                )

            # `assign` the file to this job
            ftsFile.ftsGUID = self.ftsGUID
            if ftsFile.fileID in self.fileIDsInTheJob:
                ftsFile.status = "Submitted"

        now = DiracTime.utcnow().replace(microsecond=0)
        self.submitTime = now
        self.lastUpdate = now
        self.lastMonitor = now

        return S_OK(self.fileIDsInTheJob)

    def _fillAccountingDicts(self, jobStatusDict):
        """This methods generates the necessary information to create DataOperation
        accounting records, and stores them as a instance attribute.

        For it to be relevant, it should be called only when the job is in a final state.

        :param jobStatusDict: output of FTSRestLibs.getJobStatus

        :returns: None
        """

        accountingDicts = []
        accountingDict = dict()
        sourceSE = None
        targetSE = None

        accountingDict["OperationType"] = "ReplicateAndRegister"

        accountingDict["User"] = self.username
        accountingDict["Protocol"] = "FTS3"
        accountingDict["ExecutionSite"] = self.ftsServer

        # Registration values must be set anyway
        accountingDict["RegistrationTime"] = 0.0
        accountingDict["RegistrationOK"] = 0
        accountingDict["RegistrationTotal"] = 0

        # We cannot rely on all the transient attributes (like self.filesToSubmit)
        # because it is probably not filed by the time we monitor !

        filesInfoList = jobStatusDict["files"]
        successfulFiles = []
        failedFiles = []

        for fileDict in filesInfoList:
            file_state = fileDict["file_state"].capitalize()
            if file_state in FTS3File.FTS_SUCCESS_STATES:
                successfulFiles.append(fileDict)
            else:
                failedFiles.append(fileDict)

        job_metadata = jobStatusDict["job_metadata"]
        # previous version of the code did not have dictionary as
        # job_metadata
        if isinstance(job_metadata, dict):
            sourceSE = job_metadata.get("sourceSE")
            targetSE = job_metadata.get("targetSE")

        accountingDict["Source"] = sourceSE
        accountingDict["Destination"] = targetSE

        if successfulFiles:
            successfulDict = accountingDict.copy()
            successfulDict["TransferOK"] = len(successfulFiles)
            successfulDict["TransferTotal"] = len(successfulFiles)
            # We need this if in the list comprehension because staging only jobs have `None` as filesize
            successfulDict["TransferSize"] = sum(
                fileDict["filesize"] for fileDict in successfulFiles if fileDict["filesize"]
            )
            successfulDict["FinalStatus"] = "Finished"

            # We need this if in the list comprehension because staging only jobs have `None` as tx_duration
            successfulDict["TransferTime"] = sum(
                int(fileDict["tx_duration"]) for fileDict in successfulFiles if fileDict["tx_duration"]
            )
            accountingDicts.append(successfulDict)
        if failedFiles:
            failedDict = accountingDict.copy()
            failedDict["TransferOK"] = 0
            failedDict["TransferTotal"] = len(failedFiles)
            failedDict["TransferSize"] = 0
            failedDict["FinalStatus"] = "Failed"
            failedDict["TransferTime"] = 0
            accountingDicts.append(failedDict)

        self.accountingDicts = accountingDicts
