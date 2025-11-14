"""FTS3Job module containing only the FTS3Job class"""

import datetime
import errno
import os
from packaging.version import Version

from cachetools import cachedmethod, LRUCache

# Requires at least version 3.3.3
from fts3 import __version__ as fts3_version
import fts3.rest.client.easy as fts3
from fts3.rest.client.exceptions import FTS3ClientException, NotFound

# There is a breaking change in the API in 3.13
# https://gitlab.cern.ch/fts/fts-rest-flask/-/commit/5faa283e0cd4b80a0139a547c4a6356522c8449d
FTS3_SPACETOKEN_API_CHANGE = Version("3.13")
if Version(fts3_version) >= FTS3_SPACETOKEN_API_CHANGE:
    DESTINATION_SPACETOKEN_ATTR = "destination_spacetoken"
else:
    DESTINATION_SPACETOKEN_ATTR = "spacetoken"

# We specifically use Request in the FTS client because of a leak in the
# default pycurl. See https://its.cern.ch/jira/browse/FTS-261
from fts3.rest.client.request import Request as ftsSSLRequest

from DIRAC.Resources.Storage.StorageElement import StorageElement

from DIRAC.FrameworkSystem.Client.Logger import gLogger
from DIRAC.FrameworkSystem.Client.TokenManagerClient import gTokenManager
from DIRAC.FrameworkSystem.Utilities.TokenManagementUtilities import getIdProviderClient

from DIRAC.Core.Utilities.ReturnValues import S_OK, S_ERROR, returnValueOrRaise
from DIRAC.Core.Utilities.DErrno import cmpError

from DIRAC.Core.Utilities.JEncode import JSerializable
from DIRAC.DataManagementSystem.Client.FTS3File import FTS3File

# 3 days in seconds
BRING_ONLINE_TIMEOUT = 259200

# Number of IdP to keep in cache. Should correspond roughly
# to the number of groups performing transfers
IDP_CACHE_SIZE = 8


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
        self.type = None  # Transfer, Staging

        self.sourceSE = None
        self.multiHopSE = None
        self.targetSE = None
        self.filesToSubmit = []
        self.activity = None
        self.priority = None
        self.vo = None
        self.rmsReqID = None

        # temporary used only for accounting
        # it is set by the monitor method
        # when a job is in a final state
        self.accountingDicts = None

    @classmethod
    @cachedmethod(lambda cls: cls._idp_cache)
    def _getIdpClient(cls, group_name: str):
        return returnValueOrRaise(getIdProviderClient(group_name, None, client_name_prefix="fts"))

    def monitor(self, context=None, ftsServer=None, ucert=None):
        """Queries the fts server to monitor the job.
        The internal state of the object is updated depending on the
        monitoring result.

        In case the job is not found on the server, the status is set to 'Failed'

        Within a job, only the transfers having a `fileID` metadata are considered.
        This is to allow for multihop jobs doing a staging

        This method assumes that the attribute self.ftsGUID is set

        :param context: fts3 context. If not given, it is created (see ftsServer & ucert param)
        :param ftsServer: the address of the fts server to submit to. Used only if context is
                          not given. if not given either, use the ftsServer object attribute

        :param ucert: path to the user certificate/proxy. Might be infered by the fts cli (see its doc)

        :returns: {FileID: { status, error } }

                  Possible error numbers

                  * errno.ESRCH: If the job does not exist on the server
                  * errno.EDEADLK: In case the job and file status are inconsistent (see comments inside the code)


        """

        if not self.ftsGUID:
            return S_ERROR("FTSGUID not set, FTS job not submitted?")
        if not context:
            if not ftsServer:
                ftsServer = self.ftsServer
            context = fts3.Context(endpoint=ftsServer, ucert=ucert, request_class=ftsSSLRequest, verify=False)

        jobStatusDict = None
        try:
            jobStatusDict = fts3.get_job_status(context, self.ftsGUID, list_files=True)
        # The job is not found
        # Set its status to Failed and return
        except NotFound:
            self.status = "Failed"
            return S_ERROR(errno.ESRCH, f"FTSGUID {self.ftsGUID} not found on {self.ftsServer}")
        except FTS3ClientException as e:
            return S_ERROR(f"Error getting the job status {e}")

        now = datetime.datetime.utcnow().replace(microsecond=0)
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
                # TODO: update status to defunct if not recoverable here ?

                # If the file is failed, check if it is recoverable
                if file_state in FTS3File.FTS_FAILED_STATES:
                    if not fileDict.get("Recoverable", True):
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
        """

        try:
            cancelDict = fts3.cancel(context, self.ftsGUID)
            newStatus = cancelDict["job_state"].capitalize()
            # If the status is already canceled
            # (for a reason or another, don't change the error message)
            # If the new status is Canceled, set it, and update the reason
            if newStatus == "Canceled" and self.status != "Canceled":
                self.status = "Canceled"
                self.error = "Matching RMS Request was canceled"
            return S_OK()
        # The job is not found
        except NotFound:
            return S_ERROR(errno.ESRCH, f"FTSGUID {self.ftsGUID} not found on {self.ftsServer}")
        except FTS3ClientException as e:
            return S_ERROR(f"Error canceling the job {e}")

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

        :return: S_OK( (job object, list of ftsFileIDs in the job))
        """

        log = gLogger.getSubLogger(f"constructTransferJob/{self.operationID}/{self.sourceSE}_{self.targetSE}")

        isMultiHop = False
        useTokens = False

        # Check if it is a multiHop transfer
        if self.multiHopSE:
            if len(allLFNs) != 1:
                log.debug(f"Multihop job has {len(allLFNs)} files while only 1 allowed")
                return S_ERROR(errno.E2BIG, "Trying multihop job with more than one file !")
            allHops = [(self.sourceSE, self.multiHopSE), (self.multiHopSE, self.targetSE)]
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

                    stageTrans = fts3.new_transfer(
                        stageURL,
                        stageURL,
                        checksum=f"ADLER32:{ftsFile.checksum}",
                        filesize=ftsFile.size,
                        metadata=stageTrans_metadata,
                        activity=self.activity,
                    )
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
                trans = fts3.new_transfer(
                    sourceSURL,
                    targetSURL,
                    checksum=f"ADLER32:{ftsFile.checksum.lower()}",
                    filesize=ftsFile.size,
                    metadata=trans_metadata,
                    activity=self.activity,
                    source_token=srcToken,
                    destination_token=dstToken,
                )

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
            "useTokens": useTokens,  # Store the information here to propagate it to submission
        }

        if self.activity:
            job_metadata["activity"] = self.activity

        dest_spacetoken = {DESTINATION_SPACETOKEN_ATTR: target_spacetoken}
        job = fts3.new_job(
            transfers=transfers,
            overwrite=True,
            source_spacetoken=source_spacetoken,
            bring_online=bring_online,
            copy_pin_lifetime=copy_pin_lifetime,
            retry=3,
            verify_checksum="target",  # Only check target vs specified, since we verify the source earlier
            multihop=isMultiHop,
            metadata=job_metadata,
            priority=self.priority,
            archive_timeout=archive_timeout,
            **dest_spacetoken,
        )

        return S_OK((job, fileIDsInTheJob))

    # def _constructRemovalJob(self, context, allLFNs, failedLFNs, target_spacetoken):
    #   """ Build a job for removal
    #
    #       Some attributes of the job are expected to be set
    #         * targetSE
    #         * activity (optional)
    #         * priority (optional)
    #         * filesToSubmit
    #         * operationID (optional, used as metadata for the job)
    #
    #
    #       :param context: fts3 context
    #       :param allLFNs: List of LFNs to remove
    #       :param failedLFNs: set of LFNs in filesToSubmit for which there was a problem
    #       :param target_spacetoken: the space token of the target
    #
    #       :return: S_OK( (job object, list of ftsFileIDs in the job))
    #   """
    #
    #   log = gLogger.getSubLogger(
    #       "constructRemovalJob/%s/%s" %
    #       (self.operationID, self.targetSE), True)
    #
    #   transfers = []
    #   fileIDsInTheJob = []
    #   for ftsFile in self.filesToSubmit:
    #
    #     if ftsFile.lfn in failedLFNs:
    #       log.debug("Not preparing transfer for file %s" % ftsFile.lfn)
    #       continue
    #
    #     transfers.append({'surl': allTargetSURLs[ftsFile.lfn],
    #                       'metadata': getattr(ftsFile, 'fileID')})
    #     fileIDsInTheJob.append(getattr(ftsFile, 'fileID'))
    #
    #   # We add a few metadata to the fts job so that we can reuse them later on without
    #   # querying our DB.
    #   # source and target SE are just used for accounting purpose
    #   job_metadata = {
    #       'operationID': self.operationID,
    #       'sourceSE': self.sourceSE,
    #       'targetSE': self.targetSE}
    #
    #   job = fts3.new_delete_job(transfers,
    #                             spacetoken=target_spacetoken,
    #                             metadata=job_metadata)
    #   job['params']['retry'] = 3
    #   job['params']['priority'] = self.priority
    #
    #   return S_OK((job, fileIDsInTheJob))

    def _constructStagingJob(self, pinTime, allLFNs, target_spacetoken):
        """Build a job for staging

        Some attributes of the job are expected to be set
          * targetSE
          * activity (optional)
          * priority (optional)
          * filesToSubmit
          * operationID (optional, used as metadata for the job)

        :param pinTime: pining time in case staging is needed
        :param allLFNs: List of LFNs to stage
        :param failedLFNs: set of LFNs in filesToSubmit for which there was a problem
        :param target_spacetoken: the space token of the target

        :return: S_OK( (job object, list of ftsFileIDs in the job))
        """

        log = gLogger.getSubLogger(f"constructStagingJob/{self.operationID}/{self.targetSE}")

        transfers = []
        fileIDsInTheJob = set()

        # Set of LFNs for which we did not get an SRM URL
        failedLFNs = set()

        # getting all the target surls
        res = StorageElement(self.targetSE, vo=self.vo).getURL(allLFNs, protocol="srm")
        if not res["OK"]:
            return res

        for lfn, reason in res["Value"]["Failed"].items():
            failedLFNs.add(lfn)
            log.error("Could not get target SURL", f"{lfn} {reason}")

        allTargetSURLs = res["Value"]["Successful"]

        for ftsFile in self.filesToSubmit:
            if ftsFile.lfn in failedLFNs:
                log.debug(f"Not preparing transfer for file {ftsFile.lfn}")
                continue

            sourceSURL = targetSURL = allTargetSURLs[ftsFile.lfn]
            ftsFileID = getattr(ftsFile, "fileID")
            trans_metadata = {"desc": f"Stage {ftsFileID}", "fileID": ftsFileID}
            trans = fts3.new_transfer(
                sourceSURL,
                targetSURL,
                checksum=f"ADLER32:{ftsFile.checksum}",
                filesize=ftsFile.size,
                metadata=trans_metadata,
                activity=self.activity,
            )

            transfers.append(trans)
            fileIDsInTheJob.add(ftsFileID)

        # If the source is not an tape SE, we should set the
        # copy_pin_lifetime and bring_online params to None,
        # otherwise they will do an extra useless queue in FTS
        sourceIsTape = self.__isTapeSE(self.sourceSE, self.vo)
        copy_pin_lifetime = pinTime if sourceIsTape else None
        bring_online = 86400 if sourceIsTape else None

        # We add a few metadata to the fts job so that we can reuse them later on without
        # querying our DB.
        # source and target SE are just used for accounting purpose
        job_metadata = {"operationID": self.operationID, "sourceSE": self.sourceSE, "targetSE": self.targetSE}

        if self.activity:
            job_metadata["activity"] = self.activity

        dest_spacetoken = {DESTINATION_SPACETOKEN_ATTR: target_spacetoken}

        job = fts3.new_job(
            transfers=transfers,
            overwrite=True,
            source_spacetoken=target_spacetoken,
            bring_online=bring_online,
            copy_pin_lifetime=copy_pin_lifetime,
            retry=3,
            metadata=job_metadata,
            priority=self.priority,
            unmanaged_tokens=True,
            **dest_spacetoken,
        )

        return S_OK((job, fileIDsInTheJob))

    def submit(self, context=None, ftsServer=None, ucert=None, pinTime=36000, protocols=None, fts_access_token=None):
        """submit the job to the FTS server

        Some attributes are expected to be defined for the submission to work:
          * type (set by FTS3Operation)
          * sourceSE (only for Transfer jobs)
          * targetSE
          * activity (optional)
          * priority (optional)
          * username
          * userGroup
          * filesToSubmit
          * operationID (optional, used as metadata for the job)

        We also expect the FTSFiles have an ID defined, as it is given as transfer metadata

        :param pinTime: Time the file should be pinned on disk (used for transfers and staging)
                        Used only if he source SE is a tape storage
        :param context: fts3 context. If not given, it is created (see ftsServer & ucert param)
        :param ftsServer: the address of the fts server to submit to. Used only if context is
                          not given. if not given either, use the ftsServer object attribute

        :param ucert: path to the user certificate/proxy. Might be inferred by the fts cli (see its doc)
        :param protocols: list of protocols from which we should choose the protocol to use
        :param fts_access_token: token to be used to talk to FTS and to be passed when creating a context

        :returns: S_OK([FTSFiles ids of files submitted])
        """

        log = gLogger.getLocalSubLogger(f"submit/{self.operationID}/{self.sourceSE}_{self.targetSE}")

        # Construct the target SURL
        res = self.__fetchSpaceToken(self.targetSE, self.vo)
        if not res["OK"]:
            return res
        target_spacetoken = res["Value"]

        allLFNs = [ftsFile.lfn for ftsFile in self.filesToSubmit]

        if self.type == "Transfer":
            res = self._constructTransferJob(
                pinTime, allLFNs, target_spacetoken, protocols=protocols, tokensEnabled=bool(fts_access_token)
            )

        elif self.type == "Staging":
            res = self._constructStagingJob(pinTime, allLFNs, target_spacetoken)
        # elif self.type == 'Removal':
        #   res = self._constructRemovalJob(context, allLFNs, failedLFNs, target_spacetoken)

        if not res["OK"]:
            return res

        job, fileIDsInTheJob = res["Value"]

        # If we need a token, don't use the context given in parameter
        # because the one given in parameter is only with X509 creds
        if job["params"].get("job_metadata", {}).get("useTokens"):
            if not fts_access_token:
                return S_ERROR("Job needs token support but no FTS token was supplied")
            context = None

        if not context:
            if not ftsServer:
                ftsServer = self.ftsServer
            res = self.generateContext(ftsServer, ucert, fts_access_token)
            if not res["OK"]:
                return res
            context = res["Value"]

        try:
            self.ftsGUID = fts3.submit(context, job)
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
                if ftsFile.fileID in fileIDsInTheJob:
                    ftsFile.status = "Submitted"

            now = datetime.datetime.utcnow().replace(microsecond=0)
            self.submitTime = now
            self.lastUpdate = now
            self.lastMonitor = now

        except FTS3ClientException as e:
            log.exception("Error at submission", repr(e))
            return S_ERROR(f"Error at submission: {e}")

        return S_OK(fileIDsInTheJob)

    @staticmethod
    def generateContext(ftsServer, ucert, fts_access_token=None, lifetime=25200):
        """This method generates an fts3 context

        Only a certificate or an fts token can be given

        :param ftsServer: address of the fts3 server
        :param ucert: the path to the certificate to be used
        :param fts_access_token: token to access FTS
        :param lifetime: duration (in sec) of the delegation to the FTS3 server
                        (default is 7h, like FTS3 default)

        :returns: an fts3 context
        """
        if fts_access_token and ucert:
            return S_ERROR("fts_access_token and ucert cannot be both set")

        try:
            context = fts3.Context(
                endpoint=ftsServer,
                ucert=ucert,
                request_class=ftsSSLRequest,
                verify=False,
                fts_access_token=fts_access_token,
            )

            # The delegation only makes sense for X509 auth
            if ucert:
                # Explicitely delegate to be sure we have the lifetime we want
                # Note: the delegation will re-happen only when the FTS server
                # decides that there is not enough timeleft.
                # At the moment, this is 1 hour, which effectively means that if you do
                # not submit a job for more than 1h, you have no valid proxy in FTS servers
                # anymore, and all the jobs failed. So we force it when
                # one third of the lifetime will be left.
                # Also, the proxy given as parameter might have less than "lifetime" left
                # since it is cached, but it does not matter, because in the FTS3Agent
                # we make sure that we renew it often enough
                td_lifetime = datetime.timedelta(seconds=lifetime)
                fts3.delegate(context, lifetime=td_lifetime, delegate_when_lifetime_lt=td_lifetime // 3)

            return S_OK(context)
        except FTS3ClientException as e:
            gLogger.exception("Error generating context", repr(e))
            return S_ERROR(repr(e))

    def _fillAccountingDicts(self, jobStatusDict):
        """This methods generates the necessary information to create DataOperation
        accounting records, and stores them as a instance attribute.

        For it to be relevant, it should be called only when the job is in a final state.

        :param jobStatusDict: output of fts3.get_job_status

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
