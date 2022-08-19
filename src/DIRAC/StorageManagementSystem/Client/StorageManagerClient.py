""" Class that contains client access to the StorageManagerDB handler.
"""
import random
import errno

from DIRAC import S_OK, S_ERROR, gLogger
from DIRAC.ConfigurationSystem.Client.Helpers.Registry import getVOForGroup
from DIRAC.Core.Base.Client import Client, createClient
from DIRAC.Core.Utilities.DErrno import cmpError
from DIRAC.Core.Utilities.Proxy import UserProxy
from DIRAC.DataManagementSystem.Client.DataManager import DataManager
from DIRAC.DataManagementSystem.Utilities.DMSHelpers import DMSHelpers
from DIRAC.Resources.Storage.StorageElement import StorageElement


def getFilesToStage(lfnList, jobState=None, checkOnlyTapeSEs=None, jobLog=None):
    """Utility that returns out of a list of LFNs those files that are offline,
    and those for which at least one copy is online
    """
    if not lfnList:
        return S_OK({"onlineLFNs": [], "offlineLFNs": {}, "failedLFNs": [], "absentLFNs": {}})

    dm = DataManager()
    if isinstance(lfnList, str):
        lfnList = [lfnList]

    lfnListReplicas = dm.getReplicasForJobs(lfnList, getUrl=False)
    if not lfnListReplicas["OK"]:
        return lfnListReplicas

    offlineLFNsDict = {}
    onlineLFNs = {}
    offlineLFNs = {}
    absentLFNs = {}
    failedLFNs = set()
    if lfnListReplicas["Value"]["Failed"]:
        # Check if files are not existing
        for lfn, reason in lfnListReplicas["Value"]["Failed"].items():
            # FIXME: awful check until FC returns a proper error
            if cmpError(reason, errno.ENOENT) or "No such file" in reason:
                # The file doesn't exist, job must be Failed
                # FIXME: it is not possible to return here an S_ERROR(), return the message only
                absentLFNs[lfn] = S_ERROR(errno.ENOENT, "File not in FC")["Message"]
        if absentLFNs:
            return S_OK(
                {
                    "onlineLFNs": list(onlineLFNs),
                    "offlineLFNs": offlineLFNsDict,
                    "failedLFNs": list(failedLFNs),
                    "absentLFNs": absentLFNs,
                }
            )
        return S_ERROR("Failures in getting replicas")

    lfnListReplicas = lfnListReplicas["Value"]["Successful"]
    # If a file is reported here at a tape SE, it is not at a disk SE as we use disk in priority
    # We shall check all file anyway in order to make sure they exist
    seToLFNs = dict()
    for lfn, ses in lfnListReplicas.items():
        for se in ses:
            seToLFNs.setdefault(se, list()).append(lfn)

    if seToLFNs:
        if jobState:
            # Get user name and group from the job state
            userName = jobState.getAttribute("Owner")
            if not userName["OK"]:
                return userName
            userName = userName["Value"]

            userGroup = jobState.getAttribute("OwnerGroup")
            if not userGroup["OK"]:
                return userGroup
            userGroup = userGroup["Value"]
        else:
            userName = None
            userGroup = None
        # Check whether files are Online or Offline, or missing at SE
        result = _checkFilesToStage(
            seToLFNs,
            onlineLFNs,
            offlineLFNs,
            absentLFNs,  # pylint: disable=unexpected-keyword-arg
            checkOnlyTapeSEs=checkOnlyTapeSEs,
            jobLog=jobLog,
            proxyUserName=userName,
            proxyUserGroup=userGroup,
            executionLock=True,
        )

        if not result["OK"]:
            return result
        failedLFNs = set(lfnList) - set(onlineLFNs) - set(offlineLFNs) - set(absentLFNs)

        # Get the online SEs
        dmsHelper = DMSHelpers()
        onlineSEs = {se for ses in onlineLFNs.values() for se in ses}
        onlineSites = {dmsHelper.getLocalSiteForSE(se).get("Value") for se in onlineSEs} - {None}
        for lfn in offlineLFNs:
            ses = offlineLFNs[lfn]
            if len(ses) == 1:
                # No choice, let's go
                offlineLFNsDict.setdefault(ses[0], list()).append(lfn)
                continue
            # Try and get an SE at a site already with online files
            found = False
            if onlineSites:
                # If there is at least one online site, select one
                for se in ses:
                    site = dmsHelper.getLocalSiteForSE(se)
                    if site["OK"]:
                        if site["Value"] in onlineSites:
                            offlineLFNsDict.setdefault(se, list()).append(lfn)
                            found = True
                            break
            # No online site found in common, select randomly
            if not found:
                offlineLFNsDict.setdefault(random.choice(ses), list()).append(lfn)

    return S_OK(
        {
            "onlineLFNs": list(onlineLFNs),
            "offlineLFNs": offlineLFNsDict,
            "failedLFNs": list(failedLFNs),
            "absentLFNs": absentLFNs,
            "onlineSites": onlineSites,
        }
    )


def _checkFilesToStage(
    seToLFNs,
    onlineLFNs,
    offlineLFNs,
    absentLFNs,
    checkOnlyTapeSEs=None,
    jobLog=None,
    proxyUserName=None,
    proxyUserGroup=None,
    executionLock=None,
):
    """
    Checks on SEs whether the file is NEARLINE or ONLINE
    onlineLFNs, offlineLFNs and absentLFNs are modified to contain the files found online
    If checkOnlyTapeSEs is True, disk replicas are not checked
    As soon as a replica is found Online for a file, no further check is made
    """
    # Only check on storage if it is a tape SE
    if jobLog is None:
        logger = gLogger
    else:
        logger = jobLog
    if checkOnlyTapeSEs is None:
        # Default value is True
        checkOnlyTapeSEs = True

    failed = {}
    for se, lfnsInSEList in seToLFNs.items():
        # If we have found already all files online at another SE, no need to check the others
        # but still we want to set the SE as Online if not a TapeSE
        vo = getVOForGroup(proxyUserGroup)
        seObj = StorageElement(se, vo=vo)
        status = seObj.getStatus()
        if not status["OK"]:
            return status
        tapeSE = status["Value"]["TapeSE"]
        diskSE = status["Value"]["DiskSE"]
        # If requested to check only Tape SEs and the file is at a diskSE, we guess it is Online...
        filesToCheck = []
        for lfn in lfnsInSEList:
            # If the file had already been found accessible at an SE, only check that this one is on disk
            diskIsOK = checkOnlyTapeSEs or (lfn in onlineLFNs)
            if diskIsOK and diskSE:
                onlineLFNs.setdefault(lfn, []).append(se)
            elif not diskIsOK or (tapeSE and (lfn not in onlineLFNs)):
                filesToCheck.append(lfn)
        if not filesToCheck:
            continue

        # We have to use a new SE object because it caches the proxy!
        with UserProxy(
            proxyUserName=proxyUserName, proxyUserGroup=proxyUserGroup, executionLock=executionLock
        ) as proxyResult:
            if proxyResult["OK"]:
                fileMetadata = StorageElement(se, vo=vo).getFileMetadata(filesToCheck)
            else:
                fileMetadata = proxyResult

        if not fileMetadata["OK"]:
            failed[se] = dict.fromkeys(filesToCheck, fileMetadata["Message"])
        else:
            if fileMetadata["Value"]["Failed"]:
                failed[se] = fileMetadata["Value"]["Failed"]
            # is there at least one replica online?
            for lfn, mDict in fileMetadata["Value"]["Successful"].items():
                # SRM returns Cached, but others may only return Accessible
                if mDict.get("Cached", mDict["Accessible"]):
                    onlineLFNs.setdefault(lfn, []).append(se)
                elif tapeSE:
                    # A file can be staged only at Tape SE
                    offlineLFNs.setdefault(lfn, []).append(se)
                else:
                    # File not available at a diskSE... we shall retry later
                    pass

    # Doesn't matter if some files are Offline if they are also online
    for lfn in set(offlineLFNs) & set(onlineLFNs):
        offlineLFNs.pop(lfn)

    # If the file was found staged, ignore possible errors, but print out errors
    for se, failedLfns in list(failed.items()):
        logger.error("Errors when getting files metadata", "at %s" % se)
        for lfn, reason in list(failedLfns.items()):
            if lfn in onlineLFNs:
                logger.warn(reason, "for %s, but there is an online replica" % lfn)
                failed[se].pop(lfn)
            else:
                logger.error(reason, "for %s, no online replicas" % lfn)
                if cmpError(reason, errno.ENOENT):
                    absentLFNs.setdefault(lfn, []).append(se)
                    failed[se].pop(lfn)
        if not failed[se]:
            failed.pop(se)
    # Find the files that do not exist at SE
    if failed:
        logger.error(
            "Error getting metadata", "for %d files" % len({lfn for lfnList in failed.values() for lfn in lfnList})
        )

    for lfn in absentLFNs:
        seList = absentLFNs[lfn]
        # FIXME: it is not possible to return here an S_ERROR(), return the message only
        absentLFNs[lfn] = S_ERROR(errno.ENOENT, "File not at %s" % ",".join(sorted(seList)))["Message"]
    # Format the error for absent files
    return S_OK()


@createClient("StorageManagement/StorageManager")
class StorageManagerClient(Client):
    """This is the client to the StorageManager service, so even if it is not seen, it exposes all its RPC calls"""

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.setServer("StorageManagement/StorageManager")
