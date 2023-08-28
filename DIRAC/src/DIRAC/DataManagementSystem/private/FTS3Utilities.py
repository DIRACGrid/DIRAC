""" Some utilities for FTS3...
"""
import random
import threading

from DIRAC.ConfigurationSystem.Client.Helpers.Operations import Operations as opHelper
from DIRAC.DataManagementSystem.Client.DataManager import DataManager
from DIRAC.FrameworkSystem.Client.Logger import gLogger
from DIRAC.Core.Utilities.ObjectLoader import ObjectLoader
from DIRAC.Core.Utilities.ReturnValues import S_OK, S_ERROR
from DIRAC.ResourceStatusSystem.Client.ResourceStatus import ResourceStatus


def _checkSourceReplicas(ftsFiles, preferDisk=True):
    """Check the active replicas

    :params ftsFiles: list of FT3Files
    :param preferDisk: (default True) prefer disk replicas if available
                        (see :py:meth:`DIRAC.DataManagementSystem.Client.DataManager.DataManager.getActiveReplicas`)

    :returns: Successful/Failed {lfn : { SE1 : PFN1, SE2 : PFN2 } , ... }
    """

    lfns = list({f.lfn for f in ftsFiles})
    res = DataManager().getActiveReplicas(lfns, getUrl=False, preferDisk=preferDisk)

    return res


def selectUniqueSource(ftsFiles, fts3Plugin, allowedSources=None):
    """
    For a list of FTS3files object, select a random source, and group the files by source.

    We also return the FTS3Files for which we had problems getting replicas

    :param ftsFiles: list of FTS3File object
    :param fts3Plugin: plugin instance to use to chose between sources
    :param allowedSources: list of allowed sources


    :return:  S_OK(({ sourceSE: [ FTS3Files] }, {FTS3File: errors}))

    """

    _log = gLogger.getSubLogger("selectUniqueSource")

    # destGroup will contain for each target SE a dict { source : [list of FTS3Files] }
    groupBySource = {}

    # For all files, check which possible sources they have
    # If we specify allowedSources, don't restrict the choice to disk replicas
    res = _checkSourceReplicas(ftsFiles, preferDisk=not allowedSources)
    if not res["OK"]:
        return res

    filteredReplicas = res["Value"]

    # LFNs for which we failed to get replicas
    failedFiles = {}

    for ftsFile in ftsFiles:
        # If we failed to get the replicas, add the FTS3File to the dictionary
        if ftsFile.lfn in filteredReplicas["Failed"]:
            errMsg = filteredReplicas["Failed"][ftsFile.lfn]
            failedFiles[ftsFile] = errMsg
            _log.debug("Failed to get active replicas", f"{ftsFile.lfn},{errMsg}")
            continue

        replicaDict = filteredReplicas["Successful"][ftsFile.lfn]

        try:
            uniqueSource = fts3Plugin.selectSourceSE(ftsFile, replicaDict, allowedSources)
            groupBySource.setdefault(uniqueSource, []).append(ftsFile)
        except ValueError as e:
            _log.info("No allowed replica source for file", f"{ftsFile.lfn}: {repr(e)}")
            continue

    return S_OK((groupBySource, failedFiles))


def groupFilesByTarget(ftsFiles):
    """
    For a list of FTS3files object, group the Files by target

    :param ftsFiles: list of FTS3File object
    :return: {targetSE : [ ftsFiles] } }

    """

    # destGroup will contain for each target SE a dict { possible source : transfer metadata }
    destGroup = {}

    for ftsFile in ftsFiles:
        destGroup.setdefault(ftsFile.targetSE, []).append(ftsFile)

    return S_OK(destGroup)


def getFTS3Plugin(vo=None):
    """
    Return an instance of the FTS3Plugin configured in the CS

    :param vo: vo config to look for
    """
    pluginName = opHelper(vo=vo).getValue("DataManagement/FTSPlacement/FTS3/FTS3Plugin", "Default")

    objLoader = ObjectLoader()
    _class = objLoader.loadObject(
        f"DataManagementSystem.private.FTS3Plugins.{pluginName}FTS3Plugin", f"{pluginName}FTS3Plugin"
    )

    if not _class["OK"]:
        raise Exception(_class["Message"])

    fts3Plugin = _class["Value"](vo=vo)
    return fts3Plugin


threadLocal = threading.local()


class FTS3ServerPolicy:
    """
    This class manages the policy for choosing a server
    """

    def __init__(self, serverDict, serverPolicy="Random"):
        """
        Call the init of the parent, and initialize the list of FTS3 servers
        """

        self.log = gLogger.getSubLogger(self.__class__.__name__)

        self._serverDict = serverDict
        self._serverList = list(serverDict)
        self._maxAttempts = len(self._serverList)
        self._nextServerID = 0
        self._resourceStatus = ResourceStatus()

        methName = f"_{serverPolicy.lower()}ServerPolicy"
        if not hasattr(self, methName):
            self.log.error(f"Unknown server policy {serverPolicy}. Using Random instead")
            methName = "_randomServerPolicy"

        self._policyMethod = getattr(self, methName)

    def _failoverServerPolicy(self, _attempt):
        """
        Returns always the server at a given position (normally the first one)

        :param attempt: position of the server in the list
        """
        if _attempt >= len(self._serverList):
            raise Exception("FTS3ServerPolicy.__failoverServerPolicy: attempt to reach non existing server index")
        return self._serverList[_attempt]

    def _sequenceServerPolicy(self, _attempt):
        """
        Every time the this policy is called, return the next server on the list
        """

        fts3server = self._serverList[self._nextServerID]
        self._nextServerID = (self._nextServerID + 1) % len(self._serverList)
        return fts3server

    def _randomServerPolicy(self, _attempt):
        """
        return a server from shuffledServerList
        """

        if getattr(threadLocal, "shuffledServerList", None) is None:
            threadLocal.shuffledServerList = self._serverList[:]
            random.shuffle(threadLocal.shuffledServerList)

        fts3Server = threadLocal.shuffledServerList[_attempt]

        if _attempt == self._maxAttempts - 1:
            random.shuffle(threadLocal.shuffledServerList)

        return fts3Server

    def _getFTSServerStatus(self, ftsServer):
        """Fetch the status of the FTS server from RSS"""

        res = self._resourceStatus.getElementStatus(ftsServer, "FTS")
        if not res["OK"]:
            return res

        result = res["Value"]
        if ftsServer not in result:
            return S_ERROR(f"No FTS Server {ftsServer} known to RSS")

        if result[ftsServer]["all"] == "Active":
            return S_OK(True)

        return S_OK(False)

    def chooseFTS3Server(self):
        """
        Choose the appropriate FTS3 server depending on the policy
        """

        fts3Server = None
        attempt = 0

        while not fts3Server and attempt < self._maxAttempts:
            fts3Server = self._policyMethod(attempt)
            res = self._getFTSServerStatus(fts3Server)

            if not res["OK"]:
                self.log.warn(f"Error getting the RSS status for {fts3Server}: {res}")
                fts3Server = None
                attempt += 1
                continue

            ftsServerStatus = res["Value"]

            if not ftsServerStatus:
                self.log.warn(f"FTS server {fts3Server} is not in good shape. Choose another one")
                fts3Server = None
                attempt += 1

        if fts3Server:
            return S_OK(self._serverDict[fts3Server])

        return S_ERROR("Could not find an FTS3 server (max attempt reached)")
