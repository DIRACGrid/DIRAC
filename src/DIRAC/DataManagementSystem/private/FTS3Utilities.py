""" Some utilities for FTS3...
"""
import threading

from DIRAC.ConfigurationSystem.Client.ConfigurationData import gConfigurationData
from DIRAC.ConfigurationSystem.Client.Helpers.Operations import Operations as opHelper
from DIRAC.DataManagementSystem.Client.DataManager import DataManager
from DIRAC.FrameworkSystem.Client.Logger import gLogger
from DIRAC.Core.Utilities.ObjectLoader import ObjectLoader
from DIRAC.Core.Utilities.ReturnValues import S_OK


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


# Cache of the FTS3Plugin instances, shared per VO.
# { (vo, pluginName): (csVersion, pluginInstance) }
_fts3PluginCache = {}
_fts3PluginCacheLock = threading.Lock()


def getFTS3Plugin(vo=None):
    """
    Return the instance of the FTS3Plugin configured in the CS.

    The instance is shared per VO, and re-created only if the configuration
    has been refreshed since it was instantiated
    (like :py:class:`~DIRAC.DataManagementSystem.Utilities.DMSHelpers.DMSHelpers`).
    Hence, plugins must be thread safe and should not keep per operation state.

    :param vo: vo config to look for
    """
    pluginName = opHelper(vo=vo).getValue("DataManagement/FTSPlacement/FTS3/FTS3Plugin", "Default")
    cacheKey = (vo, pluginName)
    currentVersion = gConfigurationData.getVersion()

    with _fts3PluginCacheLock:
        cachedVersion, fts3Plugin = _fts3PluginCache.get(cacheKey, (None, None))
        if fts3Plugin is not None and cachedVersion == currentVersion:
            return fts3Plugin

        objLoader = ObjectLoader()
        _class = objLoader.loadObject(
            f"DataManagementSystem.private.FTS3Plugins.{pluginName}FTS3Plugin", f"{pluginName}FTS3Plugin"
        )

        if not _class["OK"]:
            raise Exception(_class["Message"])

        fts3Plugin = _class["Value"](vo=vo)
        _fts3PluginCache[cacheKey] = (currentVersion, fts3Plugin)
        return fts3Plugin
