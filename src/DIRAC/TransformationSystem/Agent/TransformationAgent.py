"""  TransformationAgent processes transformations found in the transformation database.

The following options can be set for the TransformationAgent.

.. literalinclude:: ../ConfigTemplate.cfg
  :start-after: ##BEGIN TransformationAgent
  :end-before: ##END
  :dedent: 2
  :caption: TransformationAgent options
"""
import time
import os
import datetime
import pickle
import concurrent.futures

from DIRAC import S_OK, S_ERROR
from DIRAC.ConfigurationSystem.Client.Helpers.Operations import Operations
from DIRAC.Core.Base.AgentModule import AgentModule
from DIRAC.Core.Utilities.ThreadSafe import Synchronizer
from DIRAC.Core.Utilities.List import breakListIntoChunks, randomize
from DIRAC.DataManagementSystem.Client.DataManager import DataManager
from DIRAC.TransformationSystem.Client import TransformationFilesStatus
from DIRAC.TransformationSystem.Client.TransformationClient import TransformationClient
from DIRAC.TransformationSystem.Agent.TransformationAgentsUtilities import TransformationAgentsUtilities

AGENT_NAME = "Transformation/TransformationAgent"
gSynchro = Synchronizer()


class TransformationAgent(AgentModule, TransformationAgentsUtilities):
    """Usually subclass of AgentModule"""

    def __init__(self, *args, **kwargs):
        """c'tor"""
        AgentModule.__init__(self, *args, **kwargs)
        TransformationAgentsUtilities.__init__(self)

        # few parameters
        self.pluginLocation = ""
        self.transformationStatus = []
        self.maxFiles = 0
        self.transformationTypes = []

        # clients (out of the threads)
        self.transfClient = None

        # parameters for caching
        self.workDirectory = ""
        self.cacheFile = ""
        self.controlDirectory = ""

        self.lastFileOffset = {}
        # Validity of the cache
        self.replicaCache = None
        self.replicaCacheValidity = None
        self.writingCache = False
        self.removedFromCache = 0

        self.noUnusedDelay = 0
        self.unusedFiles = {}
        self.unusedTimeStamp = {}

        self.debug = False
        self.pluginTimeout = {}

    def initialize(self):
        """standard initialize"""
        # few parameters
        self.pluginLocation = self.am_getOption(
            "PluginLocation", "DIRAC.TransformationSystem.Agent.TransformationPlugin"
        )
        self.transformationStatus = self.am_getOption("transformationStatus", ["Active", "Completing", "Flush"])
        # Prepare to change the name of the CS option as MaxFiles is ambiguous
        self.maxFiles = self.am_getOption("MaxFilesToProcess", self.am_getOption("MaxFiles", 5000))

        agentTSTypes = self.am_getOption("TransformationTypes", [])
        if agentTSTypes:
            self.transformationTypes = sorted(agentTSTypes)
        else:
            dataProc = Operations().getValue("Transformations/DataProcessing", ["MCSimulation", "Merge"])
            dataManip = Operations().getValue("Transformations/DataManipulation", ["Replication", "Removal"])
            self.transformationTypes = sorted(dataProc + dataManip)

        # clients
        self.transfClient = TransformationClient()

        # for caching using a pickle file
        self.workDirectory = self.am_getWorkDirectory()
        self.cacheFile = os.path.join(self.workDirectory, "ReplicaCache.pkl")
        self.controlDirectory = self.am_getControlDirectory()

        # remember the offset if any in TS
        self.lastFileOffset = {}

        # Validity of the cache
        self.replicaCache = {}
        self.replicaCacheValidity = self.am_getOption("ReplicaCacheValidity", 2)

        self.noUnusedDelay = self.am_getOption("NoUnusedDelay", 6)

        # Instantiating the ThreadPoolExecutor
        maxNumberOfThreads = self.am_getOption("maxThreadsInPool", 15)
        self.log.info("Multithreaded with %d threads" % maxNumberOfThreads)
        self.threadPoolExecutor = concurrent.futures.ThreadPoolExecutor(max_workers=maxNumberOfThreads)

        self.log.info("Will treat the following transformation types: %s" % str(self.transformationTypes))

        return S_OK()

    def finalize(self):
        """graceful finalization"""

        method = "finalize"
        self._logInfo("Wait for threads to get empty before terminating the agent", method=method)
        self.threadPoolExecutor.shutdown()
        self._logInfo("Threads are empty, terminating the agent...", method=method)
        self.__writeCache()
        return S_OK()

    def execute(self):
        """Just puts transformations in the queue, and spawns threads if there's work to do."""
        # Get the transformations to process
        res = self.getTransformations()
        if not res["OK"]:
            self._logError("Failed to obtain transformations:", res["Message"])
            return S_OK()
        # Process the transformations
        count = 0
        future_to_transID = {}

        for transDict in res["Value"]:
            transID = int(transDict["TransformationID"])
            if transDict.get("InheritedFrom"):
                # Try and move datasets from the ancestor production
                res = self.transfClient.moveFilesToDerivedTransformation(transDict)
                if not res["OK"]:
                    self._logError(
                        "Error moving files from an inherited transformation", res["Message"], transID=transID
                    )
                else:
                    parentProd, movedFiles = res["Value"]
                    if movedFiles:
                        self._logInfo(
                            "Successfully moved files from %d to %d:" % (parentProd, transID), transID=transID
                        )
                        for status, val in movedFiles.items():
                            self._logInfo("\t%d files to status %s" % (val, status), transID=transID)
            count += 1
            future = self.threadPoolExecutor.submit(self._execute, transDict)
            future_to_transID[future] = transID
        self._logInfo("Out of %d transformations, %d put in thread queue" % (len(res["Value"]), count))

        for future in concurrent.futures.as_completed(future_to_transID):
            transID = future_to_transID[future]
            try:
                future.result()
            except Exception as exc:
                self._logError("%d generated an exception: %s" % (transID, exc))
            else:
                self._logInfo("Processed %d" % transID)

        return S_OK()

    def getTransformations(self):
        """Obtain the transformations to be executed - this is executed at the start of every loop (it's really the
        only real thing in the execute()
        """
        transName = self.am_getOption("Transformation", "All")
        method = "getTransformations"
        if transName == "All":
            self._logInfo(
                "Getting all transformations%s, status %s."
                % (
                    " of type %s" % str(self.transformationTypes) if self.transformationTypes else "",
                    str(self.transformationStatus),
                ),
                method=method,
            )
            transfDict = {"Status": self.transformationStatus}
            if self.transformationTypes:
                transfDict["Type"] = self.transformationTypes
            res = self.transfClient.getTransformations(transfDict, extraParams=True)
            if not res["OK"]:
                return res
            transformations = res["Value"]
            self._logInfo("Obtained %d transformations to process" % len(transformations), method=method)
        else:
            self._logInfo("Getting transformation %s." % transName, method=method)
            res = self.transfClient.getTransformation(transName, extraParams=True)
            if not res["OK"]:
                self._logError("Failed to get transformation:", res["Message"], method=method)
                return res
            transformations = [res["Value"]]
        return S_OK(transformations)

    def _getClients(self):
        """returns the clients used in the threads"""
        threadTransformationClient = TransformationClient()
        threadDataManager = DataManager()

        return {"TransformationClient": threadTransformationClient, "DataManager": threadDataManager}

    def _execute(self, transDict):
        """thread - does the real job: processing the transformation to be processed"""

        self._logDebug("Starting _execute")

        # Each thread will have its own clients
        clients = self._getClients()

        try:
            transID = int(transDict["TransformationID"])
            self._logInfo("Processing transformation %s." % transID, transID=transID)
            startTime = time.time()
            res = self.processTransformation(transDict, clients)
            if not res["OK"]:
                self._logInfo("Failed to process transformation:", res["Message"], transID=transID)
        except Exception as x:  # pylint: disable=broad-except
            self._logException("Exception in plugin", lException=x, transID=transID)
        finally:
            if not transID:
                transID = "None"
            self._logInfo("Processed transformation in %.1f seconds" % (time.time() - startTime), transID=transID)

        self._logDebug("Exiting _execute")

    def processTransformation(self, transDict, clients):
        """process a single transformation (in transDict)"""
        method = "processTransformation"
        transID = transDict["TransformationID"]
        forJobs = transDict["Type"].lower() not in ("replication", "removal")

        # First get the LFNs associated to the transformation
        transFiles = self._getTransformationFiles(transDict, clients, replicateOrRemove=not forJobs)
        if not transFiles["OK"]:
            return transFiles
        if not transFiles["Value"]:
            return S_OK()

        if transID not in self.replicaCache:
            self.__readCache(transID)
        transFiles = transFiles["Value"]
        unusedLfns = [f["LFN"] for f in transFiles]
        unusedFiles = len(unusedLfns)

        plugin = transDict.get("Plugin", "Standard")
        # Limit the number of LFNs to be considered for replication or removal as they are treated individually
        if not forJobs:
            maxFiles = Operations().getValue("TransformationPlugins/%s/MaxFilesToProcess" % plugin, 0)
            # Get plugin-specific limit in number of files (0 means no limit)
            totLfns = len(unusedLfns)
            lfnsToProcess = self.__applyReduction(unusedLfns, maxFiles=maxFiles)
            if len(lfnsToProcess) != totLfns:
                self._logInfo(
                    "Reduced number of files from %d to %d" % (totLfns, len(lfnsToProcess)),
                    method=method,
                    transID=transID,
                )
                transFiles = [f for f in transFiles if f["LFN"] in lfnsToProcess]
        else:
            lfnsToProcess = unusedLfns

        # Check the data is available with replicas
        res = self.__getDataReplicas(transDict, lfnsToProcess, clients, forJobs=forJobs)
        if not res["OK"]:
            self._logError("Failed to get data replicas:", res["Message"], method=method, transID=transID)
            return res
        dataReplicas = res["Value"]

        # Get the plug-in type and create the plug-in object
        self._logInfo("Processing transformation with '%s' plug-in." % plugin, method=method, transID=transID)
        res = self.__generatePluginObject(plugin, clients)
        if not res["OK"]:
            return res
        oPlugin = res["Value"]

        # Get the plug-in and set the required params
        oPlugin.setParameters(transDict)
        oPlugin.setInputData(dataReplicas)
        oPlugin.setTransformationFiles(transFiles)
        res = oPlugin.run()
        if not res["OK"]:
            self._logError(
                "Failed to generate tasks for transformation:", res["Message"], method=method, transID=transID
            )
            return res
        tasks = res["Value"]
        self.pluginTimeout[transID] = res.get("Timeout", False)
        # Create the tasks
        allCreated = True
        created = 0
        lfnsInTasks = []
        for se, lfns in tasks:
            res = clients["TransformationClient"].addTaskForTransformation(transID, lfns, se)
            if not res["OK"]:
                self._logError(
                    "Failed to add task generated by plug-in:", res["Message"], method=method, transID=transID
                )
                allCreated = False
            else:
                created += 1
                lfnsInTasks += [lfn for lfn in lfns if lfn in lfnsToProcess]
        if created:
            self._logInfo("Successfully created %d tasks for transformation." % created, method=method, transID=transID)
        else:
            self._logInfo("No new tasks created for transformation.", method=method, transID=transID)
        self.unusedFiles[transID] = unusedFiles - len(lfnsInTasks)
        # If not all files were obtained, move the offset
        lastOffset = self.lastFileOffset.get(transID)
        if lastOffset:
            self.lastFileOffset[transID] = max(0, lastOffset - len(lfnsInTasks))
        self.__removeFilesFromCache(transID, lfnsInTasks)

        # If this production is to Flush
        if transDict["Status"] == "Flush" and allCreated:
            res = clients["TransformationClient"].setTransformationParameter(transID, "Status", "Active")
            if not res["OK"]:
                self._logError(
                    "Failed to update transformation status to 'Active':",
                    res["Message"],
                    method=method,
                    transID=transID,
                )
            else:
                self._logInfo("Updated transformation status to 'Active'.", method=method, transID=transID)
        return S_OK()

    ######################################################################
    #
    # Internal methods used by the agent
    #

    def _getTransformationFiles(self, transDict, clients, statusList=None, replicateOrRemove=False):
        """get the data replicas for a certain transID"""
        # By default, don't skip if no new Unused for DM transformations
        skipIfNoNewUnused = not replicateOrRemove
        transID = transDict["TransformationID"]
        plugin = transDict.get("Plugin", "Standard")
        # Check if files should be sorted and limited in number
        operations = Operations()
        sortedBy = operations.getValue("TransformationPlugins/%s/SortedBy" % plugin, None)
        maxFiles = operations.getValue("TransformationPlugins/%s/MaxFilesToProcess" % plugin, 0)
        # If the NoUnuse delay is explicitly set, we want to take it into account, and skip if no new Unused
        if operations.getValue("TransformationPlugins/%s/NoUnusedDelay" % plugin, 0):
            skipIfNoNewUnused = True
        noUnusedDelay = (
            0
            if self.pluginTimeout.get(transID, False)
            else operations.getValue("TransformationPlugins/%s/NoUnusedDelay" % plugin, self.noUnusedDelay)
        )
        method = "_getTransformationFiles"
        lastOffset = self.lastFileOffset.setdefault(transID, 0)

        # Files that were problematic (either explicit or because SE was banned) may be recovered,
        # and always removing the missing ones
        if not statusList:
            statusList = [TransformationFilesStatus.UNUSED, TransformationFilesStatus.PROB_IN_FC]
        statusList += [TransformationFilesStatus.MISSING_IN_FC] if transDict["Type"] == "Removal" else []
        transClient = clients["TransformationClient"]
        res = transClient.getTransformationFiles(
            condDict={"TransformationID": transID, "Status": statusList},
            orderAttribute=sortedBy,
            offset=lastOffset,
            maxfiles=maxFiles,
        )
        if not res["OK"]:
            self._logError("Failed to obtain input data:", res["Message"], method=method, transID=transID)
            return res
        transFiles = res["Value"]
        if maxFiles and len(transFiles) == maxFiles:
            self.lastFileOffset[transID] += maxFiles
        else:
            del self.lastFileOffset[transID]

        if not transFiles:
            self._logInfo(
                "No '%s' files found for transformation." % ",".join(statusList), method=method, transID=transID
            )
            if transDict["Status"] == "Flush":
                res = transClient.setTransformationParameter(transID, "Status", "Active")
                if not res["OK"]:
                    self._logError(
                        "Failed to update transformation status to 'Active':",
                        res["Message"],
                        method=method,
                        transID=transID,
                    )
                else:
                    self._logInfo("Updated transformation status to 'Active'.", method=method, transID=transID)
            return S_OK()
        # Check if transformation is kicked
        kickFile = os.path.join(self.controlDirectory, "KickTransformation_%s" % str(transID))
        try:
            kickTrans = os.path.exists(kickFile)
            if kickTrans:
                os.remove(kickFile)
        except OSError:
            pass

        # Check if something new happened
        now = datetime.datetime.utcnow()
        if not kickTrans and skipIfNoNewUnused and noUnusedDelay:
            nextStamp = self.unusedTimeStamp.setdefault(transID, now) + datetime.timedelta(hours=noUnusedDelay)
            skip = now < nextStamp
            if len(transFiles) == self.unusedFiles.get(transID, 0) and transDict["Status"] != "Flush" and skip:
                self._logInfo(
                    "No new '%s' files found for transformation." % ",".join(statusList), method=method, transID=transID
                )
                return S_OK()

        self.unusedTimeStamp[transID] = now
        # If files are not Unused, set them Unused
        notUnused = [trFile["LFN"] for trFile in transFiles if trFile["Status"] != TransformationFilesStatus.UNUSED]
        otherStatuses = sorted({trFile["Status"] for trFile in transFiles} - {TransformationFilesStatus.UNUSED})
        if notUnused:
            res = transClient.setFileStatusForTransformation(
                transID, TransformationFilesStatus.UNUSED, notUnused, force=True
            )
            if not res["OK"]:
                self._logError(
                    "Error setting %d files Unused:" % len(notUnused), res["Message"], method=method, transID=transID
                )
            else:
                self._logInfo("Set %d files from %s to Unused" % (len(notUnused), ",".join(otherStatuses)))
                self.__removeFilesFromCache(transID, notUnused)
        return S_OK(transFiles)

    def __applyReduction(self, lfns, maxFiles=None):
        """eventually remove the number of files to be considered"""
        if maxFiles is None:
            maxFiles = self.maxFiles
        if not maxFiles or len(lfns) <= maxFiles:
            return lfns
        return randomize(lfns)[:maxFiles]

    def __getDataReplicas(self, transDict, lfns, clients, forJobs=True):
        """Get the replicas for the LFNs and check their statuses. It first looks within the cache."""
        method = "__getDataReplicas"
        transID = transDict["TransformationID"]
        if "RemoveFile" in transDict["Body"]:
            # When removing files, we don't care about their replicas
            return S_OK(dict.fromkeys(lfns, ["None"]))
        clearCacheFile = os.path.join(self.controlDirectory, "ClearCache_%s" % str(transID))
        try:
            clearCache = os.path.exists(clearCacheFile)
            if clearCache:
                os.remove(clearCacheFile)
        except Exception:
            pass
        if clearCache or transDict["Status"] == "Flush":
            self._logInfo("Replica cache cleared", method=method, transID=transID)
            # We may need to get new replicas
            self.__clearCacheForTrans(transID)
        else:
            # If the cache needs to be cleaned
            self.__cleanCache(transID)
        startTime = time.time()
        dataReplicas = {}
        nLfns = len(lfns)
        self._logVerbose("Getting replicas for %d files" % nLfns, method=method, transID=transID)
        cachedReplicaSets = self.replicaCache.get(transID, {})
        cachedReplicas = {}
        # Merge all sets of replicas
        for replicas in cachedReplicaSets.values():
            cachedReplicas.update(replicas)
        self._logInfo("Number of cached replicas: %d" % len(cachedReplicas), method=method, transID=transID)
        setCached = set(cachedReplicas)
        setLfns = set(lfns)
        for lfn in setLfns & setCached:
            dataReplicas[lfn] = cachedReplicas[lfn]
        newLFNs = setLfns - setCached
        self._logInfo(
            "ReplicaCache hit for %d out of %d LFNs" % (len(dataReplicas), nLfns), method=method, transID=transID
        )
        if newLFNs:
            startTime = time.time()
            self._logInfo("Getting replicas for %d files from catalog" % len(newLFNs), method=method, transID=transID)
            newReplicas = {}
            for chunk in breakListIntoChunks(newLFNs, 10000):
                res = self._getDataReplicasDM(transID, chunk, clients, forJobs=forJobs)
                if res["OK"]:
                    reps = {lfn: ses for lfn, ses in res["Value"].items() if ses}
                    newReplicas.update(reps)
                    self.__updateCache(transID, reps)
                else:
                    self._logWarn(
                        "Failed to get replicas for %d files" % len(chunk),
                        res["Message"],
                        method=method,
                        transID=transID,
                    )

            self._logInfo(
                "Obtained %d replicas from catalog in %.1f seconds" % (len(newReplicas), time.time() - startTime),
                method=method,
                transID=transID,
            )
            dataReplicas.update(newReplicas)
            noReplicas = newLFNs - set(dataReplicas)
            self.__writeCache(transID)
            if noReplicas:
                self._logWarn(
                    "Found %d files without replicas (or only in Failover)" % len(noReplicas),
                    method=method,
                    transID=transID,
                )
        return S_OK(dataReplicas)

    def _getDataReplicasDM(self, transID, lfns, clients, forJobs=True, ignoreMissing=False):
        """Get the replicas for the LFNs and check their statuses, using the replica manager"""
        method = "_getDataReplicasDM"

        startTime = time.time()
        self._logVerbose(
            "Getting replicas%s from catalog for %d files" % (" for jobs" if forJobs else "", len(lfns)),
            method=method,
            transID=transID,
        )
        if forJobs:
            # Get only replicas eligible for jobs
            res = clients["DataManager"].getReplicasForJobs(lfns, getUrl=False)
        else:
            # Get all replicas
            res = clients["DataManager"].getReplicas(lfns, getUrl=False)
        if not res["OK"]:
            return res
        replicas = res["Value"]
        # Prepare a dictionary for all LFNs
        dataReplicas = {}
        self._logVerbose(
            "Replica results for %d files obtained in %.2f seconds" % (len(lfns), time.time() - startTime),
            method=method,
            transID=transID,
        )
        # If files are neither Successful nor Failed, they are set problematic in the FC
        problematicLfns = [lfn for lfn in lfns if lfn not in replicas["Successful"] and lfn not in replicas["Failed"]]
        if problematicLfns:
            self._logInfo("%d files found problematic in the catalog, set ProbInFC" % len(problematicLfns))
            res = clients["TransformationClient"].setFileStatusForTransformation(
                transID, TransformationFilesStatus.PROB_IN_FC, problematicLfns
            )
            if not res["OK"]:
                self._logError(
                    "Failed to update status of problematic files:", res["Message"], method=method, transID=transID
                )
        # Create a dictionary containing all the file replicas
        failoverLfns = []
        for lfn, replicaDict in replicas["Successful"].items():
            for se in replicaDict:
                # This remains here for backward compatibility in case VOs have not defined SEs not to be used for jobs
                if forJobs and "failover" in se.lower():
                    self._logVerbose("Ignoring failover replica for %s." % lfn, method=method, transID=transID)
                else:
                    dataReplicas.setdefault(lfn, []).append(se)
            if not dataReplicas.get(lfn):
                failoverLfns.append(lfn)
        if failoverLfns:
            self._logVerbose("%d files have no replica but possibly in Failover SE" % len(failoverLfns))
        # Make sure that file missing from the catalog are marked in the transformation DB.
        missingLfns = []
        for lfn, reason in replicas["Failed"].items():
            if "No such file or directory" in reason:
                self._logVerbose("%s not found in the catalog." % lfn, method=method, transID=transID)
                missingLfns.append(lfn)
        if missingLfns:
            self._logInfo("%d files not found in the catalog" % len(missingLfns))
            if ignoreMissing:
                dataReplicas.update(dict.fromkeys(missingLfns, []))
            else:
                res = clients["TransformationClient"].setFileStatusForTransformation(
                    transID, TransformationFilesStatus.MISSING_IN_FC, missingLfns
                )
                if not res["OK"]:
                    self._logError(
                        "Failed to update status of missing files:", res["Message"], method=method, transID=transID
                    )
        return S_OK(dataReplicas)

    def __updateCache(self, transID, newReplicas):
        """Add replicas to the cache"""
        self.replicaCache.setdefault(transID, {})[datetime.datetime.utcnow()] = newReplicas

    def __clearCacheForTrans(self, transID):
        """Remove all replicas for a transformation"""
        self.replicaCache.pop(transID, None)

    def __cleanReplicas(self, transID, lfns):
        """Remove cached replicas that are not in a list"""
        cachedReplicas = set()
        for replicas in self.replicaCache.get(transID, {}).values():
            cachedReplicas.update(replicas)
        toRemove = cachedReplicas - set(lfns)
        if toRemove:
            self._logInfo("Remove %d files from cache" % len(toRemove), method="__cleanReplicas", transID=transID)
            self.__removeFromCache(transID, toRemove)

    def __cleanCache(self, transID):
        """Cleans the cache"""
        try:
            if transID in self.replicaCache:
                timeLimit = datetime.datetime.utcnow() - datetime.timedelta(days=self.replicaCacheValidity)
                for updateTime in set(self.replicaCache[transID]):
                    nCache = len(self.replicaCache[transID][updateTime])
                    if updateTime < timeLimit or not nCache:
                        self._logInfo(
                            "Clear %s replicas for transformation %s, time %s"
                            % ("%d cached" % nCache if nCache else "empty cache", str(transID), str(updateTime)),
                            transID=transID,
                            method="__cleanCache",
                        )
                        del self.replicaCache[transID][updateTime]
                # Remove empty transformations
                if not self.replicaCache[transID]:
                    del self.replicaCache[transID]
        except Exception as x:
            self._logException("Exception when cleaning replica cache:", lException=x)

    def __removeFilesFromCache(self, transID, lfns):
        removed = self.__removeFromCache(transID, lfns)
        if removed:
            self._logInfo("Removed %d replicas from cache" % removed, method="__removeFilesFromCache", transID=transID)
            self.__writeCache(transID)

    def __removeFromCache(self, transID, lfns):
        if transID not in self.replicaCache:
            return
        removed = 0
        if self.replicaCache[transID] and lfns:
            for lfn in lfns:
                for timeKey in self.replicaCache[transID]:
                    if self.replicaCache[transID][timeKey].pop(lfn, None):
                        removed += 1
        return removed

    def __cacheFile(self, transID):
        return self.cacheFile.replace(".pkl", "_%s.pkl" % str(transID))

    @gSynchro
    def __readCache(self, transID):
        """Reads from the cache"""
        if transID in self.replicaCache:
            return
        try:
            method = "__readCache"
            fileName = self.__cacheFile(transID)
            if not os.path.exists(fileName):
                self.replicaCache[transID] = {}
            else:
                with open(fileName, "rb") as cacheFile:
                    self.replicaCache[transID] = pickle.load(cacheFile)
                self._logInfo(
                    "Successfully loaded replica cache from file %s (%d files)"
                    % (fileName, self.__filesInCache(transID)),
                    method=method,
                    transID=transID,
                )
        except Exception as x:
            self._logException(
                "Failed to load replica cache from file %s" % fileName, lException=x, method=method, transID=transID
            )
            self.replicaCache[transID] = {}

    def __filesInCache(self, transID):
        cache = self.replicaCache.get(transID, {})
        return sum(len(lfns) for lfns in cache.values())

    @gSynchro
    def __writeCache(self, transID=None):
        """Writes the cache"""
        method = "__writeCache"
        try:
            startTime = time.time()
            transList = [transID] if transID else set(self.replicaCache)
            filesInCache = 0
            nCache = 0
            for t_id in transList:
                # Protect the copy of the cache
                filesInCache += self.__filesInCache(t_id)
                # write to a temporary file in order to avoid corrupted files
                cacheFile = self.__cacheFile(t_id)
                tmpFile = cacheFile + ".tmp"
                with open(tmpFile, "wb") as fd:
                    pickle.dump(self.replicaCache.get(t_id, {}), fd)
                # Now rename the file as it shold
                os.rename(tmpFile, cacheFile)
                nCache += 1
            self._logInfo(
                "Successfully wrote %d replica cache file(s) (%d files) in %.1f seconds"
                % (nCache, filesInCache, time.time() - startTime),
                method=method,
                transID=transID if transID else None,
            )
        except Exception as x:
            self._logException(
                "Could not write replica cache file %s" % cacheFile, lException=x, method=method, transID=t_id
            )

    def __generatePluginObject(self, plugin, clients):
        """This simply instantiates the TransformationPlugin class with the relevant plugin name"""
        try:
            plugModule = __import__(self.pluginLocation, globals(), locals(), ["TransformationPlugin"])
        except ImportError as e:
            self._logException(
                "Failed to import 'TransformationPlugin' %s" % plugin, lException=e, method="__generatePluginObject"
            )
            return S_ERROR()
        try:
            plugin_o = getattr(plugModule, "TransformationPlugin")(
                "%s" % plugin, transClient=clients["TransformationClient"], dataManager=clients["DataManager"]
            )
            return S_OK(plugin_o)
        except AttributeError as e:
            self._logException("Failed to create %s()" % plugin, lException=e, method="__generatePluginObject")
            return S_ERROR()
        plugin_o.setDirectory(self.workDirectory)
        plugin_o.setCallback(self.pluginCallback)

    def pluginCallback(self, transID, invalidateCache=False):
        """Standard plugin callback"""
        if invalidateCache:
            try:
                if transID in self.replicaCache:
                    self._logInfo(
                        "Removed cached replicas for transformation", method="pluginCallBack", transID=transID
                    )
                    self.replicaCache.pop(transID)
                    self.__writeCache(transID)
            except Exception:
                pass
