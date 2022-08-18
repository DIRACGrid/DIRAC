"""
  The InputData Optimizer Executor queries the file catalog for specified job input data and adds the
  relevant information to the job optimizer parameters to be used during the scheduling decision.
"""
import pprint
import time

from DIRAC import S_OK, S_ERROR
from DIRAC.Core.Utilities.Proxy import executeWithUserProxy
from DIRAC.ConfigurationSystem.Client.Helpers.Operations import Operations
from DIRAC.Resources.Storage.StorageElement import StorageElement
from DIRAC.Resources.Catalog.FileCatalog import FileCatalog
from DIRAC.DataManagementSystem.Utilities.DMSHelpers import DMSHelpers
from DIRAC.DataManagementSystem.Client.DataManager import DataManager
from DIRAC.WorkloadManagementSystem.Executor.Base.OptimizerExecutor import OptimizerExecutor
from DIRAC.WorkloadManagementSystem.Client import JobMinorStatus


class InputData(OptimizerExecutor):
    """
    The specific Optimizer must provide the following methods:
      - initializeOptimizer() before each execution cycle
      - optimizeJob() - the main method called for each job
    """

    @classmethod
    def initializeOptimizer(cls):
        """Initialize specific parameters for InputData executor."""
        cls.failedMinorStatus = cls.ex_getOption("/FailedJobStatus", "Input Data Not Available")
        # this will ignore failover SE files
        cls.checkFileMetadata = cls.ex_getOption("CheckFileMetadata", True)
        # flag to require Input Data lookup with a user proxy
        cls.checkWithUserProxy = cls.ex_getOption("CheckWithUserProxy", False)

        cls.__dataManDict = {}
        cls.__fcDict = {}
        cls.__SEToSiteMap = {}
        cls.__lastCacheUpdate = 0
        cls.__cacheLifeTime = 600

        # Note: this is a default, that right now is generically the default for user jobs, at least for main DIRAC users
        # (since this now doesn't run for production jobs)
        # But this should probably be replaced by what the job actually request.
        # The problem is that the InputDataPolicy is not easy to get (a JDL parameter).
        # This may be used but clear how now
        # cls.__connectionLevel = 'PROTOCOL'

        return S_OK()

    def __getDataManager(self, vo):
        if vo in self.__dataManDict:
            return self.__dataManDict[vo]

        try:
            self.__dataManDict[vo] = DataManager(vo=vo)
        except Exception:
            msg = "Failed to create DataManager"
            self.log.exception(msg)
            return None
        return self.__dataManDict[vo]

    def __getFileCatalog(self, vo):
        if vo in self.__fcDict:
            return self.__fcDict[vo]

        try:
            self.__fcDict[vo] = FileCatalog(vo=vo)
        except Exception:
            msg = "Failed to create FileCatalog"
            self.log.exception(msg)
            return None
        return self.__fcDict[vo]

    def optimizeJob(self, jid, jobState):
        """This is the method that needs to be implemented by each and every Executor

        This optimizer will run if and only if it is needed:
          - it will run only if there are input files
          - for production jobs this can be skipped,
            since the logic is already applied by the transformation system, via the TaskManagerPlugins
        """
        # Is it a production job?
        result = jobState.getAttribute("JobType")
        if not result["OK"]:
            self.jobLog.error("Could not retrieve job type", result["Message"])
            return result
        jobType = result["Value"]
        if jobType in Operations().getValue("Transformations/DataProcessing", []):
            self.jobLog.info("Skipping optimizer, since this is a Production job")
            return self.setNextOptimizer()

        # Is there input data or not?
        result = jobState.getInputData()
        if not result["OK"]:
            self.jobLog.error("Cannot retrieve input data", result["Message"])
            return result
        inputData = result["Value"]

        result = self._getInputSandbox(jobState)
        if not result["OK"]:
            self.jobLog.error("Cannot retrieve input sandbox", result["Message"])
            return result
        inputSandbox = result["Value"]

        if not inputData and not inputSandbox:
            self.jobLog.notice("No input data nor LFN input sandboxes. Skipping.")
            return self.setNextOptimizer()

        # From now on we know that it is a user job with input data
        # and or with input sandbox

        # Check if we already executed this Optimizer and the input data is resolved
        result = self.retrieveOptimizerParam(self.ex_getProperty("optimizerName"))
        if result["OK"] and result["Value"]:
            self.jobLog.info("InputData optimizer ran already")
        else:
            self.jobLog.info("Processing input data")
            if self.checkWithUserProxy:
                result = jobState.getAttribute("Owner")
                if not result["OK"]:
                    self.jobLog.error("Could not retrieve job owner", result["Message"])
                    return result
                userName = result["Value"]
                result = jobState.getAttribute("OwnerGroup")
                if not result["OK"]:
                    self.jobLog.error("Could not retrieve job owner group", result["Message"])
                    return result
                userGroup = result["Value"]
                if inputSandbox:
                    result = self._resolveInputSandbox(  # pylint: disable=unexpected-keyword-arg
                        jobState, inputSandbox, proxyUserName=userName, proxyUserGroup=userGroup, executionLock=True
                    )
                    if not result["OK"]:
                        self.jobLog.error("Could not resolve input sandbox", result["Message"])
                        return result

                if inputData:
                    result = self._resolveInputData(  # pylint: disable=unexpected-keyword-arg
                        jobState, inputData, proxyUserName=userName, proxyUserGroup=userGroup, executionLock=True
                    )
            else:
                if inputSandbox:
                    result = self._resolveInputSandbox(jobState, inputSandbox)
                    if not result["OK"]:
                        self.jobLog.error("Could not resolve input sandbox", result["Message"])
                        return result

                if inputData:
                    result = self._resolveInputData(jobState, inputData)
            if not result["OK"]:
                self.jobLog.warn(result["Message"])
                return result

        return self.setNextOptimizer()

    #############################################################################

    def _getInputSandbox(self, jobState):
        """Return the LFN input sandbox if any

        :param JobState jobState: the JobState object
        :returns: S_OK/S_ERROR structure with (input sandbox lfn list)
        """
        inputSandbox = []
        # Check if the InputSandbox contains LFNs, and in that case treat them as input data
        result = jobState.getManifest()
        if not result["OK"]:
            return result
        manifest = result["Value"]
        # isb below will look something horrible like "['/an/lfn/1.txt', 'another/one.boo' ]"
        isb = manifest.getOption("InputSandbox")
        if not isb:
            return S_OK(inputSandbox)
        isbList = [li.replace("[", "").replace("]", "").replace("'", "") for li in isb.replace(" ", "").split(",")]
        for li in isbList:
            if li.startswith("LFN:"):
                inputSandbox.append(li.replace("LFN:", ""))
        return S_OK(inputSandbox)

    @executeWithUserProxy
    def _resolveInputData(self, jobState, inputData):
        """This method checks the file catalog for replica information.

        :param JobState jobState: JobState object
        :param list inputData: list of LFNs

        :returns: S_OK/S_ERROR structure with resolved input data info
        """
        lfns = inputData

        result = jobState.getManifest()
        if not result["OK"]:
            self.jobLog.error("Failed to get job manifest", result["Message"])
            return result
        manifest = result["Value"]
        vo = manifest.getOption("VirtualOrganization")
        startTime = time.time()
        dm = self.__getDataManager(vo)
        if dm is None:
            return S_ERROR("Failed to instantiate DataManager for vo %s" % vo)
        else:
            # This will return already active replicas, excluding banned SEs, and
            # removing tape replicas if there are disk replicas
            result = dm.getReplicasForJobs(lfns)
        self.jobLog.verbose("Catalog replicas lookup time", "%.2f seconds " % (time.time() - startTime))
        if not result["OK"]:
            self.log.warn(result["Message"])
            return result

        replicaDict = result["Value"]

        self.jobLog.verbose("REPLICA DICT", replicaDict)

        result = self.__checkReplicas(replicaDict, vo)

        if not result["OK"]:
            self.jobLog.error("Failed to check replicas", result["Message"])
            return result
        okReplicas = result["Value"]

        result = self.__getSiteCandidates(okReplicas, vo)
        if not result["OK"]:
            self.jobLog.error("Failed to check SiteCandidates", result["Message"])
            return result
        siteCandidates = result["Value"]

        if self.ex_getOption("CheckFileMetadata", True):
            result = jobState.getManifest()
            if not result["OK"]:
                return result
            manifest = result["Value"]
            vo = manifest.getOption("VirtualOrganization")
            fc = self.__getFileCatalog(vo)
            if fc is None:
                return S_ERROR("Failed to instantiate FileCatalog for vo %s" % vo)
            else:
                guidDict = fc.getFileMetadata(lfns)
            self.jobLog.info("Catalog Metadata Lookup Time", "%.2f seconds " % (time.time() - startTime))

            if not guidDict["OK"]:
                self.log.warn(guidDict["Message"])
                return guidDict

            failed = guidDict["Value"]["Failed"]
            if failed:
                self.log.warn("Failed to establish some GUIDs")
                self.log.warn(failed)

            for lfn in replicaDict["Successful"]:
                replicas = replicaDict["Successful"][lfn]
                guidDict["Value"]["Successful"][lfn].update(replicas)

        resolvedData = {}
        resolvedData["Value"] = guidDict
        resolvedData["SiteCandidates"] = siteCandidates
        self.jobLog.debug("Storing:\n%s" % pprint.pformat(resolvedData))
        result = self.storeOptimizerParam(self.ex_getProperty("optimizerName"), resolvedData)
        if not result["OK"]:
            self.log.warn(result["Message"])
            return result
        return S_OK(resolvedData)

    #############################################################################
    def __checkReplicas(self, replicaDict, vo):
        """Check that all input lfns have valid replicas and can all be found at least in one single site.

        :returns: S_ERROR/S_OK(dict of ok replicas)
        """
        badLFNs = []

        if "Successful" not in replicaDict:
            return S_ERROR("No replica Info available")

        okReplicas = replicaDict["Successful"]
        for lfn in okReplicas:
            if not okReplicas[lfn]:
                badLFNs.append("LFN:%s -> No replicas available" % (lfn))

        if "Failed" in replicaDict:
            errorReplicas = replicaDict["Failed"]
            for lfn in errorReplicas:
                badLFNs.append(f"LFN:{lfn} -> {errorReplicas[lfn]}")

        if badLFNs:
            errorMsg = "\n".join(badLFNs)
            self.jobLog.info("Found a number of problematic LFN(s)", "%d\n: %s" % (len(badLFNs), errorMsg))
            result = self.storeOptimizerParam(self.ex_getProperty("optimizerName"), errorMsg)
            if not result["OK"]:
                self.log.error("Failed to set job parameter", result["Message"])
            return S_ERROR("Input data not available")

        return S_OK(okReplicas)

    #############################################################################
    def __getSitesForSE(self, seName):
        """Returns a list of sites having the given SE as a local one.
        Uses the local cache of the site-se information
        """

        # Empty the cache if too old
        now = time.time()
        if (now - self.__lastCacheUpdate) > self.__cacheLifeTime:
            self.log.verbose("Resetting the SE to site mapping cache")
            self.__SEToSiteMap = {}
            self.__lastCacheUpdate = now

        if seName not in self.__SEToSiteMap:
            result = DMSHelpers().getSitesForSE(seName)
            if not result["OK"]:
                self.jobLog.error("Failed to get site for SE", result["Message"])
                return result
            self.__SEToSiteMap[seName] = list(result["Value"])
        return S_OK(self.__SEToSiteMap[seName])

    #############################################################################
    def __getSiteCandidates(self, okReplicas, vo):
        """This method returns a list of possible site candidates based on the job input data requirement.

        For each site candidate, the number of files on disk and tape is resolved.
        """

        lfnSEs = {}
        for lfn in okReplicas:
            replicas = okReplicas[lfn]
            siteSet = set()
            for seName in replicas:
                result = self.__getSitesForSE(seName)
                if not result["OK"]:
                    self.jobLog.warn("Could not get sites for SE", "{}: {}".format(seName, result["Message"]))
                    return result
                siteSet.update(result["Value"])
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
                    result = self.__getSitesForSE(seName)
                    if not result["OK"]:
                        self.jobLog.warn("Could not get sites for SE", "{}: {}".format(seName, result["Message"]))
                        continue
                    siteList = result["Value"]
                    seObj = StorageElement(seName, vo=vo)
                    result = seObj.getStatus()
                    if not result["OK"]:
                        self.jobLog.error("Failed to get SE status", result["Message"])
                        return result
                    seDict[seName] = {"Sites": siteList, "Status": result["Value"]}
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
                        if lfn in tapeLFNs:
                            tapeLFNs.remove(lfn)
                    if seStatus["TapeSE"]:
                        if lfn not in diskLFNs:
                            tapeLFNs.add(lfn)

        for siteName in sitesData:
            sitesData[siteName]["disk"] = len(sitesData[siteName]["disk"])
            sitesData[siteName]["tape"] = len(sitesData[siteName]["tape"])
        return S_OK(sitesData)

    @executeWithUserProxy
    def _resolveInputSandbox(self, jobState, inputSandbox):
        """This method checks the file catalog for replica information.

        :param JobState jobState: JobState object
        :param list inputSandbox: list of LFNs for the input sandbox

        :returns: S_OK/S_ERROR structure with resolved input data info
        """

        result = jobState.getManifest()
        if not result["OK"]:
            self.jobLog.error("Failed to get job manifest", result["Message"])
            return result
        manifest = result["Value"]
        vo = manifest.getOption("VirtualOrganization")
        startTime = time.time()
        dm = self.__getDataManager(vo)
        if dm is None:
            return S_ERROR("Failed to instantiate DataManager for vo %s" % vo)

        # This will return already active replicas, excluding banned SEs, and
        # removing tape replicas if there are disk replicas

        result = dm.getReplicasForJobs(inputSandbox)
        self.jobLog.verbose("Catalog replicas lookup time", "%.2f seconds " % (time.time() - startTime))
        if not result["OK"]:
            self.log.warn(result["Message"])
            return result

        isDict = result["Value"]

        self.jobLog.verbose("REPLICA DICT", isDict)

        result = self.__checkReplicas(isDict, vo)

        if not result["OK"]:
            self.jobLog.error("Failed to check replicas", result["Message"])

        return result
