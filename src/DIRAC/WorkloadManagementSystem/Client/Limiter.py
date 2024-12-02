""" Encapsulate here the logic for limiting the matching of jobs

    Utilities and classes here are used by the Matcher
"""
import threading
from collections import defaultdict
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor, wait, Future
from functools import partial
from typing import Any

from cachetools import TTLCache

from DIRAC import S_OK, S_ERROR
from DIRAC import gLogger

from DIRAC.Core.Utilities.DictCache import DictCache
from DIRAC.Core.Utilities.DErrno import cmpError, ESECTION
from DIRAC.ConfigurationSystem.Client.Helpers.Operations import Operations
from DIRAC.WorkloadManagementSystem.DB.JobDB import JobDB
from DIRAC.WorkloadManagementSystem.Client import JobStatus


class TwoLevelCache:
    """A two-level caching system with soft and hard time-to-live (TTL) expiration.

    This cache implements a two-tier caching mechanism to allow for background refresh
    of cached values. It uses a soft TTL for quick access and a hard TTL as a fallback,
    which helps in reducing latency and maintaining data freshness.

    Attributes:
        soft_cache (TTLCache): A cache with a shorter TTL for quick access.
        hard_cache (TTLCache): A cache with a longer TTL as a fallback.
        locks (defaultdict): Thread-safe locks for each cache key.
        futures (dict): Stores ongoing asynchronous population tasks.
        pool (ThreadPoolExecutor): Thread pool for executing cache population tasks.

    Args:
        soft_ttl (int): Time-to-live in seconds for the soft cache.
        hard_ttl (int): Time-to-live in seconds for the hard cache.
        max_workers (int): Maximum number of workers in the thread pool.
        max_items (int): Maximum number of items in the cache.

    Example:
        >>> cache = TwoLevelCache(soft_ttl=60, hard_ttl=300)
        >>> def populate_func():
        ...     return "cached_value"
        >>> value = cache.get("key", populate_func)

    Note:
        The cache uses a ThreadPoolExecutor with a maximum of 10 workers to
        handle concurrent cache population requests.
    """

    def __init__(self, soft_ttl: int, hard_ttl: int, *, max_workers: int = 10, max_items: int = 1_000_000):
        """Initialize the TwoLevelCache with specified TTLs."""
        self.soft_cache = TTLCache(max_items, soft_ttl)
        self.hard_cache = TTLCache(max_items, hard_ttl)
        self.locks = defaultdict(threading.Lock)
        self.futures: dict[str, Future] = {}
        self.pool = ThreadPoolExecutor(max_workers=max_workers)

    def get(self, key: str, populate_func: Callable[[], Any]):
        """Retrieve a value from the cache, populating it if necessary.

        This method first checks the soft cache for the key. If not found,
        it checks the hard cache while initiating a background refresh.
        If the key is not in either cache, it waits for the populate_func
        to complete and stores the result in both caches.

        Locks are used to ensure there is never more than one concurrent
        population task for a given key.

        Args:
            key (str): The cache key to retrieve or populate.
            populate_func (Callable[[], Any]): A function to call to populate the cache
                                               if the key is not found.

        Returns:
            Any: The cached value associated with the key.

        Note:
            This method is thread-safe and handles concurrent requests for the same key.
        """
        if result := self.soft_cache.get(key):
            return result
        with self.locks[key]:
            if key not in self.futures:
                self.futures[key] = self.pool.submit(self._work, key, populate_func)
            if result := self.hard_cache.get(key):
                self.soft_cache[key] = result
                return result
            # It is critical that ``future`` is waited for outside of the lock as
            # _work aquires the lock before filling the caches. This also means
            # we can gaurentee that the future has not yet been removed from the
            # futures dict.
            future = self.futures[key]
        wait([future])
        return self.hard_cache[key]

    def _work(self, key: str, populate_func: Callable[[], Any]) -> None:
        """Internal method to execute the populate_func and update caches.

        This method is intended to be run in a separate thread. It calls the
        populate_func, stores the result in both caches, and cleans up the
        associated future.

        Args:
            key (str): The cache key to populate.
            populate_func (Callable[[], Any]): The function to call to get the value.

        Note:
            This method is not intended to be called directly by users of the class.
        """
        result = populate_func()
        with self.locks[key]:
            self.futures.pop(key)
            self.hard_cache[key] = result
            self.soft_cache[key] = result


class Limiter:
    # static variables shared between all instances of this class
    csDictCache = DictCache()
    condCache = DictCache()
    newCache = TwoLevelCache(10, 300)
    delayMem = {}

    def __init__(self, jobDB=None, opsHelper=None, pilotRef=None):
        """Constructor"""
        self.__runningLimitSection = "JobScheduling/RunningLimit"
        self.__matchingDelaySection = "JobScheduling/MatchingDelay"

        if jobDB:
            self.jobDB = jobDB
        else:
            self.jobDB = JobDB()

        if pilotRef:
            self.log = gLogger.getSubLogger(f"[{pilotRef}]{self.__class__.__name__}")
            self.jobDB.log = gLogger.getSubLogger(f"[{pilotRef}]{self.__class__.__name__}")
        else:
            self.log = gLogger.getSubLogger(self.__class__.__name__)

        if opsHelper:
            self.__opsHelper = opsHelper
        else:
            self.__opsHelper = Operations()

    def getNegativeCond(self):
        """Get negative condition for ALL sites"""
        orCond = self.condCache.get("GLOBAL")
        if orCond:
            return orCond
        negCond = {}
        # Run Limit
        result = self.__opsHelper.getSections(self.__runningLimitSection)
        sites = []
        if result["OK"]:
            sites = result["Value"]
        for siteName in sites:
            result = self.__getRunningCondition(siteName)
            if not result["OK"]:
                continue
            data = result["Value"]
            if data:
                negCond[siteName] = data
        # Delay limit
        result = self.__opsHelper.getSections(self.__matchingDelaySection)
        sites = []
        if result["OK"]:
            sites = result["Value"]
        for siteName in sites:
            result = self.__getDelayCondition(siteName)
            if not result["OK"]:
                continue
            data = result["Value"]
            if not data:
                continue
            if siteName in negCond:
                negCond[siteName] = self.__mergeCond(negCond[siteName], data)
            else:
                negCond[siteName] = data
        orCond = []
        for siteName in negCond:
            negCond[siteName]["Site"] = siteName
            orCond.append(negCond[siteName])
        self.condCache.add("GLOBAL", 10, orCond)
        return orCond

    def getNegativeCondForSite(self, siteName, gridCE=None):
        """Generate a negative query based on the limits set on the site"""
        # Check if Limits are imposed onto the site
        negativeCond = {}
        if self.__opsHelper.getValue("JobScheduling/CheckJobLimits", True):
            result = self.__getRunningCondition(siteName)
            if not result["OK"]:
                self.log.error("Issue getting running conditions", result["Message"])
            else:
                negativeCond = result["Value"]
            self.log.verbose(
                "Negative conditions for site", f"{siteName} after checking limits are: {str(negativeCond)}"
            )

            if gridCE:
                result = self.__getRunningCondition(siteName, gridCE)
                if not result["OK"]:
                    self.log.error("Issue getting running conditions", result["Message"])
                else:
                    negativeCondCE = result["Value"]
                    negativeCond = self.__mergeCond(negativeCond, negativeCondCE)

        if self.__opsHelper.getValue("JobScheduling/CheckMatchingDelay", True):
            result = self.__getDelayCondition(siteName)
            if result["OK"]:
                delayCond = result["Value"]
                self.log.verbose(
                    "Negative conditions for site", f"{siteName} after delay checking are: {str(delayCond)}"
                )
                negativeCond = self.__mergeCond(negativeCond, delayCond)

        if negativeCond:
            self.log.info("Negative conditions for site", f"{siteName} are: {str(negativeCond)}")

        return negativeCond

    def __mergeCond(self, negCond, addCond):
        """Merge two negative dicts"""
        # Merge both negative dicts
        for attr in addCond:
            if attr not in negCond:
                negCond[attr] = []
            for value in addCond[attr]:
                if value not in negCond[attr]:
                    negCond[attr].append(value)
        return negCond

    def __extractCSData(self, section):
        """Extract limiting information from the CS in the form:
        { 'JobType' : { 'Merge' : 20, 'MCGen' : 1000 } }
        """
        stuffDict = self.csDictCache.get(section)
        if stuffDict:
            return S_OK(stuffDict)

        result = self.__opsHelper.getSections(section)
        if not result["OK"]:
            if cmpError(result, ESECTION):
                return S_OK({})
            return result
        attribs = result["Value"]
        stuffDict = {}
        for attName in attribs:
            result = self.__opsHelper.getOptionsDict(f"{section}/{attName}")
            if not result["OK"]:
                return result
            attLimits = result["Value"]
            try:
                attLimits = {k: int(attLimits[k]) for k in attLimits}
            except Exception as excp:
                errMsg = f"{section}/{attName} has to contain numbers: {str(excp)}"
                self.log.error(errMsg)
                return S_ERROR(errMsg)
            stuffDict[attName] = attLimits

        self.csDictCache.add(section, 300, stuffDict)
        return S_OK(stuffDict)

    def __getRunningCondition(self, siteName, gridCE=None):
        """Get extra conditions allowing site throttling"""
        if gridCE:
            csSection = f"{self.__runningLimitSection}/{siteName}/CEs/{gridCE}"
        else:
            csSection = f"{self.__runningLimitSection}/{siteName}"
        result = self.__extractCSData(csSection)
        if not result["OK"]:
            return result
        limitsDict = result["Value"]
        # limitsDict is something like { 'JobType' : { 'Merge' : 20, 'MCGen' : 1000 } }
        if not limitsDict:
            return S_OK({})
        # Check if the site exceeding the given limits
        negCond = {}
        for attName in limitsDict:
            if attName not in self.jobDB.jobAttributeNames:
                self.log.error("Attribute does not exist", f"({attName}). Check the job limits")
                continue
            data = self.newCache.get(f"Running:{siteName}:{attName}", partial(self._countsByJobType, siteName, attName))
            for attValue in limitsDict[attName]:
                limit = limitsDict[attName][attValue]
                running = data.get(attValue, 0)
                if running >= limit:
                    self.log.verbose(
                        "Job Limit imposed",
                        "at %s on %s/%s=%d, %d jobs already deployed" % (siteName, attName, attValue, limit, running),
                    )
                    if attName not in negCond:
                        negCond[attName] = []
                    negCond[attName].append(attValue)
        # negCond is something like : {'JobType': ['Merge']}
        return S_OK(negCond)

    def updateDelayCounters(self, siteName, jid):
        # Get the info from the CS
        siteSection = f"{self.__matchingDelaySection}/{siteName}"
        result = self.__extractCSData(siteSection)
        if not result["OK"]:
            return result
        delayDict = result["Value"]
        # limitsDict is something like { 'JobType' : { 'Merge' : 20, 'MCGen' : 1000 } }
        if not delayDict:
            return S_OK()
        attNames = []
        for attName in delayDict:
            if attName not in self.jobDB.jobAttributeNames:
                self.log.error("Attribute does not exist in the JobDB. Please fix it!", f"({attName})")
            else:
                attNames.append(attName)
        result = self.jobDB.getJobAttributes(jid, attNames)
        if not result["OK"]:
            self.log.error("Error while retrieving attributes", f"coming from {siteSection}: {result['Message']}")
            return result
        atts = result["Value"]
        # Create the DictCache if not there
        if siteName not in self.delayMem:
            self.delayMem[siteName] = DictCache()
        # Update the counters
        delayCounter = self.delayMem[siteName]
        for attName in atts:
            attValue = atts[attName]
            if attValue in delayDict[attName]:
                delayTime = delayDict[attName][attValue]
                self.log.notice(f"Adding delay for {siteName}/{attName}={attValue} of {delayTime} secs")
                delayCounter.add((attName, attValue), delayTime)
        return S_OK()

    def __getDelayCondition(self, siteName):
        """Get extra conditions allowing matching delay"""
        if siteName not in self.delayMem:
            return S_OK({})
        lastRun = self.delayMem[siteName].getKeys()
        negCond = {}
        for attName, attValue in lastRun:
            if attName not in negCond:
                negCond[attName] = []
            negCond[attName].append(attValue)
        return S_OK(negCond)

    def _countsByJobType(self, siteName, attName):
        result = self.jobDB.getCounters(
            "Jobs",
            [attName],
            {"Site": siteName, "Status": [JobStatus.RUNNING, JobStatus.MATCHED, JobStatus.STALLED]},
        )
        if not result["OK"]:
            return result
        data = result["Value"]
        data = {k[0][attName]: k[1] for k in data}
        return data
