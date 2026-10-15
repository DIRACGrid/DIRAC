""" Encapsulate here the logic for limiting the matching of jobs

    Utilities and classes here are used by the Matcher
"""
import collections
import threading
import time
from functools import partial

from DIRAC import S_ERROR, S_OK, gLogger
from DIRAC.ConfigurationSystem.Client.Helpers.Operations import Operations
from DIRAC.Core.Utilities.DErrno import ESECTION, cmpError
from DIRAC.Core.Utilities.DictCache import DictCache, TwoLevelCache
from DIRAC.WorkloadManagementSystem.Client import JobStatus
from DIRAC.WorkloadManagementSystem.DB.JobDB import JobDB

# Sections that may sit next to the job attribute sections of a site but are not attributes
_RUNNING_LIMIT_SUBSECTIONS = ("CEs",)
_MATCHING_DELAY_SUBSECTIONS = ("Burst",)


class TokenBucket:
    """Token bucket implementing a MatchingDelay

    The bucket holds at most ``burst`` tokens and gains one every ``interval`` seconds.
    Matching a job takes a token: on average one job is matched every ``interval`` seconds,
    and up to ``burst`` jobs can be matched at once after a quiet period.
    With ``burst=1`` this is "at least ``interval`` seconds between two matches".
    """

    def __init__(self, interval, burst, now):
        self.interval = interval
        self.burst = burst
        self.tokens = float(burst)
        self.updated = now

    def configure(self, interval, burst, now):
        """Apply a (possibly) new configuration, keeping the tokens accumulated so far"""
        self.refill(now)
        self.interval = interval
        self.burst = burst
        self.tokens = min(self.tokens, burst)

    def refill(self, now):
        if self.interval > 0:
            self.tokens = min(self.burst, self.tokens + (now - self.updated) / self.interval)
        else:
            self.tokens = float(self.burst)
        self.updated = now


class MatchingReservation:
    """MatchingDelay tokens reserved for one matching attempt

    Tokens are taken *before* querying the TaskQueueDB for every (attribute, value) that has one
    available, and the values without a token are excluded from the match. Once the match is
    known, :meth:`commit` keeps the tokens corresponding to the matched job and gives the others
    back; :meth:`release` gives back whatever is left (e.g. no match, or an error). Two concurrent
    matching attempts can therefore never use the same token.
    """

    def __init__(self, limiter, siteName, reserved=(), blocked=()):
        self._limiter = limiter
        self.siteName = siteName
        self._reserved = list(reserved)
        self.negativeCond = {}
        for attName, attValue in blocked:
            self.negativeCond.setdefault(attName, []).append(attValue)

    @property
    def attributeNames(self):
        """Names of the job attributes needed by :meth:`commit`"""
        return {attName for attName, _ in self._reserved}

    def commit(self, jobAttributes):
        """Keep the tokens for the matched job, give back the others

        :param dict jobAttributes: attributes of the matched job
        :returns: list of (attName, attValue) for which a token was used
        """
        used = [(attName, attValue) for attName, attValue in self._reserved if jobAttributes.get(attName) == attValue]
        self._reserved = [key for key in self._reserved if key not in used]
        self.release()
        return used

    def release(self):
        """Give back the tokens still held (idempotent)"""
        if self._reserved:
            self._limiter._refundTokens(self.siteName, self._reserved)
            self._reserved = []


class Limiter:
    # static variables shared between all instances of this class
    csDictCache = DictCache()
    condCache = DictCache()
    newCache = TwoLevelCache(10, 300)

    # MatchingDelay state: siteName -> {(attName, attValue): TokenBucket}
    delayBuckets = {}
    delayLock = threading.Lock()

    # Matches made by this process, to correct the cached running counts until they are refreshed:
    # (siteName, attName) -> deque of (time, attValue)
    recentMatches = collections.defaultdict(collections.deque)
    recentMatchesLock = threading.Lock()
    # Matches older than this are always included in the cached counts (hard TTL of newCache)
    RECENT_MATCHES_WINDOW = 300

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
        negativeCondition = {}

        # Run Limit
        result = self.__opsHelper.getSections(self.__runningLimitSection)
        if not result["OK"]:
            self.log.error("Issue getting running conditions", result["Message"])
            sites_with_running_limits = []
        else:
            sites_with_running_limits = result["Value"]
            self.log.verbose(f"Found running conditions for {len(sites_with_running_limits)} sites")

        for siteName in sites_with_running_limits:
            result = self.__getRunningCondition(siteName)
            if not result["OK"]:
                self.log.error("Issue getting running conditions", result["Message"])
                running_condition = {}
            else:
                running_condition = result["Value"]
            if running_condition:
                negativeCondition[siteName] = running_condition

        # Delay limit
        if self.__opsHelper.getValue("JobScheduling/CheckMatchingDelay", True):
            result = self.__opsHelper.getSections(self.__matchingDelaySection)
            if not result["OK"]:
                self.log.error("Issue getting delay conditions", result["Message"])
                sites_with_matching_delay = []
            else:
                sites_with_matching_delay = result["Value"]
                self.log.verbose(f"Found delay conditions for {len(sites_with_matching_delay)} sites")

            for siteName in sites_with_matching_delay:
                delay_condition = self.__getDelayCondition(siteName)
                if siteName in negativeCondition:
                    negativeCondition[siteName] = self.mergeCond(negativeCondition[siteName], delay_condition)
                else:
                    negativeCondition[siteName] = delay_condition

        orCond = []
        for siteName in negativeCondition:
            negativeCondition[siteName]["Site"] = siteName
            orCond.append(negativeCondition[siteName])
        self.condCache.add("GLOBAL", 10, orCond)
        return orCond

    def getNegativeCondForSite(self, siteName, gridCE=None, checkDelay=True):
        """Generate a negative query based on the limits set on the site

        :param str siteName: site name
        :param str gridCE: CE name, for CE specific running limits
        :param bool checkDelay: include the MatchingDelay. The Matcher passes False and uses
                                :meth:`reserveMatchingSlots` instead, which is race free.
        """
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
                    negativeCond = self.mergeCond(negativeCond, result["Value"])

        if checkDelay and self.__opsHelper.getValue("JobScheduling/CheckMatchingDelay", True):
            delayCond = self.__getDelayCondition(siteName)
            self.log.verbose("Negative conditions for site", f"{siteName} after delay checking are: {str(delayCond)}")
            negativeCond = self.mergeCond(negativeCond, delayCond)

        if checkDelay and negativeCond:
            self.log.info("Negative conditions for site", f"{siteName} are: {str(negativeCond)}")

        return negativeCond

    @staticmethod
    def mergeCond(negCond, addCond):
        """Merge two negative dicts (``addCond`` into ``negCond``, which is returned)"""
        for attr in addCond:
            if attr not in negCond:
                negCond[attr] = []
            for value in addCond[attr]:
                if value not in negCond[attr]:
                    negCond[attr].append(value)
        return negCond

    def __extractCSData(self, section, cast=int, skipSections=()):
        """Extract limiting information from the CS in the form:
        { 'JobType' : { 'Merge' : 20, 'MCGen' : 1000 } }

        :param cast: callable used to convert each value. ``int`` for job-count
            limits (RunningLimit) and ``float`` for delays in seconds (MatchingDelay),
            which may be sub-second.
        :param skipSections: names of subsections which are not job attributes
        """
        cacheKey = f"{section}:{cast.__name__}"
        stuffDict = self.csDictCache.get(cacheKey)
        if stuffDict:
            return S_OK(stuffDict)

        result = self.__opsHelper.getSections(section)
        if not result["OK"]:
            if cmpError(result, ESECTION):
                return S_OK({})
            return result
        attribs = [attName for attName in result["Value"] if attName not in skipSections]
        stuffDict = {}
        for attName in attribs:
            result = self.__opsHelper.getOptionsDict(f"{section}/{attName}")
            if not result["OK"]:
                return result
            attLimits = result["Value"]
            try:
                attLimits = {k: cast(attLimits[k]) for k in attLimits}
            except Exception as excp:
                errMsg = f"{section}/{attName} has to contain numbers: {str(excp)}"
                self.log.error(errMsg)
                return S_ERROR(errMsg)
            stuffDict[attName] = attLimits

        self.csDictCache.add(cacheKey, 300, stuffDict)
        return S_OK(stuffDict)

    ##############################################################################
    # Running limits

    def __getRunningLimits(self, siteName, gridCE=None):
        """Get the running limits from the CS: { 'JobType' : { 'Merge' : 20, 'MCGen' : 1000 } }"""
        if gridCE:
            csSection = f"{self.__runningLimitSection}/{siteName}/CEs/{gridCE}"
        else:
            csSection = f"{self.__runningLimitSection}/{siteName}"
        return self.__extractCSData(csSection, skipSections=_RUNNING_LIMIT_SUBSECTIONS)

    def __getRunningCondition(self, siteName, gridCE=None):
        """Get extra conditions allowing site throttling"""
        result = self.__getRunningLimits(siteName, gridCE)
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
            data = self.__getRunningCounts(siteName, attName)
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

    def __getRunningCounts(self, siteName, attName):
        """Number of running jobs per value of attName at the site

        The counts from the JobDB are cached for a few seconds. To avoid overshooting the limits
        while they are cached, the matches done by this process since the counts were taken are
        added to them.
        """
        snapshot = self.newCache.get(
            f"Running:{siteName}:{attName}", partial(self._runningCountsSnapshot, siteName, attName)
        )
        counts = dict(snapshot["counts"])
        with self.recentMatchesLock:
            for matchTime, attValue in self.recentMatches.get((siteName, attName), ()):
                if matchTime > snapshot["time"]:
                    counts[attValue] = counts.get(attValue, 0) + 1
        return counts

    def _runningCountsSnapshot(self, siteName, attName):
        """Counts of running jobs, with the time at which they were taken"""
        startTime = time.time()
        data = self._countsByJobType(siteName, attName)
        if isinstance(data, dict) and data.get("OK") is False:
            self.log.error("Failed to get the number of running jobs", f"at {siteName}: {data['Message']}")
            data = {}
        return {"time": startTime, "counts": data}

    def _countsByJobType(self, siteName, attName):
        result = self.jobDB.getCounters(
            "Jobs",
            [attName],
            {"Site": siteName, "Status": [JobStatus.RUNNING, JobStatus.MATCHED, JobStatus.STALLED]},
        )
        if not result["OK"]:
            return result
        data = {k[0][attName]: k[1] for k in result["Value"]}
        return data

    def __recordRunningMatch(self, siteName, jobAttributes, now=None):
        """Remember that a job was matched at a site, for the running limits"""
        now = now or time.time()
        with self.recentMatchesLock:
            for attName, attValue in jobAttributes.items():
                matches = self.recentMatches[(siteName, attName)]
                matches.append((now, attValue))
                while matches and matches[0][0] < now - self.RECENT_MATCHES_WINDOW:
                    matches.popleft()

    ##############################################################################
    # Matching delay

    def __getDelayConfig(self, siteName):
        """Get the MatchingDelay configuration of a site from the CS

        :returns: S_OK({(attName, attValue): (interval, burst)})
        """
        siteSection = f"{self.__matchingDelaySection}/{siteName}"
        result = self.__extractCSData(siteSection, cast=float, skipSections=_MATCHING_DELAY_SUBSECTIONS)
        if not result["OK"]:
            return result
        delayDict = result["Value"]
        if not delayDict:
            return S_OK({})
        result = self.__extractCSData(f"{siteSection}/Burst", cast=int)
        if not result["OK"]:
            return result
        burstDict = result["Value"]

        config = {}
        for attName, values in delayDict.items():
            if attName not in self.jobDB.jobAttributeNames:
                self.log.error("Attribute does not exist in the JobDB. Please fix it!", f"({attName})")
                continue
            for attValue, interval in values.items():
                burst = max(1, burstDict.get(attName, {}).get(attValue, 1))
                config[(attName, attValue)] = (interval, burst)
        return S_OK(config)

    def __getBuckets(self, siteName, config, now):
        """Get the up to date token buckets of a site, the caller must hold ``delayLock``

        :param config: result of :meth:`__getDelayConfig`
        """
        if not config["OK"]:
            # Keep the current state rather than lifting the delays
            self.log.error("Issue getting delay conditions", config["Message"])
        buckets = self.delayBuckets.setdefault(siteName, {})
        if config["OK"]:
            for key in list(buckets):
                if key not in config["Value"]:
                    del buckets[key]
            for key, (interval, burst) in config["Value"].items():
                if key in buckets:
                    buckets[key].configure(interval, burst, now)
                else:
                    buckets[key] = TokenBucket(interval, burst, now)
        for bucket in buckets.values():
            bucket.refill(now)
        return buckets

    def __getDelayCondition(self, siteName):
        """Get the job attribute values currently held back by the MatchingDelay (without reserving)"""
        negCond = {}
        config = self.__getDelayConfig(siteName)
        with self.delayLock:
            for (attName, attValue), bucket in self.__getBuckets(siteName, config, time.monotonic()).items():
                if bucket.tokens < 1:
                    negCond.setdefault(attName, []).append(attValue)
        return negCond

    def reserveMatchingSlots(self, siteName):
        """Reserve MatchingDelay tokens for a matching attempt at a site

        :returns: a :class:`MatchingReservation`; its ``negativeCond`` lists the attribute values
                  that cannot be matched now. It must be committed or released.
        """
        if not self.__opsHelper.getValue("JobScheduling/CheckMatchingDelay", True):
            return MatchingReservation(self, siteName)
        reserved = []
        blocked = []
        config = self.__getDelayConfig(siteName)
        with self.delayLock:
            for key, bucket in self.__getBuckets(siteName, config, time.monotonic()).items():
                if bucket.tokens >= 1:
                    bucket.tokens -= 1
                    reserved.append(key)
                else:
                    blocked.append(key)
        return MatchingReservation(self, siteName, reserved, blocked)

    def _refundTokens(self, siteName, keys):
        """Give back tokens taken by a reservation"""
        with self.delayLock:
            buckets = self.delayBuckets.get(siteName, {})
            for key in keys:
                if key in buckets:
                    buckets[key].tokens = min(buckets[key].burst, buckets[key].tokens + 1)

    def recordMatch(self, siteName, jid, reservation=None, knownAtts=None):
        """Record that a job was matched at a site

        Keeps the MatchingDelay tokens used by the job (giving back the other reserved ones) and
        counts the job against the running limits until the running counts are refreshed.

        :param str siteName: site where the job was matched
        :param int jid: job ID
        :param reservation: the :class:`MatchingReservation` used for the match, if any
        :param dict knownAtts: job attributes already known by the caller, to avoid querying them
        """
        attNames = set(reservation.attributeNames) if reservation else set()
        if self.__opsHelper.getValue("JobScheduling/CheckJobLimits", True):
            result = self.__getRunningLimits(siteName)
            if result["OK"]:
                attNames.update(attName for attName in result["Value"] if attName in self.jobDB.jobAttributeNames)
        if not attNames:
            if reservation:
                reservation.release()
            return S_OK()

        knownAtts = knownAtts or {}
        atts = {attName: knownAtts[attName] for attName in attNames if attName in knownAtts}
        missing = [attName for attName in attNames if attName not in atts]
        if missing:
            result = self.jobDB.getJobAttributes(jid, missing)
            if not result["OK"]:
                self.log.error("Error while retrieving attributes", f"of job {jid}: {result['Message']}")
                if reservation:
                    reservation.release()
                return result
            atts.update(result["Value"])

        self.__recordRunningMatch(siteName, atts)
        if reservation:
            for attName, attValue in reservation.commit(atts):
                self.__logDelay(siteName, attName, attValue)
        return S_OK()

    def __logDelay(self, siteName, attName, attValue):
        bucket = self.delayBuckets.get(siteName, {}).get((attName, attValue))
        interval = bucket.interval if bucket else "?"
        self.log.notice(f"Adding delay for {siteName}/{attName}={attValue} of {interval} secs")

    def updateDelayCounters(self, siteName, jid, knownAtts=None):
        """Take a MatchingDelay token for a job matched at a site

        .. deprecated:: The Matcher uses :meth:`reserveMatchingSlots` and :meth:`recordMatch`,
                        which take the token before matching and so cannot be raced.
        """
        config = self.__getDelayConfig(siteName)
        if not config["OK"]:
            return config
        attNames = {attName for attName, _ in config["Value"]}
        if not attNames:
            return S_OK()
        knownAtts = knownAtts or {}
        atts = {attName: knownAtts[attName] for attName in attNames if attName in knownAtts}
        missing = [attName for attName in attNames if attName not in atts]
        if missing:
            result = self.jobDB.getJobAttributes(jid, missing)
            if not result["OK"]:
                self.log.error("Error while retrieving attributes", f"of job {jid}: {result['Message']}")
                return result
            atts.update(result["Value"])
        with self.delayLock:
            buckets = self.__getBuckets(siteName, config, time.monotonic())
            used = [key for key in atts.items() if key in buckets]
            for key in used:
                buckets[key].tokens -= 1
        for attName, attValue in used:
            self.__logDelay(siteName, attName, attValue)
        return S_OK()
