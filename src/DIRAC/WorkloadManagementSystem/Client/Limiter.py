""" Encapsulate here the logic for limiting the matching of jobs

    Utilities and classes here are used by the Matcher
"""
from DIRAC import S_OK, S_ERROR
from DIRAC import gLogger

from DIRAC.Core.Utilities.DictCache import DictCache
from DIRAC.Core.Utilities.DErrno import cmpError, ESECTION
from DIRAC.ConfigurationSystem.Client.Helpers.Operations import Operations
from DIRAC.WorkloadManagementSystem.DB.JobDB import JobDB
from DIRAC.WorkloadManagementSystem.Client import JobStatus


class Limiter:
    # static variables shared between all instances of this class
    csDictCache = DictCache()
    condCache = DictCache()
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
            cK = f"Running:{siteName}:{attName}"
            data = self.condCache.get(cK)
            if not data:
                result = self.jobDB.getCounters(
                    "Jobs",
                    [attName],
                    {"Site": siteName, "Status": [JobStatus.RUNNING, JobStatus.MATCHED, JobStatus.STALLED]},
                )
                if not result["OK"]:
                    return result
                data = result["Value"]
                data = {k[0][attName]: k[1] for k in data}
                self.condCache.add(cK, 10, data)
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
