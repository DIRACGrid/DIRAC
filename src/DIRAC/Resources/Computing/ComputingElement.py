"""  The Computing Element class is a base class for all the various
     types CEs. It serves several purposes:

      - collects general CE related parameters to generate CE description
        for the job matching
      - provides logic for evaluation of the number of available CPU slots
      - provides logic for the proxy renewal while executing jobs

     The CE parameters are collected from the following sources, in hierarchy
     descending order:

      - parameters provided through setParameters() method of the class
      - parameters in /LocalSite configuration section
      - parameters in /LocalSite/<ceName>/ResourceDict configuration section
      - parameters in /LocalSite/ResourceDict configuration section
      - parameters in /LocalSite/<ceName> configuration section
      - parameters in /Resources/Computing/<ceName> configuration section
      - parameters in /Resources/Computing/CEDefaults configuration section

     The ComputingElement objects are usually instantiated with the help of
     ComputingElementFactory.

     The ComputingElement class can be considered abstract. 3 kinds of abstract ComputingElements
     can be distinguished from it:

      - Remote ComputingElement: includes methods to interact with a remote ComputingElement
        (e.g. HtCondorCEComputingElement, AREXComputingElement).
      - Inner ComputingElement: includes methods to locally interact with an underlying worker node.
        It is worth noting that an Inner ComputingElement provides synchronous submission
        (the submission of a job is blocking the execution until its completion). It deals with one job at a time.
      - Inner Pool ComputingElement: includes methods to locally interact with Inner ComputingElements asynchronously.
        It can manage a pool of jobs running simultaneously.

     To configure the use of Tokens for CEs:

     * the CE is able to receive any token. Validation: 'Tag = Token' should be included in the CE parameters.
     * the CE is able to receive VO-specifc tokens. Validation: 'Tag = Token:<VO>' should be included in the CE parameters.

"""

import os

from DIRAC import S_ERROR, S_OK, gLogger, version
from DIRAC.ConfigurationSystem.Client.Config import gConfig
from DIRAC.Core.Security import Properties
from DIRAC.Core.Security.ProxyFile import writeToProxyFile
from DIRAC.Core.Security.ProxyInfo import formatProxyInfoAsString, getProxyInfo, getProxyInfoAsString
from DIRAC.Core.Utilities.ObjectLoader import ObjectLoader
from DIRAC.FrameworkSystem.Client.ProxyManagerClient import gProxyManager
from DIRAC.WorkloadManagementSystem.Utilities.JobParameters import (
    getNumberOfGPUs,
    getNumberOfProcessors,
)

INTEGER_PARAMETERS = ["CPUTime", "NumberOfProcessors", "NumberOfPayloadProcessors", "MaxRAM"]
FLOAT_PARAMETERS = ["WaitingToRunningRatio"]
LIST_PARAMETERS = ["Tag", "RequiredTag"]
WAITING_TO_RUNNING_RATIO = 0.5
MAX_WAITING_JOBS = 1
MAX_TOTAL_JOBS = 1


class ComputingElement:
    """ComputingElement base class"""

    #############################################################################

    def __init__(self, ceName):
        """Standard constructor"""
        self.log = gLogger.getSubLogger(ceName)
        self.ceName = ceName
        self.ceParameters = {}
        self.mandatoryParameters = []

        # Token audience
        # None by default, it needs to be redefined in subclasses
        self.audienceName = None
        self.token = None

        self.proxy = ""
        self.minProxyTime = gConfig.getValue("/Registry/MinProxyLifeTime", 10800)  # secs
        self.defaultProxyTime = gConfig.getValue("/Registry/DefaultProxyLifeTime", 43200)  # secs
        self.proxyCheckPeriod = gConfig.getValue("/Registry/ProxyCheckingPeriod", 3600)  # secs

        self.batchSystem = None
        self.taskResults = {}

        clsName = self.__class__.__name__
        if clsName.endswith("ComputingElement"):
            self.ceType = clsName[: -len("ComputingElement")]
        else:
            self.log.warning(f"{clsName} should end with 'ComputingElement'!")
            self.ceType = clsName

        self.initializeParameters()
        self.log.debug("CE parameters", self.ceParameters)

    def setProxy(self, proxy):
        """Set proxy for this instance"""
        self.proxy = proxy

    def setToken(self, token):
        self.token = token

    def _prepareProxy(self):
        """Set the environment variable X509_USER_PROXY"""
        if self.proxy:
            result = gProxyManager.dumpProxyToFile(self.proxy, requiredTimeLeft=self.minProxyTime, includeToken=False)
            if not result["OK"]:
                return result
            os.environ["X509_USER_PROXY"] = result["Value"]

            self.log.debug(f"Set proxy variable X509_USER_PROXY to {os.environ['X509_USER_PROXY']}")
        return S_OK()

    def initializeParameters(self):
        """Initialize the CE parameters after they are collected from various sources"""

        self.log.debug("Initializing the CE parameters")

        # Collect global defaults first:
        # - /Resources/Computing/CEDefaults and /Resources/Computing/<CEType>
        # Then the local CE configuration:
        # - /LocalSite/<CEName>
        # Finally the site level parameters
        # - /LocalSite
        for section in [
            "/Resources/Computing/CEDefaults",
            f"/Resources/Computing/{self.ceType}",
            f"/LocalSite/{self.ceName}",
            "/LocalSite",
        ]:
            ceParameters = getCEConfigDict(section)

            # List parameters cannot be updated as any other fields, they should be concatenated in a set(), not overriden
            for listParam in LIST_PARAMETERS:
                # If listParam is not present or null, we remove it from ceParameters and continue
                if listParam not in ceParameters or not ceParameters[listParam]:
                    ceParameters.pop(listParam, [])
                    continue
                # Initialize self.ceParameters[listParam] is not done and update the set
                if listParam not in self.ceParameters:
                    self.ceParameters[listParam] = set()
                self.ceParameters[listParam].update(set(ceParameters.pop(listParam)))

            self.log.debug(f"CE Parameters from {section}:", ceParameters)
            self.ceParameters.update(ceParameters)

        # Site level adjustments
        if "Architecture" in self.ceParameters:
            self.ceParameters["Platform"] = self.ceParameters["Architecture"]
        if "LocalSE" in self.ceParameters:
            self.ceParameters["LocalSE"] = self.ceParameters["LocalSE"].split(", ")

        # Add default values if required
        self._addCEConfigDefaults()

    def isValid(self):
        """Check the sanity of the Computing Element definition"""
        for par in self.mandatoryParameters:
            if par not in self.ceParameters:
                return S_ERROR(f"Missing Mandatory Parameter in Configuration: {par}")
        return S_OK()

    #############################################################################
    def _addCEConfigDefaults(self):
        """Method to make sure all necessary Configuration Parameters are defined"""
        self.ceParameters["WaitingToRunningRatio"] = float(
            self.ceParameters.get("WaitingToRunningRatio", WAITING_TO_RUNNING_RATIO)
        )
        self.ceParameters["MaxWaitingJobs"] = int(self.ceParameters.get("MaxWaitingJobs", MAX_WAITING_JOBS))
        self.ceParameters["MaxTotalJobs"] = int(self.ceParameters.get("MaxTotalJobs", MAX_TOTAL_JOBS))

    def _reset(self):
        """Make specific CE parameter adjustments after they are collected or added"""
        return S_OK()

    def loadBatchSystem(self, batchSystemName):
        """Instantiate object representing the backend batch system

        :param str batchSystemName: name of the batch system
        """
        if batchSystemName is None:
            batchSystemName = self.ceParameters["BatchSystem"]

        objectLoader = ObjectLoader()
        result = objectLoader.loadObject(f"Resources.Computing.BatchSystems.{batchSystemName}", batchSystemName)
        if not result["OK"]:
            self.log.error(f"Failed to load batch object: {result['Message']}")
            return result
        batchClass = result["Value"]
        batchModuleFile = result["ModuleFile"]
        self.batchSystem = batchClass()
        self.log.info("Batch system class from module: ", batchModuleFile)
        return S_OK()

    def setParameters(self, ceOptions):
        """Add parameters from the given dictionary overriding the previous values

        :param dict ceOptions: CE parameters dictionary to update already defined ones
        """
        self.ceParameters.update(ceOptions)

        # At this point we can know the exact type of CE,
        # try to get generic parameters for this type
        ceType = self.ceParameters.get("CEType")
        if ceType:
            result = gConfig.getOptionsDict(f"/Resources/Computing/{ceType}")
            if result["OK"]:
                generalCEDict = result["Value"]
                generalCEDict.update(self.ceParameters)
                self.ceParameters = generalCEDict

        # If NumberOfProcessors/GPUs is present in the description but is equal to zero
        # interpret it as needing local evaluation
        if self.ceParameters.get("NumberOfProcessors", -1) == 0:
            self.ceParameters["NumberOfProcessors"] = getNumberOfProcessors()
        if self.ceParameters.get("NumberOfGPUs", -1) == 0:
            self.ceParameters["NumberOfGPUs"] = getNumberOfGPUs()

        for key in ceOptions:
            if key in INTEGER_PARAMETERS:
                self.ceParameters[key] = int(self.ceParameters[key])
            if key in FLOAT_PARAMETERS:
                self.ceParameters[key] = float(self.ceParameters[key])

        return self._reset()

    #############################################################################
    def setCPUTimeLeft(self, cpuTimeLeft=None):
        """Update the CPUTime parameter of the CE classAd, necessary for running in filling mode"""
        if not cpuTimeLeft:
            # do nothing
            return S_OK()
        try:
            intCPUTimeLeft = int(cpuTimeLeft)
            self.ceParameters["CPUTime"] = intCPUTimeLeft
            return S_OK(intCPUTimeLeft)
        except ValueError:
            return S_ERROR("Wrong type for setCPUTimeLeft argument")

    #############################################################################
    def available(self):
        """This method returns the number of available slots in the target CE. The CE
        instance polls for waiting and running jobs and compares to the limits
        in the CE parameters.
        """
        result = self.getCEStatus()
        if not result["OK"]:
            return result

        runningJobs = result["RunningJobs"]
        waitingJobs = result["WaitingJobs"]
        availableProcessors = result.get("AvailableProcessors")
        ceInfoDict = dict(result)

        maxTotalJobs = int(self.ceParameters.get("MaxTotalJobs", 0))
        ceInfoDict["MaxTotalJobs"] = maxTotalJobs
        maxWaitingJobs = int(self.ceParameters.get("MaxWaitingJobs", 0))
        ceInfoDict["MaxWaitingJobs"] = maxWaitingJobs

        self.log.verbose("Max Number of Jobs:", maxTotalJobs)
        self.log.verbose("Max Waiting Jobs:", maxWaitingJobs)

        result["CEInfoDict"] = ceInfoDict
        # If we reached the maximum number of total jobs, then the CE is not available
        totalJobs = runningJobs + waitingJobs
        if totalJobs >= maxTotalJobs:
            self.log.verbose("Max Number of Jobs reached:", f"{totalJobs} >= {maxTotalJobs}")
            result["Value"] = 0
            return result

        # If we reached the maximum number of waiting jobs, then the CE is not available
        if waitingJobs >= maxWaitingJobs:
            self.log.verbose("Max Number of waiting jobs reached:", f"{waitingJobs} >= {maxWaitingJobs}")
            result["Value"] = 0
            return result

        additionalJobs = maxWaitingJobs - waitingJobs
        if totalJobs + additionalJobs >= maxTotalJobs:
            additionalJobs = maxTotalJobs - totalJobs

        if availableProcessors is not None:
            additionalJobs = min(additionalJobs, availableProcessors)
        result["Value"] = additionalJobs
        return result

    #############################################################################
    def writeProxyToFile(self, proxy):
        """CE helper function to write a CE proxy string to a file."""
        result = writeToProxyFile(proxy)
        if not result["OK"]:
            self.log.error("Could not write proxy to file", result["Message"])
            return result

        proxyLocation = result["Value"]
        result = getProxyInfoAsString(proxyLocation)
        if not result["OK"]:
            self.log.error("Could not get proxy info", result)
            return result
        else:
            self.log.info("Payload proxy information:")
            print(result["Value"])

        return S_OK(proxyLocation)

    #############################################################################
    def _monitorProxy(self, payloadProxy=None):
        """Base class for the monitor and update of the payload proxy, to be used in
        derived classes for the basic renewal of the proxy, if further actions are
        necessary they should be implemented there

        :param str payloadProxy: location of the payload proxy file

        :returns: S_OK(filename)/S_ERROR
        """
        if not payloadProxy:
            return S_ERROR("No payload proxy")

        # This will get the pilot proxy
        ret = getProxyInfo()
        if not ret["OK"]:
            pilotProxy = None
        else:
            pilotProxy = ret["Value"]["path"]
            self.log.notice("Pilot Proxy:", pilotProxy)

        retVal = getProxyInfo(payloadProxy)
        if not retVal["OK"]:
            self.log.error("Could not get payload proxy info", retVal)
            return retVal
        self.log.verbose(f"Payload Proxy information:\n{formatProxyInfoAsString(retVal['Value'])}")

        payloadProxyDict = retVal["Value"]
        payloadSecs = payloadProxyDict["chain"].getRemainingSecs()["Value"]
        if payloadSecs > self.minProxyTime:
            self.log.verbose("No need to renew payload Proxy")
            return S_OK()

        # if there is no pilot proxy, assume there is a certificate and try a renewal
        if not pilotProxy:
            self.log.info("Using default credentials to get a new payload Proxy")
            return gProxyManager.renewProxy(
                proxyToBeRenewed=payloadProxy,
                minLifeTime=self.minProxyTime,
                newProxyLifeTime=self.defaultProxyTime,
                proxyToConnect=pilotProxy,
            )

        # if there is pilot proxy
        retVal = getProxyInfo(pilotProxy)
        if not retVal["OK"]:
            return retVal
        pilotProxyDict = retVal["Value"]

        if "groupProperties" not in pilotProxyDict:
            self.log.error("Invalid Pilot Proxy", "Group has no properties defined")
            return S_ERROR("Proxy has no group properties defined")

        pilotProps = pilotProxyDict["groupProperties"]

        # if running with a pilot proxy, use it to renew the proxy of the payload
        if Properties.GENERIC_PILOT in pilotProps:
            self.log.info("Using Pilot credentials to get a new payload Proxy")
            return gProxyManager.renewProxy(
                proxyToBeRenewed=payloadProxy,
                minLifeTime=self.minProxyTime,
                newProxyLifeTime=self.defaultProxyTime,
                proxyToConnect=pilotProxy,
            )

        return S_ERROR("Payload proxy can not be renewed")

    def getDescription(self):
        """Get CE description as a dictionary.

        This is called by the JobAgent for the case of "inner" CEs.
        """

        ceDict = {}
        for option, value in self.ceParameters.items():
            if isinstance(value, list):
                ceDict[option] = value
            elif isinstance(value, set):
                ceDict[option] = list(value)
            elif isinstance(value, str):
                try:
                    ceDict[option] = int(value)
                except ValueError:
                    ceDict[option] = value
            elif isinstance(value, (int, float)):
                ceDict[option] = value
            else:
                self.log.warn(f"Type of option {option} = {value} not determined")

        release = gConfig.getValue("/LocalSite/ReleaseVersion", version)
        ceDict["DIRACVersion"] = release
        ceDict["ReleaseVersion"] = release
        project = gConfig.getValue("/LocalSite/ReleaseProject", "")
        if project:
            ceDict["ReleaseProject"] = project

        # the getCEStatus is implemented in each of the specific CE classes
        result = self.getCEStatus()
        if result["OK"]:
            ceDict["NumberOfProcessors"] = result.get("AvailableProcessors", result.get("NumberOfProcessors", 1))
        else:
            self.log.error(
                "Failure getting CE status", "(we keep going without the number of waiting and running pilots/jobs)"
            )

        return S_OK(ceDict)

    #############################################################################
    def sendOutput(self, stdid, line):  # pylint: disable=unused-argument, no-self-use
        """Callback function such that the results from the CE may be returned."""
        print(line)

    #############################################################################
    def submitJob(self, executableFile, proxy, **kwargs):  # pylint: disable=unused-argument
        """Method to submit job, should be overridden in sub-class."""
        name = "submitJob()"
        self.log.error("ComputingElement should be implemented in a subclass", name)
        return S_ERROR(f"ComputingElement: {name} should be implemented in a subclass")

    #############################################################################
    def getCEStatus(self):
        """Method to get dynamic job information, can be overridden in sub-class."""
        name = "getCEStatus()"
        self.log.error("ComputingElement should be implemented in a subclass", name)
        return S_ERROR(f"ComputingElement: {name} should be implemented in a subclass")

    #############################################################################
    def shutdown(self):
        """Optional method to shutdown the (Inner) Computing Element"""
        return S_OK(self.taskResults)


def getCEConfigDict(section: str) -> dict:
    """Look into section for configuration Parameters for this CE

    :param section: name of the CFG section to exploit
    """

    result = gConfig.getOptionsDict(section)

    if not result["OK"]:
        return {}

    ceOptions = result["Value"]
    for key in ceOptions:
        if key in INTEGER_PARAMETERS:
            ceOptions[key] = int(ceOptions[key])
        if key in FLOAT_PARAMETERS:
            ceOptions[key] = float(ceOptions[key])
        if key in LIST_PARAMETERS:
            ceOptions[key] = gConfig.getValue(os.path.join(section, key), [])

    return ceOptions
