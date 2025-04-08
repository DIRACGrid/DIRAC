""" The input data resolution module is a plugin that
    allows to define VO input data policy in a simple way using existing
    utilities in DIRAC or extension code supplied by the VO.

    The arguments dictionary from the Job Wrapper includes the file catalogue
    result and in principle has all the necessary information to resolve input data
    for applications.
"""
from pathlib import Path

import DIRAC
from DIRAC import S_ERROR, S_OK, gConfig, gLogger
from DIRAC.ConfigurationSystem.Client.Helpers.Operations import Operations
from DIRAC.Core.Utilities.ObjectLoader import ObjectLoader
from DIRAC.WorkloadManagementSystem.Client.PoolXMLSlice import PoolXMLSlice

CREATE_CATALOG = False


class InputDataResolution:
    """Defines the Input Data Policy"""

    def __init__(self, argumentsDict):
        """Standard constructor"""
        self.arguments = argumentsDict
        self.log = gLogger.getSubLogger(self.__class__.__name__)
        op = Operations()
        self.arguments.setdefault("Configuration", {})["AllReplicas"] = op.getValue(
            "InputDataPolicy/AllReplicas", False
        )
        self.arguments["Configuration"].setdefault("Protocol", op.getValue("InputDataPolicy/Protocols/Local", []))
        self.arguments["Configuration"].setdefault(
            "RemoteProtocol", op.getValue("InputDataPolicy/Protocols/Remote", [])
        )

        # By default put input data into the current directory
        self.arguments.setdefault("InputDataDirectory", gConfig.getValue("/LocalSite/InputDataDirectory", "CWD"))

    def execute(self):
        """Given the arguments from the Job Wrapper, this function calls existing
        utilities in DIRAC to resolve input data.
        """
        resolvedInputData = self.__resolveInputData()
        if not resolvedInputData["OK"]:
            self.log.error(f"InputData resolution failed with result:\n{resolvedInputData['Message']}")
            return resolvedInputData

        # For local running of this module we can expose an option to ignore missing files
        ignoreMissing = self.arguments.get("IgnoreMissing", False)

        # Missing some of the input files is a fatal error unless ignoreMissing option is defined
        failedReplicas = resolvedInputData["Value"].get("Failed", {})
        if failedReplicas and not ignoreMissing:
            self.log.error("Failed to obtain access to the following files:\n%s" % ("\n".join(sorted(failedReplicas))))
            return S_ERROR("Failed to access some of requested input data")

        if not resolvedInputData["Value"].get("Successful"):
            return S_ERROR("Could not access any requested input data")

        if CREATE_CATALOG:
            res = self._createCatalog(resolvedInputData)
            if not res["OK"]:
                return res

        return resolvedInputData

    def _createCatalog(self, resolvedInputData, catalogName="pool_xml_catalog.xml", pfnType="ROOT"):
        """By default uses PoolXMLSlice, VO extensions can modify at will"""

        resolvedData = resolvedInputData["Successful"]
        tmpDict = {}
        for lfn, mdata in resolvedData.items():
            tmpDict[lfn] = mdata
            tmpDict[lfn]["pfntype"] = pfnType
            self.log.verbose(f"Adding PFN file type {pfnType} for LFN:{lfn}")

        catalogName = self.arguments["Configuration"].get("CatalogName", catalogName)
        self.log.verbose(f"Catalog name will be: {catalogName}")

        resolvedData = tmpDict
        appCatalog = PoolXMLSlice(catalogName, Path(self.arguments["Configuration"].get("JobIDPath", "")))
        return appCatalog.execute(resolvedData)

    def __resolveInputData(self):
        """This method controls the execution of the DIRAC input data modules according
        to the VO policy defined in the configuration service.
        """
        site = self.arguments["Configuration"].get("SiteName", DIRAC.siteName())

        self.arguments.setdefault("Job", {})

        policy = self.arguments["Job"].get("InputDataPolicy", [])
        if policy:
            # In principle this can be a list of modules with the first taking precedence
            if isinstance(policy, str):
                policy = [policy]
            self.log.info(f"Job has a specific policy setting: {', '.join(policy)}")
        else:
            self.log.debug(f"Attempting to resolve input data policy for site {site}")
            inputDataPolicy = Operations().getOptionsDict("InputDataPolicy")
            if not inputDataPolicy["OK"]:
                return S_ERROR("Could not resolve InputDataPolicy from Operations InputDataPolicy")

            options = inputDataPolicy["Value"]
            policy = options.get(site, options.get("Default", []))
            if policy:
                policy = [x.strip() for x in policy.split(",")]
                if site in options:
                    prStr = "Found specific"
                else:
                    prStr = "Applying default"
                self.log.info("{} input data policy for site {}:\n{}".format(prStr, site, "\n".join(policy)))

        dataToResolve = []  # if none, all supplied input data is resolved
        successful = {}
        for modulePath in policy:
            self.log.info(f"Attempting to run {modulePath}")

            result = ObjectLoader().loadObject(modulePath)
            if not result["OK"]:
                return result
            module = result["Value"](self.arguments)

            result = module.execute(dataToResolve)
            if not result["OK"]:
                return result

            result = result["Value"]
            successful.update(result.get("Successful", {}))
            dataToResolve = result.get("Failed", [])
            if dataToResolve:
                self.log.info("{} failed for the following files:\n{}".format(modulePath, "\n".join(dataToResolve)))
            else:
                self.log.info(f"All replicas resolved after {modulePath} execution")
                break

        if successful:
            self.log.verbose("Successfully resolved:", str(successful))

        return S_OK({"Successful": successful, "Failed": dataToResolve})
