"""
This module contains helper methods for accessing operational attributes or parameters of DMS objects

"""

from collections import defaultdict
from DIRAC import gConfig, gLogger, S_OK, S_ERROR
from DIRAC.ConfigurationSystem.Client.Helpers.Path import cfgPath
from DIRAC.ConfigurationSystem.Client.Helpers.Operations import Operations

LOCAL = 1
PROTOCOL = LOCAL + 1
DOWNLOAD = PROTOCOL + 1

sLog = gLogger.getSubLogger(__name__)


def resolveSEGroup(seGroupList, allSEs=None):
    """
    Resolves recursively a (list of) SEs that can be groupSEs
    For modules, JobWrapper or whatever runs at a given site,
    prefer using :py:func:`~DIRAC.DataManagementSystem.Utilities.ResolveSE.getDestinationSEList`


    :param seGroupList: list of SEs to resolve or comma-separated SEs
    :type seGroupList: list or string
    :param allSEs: if provided, list of all known SEs
    :type allSEs: list

    :return: list of resolved SEs or [] if error
    """
    if allSEs is None:
        res = gConfig.getSections("/Resources/StorageElements")
        if not res["OK"]:
            sLog.fatal("Error getting list of SEs from CS", res["Message"])
            return []
        allSEs = res["Value"]
    seList = []
    if isinstance(seGroupList, str):
        seGroupList = [se.strip() for se in seGroupList.split(",") if se.strip()]
    for se in seGroupList:
        seConfig = gConfig.getValue(f"/Resources/StorageElementGroups/{se}", se)
        if seConfig != se:
            newSEs = [se1.strip() for se1 in seConfig.split(",") if se1.strip()]
            # print seList
        else:
            newSEs = [se]
        for se1 in list(newSEs):
            if se1 not in allSEs:
                # Here means se is not a group and is not an SE either, fatal!
                if se1 == se:
                    sLog.fatal(f"{se1} is not a valid SE")
                    return []
                # If not an SE, it may be a group
                recursive = resolveSEGroup(se1, allSEs=allSEs)
                if not recursive:
                    return []
                newSEs.remove(se1)
                newSEs += recursive
        seList += newSEs

    return seList


def siteGridName(site):
    """Returns the Grid name for a site"""
    if not isinstance(site, str):
        return None
    siteSplit = site.split(".")
    if len(siteSplit) < 3:
        return None
    return siteSplit[0]


def siteCountryName(site):
    """Returns the Grid name for a site"""
    if not isinstance(site, str):
        return None
    siteSplit = site.split(".")
    if len(siteSplit) < 3:
        return None
    return site.split(".")[-1].lower()


def _getConnectionIndex(connectionLevel, default=None):
    """Converts a litteral connectivity to an integer value"""
    if connectionLevel is None:
        connectionLevel = default
    if isinstance(connectionLevel, int):
        return connectionLevel
    if isinstance(connectionLevel, str):
        connectionLevel = connectionLevel.upper()
    return {"LOCAL": LOCAL, "PROTOCOL": PROTOCOL, "DOWNLOAD": DOWNLOAD}.get(connectionLevel)


class DMSHelpers:
    """
    This class is used to get information about sites, SEs and their interrelations
    """

    def __init__(self, vo=False):
        self.siteSEMapping = {}
        self.storageElementSet = set()
        self.siteSet = set()
        self.__opsHelper = Operations(vo=vo)
        self.failoverSEs = None
        self.archiveSEs = None
        self.notForJobSEs = None

    def getSiteSEMapping(self):
        """Returns a dictionary of all sites and their localSEs as a list, e.g.
        {'LCG.CERN.ch':['CERN-RAW','CERN-RDST',...]}
        """
        if self.siteSEMapping:
            return S_OK(self.siteSEMapping)

        # Get the list of SEs and keep a mapping of those using an Alias or a
        # BaseSE
        storageElements = gConfig.getSections("Resources/StorageElements")
        if not storageElements["OK"]:
            sLog.warn("Problem retrieving storage elements", storageElements["Message"])
            return storageElements
        storageElements = storageElements["Value"]
        equivalentSEs = {}
        for se in storageElements:
            for option in ("BaseSE", "Alias"):
                originalSE = gConfig.getValue(f"Resources/StorageElements/{se}/{option}")
                if originalSE:
                    equivalentSEs.setdefault(originalSE, []).append(se)
                    break

        siteSEMapping = {}
        gridTypes = gConfig.getSections("Resources/Sites/")
        if not gridTypes["OK"]:
            sLog.warn("Problem retrieving sections in /Resources/Sites", gridTypes["Message"])
            return gridTypes

        gridTypes = gridTypes["Value"]

        sLog.debug(f"Grid Types are: {', '.join(gridTypes)}")
        # Get a list of sites and their local SEs
        siteSet = set()
        storageElementSet = set()
        siteSEMapping[LOCAL] = {}
        for grid in gridTypes:
            result = gConfig.getSections(f"/Resources/Sites/{grid}")
            if not result["OK"]:
                sLog.warn(f"Problem retrieving /Resources/Sites/{grid} section")
                return result
            sites = result["Value"]
            siteSet.update(sites)
            for site in sites:
                candidateSEs = gConfig.getValue(f"/Resources/Sites/{grid}/{site}/SE", [])
                if candidateSEs:
                    candidateSEs += [eqSE for se in candidateSEs for eqSE in equivalentSEs.get(se, [])]
                    siteSEMapping[LOCAL].setdefault(site, set()).update(candidateSEs)
                    storageElementSet.update(candidateSEs)

        # Add Sites from the SiteSEMappingByProtocol in the CS
        siteSEMapping[PROTOCOL] = {}
        cfgLocalSEPath = cfgPath("SiteSEMappingByProtocol")
        result = self.__opsHelper.getOptionsDict(cfgLocalSEPath)
        if result["OK"]:
            sites = result["Value"]
            for site in sites:
                candidates = set(self.__opsHelper.getValue(cfgPath(cfgLocalSEPath, site), []))
                ses = set(resolveSEGroup(candidates - siteSet)) | (candidates & siteSet)
                # If a candidate is a site, then all local SEs are eligible
                for candidate in ses & siteSet:
                    ses.remove(candidate)
                    ses.update(siteSEMapping[LOCAL][candidate])
                siteSEMapping[PROTOCOL].setdefault(site, set()).update(ses)

        # Add Sites from the SiteSEMappingByDownload in the CS, else
        # SiteLocalSEMapping (old convention)
        siteSEMapping[DOWNLOAD] = {}
        cfgLocalSEPath = cfgPath("SiteSEMappingByDownload")
        result = self.__opsHelper.getOptionsDict(cfgLocalSEPath)
        if not result["OK"]:
            cfgLocalSEPath = cfgPath("SiteLocalSEMapping")
            result = self.__opsHelper.getOptionsDict(cfgLocalSEPath)
        if result["OK"]:
            sites = result["Value"]
            for site in sites:
                candidates = set(self.__opsHelper.getValue(cfgPath(cfgLocalSEPath, site), []))
                ses = set(resolveSEGroup(candidates - siteSet)) | (candidates & siteSet)
                # If a candidate is a site, then all local SEs are eligible
                for candidate in ses & siteSet:
                    ses.remove(candidate)
                    ses.update(siteSEMapping[LOCAL][candidate])
                siteSEMapping[DOWNLOAD].setdefault(site, set()).update(ses)

        self.siteSEMapping = siteSEMapping
        # Add storage elements that may not be associated with a site
        result = gConfig.getSections("/Resources/StorageElements")
        if not result["OK"]:
            sLog.warn("Problem retrieving /Resources/StorageElements section", result["Message"])
            return result
        self.storageElementSet = storageElementSet | set(result["Value"])
        self.siteSet = siteSet
        return S_OK(siteSEMapping)

    def getSites(self):
        """Get the list of known sites"""
        self.getSiteSEMapping()
        return sorted(self.siteSet)

    def getTiers(self, withStorage=False, tier=None):
        """Get the list of sites for a given (list of) Tier level"""
        sites = sorted(self.getShortSiteNames(withStorage=withStorage, tier=tier).values())
        if sites and isinstance(sites[0], list):
            # List of lists, flatten it
            sites = [s for sl in sites for s in sl]
        return sites

    def getShortSiteNames(self, withStorage=True, tier=None):
        """Create a directory of short site names pointing to full site names"""
        siteDict = {}
        result = self.getSiteSEMapping()
        if result["OK"]:
            for site in self.siteSEMapping[LOCAL] if withStorage else self.siteSet:
                grid, shortSite, _country = site.split(".")
                if isinstance(tier, int) and (
                    grid != "LCG" or gConfig.getValue(f"/Resources/Sites/{grid}/{site}/MoUTierLevel", 999) != tier
                ):
                    continue
                if isinstance(tier, (list, tuple, dict, set)) and (
                    grid != "LCG" or gConfig.getValue(f"/Resources/Sites/{grid}/{site}/MoUTierLevel", 999) not in tier
                ):
                    continue
                if withStorage or tier is not None:
                    siteDict[shortSite] = site
                else:
                    siteDict.setdefault(shortSite, []).append(site)
        return siteDict

    def getStorageElements(self):
        """Get the list of known SEs"""
        self.getSiteSEMapping()
        return sorted(self.storageElementSet)

    def isSEFailover(self, storageElement):
        """Is this SE a failover SE"""
        if self.failoverSEs is None:
            seList = resolveSEGroup(self.__opsHelper.getValue("DataManagement/SEsUsedForFailover", []))
            self.failoverSEs = resolveSEGroup(seList)
        # FIXME: remove string test at some point
        return storageElement in self.failoverSEs or (
            not self.failoverSEs and isinstance(storageElement, str) and "FAILOVER" in storageElement.upper()
        )

    def isSEForJobs(self, storageElement, checkSE=True):
        """Is this SE suitable for making jobs"""
        if checkSE:
            self.getSiteSEMapping()
            if storageElement not in self.storageElementSet:
                return False
        if self.notForJobSEs is None:
            seList = resolveSEGroup(self.__opsHelper.getValue("DataManagement/SEsNotToBeUsedForJobs", []))
            self.notForJobSEs = resolveSEGroup(seList)
        return storageElement not in self.notForJobSEs

    def isSEArchive(self, storageElement):
        """Is this SE an archive SE"""
        if self.archiveSEs is None:
            seList = resolveSEGroup(self.__opsHelper.getValue("DataManagement/SEsUsedForArchive", []))
            self.archiveSEs = resolveSEGroup(seList)
        # FIXME: remove string test at some point
        return storageElement in self.archiveSEs or (
            not self.archiveSEs and isinstance(storageElement, str) and "ARCHIVE" in storageElement.upper()
        )

    def getSitesForSE(self, storageElement, connectionLevel=None):
        """Get the (list of) sites for a given SE and a given connctivity"""
        connectionIndex = _getConnectionIndex(connectionLevel, default=DOWNLOAD)
        if connectionIndex == LOCAL:
            return self._getLocalSitesForSE(storageElement)
        if connectionIndex == PROTOCOL:
            return self.getProtocolSitesForSE(storageElement)
        if connectionIndex == DOWNLOAD:
            return self.getDownloadSitesForSE(storageElement)
        return S_ERROR("Unknown connection level")

    def getLocalSiteForSE(self, se):
        """Get the site at which the SE is"""
        sites = self._getLocalSitesForSE(se)
        if not sites["OK"]:
            return sites
        if not sites["Value"]:
            return S_OK(None)
        return S_OK(sites["Value"][0])

    def _getLocalSitesForSE(self, se):
        """Extract the list of sites that declare this SE"""
        mapping = self.getSiteSEMapping()
        if not mapping["OK"]:
            return mapping
        if se not in self.storageElementSet:
            return S_ERROR("Non-existing SE")
        mapping = mapping["Value"][LOCAL]
        sites = [site for site in mapping if se in mapping[site]]
        if len(sites) > 1 and self.__opsHelper.getValue("DataManagement/ForceSingleSitePerSE", True):
            return S_ERROR("SE is at more than one site")
        return S_OK(sites)

    def getProtocolSitesForSE(self, se):
        """Get sites that can access the SE by protocol"""
        mapping = self.getSiteSEMapping()
        if not mapping["OK"]:
            return mapping
        if se not in self.storageElementSet:
            return S_ERROR("Non-existing SE")
        mapping = mapping["Value"][PROTOCOL]
        sites = self._getLocalSitesForSE(se)
        if not sites["OK"]:
            return sites
        sites = set(sites["Value"])
        sites.update([site for site in mapping if se in mapping[site]])
        return S_OK(sorted(sites))

    def getDownloadSitesForSE(self, se):
        """Get the list of sites that are allowed to download files"""
        mapping = self.getSiteSEMapping()
        if not mapping["OK"]:
            return mapping
        if se not in self.storageElementSet:
            return S_ERROR("Non-existing SE")
        mapping = mapping["Value"][DOWNLOAD]
        sites = self.getProtocolSitesForSE(se)
        if not sites["OK"]:
            return sites
        sites = set(sites["Value"])
        sites.update([site for site in mapping if se in mapping[site]])
        return S_OK(sorted(sites))

    def getSEsForSite(self, site, connectionLevel=None):
        """Get all SEs accessible from a site, given a connectivity"""
        connectionIndex = _getConnectionIndex(connectionLevel, default=DOWNLOAD)
        if connectionIndex is None:
            return S_ERROR("Unknown connection level")
        if not self.siteSet:
            self.getSiteSEMapping()
        if site not in self.siteSet:
            siteList = [s for s in self.siteSet if f".{site}." in s]
        else:
            siteList = [site]
        if not siteList:
            return S_ERROR("Unknown site")
        return self._getSEsForSItes(siteList, connectionIndex=connectionIndex)

    def _getSEsForSItes(self, siteList, connectionIndex):
        """Extract list of SEs for a connectivity"""
        mapping = self.getSiteSEMapping()
        if not mapping["OK"]:
            return mapping
        ses = []
        for index in range(LOCAL, connectionIndex + 1):
            for site in siteList:
                ses += mapping["Value"][index].get(site, [])
        if not ses:
            return S_ERROR("No SE found")
        return S_OK(sorted(ses))

    def getSEsAtSite(self, site):
        """Get local SEs"""
        return self.getSEsForSite(site, connectionLevel=LOCAL)

    def isSameSiteSE(self, se1, se2):
        """Are these 2 SEs at the same site"""
        res = self.getLocalSiteForSE(se1)
        if not res["OK"]:
            return res
        site1 = res["Value"]
        res = self.getLocalSiteForSE(se2)
        if not res["OK"]:
            return res
        site2 = res["Value"]
        return S_OK(site1 == site2)

    def getSEsAtCountry(self, country, connectionLevel=None):
        """Get all SEs at a given country"""
        connectionIndex = _getConnectionIndex(connectionLevel, default=DOWNLOAD)
        if connectionIndex is None:
            return S_ERROR("Unknown connection level")
        if not self.siteSet:
            self.getSiteSEMapping()
        siteList = [site for site in self.siteSet if siteCountryName(site) == country.lower()]
        if not siteList:
            return S_ERROR("No SEs found in country")
        return self._getSEsForSItes(siteList, connectionIndex)

    def getSEInGroupAtSite(self, seGroup, site):
        """Get the SE in a group or list of SEs that is present at a site"""
        seList = self.getAllSEsInGroupAtSite(seGroup, site)
        if not seList["OK"] or seList["Value"] is None:
            return seList
        return S_OK(seList["Value"][0])

    def getAllSEsInGroupAtSite(self, seGroup, site):
        """Get all SEs in a group or list of SEs that are present at a site"""
        seList = resolveSEGroup(seGroup)
        if not seList:
            return S_ERROR("SEGroup does not exist")
        sesAtSite = self.getSEsAtSite(site)
        if not sesAtSite["OK"]:
            return sesAtSite
        foundSEs = set(seList) & set(sesAtSite["Value"])
        if not foundSEs:
            sLog.verbose("No SE found at that site", f"in group {seGroup} at {site}")
            return S_OK()
        return S_OK(sorted(foundSEs))

    def getRegistrationProtocols(self):
        """Returns the Favorite registration protocol defined in the CS, or 'srm' as default"""
        return self.__opsHelper.getValue("DataManagement/RegistrationProtocols", ["srm", "dips"])

    def getThirdPartyProtocols(self):
        """Returns the Favorite third party protocol defined in the CS, or 'srm' as default"""
        return self.__opsHelper.getValue("DataManagement/ThirdPartyProtocols", ["srm"])

    def getAccessProtocols(self):
        """Returns the Favorite access protocol defined in the CS, or 'srm' as default"""
        return self.__opsHelper.getValue("DataManagement/AccessProtocols", ["srm", "dips"])

    def getWriteProtocols(self):
        """Returns the Favorite Write protocol defined in the CS, or 'srm' as default"""
        return self.__opsHelper.getValue("DataManagement/WriteProtocols", ["srm", "dips"])

    def getStageProtocols(self):
        """Returns the Favorite staging protocol defined in the CS. There are no default"""
        return self.__opsHelper.getValue("DataManagement/StageProtocols", list())

    def getMultiHopMatrix(self):
        """
        Returns the multi-hop matrix described in DataManagement/MultiHopMatrixOfShame.

        .. code-block :: python

                  'Default': { 'Default': 'MultiHopSEUsedForAllTransfer',
                               'Dst3' : 'MultiHopFromAnySourceToDst3',
                              }
                  'Src1' : { 'Default' : 'DefaultMultiHopSEFromSrc1',
                             'Dst1': 'MultiHopSEFromSrc1ToDst1},
                  'Src2' : { 'Default' : 'DefaultMultiHopSEFromSrc2',
                             'Dst2': 'MultiHopSEFromSrc1ToDst2}


        :returns: dict of dict for all the source se / dest SE defined. We user defaultdict
                 to allow for the use of non existing source/dest.

        """
        matrixBasePath = "DataManagement/MultiHopMatrixOfShame"
        multiHopMatrix = defaultdict(lambda: defaultdict(lambda: None))
        allSrcSEs = self.__opsHelper.getSections(matrixBasePath).get("Value", [])
        for src in allSrcSEs:
            srcDst = self.__opsHelper.getOptionsDict(cfgPath(matrixBasePath, src)).get("Value")
            if srcDst:
                multiHopMatrix[src].update(srcDst)

        return multiHopMatrix
