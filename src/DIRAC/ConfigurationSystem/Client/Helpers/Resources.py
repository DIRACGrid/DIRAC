""" Helper for the CS Resources section
"""
from urllib import parse

from DIRAC import S_ERROR, S_OK, gConfig, gLogger
from DIRAC.ConfigurationSystem.Client.Helpers.Path import cfgPath
from DIRAC.Core.Utilities.List import fromChar, uniqueElements
from DIRACCommon.ConfigurationSystem.Client.Helpers.Resources import (
    getDIRACPlatform as _getDIRACPlatform,
    _platformSortKey,
)

gBaseResourcesSection = "/Resources"


def getSites():
    """Get the list of all the sites defined in the CS"""
    result = gConfig.getSections(cfgPath(gBaseResourcesSection, "Sites"))
    if not result["OK"]:
        return result
    grids = result["Value"]
    sites = []
    for grid in grids:
        result = gConfig.getSections(cfgPath(gBaseResourcesSection, "Sites", grid))
        if not result["OK"]:
            return result
        sites += result["Value"]

    return S_OK(sites)


def getSiteCEMapping():
    """:returns: dict of site: list of CEs"""
    res = getSites()
    if not res["OK"]:
        return res
    sites = res["Value"]
    sitesCEsMapping = {}
    for site in sites:
        res = sitesCEsMapping[site] = gConfig.getSections(
            cfgPath(gBaseResourcesSection, "Sites", site.split(".")[0], site, "CEs"), []
        )
        if not res["OK"]:
            gLogger.error("Wrong configuration of CE for site:", site)
            continue
        sitesCEsMapping[site] = res["Value"]
    return S_OK(sitesCEsMapping)


def getCESiteMapping(ceName=""):
    """Returns a mapping of CE -> site
    It assumes that the ce names are unique (as they should be)

    :param str ceName: optional CE name. If not present, the whole mapping will be returned
    :return: S_OK/S_ERROR structure
    """
    res = getSiteCEMapping()
    if not res["OK"]:
        return res
    sitesCEs = res["Value"]
    ceSiteMapping = {}
    for site in sitesCEs:
        for ce in sitesCEs[site]:
            if ceName:
                if ce != ceName:
                    continue
            ceSiteMapping[ce] = site
    return S_OK(ceSiteMapping)


def getGOCSiteName(diracSiteName):
    """
    Get GOC DB site name, given the DIRAC site name, as it is stored in the CS

    :param str diracSiteName: DIRAC site name (e.g. 'LCG.CERN.ch')
    :returns: S_OK/S_ERROR structure
    """
    gocDBName = gConfig.getValue(
        cfgPath(gBaseResourcesSection, "Sites", diracSiteName.split(".")[0], diracSiteName, "Name")
    )
    if not gocDBName:
        return S_ERROR(f"No GOC site name for {diracSiteName} in CS (Not a grid site ?)")
    return S_OK(gocDBName)


def getGOCSites(diracSites=None):
    if diracSites is None:
        diracSites = getSites()
        if not diracSites["OK"]:
            return diracSites
        diracSites = diracSites["Value"]

    gocSites = []

    for diracSite in diracSites:
        gocSite = getGOCSiteName(diracSite)
        if not gocSite["OK"]:
            continue
        gocSites.append(gocSite["Value"])

    return S_OK(list(set(gocSites)))


def getDIRACSiteName(gocSiteName):
    """
    Get DIRAC site name, given the GOC DB site name, as it stored in the CS

    :params str gocSiteName: GOC DB site name (e.g. 'CERN-PROD')
    :returns: S_OK/S_ERROR structure
    """
    res = getSites()
    if not res["OK"]:
        return res
    sitesList = res["Value"]

    tmpList = [
        (site, gConfig.getValue(cfgPath(gBaseResourcesSection, "Sites", site.split(".")[0], site, "Name")))
        for site in sitesList
    ]

    diracSites = [dirac for (dirac, goc) in tmpList if goc == gocSiteName]

    if diracSites:
        return S_OK(diracSites)

    return S_ERROR(f"There's no site with GOCDB name = {gocSiteName} in DIRAC CS")


def getGOCFTSName(diracFTSName):
    """
    Get GOC DB FTS server URL, given the DIRAC FTS server name, as it stored in the CS

    :param str diracFTSName: DIRAC FTS server name (e.g. 'CERN-FTS3')
    :returns: S_OK/S_ERROR structure
    """

    gocFTSName = gConfig.getValue(cfgPath(gBaseResourcesSection, "FTSEndpoints", "FTS3", diracFTSName))
    if not gocFTSName:
        return S_ERROR(f"No GOC FTS server name for {diracFTSName} in CS (Not a grid site ?)")
    return S_OK(gocFTSName)


def getFTS3Servers(hostOnly=False):
    """get list of FTS3 servers that are in CS

    :param bool hostOnly: flag for stripping down the protocol and ports
    """

    csPath = cfgPath(gBaseResourcesSection, "FTSEndpoints/FTS3")
    # We do it in two times to keep the order
    ftsServerNames = gConfig.getOptions(csPath).get("Value", [])

    ftsServers = []
    for name in ftsServerNames:
        serverPath = gConfig.getValue(cfgPath(csPath, name))
        if hostOnly:
            serverPath = parse.urlparse(serverPath).hostname
        ftsServers.append(serverPath)

    return S_OK(ftsServers)


def getFTS3ServerDict():
    """:returns: dict of key = server name and value = server url"""
    return gConfig.getOptionsDict(cfgPath(gBaseResourcesSection, "FTSEndpoints/FTS3"))


def getSiteTier(site):
    """
    Return Tier level of the given Site
    """
    result = getSitePath(site)
    if not result["OK"]:
        return result
    sitePath = result["Value"]
    return S_OK(gConfig.getValue(cfgPath(sitePath, "MoUTierLevel"), 2))


def getSitePath(site):
    """
    Return path to the Site section on CS
    """
    result = getSiteGrid(site)
    if not result["OK"]:
        return result
    grid = result["Value"]
    return S_OK(cfgPath(gBaseResourcesSection, "Sites", grid, site))


def getSiteGrid(site):
    """
    Return Grid component from Site Name
    """
    sitetuple = site.split(".")
    if len(sitetuple) != 3:
        return S_ERROR("Wrong Site Name format")
    return S_OK(sitetuple[0])


def getQueue(site, ce, queue):
    """Get parameters of the specified queue"""
    grid = site.split(".")[0]

    # Get CE parameters
    if not (result := gConfig.getOptionsDict(f"/Resources/Sites/{grid}/{site}/CEs/{ce}"))["OK"]:
        return result
    ceDict = result["Value"]

    tags = set(fromChar(ceDict.get("Tag")) or [])
    requiredTags = set(fromChar(ceDict.get("RequiredTag")) or [])

    # Get queue parameters
    if not (result := gConfig.getOptionsDict(f"/Resources/Sites/{grid}/{site}/CEs/{ce}/Queues/{queue}"))["OK"]:
        return result
    queueDict = result["Value"]

    # Union the sets to combine tags and required tags from CE and queue
    tags = tags.union(set(fromChar(queueDict.get("Tag")) or []))
    requiredTags = requiredTags.union(set(fromChar(queueDict.get("RequiredTag")) or []))

    resultDict = {**ceDict, **queueDict}
    if tags:
        resultDict["Tag"] = list(tags)
    if requiredTags:
        resultDict["RequiredTag"] = list(requiredTags)

    resultDict["Queue"] = queue
    return S_OK(resultDict)


def getQueues(siteList=None, ceList=None, ceTypeList=None, community=None, tags=None):
    """Get CE/queue options according to the specified selection

    :param str list siteList: sites to be selected
    :param str list ceList: CEs to be selected
    :param str list ceTypeList: CE types to be selected
    :param str community: selected VO
    :param str list tags: tags required for selection

    :return: S_OK/S_ERROR with Value - dictionary of selected Site/CE/Queue parameters
    """

    result = gConfig.getSections("/Resources/Sites")
    if not result["OK"]:
        return result

    resultDict = {}

    grids = result["Value"]
    for grid in grids:
        result = gConfig.getSections(f"/Resources/Sites/{grid}")
        if not result["OK"]:
            continue
        sites = result["Value"]
        for site in sites:
            if siteList and site not in siteList:
                continue

            siteCEParameters = {}
            result = gConfig.getOptionsDict(f"/Resources/Sites/{grid}/{site}/CEs")
            if result["OK"]:
                siteCEParameters = result["Value"]
            result = gConfig.getSections(f"/Resources/Sites/{grid}/{site}/CEs")
            if not result["OK"]:
                continue
            ces = result["Value"]
            for ce in ces:
                ceTags = gConfig.getValue(f"/Resources/Sites/{grid}/{site}/CEs/{ce}/Tag", [])
                if ceTypeList:
                    ceType = gConfig.getValue(f"/Resources/Sites/{grid}/{site}/CEs/{ce}/CEType", "")
                    if not ceType or ceType not in ceTypeList:
                        continue
                if ceList and ce not in ceList:
                    continue

                ceOptionsDict = dict(siteCEParameters)
                result = gConfig.getOptionsDict(f"/Resources/Sites/{grid}/{site}/CEs/{ce}")
                if not result["OK"]:
                    continue
                ceOptionsDict.update(result["Value"])
                result = gConfig.getSections(f"/Resources/Sites/{grid}/{site}/CEs/{ce}/Queues")
                if not result["OK"]:
                    continue
                queues = result["Value"]
                for queue in queues:
                    if community:
                        # Community can be defined on site, CE or queue level
                        paths = [
                            f"/Resources/Sites/{grid}/{site}/CEs/{ce}/Queues/{queue}/VO",
                            f"/Resources/Sites/{grid}/{site}/CEs/{ce}/VO",
                            f"/Resources/Sites/{grid}/{site}/VO",
                        ]

                        # Try each path in order, stopping when we find a non-empty list
                        for path in paths:
                            comList = gConfig.getValue(path, [])
                            if comList:
                                break

                        # If we found a list and the community is not in it, skip this iteration
                        if comList and community.lower() not in [cl.lower() for cl in comList]:
                            continue

                    if tags:
                        queueTags = gConfig.getValue(f"/Resources/Sites/{grid}/{site}/CEs/{ce}/Queues/{queue}/Tag", [])
                        queueTags = set(ceTags + queueTags)
                        if not queueTags or not set(tags).issubset(queueTags):
                            continue
                    resultDict.setdefault(site, {})
                    resultDict[site].setdefault(ce, ceOptionsDict)
                    resultDict[site][ce].setdefault("Queues", {})
                    result = gConfig.getOptionsDict(f"/Resources/Sites/{grid}/{site}/CEs/{ce}/Queues/{queue}")
                    if not result["OK"]:
                        continue
                    queueOptionsDict = result["Value"]
                    resultDict[site][ce]["Queues"][queue] = queueOptionsDict

    return S_OK(resultDict)


def getCompatiblePlatforms(originalPlatforms):
    """Get a list of platforms compatible with the given list"""
    if isinstance(originalPlatforms, str):
        platforms = [originalPlatforms]
    else:
        platforms = list(originalPlatforms)

    platforms = list(platform.replace(" ", "") for platform in platforms)

    result = gConfig.getOptionsDict("/Resources/Computing/OSCompatibility")
    if not (result["OK"] and result["Value"]):
        return S_ERROR("OS compatibility info not found")

    platformsDict = {k: v.replace(" ", "").split(",") for k, v in result["Value"].items()}
    for k, v in platformsDict.items():
        if k not in v:
            v.append(k)

    resultList = list(platforms)
    for p in platforms:
        tmpList = platformsDict.get(p, [])
        for pp in platformsDict:
            if p in platformsDict[pp]:
                tmpList.append(pp)
                tmpList += platformsDict[pp]
        if tmpList:
            resultList += tmpList

    return S_OK(uniqueElements(resultList))


def getDIRACPlatform(OSList):
    """Get standard DIRAC platform(s) compatible with the argument.

    NB: The returned value is a list, ordered by numeric components in the platform.
    In practice the "highest" version (which should be the most "desirable" one is returned first)

    :param list OSList: list of platforms defined by resource providers
    :return: a list of DIRAC platforms that can be specified in job descriptions
    """
    # For backward compatibility allow a single string argument
    osList = OSList
    if isinstance(OSList, str):
        osList = [OSList]

    result = gConfig.getOptionsDict("/Resources/Computing/OSCompatibility")
    if not (result["OK"] and result["Value"]):
        return S_ERROR("OS compatibility info not found")

    platformsDict = {k: set(v.replace(" ", "").split(",")) for k, v in result["Value"].items()}
    for k, v in platformsDict.items():
        if k not in v:
            v.add(k)

    return _getDIRACPlatform(osList, platformsDict)


def getDIRACPlatforms():
    """just returns list of platforms defined in the CS"""
    result = gConfig.getOptionsDict("/Resources/Computing/OSCompatibility")
    if not (result["OK"] and result["Value"]):
        return S_ERROR("OS compatibility info not found")
    return S_OK(list(result["Value"]))


def getCatalogPath(catalogName):
    """Return the configuration path of the description for a a given catalog"""
    return f"/Resources/FileCatalogs/{catalogName}"


def getBackendConfig(backendID):
    """Return a backend configuration for a given backend identifier

    :params backendID: string representing a backend identifier. Ex: stdout, file, f02
    """
    return gConfig.getOptionsDict(f"Resources/LogBackends/{backendID}")


def getFilterConfig(filterID):
    """Return a filter configuration for a given filter identifier.

    :params filterID: string representing a filter identifier.
    """
    return gConfig.getOptionsDict(f"Resources/LogFilters/{filterID}")


def getInfoAboutProviders(of=None, providerName=None, option="", section=""):
    """Get the information about providers

    :param str of: provider of what(Id, Proxy or etc.) need to look,
           None, "all" to get list of instance of what this providers
    :param str providerName: provider name,
           None, "all" to get list of providers names
    :param str option: option name that need to get,
           None, "all" to get all options in a section
    :param str section: section path in root section of provider,
           "all" to get options in all sections

    :return: S_OK()/S_ERROR()
    """
    if not of or of == "all":
        result = gConfig.getSections(gBaseResourcesSection)
        if not result["OK"]:
            return result
        return S_OK([i.replace("Providers", "") for i in result["Value"]])
    if not providerName or providerName == "all":
        return gConfig.getSections(f"{gBaseResourcesSection}/{of}Providers")
    if not option or option == "all":
        if not section:
            return gConfig.getOptionsDict(f"{gBaseResourcesSection}/{of}Providers/{providerName}")
        elif section == "all":
            resDict = {}
            relPath = f"{gBaseResourcesSection}/{of}Providers/{providerName}/"
            result = gConfig.getConfigurationTree(relPath)
            if not result["OK"]:
                return result
            for key, value in result["Value"].items():
                if value:
                    resDict[key.replace(relPath, "")] = value
            return S_OK(resDict)
        else:
            return gConfig.getSections(f"{gBaseResourcesSection}/{of}Providers/{providerName}/{section}/")
    else:
        return S_OK(gConfig.getValue(f"{gBaseResourcesSection}/{of}Providers/{providerName}/{section}/{option}"))
