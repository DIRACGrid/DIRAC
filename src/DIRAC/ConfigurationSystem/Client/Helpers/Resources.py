""" Helper for the CS Resources section
"""
import re
from urllib import parse

from DIRAC import S_OK, S_ERROR, gConfig, gLogger
from DIRAC.ConfigurationSystem.Client.Helpers import Registry, Operations
from DIRAC.ConfigurationSystem.Client.Helpers.Path import cfgPath
from DIRAC.Core.Utilities.List import uniqueElements, fromChar
from DIRAC.FrameworkSystem.Client.ProxyManagerClient import gProxyManager


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
        return S_ERROR("No GOC site name for %s in CS (Not a grid site ?)" % diracSiteName)
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

    return S_ERROR("There's no site with GOCDB name = %s in DIRAC CS" % gocSiteName)


def getGOCFTSName(diracFTSName):
    """
    Get GOC DB FTS server URL, given the DIRAC FTS server name, as it stored in the CS

    :param str diracFTSName: DIRAC FTS server name (e.g. 'CERN-FTS3')
    :returns: S_OK/S_ERROR structure
    """

    gocFTSName = gConfig.getValue(cfgPath(gBaseResourcesSection, "FTSEndpoints", "FTS3", diracFTSName))
    if not gocFTSName:
        return S_ERROR("No GOC FTS server name for %s in CS (Not a grid site ?)" % diracFTSName)
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
    result = gConfig.getOptionsDict(f"/Resources/Sites/{grid}/{site}/CEs/{ce}")
    if not result["OK"]:
        return result
    resultDict = result["Value"]

    # Get queue defaults
    result = gConfig.getOptionsDict(f"/Resources/Sites/{grid}/{site}/CEs/{ce}/Queues/{queue}")
    if not result["OK"]:
        return result
    resultDict.update(result["Value"])

    # Handle tag lists for the queue
    for tagFieldName in ("Tag", "RequiredTag"):
        tags = []
        ceTags = resultDict.get(tagFieldName)
        if ceTags:
            tags = fromChar(ceTags)
        queueTags = resultDict.get(tagFieldName)
        if queueTags:
            queueTags = fromChar(queueTags)
            tags = list(set(tags + queueTags))
        if tags:
            resultDict[tagFieldName] = tags

    resultDict["Queue"] = queue
    return S_OK(resultDict)


def getQueues(siteList=None, ceList=None, ceTypeList=None, community=None):
    """Get CE/queue options according to the specified selection"""

    result = gConfig.getSections("/Resources/Sites")
    if not result["OK"]:
        return result

    resultDict = {}

    grids = result["Value"]
    for grid in grids:
        result = gConfig.getSections("/Resources/Sites/%s" % grid)
        if not result["OK"]:
            continue
        sites = result["Value"]
        for site in sites:
            if siteList and site not in siteList:
                continue
            if community:
                comList = gConfig.getValue(f"/Resources/Sites/{grid}/{site}/VO", [])
                if comList and community.lower() not in [cl.lower() for cl in comList]:
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
                if ceTypeList:
                    ceType = gConfig.getValue(f"/Resources/Sites/{grid}/{site}/CEs/{ce}/CEType", "")
                    if not ceType or ceType not in ceTypeList:
                        continue
                if ceList and ce not in ceList:
                    continue
                if community:
                    comList = gConfig.getValue(f"/Resources/Sites/{grid}/{site}/CEs/{ce}/VO", [])
                    if comList and community.lower() not in [cl.lower() for cl in comList]:
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
                        comList = gConfig.getValue(f"/Resources/Sites/{grid}/{site}/CEs/{ce}/Queues/{queue}/VO", [])
                        if comList and community.lower() not in [cl.lower() for cl in comList]:
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

    platformsDict = {k: v.replace(" ", "").split(",") for k, v in result["Value"].items()}  # can be an iterator
    for k, v in platformsDict.items():  # can be an iterator
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

    platformsDict = {k: v.replace(" ", "").split(",") for k, v in result["Value"].items()}  # can be an iterator
    for k, v in platformsDict.items():  # can be an iterator
        if k not in v:
            v.append(k)

    # making an OS -> platforms dict
    os2PlatformDict = dict()
    for platform, osItems in platformsDict.items():  # can be an iterator
        for osItem in osItems:
            if os2PlatformDict.get(osItem):
                os2PlatformDict[osItem].append(platform)
            else:
                os2PlatformDict[osItem] = [platform]

    platforms = []
    for os in osList:
        if os in os2PlatformDict:
            platforms += os2PlatformDict[os]

    if not platforms:
        return S_ERROR("No compatible DIRAC platform found for %s" % ",".join(OSList))

    platforms.sort(key=_platformSortKey, reverse=True)

    return S_OK(platforms)


def getDIRACPlatforms():
    """just returns list of platforms defined in the CS"""
    result = gConfig.getOptionsDict("/Resources/Computing/OSCompatibility")
    if not (result["OK"] and result["Value"]):
        return S_ERROR("OS compatibility info not found")
    return S_OK(list(result["Value"]))


def getCatalogPath(catalogName):
    """Return the configuration path of the description for a a given catalog"""
    return "/Resources/FileCatalogs/%s" % catalogName


def getBackendConfig(backendID):
    """Return a backend configuration for a given backend identifier

    :params backendID: string representing a backend identifier. Ex: stdout, file, f02
    """
    return gConfig.getOptionsDict("Resources/LogBackends/%s" % backendID)


def getFilterConfig(filterID):
    """Return a filter configuration for a given filter identifier.

    :params filterID: string representing a filter identifier.
    """
    return gConfig.getOptionsDict("Resources/LogFilters/%s" % filterID)


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
            for key, value in result["Value"].items():  # can be an iterator
                if value:
                    resDict[key.replace(relPath, "")] = value
            return S_OK(resDict)
        else:
            return gConfig.getSections(f"{gBaseResourcesSection}/{of}Providers/{providerName}/{section}/")
    else:
        return S_OK(gConfig.getValue(f"{gBaseResourcesSection}/{of}Providers/{providerName}/{section}/{option}"))


def findGenericCloudCredentials(vo=False, group=False):
    """Get the cloud credentials to use for a specific VO and/or group."""
    if not group and not vo:
        return S_ERROR("Need a group or a VO to determine the Generic cloud credentials")
    if not vo:
        vo = Registry.getVOForGroup(group)
        if not vo:
            return S_ERROR("Group %s does not have a VO associated" % group)
    opsHelper = Operations.Operations(vo=vo)
    cloudGroup = opsHelper.getValue("Cloud/GenericCloudGroup", "")
    cloudDN = opsHelper.getValue("Cloud/GenericCloudDN", "")
    if not cloudDN:
        cloudUser = opsHelper.getValue("Cloud/GenericCloudUser", "")
        if cloudUser:
            result = Registry.getDNForUsername(cloudUser)
            if result["OK"]:
                cloudDN = result["Value"][0]
            else:
                return S_ERROR("Failed to find suitable CloudDN")
    if cloudDN and cloudGroup:
        gLogger.verbose(f"Cloud credentials from CS: {cloudDN}@{cloudGroup}")
        result = gProxyManager.userHasProxy(cloudDN, cloudGroup, 86400)
        if not result["OK"]:
            return result
        return S_OK((cloudDN, cloudGroup))
    return S_ERROR("Cloud credentials not found")


def getVMTypes(siteList=None, ceList=None, vmTypeList=None, vo=None):
    """Get CE/vmType options filtered by the provided parameters."""

    result = gConfig.getSections("/Resources/Sites")
    if not result["OK"]:
        return result

    resultDict = {}

    grids = result["Value"]
    for grid in grids:
        result = gConfig.getSections("/Resources/Sites/%s" % grid)
        if not result["OK"]:
            continue
        sites = result["Value"]
        for site in sites:
            if siteList is not None and site not in siteList:
                continue
            if vo:
                voList = gConfig.getValue(f"/Resources/Sites/{grid}/{site}/VO", [])
                if voList and vo not in voList:
                    continue
            result = gConfig.getSections(f"/Resources/Sites/{grid}/{site}/Cloud")
            if not result["OK"]:
                continue
            ces = result["Value"]
            for ce in ces:
                if ceList is not None and ce not in ceList:
                    continue
                if vo:
                    voList = gConfig.getValue(f"/Resources/Sites/{grid}/{site}/Cloud/{ce}/VO", [])
                    if voList and vo not in voList:
                        continue
                result = gConfig.getOptionsDict(f"/Resources/Sites/{grid}/{site}/Cloud/{ce}")
                if not result["OK"]:
                    continue
                ceOptionsDict = result["Value"]
                result = gConfig.getSections(f"/Resources/Sites/{grid}/{site}/Cloud/{ce}/VMTypes")
                if not result["OK"]:
                    result = gConfig.getSections(f"/Resources/Sites/{grid}/{site}/Cloud/{ce}/Images")
                    if not result["OK"]:
                        return result
                vmTypes = result["Value"]
                for vmType in vmTypes:
                    if vmTypeList is not None and vmType not in vmTypeList:
                        continue
                    if vo:
                        voList = gConfig.getValue(f"/Resources/Sites/{grid}/{site}/Cloud/{ce}/VMTypes/{vmType}/VO", [])
                        if not voList:
                            voList = gConfig.getValue(
                                f"/Resources/Sites/{grid}/{site}/Cloud/{ce}/Images/{vmType}/VO", []
                            )
                        if voList and vo not in voList:
                            continue
                    resultDict.setdefault(site, {})
                    resultDict[site].setdefault(ce, ceOptionsDict)
                    resultDict[site][ce].setdefault("VMTypes", {})
                    result = gConfig.getOptionsDict(f"/Resources/Sites/{grid}/{site}/Cloud/{ce}/VMTypes/{vmType}")
                    if not result["OK"]:
                        result = gConfig.getOptionsDict(f"/Resources/Sites/{grid}/{site}/Cloud/{ce}/Images/{vmType}")
                        if not result["OK"]:
                            continue
                    vmTypeOptionsDict = result["Value"]
                    resultDict[site][ce]["VMTypes"][vmType] = vmTypeOptionsDict

    return S_OK(resultDict)


def getVMTypeConfig(site, ce="", vmtype=""):
    """Get the VM image type parameters of the specified queue"""
    tags = []
    reqtags = []
    grid = site.split(".")[0]
    if not ce:
        result = gConfig.getSections(f"/Resources/Sites/{grid}/{site}/Cloud")
        if not result["OK"]:
            return result
        ceList = result["Value"]
        if len(ceList) == 1:
            ce = ceList[0]
        else:
            return S_ERROR("No cloud endpoint specified")

    result = gConfig.getOptionsDict(f"/Resources/Sites/{grid}/{site}/Cloud/{ce}")
    if not result["OK"]:
        return result
    resultDict = result["Value"]
    ceTags = resultDict.get("Tag")
    if ceTags:
        tags = fromChar(ceTags)
    ceTags = resultDict.get("RequiredTag")
    if ceTags:
        reqtags = fromChar(ceTags)
    resultDict["CEName"] = ce

    if vmtype:
        result = gConfig.getOptionsDict(f"/Resources/Sites/{grid}/{site}/Cloud/{ce}/VMTypes/{vmtype}")
        if not result["OK"]:
            return result
        resultDict.update(result["Value"])
        queueTags = resultDict.get("Tag")
        if queueTags:
            queueTags = fromChar(queueTags)
            tags = list(set(tags + queueTags))
        queueTags = resultDict.get("RequiredTag")
        if queueTags:
            queueTags = fromChar(queueTags)
            reqtags = list(set(reqtags + queueTags))

    if tags:
        resultDict["Tag"] = tags
    if reqtags:
        resultDict["RequiredTag"] = reqtags
    resultDict["VMType"] = vmtype
    resultDict["Site"] = site
    return S_OK(resultDict)


def getPilotBootstrapParameters(vo="", runningPod=""):
    """Get all of the settings required to bootstrap a cloud instance."""
    op = Operations.Operations(vo=vo)
    result = op.getOptionsDict("Cloud")
    opParameters = {}
    if result["OK"]:
        opParameters = result["Value"]
    opParameters["VO"] = vo
    # FIXME: The majority of these settings can be removed once the old vm-pilot
    #        scripts have been removed.
    opParameters["ReleaseProject"] = op.getValue("Cloud/ReleaseProject", "DIRAC")
    opParameters["ReleaseVersion"] = op.getValue("Cloud/ReleaseVersion", op.getValue("Pilot/Version"))
    opParameters["Setup"] = gConfig.getValue("/DIRAC/Setup", "unknown")
    opParameters["SubmitPool"] = op.getValue("Cloud/SubmitPool")
    opParameters["CloudPilotCert"] = op.getValue("Cloud/CloudPilotCert")
    opParameters["CloudPilotKey"] = op.getValue("Cloud/CloudPilotKey")
    opParameters["pilotFileServer"] = op.getValue("Pilot/pilotFileServer")
    result = op.getOptionsDict("Cloud/%s" % runningPod)
    if result["OK"]:
        opParameters.update(result["Value"])

    # Get standard pilot version now
    if "Version" in opParameters:
        gLogger.warn(
            "Cloud bootstrap version now uses standard Pilot/Version setting. "
            "Please remove all obsolete (Cloud/Version) setting(s)."
        )
    pilotVersions = op.getValue("Pilot/Version")
    if isinstance(pilotVersions, str):
        pilotVersions = [pilotVersions]
    if not pilotVersions:
        return S_ERROR("Failed to get pilot version.")
    opParameters["Version"] = pilotVersions[0].strip()

    return S_OK(opParameters)


def _platformSortKey(version: str) -> list[str]:
    # Loosely based on distutils.version.LooseVersion
    parts = []
    for part in re.split(r"(\d+|[a-z]+|\.| -)", version.lower()):
        if not part or part == ".":
            continue
        if part[:1] in "0123456789":
            part = part.zfill(8)
        else:
            while parts and parts[-1] == "00000000":
                parts.pop()
        parts.append(part)
    return parts
