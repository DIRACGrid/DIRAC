"""
Module containing functions interacting with the CS and useful for the RSS
modules.
"""

from DIRAC import S_OK, gConfig, gLogger
from DIRAC.ConfigurationSystem.Client.Helpers.Resources import getQueues
from DIRAC.Core.Utilities.SiteSEMapping import getSEParameters
from DIRAC.DataManagementSystem.Utilities.DMSHelpers import DMSHelpers


def warmUp():
    """
    gConfig has its own dark side, it needs some warm up phase.
    """
    from DIRAC.ConfigurationSystem.private.Refresher import gRefresher

    gRefresher.refreshConfigurationIfNeeded()


def getStorageElementEndpoint(seName):
    """Get endpoints of a StorageElement

    :param str seName: name of the storage element

    :returns: S_OK() or S_ERROR
              for historical reasons, if the protocol is SRM, you get  'httpg://host:port/WSUrl'
              For other protocols, you get :py:meth:`~DIRAC.Resources.Storage.StorageBase.StorageBase.getEndpoint`

    """
    seParameters = getSEParameters(seName)
    if not seParameters["OK"]:
        gLogger.warn("Could not get SE parameters", f"for SE {seName}")
        return seParameters

    seEndpoints = []

    for parameters in seParameters["Value"]:
        if parameters["Protocol"].lower() == "srm":
            # we need to construct the URL with httpg://
            host = parameters["Host"]
            port = parameters["Port"]
            wsurl = parameters["WSUrl"]
            # MAYBE wusrl is not defined
            if host and port:
                url = f"httpg://{host}:{port}{wsurl}"
                url = url.replace("?SFN=", "")
                seEndpoints.append(url)
        else:
            seEndpoints.append(parameters["Endpoint"])

    return S_OK(seEndpoints)


def getFTS():
    """
    Gets all FTS endpoints
    """

    result = gConfig.getOptions("Resources/FTSEndpoints/FTS3")
    if result["OK"]:
        return result
    return S_OK([])


def getFileCatalogs():
    """
    Gets all storage elements from /Resources/FileCatalogs
    """

    _basePath = "Resources/FileCatalogs"

    fileCatalogs = gConfig.getSections(_basePath)
    return fileCatalogs


def getSiteElements(siteName):
    """
    Gets all the computing and storage elements for a given site
    """

    res = DMSHelpers().getSiteSEMapping()
    if not res["OK"]:
        return res
    resources = res["Value"][1].get(siteName, [])

    res = getQueues(siteName)
    if not res["OK"]:
        return res
    resources = list(resources) + list(res["Value"].get(siteName, []))

    return S_OK(resources)


def getQueuesRSS():
    """
    Gets all queues from /Resources/Sites/<>/<>/CEs/<>/Queues
    """
    _basePath = "Resources/Sites"

    queues = []

    domainNames = gConfig.getSections(_basePath)
    if not domainNames["OK"]:
        return domainNames
    domainNames = domainNames["Value"]

    for domainName in domainNames:
        domainSites = gConfig.getSections(f"{_basePath}/{domainName}")
        if not domainSites["OK"]:
            return domainSites
        domainSites = domainSites["Value"]

        for site in domainSites:
            siteCEs = gConfig.getSections(f"{_basePath}/{domainName}/{site}/CEs")
            if not siteCEs["OK"]:
                # return siteCEs
                gLogger.error(siteCEs["Message"])
                continue
            siteCEs = siteCEs["Value"]

            for siteCE in siteCEs:
                siteQueue = gConfig.getSections(f"{_basePath}/{domainName}/{site}/CEs/{siteCE}/Queues")
                if not siteQueue["OK"]:
                    # return siteQueue
                    gLogger.error(siteQueue["Message"])
                    continue
                siteQueue = siteQueue["Value"]

                queues.extend(siteQueue)

    # Remove duplicated ( just in case )
    queues = list(set(queues))

    return S_OK(queues)
