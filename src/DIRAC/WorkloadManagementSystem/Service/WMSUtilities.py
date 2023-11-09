""" A set of utilities used in the WMS services
    Requires the Nordugrid ARC plugins. In particular : nordugrid-arc-python
"""
from tempfile import mkdtemp
import shutil

from DIRAC import S_OK, S_ERROR, gLogger, gConfig
from DIRAC.ConfigurationSystem.Client.Helpers.Resources import getQueue
from DIRAC.ConfigurationSystem.Client.Helpers.Registry import getGroupOption, getUsernameForDN, getVOForGroup
from DIRAC.FrameworkSystem.Client.ProxyManagerClient import gProxyManager
from DIRAC.FrameworkSystem.Client.TokenManagerClient import gTokenManager
from DIRAC.Resources.Computing.ComputingElementFactory import ComputingElementFactory
from DIRAC.WorkloadManagementSystem.Client.PilotScopes import PILOT_SCOPES


# List of files to be inserted/retrieved into/from pilot Output Sandbox
# first will be defined as StdOut in JDL and the second as StdErr
outputSandboxFiles = ["StdOut", "StdErr"]

COMMAND_TIMEOUT = 60
###########################################################################


def getGridEnv():
    gridEnv = ""
    setup = gConfig.getValue("/DIRAC/Setup", "")
    if setup:
        instance = gConfig.getValue(f"/DIRAC/Setups/{setup}/WorkloadManagement", "")
        if instance:
            gridEnv = gConfig.getValue(f"/Systems/WorkloadManagement/{instance}/GridEnv", "")

    return gridEnv


def getPilotCE(pilotDict):
    """Instantiate and return a CE bound to a pilot"""
    ceFactory = ComputingElementFactory()
    result = getQueue(pilotDict["GridSite"], pilotDict["DestinationSite"], pilotDict["Queue"])
    if not result["OK"]:
        return result
    queueDict = result["Value"]
    gridEnv = getGridEnv()
    queueDict["GridEnv"] = gridEnv
    queueDict["WorkingDirectory"] = mkdtemp()
    result = ceFactory.getCE(pilotDict["GridType"], pilotDict["DestinationSite"], queueDict)
    if not result["OK"]:
        shutil.rmtree(queueDict["WorkingDirectory"])
        return result
    ce = result["Value"]
    return S_OK(ce)


def getPilotProxy(pilotDict):
    """Get a proxy bound to a pilot

    :param dict pilotDict: pilot parameters
    :return: S_OK/S_ERROR with proxy as Value
    """
    owner = pilotDict["OwnerDN"]
    group = pilotDict["OwnerGroup"]

    groupVOMS = getGroupOption(group, "VOMSRole", group)
    result = gProxyManager.getPilotProxyFromVOMSGroup(owner, groupVOMS)
    if not result["OK"]:
        gLogger.error("Could not get proxy:", f"User \"{owner}\" Group \"{groupVOMS}\" : {result['Message']}")
        return S_ERROR("Failed to get the pilot's owner proxy")
    proxy = result["Value"]
    return S_OK(proxy)


def setPilotCredentials(ce, pilotDict):
    """Instrument the given CE with proxy or token

    :param obj ce:  CE object
    :param pilotDict: pilot parameter dictionary
    :return: S_OK/S_ERROR
    """
    vo = getVOForGroup(pilotDict["OwnerGroup"])
    if "Token" in ce.ceParameters.get("Tag", []) or f"Token:{vo}" in ce.ceParameters.get("Tag", []):
        result = gTokenManager.getToken(
            userGroup=pilotDict["OwnerGroup"],
            scope=PILOT_SCOPES,
            audience=ce.audienceName,
            requiredTimeLeft=150,
        )
        if not result["OK"]:
            return result
        ce.setToken(result["Value"])
    else:
        result = getPilotProxy(pilotDict)
        if not result["OK"]:
            return result
        ce.setProxy(result["Value"])
    return S_OK()


def getPilotRef(pilotReference, pilotDict):
    """Add the pilotStamp to the pilotReference, if the pilotStamp is in the dictionary,
    otherwise return unchanged pilotReference.
    """
    pilotStamp = pilotDict["PilotStamp"]
    pRef = pilotReference
    if pilotStamp:
        pRef = pRef + ":::" + pilotStamp
    return S_OK(pRef)


def killPilotsInQueues(pilotRefDict):
    """kill pilots queue by queue

    :params dict pilotRefDict: a dict of pilots in queues
    """

    ceFactory = ComputingElementFactory()

    for key, pilotDict in pilotRefDict.items():
        owner, group, site, ce, queue = key.split("@@@")
        result = getQueue(site, ce, queue)
        if not result["OK"]:
            return result
        queueDict = result["Value"]
        gridType = pilotDict["GridType"]
        result = ceFactory.getCE(gridType, ce, queueDict)
        if not result["OK"]:
            return result
        ce = result["Value"]

        group = getGroupOption(group, "VOMSRole", group)
        ret = gProxyManager.getPilotProxyFromVOMSGroup(owner, group)
        if not ret["OK"]:
            gLogger.error("Could not get proxy:", f"User '{owner}' Group '{group}' : {ret['Message']}")
            return S_ERROR("Failed to get the pilot's owner proxy")
        proxy = ret["Value"]
        ce.setProxy(proxy)

        pilotList = pilotDict["PilotList"]
        result = ce.killJob(pilotList)
        if not result["OK"]:
            return result

    return S_OK()
