""" A set of utilities used in the WMS services
"""
import shutil
from tempfile import mkdtemp

from DIRAC import S_ERROR, S_OK, gLogger
from DIRAC.ConfigurationSystem.Client.Helpers.Operations import Operations
from DIRAC.ConfigurationSystem.Client.Helpers.Registry import (
    getDNForUsername,
    getGroupOption,
)
from DIRAC.ConfigurationSystem.Client.Helpers.Resources import getQueue
from DIRAC.FrameworkSystem.Client.ProxyManagerClient import gProxyManager
from DIRAC.FrameworkSystem.Client.TokenManagerClient import gTokenManager
from DIRAC.Resources.Computing.ComputingElementFactory import ComputingElementFactory
from DIRAC.WorkloadManagementSystem.Client.PilotScopes import PILOT_SCOPES

# List of files to be inserted/retrieved into/from pilot Output Sandbox
# first will be defined as StdOut in JDL and the second as StdErr
outputSandboxFiles = ["StdOut", "StdErr"]

COMMAND_TIMEOUT = 60


def getPilotCE(pilotDict):
    """Instantiate and return a CE bound to a pilot"""
    ceFactory = ComputingElementFactory()
    result = getQueue(pilotDict["GridSite"], pilotDict["DestinationSite"], pilotDict["Queue"])
    if not result["OK"]:
        return result
    queueDict = result["Value"]
    queueDict["WorkingDirectory"] = mkdtemp()
    result = ceFactory.getCE(pilotDict["GridType"], pilotDict["DestinationSite"], queueDict)
    if not result["OK"]:
        shutil.rmtree(queueDict["WorkingDirectory"])
        return result
    return result


def getPilotProxy(pilotDict):
    """Get a proxy bound to a pilot

    :param dict pilotDict: pilot parameters
    :return: S_OK/S_ERROR with proxy as Value
    """

    opsH = Operations(vo=pilotDict["VO"])

    pilotDN = opsH.getValue("Pilot/GenericPilotDN")
    if not pilotDN:
        owner = Operations(vo=pilotDict["VO"]).getValue("Pilot/GenericPilotUser")
        res = getDNForUsername(owner)
        if not res["OK"]:
            return S_ERROR(f"Cannot get the generic pilot DN: {res['Message']}")
        pilotDN = res["Value"][0]
    pilotGroup = Operations(vo=pilotDict["VO"]).getValue("Pilot/GenericPilotGroup")

    groupVOMS = getGroupOption(pilotGroup, "VOMSRole")
    result = gProxyManager.getPilotProxyFromVOMSGroup(pilotDN, groupVOMS)
    if not result["OK"]:
        gLogger.error("Could not get proxy from VOMS:", f"DN \"{pilotDN}\" Group \"{groupVOMS}\" : {result['Message']}")
        return S_ERROR("Failed to get the pilot's owner proxy")
    return result


def setPilotCredentials(ce, pilotDict):
    """Instrument the given CE with proxy or token

    :param obj ce:  CE object
    :param pilotDict: pilot parameter dictionary
    :return: S_OK/S_ERROR
    """
    vo = pilotDict["VO"]
    if "Token" in ce.ceParameters.get("Tag", []) or f"Token:{vo}" in ce.ceParameters.get("Tag", []):
        pilotGroup = Operations(vo=vo).getValue("Pilot/GenericPilotGroup")
        result = gTokenManager.getToken(
            userGroup=pilotGroup,
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
        vo, site, ce, queue = key.split("@@@")
        result = getQueue(site, ce, queue)
        if not result["OK"]:
            return result
        queueDict = result["Value"]
        gridType = pilotDict["GridType"]
        result = ceFactory.getCE(gridType, ce, queueDict)
        if not result["OK"]:
            return result
        ce = result["Value"]

        pilotDict["VO"] = vo
        result = setPilotCredentials(ce, pilotDict)
        if not result["OK"]:
            return result

        pilotList = pilotDict["PilotList"]
        result = ce.killJob(pilotList)
        if not result["OK"]:
            return result

    return S_OK()
