""" A set of utilities used in the WMS services
    Requires the Nordugrid ARC plugins. In particular : nordugrid-arc-python
"""
from tempfile import mkdtemp
import shutil

from DIRAC import S_OK, S_ERROR, gLogger, gConfig
from DIRAC.ConfigurationSystem.Client.Helpers.Resources import getQueue
from DIRAC.ConfigurationSystem.Client.Helpers.Registry import getGroupOption
from DIRAC.FrameworkSystem.Client.ProxyManagerClient import gProxyManager
from DIRAC.Resources.Computing.ComputingElementFactory import ComputingElementFactory


# List of files to be inserted/retrieved into/from pilot Output Sandbox
# first will be defined as StdOut in JDL and the second as StdErr
outputSandboxFiles = ["StdOut", "StdErr"]

COMMAND_TIMEOUT = 60
###########################################################################


def getGridEnv():

    gridEnv = ""
    setup = gConfig.getValue("/DIRAC/Setup", "")
    if setup:
        instance = gConfig.getValue("/DIRAC/Setups/%s/WorkloadManagement" % setup, "")
        if instance:
            gridEnv = gConfig.getValue("/Systems/WorkloadManagement/%s/GridEnv" % instance, "")

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
    """Get a proxy bound to a pilot"""
    owner = pilotDict["OwnerDN"]
    group = pilotDict["OwnerGroup"]

    groupVOMS = getGroupOption(group, "VOMSRole", group)
    result = gProxyManager.getPilotProxyFromVOMSGroup(owner, groupVOMS)
    if not result["OK"]:
        gLogger.error("Could not get proxy:", 'User "{}" Group "{}" : {}'.format(owner, groupVOMS, result["Message"]))
        return S_ERROR("Failed to get the pilot's owner proxy")
    proxy = result["Value"]
    return S_OK(proxy)


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
