########################################################################
# File :    CPUNormalization.py
# Author :  Ricardo Graciani
########################################################################

""" DIRAC Workload Management System Client module that encapsulates all the
    methods necessary to handle CPU normalization
"""

import DIRAC
from DIRAC import S_ERROR, S_OK, gConfig, gLogger
from DIRAC.ConfigurationSystem.Client.Helpers.Resources import getCESiteMapping
from DIRAC.Resources.Computing.BatchSystems.TimeLeft.TimeLeft import TimeLeft


def getCPUTime(cpuNormalizationFactor):
    """Trying to get CPUTime left for execution (in seconds).

    It will first look to get the work left looking for batch system information useing the TimeLeft utility.
    If it succeeds, it will convert it in real second, and return it.

    If it fails, it tries to get it from the static info found in CS.
    If it fails, it returns the default, which is a large 9999999, that we may consider as "Infinite".

    This is a generic method, independent from the middleware of the resource if TimeLeft doesn't return a value

    args:
      cpuNormalizationFactor (float): the CPU power of the current Worker Node.
      If not passed in, it's get from the local configuration

    returns:
      cpuTimeLeft (int): the CPU time left, in seconds
    """
    cpuTimeLeft = 0.0
    cpuWorkLeft = gConfig.getValue("/LocalSite/CPUTimeLeft", 0)

    if not cpuWorkLeft:
        # Try and get the information from the CPU left utility
        result = TimeLeft().getTimeLeft()
        if result["OK"]:
            cpuWorkLeft = result["Value"]

    if cpuWorkLeft > 0:
        # This is in HS06sseconds
        # We need to convert in real seconds
        if not cpuNormalizationFactor:  # if cpuNormalizationFactor passed in is 0, try get it from the local cfg
            cpuNormalizationFactor = gConfig.getValue("/LocalSite/CPUNormalizationFactor", 0.0)
        if cpuNormalizationFactor:
            cpuTimeLeft = cpuWorkLeft / cpuNormalizationFactor

    if not cpuTimeLeft:
        # now we know that we have to find the CPUTimeLeft by looking in the CS
        # this is not granted to be correct as the CS units may not be real seconds
        gridCE = gConfig.getValue("/LocalSite/GridCE")
        ceQueue = gConfig.getValue("/LocalSite/CEQueue")
        if not ceQueue:
            # we have to look for a ceQueue in the CS
            # A bit hacky. We should better profit from something generic
            gLogger.warn("No CEQueue in local configuration, looking to find one in CS")
            siteName = DIRAC.siteName()
            queueSection = f"/Resources/Sites/{siteName.split('.')[0]}/{siteName}/CEs/{gridCE}/Queues"
            res = gConfig.getSections(queueSection)
            if not res["OK"]:
                raise RuntimeError(res["Message"])
            queues = res["Value"]
            cpuTimes = [gConfig.getValue(queueSection + "/" + queue + "/maxCPUTime", 9999999.0) for queue in queues]
            # These are (real, wall clock) minutes - damn BDII!
            cpuTimeLeft = min(cpuTimes) * 60
        else:
            queueInfo = getQueueInfo(f"{gridCE}/{ceQueue}")
            cpuTimeLeft = 9999999.0
            if not queueInfo["OK"] or not queueInfo["Value"]:
                gLogger.warn("Can't find a CE/queue, defaulting CPUTime to %d" % cpuTimeLeft)
            else:
                queueCSSection = queueInfo["Value"]["QueueCSSection"]
                # These are (real, wall clock) minutes - damn BDII!
                cpuTimeInMinutes = gConfig.getValue(f"{queueCSSection}/maxCPUTime", 0.0)
                if cpuTimeInMinutes:
                    cpuTimeLeft = cpuTimeInMinutes * 60.0
                    gLogger.info(f"CPUTime for {queueCSSection}: {cpuTimeLeft:f}")
                else:
                    gLogger.warn(f"Can't find maxCPUTime for {queueCSSection}, defaulting CPUTime to {cpuTimeLeft:f}")

    return int(cpuTimeLeft)


def getQueueInfo(ceUniqueID, diracSiteName=""):
    """
    Extract information from full CE Name including associate DIRAC Site
    """
    try:
        subClusterUniqueID = ceUniqueID.split("/")[0].split(":")[0]
        queueID = ceUniqueID.split("/")[1]
    except IndexError:
        return S_ERROR("Wrong full queue Name")

    if not diracSiteName:
        gLogger.debug("SiteName not given, looking in /LocaSite/Site")
        diracSiteName = gConfig.getValue("/LocalSite/Site", "")

        if not diracSiteName:
            gLogger.debug("Can't find LocalSite name, looking in CS")
            result = getCESiteMapping(subClusterUniqueID)
            if not result["OK"]:
                return result
            diracSiteName = result["Value"][subClusterUniqueID]

            if not diracSiteName:
                gLogger.error("Can not find corresponding Site in CS")
                return S_ERROR("Can not find corresponding Site in CS")

    gridType = diracSiteName.split(".")[0]

    siteCSSEction = f"/Resources/Sites/{gridType}/{diracSiteName}/CEs/{subClusterUniqueID}"
    queueCSSection = f"{siteCSSEction}/Queues/{queueID}"

    resultDict = {
        "SubClusterUniqueID": subClusterUniqueID,
        "QueueID": queueID,
        "SiteName": diracSiteName,
        "Grid": gridType,
        "SiteCSSEction": siteCSSEction,
        "QueueCSSection": queueCSSection,
    }

    return S_OK(resultDict)
