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
    """Compute the initial CPUTime left for execution (in seconds).

    This is called at pilot bootstrap (via dirac-wms-get-queue-cpu-time) to seed
    the initial CPUTimeLeft value. It queries the batch system first, then falls
    back to static CS configuration.

    args:
      cpuNormalizationFactor (float): the CPU power of the current Worker Node.
      If not passed in, it's get from the local configuration

    returns:
      cpuTimeLeft (int): the CPU time left, in seconds
    """

    # 1. Try to compute time left from the batch system (sacct, qstat, etc.)
    result = TimeLeft().getTimeLeft()
    if result["OK"]:
        cpuWorkLeft = result["Value"]
        # Batch system answered — trust it, even if 0
        if not cpuNormalizationFactor:
            cpuNormalizationFactor = gConfig.getValue("/LocalSite/CPUNormalizationFactor", 0.0)
        if cpuNormalizationFactor:
            return int(cpuWorkLeft / cpuNormalizationFactor)
        return 0

    cpuTimeLeft = 0.0

    # 2. Fall back to queue configuration in the CS.
    # These values are wall-clock minutes from BDII, so we convert to seconds.
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
        cpuTimes = [gConfig.getValue(queueSection + "/" + queue + "/maxCPUTime", 0.0) for queue in queues]
        cpuTimes = [t for t in cpuTimes if t > 0]
        if cpuTimes:
            cpuTimeLeft = min(cpuTimes) * 60
    else:
        queueInfo = getQueueInfo(f"{gridCE}/{ceQueue}")
        if not queueInfo["OK"] or not queueInfo["Value"]:
            gLogger.warn("Can't find a CE/queue in CS")
        else:
            queueCSSection = queueInfo["Value"]["QueueCSSection"]
            cpuTimeInMinutes = gConfig.getValue(f"{queueCSSection}/maxCPUTime", 0.0)
            if cpuTimeInMinutes:
                cpuTimeLeft = cpuTimeInMinutes * 60.0
                gLogger.info(f"CPUTime for {queueCSSection}: {cpuTimeLeft:f}")
            else:
                gLogger.warn(f"Can't find maxCPUTime for {queueCSSection}")

    if not cpuTimeLeft:
        # 3. Last resort: global default from CS, or 0 (fail safe: match no more jobs)
        cpuTimeLeft = gConfig.getValue("/Resources/Computing/CEDefaults/MaxCPUTime", 0)
        if cpuTimeLeft:
            gLogger.warn(f"Using fallback MaxCPUTime: {cpuTimeLeft}")
        else:
            gLogger.warn("Could not determine CPUTime left")

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
