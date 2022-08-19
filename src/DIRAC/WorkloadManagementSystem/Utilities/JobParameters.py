""" DIRAC Workload Management System utility module to get available memory and processors from mjf
"""
import os
import multiprocessing
from urllib.request import urlopen

from DIRAC import gLogger, gConfig
from DIRAC.Core.Utilities.List import fromChar


def getJobFeatures():
    features = {}
    if "JOBFEATURES" not in os.environ:
        return features
    for item in (
        "allocated_cpu",
        "hs06_job",
        "shutdowntime_job",
        "grace_secs_job",
        "jobstart_secs",
        "job_id",
        "wall_limit_secs",
        "cpu_limit_secs",
        "max_rss_bytes",
        "max_swap_bytes",
        "scratch_limit_bytes",
    ):
        fname = os.path.join(os.environ["JOBFEATURES"], item)
        try:
            val = urlopen(fname).read()
        except Exception:
            val = 0
        features[item] = val
    return features


def getProcessorFromMJF():
    jobFeatures = getJobFeatures()
    if jobFeatures:
        try:
            return int(jobFeatures["allocated_cpu"])
        except KeyError:
            gLogger.error(
                "MJF is available but allocated_cpu is not an integer", repr(jobFeatures.get("allocated_cpu"))
            )
    return None


def getMemoryFromMJF():
    jobFeatures = getJobFeatures()
    if jobFeatures:
        try:
            return int(jobFeatures["max_rss_bytes"])
        except KeyError:
            gLogger.error(
                "MJF is available but max_rss_bytes is not an integer", repr(jobFeatures.get("max_rss_bytes"))
            )
    return None


def getMemoryFromProc():
    meminfo = {i.split()[0].rstrip(":"): int(i.split()[1]) for i in open("/proc/meminfo").readlines()}
    maxRAM = meminfo["MemTotal"]
    if maxRAM:
        return int(maxRAM / 1024)


def getNumberOfProcessors(siteName=None, gridCE=None, queue=None):
    """gets the number of processors on a certain CE/queue/node (what the pilot administers)

    The siteName/gridCE/queue parameters are normally not necessary.

    Tries to find it in this order:
    1) from the /Resources/Computing/CEDefaults/NumberOfProcessors (which is what the pilot fills up)
    2) if not present from JobFeatures
    3) if not present looks in CS for "NumberOfProcessors" Queue or CE option
    4) if not present but there's WholeNode tag, look what the WN provides using multiprocessing.cpu_count()
    5) return 1
    """

    # 1) from /Resources/Computing/CEDefaults/NumberOfProcessors
    gLogger.info("Getting numberOfProcessors from /Resources/Computing/CEDefaults/NumberOfProcessors")
    numberOfProcessors = gConfig.getValue("/Resources/Computing/CEDefaults/NumberOfProcessors", 0)
    if numberOfProcessors:
        return numberOfProcessors

    # 2) from MJF
    gLogger.info("Getting numberOfProcessors from MJF")
    numberOfProcessors = getProcessorFromMJF()
    if numberOfProcessors:
        return numberOfProcessors
    gLogger.info("NumberOfProcessors could not be found in MJF")

    # 3) looks in CS for "NumberOfProcessors" Queue or CE or site option
    if not siteName:
        siteName = gConfig.getValue("/LocalSite/Site", "")
    if not gridCE:
        gridCE = gConfig.getValue("/LocalSite/GridCE", "")
    if not queue:
        queue = gConfig.getValue("/LocalSite/CEQueue", "")
    if not (siteName and gridCE and queue):
        gLogger.error("Could not find NumberOfProcessors: missing siteName or gridCE or queue. Returning '1'")
        return 1

    grid = siteName.split(".")[0]
    csPaths = [
        f"/Resources/Sites/{grid}/{siteName}/CEs/{gridCE}/Queues/{queue}/NumberOfProcessors",
        f"/Resources/Sites/{grid}/{siteName}/CEs/{gridCE}/NumberOfProcessors",
        f"/Resources/Sites/{grid}/{siteName}/Cloud/{gridCE}/VMTypes/{queue}/NumberOfProcessors",
        f"/Resources/Sites/{grid}/{siteName}/Cloud/{gridCE}/NumberOfProcessors",
        f"/Resources/Sites/{grid}/{siteName}/NumberOfProcessors",
    ]
    for csPath in csPaths:
        gLogger.info("Looking in", csPath)
        numberOfProcessors = gConfig.getValue(csPath, 0)
        if numberOfProcessors:
            return numberOfProcessors

    # 4) looks in CS for tags
    gLogger.info("Getting tags" "for %s: %s: %s" % (siteName, gridCE, queue))
    # Tags of the CE
    tags = fromChar(
        gConfig.getValue("/Resources/Sites/{}/{}/CEs/{}/Tag".format(siteName.split(".")[0], siteName, gridCE), "")
    ) + fromChar(
        gConfig.getValue("/Resources/Sites/{}/{}/Cloud/{}/Tag".format(siteName.split(".")[0], siteName, gridCE), "")
    )
    # Tags of the Queue
    tags += fromChar(
        gConfig.getValue(
            "/Resources/Sites/{}/{}/CEs/{}/Queues/{}/Tag".format(siteName.split(".")[0], siteName, gridCE, queue), ""
        )
    ) + fromChar(
        gConfig.getValue(
            "/Resources/Sites/{}/{}/Cloud/{}/VMTypes/{}/Tag".format(siteName.split(".")[0], siteName, gridCE, queue), ""
        )
    )
    gLogger.info("NumberOfProcessors could not be found in CS")
    if "WholeNode" in tags:
        gLogger.info("Found WholeNode tag, using multiprocessing.cpu_count()")
        return multiprocessing.cpu_count()

    # 5) return the default
    return 1


def getNumberOfPayloadProcessors(siteName=None, gridCE=None, queue=None):
    """Gets the number of processors allowed for a single JobAgent (so for a "inner" CE).
    (NB: this does not refer to the job processors).
    This is normally used ONLY when a pilot instantiates more than one JobAgent (MultiLaunchAgent pilot command).

    The siteName/gridCE/queue parameters are normally not necessary.

    Tries to find it in this order:
    1) from the /Resources/Computing/CEDefaults/NumberOfPayloadProcessors (which is what pilot 3 fills up)
    2) if not present but there's WholeNode tag, use the getNumberOfProcessors function above
    3) otherwise returns 1
    """

    # 1) from /Resources/Computing/CEDefaults/NumberOfPayloadProcessors
    gLogger.info("Getting NumberOfPayloadProcessors from /Resources/Computing/CEDefaults/NumberOfPayloadProcessors")
    NumberOfPayloadProcessors = gConfig.getValue("/Resources/Computing/CEDefaults/NumberOfPayloadProcessors")
    if NumberOfPayloadProcessors:
        return NumberOfPayloadProcessors

    # 2) Checks if 'WholeNode' is one of the used tags
    # Tags of the CE
    tags = fromChar(
        gConfig.getValue("/Resources/Sites/{}/{}/CEs/{}/Tag".format(siteName.split(".")[0], siteName, gridCE), "")
    ) + fromChar(
        gConfig.getValue("/Resources/Sites/{}/{}/Cloud/{}/Tag".format(siteName.split(".")[0], siteName, gridCE), "")
    )
    # Tags of the Queue
    tags += fromChar(
        gConfig.getValue(
            "/Resources/Sites/{}/{}/CEs/{}/Queues/{}/Tag".format(siteName.split(".")[0], siteName, gridCE, queue), ""
        )
    ) + fromChar(
        gConfig.getValue(
            "/Resources/Sites/{}/{}/Cloud/{}/VMTypes/{}/Tag".format(siteName.split(".")[0], siteName, gridCE, queue), ""
        )
    )

    if "WholeNode" in tags:
        return getNumberOfProcessors()

    # 3) Just returns a conservative "1"
    return 1


def getNumberOfJobProcessors(jobID):
    """Gets the number of processors allowed for the job.
    This can be used to communicate to your job payload the number of processors it's allowed to use,
    so this function should be called from your extension.

    If the JobAgent is using "InProcess" CE (which is the default),
    then what's returned will basically be the same of what's returned by the getNumberOfProcessors() function above
    """

    # from /Resources/Computing/JobLimits/jobID/NumberOfProcessors (set by PoolComputingElement)
    numberOfProcessors = gConfig.getValue("Resources/Computing/JobLimits/%s/NumberOfProcessors" % jobID)
    if numberOfProcessors:
        return numberOfProcessors

    return getNumberOfProcessors()


def getNumberOfGPUs(siteName=None, gridCE=None, queue=None):
    """Gets GPUs on a certain CE/queue/node (what the pilot administers)

    The siteName/gridCE/queue parameters are normally not necessary.

    Tries to find it in this order:
    1) from the /Resources/Computing/CEDefaults/GPUs (which is what the pilot might fill up)
    2) if not present looks in CS for "NumberOfGPUs" Queue or CE option
    3) return 0
    """

    # 1) from /Resources/Computing/CEDefaults/NumberOfGPUs
    gLogger.info("Getting GPUs from /Resources/Computing/CEDefaults/NumberOfGPUs")
    gpus = gConfig.getValue("/Resources/Computing/CEDefaults/NumberOfGPUs", 0)
    if gpus:
        return gpus

    # 2) looks in CS for "NumberOfGPUs" Queue or CE or site option
    if not siteName:
        siteName = gConfig.getValue("/LocalSite/Site", "")
    if not gridCE:
        gridCE = gConfig.getValue("/LocalSite/GridCE", "")
    if not queue:
        queue = gConfig.getValue("/LocalSite/CEQueue", "")
    if not (siteName and gridCE and queue):
        gLogger.error("Could not find NumberOfGPUs: missing siteName or gridCE or queue. Returning '0'")
        return 0

    grid = siteName.split(".")[0]
    csPaths = [
        f"/Resources/Sites/{grid}/{siteName}/CEs/{gridCE}/Queues/{queue}/NumberOfGPUs",
        f"/Resources/Sites/{grid}/{siteName}/CEs/{gridCE}/NumberOfGPUs",
        f"/Resources/Sites/{grid}/{siteName}/Cloud/{gridCE}/VMTypes/{queue}/NumberOfGPUs",
        f"/Resources/Sites/{grid}/{siteName}/Cloud/{gridCE}/NumberOfGPUs",
        f"/Resources/Sites/{grid}/{siteName}/NumberOfGPUs",
    ]
    for csPath in csPaths:
        gLogger.info("Looking in", csPath)
        numberOfGPUs = gConfig.getValue(csPath, 0)
        if numberOfGPUs:
            return numberOfGPUs

    # 3) return 0
    gLogger.info("NumberOfGPUs could not be found in CS")
    return 0
