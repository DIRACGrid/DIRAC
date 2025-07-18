"""DIRAC Workload Management System utility module to get available memory and processors"""

import multiprocessing

from DIRAC import S_OK, gConfig, gLogger
from DIRAC.Core.Utilities.List import fromChar


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
    2) if not present looks in CS for "NumberOfProcessors" Queue or CE option
    3) if not present but there's WholeNode tag, look what the WN provides using multiprocessing.cpu_count()
    4) return 1
    """

    # 1) from /Resources/Computing/CEDefaults/NumberOfProcessors
    gLogger.info("Getting numberOfProcessors from /Resources/Computing/CEDefaults/NumberOfProcessors")
    numberOfProcessors = gConfig.getValue("/Resources/Computing/CEDefaults/NumberOfProcessors", 0)
    if numberOfProcessors:
        return numberOfProcessors

    # 2) looks in CS for "NumberOfProcessors" Queue or CE or site option
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

    # 3) looks in CS for tags
    gLogger.info(f"Getting tagsfor {siteName}: {gridCE}: {queue}")
    # Tags of the CE
    tags = fromChar(
        gConfig.getValue(f"/Resources/Sites/{siteName.split('.')[0]}/{siteName}/CEs/{gridCE}/Tag", "")
    ) + fromChar(gConfig.getValue(f"/Resources/Sites/{siteName.split('.')[0]}/{siteName}/Cloud/{gridCE}/Tag", ""))
    # Tags of the Queue
    tags += fromChar(
        gConfig.getValue(f"/Resources/Sites/{siteName.split('.')[0]}/{siteName}/CEs/{gridCE}/Queues/{queue}/Tag", "")
    ) + fromChar(
        gConfig.getValue(f"/Resources/Sites/{siteName.split('.')[0]}/{siteName}/Cloud/{gridCE}/VMTypes/{queue}/Tag", "")
    )
    gLogger.info("NumberOfProcessors could not be found in CS")
    if "WholeNode" in tags:
        gLogger.info("Found WholeNode tag, using multiprocessing.cpu_count()")
        return multiprocessing.cpu_count()

    # 4) return the default
    return 1


def getNumberOfJobProcessors(jobID):
    """Gets the number of processors allowed for the job.
    This can be used to communicate to your job payload the number of processors it's allowed to use,
    so this function should be called from your extension.

    If the JobAgent is using "InProcess" CE (which is the default),
    then what's returned will basically be the same of what's returned by the getNumberOfProcessors() function above
    """

    # from /Resources/Computing/JobLimits/jobID/NumberOfProcessors (set by PoolComputingElement)
    numberOfProcessors = gConfig.getValue(f"Resources/Computing/JobLimits/{jobID}/NumberOfProcessors")
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


def getJobParameters(jobIDs: list[int], parName: str | None, vo: str = "") -> dict:
    """Utility to get a job parameter for a list of jobIDs pertaining to a VO.
    If the jobID is not in the JobParametersDB, it will be looked up in the JobDB.

    Requires direct access to the JobParametersDB and JobDB.

    :param jobIDs: list of jobIDs
    :param parName: name of the parameter to be retrieved
    :param vo: VO of the jobIDs
    :return: dictionary with jobID as key and the parameter as value
    :rtype: dict
    """
    from DIRAC.WorkloadManagementSystem.DB.JobParametersDB import JobParametersDB
    from DIRAC.WorkloadManagementSystem.DB.JobDB import JobDB

    elasticJobParametersDB = JobParametersDB()
    jobDB = JobDB()

    if vo:  # a user is connecting, with a proxy
        res = elasticJobParametersDB.getJobParameters(jobIDs, vo, parName)
        if not res["OK"]:
            return res
        parameters = res["Value"]
    else:  # a service is connecting, no proxy, e.g. StalledJobAgent
        q = f"SELECT JobID, VO FROM Jobs WHERE JobID IN ({','.join([str(jobID) for jobID in jobIDs])})"
        res = jobDB._query(q)
        if not res["OK"]:
            return res
        if not res["Value"]:
            return S_OK({})
        # get the VO for each jobID
        voDict = {}
        for jobID, vo in res["Value"]:
            if vo not in voDict:
                voDict[vo] = []
            voDict[vo].append(jobID)
        # get the parameters for each VO
        parameters = {}
        for vo, jobIDs in voDict.items():
            res = elasticJobParametersDB.getJobParameters(jobIDs, vo, parName)
            if not res["OK"]:
                return res
            parameters.update(res["Value"])

    # Need anyway to get also from JobDB, for those jobs with parameters registered in MySQL or in both backends
    res = jobDB.getJobParameters(jobIDs, parName)
    if not res["OK"]:
        return res
    parametersM = res["Value"]

    # and now combine
    final = dict(parametersM)
    # if job in JobDB, update with parameters from ES if any
    for jobID in final:
        final[jobID].update(parameters.get(jobID, {}))
    # if job in ES and not in JobDB, take ES
    for jobID in parameters:
        if jobID not in final:
            final[jobID] = parameters[jobID]
    return S_OK(final)
