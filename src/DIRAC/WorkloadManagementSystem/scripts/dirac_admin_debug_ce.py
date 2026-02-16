#!/usr/bin/env python
"""
Test the interactions with a given set of Computing Elements (CE). For each CE:

- Get the CE status if available
- Submit a job to the CE
- Get the job status
- Get the job output/error/log if available

Conditions:

- The CEs must be configured in the DIRAC configuration
- The script should be executed with an admin proxy: used to fetch a pilot proxy and a token
- The script should be executed:

  - in a DIRAC client environment for normal CEs, such as AREX and HTCondor
  - in a DIRAC host environment for SSH/Local CEs (credentials would not be available otherwise)

Usage:
  dirac-admin-debug-ce <VO> [--site <site>] [--ce <ce>] [--ce-type <type>] [--script <path>]

Example:
  $ dirac-admin-debug-ce dteam --site LCG.CERN.cern --ce-type HTCondorCE
"""
import concurrent.futures
import time
from pathlib import Path

import DIRAC
from DIRAC import S_OK, gLogger
from DIRAC.Core.Base.Script import Script

# Maximum time (in seconds) to wait for a job to reach a final state
JOB_STATUS_TIMEOUT = 1800

sites = []
ces = []
ceTypes = []
scriptPath = None


def setTimeout(val):
    global JOB_STATUS_TIMEOUT
    JOB_STATUS_TIMEOUT = int(val)
    return S_OK()


def addSite(val):
    sites.append(val)
    return S_OK()


def addCE(val):
    ces.append(val)
    return S_OK()


def addCEType(val):
    ceTypes.append(val)
    return S_OK()


def setScript(val):
    global scriptPath
    scriptPath = val
    return S_OK()


def findGenericCreds(vo):
    """Find the generic pilot credentials for the given VO.

    :param str vo: The Virtual Organization to use for credentials.
    :return: A tuple containing the pilot DN and group.
    """
    from DIRAC.ConfigurationSystem.Client.Helpers import Operations, Registry

    opsHelper = Operations.Operations(vo=vo)
    pilotGroup = opsHelper.getValue("Pilot/GenericPilotGroup", "")
    pilotDN = opsHelper.getValue("Pilot/GenericPilotDN", "")
    if not pilotDN:
        pilotUser = opsHelper.getValue("Pilot/GenericPilotUser", "")
        if pilotUser:
            result = Registry.getDNForUsername(pilotUser)
            if result["OK"]:
                pilotDN = result["Value"][0]
    return pilotDN, pilotGroup


def getCredentials(pilotDN, pilotGroup, ce):
    """Get the pilot credentials for the dn/group.

    :param str pilotDN: The pilot DN.
    :param str pilotGroup: The pilot group.
    :param ce: The Computing Element instance.
    :return: A tuple containing the proxy and the token, or (None, None) on failure.
    """
    from DIRAC.FrameworkSystem.Client.ProxyManagerClient import gProxyManager
    from DIRAC.FrameworkSystem.Client.TokenManagerClient import gTokenManager
    from DIRAC.WorkloadManagementSystem.Client.PilotScopes import PILOT_SCOPES

    result = gProxyManager.getPilotProxyFromDIRACGroup(pilotDN, pilotGroup, 3600)
    if not result["OK"]:
        gLogger.error("Cannot get pilot proxy", result["Message"])
        return None, None
    proxy = result["Value"]

    result = gTokenManager.getToken(
        userGroup=pilotGroup,
        scope=PILOT_SCOPES,
        audience=ce.audienceName,
        requiredTimeLeft=1200,
    )
    if not result["OK"]:
        gLogger.error("Cannot get pilot token", result["Message"])
        return None, None
    token = result["Value"]

    return proxy, token


def buildQueues(vo, sites, ces, ceTypes):
    """Get the list of queues for the given community, site list, CE list, and CE type list.

    :param str vo: The Virtual Organization to use for credentials.
    :param list sites: The list of sites to filter the queues.
    :param list ces: The list of Computing Elements to filter the queues.
    :param list ceTypes: The list of Computing Element types to filter the queues.
    :return: A dictionary containing the queues for the given parameters.
    """
    from DIRAC.ConfigurationSystem.Client.Helpers.Resources import getQueues
    from DIRAC.WorkloadManagementSystem.Utilities.QueueUtilities import getQueuesResolved

    result = getQueues(
        community=vo,
        siteList=sites,
        ceList=ces,
        ceTypeList=ceTypes,
    )
    if not result["OK"]:
        gLogger.error("Cannot get queues", result["Message"])
        return {}

    result = getQueuesResolved(
        siteDict=result["Value"],
        queueCECache={},
        vo=vo,
        instantiateCEs=True,
    )
    if not result["OK"]:
        gLogger.error("Cannot resolve queues", result["Message"])
        return {}

    return result["Value"]


def interactWithCE(ce):
    """Interact with a given Computing Element (CE).

    :param ce: The Computing Element instance.
    :return: A dictionary with the result of each check.
    """
    checks = {
        "ce_status": {"OK": False, "Message": ""},
        "job_submit": {"OK": False, "Message": ""},
        "job_status": {"OK": False, "Message": ""},
        "job_output": {"OK": False, "Message": ""},
        "job_log": {"OK": False, "Message": ""},
    }

    # Get CE Status
    gLogger.info(f"[{ce.ceName}]Getting CE status")
    result = ce.getCEStatus()
    if not result["OK"] and ce.ceType != "HTCondorCE":
        gLogger.error(f"[{ce.ceName}]Cannot get CE status: {result['Message']}")
        checks["ce_status"]["Message"] = result["Message"]
        return checks
    checks["ce_status"]["OK"] = True

    # Submit a job to the CE
    gLogger.info(f"[{ce.ceName}]Submitting a job")
    res = ce.submitJob("workloadExec.sh", None)
    if not res["OK"]:
        gLogger.error(f"[{ce.ceName}]Cannot submit job to CE: {res['Message']}")
        checks["job_submit"]["Message"] = res["Message"]
        return checks
    checks["job_submit"]["OK"] = True

    job_id = res["Value"][0]
    stamp = res["PilotStampDict"][job_id]

    # Wait for the job to finish
    gLogger.info(f"[{ce.ceName}]Getting job status")
    status = "Waiting"
    deadline = time.monotonic() + JOB_STATUS_TIMEOUT
    while status != "Done" and status != "Failed":
        if time.monotonic() > deadline:
            gLogger.error(f"[{ce.ceName}]Timed out after {JOB_STATUS_TIMEOUT}s waiting for job {job_id}")
            checks["job_status"]["Message"] = f"Timed out after {JOB_STATUS_TIMEOUT}s (last status: {status})"
            return checks
        res = ce.getJobStatus([job_id])
        if not res["OK"]:
            gLogger.error(f"[{ce.ceName}]Cannot get job status: {res['Message']}")
            checks["job_status"]["Message"] = res["Message"]
            return checks
        status = res["Value"][job_id]
        time.sleep(5)
        gLogger.verbose(f"[{ce.ceName}]Job {job_id} in status {status}")
    checks["job_status"]["OK"] = True

    # Get job output, error, and log
    gLogger.info(f"[{ce.ceName}]Getting job output and log")
    res = ce.getJobOutput(job_id + ":::" + stamp)
    if not res["OK"]:
        gLogger.error(f"[{ce.ceName}]Cannot get job output: {res['Message']}")
        checks["job_output"]["Message"] = res["Message"]
        return checks
    checks["job_output"]["OK"] = True

    if hasattr(ce, "getJobLog"):
        res = ce.getJobLog(job_id + ":::" + stamp)
        if not res["OK"]:
            gLogger.error(f"[{ce.ceName}]Cannot get job log: {res['Message']}")
            checks["job_log"]["Message"] = res["Message"]
            return checks
    checks["job_log"]["OK"] = True
    return checks


@Script()
def main():
    global scriptPath

    Script.registerSwitch("", "site=", "Select site (can be used multiple times)", addSite)
    Script.registerSwitch("", "ce=", "Select CE (can be used multiple times)", addCE)
    Script.registerSwitch("", "ce-type=", "Select CE type (can be used multiple times)", addCEType)
    Script.registerSwitch("", "script=", "Path to custom executable script (default: workloadExec.sh)", setScript)
    Script.registerSwitch("", "timeout=", "Timeout in seconds for job status polling (default: 1800)", setTimeout)
    Script.registerArgument("VO: Virtual Organization")
    Script.parseCommandLine()

    from DIRAC.Core.Security.Properties import SecurityProperty
    from DIRAC.Core.Security.ProxyInfo import getProxyInfo

    # Check credentials
    result = getProxyInfo()
    if not result["OK"]:
        gLogger.error("Do you have a valid proxy?")
        gLogger.error(result["Message"])
        DIRAC.exit(1)
    proxyProps = result["Value"]

    if SecurityProperty.FULL_DELEGATION not in proxyProps.get("groupProperties", []):
        gLogger.error("You need an admin proxy (with FullDelegation property) to run this script")
        DIRAC.exit(1)

    vo = Script.getPositionalArgs()[0]

    # Get credentials for the given VO
    pilotDN, pilotGroup = findGenericCreds(vo)
    if not pilotDN or not pilotGroup:
        gLogger.error("Cannot get pilot credentials")
        DIRAC.exit(1)

    # Get the queues
    queueDict = buildQueues(
        vo=vo,
        sites=sites,
        ces=ces,
        ceTypes=ceTypes,
    )
    if not queueDict:
        gLogger.error("Cannot get queues")
        DIRAC.exit(1)

    if scriptPath:
        gLogger.info(f"Using custom script: {scriptPath}")
        executable = Path(scriptPath)
        if not executable.exists() or not executable.is_file():
            gLogger.error(f"Provided script {scriptPath} does not exist or is not a file")
            DIRAC.exit(1)
    else:
        gLogger.info("Creating default workloadExec.sh")
        executable = Path("workloadExec.sh")
        with open(executable, "w") as f:
            f.write("#!/bin/bash\n")
            f.write("echo 'Hello from DIRAC!'\n")

    # Make sure the script is executable
    executable.chmod(0o755)

    # Prepare to interact with each CE
    overallState = {}

    def process_queue(queueName):
        ce = queueDict[queueName]["CE"]
        if ce.ceType != "SSH":
            gLogger.info(f"Getting creds for CE: {ce.ceName} ({ce.ceType})")
            proxy, token = getCredentials(pilotDN, pilotGroup, ce)
            if not proxy or not token:
                gLogger.error(f"[{ce.ceName}]Failed to get credentials, skipping")
                return queueName, {
                    check: {"OK": False, "Message": "Failed to get credentials"}
                    for check in ("ce_status", "job_submit", "job_status", "job_output", "job_log")
                }
            ce.setProxy(proxy)
            if "Token" in ce.ceParameters.get("Tag", []):
                ce.setToken(token)
        if ce.ceType == "HTCondorCE":
            ce.workingDirectory = str(Path.cwd())
        gLogger.info(f"Interacting with CE: {ce.ceName} ({ce.ceType})")
        return queueName, interactWithCE(ce)

    with concurrent.futures.ThreadPoolExecutor() as executor:
        results = executor.map(process_queue, list(queueDict.keys()))
        for queueName, state in results:
            overallState[queueName] = state

    # Clean up the script file after submission (only if auto-generated)
    if not scriptPath:
        executable.unlink()

    gLogger.notice("Overall interaction state:")
    total = len(overallState)
    # human-friendly names for each check
    pretty = {
        "ce_status": "reported CE status",
        "job_submit": "submitted a job",
        "job_status": "retrieved job status",
        "job_output": "fetched job output",
        "job_log": "fetched job log",
    }

    for check in pretty:
        okCount = sum(1 for queueState in overallState.values() if queueState[check]["OK"])
        issueCount = total - okCount
        pct = int(okCount / total * 100) if total else 0
        gLogger.notice(f"- {pct}% of the queues correctly {pretty[check]}. Issues with {issueCount} queue(s):")
        for qname, qState in overallState.items():
            if not qState[check]["OK"]:
                msg = qState[check]["Message"] or "unknown error"
                gLogger.notice(f"  - {qname}: {msg}")
        gLogger.notice("")


if __name__ == "__main__":
    main()
