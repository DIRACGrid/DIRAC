"""Test the interactions with a given set of Computing Elements (CE). For each CE:
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
"""
import concurrent.futures
import time
from pathlib import Path
from typing import List

import typer

import DIRAC
from DIRAC import gLogger
from DIRAC.ConfigurationSystem.Client.Helpers import Operations, Registry
from DIRAC.ConfigurationSystem.Client.Helpers.Resources import getQueues
from DIRAC.Core.Base.Script import Script
from DIRAC.FrameworkSystem.Client.ProxyManagerClient import gProxyManager
from DIRAC.FrameworkSystem.Client.TokenManagerClient import gTokenManager
from DIRAC.Resources.Computing.ComputingElement import ComputingElement
from DIRAC.WorkloadManagementSystem.Client.PilotScopes import PILOT_SCOPES
from DIRAC.WorkloadManagementSystem.Utilities.QueueUtilities import getQueuesResolved

app = typer.Typer(help="Test the interactions with a given set of Computing Elements (CE)")


def findGenericCreds(vo: str):
    """
    Find the generic pilot credentials for the given VO.
    :param vo: The Virtual Organization to use for credentials.
    :return: A tuple containing the pilot DN and group.
    """
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


def getCredentials(pilotDN: str, pilotGroup: str, ce: ComputingElement):
    """
    Get the pilot credentials for the dn/group.
    :return: A tuple containing the proxy and the token.
    """
    # Get the pilot proxy from the ProxyManager
    result = gProxyManager.getPilotProxyFromDIRACGroup(pilotDN, pilotGroup, 3600)
    if not result["OK"]:
        gLogger.error("Cannot get pilot proxy", result["Message"])
        return None, None
    proxy = result["Value"]

    # Get the pilot token from the TokenManager
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


def buildQueues(vo: str, sites: list[str], ces: list[str], ceTypes: list[str]) -> dict:
    """
    Get the list of queues for the given community, site list, CE list, and CE type list.
    :param vo: The Virtual Organization to use for credentials.
    :param sites: The list of sites to filter the queues.
    :param ces: The list of Computing Elements to filter the queues.
    :param ceTypes: The list of Computing Element types to filter the queues.
    :return: A dictionary containing the queues for the given parameters.
    """
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


def interactWithCE(ce: ComputingElement):
    """
    Interact with a given Computing Element (CE).
    :param ceName: The name of the CE.
    :param port: The port of the CE.
    :param vo: The Virtual Organization to use for credentials.
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
    while status != "Done" and status != "Failed":
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


@app.command()
def main(
    vo: str = typer.Argument(help="Select Virtual Organization"),
    sites: list[str] = typer.Option([], "--site", help="Select sites (can be used multiple times)"),
    ces: list[str] = typer.Option([], "--ce", help="Select CEs (can be used multiple times)"),
    ce_types: list[str] = typer.Option([], "--ce-type", help="Select CE types (can be used multiple times)"),
    log_level: str = typer.Option("INFO", "--log-level", help="Set the log level (DEBUG, VERBOSE, INFO)"),
    script: str = typer.Option(None, "--script", help="Path to custom executable script (default: workloadExec.sh)"),
):
    """Test the interactions with a given set of Computing Elements (CE)."""
    Script.initialize()

    if log_level:
        gLogger.setLevel(log_level.upper())
        # If you set a log level for a specific backend and want more details to debug
        # then uncomment the next line
        # gLogger._backendsList[0]._handler.setLevel(log_level.upper())

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
        ceTypes=ce_types,
    )
    if not queueDict:
        gLogger.error("Cannot get queues")
        DIRAC.exit(1)

    if script:
        # Use the provided custom script
        gLogger.info(f"Using custom script: {script}")
        executable = Path(script)
        if not executable.exists() or not executable.is_file():
            gLogger.error(f"Provided script {script} does not exist or is not a file")
            DIRAC.exit(1)
    else:
        # Create default workloadExec.sh
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
                DIRAC.exit(1)
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
    if not script:
        executable.unlink()

    gLogger.info("Overall interaction state:")
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
        typer.echo(f"- {pct}% of the queues correctly {pretty[check]}. " f"Issues with {issueCount} queue(s):")
        for qname, qState in overallState.items():
            if not qState[check]["OK"]:
                msg = qState[check]["Message"] or "unknown error"
                typer.echo(f"  - {qname}: {msg}")
        typer.echo("")


if __name__ == "__main__":
    app()
