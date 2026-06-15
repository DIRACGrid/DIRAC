"""Utilities for WMS"""

import os
from pathlib import Path
from glob import glob
import subprocess
import sys
import json

from DIRAC import gLogger, S_OK, S_ERROR
from DIRAC.Core.Utilities.File import mkDir
from DIRAC.FrameworkSystem.private.standardLogging.Logging import Logging
from DIRAC.WorkloadManagementSystem.DB.JobLoggingDB import JobLoggingDB
from DIRAC.WorkloadManagementSystem.DB.JobDB import JobDB
from DIRAC.WorkloadManagementSystem.DB.TaskQueueDB import TaskQueueDB


def createJobWrapper(
    jobID: str,
    jobParams: dict,
    resourceParams: dict,
    optimizerParams: dict,
    payloadParams: dict | None = None,
    extraOptions: str | None = None,
    wrapperPath: str | None = None,
    rootLocation: str | None = None,
    pythonPath: str | None = None,
    defaultWrapperLocation: str | None = "DIRAC/WorkloadManagementSystem/JobWrapper/JobWrapperTemplate.py",
    log: Logging | None = gLogger,
    logLevel: str | None = "INFO",
    cfgPath: str | None = None,
):
    """This method creates a job wrapper filled with the CE and Job parameters to execute the job.
    Main user is the JobAgent.

    :param jobID: Job ID
    :param jobParams: Job parameters
    :param resourceParams: CE parameters
    :param optimizerParams: Optimizer parameters
    :param payloadParams: Payload parameters
    :param extraOptions: Extra options to be passed to the job wrapper
    :param wrapperPath: Path where the job wrapper will be created
    :param rootLocation: Location where the job wrapper will be executed
    :param pythonPath: Path to the python executable
    :param defaultWrapperLocation: Location of the default job wrapper template
    :param log: Logger
    :param logLevel: Log level
    :param cfgPath: Path to a specific configuration file
    :return: S_OK with the path to the job wrapper and the path to the job wrapper json file
    """
    if isinstance(extraOptions, str) and extraOptions.endswith(".cfg"):
        extraOptions = f"--cfg {extraOptions}"

    arguments = {"Job": jobParams, "CE": resourceParams, "Optimizer": optimizerParams}
    if payloadParams:
        arguments["Payload"] = payloadParams
    log.verbose(f"Job arguments are: \n {arguments}")

    if not wrapperPath:
        wrapperPath = os.path.join(os.getcwd(), "job/Wrapper")
        mkDir(wrapperPath)

    diracRoot = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))

    jobWrapperFile = os.path.join(wrapperPath, f"Wrapper_{jobID}")
    if os.path.exists(jobWrapperFile):
        log.verbose("Removing existing Job Wrapper for", jobID)
        os.remove(jobWrapperFile)

    if "LogLevel" in jobParams:
        logLevel = jobParams["LogLevel"]
        log.info("Found Job LogLevel JDL parameter with value", logLevel)
    else:
        log.info("Applying default LogLevel JDL parameter with value", logLevel)

    if not pythonPath:
        pythonPath = os.path.realpath(sys.executable)
        log.debug("Real python path after resolving links is: ", pythonPath)

    if "Executable" in jobParams and jobParams["Executable"] == "dirac-cwl-exec":
        ret = __createCWLJobWrapper(jobID, wrapperPath, log, rootLocation)
        if not ret["OK"]:
            return ret
        jobWrapperFile, jobWrapperJsonFile, jobExeFile, jobFileContents = ret["Value"]
    else:
        with open(os.path.join(diracRoot, defaultWrapperLocation)) as fd:
            wrapperTemplate = fd.read()

        # Making real substitutions
        sitePython = os.getcwd()
        if rootLocation:
            sitePython = rootLocation
        wrapperTemplate = wrapperTemplate.replace("@SITEPYTHON@", sitePython)

        jobWrapperJsonFile = jobWrapperFile + ".json"
        with open(jobWrapperJsonFile, "w", encoding="utf8") as jsonFile:
            json.dump(arguments, jsonFile, ensure_ascii=False)

        with open(jobWrapperFile, "w") as wrapper:
            wrapper.write(wrapperTemplate)

        if not rootLocation:
            rootLocation = wrapperPath

        # The "real" location of the jobwrapper after it is started
        jobWrapperDirect = os.path.join(rootLocation, f"Wrapper_{jobID}")
        jobExeFile = os.path.join(wrapperPath, f"Job{jobID}")
        jobFileContents = """#!/bin/sh
    {} {} {} -o LogLevel={} -o /DIRAC/Security/UseServerCertificate=no {}
    """.format(
            pythonPath,
            jobWrapperDirect,
            extraOptions if extraOptions else "",
            logLevel,
            cfgPath if cfgPath else "",
        )

    with open(jobExeFile, "w") as jobFile:
        jobFile.write(jobFileContents)

    generatedFiles = {
        "JobExecutablePath": jobExeFile,
        "JobWrapperConfigPath": jobWrapperJsonFile,
        "JobWrapperPath": jobWrapperFile,
    }
    if rootLocation and rootLocation != wrapperPath:
        generatedFiles["JobExecutableRelocatedPath"] = os.path.join(rootLocation, os.path.basename(jobExeFile))
    return S_OK(generatedFiles)


def __createCWLJobWrapper(jobID, wrapperPath, log, rootLocation):
    # Get the new JobWrapper
    if not rootLocation:
        rootLocation = wrapperPath
    protoPath = Path(wrapperPath) / f"proto{jobID}"
    protoPath.unlink(missing_ok=True)
    log.info("Cloning JobWrapper from repository https://github.com/DIRACGrid/dirac-cwl.git into", protoPath)
    try:
        cmd = ["git", "clone", "https://github.com/DIRACGrid/dirac-cwl.git", str(protoPath)]
        subprocess.run(cmd, check=True)  # nosec: B603
    except subprocess.CalledProcessError:
        return S_ERROR("Failed to clone the JobWrapper repository")
    wrapperFound = glob(os.path.join(str(protoPath), "**", "job_wrapper_template.py"), recursive=True)
    if len(wrapperFound) < 1 or not Path(wrapperFound[0]).is_file():
        return S_ERROR("Could not find the JobWrapper in the cloned repository")
    jobWrapperFile = wrapperFound[0]
    directJobWrapperFile = str(Path(rootLocation) / Path(wrapperFound[0]).relative_to(wrapperPath))

    jobWrapperJsonFile = Path(wrapperPath) / f"InputSandbox{jobID}" / "job.json"
    directJobWrapperJsonFile = Path(rootLocation) / f"InputSandbox{jobID}" / "job.json"
    # Create the executable file
    jobExeFile = os.path.join(wrapperPath, f"Job{jobID}")
    protoPath = str(Path(rootLocation) / Path(protoPath).relative_to(wrapperPath))
    pixiPath = str(Path(rootLocation) / ".pixi")
    jobFileContents = f"""#!/bin/bash
# Install pixi
export PIXI_NO_PATH_UPDATE=1
export PIXI_HOME={pixiPath}
curl -fsSL https://pixi.sh/install.sh | bash
export PATH="{pixiPath}/bin:$PATH"
pixi install --manifest-path {protoPath}
# Get json
dirac-wms-job-get-input {jobID} -D {rootLocation}
# Run JobWrapper
pixi run --manifest-path {protoPath} python {directJobWrapperFile} {directJobWrapperJsonFile} {jobID}
"""
    return S_OK((jobWrapperFile, jobWrapperJsonFile, jobExeFile, jobFileContents))


def rescheduleJobs(
    jobIDs: list[int],
    source: str = "",
    jobDB: JobDB | None = None,
    taskQueueDB: TaskQueueDB | None = None,
    jobLoggingDB: JobLoggingDB | None = None,
) -> dict:
    """Utility to reschedule jobs (not atomic, nor bulk)
    Requires direct access to the JobDB and TaskQueueDB

    :param jobIDs: list of jobIDs
    :param source: source of the reschedule
    :param jobDB: optional JobDB instance to reuse (creates new if not provided)
    :param taskQueueDB: optional TaskQueueDB instance to reuse (creates new if not provided)
    :param jobLoggingDB: optional JobLoggingDB instance to reuse (creates new if not provided)
    :return: S_OK/S_ERROR
    :rtype: dict

    """

    failedJobs = []

    # Reuse provided DB instances or create new ones
    if jobDB is None:
        jobDB = JobDB()
    if taskQueueDB is None:
        taskQueueDB = TaskQueueDB()
    if jobLoggingDB is None:
        jobLoggingDB = JobLoggingDB()

    for jobID in jobIDs:
        result = jobDB.rescheduleJob(jobID)
        if not result["OK"]:
            failedJobs.append(jobID)
            continue
        taskQueueDB.deleteJob(jobID)
        jobLoggingDB.addLoggingRecord(
            result["JobID"],
            status=result["Status"],
            minorStatus=result["MinorStatus"],
            applicationStatus="Unknown",
            source=source,
        )

    if failedJobs:
        return S_ERROR(f"Failed to reschedule jobs {failedJobs}")
    return S_OK()
