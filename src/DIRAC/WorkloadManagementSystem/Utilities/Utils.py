""" Utilities for WMS
"""
import os
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
    with open(os.path.join(diracRoot, defaultWrapperLocation)) as fd:
        wrapperTemplate = fd.read()

    if "LogLevel" in jobParams:
        logLevel = jobParams["LogLevel"]
        log.info("Found Job LogLevel JDL parameter with value", logLevel)
    else:
        log.info("Applying default LogLevel JDL parameter with value", logLevel)

    if not pythonPath:
        pythonPath = os.path.realpath(sys.executable)
        log.debug("Real python path after resolving links is: ", pythonPath)

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
    if rootLocation != wrapperPath:
        generatedFiles["JobExecutableRelocatedPath"] = os.path.join(rootLocation, os.path.basename(jobExeFile))
    return S_OK(generatedFiles)


def rescheduleJobs(jobIDs: list[int], source: str = "") -> dict:
    """Utility to reschedule jobs (not atomic, nor bulk)
    Requires direct access to the JobDB and TaskQueueDB

    :param jobIDs: list of jobIDs
    :param source: source of the reschedule
    :return: S_OK/S_ERROR
    :rtype: dict

    """

    failedJobs = []

    for jobID in jobIDs:
        result = JobDB().rescheduleJob(jobID)
        if not result["OK"]:
            failedJobs.append(jobID)
            continue
        TaskQueueDB().deleteJob(jobID)
        JobLoggingDB().addLoggingRecord(
            result["JobID"],
            status=result["Status"],
            minorStatus=result["MinorStatus"],
            applicationStatus="Unknown",
            source=source,
        )

    if failedJobs:
        return S_ERROR(f"Failed to reschedule jobs {failedJobs}")
    return S_OK()
