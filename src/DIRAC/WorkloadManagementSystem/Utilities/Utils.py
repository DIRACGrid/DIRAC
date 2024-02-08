""" Utilities for WMS
"""
import os
import sys
import json
from typing import Optional

from DIRAC import gLogger, S_OK
from DIRAC.Core.Utilities.File import mkDir
from DIRAC.FrameworkSystem.private.standardLogging.Logging import Logging


def createJobWrapper(
    jobID: str,
    jobParams: dict,
    resourceParams: dict,
    optimizerParams: dict,
    payloadParams: Optional[dict] = None,
    extraOptions: Optional[str] = None,
    wrapperPath: Optional[str] = None,
    rootLocation: Optional[str] = None,
    pythonPath: Optional[str] = None,
    defaultWrapperLocation: Optional[str] = "DIRAC/WorkloadManagementSystem/JobWrapper/JobWrapperTemplate.py",
    log: Optional[Logging] = gLogger,
    logLevel: Optional[str] = "INFO",
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
    :param defaultWrapperLocation: Location of the default job wrapper template
    :param pythonPath: Path to the python executable
    :param log: Logger
    :param logLevel: Log level
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

    if not rootLocation:
        rootLocation = wrapperPath

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
    wrapperTemplate = wrapperTemplate.replace("@SITEPYTHON@", rootLocation)

    jobWrapperJsonFile = jobWrapperFile + ".json"
    with open(jobWrapperJsonFile, "w", encoding="utf8") as jsonFile:
        json.dump(arguments, jsonFile, ensure_ascii=False)

    with open(jobWrapperFile, "w") as wrapper:
        wrapper.write(wrapperTemplate)

    # The "real" location of the jobwrapper after it is started
    jobWrapperDirect = os.path.join(rootLocation, f"Wrapper_{jobID}")
    jobExeFile = os.path.join(wrapperPath, f"Job{jobID}")
    jobFileContents = """#!/bin/sh
{} {} {} -o LogLevel={} -o /DIRAC/Security/UseServerCertificate=no
""".format(
        pythonPath,
        jobWrapperDirect,
        extraOptions if extraOptions else "",
        logLevel,
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
