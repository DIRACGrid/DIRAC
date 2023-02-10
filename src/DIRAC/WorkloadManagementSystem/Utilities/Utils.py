""" Utilities for WMS
"""
import os
import sys
import json

from DIRAC import gLogger, S_OK, S_ERROR
from DIRAC.Core.Utilities.File import mkDir
from DIRAC.ConfigurationSystem.Client.Helpers import Registry
from DIRAC.FrameworkSystem.Client.ProxyManagerClient import gProxyManager


def createJobWrapper(
    jobID,
    jobParams,
    resourceParams,
    optimizerParams,
    extraOptions="",
    defaultWrapperLocation="DIRAC/WorkloadManagementSystem/JobWrapper/JobWrapperTemplate.py",
    log=gLogger,
    logLevel="INFO",
):
    """This method creates a job wrapper filled with the CE and Job parameters to execute the job.
    Main user is the JobAgent
    """
    if isinstance(extraOptions, str) and extraOptions.endswith(".cfg"):
        extraOptions = f"--cfg {extraOptions}"

    arguments = {"Job": jobParams, "CE": resourceParams, "Optimizer": optimizerParams}
    log.verbose(f"Job arguments are: \n {arguments}")

    mkDir(os.path.join(os.getcwd(), "job/Wrapper"))
    diracRoot = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))

    jobWrapperFile = f"{os.getcwd()}/job/Wrapper/Wrapper_{jobID}"
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

    dPython = sys.executable
    realPythonPath = os.path.realpath(dPython)
    log.debug("Real python path after resolving links is: ", realPythonPath)
    dPython = realPythonPath

    # Making real substitutions
    # wrapperTemplate = wrapperTemplate.replace( "@JOBARGS@", str( arguments ) )
    wrapperTemplate = wrapperTemplate.replace("@SITEPYTHON@", os.getcwd())

    jobWrapperJsonFile = jobWrapperFile + ".json"
    with open(jobWrapperJsonFile, "w", encoding="utf8") as jsonFile:
        json.dump(str(arguments), jsonFile, ensure_ascii=False)

    with open(jobWrapperFile, "w") as wrapper:
        wrapper.write(wrapperTemplate)

    jobExeFile = f"{os.getcwd()}/job/Wrapper/Job{jobID}"
    jobFileContents = """#!/bin/sh
{} {} {} -o LogLevel={} -o /DIRAC/Security/UseServerCertificate=no
""".format(
        dPython,
        jobWrapperFile,
        extraOptions,
        logLevel,
    )
    with open(jobExeFile, "w") as jobFile:
        jobFile.write(jobFileContents)

    return S_OK((jobExeFile, jobWrapperJsonFile, jobWrapperFile))


def createRelocatedJobWrapper(
    wrapperPath,
    rootLocation,
    jobID,
    jobParams,
    resourceParams,
    optimizerParams,
    extraOptions="",
    defaultWrapperLocation="DIRAC/WorkloadManagementSystem/JobWrapper/JobWrapperTemplate.py",
    log=gLogger,
    logLevel="INFO",
):
    """This method creates a job wrapper for a specific job in wrapperPath,
    but assumes this has been reloated to rootLocation before running it.
    """
    if isinstance(extraOptions, str) and extraOptions.endswith(".cfg") and "--cfg" not in extraOptions:
        extraOptions = f"--cfg {extraOptions}"

    arguments = {"Job": jobParams, "CE": resourceParams, "Optimizer": optimizerParams}
    log.verbose(f"Job arguments are: \n {arguments}")

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

    # Making real substitutions
    # wrapperTemplate = wrapperTemplate.replace( "@JOBARGS@", str( arguments ) )
    wrapperTemplate = wrapperTemplate.replace("@SITEPYTHON@", rootLocation)

    jobWrapperJsonFile = jobWrapperFile + ".json"
    with open(jobWrapperJsonFile, "w", encoding="utf8") as jsonFile:
        json.dump(str(arguments), jsonFile, ensure_ascii=False)

    with open(jobWrapperFile, "w") as wrapper:
        wrapper.write(wrapperTemplate)

    # The "real" location of the jobwrapper after it is started
    jobWrapperDirect = os.path.join(rootLocation, f"Wrapper_{jobID}")
    jobExeFile = os.path.join(wrapperPath, f"Job{jobID}")
    jobFileContents = """#!/bin/sh
python {} {} -o LogLevel={} -o /DIRAC/Security/UseServerCertificate=no
""".format(
        jobWrapperDirect,
        extraOptions,
        logLevel,
    )
    with open(jobExeFile, "w") as jobFile:
        jobFile.write(jobFileContents)

    jobExeDirect = os.path.join(rootLocation, f"Job{jobID}")
    return S_OK(jobExeDirect)
