""" Utilities for WMS
"""
import io
import os
import sys
import json

from DIRAC import gConfig, gLogger, S_OK, S_ERROR
from DIRAC.Core.Utilities.File import mkDir
from DIRAC.ConfigurationSystem.Client.Helpers import Registry
from DIRAC.FrameworkSystem.Client.ProxyManagerClient import gProxyManager
from DIRAC.ConfigurationSystem.Client.Helpers.Resources import findGenericCloudCredentials


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
        extraOptions = "--cfg %s" % extraOptions

    arguments = {"Job": jobParams, "CE": resourceParams, "Optimizer": optimizerParams}
    log.verbose("Job arguments are: \n %s" % (arguments))

    siteRoot = gConfig.getValue("/LocalSite/Root", os.getcwd())
    log.debug("SiteRootPythonDir is:\n%s" % siteRoot)
    workingDir = gConfig.getValue("/LocalSite/WorkingDirectory", siteRoot)
    mkDir("%s/job/Wrapper" % (workingDir))

    diracRoot = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))

    jobWrapperFile = f"{workingDir}/job/Wrapper/Wrapper_{jobID}"
    if os.path.exists(jobWrapperFile):
        log.verbose("Removing existing Job Wrapper for %s" % (jobID))
        os.remove(jobWrapperFile)
    with open(os.path.join(diracRoot, defaultWrapperLocation)) as fd:
        wrapperTemplate = fd.read()

    if "LogLevel" in jobParams:
        logLevel = jobParams["LogLevel"]
        log.info("Found Job LogLevel JDL parameter with value: %s" % (logLevel))
    else:
        log.info("Applying default LogLevel JDL parameter with value: %s" % (logLevel))

    dPython = sys.executable
    realPythonPath = os.path.realpath(dPython)
    log.debug("Real python path after resolving links is: ", realPythonPath)
    dPython = realPythonPath

    # Making real substitutions
    # wrapperTemplate = wrapperTemplate.replace( "@JOBARGS@", str( arguments ) )
    wrapperTemplate = wrapperTemplate.replace("@SITEPYTHON@", str(siteRoot))

    jobWrapperJsonFile = jobWrapperFile + ".json"
    with open(jobWrapperJsonFile, "w", encoding="utf8") as jsonFile:
        json.dump(str(arguments), jsonFile, ensure_ascii=False)

    with open(jobWrapperFile, "w") as wrapper:
        wrapper.write(wrapperTemplate)

    jobExeFile = f"{workingDir}/job/Wrapper/Job{jobID}"
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
        extraOptions = "--cfg %s" % extraOptions

    arguments = {"Job": jobParams, "CE": resourceParams, "Optimizer": optimizerParams}
    log.verbose("Job arguments are: \n %s" % (arguments))

    diracRoot = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))

    jobWrapperFile = os.path.join(wrapperPath, "Wrapper_%s" % jobID)
    if os.path.exists(jobWrapperFile):
        log.verbose("Removing existing Job Wrapper for %s" % (jobID))
        os.remove(jobWrapperFile)
    with open(os.path.join(diracRoot, defaultWrapperLocation)) as fd:
        wrapperTemplate = fd.read()

    if "LogLevel" in jobParams:
        logLevel = jobParams["LogLevel"]
        log.info("Found Job LogLevel JDL parameter with value: %s" % (logLevel))
    else:
        log.info("Applying default LogLevel JDL parameter with value: %s" % (logLevel))

    # Making real substitutions
    # wrapperTemplate = wrapperTemplate.replace( "@JOBARGS@", str( arguments ) )
    wrapperTemplate = wrapperTemplate.replace("@SITEPYTHON@", rootLocation)

    jobWrapperJsonFile = jobWrapperFile + ".json"
    with open(jobWrapperJsonFile, "w", encoding="utf8") as jsonFile:
        json.dump(str(arguments), jsonFile, ensure_ascii=False)

    with open(jobWrapperFile, "w") as wrapper:
        wrapper.write(wrapperTemplate)

    # The "real" location of the jobwrapper after it is started
    jobWrapperDirect = os.path.join(rootLocation, "Wrapper_%s" % jobID)
    jobExeFile = os.path.join(wrapperPath, "Job%s" % jobID)
    jobFileContents = """#!/bin/sh
python {} {} -o LogLevel={} -o /DIRAC/Security/UseServerCertificate=no
""".format(
        jobWrapperDirect,
        extraOptions,
        logLevel,
    )
    with open(jobExeFile, "w") as jobFile:
        jobFile.write(jobFileContents)

    jobExeDirect = os.path.join(rootLocation, "Job%s" % jobID)
    return S_OK(jobExeDirect)


def getProxyFileForCloud(ce):
    """Get a file with the proxy to be used to connect to the
        given cloud endpoint

    :param ce: cloud endpoint object
    :return: S_OK/S_ERROR, value is the path to the proxy file
    """

    vo = ce.parameters.get("VO")
    cloudDN = None
    cloudGroup = None
    if vo:
        result = findGenericCloudCredentials(vo=vo)
        if not result["OK"]:
            return result
        cloudDN, cloudGroup = result["Value"]

    cloudUser = ce.parameters.get("GenericCloudUser")
    if cloudUser:
        result = Registry.getDNForUsername(cloudUser)
        if not result["OK"]:
            return result
        cloudDN = result["Value"][0]
    cloudGroup = ce.parameters.get("GenericCloudGroup", cloudGroup)

    if cloudDN and cloudGroup:
        result = gProxyManager.getPilotProxyFromDIRACGroup(cloudDN, cloudGroup, 3600)
        if not result["OK"]:
            return result
        proxy = result["Value"]
        result = gProxyManager.dumpProxyToFile(proxy)
        return result
    else:
        return S_ERROR("Could not find generic cloud credentials")
