#!/usr/bin/env python
""" This template will become the job wrapper that's actually executed.

    The JobWrapperTemplate is completed and invoked by the jobAgent and uses functionalities from JobWrapper module.
    It has to be an executable.

    The JobWrapperTemplate will reschedule the job according to certain criteria:
    - the working directory could not be created
    - the jobWrapper initialization phase failed
    - the inputSandbox download failed
    - the resolution of the inpt data failed
    - the JobWrapper ended with the status DErrno.EWMSRESC
"""
import sys
import json
import os

sitePython = os.path.realpath("@SITEPYTHON@")
if sitePython:
    sys.path.insert(0, sitePython)

from DIRAC.Core.Base.Script import Script

Script.parseCommandLine()

from DIRAC import gLogger
from DIRAC.WorkloadManagementSystem.Client.JobReport import JobReport
from DIRAC.WorkloadManagementSystem.JobWrapper.JobWrapperUtilities import (
    createAndEnterWorkingDirectory,
    executePayload,
    finalize,
    getJobWrapper,
    processJobOutputs,
    resolveInputData,
    transferInputSandbox,
)


os.umask(0o22)


def execute(jobID: int, arguments: dict, jobReport: JobReport):
    """The only real function executed here"""

    if "WorkingDirectory" in arguments:
        if not createAndEnterWorkingDirectory(jobID, arguments["WorkingDirectory"], jobReport):
            return 1

    job = getJobWrapper(jobID, arguments, jobReport)
    if not job:
        return 1

    if "InputSandbox" in arguments["Job"]:
        jobReport.commit()
        if not transferInputSandbox(job, arguments["Job"]["InputSandbox"]):
            return 1
    else:
        gLogger.verbose("Job has no InputSandbox requirement")

    jobReport.commit()

    if "InputData" in arguments["Job"]:
        if arguments["Job"]["InputData"]:
            if not resolveInputData(job):
                return 1
        else:
            gLogger.verbose("Job has a null InputData requirement:")
            gLogger.verbose(arguments)
    else:
        gLogger.verbose("Job has no InputData requirement")

    jobReport.commit()

    if not executePayload(job):
        return 1

    if "OutputSandbox" in arguments["Job"] or "OutputData" in arguments["Job"]:
        if not processJobOutputs(job):
            return 2
    else:
        gLogger.verbose("Job has no OutputData or OutputSandbox requirement")

    return finalize(job)


##########################################################


ret = -3
try:
    jsonFileName = os.path.realpath(__file__) + ".json"
    with open(jsonFileName) as f:
        jobArgs = json.load(f)
    if not isinstance(jobArgs, dict):
        raise TypeError(f"jobArgs is of type {type(jobArgs)}")
    if "Job" not in jobArgs:
        raise ValueError(f"jobArgs does not contain 'Job' key: {str(jobArgs)}")

    jobID = jobArgs["Job"].get("JobID", 0)
    jobID = int(jobID)
    jobReport = JobReport(jobID, "JobWrapper")

    ret = execute(jobID, jobArgs, jobReport)
    jobReport.commit()
except Exception:  # pylint: disable=broad-except
    gLogger.exception("JobWrapperTemplate exception")
    try:
        jobReport.commit()
        ret = -1
    except Exception:  # pylint: disable=broad-except
        gLogger.exception("Could not commit the job report")
        ret = -2

sys.exit(ret)
