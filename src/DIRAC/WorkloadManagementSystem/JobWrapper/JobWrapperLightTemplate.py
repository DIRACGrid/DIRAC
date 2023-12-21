#!/usr/bin/env python
""" This template will become the job wrapper that's actually executed.

    The JobWrapperLighTemplate is completed and invoked by the PushJobAgent and uses functionalities from JobWrapper module.
    It is executed in environment where external connections are not allowed.

    The JobWrapperTemplate will reschedule the job according to certain criteria:
    - the working directory could not be created
    - the jobWrapper initialization phase failed
    - the inputSandbox download failed
    - the resolution of the inpt data failed
    - the JobWrapper ended with the status DErrno.EWMSRESC
"""
import hashlib
import sys
import json
import ast
import os

from DIRAC.WorkloadManagementSystem.JobWrapper.JobWrapperUtilities import getJobWrapper

sitePython = os.path.realpath("@SITEPYTHON@")
if sitePython:
    sys.path.insert(0, sitePython)

from DIRAC.Core.Base.Script import Script

Script.parseCommandLine()

from DIRAC import gLogger
from DIRAC.WorkloadManagementSystem.Client.JobReport import JobReport


os.umask(0o22)


def execute(jobID: str, arguments: dict, jobReport: JobReport):
    """The only real function executed here"""
    payloadParams = arguments.pop("Payload", {})
    if not payloadParams:
        return 1

    job = getJobWrapper(jobID, arguments, jobReport)
    payloadResult = job.process(**payloadParams)
    if not payloadResult["OK"]:
        return 1

    if not "PayloadResults" in arguments["Job"] or not "Checksum" in arguments["Job"]:
        return 1

    # Store the payload result
    with open(arguments["Job"]["PayloadResults"], "w") as f:
        json.dump(payloadResult, f)

    # Generate the checksum of the files present in the current directory
    checksums = {}
    for file in os.listdir("."):
        if os.path.isfile(file):
            hash_md5 = hashlib.md5()
            with open(file, "rb") as f:
                while chunk := f.read(128 * hash.block_size):
                    hash_md5.update(chunk)
            checksums[file] = hash_md5.hexdigest()

    with open(arguments["Job"]["Checksum"], "w") as f:
        json.dump(checksums, f)

    return 0


##########################################################


ret = -3
try:
    jsonFileName = os.path.realpath(__file__) + ".json"
    with open(jsonFileName) as f:
        jobArgsFromJSON = json.loads(f.readlines()[0])
    jobArgs = ast.literal_eval(jobArgsFromJSON)
    if not isinstance(jobArgs, dict):
        raise TypeError(f"jobArgs is of type {type(jobArgs)}")
    if "Job" not in jobArgs:
        raise ValueError(f"jobArgs does not contain 'Job' key: {str(jobArgs)}")

    jobID = jobArgs["Job"].get("JobID", 0)
    jobID = int(jobID)
    jobReport = JobReport(jobID, "JobWrapper")

    ret = execute(jobID, jobArgs, jobReport)
    jobReport.commit()
except Exception as exc:  # pylint: disable=broad-except
    gLogger.exception("JobWrapperTemplate exception")
    try:
        jobReport.commit()
        ret = -1
    except Exception as exc:  # pylint: disable=broad-except
        gLogger.exception("Could not commit the job report")
        ret = -2

sys.exit(ret)
