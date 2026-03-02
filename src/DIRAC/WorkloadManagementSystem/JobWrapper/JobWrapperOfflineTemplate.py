#!/usr/bin/env python
""" This template will become the job wrapper that's actually executed.

The JobWrapperOfflineTemplate is completed and invoked by the PushJobAgent and uses functionalities from JobWrapper module.
It is executed in environment where external connections are not allowed.
We assume this script is executed in a specific environment where DIRAC is available.
"""
import hashlib
import sys
import json
import os

sitePython = os.path.realpath("@SITEPYTHON@")
if sitePython:
    sys.path.insert(0, sitePython)

from DIRAC.Core.Base.Script import Script

Script.parseCommandLine()

from DIRAC import gLogger
from DIRAC.WorkloadManagementSystem.JobWrapper.JobWrapper import JobWrapper

os.umask(0o22)


def execute(arguments: dict):
    """The only real function executed here"""
    payloadParams = arguments.pop("Payload", {})
    if not payloadParams:
        return 1

    if "PayloadResults" not in arguments["Job"] or "Checksum" not in arguments["Job"]:
        return 1

    try:
        job = JobWrapper()
        job.initialize(arguments)  # initialize doesn't return S_OK/S_ERROR
    except Exception as exc:  # pylint: disable=broad-except
        gLogger.exception("JobWrapper failed the initialization phase", lException=exc)
        return 1

    payloadResult = job.process(**payloadParams)
    if not payloadResult["OK"]:
        return 1

    # Store the payload result
    with open(arguments["Job"]["PayloadResults"], "w") as f:
        json.dump(payloadResult, f)

    # Generate the checksum of the files present in the current directory
    checksums = {}
    for file in os.listdir("."):
        if not os.path.isfile(file):
            continue
        with open(file, "rb") as f:
            digest = hashlib.file_digest(f, "sha256")
        checksums[file] = digest.hexdigest()

    with open(arguments["Job"]["Checksum"], "w") as f:
        json.dump(checksums, f)

    return 0


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

    ret = execute(jobArgs)
except Exception:  # pylint: disable=broad-except
    gLogger.exception("JobWrapperTemplate exception")
    ret = -1

sys.exit(ret)
