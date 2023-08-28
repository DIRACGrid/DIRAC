#!/usr/bin/env python
""" Submission of test jobs for use by Jenkins
"""
# pylint: disable=wrong-import-position,unused-wildcard-import,wildcard-import

import os.path

import DIRAC

DIRAC.initialize()  # Initialize configuration

from DIRAC import gLogger

from DIRAC.tests.Utilities.utils import find_all

from DIRAC.Interfaces.API.Job import Job
from DIRAC.Interfaces.API.Dirac import Dirac

# from tests.Workflow.Integration.Test_UserJobs import createJob

gLogger.setLevel("DEBUG")

cwd = os.path.realpath(".")

dirac = Dirac()


def base():
    job = Job()
    executablePath = find_all("exe-script.py", "..", "/DIRAC/tests/Workflow/")[0]
    job.setInputSandbox([executablePath])
    job.setExecutable(executablePath, "", "helloWorld.log")
    job.setCPUTime(1780)
    job.setDestination("DIRAC.Jenkins.ch")
    job.setLogLevel("DEBUG")
    return job


def helloJob():
    """Simple Hello Word job to DIRAC.Jenkins.ch"""
    gLogger.info("\n Submitting hello world job targeting DIRAC.Jenkins.ch")
    job = base()
    job.setName("helloWorld-Jenkins_base")
    result = dirac.submitJob(job)
    gLogger.info("Hello world job: ", result)
    if not result["OK"]:
        gLogger.error("Problem submitting job", result["Message"])
        exit(1)


def helloMP():
    """Simple Hello Word job to DIRAC.Jenkins.ch, that needs to be matched by a MP WN"""
    gLogger.info("\n Submitting hello world job targeting DIRAC.Jenkins.ch and a MP WN")
    job = base()
    job.setName("helloWorld-Jenkins_MP")
    job.setNumberOfProcessors(2)
    result = dirac.submitJob(job)
    gLogger.info("Hello world job MP: ", result)
    if not result["OK"]:
        gLogger.error("Problem submitting job", result["Message"])
        exit(1)


# let's sumbit 6 jobs (3 and 3)
for _ in range(3):
    helloJob()
    helloMP()
