#!/bin/env python
"""
tests for InProcessComputingElement module
"""

import os
import shutil

import pytest

from DIRAC.Resources.Computing.test.Test_PoolComputingElement import jobScript, _stopJob
from DIRAC.WorkloadManagementSystem.Utilities.Utils import createJobWrapper

# sut
from DIRAC.Resources.Computing.InProcessComputingElement import InProcessComputingElement


@pytest.mark.slow
def test_submitJob():
    with open("testJob.py", "w") as execFile:
        execFile.write(jobScript % "1")
    os.chmod("testJob.py", 0o755)

    ce = InProcessComputingElement("InProcessCE")
    res = ce.submitJob("testJob.py", None)
    assert res["OK"] is True
    res = ce.getCEStatus()
    assert res["OK"] is True
    assert res["SubmittedJobs"] == 1
    _stopJob(1)
    for ff in ["testJob.py", "stop_job_2", "job.info", "std.out"]:
        if os.path.isfile(ff):
            os.remove(ff)

    #
    # With a job wrapper and some MP parameters
    with open("testJob.py", "w") as execFile:
        execFile.write(jobScript % "2")
    os.chmod("testJob.py", 0o755)

    jobParams = {"JobType": "User", "Executable": "testJob.py"}
    resourceParams = {"GridCE": "some_CE"}
    optimizerParams = {}

    wrapperFile = createJobWrapper(2, jobParams, resourceParams, optimizerParams, logLevel="DEBUG")["Value"][
        0
    ]  # This is not under test, assuming it works fine
    res = ce.submitJob(
        wrapperFile,
        proxy=None,
        numberOfProcessors=4,
        maxNumberOfProcessors=8,
        wholeNode=False,
        mpTag=True,
        jobDesc={"jobParams": jobParams, "resourceParams": resourceParams, "optimizerParams": optimizerParams},
    )
    assert res["OK"] is True

    res = ce.getCEStatus()
    assert res["OK"] is True
    assert res["SubmittedJobs"] == 2

    _stopJob(2)
    for ff in ["testJob.py", "stop_job_2", "job.info", "std.out"]:
        if os.path.isfile(ff):
            os.remove(ff)
    if os.path.isdir("job"):
        shutil.rmtree("job")
