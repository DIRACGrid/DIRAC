#!/bin/env python
"""
tests for InProcessComputingElement module
"""

import os
import shutil

import pytest

# sut
from DIRAC.Resources.Computing.InProcessComputingElement import InProcessComputingElement
from DIRAC.Resources.Computing.test.Test_PoolComputingElement import _stopJob, jobScript
from DIRAC.WorkloadManagementSystem.Utilities.Utils import createJobWrapper


@pytest.mark.slow
@pytest.mark.parametrize(
    "ce_parameters, available_processors, ram",
    [
        ({}, 1, 0),
        ({"NumberOfProcessors": 8}, 8, 0),
        ({"MaxRAM": 2048}, 1, 2048),
        ({"NumberOfProcessors": 8, "MaxRAM": 2048}, 8, 2048),
    ],
)
def test_submitJob(ce_parameters, available_processors, ram):
    # initialization
    ce = InProcessComputingElement("InProcessCE")
    ce.ceParameters = ce_parameters

    # simple
    with open("testJob.py", "w") as execFile:
        execFile.write(jobScript % "1")
    os.chmod("testJob.py", 0o755)

    res = ce.submitJob("testJob.py", None)
    assert res["OK"] is True
    res = ce.getCEStatus()
    assert res["OK"] is True
    assert res["SubmittedJobs"] == 1
    assert res["RunningJobs"] == 0
    assert res["WaitingJobs"] == 0
    assert res["AvailableProcessors"] == available_processors
    assert res["AvailableRAM"] == ram
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

    wrapperFile = createJobWrapper(
        jobID=2, jobParams=jobParams, resourceParams=resourceParams, optimizerParams=optimizerParams, logLevel="DEBUG"
    )["Value"][
        "JobExecutablePath"
    ]  # This is not under test, assuming it works fine
    res = ce.submitJob(
        wrapperFile,
        proxy=None,
        numberOfProcessors=available_processors,
        maxNumberOfProcessors=available_processors,
        wholeNode=False,
        mpTag=True,
        MinRAM=ram,
        MaxRAM=ram,
        jobDesc={"jobParams": jobParams, "resourceParams": resourceParams, "optimizerParams": optimizerParams},
    )
    assert res["OK"] is True
    _stopJob(2)

    res = ce.getCEStatus()
    assert res["OK"] is True
    assert res["SubmittedJobs"] == 2
    assert res["RunningJobs"] == 0
    assert res["WaitingJobs"] == 0
    assert res["AvailableProcessors"] == available_processors
    assert res["AvailableRAM"] == ram

    for ff in ["testJob.py", "stop_job_2", "job.info", "std.out"]:
        if os.path.isfile(ff):
            os.remove(ff)
    if os.path.isdir("job"):
        shutil.rmtree("job")

    # failing
    with open("testJob.py", "w") as execFile:
        execFile.write(jobScript % "3")
    os.chmod("testJob.py", 0o755)

    jobParams = {"JobType": "User", "Executable": "testJob.py"}
    resourceParams = {"GridCE": "some_CE"}
    optimizerParams = {}

    wrapperFile = createJobWrapper(
        jobID=3, jobParams=jobParams, resourceParams=resourceParams, optimizerParams=optimizerParams, logLevel="DEBUG"
    )["Value"][
        "JobExecutablePath"
    ]  # This is not under test, assuming it works fine

    res = ce.submitJob(
        wrapperFile,
        proxy=None,
        numberOfProcessors=4 + available_processors,
        maxNumberOfProcessors=8 + available_processors,
        wholeNode=False,
        mpTag=True,
        MinRAM=2500,
        MaxRAM=4000,
        jobDesc={"jobParams": jobParams, "resourceParams": resourceParams, "optimizerParams": optimizerParams},
    )
    assert res["OK"] is False
    res = ce.getCEStatus()
    assert res["OK"] is True
    assert res["SubmittedJobs"] == 2
    assert res["RunningJobs"] == 0
    assert res["WaitingJobs"] == 0
    assert res["AvailableProcessors"] == available_processors
    assert res["AvailableRAM"] == ram
    _stopJob(1)
    for ff in ["testJob.py", "stop_job_3", "job.info", "std.out"]:
        if os.path.isfile(ff):
            os.remove(ff)
