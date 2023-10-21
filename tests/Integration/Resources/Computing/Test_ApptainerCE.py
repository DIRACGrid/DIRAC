#!/bin/env python

""" This integration test is for "Inner" Computing Element ApptainerComputingElement
    This test is here and not in the unit tests because it requires apptainer to be installed.
"""

import six
import os
import shutil

from DIRAC import gLogger
from DIRAC.tests.Utilities.utils import find_all

from DIRAC.Resources.Computing.test.Test_PoolComputingElement import jobScript, _stopJob
from DIRAC.WorkloadManagementSystem.Utilities.Utils import createJobWrapper

# sut
from DIRAC.Resources.Computing.ApptainerComputingElement import ApptainerComputingElement


gLogger.setLevel("DEBUG")
fj = find_all("pilot.json", "../", "tests/Integration/Resources/Computing")[0]
fc = find_all("pilot.cfg.test", "../", "tests/Integration/Resources/Computing")[0]


def test_submitJob():
    shutil.copy(fj, os.curdir)
    shutil.copyfile(fc, os.path.join(os.curdir, "pilot.cfg"))
    with open("testJob.py", "w") as execFile:
        execFile.write(jobScript % "1")
    os.chmod("testJob.py", 0o755)

    ce = ApptainerComputingElement("ApptainerComputingElement")
    res = ce.submitJob("testJob.py", None)
    assert res["OK"] is False
    assert res["ReschedulePayload"] is True
    res = ce.getCEStatus()
    assert res["OK"] is True
    if six.PY2:
        assert res["SubmittedJobs"] == 0
    else:
        assert res["SubmittedJobs"] == 1
    _stopJob(1)
    for ff in ["testJob.py", "pilot.json"]:
        if os.path.isfile(ff):
            os.remove(ff)


def test_submitJobWrapper():
    with open("testJob.py", "w") as execFile:
        execFile.write(jobScript % "2")
    os.chmod("testJob.py", 0o755)

    jobParams = {"JobType": "User", "Executable": "testJob.py"}
    resourceParams = {"GridCE": "some_CE"}
    optimizerParams = {}

    wrapperFile = createJobWrapper(2, jobParams, resourceParams, optimizerParams, logLevel="DEBUG")[
        "Value"
    ]  # This is not under test, assuming it works fine

    shutil.copy(fj, os.curdir)

    ce = ApptainerComputingElement("ApptainerComputingElement")
    res = ce.submitJob(
        wrapperFile,
        proxy=None,
        numberOfProcessors=4,
        maxNumberOfProcessors=8,
        wholeNode=False,
        mpTag=True,
        jobDesc={"jobParams": jobParams, "resourceParams": resourceParams, "optimizerParams": optimizerParams},
    )

    assert res["OK"] is False  # This is False because the image can't be found
    assert res["ReschedulePayload"] is True

    res = ce.getCEStatus()
    assert res["OK"] is True
    if six.PY2:
        assert res["SubmittedJobs"] == 0
    else:
        assert res["SubmittedJobs"] == 1

    _stopJob(2)
    for ff in ["testJob.py", "stop_job_2", "job.info", "std.out", "pilot.json"]:
        if os.path.isfile(ff):
            os.remove(ff)
    if os.path.isdir("job"):
        shutil.rmtree("job")
