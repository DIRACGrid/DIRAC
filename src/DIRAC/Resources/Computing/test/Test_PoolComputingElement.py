#!/bin/env python
"""
tests for PoolComputingElement module
"""
import os
import time

import pytest

# sut
from DIRAC import S_ERROR
from DIRAC.Resources.Computing.PoolComputingElement import PoolComputingElement

jobScript = """#!/usr/bin/env python
import time
import os

jobNumber = %s
stopFile = 'stop_job_' + str(jobNumber)
start = time.time()

print("Start job", jobNumber, start)
while True:
  time.sleep(0.1)
  if os.path.isfile(stopFile):
    os.remove(stopFile)
    break
  if (time.time() - start) > 5:
    break
print("End job", jobNumber, time.time())
"""

badJobScript = """#!/usr/bin/env python

import sys
import time

time.sleep(10)
sys.exit(5)
"""


def _stopJob(nJob):
    with open(f"stop_job_{nJob}", "w") as stopFile:
        stopFile.write("Stop")
    time.sleep(0.3)
    if os.path.isfile(f"stop_job_{nJob}"):
        os.remove(f"stop_job_{nJob}")


@pytest.fixture
def createAndDelete():
    for i in range(6):
        with open(f"testPoolCEJob_{i}.py", "w") as execFile:
            execFile.write(jobScript % i)
        os.chmod(f"testPoolCEJob_{i}.py", 0o755)

    with open("testBadPoolCEJob.py", "w") as execFile:
        execFile.write(badJobScript)
    os.chmod("testBadPoolCEJob.py", 0o755)

    yield createAndDelete

    # from here on is teardown

    time.sleep(0.5)

    # stopping the jobs
    for i in range(6):
        _stopJob(i)

    # removing testPoolCEJob files
    # this will also stop the futures unless they are already stopped!
    for i in range(6):
        try:
            os.remove(f"testPoolCEJob_{i}.py")
            os.remove("testBadPoolCEJob.py")
        except OSError:
            pass


@pytest.mark.slow
def test_submit_and_shutdown(createAndDelete):
    time.sleep(0.5)

    ceParameters = {"WholeNode": True, "NumberOfProcessors": 4}
    ce = PoolComputingElement("TestPoolCE")
    ce.setParameters(ceParameters)

    result = ce.submitJob("testPoolCEJob_0.py", None)
    assert result["OK"] is True

    result = ce.shutdown()
    assert result["OK"] is True
    assert isinstance(result["Value"], dict)
    assert list(result["Value"].values())[0]["OK"] is True


@pytest.mark.slow
@pytest.mark.parametrize(
    "script, ceSubmissionFailure, expected",
    [
        # The script is fine, but the InProcess submission is going to fail
        ("testPoolCEJob_0.py", True, False),
        # The script is wrong, but the InProcess submission will be fine
        ("testBadPoolCEJob.py", False, True),
    ],
)
def test_submitBadJobs_and_getResult(mocker, createAndDelete, script, ceSubmissionFailure, expected):
    """Consists in testing failures during the submission process or the job execution"""
    # Mocker configuration
    # Only enabled if ceSubmissionFailure is True
    proxy = None
    if ceSubmissionFailure:
        mocker.patch(
            "DIRAC.Resources.Computing.ComputingElement.ComputingElement.writeProxyToFile",
            return_value=S_ERROR("Unexpected failure"),
        )
        proxy = "any value to go in the branch that will fail the submission"

    time.sleep(0.5)

    ceParameters = {"WholeNode": True, "NumberOfProcessors": 4}
    ce = PoolComputingElement("TestPoolCE")
    ce.setParameters(ceParameters)
    result = ce.submitJob(script, proxy=proxy)

    # The PoolCE always return S_OK
    # It cannot capture failures occurring during the submission or after
    # because it is asynchronous
    assert result["OK"] is True
    assert result["Value"] == 0

    # Waiting for the results of the submission/execution of the script
    while not ce.taskResults:
        time.sleep(0.1)

    # Test the results
    for _, result in ce.taskResults.items():
        assert result["OK"] == expected


@pytest.mark.slow
def test_executeJob_wholeNode4(createAndDelete):
    time.sleep(0.5)
    taskIDs = {}

    ceParameters = {"WholeNode": True, "NumberOfProcessors": 4}
    ce = PoolComputingElement("TestPoolCE")
    ce.setParameters(ceParameters)

    # Test that max 4 processors can be used at a time
    result = ce.submitJob("testPoolCEJob_0.py", None)
    assert result["OK"] is True
    taskID = result["Value"]
    assert taskID == 0
    taskIDs[taskID] = True

    result = ce.getCEStatus()
    assert result["UsedProcessors"] == 1
    assert result["AvailableProcessors"] == 3
    assert result["RunningJobs"] == 1

    jobParams = {"mpTag": True, "numberOfProcessors": 2}
    result = ce.submitJob("testPoolCEJob_1.py", None, **jobParams)
    assert result["OK"] is True
    taskID = result["Value"]
    assert taskID == 1
    taskIDs[taskID] = True

    result = ce.getCEStatus()
    assert result["UsedProcessors"] == 3
    assert result["AvailableProcessors"] == 1
    assert result["RunningJobs"] == 2

    # now trying again would fail
    jobParams = {"mpTag": True, "numberOfProcessors": 2}
    result = ce.submitJob("testPoolCEJob_1.py", None, **jobParams)
    assert result["OK"] is True
    taskID = result["Value"]
    assert taskID == 2
    taskIDs[taskID] = False

    while len(ce.taskResults) < 3:
        time.sleep(0.1)

    for taskID, expectedResult in taskIDs.items():
        submissionResult = ce.taskResults[taskID]
        assert submissionResult["OK"] is expectedResult


@pytest.mark.slow
def test_executeJob_wholeNode8(createAndDelete):
    time.sleep(0.5)
    taskIDs = {}

    ceParameters = {"WholeNode": True, "NumberOfProcessors": 8}
    ce = PoolComputingElement("TestPoolCE")
    ce.setParameters(ceParameters)

    jobParams = {"mpTag": True, "numberOfProcessors": 2, "maxNumberOfProcessors": 2}
    result = ce.submitJob("testPoolCEJob_2.py", None, **jobParams)
    assert result["OK"] is True
    taskID = result["Value"]
    assert taskID == 0
    taskIDs[taskID] = True

    result = ce.getCEStatus()
    assert result["UsedProcessors"] == 2

    jobParams = {"mpTag": True, "numberOfProcessors": 1, "maxNumberOfProcessors": 3}
    result = ce.submitJob("testPoolCEJob_3.py", None, **jobParams)
    assert result["OK"] is True
    taskID = result["Value"]
    assert taskID == 1
    taskIDs[taskID] = True

    result = ce.getCEStatus()
    assert result["UsedProcessors"] == 5

    jobParams = {"numberOfProcessors": 2}  # This is same as asking for SP
    result = ce.submitJob("testPoolCEJob_4.py", None, **jobParams)
    assert result["OK"] is True
    taskID = result["Value"]
    assert taskID == 2
    taskIDs[taskID] = True

    result = ce.getCEStatus()
    assert result["UsedProcessors"] == 6

    # now trying again would fail
    jobParams = {"mpTag": True, "numberOfProcessors": 3}
    result = ce.submitJob("testPoolCEJob_5.py", None, **jobParams)
    assert result["OK"] is True
    taskID = result["Value"]
    assert taskID == 3
    taskIDs[taskID] = False

    # waiting and submit again
    while len(ce.taskResults) < 2:
        time.sleep(0.1)

    jobParams = {"mpTag": True, "numberOfProcessors": 3}
    result = ce.submitJob("testPoolCEJob_5.py", None, **jobParams)
    assert result["OK"] is True
    taskID = result["Value"]
    assert taskID == 4
    taskIDs[taskID] = True

    result = ce.shutdown()
    assert result["OK"] is True
    assert isinstance(result["Value"], dict)
    assert len(result["Value"]) == 5

    while len(ce.taskResults) < 5:
        time.sleep(0.1)

    for taskID, expectedResult in taskIDs.items():
        submissionResult = ce.taskResults[taskID]
        assert submissionResult["OK"] is expectedResult
        if not submissionResult["OK"]:
            assert "Not enough processors" in submissionResult["Message"]


@pytest.mark.slow
def test_executeJob_submitAndStop(createAndDelete):
    time.sleep(0.5)

    ceParameters = {"WholeNode": True, "NumberOfProcessors": 4}
    ce = PoolComputingElement("TestPoolCE")
    ce.setParameters(ceParameters)

    jobParams = {"mpTag": True, "numberOfProcessors": 2, "maxNumberOfProcessors": 2}
    result = ce.submitJob("testPoolCEJob_0.py", None, **jobParams)
    assert result["OK"] is True
    result = ce.getCEStatus()
    assert result["UsedProcessors"] == 2
    assert result["AvailableProcessors"] == 2
    assert result["RunningJobs"] == 1

    time.sleep(5)
    _stopJob(0)
    # Allow job to stop
    time.sleep(2)

    result = ce.getCEStatus()
    assert result["RunningJobs"] == 0
    assert result["UsedProcessors"] == 0
    assert result["AvailableProcessors"] == 4


@pytest.mark.slow
def test_executeJob_WholeNodeJobs(createAndDelete):
    time.sleep(0.5)
    taskIDs = {}

    ce = PoolComputingElement("TestPoolCE")
    ceParameters = {"WholeNode": False, "NumberOfProcessors": 4}
    ce.setParameters(ceParameters)

    jobParams = {"mpTag": True, "numberOfProcessors": 2, "maxNumberOfProcessors": 2}
    result = ce.submitJob("testPoolCEJob_0.py", None, **jobParams)
    assert result["OK"] is True
    taskID = result["Value"]
    assert taskID == 0
    taskIDs[taskID] = True

    jobParams = {"mpTag": True, "numberOfProcessors": 2}
    result = ce.submitJob("testPoolCEJob_1.py", None, **jobParams)
    assert result["OK"] is True
    taskID = result["Value"]
    assert taskID == 1
    taskIDs[taskID] = True

    result = ce.getCEStatus()
    assert result["UsedProcessors"] == 4
    assert result["AvailableProcessors"] == 0
    assert result["RunningJobs"] == 2

    # Allow jobs to start, then stopping them
    time.sleep(5)
    for i in range(2):
        _stopJob(i)
    # Allow jobs to stop
    time.sleep(2)

    result = ce.getCEStatus()
    assert result["UsedProcessors"] == 0

    # Trying with whole node jobs
    result = ce.submitJob("testPoolCEJob_2.py", None)  # first 1 SP job
    assert result["OK"] is True
    taskID = result["Value"]
    assert taskID == 2
    taskIDs[taskID] = True

    result = ce.getCEStatus()
    assert result["UsedProcessors"] == 1

    jobParams = {"mpTag": True, "wholeNode": True}
    result = ce.submitJob("testPoolCEJob_3.py", None, **jobParams)
    assert result["OK"] is True
    taskID = result["Value"]
    assert taskID == 3
    taskIDs[taskID] = False

    # Allow job to start
    time.sleep(5)

    _stopJob(2)
    # Allow job to stop
    time.sleep(2)

    jobParams = {"mpTag": True, "wholeNode": True}
    result = ce.submitJob("testPoolCEJob_4.py", None, **jobParams)
    assert result["OK"] is True
    taskID = result["Value"]
    assert taskID == 4
    taskIDs[taskID] = True

    result = ce.getCEStatus()
    assert result["UsedProcessors"] == 4

    while len(ce.taskResults) < 5:
        time.sleep(0.1)

    for taskID, expectedResult in taskIDs.items():
        submissionResult = ce.taskResults[taskID]
        assert submissionResult["OK"] is expectedResult
        if not submissionResult["OK"]:
            assert "Not enough processors" in submissionResult["Message"]


@pytest.mark.parametrize(
    "processorsPerTask, kwargs, expected",
    [
        (None, {}, 1),
        (None, {"mpTag": False}, 1),
        (None, {"mpTag": True}, 1),
        (None, {"mpTag": True, "wholeNode": True}, 16),
        (None, {"mpTag": True, "wholeNode": False}, 1),
        (None, {"mpTag": True, "numberOfProcessors": 4}, 4),
        (None, {"mpTag": True, "numberOfProcessors": 4, "maxNumberOfProcessors": 8}, 8),
        (None, {"mpTag": True, "numberOfProcessors": 4, "maxNumberOfProcessors": 32}, 16),
        ({1: 4}, {"mpTag": True, "wholeNode": True}, 0),
        ({1: 4}, {"mpTag": True, "wholeNode": False}, 1),
        ({1: 4}, {"mpTag": True, "numberOfProcessors": 2}, 2),
        ({1: 4}, {"mpTag": True, "maxNumberOfProcessors": 2}, 2),
        ({1: 4}, {"mpTag": True, "maxNumberOfProcessors": 16}, 12),
    ],
)
def test__getProcessorsForJobs(processorsPerTask, kwargs, expected):
    ce = PoolComputingElement("TestPoolCE")
    ce.processors = 16

    if processorsPerTask:
        ce.processorsPerTask = processorsPerTask
    res = ce._getProcessorsForJobs(kwargs)
    assert res == expected
