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
    for i in range(9):
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
    for i in range(9):
        _stopJob(i)

    # removing testPoolCEJob files
    # this will also stop the futures unless they are already stopped!
    for i in range(9):
        try:
            os.remove(f"testPoolCEJob_{i}.py")
        except OSError:
            pass

    try:
        os.remove("testBadPoolCEJob.py")
    except OSError:
        pass


@pytest.mark.slow
def test_submit_and_shutdown(createAndDelete):
    time.sleep(0.5)

    ceParameters = {"WholeNode": True, "NumberOfProcessors": 4, "MaxRAM": 3800}
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

    ceParameters = {"WholeNode": True, "NumberOfProcessors": 4, "MaxRAM": 16000}
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
    assert result["UsedRAM"] == 0
    assert result["AvailableRAM"] == 16000
    assert result["RunningJobs"] == 1

    jobParams = {"mpTag": True, "numberOfProcessors": 2, "MaxRAM": 4000}
    result = ce.submitJob("testPoolCEJob_1.py", None, **jobParams)
    assert result["OK"] is True
    taskID = result["Value"]
    assert taskID == 1
    taskIDs[taskID] = True

    result = ce.getCEStatus()
    assert result["UsedProcessors"] == 3
    assert result["AvailableProcessors"] == 1
    assert result["UsedRAM"] == 4000
    assert result["AvailableRAM"] == 12000

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
@pytest.mark.parametrize(
    "ce_parameters",
    [
        ({"NumberOfProcessors": 8}),
        ({"NumberOfProcessors": 8, "MaxRAM": 32000}),
        ({"WholeNode": True, "NumberOfProcessors": 8, "MaxRAM": 32000}),
    ],
)
def test_executeJob_wholeNode8(createAndDelete, ce_parameters):
    time.sleep(0.5)
    taskIDs = {}

    ce = PoolComputingElement("TestPoolCE")
    ce.setParameters(ce_parameters)

    jobParams = {"mpTag": True, "numberOfProcessors": 2, "maxNumberOfProcessors": 2}
    result = ce.submitJob("testPoolCEJob_2.py", None, **jobParams)
    assert result["OK"] is True
    taskID = result["Value"]
    assert taskID == 0
    taskIDs[taskID] = True

    result = ce.getCEStatus()
    assert result["UsedProcessors"] == 2
    assert result["UsedRAM"] == 0
    assert result["AvailableRAM"] == ce_parameters.get("MaxRAM", 0)

    jobParams = {"mpTag": True, "numberOfProcessors": 1, "maxNumberOfProcessors": 3}
    result = ce.submitJob("testPoolCEJob_3.py", None, **jobParams)
    assert result["OK"] is True
    taskID = result["Value"]
    assert taskID == 1
    taskIDs[taskID] = True

    result = ce.getCEStatus()
    assert result["UsedProcessors"] == 5
    assert result["UsedRAM"] == 0
    assert result["AvailableRAM"] == ce_parameters.get("MaxRAM", 0)

    jobParams = {"numberOfProcessors": 2, "MinRAM": 4000, "MaxRAM": 8000}  # This is same as asking for SP
    result = ce.submitJob("testPoolCEJob_4.py", None, **jobParams)
    assert result["OK"] is True
    taskID = result["Value"]
    assert taskID == 2
    taskIDs[taskID] = True

    result = ce.getCEStatus()
    assert result["UsedProcessors"] == 6
    assert result["UsedRAM"] == 8000
    assert result["AvailableRAM"] == (
        ce_parameters.get("MaxRAM") - result["UsedRAM"] if ce_parameters.get("MaxRAM") else 0
    )

    jobParams = {"MinRAM": 8000, "MaxRAM": 8000}  # This is same as asking for SP
    result = ce.submitJob("testPoolCEJob_5.py", None, **jobParams)
    assert result["OK"] is True
    taskID = result["Value"]
    assert taskID == 3
    taskIDs[taskID] = True

    result = ce.getCEStatus()
    assert result["UsedProcessors"] == 7
    assert result["UsedRAM"] == 16000
    assert result["AvailableRAM"] == (
        ce_parameters.get("MaxRAM") - result["UsedRAM"] if ce_parameters.get("MaxRAM") else 0
    )

    jobParams = {"MaxRAM": 24000}  # This will fail for the case when the ce have set a RAM
    result = ce.submitJob("testPoolCEJob_6.py", None, **jobParams)
    assert result["OK"] is True
    taskID = result["Value"]
    assert taskID == 4
    if ce_parameters.get("MaxRAM"):
        assert ce.taskResults[taskID]["OK"] is False

    result = ce.getCEStatus()
    assert result["UsedProcessors"] == 7 if ce_parameters.get("MaxRAM") else 8
    assert result["UsedRAM"] == 16000 if ce_parameters.get("MaxRAM") else 40000
    assert result["AvailableRAM"] == (
        ce_parameters.get("MaxRAM") - result["UsedRAM"] if ce_parameters.get("MaxRAM") else 0
    )

    # now trying again would fail
    jobParams = {"mpTag": True, "numberOfProcessors": 3}
    result = ce.submitJob("testPoolCEJob_7.py", None, **jobParams)
    assert result["OK"] is True
    taskID = result["Value"]
    assert taskID == 5
    taskIDs[taskID] = False

    # waiting and submit again
    while len(ce.taskResults) < 2:
        time.sleep(0.1)

    jobParams = {"mpTag": True, "numberOfProcessors": 1}
    result = ce.submitJob("testPoolCEJob_8.py", None, **jobParams)
    assert result["OK"] is True
    taskID = result["Value"]
    assert taskID == 6
    taskIDs[taskID] = True

    result = ce.shutdown()
    assert result["OK"] is True
    assert isinstance(result["Value"], dict)
    assert len(result["Value"]) == 7

    while len(ce.taskResults) < 7:
        time.sleep(0.1)

    for taskID, expectedResult in taskIDs.items():
        submissionResult = ce.taskResults[taskID]
        assert submissionResult["OK"] is expectedResult
        if not submissionResult["OK"]:
            assert submissionResult["Message"] in ["Not enough processors for the job", "Not enough memory for the job"]


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
    "processorsPerTask, ramPerTask, kwargs, expected_processors, expected_memory",
    [
        (None, None, {}, 1, 0),
        (None, None, {"mpTag": False}, 1, 0),
        (None, None, {"mpTag": True, "MaxRAM": 8000}, 1, 8000),
        (None, None, {"mpTag": True, "wholeNode": True}, 16, 0),
        (None, None, {"mpTag": True, "wholeNode": False}, 1, 0),
        (None, None, {"mpTag": True, "numberOfProcessors": 4, "MinRAM": 2000}, 4, 2000),
        (None, None, {"mpTag": True, "numberOfProcessors": 4, "MaxRAM": 4000}, 4, 4000),
        (None, None, {"mpTag": True, "numberOfProcessors": 4, "MaxRAM": 36000}, 4, None),
        (None, None, {"mpTag": True, "numberOfProcessors": 4, "MinRAM": 2000, "MaxRAM": 4000}, 4, 4000),
        (None, None, {"mpTag": True, "numberOfProcessors": 4, "maxNumberOfProcessors": 8}, 8, 0),
        (None, None, {"mpTag": True, "numberOfProcessors": 4, "maxNumberOfProcessors": 32}, 16, 0),
        ({1: 4}, {1: 4000}, {"mpTag": True, "wholeNode": True}, 0, 0),
        ({1: 4}, {1: 4000}, {"mpTag": True, "wholeNode": False}, 1, 0),
        ({1: 4}, {1: 4000}, {"mpTag": True, "numberOfProcessors": 2, "MinRAM": 8000}, 2, 8000),
        ({1: 4}, {1: 4000}, {"mpTag": True, "numberOfProcessors": 16, "MinRAM": 8000, "MaxRAM": 12000}, 0, 12000),
        ({1: 4}, {1: 4000}, {"mpTag": True, "maxNumberOfProcessors": 2, "MaxRAM": 16000}, 2, 16000),
        ({1: 4}, {1: 4000}, {"mpTag": True, "numberOfProcessors": 2, "MaxRAM": 8000}, 2, 8000),
        ({1: 4}, {1: 4000}, {"mpTag": True, "maxNumberOfProcessors": 16, "MaxRAM": 32000}, 12, None),
        ({1: 4, 2: 8}, {1: 4000}, {"mpTag": True, "numberOfProcessors": 2}, 2, 0),
        ({1: 4, 2: 8}, {1: 4000}, {"mpTag": True, "numberOfProcessors": 4}, 4, 0),
        ({1: 4, 2: 8, 3: 8}, {1: 4000}, {"mpTag": True, "numberOfProcessors": 4}, 0, 0),
    ],
)
def test__getLimitsForJobs(processorsPerTask, ramPerTask, kwargs, expected_processors, expected_memory):
    ce = PoolComputingElement("TestPoolCE")
    ce.processors = 16
    ce.ram = 32000

    if processorsPerTask:
        ce.processorsPerTask = processorsPerTask
    if ramPerTask:
        ce.ramPerTask = ramPerTask
    res = ce._getProcessorsForJobs(kwargs)
    assert res == expected_processors
    res = ce._getMemoryForJobs(kwargs)
    assert res == expected_memory
