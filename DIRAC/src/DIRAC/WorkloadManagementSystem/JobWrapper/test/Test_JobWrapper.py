""" Test class for JobWrapper
"""
import os
import shutil
import pytest
from unittest.mock import MagicMock

from DIRAC import gLogger

from DIRAC.DataManagementSystem.Client.test.mock_DM import dm_mock
from DIRAC.Resources.Catalog.test.mock_FC import fc_mock

from DIRAC.WorkloadManagementSystem.JobWrapper.JobWrapper import JobWrapper
from DIRAC.WorkloadManagementSystem.JobWrapper.Watchdog import Watchdog
from DIRAC.WorkloadManagementSystem.Client import JobStatus, JobMinorStatus

getSystemSectionMock = MagicMock()
getSystemSectionMock.return_value = "aValue"

gLogger.setLevel("DEBUG")


def test_InputData(mocker):
    mocker.patch(
        "DIRAC.WorkloadManagementSystem.JobWrapper.JobWrapper.getSystemSection", side_effect=getSystemSectionMock
    )
    mocker.patch("DIRAC.WorkloadManagementSystem.JobWrapper.JobWrapper.ObjectLoader", side_effect=MagicMock())

    jw = JobWrapper()
    jw.jobArgs["InputData"] = ""
    res = jw.resolveInputData()
    assert res["OK"] is False

    jw = JobWrapper()
    jw.jobArgs["InputData"] = "pippo"
    jw.dm = dm_mock
    jw.fc = fc_mock
    res = jw.resolveInputData()
    assert res["OK"]

    jw = JobWrapper()
    jw.jobArgs["InputData"] = "pippo"
    jw.jobArgs["LocalSE"] = "mySE"
    jw.jobArgs["InputDataModule"] = "aa.bb"
    jw.dm = dm_mock
    jw.fc = fc_mock
    res = jw.resolveInputData()
    assert res["OK"]


def test_performChecks():
    wd = Watchdog(
        pid=os.getpid(),
        exeThread=MagicMock(),
        spObject=MagicMock(),
        jobCPUTime=1000,
        memoryLimit=1024 * 1024,
        jobArgs={"StopSigNumber": 10},
    )
    res = wd._performChecks()
    assert res["OK"]


@pytest.mark.slow
@pytest.mark.parametrize(
    "executable, args, src, expectedResult",
    [
        ("/bin/ls", None, None, "Application Finished Successfully"),
        (
            "script-OK.sh",
            None,
            "src/DIRAC/WorkloadManagementSystem/JobWrapper/test/",
            "Application Finished Successfully",
        ),
        ("script.sh", "111", "src/DIRAC/WorkloadManagementSystem/JobWrapper/test/", "Application Finished With Errors"),
        ("script.sh", 111, "src/DIRAC/WorkloadManagementSystem/JobWrapper/test/", "Application Finished With Errors"),
        ("script-RESC.sh", None, "src/DIRAC/WorkloadManagementSystem/JobWrapper/test/", "Going to reschedule job"),
        (
            "src/DIRAC/WorkloadManagementSystem/scripts/dirac_jobexec.py",
            "src/DIRAC/WorkloadManagementSystem/JobWrapper/test/jobDescription.xml -o /DIRAC/Setup=Test",
            None,
            "Application Finished Successfully",
        ),
    ],
)
def test_execute(mocker, executable, args, src, expectedResult):
    """Test the status of the job after JobWrapper.execute().
    The returned value of JobWrapper.execute() is not checked as it can apparently be wrong depending on the shell used.
    """

    mocker.patch(
        "DIRAC.WorkloadManagementSystem.JobWrapper.JobWrapper.getSystemSection", side_effect=getSystemSectionMock
    )
    mocker.patch(
        "DIRAC.WorkloadManagementSystem.JobWrapper.Watchdog.getSystemInstance", side_effect=getSystemSectionMock
    )

    if src:
        shutil.copy(os.path.join(src, executable), executable)

    jw = JobWrapper()
    jw.jobArgs = {"Executable": executable}
    if args:
        jw.jobArgs["Arguments"] = args
    res = jw.execute()
    assert expectedResult in jw.jobReport.jobStatusInfo[-1]

    if src:
        os.remove(executable)

    if os.path.exists("std.out"):
        os.remove("std.out")


@pytest.mark.parametrize(
    "failedFlag, expectedRes, finalStates",
    [
        (True, 1, [JobStatus.FAILED, ""]),
        (False, 0, [JobStatus.DONE, JobMinorStatus.EXEC_COMPLETE]),
    ],
)
def test_finalize(mocker, failedFlag, expectedRes, finalStates):
    mocker.patch(
        "DIRAC.WorkloadManagementSystem.JobWrapper.JobWrapper.getSystemSection", side_effect=getSystemSectionMock
    )
    mocker.patch("DIRAC.WorkloadManagementSystem.JobWrapper.JobWrapper.ObjectLoader", side_effect=MagicMock())

    jw = JobWrapper()
    jw.jobArgs = {"Executable": "/bin/ls"}
    jw.failedFlag = failedFlag

    res = jw.finalize()

    assert res == expectedRes
    assert jw.jobReport.jobStatusInfo[0][0] == finalStates[0]
    assert jw.jobReport.jobStatusInfo[0][1] == finalStates[1]
