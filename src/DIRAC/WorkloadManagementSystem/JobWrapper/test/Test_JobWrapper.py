""" Test class for JobWrapper
"""
import os
import shutil
import pytest
from unittest.mock import MagicMock
from pathlib import Path

from DIRAC import gLogger

from DIRAC.DataManagementSystem.Client.test.mock_DM import dm_mock
from DIRAC.Resources.Catalog.test.mock_FC import fc_mock

from DIRAC.WorkloadManagementSystem.JobWrapper.JobWrapper import JobWrapper
from DIRAC.WorkloadManagementSystem.JobWrapper.Watchdog import Watchdog
from DIRAC.WorkloadManagementSystem.Client import JobStatus, JobMinorStatus

getSystemSectionMock = MagicMock()
getSystemSectionMock.return_value = "aValue"
uploadSandboxMock = MagicMock()
uploadSandboxMock.return_value = {"OK": True}


def uploadFileMockFunc(**kwargs):
    destinationSEList = kwargs["destinationSEList"]
    return {"OK": True, "Value": {"uploadedSE": destinationSEList[0]}}


gLogger.setLevel("DEBUG")


def test_InputData(mocker):
    mocker.patch(
        "DIRAC.WorkloadManagementSystem.JobWrapper.JobWrapper.getSystemSection", side_effect=getSystemSectionMock
    )
    mocker.patch("DIRAC.WorkloadManagementSystem.JobWrapper.JobWrapper.ModuleFactory", side_effect=MagicMock())

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


@pytest.fixture
def jobIDPath():
    """Return the path to the job ID file."""
    # Create a temporary directory named ./123123/
    jobid = "123123"
    p = Path(jobid)
    if p.exists():
        shutil.rmtree(jobid)
    p.mkdir()

    # Output sandbox files
    (p / "std.out").touch()
    (p / "std.err").touch()
    # Output data files
    (p / "00232454_00000244.xml").touch()
    (p / "result_dir").mkdir()
    (p / "result_dir" / "output.xml").touch()
    (p / "result_dir" / "output.txt").touch()
    (p / "00232454_00000244_1.sim").touch()
    (p / "1720442808testFileUpload.txt").touch()
    (p / "testFileUploadFullLFN.txt").touch()

    yield int(jobid)

    # Remove the temporary directory
    shutil.rmtree(jobid)


@pytest.mark.parametrize(
    "outputData, outputPath, expectedResult",
    [
        (
            "00232454_00000244.xml",
            None,
            "/dirac/user/u/unknown/123/123123/00232454_00000244.xml",
        ),
        (
            "00232454_00000244*",
            None,
            "/dirac/user/u/unknown/123/123123/00232454_00000244.xml, "
            "/dirac/user/u/unknown/123/123123/00232454_00000244_1.sim",
        ),
        (
            "*.txt",
            None,
            "/dirac/user/u/unknown/123/123123/1720442808testFileUpload.txt, "
            "/dirac/user/u/unknown/123/123123/testFileUploadFullLFN.txt",
        ),
        (
            "00232454_00000244.xml",
            "/my_output_dir/00232454",
            "/dirac/user/u/unknown/my_output_dir/00232454/00232454_00000244.xml",
        ),
        (
            "00232454_00000244.xml",
            "LFN:/dirac/prod/00232454",
            "/dirac/prod/00232454/00232454_00000244.xml",
        ),
        (
            "LFN:/dirac/prod/00232454/00232454_00000244.xml",
            None,
            "/dirac/prod/00232454/00232454_00000244.xml",
        ),
        (
            "LFN:/dirac/prod/00232454/00232454_00000244.xml",
            "/my_output_dir/00232454",
            "/dirac/prod/00232454/00232454_00000244.xml",
        ),
        (
            "result_dir",
            None,
            "/dirac/user/u/unknown/123/123123/output.xml, /dirac/user/u/unknown/123/123123/output.txt",
        ),
        (
            "result_dir/*.xml",
            None,
            "/dirac/user/u/unknown/123/123123/output.xml",
        ),
        (
            "result_dir/*.xml",
            "/my_output_dir/00232454",
            "/dirac/user/u/unknown/my_output_dir/00232454/output.xml",
        ),
    ],
)
def test_OutputData(mocker, jobIDPath, outputData, outputPath, expectedResult):
    mocker.patch(
        "DIRAC.WorkloadManagementSystem.JobWrapper.JobWrapper.getSystemSection", side_effect=getSystemSectionMock
    )
    mocker.patch(
        "DIRAC.DataManagementSystem.Client.FailoverTransfer.FailoverTransfer.transferAndRegisterFile",
        side_effect=uploadFileMockFunc,
    )
    mocker.patch(
        "DIRAC.WorkloadManagementSystem.Client.SandboxStoreClient.SandboxStoreClient.uploadFilesAsSandboxForJob",
        side_effect=uploadSandboxMock,
    )

    jw = JobWrapper(jobIDPath)
    os.chdir(str(jw.jobID))
    jw.jobArgs = {
        "OutputData": outputData,
        "OutputPath": outputPath,
        "Owner": "duser",
        "OutputSE": "DIRAC-disk",
        "OutputSandbox": ["std.out", "std.err"],
    }

    jw.failedFlag = False
    jw.dm = dm_mock
    jw.fc = fc_mock

    result = jw.processJobOutputs()
    os.chdir(jw.root)
    assert result["OK"]
    assert jw.jobReport.jobParameters[0][1] == expectedResult


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
        "DIRAC.WorkloadManagementSystem.JobWrapper.Watchdog.getSystemSection", side_effect=getSystemSectionMock
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
    mocker.patch("DIRAC.WorkloadManagementSystem.JobWrapper.JobWrapper.ModuleFactory", side_effect=MagicMock())

    jw = JobWrapper()
    jw.jobArgs = {"Executable": "/bin/ls"}
    jw.failedFlag = failedFlag

    res = jw.finalize()

    assert res == expectedRes
    assert jw.jobReport.jobStatusInfo[0][0] == finalStates[0]
    assert jw.jobReport.jobStatusInfo[0][1] == finalStates[1]
