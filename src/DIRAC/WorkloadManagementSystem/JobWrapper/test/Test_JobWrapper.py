""" Test class for JobWrapper
"""
import os
import shutil
import tempfile
import time
from pathlib import Path
from unittest.mock import MagicMock

import pytest

import DIRAC
from DIRAC import S_OK, gLogger
from DIRAC.Core.Utilities import DErrno
from DIRAC.Core.Utilities.ReturnValues import S_ERROR
from DIRAC.DataManagementSystem.Client.test.mock_DM import dm_mock
from DIRAC.Resources.Catalog.test.mock_FC import fc_mock
from DIRAC.WorkloadManagementSystem.Client import JobMinorStatus, JobStatus
from DIRAC.WorkloadManagementSystem.JobWrapper.JobExecutionCoordinator import JobExecutionCoordinator
from DIRAC.WorkloadManagementSystem.JobWrapper.JobWrapper import JobWrapper

gLogger.setLevel("DEBUG")

# -------------------------------------------------------------------------------------------------


@pytest.fixture
def setup_job_wrapper(mocker):
    """Fixture to create a JobWrapper instance with a JobExecutionCoordinator."""

    def _setup(jobArgs=None, ceArgs=None):
        jw = JobWrapper()
        if jobArgs:
            jw.jobArgs = jobArgs
        if ceArgs:
            jw.ceArgs = ceArgs
        jw.jobExecutionCoordinator = JobExecutionCoordinator(None, None)
        return jw

    return _setup


def test_preProcess_no_arguments(setup_job_wrapper):
    """Test the pre process method of the JobWrapper class: no arguments."""
    ls = shutil.which("ls")
    jw = setup_job_wrapper({"Executable": ls})

    result = jw.preProcess()
    assert result["OK"]
    assert result["Value"]["command"] == ls
    assert result["Value"]["env"]["DIRAC_PROCESSORS"] == "1"
    assert result["Value"]["env"]["DIRAC_WHOLENODE"] == "False"


def test_preProcess_with_arguments(setup_job_wrapper):
    """Test the pre process method of the JobWrapper class: with arguments."""
    echoLocation = shutil.which("echo")
    jw = setup_job_wrapper({"Executable": echoLocation, "Arguments": "hello"})

    result = jw.preProcess()
    assert result["OK"]
    assert result["Value"]["command"] == f"{echoLocation} hello"
    assert result["Value"]["env"]["DIRAC_PROCESSORS"] == "1"
    assert result["Value"]["env"]["DIRAC_WHOLENODE"] == "False"


def test_preProcess_command_in_PATH(setup_job_wrapper):
    """Test the pre process method of the JobWrapper class: command in PATH."""
    echoLocation = shutil.which("echo")
    jw = setup_job_wrapper({"Executable": "echo", "Arguments": "hello"})

    result = jw.preProcess()
    assert result["OK"]
    assert result["Value"]["command"] == f"{echoLocation} hello"
    assert result["Value"]["env"]["DIRAC_PROCESSORS"] == "1"
    assert result["Value"]["env"]["DIRAC_WHOLENODE"] == "False"


def test_preProcess_specify_outputs(setup_job_wrapper):
    """Test the pre process method of the JobWrapper class: specify outputs."""
    echoLocation = shutil.which("echo")
    jw = setup_job_wrapper(
        {"Executable": "echo", "Arguments": "hello", "StdError": "error.log", "StdOutput": "output.log"}
    )

    result = jw.preProcess()
    assert result["OK"]
    assert result["Value"]["command"] == f"{echoLocation} hello"
    assert result["Value"]["env"]["DIRAC_PROCESSORS"] == "1"
    assert result["Value"]["env"]["DIRAC_WHOLENODE"] == "False"


def test_preProcess_specify_processors(setup_job_wrapper):
    """Test the pre process method of the JobWrapper class: specify number of processors."""
    echoLocation = shutil.which("echo")
    jw = setup_job_wrapper({"Executable": "echo", "Arguments": "hello"}, {"Processors": 2})

    result = jw.preProcess()
    assert result["OK"]
    assert result["Value"]["command"] == f"{echoLocation} hello"
    assert result["Value"]["env"]["DIRAC_PROCESSORS"] == "2"
    assert result["Value"]["env"]["DIRAC_WHOLENODE"] == "False"


def test_preProcess_with_env_variable(setup_job_wrapper):
    """Test the pre process method of the JobWrapper class: with environment variable in the executable."""
    echoLocation = shutil.which("echo")
    os.environ["CMD"] = echoLocation
    jw = setup_job_wrapper({"Executable": "${CMD}", "Arguments": "hello"})

    result = jw.preProcess()
    assert result["OK"]
    assert result["Value"]["command"] == f"{echoLocation} hello"
    assert result["Value"]["env"]["DIRAC_PROCESSORS"] == "1"
    assert result["Value"]["env"]["DIRAC_WHOLENODE"] == "False"


def test_preProcess_empty_executable(setup_job_wrapper):
    """Test the pre process method of the JobWrapper class: empty executable."""
    jw = setup_job_wrapper({})

    result = jw.preProcess()
    assert not result["OK"]
    assert result["Message"] == "Job 0 has no specified executable"


def test_preProcess_nonexistent_executable(setup_job_wrapper):
    """Test the pre process method of the JobWrapper class: nonexistent executable."""
    jw = setup_job_wrapper({"Executable": "pippo"})

    # Without a jobID
    result = jw.preProcess()
    assert not result["OK"]
    assert result["Message"] == f"Path to executable {os.getcwd()}/pippo not found"

    # With a jobID
    jw.jobIDPath = jw.jobIDPath / "123"
    result = jw.preProcess()
    assert not result["OK"]
    assert result["Message"] == f"Path to executable {os.getcwd()}/123/pippo not found"


def test_preProcess_dirac_jobexec(setup_job_wrapper):
    """Test the pre process method of the JobWrapper class: dirac-jobexec."""
    diracJobExecLocation = shutil.which("dirac-jobexec")
    jw = setup_job_wrapper({"Executable": "dirac-jobexec", "Arguments": "jobDescription.xml"})

    result = jw.preProcess()
    assert result["OK"]
    expectedOptions = [
        "-o /LocalSite/CPUNormalizationFactor=0.0",
        f"-o /LocalSite/Site={DIRAC.siteName()}",
        "-o /LocalSite/GridCE=",
        "-o /LocalSite/CEQueue=",
    ]
    assert (
        result["Value"]["command"].strip() == f"{diracJobExecLocation} jobDescription.xml {' '.join(expectedOptions)}"
    )
    assert result["Value"]["env"]["DIRAC_PROCESSORS"] == "1"
    assert result["Value"]["env"]["DIRAC_WHOLENODE"] == "False"


def test_preProcess_dirac_jobexec_with_args(setup_job_wrapper):
    """Test the pre process method of the JobWrapper class: dirac-jobexec with arguments."""
    diracJobExecLocation = shutil.which("dirac-jobexec")
    jw = setup_job_wrapper(
        {"Executable": "dirac-jobexec", "Arguments": "jobDescription.xml"},
        {"GridCE": "CE", "Queue": "Queue", "SubmissionPolicy": "Application"},
    )

    result = jw.preProcess()
    assert result["OK"]
    expectedOptions = [
        "-o /LocalSite/CPUNormalizationFactor=0.0",
        f"-o /LocalSite/Site={DIRAC.siteName()}",
        "-o /LocalSite/GridCE=CE",
        "-o /LocalSite/CEQueue=Queue",
        "-o /LocalSite/RemoteExecution=True",
    ]
    assert (
        result["Value"]["command"].strip() == f"{diracJobExecLocation} jobDescription.xml {' '.join(expectedOptions)}"
    )
    assert result["Value"]["env"]["DIRAC_PROCESSORS"] == "1"
    assert result["Value"]["env"]["DIRAC_WHOLENODE"] == "False"


# -------------------------------------------------------------------------------------------------


@pytest.mark.slow
def test_processSuccessfulCommand(mocker):
    """Test the process method of the JobWrapper class: most common scenario."""
    jw = JobWrapper()
    jw.jobArgs = {"CPUTime": 100, "Memory": 1}

    mocker.patch.object(jw, "_JobWrapper__report")
    mocker.patch.object(jw, "_JobWrapper__setJobParam")

    with tempfile.NamedTemporaryFile(delete=True) as std_out, tempfile.NamedTemporaryFile(delete=True) as std_err:
        jw.outputFile = std_out.name
        jw.errorFile = std_err.name
        result = jw.process(
            command=f"{os.path.dirname(os.path.abspath(__file__))}/script-long.sh",
            env={},
        )

    assert result["OK"]
    assert result["Value"]["payloadStatus"] == 0
    assert result["Value"]["payloadOutput"] == "Hello World"
    assert not result["Value"]["payloadExecutorError"]
    assert result["Value"]["cpuTimeConsumed"][0] > 0
    assert not result["Value"]["watchdogError"]
    assert "LastUpdateCPU(s)" in result["Value"]["watchdogStats"]
    assert "MemoryUsed(MB)" in result["Value"]["watchdogStats"]


@pytest.mark.slow
def test_processSuccessfulDiracJobExec(mocker):
    """Test the process method of the JobWrapper class: most common scenario with dirac-jobexec."""
    jw = JobWrapper()
    jw.jobArgs = {"CPUTime": 100, "Memory": 1}

    mocker.patch.object(jw, "_JobWrapper__report")
    mocker.patch.object(jw, "_JobWrapper__setJobParam")

    with tempfile.NamedTemporaryFile(delete=True) as std_out, tempfile.NamedTemporaryFile(delete=True) as std_err:
        executable = shutil.which("dirac-jobexec")
        jw.outputFile = std_out.name
        jw.errorFile = std_err.name
        result = jw.process(
            command=f"{executable} {os.path.dirname(os.path.abspath(__file__))}/jobDescription.xml",
            env={},
        )

    assert result["OK"]
    assert result["Value"]["payloadStatus"] == 0
    assert "ls successful" in result["Value"]["payloadOutput"]
    assert not result["Value"]["payloadExecutorError"]


@pytest.mark.slow
def test_processFailedCommand(mocker):
    """Test the process method of the JobWrapper class: the command fails."""
    jw = JobWrapper()
    jw.jobArgs = {"CPUTime": 100, "Memory": 1}

    mocker.patch.object(jw, "_JobWrapper__report")
    mocker.patch.object(jw, "_JobWrapper__setJobParam")

    with tempfile.NamedTemporaryFile(delete=True) as std_out, tempfile.NamedTemporaryFile(delete=True) as std_err:
        jw.outputFile = std_out.name
        jw.errorFile = std_err.name
        result = jw.process(
            command=f"{os.path.dirname(os.path.abspath(__file__))}/script-fail.sh",
            env={},
        )

    assert result["OK"]
    assert result["Value"]["payloadStatus"] == 127
    assert (
        result["Value"]["payloadOutput"]
        == f"Hello World\n{os.path.dirname(os.path.abspath(__file__))}/script-fail.sh: line 5: command_not_found: command not found"
    )
    assert not result["Value"]["payloadExecutorError"]
    assert result["Value"]["cpuTimeConsumed"][0] > 0
    assert not result["Value"]["watchdogError"]
    assert "LastUpdateCPU(s)" in result["Value"]["watchdogStats"]
    assert "MemoryUsed(MB)" in result["Value"]["watchdogStats"]


@pytest.mark.slow
def test_processFailedSubprocess(mocker):
    """Test the process method of the JobWrapper class: the subprocess fails."""
    mock_system_call = mocker.patch("DIRAC.Core.Utilities.Subprocess.Subprocess.systemCall")
    mock_system_call.return_value = S_ERROR("Any problem")
    mock_system_call = mocker.patch("DIRAC.Core.Utilities.Subprocess.Subprocess.getChildPID")
    mock_system_call.return_value = 123

    jw = JobWrapper()
    jw.jobArgs = {"CPUTime": 100, "Memory": 1}

    mocker.patch.object(jw, "_JobWrapper__report")
    mocker.patch.object(jw, "_JobWrapper__setJobParam")

    with tempfile.NamedTemporaryFile(delete=True) as std_out, tempfile.NamedTemporaryFile(delete=True) as std_err:
        jw.outputFile = std_out.name
        jw.errorFile = std_err.name
        result = jw.process("mock_command", {})

    assert result["OK"]
    assert not result["Value"]["payloadStatus"]
    assert not result["Value"]["payloadOutput"]
    assert result["Value"]["payloadExecutorError"] == "Any problem"
    assert round(result["Value"]["cpuTimeConsumed"][0], 1) == 0.0
    assert not result["Value"]["watchdogError"]
    assert not result["Value"]["watchdogStats"]


@pytest.mark.slow
def test_processKilledSubprocess(mocker):
    """Test the process method of the JobWrapper class: the job is stalled and is killed by the Watchdog."""
    jw = JobWrapper()
    jw.jobArgs = {"CPUTime": 100, "Memory": 1}

    mocker.patch.object(jw, "_JobWrapper__report")
    mocker.patch.object(jw, "_JobWrapper__setJobParam")

    mock_progress_call = mocker.patch("DIRAC.WorkloadManagementSystem.JobWrapper.Watchdog.Watchdog._checkProgress")
    mock_progress_call.return_value = S_ERROR("Job is stalled!")

    with tempfile.NamedTemporaryFile(delete=True) as std_out, tempfile.NamedTemporaryFile(delete=True) as std_err:
        jw.outputFile = std_out.name
        jw.errorFile = std_err.name
        result = jw.process("sleep 20", {})

    assert result["OK"]
    assert result["Value"]["payloadStatus"] == 15  # SIGTERM
    assert not result["Value"]["payloadOutput"]
    assert not result["Value"]["payloadExecutorError"]
    assert result["Value"]["watchdogError"] == "Job is stalled!"  # Error message from the watchdog


@pytest.mark.slow
def test_processQuickExecutionNoWatchdog(mocker):
    """Test the process method of the JobWrapper class: the payload is too fast to start the watchdog."""
    jw = JobWrapper()
    jw.jobArgs = {"CPUTime": 100, "Memory": 1}

    mocker.patch.object(jw, "_JobWrapper__report")
    mocker.patch.object(jw, "_JobWrapper__setJobParam")

    with tempfile.NamedTemporaryFile(delete=True) as std_out, tempfile.NamedTemporaryFile(delete=True) as std_err:
        jw.outputFile = std_out.name
        jw.errorFile = std_err.name
        result = jw.process(command="echo hello", env={})

    assert result["OK"]
    assert result["Value"]["payloadStatus"] == 0
    assert result["Value"]["payloadOutput"] == "hello"
    assert not result["Value"]["payloadExecutorError"]
    assert round(result["Value"]["cpuTimeConsumed"][0], 1) == 0.0
    assert not result["Value"]["watchdogError"]
    assert not result["Value"]["watchdogStats"]


@pytest.mark.slow
@pytest.mark.parametrize("expect_failure", [True, False])
def test_processSubprocessFailureNoPid(mocker, monkeypatch, expect_failure):
    """Test the process method of the JobWrapper class: the subprocess fails and no PID is returned.

    expect_failure is used to ensure that the JobWrapper is functioning correctly even with the other patching
    that is applied in the test (e.g. CHILD_PID_POLL_INTERVALS).
    """
    # Test failure in starting the payload process
    jw = JobWrapper()
    jw.jobArgs = {}

    mocker.patch.object(jw, "_JobWrapper__report")
    mocker.patch.object(jw, "_JobWrapper__setJobParam")
    monkeypatch.setattr(
        "DIRAC.WorkloadManagementSystem.JobWrapper.JobWrapper.CHILD_PID_POLL_INTERVALS", [0.1, 0.2, 0.3, 0.4, 0.5]
    )

    mock_exeThread = mocker.Mock()
    mock_exeThread.start.side_effect = lambda: time.sleep(0.1)
    if expect_failure:
        mocker.patch(
            "DIRAC.WorkloadManagementSystem.JobWrapper.JobWrapper.ExecutionThread", return_value=mock_exeThread
        )

    with tempfile.NamedTemporaryFile(delete=True) as std_out, tempfile.NamedTemporaryFile(delete=True) as std_err:
        jw.outputFile = std_out.name
        jw.errorFile = std_err.name
        result = jw.process(command="mock_command", env={})

    if expect_failure:
        assert not result["OK"]
        assert "Payload process could not start after 1.5 seconds" in result["Message"]
    else:
        assert result["OK"]


# -------------------------------------------------------------------------------------------------


@pytest.fixture
def mock_report_and_set_param(mocker):
    """Fixture to mock the __report and __setJobParam methods."""
    report_args = []
    set_param_args = []

    def report_side_effect(*args, **kwargs):
        report_args.append(kwargs)

    def set_param_side_effect(*args, **kwargs):
        set_param_args.append((args, kwargs))

    return report_args, set_param_args, report_side_effect, set_param_side_effect


def test_postProcess_payload_success(setup_job_wrapper, mocker, mock_report_and_set_param):
    """Test the postProcess method of the JobWrapper class: payload success."""
    jw = setup_job_wrapper()
    report_args, set_param_args, report_side_effect, set_param_side_effect = mock_report_and_set_param

    mocker.patch.object(jw, "_JobWrapper__report", side_effect=report_side_effect)
    mocker.patch.object(jw, "_JobWrapper__setJobParam", side_effect=set_param_side_effect)

    payloadResult = {
        "payloadStatus": 0,
        "payloadOutput": "Hello World",
        "payloadExecutorError": None,
        "cpuTimeConsumed": [100, 200, 300, 400, 500],
        "watchdogError": None,
        "watchdogStats": {"LastUpdateCPU(s)": "100", "MemoryUsed(MB)": "100"},
    }
    jw.executionResults["CPU"] = payloadResult["cpuTimeConsumed"]

    result = jw.postProcess(**payloadResult)
    assert result["OK"]
    assert report_args[-1]["status"] == JobStatus.COMPLETING
    assert report_args[-1]["minorStatus"] == JobMinorStatus.APP_SUCCESS


def test_postProcess_payload_failed(setup_job_wrapper, mocker, mock_report_and_set_param):
    """Test the postProcess method of the JobWrapper class: payload failed."""
    jw = setup_job_wrapper()
    report_args, set_param_args, report_side_effect, set_param_side_effect = mock_report_and_set_param

    mocker.patch.object(jw, "_JobWrapper__report", side_effect=report_side_effect)
    mocker.patch.object(jw, "_JobWrapper__setJobParam", side_effect=set_param_side_effect)

    payloadResult = {
        "payloadStatus": 126,
        "payloadOutput": "Hello World\nscript.sh: line 5: command_not_found: command not found",
        "payloadExecutorError": None,
        "cpuTimeConsumed": [100, 200, 300, 400, 500],
        "watchdogError": None,
        "watchdogStats": {"LastUpdateCPU(s)": "100", "MemoryUsed(MB)": "100"},
    }
    jw.executionResults["CPU"] = payloadResult["cpuTimeConsumed"]

    result = jw.postProcess(**payloadResult)
    assert result["OK"]
    assert report_args[-1]["status"] == JobStatus.COMPLETING
    assert report_args[-1]["minorStatus"] == JobMinorStatus.APP_ERRORS


def test_postProcess_payload_failed_reschedule(setup_job_wrapper, mocker, mock_report_and_set_param):
    """Test the postProcess method of the JobWrapper class: payload failed and reschedule."""
    jw = setup_job_wrapper()
    report_args, set_param_args, report_side_effect, set_param_side_effect = mock_report_and_set_param

    mocker.patch.object(jw, "_JobWrapper__report", side_effect=report_side_effect)
    mocker.patch.object(jw, "_JobWrapper__setJobParam", side_effect=set_param_side_effect)

    payloadResult = {
        "payloadStatus": DErrno.EWMSRESC,
        "payloadOutput": "Hello World\nscript.sh: line 5: command_not_found: command not found",
        "payloadExecutorError": None,
        "cpuTimeConsumed": [100, 200, 300, 400, 500],
        "watchdogError": None,
        "watchdogStats": {"LastUpdateCPU(s)": "100", "MemoryUsed(MB)": "100"},
    }
    jw.executionResults["CPU"] = payloadResult["cpuTimeConsumed"]

    result = jw.postProcess(**payloadResult)
    assert not result["OK"]
    assert result["Errno"] == DErrno.EWMSRESC
    assert report_args[-2]["status"] == JobStatus.COMPLETING
    assert report_args[-2]["minorStatus"] == JobMinorStatus.APP_ERRORS
    assert report_args[-1]["minorStatus"] == JobMinorStatus.GOING_RESCHEDULE


def test_postProcess_no_output(setup_job_wrapper, mocker, mock_report_and_set_param):
    """Test the postProcess method of the JobWrapper class: no output generated."""
    jw = setup_job_wrapper()
    report_args, set_param_args, report_side_effect, set_param_side_effect = mock_report_and_set_param

    mocker.patch.object(jw, "_JobWrapper__report", side_effect=report_side_effect)
    mocker.patch.object(jw, "_JobWrapper__setJobParam", side_effect=set_param_side_effect)

    payloadResult = {
        "payloadStatus": 0,
        "payloadOutput": "",
        "payloadExecutorError": None,
        "cpuTimeConsumed": [100, 200, 300, 400, 500],
        "watchdogError": None,
        "watchdogStats": {"LastUpdateCPU(s)": "100", "MemoryUsed(MB)": "100"},
    }
    jw.executionResults["CPU"] = payloadResult["cpuTimeConsumed"]

    result = jw.postProcess(**payloadResult)
    assert result["OK"]
    assert report_args[-1]["status"] == JobStatus.COMPLETING
    assert report_args[-1]["minorStatus"] == JobMinorStatus.APP_SUCCESS


def test_postProcess_watchdog_error(setup_job_wrapper, mocker, mock_report_and_set_param):
    """Test the postProcess method of the JobWrapper class: watchdog error."""
    jw = setup_job_wrapper()
    report_args, set_param_args, report_side_effect, set_param_side_effect = mock_report_and_set_param

    mocker.patch.object(jw, "_JobWrapper__report", side_effect=report_side_effect)
    mocker.patch.object(jw, "_JobWrapper__setJobParam", side_effect=set_param_side_effect)

    payloadResult = {
        "payloadStatus": 1,
        "payloadOutput": "Error output",
        "payloadExecutorError": None,
        "cpuTimeConsumed": [100, 200, 300, 400, 500],
        "watchdogError": "Watchdog error",
        "watchdogStats": {"LastUpdateCPU(s)": "100", "MemoryUsed(MB)": "100"},
    }
    jw.executionResults["CPU"] = payloadResult["cpuTimeConsumed"]

    result = jw.postProcess(**payloadResult)
    assert result["OK"]
    assert report_args[-1]["status"] == JobStatus.FAILED
    assert report_args[-1]["minorStatus"] == payloadResult["watchdogError"]


def test_postProcess_executor_failed_no_status(setup_job_wrapper, mocker, mock_report_and_set_param):
    """Test the postProcess method of the JobWrapper class: executor failed and no status defined."""
    jw = setup_job_wrapper()
    report_args, set_param_args, report_side_effect, set_param_side_effect = mock_report_and_set_param

    mocker.patch.object(jw, "_JobWrapper__report", side_effect=report_side_effect)
    mocker.patch.object(jw, "_JobWrapper__setJobParam", side_effect=set_param_side_effect)

    payloadResult = {
        "payloadStatus": None,
        "payloadOutput": None,
        "payloadExecutorError": "Execution failed",
        "cpuTimeConsumed": None,
        "watchdogError": None,
        "watchdogStats": None,
    }

    result = jw.postProcess(**payloadResult)
    assert not result["OK"]
    assert report_args[-1]["status"] == JobStatus.FAILED
    assert report_args[-1]["minorStatus"] == JobMinorStatus.APP_THREAD_FAILED
    assert set_param_args[-1][0][1] == "None reported"


def test_postProcess_executor_failed_status_defined(setup_job_wrapper, mocker, mock_report_and_set_param):
    """Test the postProcess method of the JobWrapper class: executor failed and status defined."""
    jw = setup_job_wrapper()
    report_args, set_param_args, report_side_effect, set_param_side_effect = mock_report_and_set_param

    mocker.patch.object(jw, "_JobWrapper__report", side_effect=report_side_effect)
    mocker.patch.object(jw, "_JobWrapper__setJobParam", side_effect=set_param_side_effect)

    payloadResult = {
        "payloadStatus": 126,
        "payloadOutput": None,
        "payloadExecutorError": "Execution failed",
        "cpuTimeConsumed": [100, 200, 300, 400, 500],
        "watchdogError": None,
        "watchdogStats": None,
    }
    jw.executionResults["CPU"] = payloadResult["cpuTimeConsumed"]

    result = jw.postProcess(**payloadResult)
    assert result["OK"]
    assert report_args[-1]["status"] == JobStatus.COMPLETING
    assert report_args[-1]["minorStatus"] == JobMinorStatus.APP_ERRORS
    assert set_param_args[-3][0][1] == 126


def test_postProcess_subprocess_not_complete(setup_job_wrapper, mocker, mock_report_and_set_param):
    """Test the postProcess method of the JobWrapper class: subprocess not complete."""
    jw = setup_job_wrapper()
    report_args, set_param_args, report_side_effect, set_param_side_effect = mock_report_and_set_param

    mocker.patch.object(jw, "_JobWrapper__report", side_effect=report_side_effect)
    mocker.patch.object(jw, "_JobWrapper__setJobParam", side_effect=set_param_side_effect)

    payloadResult = {
        "payloadStatus": None,
        "payloadOutput": None,
        "payloadExecutorError": None,
        "cpuTimeConsumed": None,
        "watchdogError": None,
        "watchdogStats": None,
    }

    result = jw.postProcess(**payloadResult)
    assert not result["OK"]
    assert "No outputs generated from job execution" in result["Message"]
    assert report_args[-1]["status"] == JobStatus.FAILED
    assert report_args[-1]["minorStatus"] == JobMinorStatus.APP_THREAD_NOT_COMPLETE


# -------------------------------------------------------------------------------------------------


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
            "src/DIRAC/WorkloadManagementSystem/JobWrapper/test/jobDescription.xml",
            None,
            "Application Finished Successfully",
        ),
    ],
)
def test_execute(mocker, executable, args, src, expectedResult):
    """Test the status of the job after JobWrapper.execute().
    The returned value of JobWrapper.execute() is not checked as it can apparently be wrong depending on the shell used.
    """

    if src:
        shutil.copy(os.path.join(src, executable), executable)

    jw = JobWrapper()
    jw.jobArgs = {"Executable": executable}
    jw.jobExecutionCoordinator = JobExecutionCoordinator(None, None)

    if args:
        jw.jobArgs["Arguments"] = args
    jw.execute()
    assert expectedResult in jw.jobReport.jobStatusInfo[-1]

    if src:
        os.remove(executable)

    if os.path.exists("std.out"):
        os.remove("std.out")


# -------------------------------------------------------------------------------------------------


@pytest.fixture
def jobIDPath():
    """Return the path to the job ID file."""
    # Create a temporary directory named ./123/
    jobid = "123"
    p = Path(jobid)
    if p.exists():
        shutil.rmtree(jobid)
    p.mkdir()

    # Output sandbox files
    (p / "std.out").touch()
    (p / "std.err").touch()
    (p / "summary_123.xml").touch()
    (p / "result_dir").mkdir()
    (p / "result_dir" / "file1").touch()
    # Output data files
    (p / "00232454_00000244_1.sim").touch()
    (p / "1720442808testFileUpload.txt").touch()
    (p / "testFileUploadFullLFN.txt").touch()

    with open(p / "pool_xml_catalog.xml", "w") as f:
        f.write(
            """<?xml version="1.0" encoding="UTF-8" standalone="no" ?>
<!-- Edited By POOL -->
<!DOCTYPE POOLFILECATALOG SYSTEM "InMemory">
<POOLFILECATALOG>
  <File ID="47D01192-4405-11EF-BB92-E8808801FBBD">
    <physical>
      <pfn filetype="ROOT" name="00232454_00000244_1.sim"/>
    </physical>
    <logical/>
  </File>

</POOLFILECATALOG>"""
        )

    yield int(jobid)

    # Remove the temporary directory
    shutil.rmtree(jobid)


@pytest.fixture
def setup_another_job_wrapper(mocker, jobIDPath):
    """Fixture to create a JobWrapper instance with the jobIDPath."""
    jw = JobWrapper(jobIDPath)
    jw.jobIDPath = Path(str(jobIDPath))
    jw.failedFlag = False
    return jw


def test_processJobOutputs_no_output(setup_another_job_wrapper):
    """Test the processJobOutputs method of the JobWrapper class: no output files."""
    jw = setup_another_job_wrapper
    jw.jobArgs = {
        "OutputSandbox": [],
        "OutputData": [],
    }

    result = jw.processJobOutputs()
    assert result["OK"]
    assert jw.jobReport.jobStatusInfo == []
    assert jw.jobReport.jobParameters == []
    assert result["Value"] == "Job has no owner specified"


def test_processJobOutputs_no_output_with_owner(setup_another_job_wrapper):
    """Test the processJobOutputs method of the JobWrapper class: no output files with owner."""
    jw = setup_another_job_wrapper
    jw.jobArgs = {
        "OutputSandbox": [],
        "OutputData": [],
        "Owner": "Jane Doe",
    }

    result = jw.processJobOutputs()
    assert result["OK"]
    assert result["Value"] == "Job outputs processed"
    assert len(jw.jobReport.jobStatusInfo) == 1
    assert jw.jobReport.jobStatusInfo[0][0] == JobStatus.COMPLETING
    assert jw.jobReport.jobParameters == []


def test_processJobOutputs_no_output_with_failure(setup_another_job_wrapper):
    """Test the processJobOutputs method of the JobWrapper class: no output files with payload failure."""
    jw = setup_another_job_wrapper
    # Set the failed flag to True
    jw.failedFlag = True

    jw.jobArgs = {
        "OutputSandbox": [],
        "OutputData": [],
        "Owner": "Jane Doe",
    }

    result = jw.processJobOutputs()
    assert result["OK"]
    assert result["Value"] == "Job outputs processed"
    assert jw.jobReport.jobStatusInfo == []
    assert jw.jobReport.jobParameters == []


def test_processJobOutputs_output_sandbox(mocker, setup_another_job_wrapper):
    """Test the processJobOutputs method of the JobWrapper class: output sandbox.

    The following files are expected to be uploaded:
    - std.out
    - std.err
    - summary_123.xml
    - result_dir.tar

    The following files are expected to be missing:
    - test.log
    - test_dir
    """
    jw = setup_another_job_wrapper
    # Mock the uploadFilesAsSandbox method
    uploadFiles = mocker.patch(
        "DIRAC.WorkloadManagementSystem.Client.SandboxStoreClient.SandboxStoreClient.uploadFilesAsSandbox",
        return_value=S_OK(),
    )

    jw.jobArgs = {
        "OutputSandbox": ["std.out", "std.err", "summary*.xml", "result_dir", "test.log", "test*.sim", "test_dir"],
        "OutputData": [],
        "Owner": "Jane Doe",
    }

    result = jw.processJobOutputs()
    assert result["OK"]
    assert result["Value"] == "Job outputs processed"

    uploadFiles.assert_called_once()
    args, _ = uploadFiles.call_args
    upload_args = args[0]
    assert sorted(["123/std.out", "123/std.err", "123/summary_123.xml", "123/result_dir.tar"]) == sorted(upload_args)

    assert len(jw.jobReport.jobStatusInfo) == 2
    assert jw.jobReport.jobStatusInfo[0][:-1] == (JobStatus.COMPLETING, JobMinorStatus.UPLOADING_OUTPUT_SANDBOX)
    assert jw.jobReport.jobStatusInfo[1][:-1] == (JobStatus.COMPLETING, JobMinorStatus.OUTPUT_SANDBOX_UPLOADED)
    assert len(jw.jobReport.jobParameters) == 1
    assert jw.jobReport.jobParameters[0] == ("OutputSandboxMissingFiles", "test.log, test_dir")


def test_processJobOutputs_output_sandbox_upload_fails_no_sandbox_name(mocker, setup_another_job_wrapper):
    """Test the processJobOutputs method of the JobWrapper class: output sandbox upload fails with no sandbox name."""
    jw = setup_another_job_wrapper
    # Mock the uploadFilesAsSandbox method: upload failed
    uploadFiles = mocker.patch(
        "DIRAC.WorkloadManagementSystem.Client.SandboxStoreClient.SandboxStoreClient.uploadFilesAsSandbox",
        return_value=S_ERROR("Upload failed"),
    )

    jw.jobArgs = {
        "OutputSandbox": ["std.out", "std.err", "summary*.xml", "result_dir", "test.log", "test*.sim", "test_dir"],
        "OutputData": [],
        "Owner": "Jane Doe",
    }

    result = jw.processJobOutputs()
    assert not result["OK"]
    assert "no file name supplied for failover to Grid storage" in result["Message"]

    uploadFiles.assert_called_once()
    assert len(jw.jobReport.jobStatusInfo) == 1
    assert jw.jobReport.jobStatusInfo[0][:-1] == (JobStatus.COMPLETING, JobMinorStatus.UPLOADING_OUTPUT_SANDBOX)
    assert len(jw.jobReport.jobParameters) == 1
    assert jw.jobReport.jobParameters[0] == ("OutputSandboxMissingFiles", "test.log, test_dir")


def test_processJobOutputs_output_sandbox_upload_fails_with_sandbox_name_no_outputSE(mocker, setup_another_job_wrapper):
    """Test the processJobOutputs method of the JobWrapper class: output sandbox upload fails with sandbox name
    but there is no output SE defined.
    """
    jw = setup_another_job_wrapper
    # Mock the uploadFilesAsSandbox method: upload failed but with a sandbox name
    uploadFiles = mocker.patch(
        "DIRAC.WorkloadManagementSystem.Client.SandboxStoreClient.SandboxStoreClient.uploadFilesAsSandbox",
        return_value={"OK": False, "Message": "Upload failed", "SandboxFileName": "/tmp/Sandbox1"},
    )

    jw.jobArgs = {
        "OutputSandbox": ["std.out", "std.err", "summary*.xml", "result_dir", "test.log", "test*.sim", "test_dir"],
        "OutputData": [],
        "Owner": "Jane Doe",
    }

    result = jw.processJobOutputs()
    assert not result["OK"]
    assert "No output SEs defined" in result["Message"]

    uploadFiles.assert_called_once()
    assert len(jw.jobReport.jobStatusInfo) == 1
    assert jw.jobReport.jobStatusInfo[0][:-1] == (JobStatus.COMPLETING, JobMinorStatus.UPLOADING_OUTPUT_SANDBOX)
    assert len(jw.jobReport.jobParameters) == 3
    assert jw.jobReport.jobParameters[0] == ("OutputSandboxMissingFiles", "test.log, test_dir")
    assert jw.jobReport.jobParameters[1] == ("OutputSandbox", "Sandbox uploaded to grid storage")
    assert jw.jobReport.jobParameters[2] == ("OutputSandboxLFN", "/dirac/user/u/unknown/0/123/Sandbox1")


def test_processJobOutputs_output_data_upload(mocker, setup_another_job_wrapper):
    """Test the processJobOutputs method of the JobWrapper class: output sandbox upload fails with sandbox name
    but there is no output SE defined.
    """
    jw = setup_another_job_wrapper
    jw.defaultFailoverSE = "TestFailoverSE"

    # Mock the transferAndRegisterFile method: transfer does not fail
    transferFiles = mocker.patch.object(
        jw.failoverTransfer, "transferAndRegisterFile", return_value=S_OK({"uploadedSE": jw.defaultFailoverSE})
    )

    # TODO: LFNs does not seem to be well supported, they would not be extracted properly from the pool_xml_catalog.xml
    # In getGuidByPfn(), pfn (which still contains LFN: at this moment) is compared to the value in pool_xml_catalog.xml
    # (which does not contains LFN:)
    # BTW, isn't the concept of pool_xml_catalog.xml from lhcbdirac?
    jw.jobArgs = {
        "OutputSandbox": [],
        "OutputData": [
            "1720442808testFileUpload.txt",
            "LFN:00232454_00000244_1.sim",
            "LFN:/dirac/user/u/unknown/testFileUploadFullLFN.txt",
        ],
        "Owner": "Jane Doe",
    }

    result = jw.processJobOutputs()
    assert result["OK"]
    assert "Job outputs processed" in result["Value"]

    # how many times transferAndRegisterFile was called: 2 times
    transferFiles.assert_called()
    assert len(jw.jobReport.jobStatusInfo) == 3
    # TODO: Uploading output sandbox is reported whereas there was no output sandbox
    assert jw.jobReport.jobStatusInfo[0][:-1] == (JobStatus.COMPLETING, JobMinorStatus.UPLOADING_OUTPUT_SANDBOX)
    assert jw.jobReport.jobStatusInfo[1][:-1] == ("", JobMinorStatus.UPLOADING_OUTPUT_DATA)
    assert jw.jobReport.jobStatusInfo[2][:-1] == (JobStatus.COMPLETING, JobMinorStatus.OUTPUT_DATA_UPLOADED)
    assert len(jw.jobReport.jobParameters) == 1

    expected_files = {
        "00232454_00000244_1.sim",
        "/dirac/user/u/unknown/0/123/1720442808testFileUpload.txt",
        "/dirac/user/u/unknown/testFileUploadFullLFN.txt",
    }
    assert jw.jobReport.jobParameters[0][0] == "UploadedOutputData"
    uploaded_files = set(jw.jobReport.jobParameters[0][1].split(", "))
    assert uploaded_files == expected_files


# -------------------------------------------------------------------------------------------------


def test_resolveInputData(mocker):
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


# -------------------------------------------------------------------------------------------------


def test_transferInputSandbox_no_sandbox(setup_another_job_wrapper):
    """Test the transferInputSandbox method of the JobWrapper class: no sandbox to transfer."""
    jw = setup_another_job_wrapper

    res = jw.transferInputSandbox([""])
    assert res["OK"]
    assert res["Value"] == "InputSandbox downloaded"

    assert len(jw.jobReport.jobStatusInfo) == 1
    assert jw.jobReport.jobStatusInfo[0][:-1] == ("", JobMinorStatus.DOWNLOADING_INPUT_SANDBOX)


def test_transferInputSandbox_invalid_sb_url(setup_another_job_wrapper):
    """Test the transferInputSandbox method of the JobWrapper class: invalid sandbox URL."""
    jw = setup_another_job_wrapper

    # SB:anotherfile.txt is not formatted correctly: should be SB:<se name>|<path>
    res = jw.transferInputSandbox(["SB:anotherfile.txt"])
    assert not res["OK"]
    assert "Invalid sandbox" in res["Message"]

    assert len(jw.jobReport.jobStatusInfo) == 2
    assert jw.jobReport.jobStatusInfo[0][:-1] == ("", JobMinorStatus.DOWNLOADING_INPUT_SANDBOX)
    assert jw.jobReport.jobStatusInfo[1][:-1] == ("", JobMinorStatus.FAILED_DOWNLOADING_INPUT_SANDBOX)


def test_transferInputSandbox(mocker, setup_another_job_wrapper):
    """Test the transferInputSandbox method of the JobWrapper class."""
    jw = setup_another_job_wrapper

    mocker.patch(
        "DIRAC.WorkloadManagementSystem.Client.SandboxStoreClient.SandboxStoreClient.downloadSandbox",
        return_values=S_OK(100),
    )
    mocker.patch.object(
        jw.dm, "getFile", return_value=S_OK({"Failed": [], "Successful": {"file1.txt": "/path/to/file1.txt"}})
    )

    res = jw.transferInputSandbox(["jobDescription.xml", "script.sh", "LFN:file1.txt", "SB:se|anotherfile.txt"])
    assert res["OK"]
    assert res["Value"] == "InputSandbox downloaded"

    assert len(jw.jobReport.jobStatusInfo) == 2
    assert jw.jobReport.jobStatusInfo[0][:-1] == ("", JobMinorStatus.DOWNLOADING_INPUT_SANDBOX)
    assert jw.jobReport.jobStatusInfo[1][:-1] == ("", JobMinorStatus.DOWNLOADING_INPUT_SANDBOX_LFN)


# -------------------------------------------------------------------------------------------------


@pytest.mark.parametrize(
    "failedFlag, expectedRes, finalStates",
    [
        (True, 1, [JobStatus.FAILED, ""]),
        (False, 0, [JobStatus.DONE, JobMinorStatus.EXEC_COMPLETE]),
    ],
)
def test_finalize(mocker, failedFlag, expectedRes, finalStates):
    mocker.patch("DIRAC.WorkloadManagementSystem.JobWrapper.JobWrapper.ObjectLoader", side_effect=MagicMock())

    jw = JobWrapper()
    jw.jobArgs = {"Executable": "/bin/ls"}
    jw.failedFlag = failedFlag

    res = jw.finalize()

    assert res == expectedRes
    assert jw.jobReport.jobStatusInfo[0][0] == finalStates[0]
    assert jw.jobReport.jobStatusInfo[0][1] == finalStates[1]
