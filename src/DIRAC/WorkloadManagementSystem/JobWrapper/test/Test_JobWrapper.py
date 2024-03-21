""" Test class for JobWrapper
"""
import os
import shutil
import tempfile
import time
from unittest.mock import MagicMock

import pytest

import DIRAC
from DIRAC import gLogger
from DIRAC.Core.Utilities import DErrno
from DIRAC.Core.Utilities.ReturnValues import S_ERROR
from DIRAC.DataManagementSystem.Client.test.mock_DM import dm_mock
from DIRAC.Resources.Catalog.test.mock_FC import fc_mock
from DIRAC.WorkloadManagementSystem.Client import JobMinorStatus, JobStatus
from DIRAC.WorkloadManagementSystem.JobWrapper.JobWrapper import JobWrapper

getSystemSectionMock = MagicMock()
getSystemSectionMock.return_value = "aValue"

gLogger.setLevel("DEBUG")

# PreProcess method


def test_preProcess(mocker):
    """Test the pre process method of the JobWrapper class."""
    mocker.patch("DIRAC.WorkloadManagementSystem.JobWrapper.JobWrapper.getSystemSection", return_value="Value")

    echoLocation = shutil.which("echo")
    diracJobExecLocation = shutil.which("dirac-jobexec")

    # Test a simple command without argument
    jw = JobWrapper()
    jw.jobArgs = {"Executable": echoLocation}

    result = jw.preProcess()
    assert result["OK"]
    assert result["Value"]["command"] == echoLocation
    assert result["Value"]["error"] == "std.err"
    assert result["Value"]["output"] == "std.out"
    assert result["Value"]["env"]["DIRAC_PROCESSORS"] == "1"
    assert result["Value"]["env"]["DIRAC_WHOLENODE"] == "False"

    # Test a command with arguments
    jw = JobWrapper()
    jw.jobArgs = {"Executable": echoLocation, "Arguments": "hello"}

    result = jw.preProcess()
    assert result["OK"]
    assert result["Value"]["command"] == f"{echoLocation} hello"
    assert result["Value"]["error"] == "std.err"
    assert result["Value"]["output"] == "std.out"
    assert result["Value"]["env"]["DIRAC_PROCESSORS"] == "1"
    assert result["Value"]["env"]["DIRAC_WHOLENODE"] == "False"

    # Test a command that is included in the PATH
    jw = JobWrapper()
    jw.jobArgs = {"Executable": "echo", "Arguments": "hello"}

    result = jw.preProcess()
    assert result["OK"]
    assert result["Value"]["command"] == f"{echoLocation} hello"
    assert result["Value"]["error"] == "std.err"
    assert result["Value"]["output"] == "std.out"
    assert result["Value"]["env"]["DIRAC_PROCESSORS"] == "1"
    assert result["Value"]["env"]["DIRAC_WHOLENODE"] == "False"

    # Test a command and specify outputs
    jw = JobWrapper()
    jw.jobArgs = {"Executable": "echo", "Arguments": "hello", "StdError": "error.log", "StdOutput": "output.log"}

    result = jw.preProcess()
    assert result["OK"]
    assert result["Value"]["command"] == f"{echoLocation} hello"
    assert result["Value"]["error"] == "error.log"
    assert result["Value"]["output"] == "output.log"
    assert result["Value"]["env"]["DIRAC_PROCESSORS"] == "1"
    assert result["Value"]["env"]["DIRAC_WHOLENODE"] == "False"

    # Test a command and specify number of processors
    jw = JobWrapper()
    jw.jobArgs = {"Executable": "echo", "Arguments": "hello"}
    jw.ceArgs = {"Processors": 2}

    result = jw.preProcess()
    assert result["OK"]
    assert result["Value"]["command"] == f"{echoLocation} hello"
    assert result["Value"]["error"] == "std.err"
    assert result["Value"]["output"] == "std.out"
    assert result["Value"]["env"]["DIRAC_PROCESSORS"] == "2"
    assert result["Value"]["env"]["DIRAC_WHOLENODE"] == "False"

    # Test a command with environment variable in the executable
    jw = JobWrapper()
    jw.jobArgs = {"Executable": "${CMD}", "Arguments": "hello"}

    os.environ["CMD"] = echoLocation

    result = jw.preProcess()
    assert result["OK"]
    assert result["Value"]["command"] == f"{echoLocation} hello"
    assert result["Value"]["error"] == "std.err"
    assert result["Value"]["output"] == "std.out"
    assert result["Value"]["env"]["DIRAC_PROCESSORS"] == "1"
    assert result["Value"]["env"]["DIRAC_WHOLENODE"] == "False"

    # Test a command with an empty executable
    jw = JobWrapper()
    jw.jobArgs = {}
    result = jw.preProcess()
    assert not result["OK"]
    assert result["Message"] == "Job 0 has no specified executable"

    # Test a command with an executable that does not exist
    jw = JobWrapper()
    jw.jobArgs = {"Executable": "pippo"}
    result = jw.preProcess()
    assert not result["OK"]
    assert result["Message"] == f"Path to executable {os.getcwd()}/pippo not found"

    # Test dirac-jobexec
    jw = JobWrapper()
    jw.jobArgs = {"Executable": "dirac-jobexec", "Arguments": "jobDescription.xml"}

    result = jw.preProcess()
    assert result["OK"]
    expectedOptions = [
        "-o /LocalSite/CPUNormalizationFactor=0.0",
        f"-o /LocalSite/Site={DIRAC.siteName()}",
        "-o /LocalSite/GridCE=",
        "-o /LocalSite/CEQueue=",
        "-o /LocalSite/RemoteExecution=False",
    ]
    assert (
        result["Value"]["command"].strip() == f"{diracJobExecLocation} jobDescription.xml {' '.join(expectedOptions)}"
    )
    assert result["Value"]["error"] == "std.err"
    assert result["Value"]["output"] == "std.out"
    assert result["Value"]["env"]["DIRAC_PROCESSORS"] == "1"
    assert result["Value"]["env"]["DIRAC_WHOLENODE"] == "False"

    # Test dirac-jobexec with arguments
    jw = JobWrapper()
    jw.jobArgs = {"Executable": "dirac-jobexec", "Arguments": "jobDescription.xml"}
    jw.ceArgs = {"GridCE": "CE", "Queue": "Queue", "RemoteExecution": True}
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
    assert result["Value"]["error"] == "std.err"
    assert result["Value"]["output"] == "std.out"
    assert result["Value"]["env"]["DIRAC_PROCESSORS"] == "1"
    assert result["Value"]["env"]["DIRAC_WHOLENODE"] == "False"


# Process method


@pytest.mark.slow
def test_processSuccessfulCommand(mocker):
    """Test the process method of the JobWrapper class: most common scenario."""
    mocker.patch("DIRAC.WorkloadManagementSystem.JobWrapper.JobWrapper.getSystemSection", return_value="Value")
    mocker.patch("DIRAC.WorkloadManagementSystem.JobWrapper.Watchdog.getSystemInstance", return_value="Value")
    jw = JobWrapper()
    jw.jobArgs = {"CPUTime": 100, "Memory": 1}

    mocker.patch.object(jw, "_JobWrapper__report")
    mocker.patch.object(jw, "_JobWrapper__setJobParam")

    with tempfile.NamedTemporaryFile(delete=True) as std_out, tempfile.NamedTemporaryFile(delete=True) as std_err:
        result = jw.process(
            command=f"{os.path.dirname(os.path.abspath(__file__))}/script-long.sh",
            output=std_out.name,
            error=std_err.name,
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
    mocker.patch("DIRAC.WorkloadManagementSystem.JobWrapper.JobWrapper.getSystemSection", return_value="Value")
    mocker.patch("DIRAC.WorkloadManagementSystem.JobWrapper.Watchdog.getSystemInstance", return_value="Value")
    jw = JobWrapper()
    jw.jobArgs = {"CPUTime": 100, "Memory": 1}

    mocker.patch.object(jw, "_JobWrapper__report")
    mocker.patch.object(jw, "_JobWrapper__setJobParam")

    with tempfile.NamedTemporaryFile(delete=True) as std_out, tempfile.NamedTemporaryFile(delete=True) as std_err:
        executable = shutil.which("dirac-jobexec")
        result = jw.process(
            command=f"{executable} {os.path.dirname(os.path.abspath(__file__))}/jobDescription.xml --o /DIRAC/Setup=Test",
            output=std_out.name,
            error=std_err.name,
            env={},
        )

    assert result["OK"]
    assert result["Value"]["payloadStatus"] == 0
    assert "ls successful" in result["Value"]["payloadOutput"]
    assert not result["Value"]["payloadExecutorError"]


@pytest.mark.slow
def test_processFailedCommand(mocker):
    """Test the process method of the JobWrapper class: the command fails."""
    mocker.patch("DIRAC.WorkloadManagementSystem.JobWrapper.JobWrapper.getSystemSection", return_value="Value")
    mocker.patch("DIRAC.WorkloadManagementSystem.JobWrapper.Watchdog.getSystemInstance", return_value="Value")
    jw = JobWrapper()
    jw.jobArgs = {"CPUTime": 100, "Memory": 1}

    mocker.patch.object(jw, "_JobWrapper__report")
    mocker.patch.object(jw, "_JobWrapper__setJobParam")

    with tempfile.NamedTemporaryFile(delete=True) as std_out, tempfile.NamedTemporaryFile(delete=True) as std_err:
        result = jw.process(
            command=f"{os.path.dirname(os.path.abspath(__file__))}/script-fail.sh",
            output=std_out.name,
            error=std_err.name,
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
    mocker.patch("DIRAC.WorkloadManagementSystem.JobWrapper.JobWrapper.getSystemSection", return_value="Value")
    mocker.patch("DIRAC.WorkloadManagementSystem.JobWrapper.Watchdog.getSystemInstance", return_value="Value")
    mock_system_call = mocker.patch("DIRAC.Core.Utilities.Subprocess.Subprocess.systemCall")
    mock_system_call.return_value = S_ERROR("Any problem")
    mock_system_call = mocker.patch("DIRAC.Core.Utilities.Subprocess.Subprocess.getChildPID")
    mock_system_call.return_value = 123

    jw = JobWrapper()
    jw.jobArgs = {"CPUTime": 100, "Memory": 1}

    mocker.patch.object(jw, "_JobWrapper__report")
    mocker.patch.object(jw, "_JobWrapper__setJobParam")

    with tempfile.NamedTemporaryFile(delete=True) as std_out, tempfile.NamedTemporaryFile(delete=True) as std_err:
        result = jw.process("mock_command", std_out.name, std_err.name, {})

    assert result["OK"]
    assert not result["Value"]["payloadStatus"]
    assert not result["Value"]["payloadOutput"]
    assert result["Value"]["payloadExecutorError"] == "Any problem"
    assert result["Value"]["cpuTimeConsumed"][0] == 0.0
    assert not result["Value"]["watchdogError"]
    assert not result["Value"]["watchdogStats"]


@pytest.mark.slow
def test_processQuickExecutionNoWatchdog(mocker):
    """Test the process method of the JobWrapper class: the payload is too fast to start the watchdog."""
    mocker.patch("DIRAC.WorkloadManagementSystem.JobWrapper.JobWrapper.getSystemSection", return_value="Value")
    mocker.patch("DIRAC.WorkloadManagementSystem.JobWrapper.Watchdog.getSystemInstance", return_value="Value")
    jw = JobWrapper()
    jw.jobArgs = {"CPUTime": 100, "Memory": 1}

    mocker.patch.object(jw, "_JobWrapper__report")
    mocker.patch.object(jw, "_JobWrapper__setJobParam")

    with tempfile.NamedTemporaryFile(delete=True) as std_out, tempfile.NamedTemporaryFile(delete=True) as std_err:
        result = jw.process(command=f"echo hello", output=std_out.name, error=std_err.name, env={})

    assert result["OK"]
    assert result["Value"]["payloadStatus"] == 0
    assert result["Value"]["payloadOutput"] == "hello"
    assert not result["Value"]["payloadExecutorError"]
    assert result["Value"]["cpuTimeConsumed"][0] == 0.0
    assert not result["Value"]["watchdogError"]
    assert not result["Value"]["watchdogStats"]


@pytest.mark.slow
def test_processSubprocessFailureNoPid(mocker):
    """Test the process method of the JobWrapper class: the subprocess fails and no PID is returned."""
    mocker.patch("DIRAC.WorkloadManagementSystem.JobWrapper.JobWrapper.getSystemSection", return_value="Value")
    mocker.patch("DIRAC.WorkloadManagementSystem.JobWrapper.Watchdog.getSystemInstance", return_value="Value")
    # Test failure in starting the payload process
    jw = JobWrapper()
    jw.jobArgs = {}

    mocker.patch.object(jw, "_JobWrapper__report")
    mocker.patch.object(jw, "_JobWrapper__setJobParam")
    mock_exeThread = mocker.Mock()
    mock_exeThread.start.side_effect = lambda: time.sleep(0.1)
    mocker.patch("DIRAC.WorkloadManagementSystem.JobWrapper.JobWrapper.ExecutionThread", return_value=mock_exeThread)

    with tempfile.NamedTemporaryFile(delete=True) as std_out, tempfile.NamedTemporaryFile(delete=True) as std_err:
        result = jw.process(command=f"mock_command", output=std_out.name, error=std_err.name, env={})
    assert not result["OK"]
    assert "Payload process could not start after 140 seconds" in result["Message"]


# PostProcess method


def test_postProcess(mocker):
    """Test the post process method of the JobWrapper class."""
    mocker.patch("DIRAC.WorkloadManagementSystem.JobWrapper.JobWrapper.getSystemSection", return_value="Value")
    # Mimic the behaviour of __report and __setJobParam to get the arguments passed to them
    report_args = []
    set_param_args = []

    def report_side_effect(*args, **kwargs):
        report_args.append(kwargs)

    def set_param_side_effect(*args, **kwargs):
        set_param_args.append((args, kwargs))

    # Test when the payload finished successfully
    jw = JobWrapper()

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

    # Test when the payload failed
    jw = JobWrapper()

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

    # Test when the payload failed: should be rescheduled
    jw = JobWrapper()

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

    # Test when there is no output
    jw = JobWrapper()

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

    # Test when there is a watchdog error
    jw = JobWrapper()

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

    # Test when the executor failed: no status defined
    jw = JobWrapper()

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

    # Test when the executor failed: status defined
    jw = JobWrapper()

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

    # Test when the subprocess did not complete
    jw = JobWrapper()

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


# Execute method


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

    mocker.patch("DIRAC.WorkloadManagementSystem.JobWrapper.JobWrapper.getSystemSection", return_value="Value")
    mocker.patch("DIRAC.WorkloadManagementSystem.JobWrapper.Watchdog.getSystemInstance", return_value="Value")

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
