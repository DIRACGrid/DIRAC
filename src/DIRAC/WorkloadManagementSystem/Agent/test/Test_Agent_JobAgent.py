""" Test class for Job Agent
"""
import multiprocessing
import os
from pathlib import Path
import pytest
import time
from concurrent.futures import ProcessPoolExecutor
from functools import partial

from DIRAC import gLogger, S_OK, S_ERROR
from DIRAC.Core.Security.X509Chain import X509Chain  # pylint: disable=import-error
from DIRAC.Resources.Computing.BatchSystems.TimeLeft.TimeLeft import TimeLeft
from DIRAC.Resources.Computing.ComputingElementFactory import ComputingElementFactory
from DIRAC.Resources.Computing.test.Test_PoolComputingElement import badJobScript, jobScript
from DIRAC.WorkloadManagementSystem.Agent.JobAgent import JobAgent
from DIRAC.WorkloadManagementSystem.Client import JobStatus
from DIRAC.WorkloadManagementSystem.Client.JobReport import JobReport

gLogger.setLevel("DEBUG")


@pytest.mark.parametrize(
    "ceType, expectedType, expectedNumberElement",
    [
        ("InProcess", list, 1),
        ("Pool", list, 2),
    ],
)
def test__getCEDict(mocker, ceType, expectedType, expectedNumberElement):
    """Test JobAgent()._getCEDict()"""
    mocker.patch("DIRAC.WorkloadManagementSystem.Agent.JobAgent.AgentModule.__init__")

    jobAgent = JobAgent("Test", "Test1")
    jobAgent.log = gLogger
    jobAgent.log.setLevel("DEBUG")

    result = ComputingElementFactory().getCE(ceType)
    assert result["OK"]

    ce = result["Value"]
    ce.ceParameters["MultiProcessorStrategy"] = True
    ce.ceParameters["NumberOfProcessors"] = 4
    result = jobAgent._getCEDict(ce)
    assert result["OK"]
    ceDict = result["Value"]
    assert isinstance(ceDict, expectedType)
    assert len(ceDict) == expectedNumberElement


@pytest.mark.parametrize(
    "ceDict, mockGCReply, mockGCReply2, expected",
    [
        (
            {},
            "Test",
            {"OK": False},
            {
                "GridCE": "Test",
                "PilotBenchmark": 0.0,
                "PilotInfoReportedFlag": False,
                "PilotReference": "Unknown",
            },
        ),
        (
            {},
            None,
            {"OK": False},
            {
                "PilotBenchmark": 0.0,
                "PilotInfoReportedFlag": False,
                "PilotReference": "Unknown",
            },
        ),
        (
            {"PilotReference": "ref"},
            "Test",
            {"OK": False},
            {
                "GridCE": "Test",
                "PilotBenchmark": 0.0,
                "PilotInfoReportedFlag": False,
                "PilotReference": "ref",
            },
        ),
        (
            {"PilotReference": "ref"},
            None,
            {"OK": True, "Value": {"JobReq": "test"}},
            {
                "PilotBenchmark": 0.0,
                "PilotInfoReportedFlag": False,
                "PilotReference": "ref",
                "JobReq": "test",
            },
        ),
    ],
)
def test__setCEDict(mocker, ceDict, mockGCReply, mockGCReply2, expected):
    """Test JobAgent()._setCEDict()"""
    if mockGCReply:
        mocker.patch("DIRAC.WorkloadManagementSystem.Agent.JobAgent.gConfig.getValue", return_value=mockGCReply)
    mocker.patch("DIRAC.WorkloadManagementSystem.Agent.JobAgent.gConfig.getOptionsDict", return_value=mockGCReply2)
    mocker.patch("DIRAC.WorkloadManagementSystem.Agent.JobAgent.AgentModule.__init__")

    jobAgent = JobAgent("Test", "Test1")
    jobAgent.log = gLogger
    jobAgent.log.setLevel("DEBUG")

    jobAgent._setCEDict(ceDict)
    assert ceDict == expected


@pytest.mark.parametrize(
    "ceType, mockCEReply, expectedResult",
    [
        ("InProcess", {"OK": False, "Message": "CE Not Available"}, {"OK": False, "Message": "CE Not Available"}),
        ("InProcess", {"OK": True, "Value": None, "CEInfoDict": {}}, {"OK": False, "Message": "CE Not Available"}),
        (
            "InProcess",
            {"OK": True, "Value": None, "CEInfoDict": {"RunningJobs": 1}},
            {"OK": True, "Value": "Job Agent cycle complete with 1 running jobs"},
        ),
        ("InProcess", {"OK": True, "Value": 1, "CEInfoDict": {}}, {"OK": True}),
    ],
)
def test__checkCEAvailability(mocker, ceType, mockCEReply, expectedResult):
    """Test JobAgent()._checkAvailability()"""
    mocker.patch("DIRAC.WorkloadManagementSystem.Agent.JobAgent.AgentModule.__init__")
    mocker.patch("DIRAC.Resources.Computing.ComputingElement.ComputingElement.available", return_value=mockCEReply)

    jobAgent = JobAgent("Test", "Test1")
    jobAgent.log = gLogger
    jobAgent.log.setLevel("DEBUG")

    result = ComputingElementFactory().getCE(ceType)
    assert result["OK"]

    ce = result["Value"]
    result = jobAgent._checkCEAvailability(ce)
    assert result["OK"] == expectedResult["OK"]
    if "Value" in expectedResult:
        assert result["Value"] == expectedResult["Value"]
    if "Message" in expectedResult:
        assert result["Message"] == expectedResult["Message"]


#############################################################################


@pytest.mark.parametrize(
    "initTimeLeft, timeLeft, cpuFactor, mockTimeLeftReply, expectedTimeLeft",
    [
        (100000, 75000, None, {"OK": False, "Message": "Error"}, 75000),
        (100000, 75000, 10, {"OK": False, "Message": "Error"}, 100000),
        (100000, 75000, 10, {"OK": True, "Value": 25000}, 25000),
    ],
)
def test__computeCPUWorkLeft(mocker, initTimeLeft, timeLeft, cpuFactor, mockTimeLeftReply, expectedTimeLeft):
    """Test JobAgent()._computeCPUWorkLeft()"""
    mocker.patch("DIRAC.WorkloadManagementSystem.Agent.JobAgent.AgentModule.__init__")
    mocker.patch(
        "DIRAC.Resources.Computing.BatchSystems.TimeLeft.TimeLeft.TimeLeft.getTimeLeft", return_value=mockTimeLeftReply
    )

    jobAgent = JobAgent("Test", "Test1")
    jobAgent.log = gLogger
    jobAgent.log.setLevel("DEBUG")
    jobAgent.timeLeftUtil = TimeLeft()

    jobAgent.initTimeLeft = initTimeLeft
    jobAgent.timeLeft = timeLeft
    jobAgent.cpuFactor = cpuFactor
    result = jobAgent._computeCPUWorkLeft()

    assert abs(result - expectedTimeLeft) < 10


@pytest.mark.parametrize(
    "cpuWorkLeft, fillingMode, expectedResult",
    [
        (10000, False, {"OK": False, "Message": "Filling Mode is Disabled"}),
        (10000, True, {"OK": True, "Value": None}),
        (1000, False, {"OK": False, "Message": "Filling Mode is Disabled"}),
        (1000, True, {"OK": False, "Message": "No more time left"}),
    ],
)
def test__checkCPUWorkLeft(mocker, cpuWorkLeft, fillingMode, expectedResult):
    """Test JobAgent()._checkCPUWorkLeft()"""
    mocker.patch("DIRAC.WorkloadManagementSystem.Agent.JobAgent.AgentModule.__init__")
    mocker.patch("DIRAC.WorkloadManagementSystem.Agent.JobAgent.AgentModule.am_stopExecution")

    jobAgent = JobAgent("Test", "Test1")
    jobAgent.log = gLogger
    jobAgent.log.setLevel("DEBUG")

    jobAgent.fillingMode = fillingMode

    result = jobAgent._checkCPUWorkLeft(cpuWorkLeft)
    assert result["OK"] == expectedResult["OK"]
    if "Value" in result:
        assert result["Value"] == expectedResult["Value"]
    if "Message" in result:
        assert result["Message"] == expectedResult["Message"]


#############################################################################


@pytest.mark.parametrize(
    "issueMessage, stopAfterFailedMatches, matchFailedCount, expectedResult",
    [("Pilot version does not match", 5, 0, False), ("No match found", 5, 0, True), ("No match found", 5, 5, False)],
)
def test__checkMatchingIssues(mocker, issueMessage, stopAfterFailedMatches, matchFailedCount, expectedResult):
    """Test JobAgent()._checkMatchingIssues()"""
    mocker.patch("DIRAC.WorkloadManagementSystem.Agent.JobAgent.AgentModule.__init__")
    mocker.patch("DIRAC.WorkloadManagementSystem.Agent.JobAgent.AgentModule.am_stopExecution")

    jobAgent = JobAgent("Test", "Test1")
    jobAgent.log = gLogger
    jobAgent.log.setLevel("DEBUG")

    jobAgent.stopAfterFailedMatches = stopAfterFailedMatches
    jobAgent.matchFailedCount = matchFailedCount

    result = jobAgent._checkMatchingIssues(S_ERROR(issueMessage))
    assert result["OK"] == expectedResult


@pytest.mark.parametrize(
    "matcherInfo, matcherParams, expectedResult",
    [
        ({}, [], {"OK": True, "Value": None}),
        ({}, ["Param1"], {"OK": False, "Message": "Matcher Failed"}),
        ({"Param1": None}, ["Param1"], {"OK": False, "Message": "Matcher Failed"}),
        ({"Param1": "Value1"}, ["Param1"], {"OK": True, "Value": None}),
    ],
)
def test__checkMatcherInfo(mocker, matcherInfo, matcherParams, expectedResult):
    """Test JobAgent()._checkMatcherInfo()"""
    mocker.patch("DIRAC.WorkloadManagementSystem.Agent.JobAgent.AgentModule.__init__")
    mocker.patch("DIRAC.WorkloadManagementSystem.Client.JobReport.JobReport.setJobStatus")

    jobID = 123
    jobAgent = JobAgent("Test", "Test1")
    jobAgent.log = gLogger
    jobAgent.log.setLevel("DEBUG")

    jobAgent.jobs[jobID] = {}
    jobAgent.jobs[jobID]["JobReport"] = JobReport(jobID)

    result = jobAgent._checkMatcherInfo(jobID, matcherInfo, matcherParams)
    assert result["OK"] == expectedResult["OK"]
    if "Value" in result:
        assert result["Value"] == expectedResult["Value"]
    if "Message" in result:
        assert result["Message"] == expectedResult["Message"]


#############################################################################


@pytest.mark.slow
@pytest.mark.parametrize(
    "mockGCReply, mockPMReply, expected",
    [
        (True, {"OK": True, "Value": "Test"}, {"OK": True, "Value": "Test"}),
        (
            True,
            {"OK": False, "Message": "Test"},
            {"OK": False, "Message": "Failed to setup proxy: Error retrieving proxy"},
        ),
        (
            False,
            {"OK": True, "Value": "Test"},
            {"OK": False, "Message": "Invalid Proxy"},
        ),
        (
            False,
            {"OK": False, "Message": "Test"},
            {"OK": False, "Message": "Invalid Proxy"},
        ),
    ],
)
def test__setupProxy(mocker, mockGCReply, mockPMReply, expected):
    """Testing JobAgent()._setupProxy()"""
    mocker.patch("DIRAC.WorkloadManagementSystem.Agent.JobAgent.AgentModule.__init__")
    mocker.patch("DIRAC.WorkloadManagementSystem.Agent.JobAgent.AgentModule")
    mocker.patch("DIRAC.WorkloadManagementSystem.Agent.JobAgent.getDNForUsername", return_value=S_OK(["mockedDN"]))
    mocker.patch("DIRAC.WorkloadManagementSystem.Agent.JobAgent.gConfig.getValue", return_value=mockGCReply)
    module_str = "DIRAC.WorkloadManagementSystem.Agent.JobAgent.gProxyManager.getPayloadProxyFromDIRACGroup"
    mocker.patch(module_str, return_value=mockPMReply)

    jobAgent = JobAgent("Test", "Test1")

    ownerDN = "DIRAC"
    ownerGroup = "DIRAC"

    jobAgent.log = gLogger
    jobAgent.log.setLevel("DEBUG")

    result = jobAgent._setupProxy(ownerDN, ownerGroup)

    assert result["OK"] == expected["OK"]

    if result["OK"]:
        assert result["Value"] == expected["Value"]
    else:
        assert result["Message"] == expected["Message"]


@pytest.mark.slow
@pytest.mark.parametrize(
    "mockGCReply, mockPMReply, expected",
    [
        (True, {"OK": True, "Value": "Test"}, {"OK": True, "Value": "Test"}),
        (
            True,
            {"OK": False, "Message": "Test"},
            {"OK": False, "Message": "Error retrieving proxy"},
        ),
        (False, {"OK": True, "Value": "Test"}, {"OK": True, "Value": "Test"}),
        (
            False,
            {"OK": False, "Message": "Test"},
            {"OK": False, "Message": "Error retrieving proxy"},
        ),
    ],
)
def test__requestProxyFromProxyManager(mocker, mockGCReply, mockPMReply, expected):
    """Testing JobAgent()._requestProxyFromProxyManager()"""
    mocker.patch("DIRAC.WorkloadManagementSystem.Agent.JobAgent.AgentModule.__init__")
    mocker.patch("DIRAC.WorkloadManagementSystem.Agent.JobAgent.AgentModule")
    mocker.patch("DIRAC.WorkloadManagementSystem.Agent.JobAgent.gConfig.getValue", return_value=mockGCReply)
    module_str = "DIRAC.WorkloadManagementSystem.Agent.JobAgent.gProxyManager.getPayloadProxyFromDIRACGroup"
    mocker.patch(module_str, return_value=mockPMReply)

    jobAgent = JobAgent("Test", "Test1")

    ownerDN = "DIRAC"
    ownerGroup = "DIRAC"

    jobAgent.log = gLogger
    jobAgent.log.setLevel("DEBUG")

    result = jobAgent._requestProxyFromProxyManager(ownerDN, ownerGroup)

    assert result["OK"] == expected["OK"]

    if result["OK"]:
        assert result["Value"] == expected["Value"]

    else:
        assert result["Message"] == expected["Message"]


#############################################################################


def test__checkInstallSoftware(mocker):
    """Testing JobAgent()._checkInstallSoftware()"""

    mocker.patch("DIRAC.WorkloadManagementSystem.Agent.JobAgent.AgentModule.__init__")

    jobID = 123

    jobAgent = JobAgent("Test", "Test1")
    jobAgent.log = gLogger
    jobAgent.log.setLevel("DEBUG")

    jobAgent.jobs[jobID] = {}
    jobAgent.jobs[jobID]["JobReport"] = JobReport(jobID)

    result = jobAgent._checkInstallSoftware({}, {})

    assert result["OK"], result["Message"]
    assert result["Value"] == "Job has no software installation requirement"


#############################################################################


def test__getJDLParameters(mocker):
    """Testing JobAgent()._getJDLParameters()"""

    mocker.patch("DIRAC.WorkloadManagementSystem.Agent.JobAgent.AgentModule.__init__")

    jobAgent = JobAgent("Test", "Test1")
    jobAgent.log = gLogger
    jobAgent.log.setLevel("DEBUG")

    jdl = """
        [
            Executable = "dirac-jobexec";
            StdError = "std.err";
            LogLevel = "info";
            JobName = "helloWorld";
            Priority = "1";
            InputSandbox =
                {
                    "../../Integration/WorkloadManagementSystem/exe-script.py",
                    "exe-script.py",
                    "/tmp/tmpMQEink/jobDescription.xml",
                    "SB:FedericoSandboxSE|/SandBox/f/fstagni.lhcb_user/0c2/9f5/0c29f53a47d051742346b744c793d4d0.tar.bz2"
                };
            Arguments = "jobDescription.xml -o LogLevel=info";
            JobGroup = "lhcb";
            OutputSandbox =
                {
                    "helloWorld.log",
                    "std.err",
                    "std.out"
                };
            StdOutput = "std.out";
            InputData = "";
            JobType = "User";
            NumberOfProcessors = 16;
            Tags =
                {
                    "16Processors",
                    "MultiProcessor"
                };
        ]
        """

    result = jobAgent._getJDLParameters(jdl)

    assert result["OK"], result["Message"]
    assert result["Value"]["NumberOfProcessors"] == "16"
    assert result["Value"]["Tags"] == ["16Processors", "MultiProcessor"]


#############################################################################


def test__rescheduleFailedJob_success(mocker):
    """Testing rescheduleFailedJob success"""
    mocker.patch("DIRAC.WorkloadManagementSystem.Agent.JobAgent.AgentModule.__init__")
    mocker.patch(
        "DIRAC.WorkloadManagementSystem.Client.JobManagerClient.JobManagerClient.rescheduleJob", return_value=S_OK()
    )

    jobAgent = JobAgent("Test", "Test1")

    jobID = 101
    message = "An error occurred"

    jobAgent.log = gLogger
    jobAgent.log.setLevel("DEBUG")

    jobAgent.jobs[jobID] = {}
    jobAgent.jobs[jobID]["JobReport"] = JobReport(jobID)

    result = jobAgent._rescheduleFailedJob(jobID, message)

    assert not result["OK"], result
    assert result["Message"] == "Job Rescheduled", result

    # Because the jobReport cannot communicate with the JobManager, we are supposed to have the report info here
    jobReport = jobAgent.jobs[jobID]["JobReport"]
    assert len(jobReport.jobStatusInfo) == 1, jobReport.jobStatusInfo
    assert jobReport.jobStatusInfo[0][0] == "Rescheduled", jobReport.jobStatusInfo[0][0]
    assert jobReport.jobStatusInfo[0][1] == "", jobReport.jobStatusInfo[0][1]

    assert len(jobReport.appStatusInfo) == 1, jobReport.appStatusInfo
    assert jobReport.appStatusInfo[0][0] == message, jobReport.appStatusInfo[0][0]


def test__rescheduleFailedJob_fail(mocker):
    """Testing rescheduleFailedJob when the job fails to be rescheduled."""
    mocker.patch("DIRAC.WorkloadManagementSystem.Agent.JobAgent.AgentModule.__init__")
    mocker.patch(
        "DIRAC.WorkloadManagementSystem.Client.JobManagerClient.JobManagerClient.rescheduleJob",
        return_value=S_ERROR("Cannot contact JobManager"),
    )

    jobAgent = JobAgent("Test", "Test1")

    jobID = 101
    message = "An error occurred"

    jobAgent.log = gLogger
    jobAgent.log.setLevel("DEBUG")

    jobAgent.jobs[jobID] = {}
    jobAgent.jobs[jobID]["JobReport"] = JobReport(jobID)

    result = jobAgent._rescheduleFailedJob(jobID, message)

    assert not result["OK"], result
    assert result["Message"] == "Problem Rescheduling Job"

    # The JobManager could not be contacted to reschedule the jobs
    # In such a case, we do not expect any status in the jobReport job/appStatusInfo
    # TODO: rescheduling is currently performed in 2 operations: setJobStatus and rescheduleJob
    #       This should be changed to a single operation in the future, then we can adjust this test
    # jobReport = jobAgent.jobs[jobID]["JobReport"]
    # assert len(jobReport.jobStatusInfo) == 0
    # assert len(jobReport.appStatusInfo) == 0


def test__rescheduleFailedJob_multipleJobIDs(mocker):
    """Testing rescheduleFailedJob() with multiple jobIDs"""
    mocker.patch("DIRAC.WorkloadManagementSystem.Agent.JobAgent.AgentModule.__init__")
    mocker.patch(
        "DIRAC.WorkloadManagementSystem.Client.JobManagerClient.JobManagerClient.rescheduleJob",
        return_value=S_ERROR("Cannot contact JobManager"),
    )

    jobAgent = JobAgent("Test", "Test1")

    jobID1 = 101
    jobID2 = 102
    message = "An error occurred"

    jobAgent.log = gLogger
    jobAgent.log.setLevel("DEBUG")

    jobAgent.jobs[jobID1] = {}
    jobAgent.jobs[jobID1]["JobReport"] = JobReport(jobID1)

    jobAgent.jobs[jobID2] = {}
    jobAgent.jobs[jobID2]["JobReport"] = JobReport(jobID2)

    # First rescheduled job
    result = jobAgent._rescheduleFailedJob(jobID1, message)

    assert not result["OK"], result
    assert result["Message"] == "Problem Rescheduling Job"

    # Second rescheduled job
    result = jobAgent._rescheduleFailedJob(jobID2, message)

    assert not result["OK"], result
    assert result["Message"] == "Problem Rescheduling Job"

    # Here we want to make sure that rescheduleFailedJob created 2 distinct reports
    jobReport1 = jobAgent.jobs[jobID1]["JobReport"]
    assert len(jobReport1.jobStatusInfo) == 1
    assert len(jobReport1.appStatusInfo) == 1

    jobReport2 = jobAgent.jobs[jobID2]["JobReport"]
    assert len(jobReport2.jobStatusInfo) == 1
    assert len(jobReport2.appStatusInfo) == 1


#############################################################################
@pytest.fixture
def manageJobFiles():
    """Create fake job files and yield their paths."""
    jobExecutablePath = "testJob.py"
    with open(jobExecutablePath, "w") as execFile:
        pass
    os.chmod(jobExecutablePath, 0o755)

    # Generate fake jobWrapperPath and jobWrapperConfigPath
    jobWrapperPath = "Wrapper_123"
    with open(jobWrapperPath, "w") as temp:
        temp.write("test")
    jobWrapperConfigPath = "Wrapper_123.json"
    with open(jobWrapperConfigPath, "w") as temp:
        temp.write("test")

    yield (jobExecutablePath, jobWrapperPath, jobWrapperConfigPath)

    Path(jobExecutablePath).unlink(missing_ok=True)
    Path(jobWrapperPath).unlink(missing_ok=True)
    Path(jobWrapperConfigPath).unlink(missing_ok=True)


@pytest.mark.parametrize(
    "mockJWInput, expected", [({"OK": False, "Message": "Test"}, {"OK": False, "Message": "Test"})]
)
def test_submitJob(mocker, mockJWInput, expected):
    """Testing JobAgent()._submitJob()"""
    mocker.patch("DIRAC.WorkloadManagementSystem.Agent.JobAgent.AgentModule.__init__")
    mocker.patch(
        "DIRAC.WorkloadManagementSystem.Agent.JobAgent.AgentModule._AgentModule__moduleProperties",
        side_effect=lambda x, y=None: y,
        create=True,
    )
    mocker.patch("DIRAC.WorkloadManagementSystem.Agent.JobAgent.createJobWrapper", return_value=mockJWInput)

    jobAgent = JobAgent("Test", "Test1")
    jobAgent.log = gLogger
    jobAgent.log.setLevel("DEBUG")
    jobAgent.ceName = "Test"

    result = jobAgent._submitJob(101, {}, {}, {}, "", 1)

    assert result["OK"] == expected["OK"]

    if not result["OK"]:
        assert result["Message"] == expected["Message"]


@pytest.mark.slow
@pytest.mark.parametrize(
    "localCE, job, expectedResult1, expectedResult2",
    [
        # Sync submission, should not encounter any issue
        ("InProcess", jobScript % "1", ([], []), ([], [])),
        # Async submission, should not encounter any issue
        ("Pool/InProcess", jobScript % "1", ([], []), ([], [])),
        # Sync submission of a failed job, first time the job is failed, second time is ok since the job
        # as already been processed
        ("InProcess", badJobScript, ([], ["Payload execution failed with error code 5"]), ([], [])),
        # Async submission of a failed job, first time the job has not failed yet, second time it is failed
        ("Pool/InProcess", badJobScript, ([], []), ([], ["Payload execution failed with error code 5"])),
        # Sync submission, should fail because of a problem in the Singularity CE
        ("Singularity", jobScript % "1", (["Failed to find singularity"], []), ([], [])),
        # Async submission, should fail because of a problem in the Singularity CE
        ("Pool/Singularity", jobScript % "1", (["Failed to find singularity"], []), ([], [])),
    ],
)
def test_submitAndCheckJob(mocker, manageJobFiles, localCE, job, expectedResult1, expectedResult2, tmp_path):
    """Test the submission and the management of the job status."""
    jobID = "123"
    jobExecutablePath, jobWrapperPath, jobWrapperConfigPath = manageJobFiles
    with open(jobExecutablePath, "w") as execFile:
        execFile.write(job)

    mocker.patch("DIRAC.WorkloadManagementSystem.Agent.JobAgent.AgentModule.__init__")
    mocker.patch("DIRAC.WorkloadManagementSystem.Agent.JobAgent.JobAgent.am_stopExecution")
    mocker.patch(
        "DIRAC.WorkloadManagementSystem.Agent.JobAgent.JobMonitoringClient.getJobsStatus",
        return_value=S_OK({int(jobID): {"Status": JobStatus.RUNNING}}),
    )
    mocker.patch(
        "DIRAC.WorkloadManagementSystem.Agent.JobAgent.createJobWrapper",
        return_value=S_OK(
            {
                "JobExecutablePath": jobExecutablePath,
                "JobWrapperPath": jobWrapperPath,
                "JobWrapperConfigPath": jobWrapperConfigPath,
            }
        ),
    )
    mocker.patch("DIRAC.WorkloadManagementSystem.Agent.JobAgent.JobAgent._sendFailoverRequest", return_value=S_OK())
    empty_file_path = tmp_path / "empty_file"
    empty_file_path.touch()
    mocker.patch(
        "DIRAC.WorkloadManagementSystem.Agent.JobAgent.writeChainToTemporaryFile",
        return_value=S_OK(str(empty_file_path)),
    )
    mocker.patch(
        "DIRAC.Resources.Computing.SingularityComputingElement.SingularityComputingElement.submitJob",
        return_value=S_ERROR("Failed to find singularity"),
    )
    # We need to force ProcessPoolExecutor to use the fork context to enable the
    # mocks to propagate to the subprocesses used by PoolComputingElement
    mocker.patch(
        "concurrent.futures.ProcessPoolExecutor",
        side_effect=partial(ProcessPoolExecutor, mp_context=multiprocessing.get_context("fork")),
    )

    jobAgent = JobAgent("JobAgent", "Test")
    jobAgent.log = gLogger.getSubLogger("JobAgent")
    jobAgent._initializeComputingElement(localCE)
    jobAgent.jobSubmissionDelay = 3

    jobAgent.jobs[jobID] = {}
    jobAgent.jobs[jobID]["JobReport"] = JobReport(jobID)

    # Submit a job
    result = jobAgent._submitJob(
        jobID=jobID, jobParams={}, resourceParams={}, optimizerParams={}, proxyChain=X509Chain()
    )
    # Check that no error occurred during the submission process
    # at the level of the JobAgent
    assert result["OK"]

    # Check that the job was added to jobAgent.jobs
    assert len(jobAgent.jobs) == 1
    assert jobID in jobAgent.jobs

    # If the submission is synchronous jobAgent.computingElement.taskResults
    # should already contain the result
    if not jobAgent.computingElement.ceParameters.get("AsyncSubmission", False):
        assert len(jobAgent.computingElement.taskResults) == 1
    # Else, the job is still running, the result should not already be present
    # Unless, an error occurred during the submission
    else:
        if expectedResult1[0]:
            assert len(jobAgent.computingElement.taskResults) == 1
        else:
            assert len(jobAgent.computingElement.taskResults) == 0

    # Check errors that could have occurred in the innerCE
    result = jobAgent._checkSubmittedJobs()
    assert result["OK"], result
    assert result["Value"] == expectedResult1, result

    # If the submission is synchronous jobAgent.computingElement.taskResults
    # should not contain the result anymore: already processed by checkSubmittedJobs
    if not jobAgent.computingElement.ceParameters.get("AsyncSubmission", False):
        assert len(jobAgent.computingElement.taskResults) == 0
    # Else, the job is still running, the result should not already be present
    # Unless, an error occurred during the submission
    else:
        if expectedResult1[0]:
            assert len(jobAgent.computingElement.taskResults) == 0
        else:
            # Wait for the end of the job
            attempts = 0
            while len(jobAgent.computingElement.taskResults) < 1:
                time.sleep(0.1)
                attempts += 1
                if attempts == 1200:
                    break
            assert len(jobAgent.computingElement.taskResults) == 1

    # Check errors that could have occurred in the innerCE
    result = jobAgent._checkSubmittedJobs()
    assert result["OK"]
    assert result["Value"] == expectedResult2

    # From here, taskResults should be empty
    assert len(jobAgent.computingElement.taskResults) == 0


def test_submitAndCheck2Jobs(mocker, tmp_path):
    """Test the submission and the management of the job status.

    This time, a first job is successfully submitted, but the second submission fails.
    We want to make sure that both jobs are correctly managed.
    """
    # Mock the JobAgent
    mocker.patch("DIRAC.WorkloadManagementSystem.Agent.JobAgent.AgentModule.__init__")
    mocker.patch("DIRAC.WorkloadManagementSystem.Agent.JobAgent.JobAgent.am_stopExecution")
    mocker.patch(
        "DIRAC.WorkloadManagementSystem.Agent.JobAgent.createJobWrapper",
        return_value=S_OK(
            {"JobExecutablePath": "jobName", "JobWrapperPath": "jobName", "JobWrapperConfigPath": "jobName"}
        ),
    )
    mocker.patch("DIRAC.WorkloadManagementSystem.Agent.JobAgent.JobAgent._sendFailoverRequest", return_value=S_OK())

    def make_empty_file(*args, **kwargs):
        """Create an empty file and return its path."""
        empty_file_path = tmp_path / "empty_file"
        empty_file_path.touch()
        return S_OK(str(empty_file_path))

    mocker.patch(
        "DIRAC.WorkloadManagementSystem.Agent.JobAgent.writeChainToTemporaryFile",
        make_empty_file,
    )
    mocker.patch(
        "DIRAC.Resources.Computing.InProcessComputingElement.InProcessComputingElement.submitJob",
        side_effect=[S_OK(), S_ERROR("ComputingElement error")],
    )

    jobAgent = JobAgent("JobAgent", "Test")
    jobAgent.log = gLogger.getSubLogger("JobAgent")
    jobAgent._initializeComputingElement("InProcess")
    jobAgent.ceName = "InProcess"
    jobAgent.jobSubmissionDelay = 0

    mock_rescheduleFailedJob = mocker.patch.object(jobAgent, "_rescheduleFailedJob")

    # Submit a first job: should be successful
    jobID = "123"
    jobAgent.jobs[jobID] = {}
    jobAgent.jobs[jobID]["JobReport"] = JobReport(jobID)
    result = jobAgent._submitJob(
        jobID=jobID, jobParams={}, resourceParams={}, optimizerParams={}, proxyChain=X509Chain()
    )
    # Check that no error occurred during the submission process
    # at the level of the JobAgent
    assert result["OK"]

    # Check that the job was added to jobAgent.jobs
    assert len(jobAgent.jobs) == 1
    assert jobID in jobAgent.jobs

    # The submission is synchronous taskResults should already contain the result
    assert len(jobAgent.computingElement.taskResults) == 1

    # Check errors that could have occurred in the innerCE
    result = jobAgent._checkSubmittedJobs()
    assert result["OK"]
    assert result["Value"] == ([], [])

    mock_rescheduleFailedJob.assert_not_called()

    # Submit a second job: should fail
    jobID = "456"
    jobAgent.jobs[jobID] = {}
    jobAgent.jobs[jobID]["JobReport"] = JobReport(jobID)
    result = jobAgent._submitJob(
        jobID=jobID, jobParams={}, resourceParams={}, optimizerParams={}, proxyChain=X509Chain()
    )
    # Check that no error occurred during the submission process
    # at the level of the JobAgent
    assert result["OK"]

    # Check errors that could have occurred in the innerCE
    result = jobAgent._checkSubmittedJobs()
    assert result["OK"]
    assert result["Value"] == (["ComputingElement error"], [])

    # Make sure that the correct job is rescheduled
    mock_rescheduleFailedJob.assert_called_with(jobID, "ComputingElement error")

    # From here, taskResults should be empty
    assert len(jobAgent.computingElement.taskResults) == 0


def test_failureBeforeSubmission(mocker):
    """Test that a failure before job submission is handled correctly.

    We want to make sure that there is no endless loop in the finalize method.
    """
    # Mock the JobAgent
    mocker.patch("DIRAC.WorkloadManagementSystem.Agent.JobAgent.AgentModule.__init__")

    jobAgent = JobAgent("JobAgent", "Test")
    jobAgent.log = gLogger.getSubLogger("JobAgent")

    # Here we simulate a failure before the job submission: no TaskID
    jobID = "123"
    jobAgent.jobs[jobID] = {}
    jobAgent.jobs[jobID]["JobReport"] = JobReport(jobID)

    # Make sure that the job is removed from jobAgent.jobs
    result = jobAgent._checkSubmittedJobs()
    assert result["OK"]
    assert result["Value"] == ([], [])

    assert jobAgent.jobs == {}
