""" Test class for Job Agent
"""

# imports
import os
from pathlib import Path
import shutil
from unittest.mock import Mock
import pytest
from collections import defaultdict

# DIRAC Components
from DIRAC.ConfigurationSystem.Client.Helpers.Operations import Operations
from DIRAC.WorkloadManagementSystem.Agent.PushJobAgent import PushJobAgent
from DIRAC.WorkloadManagementSystem.Agent.test.Test_Agent_SiteDirector import config

from DIRAC import gLogger, S_OK, S_ERROR
from DIRAC.WorkloadManagementSystem.Client import JobMinorStatus
from DIRAC.WorkloadManagementSystem.Client.JobReport import JobReport

gLogger.setLevel("DEBUG")


@pytest.mark.parametrize(
    "queue, failedQueues, failedQueueCycleFactor, expectedResult",
    [
        ("queue1", {"queue1": 0}, 3, True),
        ("queue1", {"queue1": 1}, 3, False),
        ("queue1", {"queue1": 2}, 3, False),
        ("queue1", {"queue1": 3}, 3, True),
        ("queue1", {"queue1": 4}, 3, False),
        ("queue2", {"queue1": 4}, 3, True),
    ],
)
def test__allowedToSubmit(mocker, queue, failedQueues, failedQueueCycleFactor, expectedResult):
    """Test JobAgent()._checkAvailability()"""
    mocker.patch("DIRAC.WorkloadManagementSystem.Agent.JobAgent.AgentModule.__init__")
    mocker.patch(
        "DIRAC.WorkloadManagementSystem.Agent.JobAgent.AgentModule._AgentModule__moduleProperties",
        side_effect=lambda x, y=None: y,
        create=True,
    )

    jobAgent = PushJobAgent("Test", "Test1")
    jobAgent.log = gLogger
    jobAgent.log.setLevel("DEBUG")

    jobAgent.failedQueues = defaultdict(int)
    jobAgent.failedQueues.update(failedQueues)
    jobAgent.failedQueueCycleFactor = failedQueueCycleFactor

    result = jobAgent._allowedToSubmit(queue)
    assert result == expectedResult


@pytest.mark.parametrize(
    "ceDict, pilotVersion, pilotProject, expected",
    [
        ({}, None, None, {"SubmissionPolicy": "JobWrapper"}),
        ({}, "8.0.0", None, {"DIRACVersion": "8.0.0", "ReleaseVersion": "8.0.0", "SubmissionPolicy": "JobWrapper"}),
        (
            {},
            ["8.0.0", "7.3.7"],
            None,
            {"DIRACVersion": "8.0.0", "ReleaseVersion": "8.0.0", "SubmissionPolicy": "JobWrapper"},
        ),
        ({}, None, "Project", {"ReleaseProject": "Project", "SubmissionPolicy": "JobWrapper"}),
        (
            {},
            "8.0.0",
            "Project",
            {
                "DIRACVersion": "8.0.0",
                "ReleaseVersion": "8.0.0",
                "ReleaseProject": "Project",
                "SubmissionPolicy": "JobWrapper",
            },
        ),
    ],
)
def test__setCEDict(mocker, ceDict, pilotVersion, pilotProject, expected):
    """Test JobAgent()._setCEDict()"""
    opsSideEffect = [pilotVersion, pilotProject]
    mocker.patch("DIRAC.ConfigurationSystem.Client.Helpers.Operations.Operations.getValue", side_effect=opsSideEffect)
    mocker.patch("DIRAC.WorkloadManagementSystem.Agent.JobAgent.AgentModule.__init__")
    mocker.patch(
        "DIRAC.WorkloadManagementSystem.Agent.JobAgent.AgentModule._AgentModule__moduleProperties",
        side_effect=lambda x, y=None: y,
        create=True,
    )

    jobAgent = PushJobAgent("Test", "Test1")
    jobAgent.log = gLogger
    jobAgent.log.setLevel("DEBUG")
    jobAgent.opsHelper = Operations()

    jobAgent._setCEDict(ceDict)
    assert ceDict == expected


@pytest.mark.parametrize(
    "issueMessage, expectedResult",
    [
        ("No match found", True),
        ("seconds timeout", True),
        ("another error", True),
    ],
)
def test__checkMatchingIssues(mocker, issueMessage, expectedResult):
    """Test JobAgent()._checkMatchingIssues()"""
    mocker.patch("DIRAC.WorkloadManagementSystem.Agent.JobAgent.AgentModule.__init__")
    mocker.patch(
        "DIRAC.WorkloadManagementSystem.Agent.JobAgent.AgentModule._AgentModule__moduleProperties",
        side_effect=lambda x, y=None: y,
        create=True,
    )

    jobAgent = PushJobAgent("Test", "Test1")
    jobAgent.log = gLogger
    jobAgent.log.setLevel("DEBUG")

    result = jobAgent._checkMatchingIssues(S_ERROR(issueMessage))
    assert result["OK"] == expectedResult


def test_execute_application_localCENotAvailable(config, mocker):
    """Test when local CE is not available: it should not check submitted jobs and return an error message"""

    mocker.patch("DIRAC.WorkloadManagementSystem.Agent.JobAgent.AgentModule.__init__")
    mocker.patch(
        "DIRAC.WorkloadManagementSystem.Agent.JobAgent.AgentModule._AgentModule__moduleProperties",
        side_effect=lambda x, y=None: y,
        create=True,
    )

    jobAgent = PushJobAgent("Test", "Test1")
    jobAgent.submissionPolicy = "Application"
    jobAgent.queueDict = jobAgent._buildQueueDict(
        siteNames=["LCG.Site1.com", "LCG.Site2.site2"], ces=None, ceTypes=None
    )["Value"]

    # Mock the CE availability
    errorMessage = "CE Not Available"
    checkCEAvailability = mocker.patch.object(jobAgent, "_checkCEAvailability", return_value=S_ERROR(errorMessage))

    checkSubmittedJobs = mocker.patch.object(jobAgent, "_checkSubmittedJobs")
    checkSubmittedJobWrappers = mocker.patch.object(jobAgent, "_checkSubmittedJobWrappers")
    allowedToSubmit = mocker.patch.object(jobAgent, "_allowedToSubmit")
    matchAJob = mocker.patch.object(jobAgent, "_matchAJob")

    # Initialize logger
    jobAgent.log = gLogger
    jobAgent.log.setLevel("DEBUG")
    # Initialize inner CE
    jobAgent._initializeComputingElement("Pool")

    result = jobAgent.execute()
    # Check the CE availability and submitted jobs
    checkCEAvailability.assert_called()
    checkSubmittedJobs.assert_not_called()
    # Does not check if allowed to submit and does not match a job
    allowedToSubmit.assert_not_called()
    matchAJob.assert_not_called()
    # This is not called because submission policy is Application
    checkSubmittedJobWrappers.assert_not_called()
    # Result should not be OK
    assert not result["OK"], result
    assert result["Message"] == errorMessage


@pytest.fixture
def jobID():
    jobID = "123"
    Path(jobID).mkdir(parents=True, exist_ok=True)

    yield jobID

    shutil.rmtree(jobID)


@pytest.mark.slow
def test_submitJobWrapper(mocker, jobID):
    """Test JobAgent._submitJobWrapper()"""
    mocker.patch("DIRAC.WorkloadManagementSystem.Agent.JobAgent.AgentModule.__init__")
    mocker.patch(
        "DIRAC.WorkloadManagementSystem.Agent.JobAgent.AgentModule._AgentModule__moduleProperties",
        side_effect=lambda x, y=None: y,
        create=True,
    )

    # Initialize PJA
    jobAgent = PushJobAgent("Test", "Test1")
    jobAgent.submissionPolicy = "JobWrapper"
    jobAgent.queueDict = jobAgent._buildQueueDict(
        siteNames=["LCG.Site1.com", "LCG.Site2.site2"], ces=None, ceTypes=None
    )["Value"]
    jobAgent.log = gLogger
    jobAgent.log.setLevel("DEBUG")

    jobAgent.jobs[jobID] = {"JobReport": JobReport(jobID)}
    jobParams = {}

    # Current working directory: it should not change
    cwd = os.getcwd()

    # Mock the JobWrapper
    # Create a mock JobWrapper instance
    job = Mock()
    job.sendJobAccounting = Mock()

    mocker.patch("DIRAC.WorkloadManagementSystem.JobWrapper.JobWrapperUtilities.JobWrapper", return_value=job)

    rescheduleValue = "valueProvingRescheduling"
    mocker.patch(
        "DIRAC.WorkloadManagementSystem.JobWrapper.JobWrapperUtilities.rescheduleFailedJob",
        return_value=rescheduleValue,
    )
    mocker.patch("DIRAC.WorkloadManagementSystem.Agent.PushJobAgent.rescheduleFailedJob", return_value=rescheduleValue)

    # 1. getJobWrapper returns an error
    job.initialize = Mock(side_effect=Exception("Error initializing JobWrapper"))
    result = jobAgent._submitJobWrapper(
        jobID=jobID,
        ce=jobAgent.queueDict["ce1.site2.com_condor"]["CE"],
        diracInstallLocation="diracInstallLocation",
        jobParams=jobParams,
        resourceParams={},
        optimizerParams={},
        processors=1,
    )

    assert "PayloadResults" in jobParams
    assert jobParams["PayloadResults"] == jobAgent.payloadResultFile
    assert "Checksum" in jobParams
    assert jobParams["Checksum"] == jobAgent.checkSumResultsFile

    assert not result["OK"], result
    assert result["Message"] == f"Cannot get a JobWrapper instance for job {jobID}"

    assert os.getcwd() == cwd

    job.sendJobAccounting.assert_called_with(
        status=rescheduleValue, minorStatus=JobMinorStatus.JOB_WRAPPER_INITIALIZATION
    )

    # 2. getJobWrapper returns a JobWrapper instance but fails to process input sandbox
    jobParams = {"InputSandbox": True}
    job.initialize = Mock()
    job.sendJobAccounting.reset_mock()
    job.transferInputSandbox = Mock(side_effect=Exception("Error transferring input sandbox"))

    job.owner = None
    job.userGroup = None
    job.jobArgs = jobParams

    result = jobAgent._submitJobWrapper(
        jobID=jobID,
        ce=jobAgent.queueDict["ce1.site2.com_condor"]["CE"],
        diracInstallLocation="diracInstallLocation",
        jobParams=jobParams,
        resourceParams={},
        optimizerParams={},
        processors=1,
    )

    assert "PayloadResults" in jobParams
    assert jobParams["PayloadResults"] == jobAgent.payloadResultFile
    assert "Checksum" in jobParams
    assert jobParams["Checksum"] == jobAgent.checkSumResultsFile

    assert not result["OK"], result
    assert "Cannot get input sandbox of job" in result["Message"]

    assert os.getcwd() == cwd

    job.sendJobAccounting.assert_called_with(
        status=rescheduleValue, minorStatus=JobMinorStatus.DOWNLOADING_INPUT_SANDBOX
    )

    # 3. getJobWrapper returns a JobWrapper instance but fails to process input data
    jobParams = {"InputSandbox": True, "InputData": True}
    job.initialize = Mock()
    job.sendJobAccounting.reset_mock()
    job.transferInputSandbox = Mock(return_value=S_OK())
    job.resolveInputData = Mock(side_effect=Exception("Error resolving input data"))

    job.owner = None
    job.userGroup = None
    job.jobArgs = jobParams

    result = jobAgent._submitJobWrapper(
        jobID=jobID,
        ce=jobAgent.queueDict["ce1.site2.com_condor"]["CE"],
        diracInstallLocation="diracInstallLocation",
        jobParams=jobParams,
        resourceParams={},
        optimizerParams={},
        processors=1,
    )

    assert "PayloadResults" in jobParams
    assert jobParams["PayloadResults"] == jobAgent.payloadResultFile
    assert "Checksum" in jobParams
    assert jobParams["Checksum"] == jobAgent.checkSumResultsFile

    assert not result["OK"], result
    assert "Cannot get input data of job" in result["Message"]

    assert os.getcwd() == cwd

    job.sendJobAccounting.assert_called_with(status=rescheduleValue, minorStatus=JobMinorStatus.INPUT_DATA_RESOLUTION)

    # 4. getJobWrapper returns a JobWrapper instance but fails to pre-process payload
    jobParams = {"InputSandbox": True, "InputData": True, "Payload": True}
    job.initialize = Mock()
    job.sendJobAccounting.reset_mock()
    job.transferInputSandbox = Mock(return_value=S_OK())
    job.resolveInputData = Mock(return_value=S_OK())
    job.preProcess = Mock(side_effect=S_ERROR("Error pre-processing payload"))

    job.owner = None
    job.userGroup = None
    job.jobArgs = jobParams

    result = jobAgent._submitJobWrapper(
        jobID=jobID,
        ce=jobAgent.queueDict["ce1.site2.com_condor"]["CE"],
        diracInstallLocation="diracInstallLocation",
        jobParams=jobParams,
        resourceParams={},
        optimizerParams={},
        processors=1,
    )

    assert "PayloadResults" in jobParams
    assert jobParams["PayloadResults"] == jobAgent.payloadResultFile
    assert "Checksum" in jobParams
    assert jobParams["Checksum"] == jobAgent.checkSumResultsFile

    assert not result["OK"], result
    assert "JobWrapper failed the preprocessing phase for" in result["Message"]

    assert os.getcwd() == cwd

    job.sendJobAccounting.assert_called_with(status=rescheduleValue, minorStatus=JobMinorStatus.JOB_WRAPPER_EXECUTION)

    # 5. getJobWrapper returns a JobWrapper instance but fails to submit the job
    mocker.patch("DIRAC.WorkloadManagementSystem.Utilities.Utils.createJobWrapper", return_value=S_OK({}))
    mocker.patch("DIRAC.gConfig.getOption", return_value=S_OK("Setup"))
    jobParams = {"InputSandbox": True, "InputData": True, "Payload": True}
    job.initialize = Mock()

    jobID = jobID
    job.jobIDPath = Path(jobID)

    job.sendJobAccounting.reset_mock()
    job.transferInputSandbox = Mock(return_value=S_OK())
    job.resolveInputData = Mock(return_value=S_OK())
    job.preProcess = Mock(return_value=S_OK())

    job.owner = None
    job.userGroup = None
    job.jobArgs = jobParams

    ce = Mock()
    ce.submitJob = Mock(return_value=S_ERROR("Error submitting job"))

    result = jobAgent._submitJobWrapper(
        jobID=jobID,
        ce=ce,
        diracInstallLocation="diracInstallLocation",
        jobParams=jobParams,
        resourceParams={},
        optimizerParams={},
        processors=1,
    )

    assert "PayloadResults" in jobParams
    assert jobParams["PayloadResults"] == jobAgent.payloadResultFile
    assert "Checksum" in jobParams
    assert jobParams["Checksum"] == jobAgent.checkSumResultsFile

    assert not result["OK"], result
    assert result["Message"] == "Error submitting job"

    assert os.getcwd() == cwd

    job.sendJobAccounting.assert_called_with(status=rescheduleValue, minorStatus=JobMinorStatus.JOB_WRAPPER_EXECUTION)

    # 6. getJobWrapper returns a JobWrapper instance and submits it successfully
    mocker.patch("DIRAC.WorkloadManagementSystem.Utilities.Utils.createJobWrapper", return_value=S_OK({}))
    jobParams = {"InputSandbox": True, "InputData": True, "Payload": True}
    job.initialize = Mock()

    jobID = jobID
    job.jobIDPath = Path(jobID)

    job.sendJobAccounting.reset_mock()
    job.transferInputSandbox = Mock(return_value=S_OK())
    job.resolveInputData = Mock(return_value=S_OK())
    job.preProcess = Mock(return_value=S_OK())

    job.owner = None
    job.userGroup = None
    job.jobArgs = jobParams

    ce = Mock()
    ce.submitJob = Mock(return_value={"OK": True, "Value": ["456"], "PilotStampDict": {"456": "abcdef"}})

    result = jobAgent._submitJobWrapper(
        jobID=jobID,
        ce=ce,
        diracInstallLocation="diracInstallLocation",
        jobParams=jobParams,
        resourceParams={},
        optimizerParams={},
        processors=1,
    )

    assert "PayloadResults" in jobParams
    assert jobParams["PayloadResults"] == jobAgent.payloadResultFile
    assert "Checksum" in jobParams
    assert jobParams["Checksum"] == jobAgent.checkSumResultsFile

    assert result["OK"], result

    assert os.getcwd() == cwd

    job.sendJobAccounting.assert_not_called()
    shutil.rmtree("job")
