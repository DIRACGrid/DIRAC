""" Test class for Job Agent
"""

# imports
import pytest
from collections import defaultdict

# DIRAC Components
from DIRAC.ConfigurationSystem.Client.Helpers.Operations import Operations
from DIRAC.WorkloadManagementSystem.Agent.PushJobAgent import PushJobAgent

from DIRAC import gLogger, S_ERROR

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
        ({}, None, None, {"RemoteExecution": True}),
        ({}, "8.0.0", None, {"ReleaseVersion": "8.0.0", "RemoteExecution": True}),
        ({}, ["8.0.0", "7.3.7"], None, {"ReleaseVersion": "8.0.0", "RemoteExecution": True}),
        ({}, None, "Project", {"ReleaseProject": "Project", "RemoteExecution": True}),
        ({}, "8.0.0", "Project", {"ReleaseVersion": "8.0.0", "ReleaseProject": "Project", "RemoteExecution": True}),
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
