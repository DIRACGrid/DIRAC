""" Test class for Submission policy
"""
# pylint: disable=protected-access

import pytest
from DIRAC.Core.Utilities.ReturnValues import S_OK

from DIRAC.WorkloadManagementSystem.Client import PilotStatus
from DIRAC.WorkloadManagementSystem.Utilities.SubmissionPolicy import (
    SUBMISSION_POLICIES,
    AggressiveFillingPolicy,
    WaitingSupportedJobsPolicy,
)


def test_AggressiveFillingPolicy():
    """Make sure it always return the number of slots provided"""
    policy = AggressiveFillingPolicy()

    # 1. We want to submit 50 elements
    numberToSubmit = policy.apply(50)
    assert numberToSubmit == 50

    # 2. We want to submit 0 element
    numberToSubmit = policy.apply(0)
    assert numberToSubmit == 0

    # 3. We want to submit -10 elements
    with pytest.raises(RuntimeError):
        numberToSubmit = policy.apply(-10)


def test_WaitingSupportedJobsPolicy(mocker):
    """Make sure it returns the min between the available slots and the jobs available"""
    policy = WaitingSupportedJobsPolicy()

    # 1. We want to submit 50 elements without specifying the CE parameters
    with pytest.raises(KeyError):
        numberToSubmit = policy.apply(50)

    # 2. We want to submit 50 elements but there are no waiting job
    # Because it requires an access to a DB, we mock the value returned by the Matcher
    mocker.patch(
        "DIRAC.WorkloadManagementSystem.Client.MatcherClient.MatcherClient.getMatchingTaskQueues", return_value=S_OK({})
    )
    numberToSubmit = policy.apply(50, ceParameters={})
    assert numberToSubmit == 0

    # 3. We want to submit 50 elements and we have 10 similar waiting jobs
    mocker.patch(
        "DIRAC.WorkloadManagementSystem.Client.MatcherClient.MatcherClient.getMatchingTaskQueues",
        return_value=S_OK({"TQ1": {"Jobs": 10}}),
    )
    numberToSubmit = policy.apply(50, ceParameters={})
    assert numberToSubmit == 10

    # 4. We want to submit 50 elements and we have 10 waiting jobs, split into 2 task queues
    mocker.patch(
        "DIRAC.WorkloadManagementSystem.Client.MatcherClient.MatcherClient.getMatchingTaskQueues",
        return_value=S_OK({"TQ1": {"Jobs": 8}, "TQ2": {"Jobs": 2}}),
    )
    numberToSubmit = policy.apply(50, ceParameters={})
    assert numberToSubmit == 10

    # 5. We want to submit 50 elements and we have 60 similar waiting jobs
    mocker.patch(
        "DIRAC.WorkloadManagementSystem.Client.MatcherClient.MatcherClient.getMatchingTaskQueues",
        return_value=S_OK({"TQ1": {"Jobs": 60}}),
    )
    numberToSubmit = policy.apply(50, ceParameters={})
    assert numberToSubmit == 50

    # 6. We want to submit 50 elements and we have 60 waiting jobs, split into 2 task queues
    mocker.patch(
        "DIRAC.WorkloadManagementSystem.Client.MatcherClient.MatcherClient.getMatchingTaskQueues",
        return_value=S_OK({"TQ1": {"Jobs": 35}, "TQ2": {"Jobs": 25}}),
    )
    numberToSubmit = policy.apply(50, ceParameters={})
    assert numberToSubmit == 50

    # 6. We want to submit 50 elements and we have 60 waiting jobs, split into 2 task queues
    mocker.patch(
        "DIRAC.WorkloadManagementSystem.Client.MatcherClient.MatcherClient.getMatchingTaskQueues",
        return_value=S_OK({"TQ1": {"Jobs": 35}, "TQ2": {"Jobs": 25}}),
    )
    numberToSubmit = policy.apply(50, ceParameters={})
    assert numberToSubmit == 50

    # 7. We want to submit -10 elements
    with pytest.raises(RuntimeError):
        numberToSubmit = policy.apply(-10, ceParameters={})
