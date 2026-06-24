# pylint: disable=missing-docstring
from unittest.mock import MagicMock

from DIRAC.TransformationSystem.Agent.TransformationCleaningAgent import TransformationCleaningAgent

# The agent's __init__ needs a running config, so these exercise the methods with a
# stand-in ``self`` (a MagicMock) carrying only the attributes the methods touch.


def test_unassign_callsDBWithTransformationEntity():
    agent = MagicMock()
    agent.sandboxDB.unassignEntities.return_value = {"OK": True, "Value": 1}

    res = TransformationCleaningAgent._unassignTransformationSandboxes(agent, 12345)

    assert res["OK"]
    agent.sandboxDB.unassignEntities.assert_called_once_with(["Transformation:12345"])
    agent.log.error.assert_not_called()
    agent.log.exception.assert_not_called()


def test_unassign_noDBisOk():
    agent = MagicMock()
    agent.sandboxDB = None
    # Nothing to unassign: succeed so cleaning is not blocked in DB-less deployments.
    assert TransformationCleaningAgent._unassignTransformationSandboxes(agent, 12345)["OK"]


def test_unassign_dbErrorIsLoggedAndReturnsError():
    agent = MagicMock()
    agent.sandboxDB.unassignEntities.return_value = {"OK": False, "Message": "boom"}

    # A failed unassignment must be loud AND returned as an error so the caller fails
    # the cleaning and retries (otherwise the sandboxes leak).
    res = TransformationCleaningAgent._unassignTransformationSandboxes(agent, 12345)

    assert not res["OK"]
    agent.sandboxDB.unassignEntities.assert_called_once_with(["Transformation:12345"])
    agent.log.error.assert_called_once()


def test_unassign_unexpectedExceptionIsLoggedAndReturnsError():
    agent = MagicMock()
    agent.sandboxDB.unassignEntities.side_effect = RuntimeError("db down")

    # An unexpected exception must not propagate, but must be logged loudly and returned
    # as an error so the cleaning is retried.
    res = TransformationCleaningAgent._unassignTransformationSandboxes(agent, 12345)

    assert not res["OK"]
    agent.log.exception.assert_called_once()


def test_cleanTransformation_failsWhenUnassignFails():
    # The whole clean must fail (and be retried) if the sandbox can't be unassigned,
    # rather than proceeding and leaking the now-orphaned sandbox assignment.
    agent = MagicMock()
    agent._unassignTransformationSandboxes.return_value = {"OK": False, "Message": "boom"}

    res = TransformationCleaningAgent.cleanTransformation(agent, 12345)

    assert not res["OK"]
    agent.getTransformationDirectories.assert_not_called()
