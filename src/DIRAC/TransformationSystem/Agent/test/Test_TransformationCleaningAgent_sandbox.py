# pylint: disable=missing-docstring
from unittest.mock import MagicMock

from DIRAC import gLogger
from DIRAC.TransformationSystem.Agent.TransformationCleaningAgent import TransformationCleaningAgent


def _bare_agent():
    # Bypass AgentModule.__init__ (needs a running config); we only test the helper.
    agent = TransformationCleaningAgent.__new__(TransformationCleaningAgent)
    agent.log = gLogger.getSubLogger("test")
    agent.sandboxDB = None
    return agent


def test_unassign_callsDBWithTransformationEntity():
    agent = _bare_agent()
    agent.sandboxDB = MagicMock()
    agent.sandboxDB.unassignEntities.return_value = {"OK": True, "Value": 1}

    agent._unassignTransformationSandboxes(12345)

    agent.sandboxDB.unassignEntities.assert_called_once_with(["Transformation:12345"])


def test_unassign_noDBisNoop():
    agent = _bare_agent()  # sandboxDB is None
    agent._unassignTransformationSandboxes(12345)  # must not raise


def test_unassign_dbErrorDoesNotRaise():
    agent = _bare_agent()
    agent.sandboxDB = MagicMock()
    agent.sandboxDB.unassignEntities.return_value = {"OK": False, "Message": "boom"}

    agent._unassignTransformationSandboxes(12345)
    agent.sandboxDB.unassignEntities.assert_called_once_with(["Transformation:12345"])
