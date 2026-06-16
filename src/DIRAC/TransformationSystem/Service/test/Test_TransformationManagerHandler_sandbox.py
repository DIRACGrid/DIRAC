# pylint: disable=missing-docstring
from unittest.mock import MagicMock

from DIRAC.Core.Workflow.Workflow import Workflow
from DIRAC.Core.Workflow.Parameter import Parameter
from DIRAC.TransformationSystem.Service.TransformationManagerHandler import TransformationManagerHandlerMixin


def _make_handler(sandboxDB):
    handler = TransformationManagerHandlerMixin.__new__(TransformationManagerHandlerMixin)
    handler.sandboxDB = sandboxDB
    handler.log = MagicMock()
    return handler


def _body_with_input_sandbox(value):
    wf = Workflow()
    wf.setName("testprod")
    wf.addParameter(Parameter("InputSandbox", value, "JDL", "", "", True, False, "Input sandbox"))
    return wf.toXMLString()


def test_assign_extractsSBRefsAndDelegates():
    sandboxDB = MagicMock()
    sandboxDB.assignSandboxesToEntities.return_value = {"OK": True, "Value": 1}
    handler = _make_handler(sandboxDB)

    body = _body_with_input_sandbox("SB:SE|/p/opts.tar.bz2;local.txt")
    handler._assignInputSandboxesToTransformation(321, body, "alice", "lhcb_prod")

    sandboxDB.assignSandboxesToEntities.assert_called_once_with(
        {"Transformation:321": [("SB:SE|/p/opts.tar.bz2", "Input")]}, "alice", "lhcb_prod"
    )


def test_assign_noSBRefsIsNoop():
    sandboxDB = MagicMock()
    handler = _make_handler(sandboxDB)
    handler._assignInputSandboxesToTransformation(321, _body_with_input_sandbox("local.txt;lfn:/foo"), "alice", "g")
    sandboxDB.assignSandboxesToEntities.assert_not_called()


def test_assign_noInputSandboxParamIsNoop():
    sandboxDB = MagicMock()
    handler = _make_handler(sandboxDB)
    wf = Workflow()
    wf.setName("testprod")
    handler._assignInputSandboxesToTransformation(321, wf.toXMLString(), "alice", "g")
    sandboxDB.assignSandboxesToEntities.assert_not_called()


def test_assign_nonWorkflowBodyDoesNotRaise():
    sandboxDB = MagicMock()
    handler = _make_handler(sandboxDB)
    handler._assignInputSandboxesToTransformation(321, "not-a-workflow", "alice", "g")
    sandboxDB.assignSandboxesToEntities.assert_not_called()


def test_assign_noSandboxDBIsNoop():
    handler = _make_handler(None)
    handler._assignInputSandboxesToTransformation(321, _body_with_input_sandbox("SB:SE|/p/1"), "a", "g")


def test_assign_dbErrorIsSwallowed():
    sandboxDB = MagicMock()
    sandboxDB.assignSandboxesToEntities.return_value = {"OK": False, "Message": "boom"}
    handler = _make_handler(sandboxDB)
    handler._assignInputSandboxesToTransformation(321, _body_with_input_sandbox("SB:SE|/p/1"), "a", "g")
    sandboxDB.assignSandboxesToEntities.assert_called_once()


def test_assign_multipleSBRefs():
    sandboxDB = MagicMock()
    sandboxDB.assignSandboxesToEntities.return_value = {"OK": True, "Value": 2}
    handler = _make_handler(sandboxDB)
    body = _body_with_input_sandbox("SB:SE|/a.tar.bz2;local.txt;SB:SE|/b.tar.bz2")
    handler._assignInputSandboxesToTransformation(321, body, "alice", "lhcb_prod")
    sandboxDB.assignSandboxesToEntities.assert_called_once_with(
        {"Transformation:321": [("SB:SE|/a.tar.bz2", "Input"), ("SB:SE|/b.tar.bz2", "Input")]},
        "alice",
        "lhcb_prod",
    )
