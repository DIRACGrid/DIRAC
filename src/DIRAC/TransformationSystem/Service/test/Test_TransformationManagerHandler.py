# pylint: disable=missing-docstring
from unittest.mock import MagicMock, patch

from DIRAC.Core.Security.Properties import SecurityProperty
from DIRAC.Core.Workflow.Workflow import Workflow
from DIRAC.Core.Workflow.Parameter import Parameter
from DIRAC.TransformationSystem.Service.TransformationManagerHandler import TransformationManagerHandlerMixin

# The handler needs a running service to initialise, so these exercise the methods with
# a stand-in ``self`` (a MagicMock) carrying only the attributes the methods touch.


def _body_with_input_sandbox(value):
    wf = Workflow()
    wf.setName("testprod")
    wf.addParameter(Parameter("InputSandbox", value, "JDL", "", "", True, False, "Input sandbox"))
    return wf.toXMLString()


def test_assign_extractsSBRefsAndDelegates():
    handler = MagicMock()
    handler.sandboxDB.assignSandboxesToEntities.return_value = {"OK": True, "Value": 1}

    body = _body_with_input_sandbox("SB:SE|/p/opts.tar.bz2;local.txt")
    res = TransformationManagerHandlerMixin._assignInputSandboxesToTransformation(
        handler, 321, body, "alice", "lhcb_prod"
    )

    assert res["OK"]
    handler.sandboxDB.assignSandboxesToEntities.assert_called_once_with(
        {"Transformation:321": [("SB:SE|/p/opts.tar.bz2", "Input")]}, "alice", "lhcb_prod"
    )


def test_assign_noSBRefsIsNoop():
    handler = MagicMock()
    res = TransformationManagerHandlerMixin._assignInputSandboxesToTransformation(
        handler, 321, _body_with_input_sandbox("local.txt;lfn:/foo"), "alice", "g"
    )
    assert res["OK"]
    handler.sandboxDB.assignSandboxesToEntities.assert_not_called()


def test_assign_noInputSandboxParamIsNoop():
    handler = MagicMock()
    wf = Workflow()
    wf.setName("testprod")
    res = TransformationManagerHandlerMixin._assignInputSandboxesToTransformation(
        handler, 321, wf.toXMLString(), "a", "g"
    )
    assert res["OK"]
    handler.sandboxDB.assignSandboxesToEntities.assert_not_called()


def test_assign_nonWorkflowBodyIsNoop():
    handler = MagicMock()
    res = TransformationManagerHandlerMixin._assignInputSandboxesToTransformation(
        handler, 321, "not-a-workflow", "a", "g"
    )
    assert res["OK"]
    handler.sandboxDB.assignSandboxesToEntities.assert_not_called()


def test_assign_sbRefsButNoSandboxDBIsError():
    # SB: refs present but no SandboxMetadataDB: must NOT silently succeed, because the
    # sandboxes would then be cleaned and break every downstream job.
    handler = MagicMock()
    handler.sandboxDB = None
    res = TransformationManagerHandlerMixin._assignInputSandboxesToTransformation(
        handler, 321, _body_with_input_sandbox("SB:SE|/p/1"), "a", "g"
    )
    assert not res["OK"]


def test_assign_dbErrorIsError():
    handler = MagicMock()
    handler.sandboxDB.assignSandboxesToEntities.return_value = {"OK": False, "Message": "boom"}
    res = TransformationManagerHandlerMixin._assignInputSandboxesToTransformation(
        handler, 321, _body_with_input_sandbox("SB:SE|/p/1"), "a", "g"
    )
    assert not res["OK"]
    assert "boom" in res["Message"]
    handler.sandboxDB.assignSandboxesToEntities.assert_called_once()


def test_assign_multipleSBRefs():
    handler = MagicMock()
    handler.sandboxDB.assignSandboxesToEntities.return_value = {"OK": True, "Value": 2}
    body = _body_with_input_sandbox("SB:SE|/a.tar.bz2;local.txt;SB:SE|/b.tar.bz2")
    res = TransformationManagerHandlerMixin._assignInputSandboxesToTransformation(
        handler, 321, body, "alice", "lhcb_prod"
    )
    assert res["OK"]
    handler.sandboxDB.assignSandboxesToEntities.assert_called_once_with(
        {"Transformation:321": [("SB:SE|/a.tar.bz2", "Input"), ("SB:SE|/b.tar.bz2", "Input")]},
        "alice",
        "lhcb_prod",
    )


def test_assign_acceptsListValuedInputSandbox():
    # Belt-and-suspenders: if a producer ever stores InputSandbox list-form rather
    # than the canonical ';'-joined string, we still extract the SB: refs.
    handler = MagicMock()
    handler.sandboxDB.assignSandboxesToEntities.return_value = {"OK": True, "Value": 1}

    param = MagicMock()
    param.getValue.return_value = ["SB:SE|/a.tar.bz2", "local.txt"]
    workflow = MagicMock()
    workflow.parameters.find.return_value = param

    with patch(
        "DIRAC.TransformationSystem.Service.TransformationManagerHandler.fromXMLString",
        return_value=workflow,
    ):
        res = TransformationManagerHandlerMixin._assignInputSandboxesToTransformation(
            handler, 321, "<body/>", "alice", "lhcb_prod"
        )

    assert res["OK"]
    handler.sandboxDB.assignSandboxesToEntities.assert_called_once_with(
        {"Transformation:321": [("SB:SE|/a.tar.bz2", "Input")]}, "alice", "lhcb_prod"
    )


def test_addTransformation_rollsBackWhenPinningFails():
    # The critical safety property: if a transformation references sandboxes that
    # cannot be pinned, the transformation must be deleted and an error returned,
    # never left created-but-unpinned.
    handler = MagicMock()
    handler.transformationDB.addTransformation.return_value = {"OK": True, "Value": 4242}
    handler.transformationDB.deleteTransformation.return_value = {"OK": True}
    handler.getRemoteCredentials.return_value = {
        "username": "alice",
        "group": "lhcb_prod",
        "properties": [SecurityProperty.PRODUCTION_MANAGEMENT],
    }
    handler._assignInputSandboxesToTransformation.return_value = {"OK": False, "Message": "no pin"}

    body = _body_with_input_sandbox("SB:SE|/p/opts.tar.bz2")
    res = TransformationManagerHandlerMixin.export_addTransformation(
        handler, "prod", "desc", "long", "MCSimulation", "Standard", "Manual", "", body=body
    )

    assert not res["OK"]
    handler.transformationDB.deleteTransformation.assert_called_once_with(4242, author="alice")
