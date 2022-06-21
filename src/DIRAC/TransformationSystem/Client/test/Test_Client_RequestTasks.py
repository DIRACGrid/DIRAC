""" pytest for WorkflowTasks
"""
# pylint: disable=protected-access,missing-docstring,invalid-name
import json
from unittest.mock import MagicMock

from pytest import mark

parametrize = mark.parametrize

from hypothesis import given, settings
from hypothesis.strategies import (
    composite,
    integers,
    lists,
    text,
    dictionaries,
    from_regex,
)
from string import ascii_letters, digits


from DIRAC.Core.Utilities.JEncode import encode


from DIRAC.TransformationSystem.Client.BodyPlugin.DummyBody import DummyBody
from DIRAC.TransformationSystem.Client.RequestTasks import RequestTasks


@composite
def taskStrategy(draw):
    """Generate a strategy that returns a task dictionary"""
    transformationID = draw(integers(min_value=1))
    targetSE = ",".join(draw(lists(text(ascii_letters, min_size=5, max_size=10), min_size=1, max_size=3)))
    inputData = draw(lists(from_regex("(/[a-z]+)+", fullmatch=True), min_size=1, max_size=10))

    return {"TransformationID": transformationID, "TargetSE": targetSE, "InputData": inputData}


def taskDictStrategy():
    return dictionaries(
        integers(min_value=1), taskStrategy(), min_size=1, max_size=10  # pylint: disable=no-value-for-parameter
    )


mockTransClient = MagicMock()
mockReqClient = MagicMock()
mockReqValidator = MagicMock()


reqTasks = RequestTasks(
    transClient=mockTransClient,
    requestClient=mockReqClient,
    requestValidator=mockReqValidator,
)


@parametrize(
    "transBody",
    [
        "removal;RemoveFile",  # Transformation to remove files
        "removal;RemoveReplica",  # Transformation to remove Replicas
        "anything;ReplicateAndRegister",  # Transformation to replicate and register, first parameter is useless
        "",  # if no Body, we expect replicateAndRegister
    ],
)
@mark.slow
@settings(max_examples=50, deadline=500)
@given(
    owner=text(ascii_letters + "-_" + digits, min_size=1),
    taskDict=taskDictStrategy(),
)
def test_prepareSingleOperationsBody(transBody, owner, taskDict):
    """Test different bodies that should be routed through the
    singleOperationBody method.
    """

    # keep the number of tasks for later
    originalNbOfTasks = len(taskDict)

    # Make up the DN and the group
    ownerDN = "DN_" + owner
    ownerGroup = "group_" + owner

    res = reqTasks.prepareTransformationTasks(transBody, taskDict, owner=owner, ownerGroup=ownerGroup, ownerDN=ownerDN)

    assert res["OK"], res

    # prepareTransformationTasks can pop tasks if a problem occurs,
    # so check that this did not happen
    assert len(res["Value"]) == originalNbOfTasks

    for _taskID, task in taskDict.items():

        req = task.get("TaskObject")

        # Checks whether we got a Request assigned
        assert req

        # Check that the attributes of the request are what
        # we expect them to be
        assert req.OwnerDN == ownerDN
        assert req.OwnerGroup == ownerGroup

        # Make sure we only have one operation
        assert len(req) == 1

        ops = req[0]

        # Check the operation type
        # The operation type is either given as second parameter of the body
        # or if not, it is ReplicateAndRegister

        expectedOpsType = transBody.split(";")[-1] if transBody else "ReplicateAndRegister"
        assert ops.Type == expectedOpsType

        # Check that the targetSE is set correctly
        assert ops.TargetSE == task["TargetSE"]

        # Make sure we have one file per LFN in the task
        assert len(ops) == len(task["InputData"])

        # Checks that there is one file per LFN
        assert {f.LFN for f in ops} == set(task["InputData"])


@parametrize(
    "transBody",
    [
        [
            ("ReplicateAndRegister", {"TargetSE": "BAR-SRM"}),
        ],
        [
            ("ReplicateAndRegister", {"TargetSE": "TASK:TargetSE"}),
        ],
        [
            ("ReplicateAndRegister", {"SourceSE": "FOO-SRM", "TargetSE": "BAR-SRM"}),
            ("RemoveReplica", {"TargetSE": "FOO-SRM"}),
        ],
        [
            ("ReplicateAndRegister", {"SourceSE": "FOO-SRM", "TargetSE": "TASK:TargetSE"}),
            ("RemoveReplica", {"TargetSE": "FOO-SRM"}),
        ],
    ],
    ids=[
        "Single operation, no substitution",
        "Single operation, with substitution",
        "Multiple operations, no substitution",
        "Multiple operations, with substitution",
    ],
)
@mark.slow
@settings(max_examples=50, deadline=500)
@given(
    owner=text(ascii_letters + "-_" + digits, min_size=1),
    taskDict=taskDictStrategy(),
)
def test_prepareMultiOperationsBody(transBody, owner, taskDict):
    """Test different bodies that should be routed through the
    multiOperationsBody method.
    """

    # keep the number of tasks for later
    originalNbOfTasks = len(taskDict)

    # Make up the DN and the group
    ownerDN = "DN_" + owner
    ownerGroup = "group_" + owner

    res = reqTasks.prepareTransformationTasks(
        json.dumps(transBody), taskDict, owner=owner, ownerGroup=ownerGroup, ownerDN=ownerDN
    )

    assert res["OK"], res

    # prepareTransformationTasks can pop tasks if a problem occurs,
    # so check that this did not happen
    assert len(res["Value"]) == originalNbOfTasks

    for _taskID, task in taskDict.items():

        req = task.get("TaskObject")

        # Checks whether we got a Request assigned
        assert req

        # Check that the attributes of the request are what
        # we expect them to be
        assert req.OwnerDN == ownerDN
        assert req.OwnerGroup == ownerGroup

        # Make sure we have as many operations as tuple in the body
        assert len(req) == len(transBody)

        # Loop over each operation
        # to check their attributes
        for opsID, ops in enumerate(req):

            expectedOpsType, expectedOpsAttributes = transBody[opsID]

            # Compare the operation type with what we want
            assert ops.Type == expectedOpsType

            # Check the operation attributes one after the other
            for opsAttr, opsVal in expectedOpsAttributes.items():

                # If the expected value starts with 'TASK:'
                # we should make the substitution with whatever is in
                # the taskDict.
                # So it should be different
                if opsVal.startswith("TASK:"):
                    assert getattr(ops, opsAttr) != opsVal
                # Otherwise, make sure it matches
                else:
                    assert getattr(ops, opsAttr) == opsVal

                # Make sure we have one file per LFN in the task
                assert len(ops) == len(task["InputData"])

                # Checks that there is one file per LFN
                assert {f.LFN for f in ops} == set(task["InputData"])


@parametrize(
    "transBody",
    [
        # We request a key that does not exist in the taskDict
        [
            ("ReplicateAndRegister", {"TargetSE": "TASK:NotInTaskDict"}),
        ],
    ],
    ids=[
        "Non existing substitution",
    ],
)
@mark.slow
@settings(max_examples=50, deadline=500)
@given(
    owner=text(ascii_letters + "-_" + digits, min_size=1),
    taskDict=taskDictStrategy(),
)
def test_prepareProblematicMultiOperationsBody(transBody, owner, taskDict):
    """Test different bodies that should be routed through the
    multiOperationBody method, but that have a problem
    """

    # keep the number of tasks for later
    originalNbOfTasks = len(taskDict)

    # Make up the DN and the group
    ownerDN = "DN_" + owner
    ownerGroup = "group_" + owner

    res = reqTasks.prepareTransformationTasks(
        json.dumps(transBody), taskDict, owner=owner, ownerGroup=ownerGroup, ownerDN=ownerDN
    )

    assert res["OK"], res

    # prepareTransformationTasks pop tasks if a problem occurs,
    # so make sure it happened
    assert len(res["Value"]) != originalNbOfTasks

    # Check that other tasks are fine
    # Note: currently, we will never enter this loop as
    # the transbody are buggy, so all the tasks should be removed.
    # I just prepare the future :-)

    for _taskID, task in taskDict.items():

        req = task.get("TaskObject")

        # Checks whether we got a Request assigned
        assert req

        # Check that the attributes of the request are what
        # we expect them to be
        assert req.OwnerDN == ownerDN
        assert req.OwnerGroup == ownerGroup

        # Make sure we have as many operations as tuple in the body
        assert len(req) == len(transBody)

        # Loop over each operation
        # to check their attributes
        for opsID, ops in enumerate(req):

            expectedOpsType, expectedOpsAttributes = transBody[opsID]

            # Compare the operation type with what we want
            assert ops.Type == expectedOpsType

            # Check the operation attributes one after the other
            for opsAttr, opsVal in expectedOpsAttributes.items():

                # If the expected value starts with 'TASK:'
                # we should make the substitution with whatever is in
                # the taskDict.
                # So it should be different
                if opsVal.startswith("TASK:"):
                    assert getattr(ops, opsAttr) != opsVal
                # Otherwise, make sure it matches
                else:
                    # Check that the targetSE is set correctly
                    assert getattr(ops, opsAttr) == opsVal

                # Make sure we have one file per LFN in the task
                assert len(ops) == len(task["InputData"])

                # Checks that there is one file per LFN
                assert {f.LFN for f in ops} == set(task["InputData"])


@mark.slow
@settings(max_examples=50, deadline=500)
@given(
    taskDict=taskDictStrategy(),
    pluginFactor=integers(),
)
def test_complexBodyPlugin(taskDict, pluginFactor):
    """This test makes sure that we can load the BodyPlugin objects"""

    transBody = DummyBody(factor=pluginFactor)

    # keep the number of tasks for later
    originalNbOfTasks = len(taskDict)

    # Make up the DN and the group
    ownerDN = "DN_owner"
    ownerGroup = "group_owner"

    res = reqTasks.prepareTransformationTasks(
        encode(transBody), taskDict, owner="owner", ownerGroup=ownerGroup, ownerDN=ownerDN
    )

    assert res["OK"], res

    # prepareTransformationTasks can pop tasks if a problem occurs,
    # so check that this did not happen
    assert len(res["Value"]) == originalNbOfTasks

    for _taskID, task in taskDict.items():

        req = task.get("TaskObject")

        # Checks whether we got a Request assigned
        assert req

        # Check that the attributes of the request are what
        # we expect them to be
        assert req.OwnerDN == ownerDN
        assert req.OwnerGroup == ownerGroup

        # DummyBody only creates a single operation.
        # It should be a forward diset, and the
        # argument should be the number of files in the task
        # multiplied by the pluginParam

        assert len(req) == 1

        ops = req[0]
        assert ops.Type == "ForwardDISET"
        assert json.loads(ops.Arguments) == pluginFactor * len(task["InputData"])
