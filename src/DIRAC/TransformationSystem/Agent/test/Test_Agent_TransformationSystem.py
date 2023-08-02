""" Test class for agents
"""
# pylint: disable=protected-access, missing-docstring, invalid-name, line-too-long

# imports
import datetime

import pytest
from unittest.mock import MagicMock

from DIRAC.TransformationSystem.Client import TransformationFilesStatus

# sut
from DIRAC.TransformationSystem.Agent.TaskManagerAgentBase import TaskManagerAgentBase
from DIRAC.TransformationSystem.Agent.TransformationAgent import TransformationAgent

mockAM = MagicMock()


tc_mock = MagicMock()
tm_mock = MagicMock()
clients = {"TransformationClient": tc_mock, "TaskManager": tm_mock}

transDict = {"TransformationID": 1, "Operations": ["op1", "op2"], "Body": "veryBigBody"}
tasks = {
    "OK": True,
    "Value": [
        {
            "CreationTime": None,
            "ExternalID": "1",
            "ExternalStatus": "Reserved",
            "LastUpdateTime": None,
            "RunNumber": 0,
            "TargetSE": "Unknown",
            "TaskID": 1,
            "TransformationID": 101,
        },
        {
            "CreationTime": datetime.datetime(2014, 7, 21, 14, 19, 3),
            "ExternalID": "0",
            "ExternalStatus": "Reserved",
            "LastUpdateTime": datetime.datetime(2014, 7, 21, 14, 19, 3),
            "RunNumber": 0,
            "TargetSE": "Unknown",
            "TaskID": 2,
            "TransformationID": 101,
        },
    ],
}
sOk = {"OK": True, "Value": []}
sError = {"OK": False, "Message": "a mess"}


@pytest.mark.parametrize(
    "tcMockReturnValue, tmMockGetSubmittedTaskStatusReturnvalue, expected",
    [
        (sError, {"OK": True}, False),  # errors
        (sOk, {"OK": True}, True),  # no tasks
        (tasks, sError, False),  # tasks, fail in update
        (tasks, {"OK": True, "Value": {}}, True),  # tasks, nothing to update
        (tasks, {"OK": True, "Value": {"Running": [1, 2], "Done": [3]}}, True),  # tasks, to update, no errors
    ],
)
def test_updateTaskStatusSuccess(mocker, tcMockReturnValue, tmMockGetSubmittedTaskStatusReturnvalue, expected):
    mocker.patch("DIRAC.TransformationSystem.Agent.TaskManagerAgentBase.AgentModule", side_effect=mockAM)
    mocker.patch("DIRAC.TransformationSystem.Agent.TaskManagerAgentBase.FileReport", side_effect=MagicMock())
    mocker.patch(
        "DIRAC.TransformationSystem.Agent.TaskManagerAgentBase.TaskManagerAgentBase.am_getOption", side_effect=mockAM
    )
    tmab = TaskManagerAgentBase()
    tc_mock.getTransformationTasks.return_value = tcMockReturnValue
    tm_mock.getSubmittedTaskStatus.return_value = tmMockGetSubmittedTaskStatusReturnvalue
    res = tmab.updateTaskStatus(transDict, clients)
    assert res["OK"] == expected


@pytest.mark.parametrize(
    "tcMockGetTransformationFilesReturnValue, tmMockGetSubmittedFileStatusReturnValue, expected",
    [
        (sError, None, False),  # errors
        (sOk, None, True),  # no files
        ({"OK": True, "Value": [{"file1": "boh", "TaskID": 1}]}, sError, False),  # files, failing to update
        ({"OK": True, "Value": [{"file1": "boh", "TaskID": 1}]}, sOk, True),  # files, nothing to update
        (
            {"OK": True, "Value": [{"file1": "boh", "TaskID": 1}]},
            {"OK": True, "Value": {"file1": "OK", "file2": "NOK"}},
            True,
        ),  # files, something to update
    ],
)
def test_updateFileStatusSuccess(
    mocker, tcMockGetTransformationFilesReturnValue, tmMockGetSubmittedFileStatusReturnValue, expected
):
    mocker.patch("DIRAC.TransformationSystem.Agent.TaskManagerAgentBase.AgentModule", side_effect=mockAM)
    mocker.patch("DIRAC.TransformationSystem.Agent.TaskManagerAgentBase.FileReport", side_effect=MagicMock())
    tmab = TaskManagerAgentBase()
    tc_mock.getTransformationFiles.return_value = tcMockGetTransformationFilesReturnValue
    tm_mock.getSubmittedFileStatus.return_value = tmMockGetSubmittedFileStatusReturnValue
    res = tmab.updateFileStatus(transDict, clients)
    assert res["OK"] == expected


@pytest.mark.parametrize(
    ", ".join(
        [
            "tcMockGetTransformationTasksReturnValue",
            "tmMockUpdateTransformationReservedTasksReturnValue",
            "tcMockSetTaskStatusAndWmsIDReturnValue",
            "expected",
        ]
    ),
    [
        (sError, None, None, False),  # errors getting
        (sOk, None, None, True),  # no tasks
        (tasks, sError, None, False),  # tasks, failing to update
        (
            tasks,
            {"OK": True, "Value": {"NoTasks": [], "TaskNameIDs": {"1_1": 123, "2_1": 456}}},
            sError,
            False,
        ),  # tasks, something to update, fail
        (
            tasks,
            {"OK": True, "Value": {"NoTasks": ["3_4", "5_6"], "TaskNameIDs": {"1_1": 123, "2_1": 456}}},
            {"OK": True},
            True,
        ),
    ],
)  # tasks, something to update, no fail
def test_checkReservedTasks(
    mocker,
    tcMockGetTransformationTasksReturnValue,
    tmMockUpdateTransformationReservedTasksReturnValue,
    tcMockSetTaskStatusAndWmsIDReturnValue,
    expected,
):
    mocker.patch("DIRAC.TransformationSystem.Agent.TaskManagerAgentBase.AgentModule", side_effect=mockAM)
    mocker.patch("DIRAC.TransformationSystem.Agent.TaskManagerAgentBase.FileReport", side_effect=MagicMock())
    tmab = TaskManagerAgentBase()
    tc_mock.getTransformationTasks.return_value = tcMockGetTransformationTasksReturnValue
    tm_mock.updateTransformationReservedTasks.return_value = tmMockUpdateTransformationReservedTasksReturnValue
    tc_mock.setTaskStatusAndWmsID.return_value = tcMockSetTaskStatusAndWmsIDReturnValue
    res = tmab.checkReservedTasks(transDict, clients)
    assert res["OK"] == expected


transDict = {
    "TransformationID": 1,
    "Operations": ["op1", "op2"],
    "Body": "veryBigBody",
    "Owner": "prod",
    "OwnerGroup": "prods",
}
sOkJobDict = {"OK": True, "Value": {"JobDictionary": {123: "foo", 456: "bar"}}}
sOkJobs = {"OK": True, "Value": {123: "foo", 456: "bar"}}


@pytest.mark.parametrize(
    ", ".join(
        [
            "tcMockGetTasksToSubmitReturnValue",
            "tmMockPrepareTransformationTasksReturnValue",
            "tmMockSubmitTransformationTasksReturnValue",
            "tmMockUpdateDBAfterTaskSubmissionReturnValue",
            "expected",
        ]
    ),
    [
        (sError, None, None, None, False),  # errors getting
        ({"OK": True, "Value": {"JobDictionary": {}}}, None, None, None, True),  # no tasks
        (sOkJobDict, sError, None, None, False),  # tasks, errors
        (sOkJobDict, sOkJobs, sError, None, False),  # tasks, still errors
        (sOkJobDict, sOkJobs, sOk, sError, False),  # tasks, still errors
        (sOkJobDict, sOkJobs, sOk, sOk, True),
    ],
)  # tasks, no errors
def test_submitTasks(
    mocker,
    tcMockGetTasksToSubmitReturnValue,
    tmMockPrepareTransformationTasksReturnValue,
    tmMockSubmitTransformationTasksReturnValue,
    tmMockUpdateDBAfterTaskSubmissionReturnValue,
    expected,
):
    mocker.patch("DIRAC.TransformationSystem.Agent.TaskManagerAgentBase.AgentModule", side_effect=mockAM)
    mocker.patch("DIRAC.TransformationSystem.Agent.TaskManagerAgentBase.FileReport", side_effect=MagicMock())
    tmab = TaskManagerAgentBase()
    tc_mock.getTasksToSubmit.return_value = tcMockGetTasksToSubmitReturnValue
    tm_mock.prepareTransformationTasks.return_value = tmMockPrepareTransformationTasksReturnValue
    tm_mock.submitTransformationTasks.return_value = tmMockSubmitTransformationTasksReturnValue
    tm_mock.updateDBAfterTaskSubmission.return_value = tmMockUpdateDBAfterTaskSubmissionReturnValue
    res = tmab.submitTasks(transDict, clients)
    assert res["OK"] == expected
    tmab.maxParametricJobs = 10
    tmab.bulkSubmissionFlag = True
    res = tmab.submitTasks(transDict, clients)
    assert res["OK"] == expected


# TransformationAgent

goodFiles = {
    "OK": True,
    "Value": [
        {
            "ErrorCount": 1,
            "FileID": 17990660,
            "InsertedTime": datetime.datetime(2012, 3, 15, 17, 5, 50),
            "LFN": "/00012574_00000239_1.charmcompleteevent.dst",
            "LastUpdate": datetime.datetime(2012, 3, 16, 23, 43, 26),
            "RunNumber": 90269,
            "Status": TransformationFilesStatus.UNUSED,
            "TargetSE": "Unknown",
            "TaskID": "222",
            "TransformationID": 17042,
            "UsedSE": "CERN-DST,IN2P3-DST,PIC-DST,RAL-DST",
        },
        {
            "ErrorCount": 1,
            "FileID": 17022945,
            "InsertedTime": datetime.datetime(2012, 3, 15, 17, 5, 50),
            "LFN": "/00012574_00000119_1.charmcompleteevent.dst",
            "LastUpdate": datetime.datetime(2012, 3, 16, 23, 54, 59),
            "RunNumber": 90322,
            "Status": TransformationFilesStatus.UNUSED,
            "TargetSE": "Unknown",
            "TaskID": "82",
            "TransformationID": 17042,
            "UsedSE": "CERN-DST,CNAF-DST,RAL-DST,SARA-DST",
        },
    ],
}
noFiles = {"OK": True, "Value": []}


@pytest.mark.parametrize(
    "transDict, getTFiles, expected",
    [
        ({"TransformationID": 123, "Status": "Stopped", "Type": "Replication"}, goodFiles, True),
        ({"TransformationID": 123, "Status": "Stopped", "Type": "Removal"}, noFiles, True),
    ],
)
def test__getTransformationFiles(mocker, transDict, getTFiles, expected):
    mocker.patch("DIRAC.TransformationSystem.Agent.TransformationAgent.AgentModule", side_effect=mockAM)
    tc_mock.getTransformationFiles.return_value = getTFiles
    res = TransformationAgent()._getTransformationFiles(transDict, {"TransformationClient": tc_mock})
    assert res["OK"] == expected
