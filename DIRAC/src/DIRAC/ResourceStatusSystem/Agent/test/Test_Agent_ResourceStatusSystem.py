""" Test class for agents
"""
# pylint: disable=protected-access, missing-docstring, invalid-name, line-too-long

import pytest
from unittest.mock import MagicMock

from DIRAC import gLogger

# sut
from DIRAC.ResourceStatusSystem.Agent.SummarizeLogsAgent import SummarizeLogsAgent

# mocks
rsClientMock = MagicMock()

# useful stuff
sOk = {"OK": True, "Value": []}
sError = {"OK": False, "Message": "a mess"}


columns = ["ID", "Name", "StatusType", "Status", "ElementType", "TokenOwner"]
el101 = [101, "RRCKI-BUFFER", "WriteAccess", "Active", "StorageElement", "rs_svc"]
el102 = [102, "RRCKI-BUFFER", "WriteAccess", "Active", "StorageElement", "rs_svc"]
el103 = [103, "RRCKI-BUFFER", "ReadAccess", "Active", "StorageElement", "rs_svc"]
el104 = [104, "RRCKI-BUFFER", "ReadAccess", "Active", "StorageElement", "rs_svc"]
el105 = [105, "RRCKI-BUFFER", "WriteAccess", "Banned", "StorageElement", "rs_svc"]

response1 = {"OK": True, "Value": [el101], "Columns": columns}

response2 = {"OK": True, "Value": [el101, el102], "Columns": columns}

response3 = {"OK": True, "Value": [el101, el102, el103], "Columns": columns}

response4 = {"OK": True, "Value": [el101, el102, el103, el104], "Columns": columns}

response4_mixed = {"OK": True, "Value": [el102, el103, el104, el101], "Columns": columns}

response5 = {"OK": True, "Value": [el101, el102, el103, el104, el105], "Columns": columns}

mockAM = MagicMock()


@pytest.fixture()
def sla(mocker):
    mocker.patch("DIRAC.ResourceStatusSystem.Agent.SummarizeLogsAgent.AgentModule", new=mockAM)
    theSLA = SummarizeLogsAgent()
    theSLA.log = gLogger
    theSLA.log.setLevel("DEBUG")
    theSLA.am_getOption = mockAM
    theSLA.rsClient = rsClientMock
    return theSLA


@pytest.mark.parametrize(
    "rsClientMockSelectStatusElementReturnValue, expected, expectedValue",
    [
        (sError, False, None),
        (sOk, True, (None, {})),
        ({"OK": True, "Value": [], "Columns": []}, True, (None, {})),
        (response1, True, (101, {("RRCKI-BUFFER", "WriteAccess"): [dict(zip(columns, el101))]})),
        (response2, True, (102, {("RRCKI-BUFFER", "WriteAccess"): [dict(zip(columns, el101))]})),
        (
            response3,
            True,
            (
                103,
                {
                    ("RRCKI-BUFFER", "WriteAccess"): [dict(zip(columns, el101))],
                    ("RRCKI-BUFFER", "ReadAccess"): [dict(zip(columns, el103))],
                },
            ),
        ),
        (
            response4,
            True,
            (
                104,
                {
                    ("RRCKI-BUFFER", "WriteAccess"): [dict(zip(columns, el101))],
                    ("RRCKI-BUFFER", "ReadAccess"): [dict(zip(columns, el103))],
                },
            ),
        ),
        (
            response4_mixed,
            True,
            (
                101,
                {
                    ("RRCKI-BUFFER", "WriteAccess"): [dict(zip(columns, el102))],
                    ("RRCKI-BUFFER", "ReadAccess"): [dict(zip(columns, el103))],
                },
            ),
        ),
        (
            response5,
            True,
            (
                105,
                {
                    ("RRCKI-BUFFER", "WriteAccess"): [dict(zip(columns, el101)), dict(zip(columns, el105))],
                    ("RRCKI-BUFFER", "ReadAccess"): [dict(zip(columns, el103))],
                },
            ),
        ),
    ],
)
def test__summarizeLogs(rsClientMockSelectStatusElementReturnValue, expected, expectedValue, sla):
    rsClientMock.selectStatusElement.return_value = rsClientMockSelectStatusElementReturnValue
    res = sla._summarizeLogs("element")
    assert res["OK"] == expected
    assert res.get("Value") == expectedValue


@pytest.mark.parametrize(
    "key, logs, rsClientMockSelectStatusElementReturnValue, expected",
    [
        ((None, None), [], sError, True),
        ((None, None), ["bof"], sError, False),
        ((None, None), [], sOk, True),
        ((None, None), [], {"OK": True, "Value": [], "Columns": ["Status", "TokenOwner"]}, True),
        ((None, None), [], {"OK": True, "Value": [["Active", "rs_svc"]], "Columns": ["status", "tokenowner"]}, True),
        (
            ("RRCKI-BUFFER", "WriteAccess"),  # this should remove everything
            [dict(zip(columns, el101))],
            {"OK": True, "Value": [["Active", "rs_svc"]], "Columns": ["status", "tokenowner"]},
            True,
        ),
        (
            ("RRCKI-BUFFER", "WriteAccess"),  # this should keep it
            [dict(zip(columns, el101))],
            {"OK": True, "Value": [["Banned", "rs_svc"]], "Columns": ["status", "tokenowner"]},
            True,
        ),
    ],
)
def test__registerLogs(key, logs, rsClientMockSelectStatusElementReturnValue, expected, sla):
    rsClientMock.selectStatusElement.return_value = rsClientMockSelectStatusElementReturnValue
    res = sla._registerLogs("Resource", key, logs)
    assert res["OK"] == expected
