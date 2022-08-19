""" Test class for SiteDirector
"""
# pylint: disable=protected-access

# imports
import datetime
import pytest
from unittest.mock import MagicMock

from DIRAC import gLogger

# sut
from DIRAC.WorkloadManagementSystem.Agent.SiteDirector import SiteDirector

mockAM = MagicMock()
mockGCReply = MagicMock()
mockGCReply.return_value = "TestSetup"
mockOPSObject = MagicMock()
mockOPSObject.getValue.return_value = "123"
mockOPSReply = MagicMock()
mockOPSReply.return_value = "123"

mockOPS = MagicMock()
mockOPS.return_value = mockOPSObject
# mockOPS.Operations = mockOPSObject
mockPM = MagicMock()
mockPM.requestToken.return_value = {"OK": True, "Value": ("token", 1)}
mockPMReply = MagicMock()
mockPMReply.return_value = {"OK": True, "Value": ("token", 1)}

mockCSGlobalReply = MagicMock()
mockCSGlobalReply.return_value = "TestSetup"
mockResourcesReply = MagicMock()
mockResourcesReply.return_value = {"OK": True, "Value": ["x86_64-slc6", "x86_64-slc5"]}

mockPilotAgentsDB = MagicMock()
mockPilotAgentsDB.setPilotStatus.return_value = {"OK": True}

gLogger.setLevel("DEBUG")


@pytest.fixture
def sd(mocker):
    """mocker for SiteDirector"""
    mocker.patch("DIRAC.WorkloadManagementSystem.Agent.SiteDirector.AgentModule.__init__")
    mocker.patch("DIRAC.WorkloadManagementSystem.Agent.SiteDirector.gConfig.getValue", side_effect=mockGCReply)
    mocker.patch("DIRAC.WorkloadManagementSystem.Agent.SiteDirector.Operations", side_effect=mockOPS)
    mocker.patch(
        "DIRAC.WorkloadManagementSystem.Agent.SiteDirector.gProxyManager.requestToken", side_effect=mockPMReply
    )
    mocker.patch("DIRAC.WorkloadManagementSystem.Agent.SiteDirector.AgentModule", side_effect=mockAM)
    sd = SiteDirector()
    sd.log = gLogger
    sd.am_getOption = mockAM
    sd.log.setLevel("DEBUG")
    sd.rpcMatcher = MagicMock()
    sd.rssClient = MagicMock()
    sd.workingDirectory = ""
    sd.queueDict = {
        "aQueue": {
            "Site": "LCG.CERN.cern",
            "CEName": "aCE",
            "CEType": "SSH",
            "QueueName": "aQueue",
            "ParametersDict": {
                "CPUTime": 12345,
                "Community": "lhcb",
                "OwnerGroup": ["lhcb_user"],
                "Setup": "LHCb-Production",
                "Site": "LCG.CERN.cern",
            },
        }
    }
    return sd


def test__getPilotOptions(sd):
    """Testing SiteDirector()._getPilotOptions()"""
    res = sd._getPilotOptions("aQueue")
    assert {"-S TestSetup", "-V 123", "-l 123", "-n LCG.CERN.cern"} <= set(res)


@pytest.mark.parametrize(
    "mockMatcherReturnValue, expected, anyExpected, sitesExpected",
    [
        ({"OK": False, "Message": "boh"}, False, True, set()),
        ({"OK": True, "Value": None}, False, True, set()),
        ({"OK": True, "Value": {"1": {"Jobs": 10}, "2": {"Jobs": 20}}}, True, True, set()),
        ({"OK": True, "Value": {"1": {"Jobs": 10, "Sites": ["Site1"]}, "2": {"Jobs": 20}}}, True, True, {"Site1"}),
        (
            {"OK": True, "Value": {"1": {"Jobs": 10, "Sites": ["Site1", "Site2"]}, "2": {"Jobs": 20}}},
            True,
            True,
            {"Site1", "Site2"},
        ),
        (
            {
                "OK": True,
                "Value": {"1": {"Jobs": 10, "Sites": ["Site1", "Site2"]}, "2": {"Jobs": 20, "Sites": ["Site1"]}},
            },
            True,
            False,
            {"Site1", "Site2"},
        ),
        (
            {
                "OK": True,
                "Value": {"1": {"Jobs": 10, "Sites": ["Site1", "Site2"]}, "2": {"Jobs": 20, "Sites": ["ANY"]}},
            },
            True,
            False,
            {"Site1", "Site2", "ANY"},
        ),
        (
            {
                "OK": True,
                "Value": {"1": {"Jobs": 10, "Sites": ["Site1", "Site2"]}, "2": {"Jobs": 20, "Sites": ["ANY", "Site3"]}},
            },
            True,
            False,
            {"Site1", "Site2", "Site3", "ANY"},
        ),
        (
            {
                "OK": True,
                "Value": {"1": {"Jobs": 10, "Sites": ["Site1", "Site2"]}, "2": {"Jobs": 20, "Sites": ["Any", "Site3"]}},
            },
            True,
            False,
            {"Site1", "Site2", "Site3", "Any"},
        ),
        (
            {
                "OK": True,
                "Value": {
                    "1": {"Jobs": 10, "Sites": ["Site1", "Site2"]},
                    "2": {"Jobs": 20, "Sites": ["NotAny", "Site2"]},
                },
            },
            True,
            False,
            {"Site1", "Site2", "NotAny"},
        ),
    ],
)
def test__ifAndWhereToSubmit(sd, mockMatcherReturnValue, expected, anyExpected, sitesExpected):
    """Testing SiteDirector()._ifAndWhereToSubmit()"""
    sd.matcherClient = MagicMock()
    sd.matcherClient.getMatchingTaskQueues.return_value = mockMatcherReturnValue
    res = sd._ifAndWhereToSubmit()
    assert res[0] == expected
    if res[0]:
        assert res == (expected, anyExpected, sitesExpected, set())


def test__allowedToSubmit(sd):
    """Testing SiteDirector()._allowedToSubmit()"""
    submit = sd._allowedToSubmit("aQueue", True, {"LCG.CERN.cern"}, set())
    assert submit is False

    sd.siteMaskList = ["LCG.CERN.cern", "DIRAC.CNAF.it"]
    submit = sd._allowedToSubmit("aQueue", True, {"LCG.CERN.cern"}, set())
    assert submit is True

    sd.rssFlag = True
    submit = sd._allowedToSubmit("aQueue", True, {"LCG.CERN.cern"}, set())
    assert submit is False

    sd.ceMaskList = ["aCE", "anotherCE"]
    submit = sd._allowedToSubmit("aQueue", True, {"LCG.CERN.cern"}, set())
    assert submit is True


def test__submitPilotsToQueue(sd):
    """Testing SiteDirector()._submitPilotsToQueue()"""
    # Create a MagicMock that does not have the workingDirectory
    # attribute (https://cpython-test-docs.readthedocs.io/en/latest/library/unittest.mock.html#deleting-attributes)
    # This is to use the SiteDirector's working directory, not the CE one
    ceMock = MagicMock()
    del ceMock.workingDirectory

    sd.queueCECache = {"aQueue": {"CE": ceMock}}
    sd.queueSlots = {"aQueue": {"AvailableSlots": 10}}
    assert sd._submitPilotsToQueue(1, MagicMock(), "aQueue")["OK"]


@pytest.mark.parametrize(
    "pilotRefs, pilotDict, pilotCEDict, expected",
    [
        ([], {}, {}, (0, [])),
        (
            ["aPilotRef"],
            {"aPilotRef": {"Status": "Running", "LastUpdateTime": datetime.datetime(2000, 1, 1).utcnow()}},
            {},
            (0, []),
        ),
        (
            ["aPilotRef"],
            {"aPilotRef": {"Status": "Running", "LastUpdateTime": datetime.datetime(2000, 1, 1).utcnow()}},
            {"aPilotRef": "Running"},
            (0, []),
        ),
        (
            ["aPilotRef"],
            {"aPilotRef": {"Status": "Running", "LastUpdateTime": datetime.datetime(2000, 1, 1).utcnow()}},
            {"aPilotRef": "Unknown"},
            (0, []),
        ),
    ],
)
def test__updatePilotStatus(sd, pilotRefs, pilotDict, pilotCEDict, expected):
    """Testing SiteDirector()._updatePilotStatus()"""
    res = sd._updatePilotStatus(pilotRefs, pilotDict, pilotCEDict)
    assert res == expected
