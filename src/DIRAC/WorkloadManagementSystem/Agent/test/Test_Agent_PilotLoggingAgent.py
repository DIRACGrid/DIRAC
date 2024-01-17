""" Test class for PilotLoggingAgent Agent
"""
import os
import time
import tempfile

import pytest
from unittest.mock import MagicMock, patch

# DIRAC Components
import DIRAC.WorkloadManagementSystem.Agent.PilotLoggingAgent as plaModule
from DIRAC.WorkloadManagementSystem.Agent.PilotLoggingAgent import PilotLoggingAgent
from DIRAC import gLogger, gConfig, S_OK, S_ERROR

gLogger.setLevel("DEBUG")

# Mock Objects
mockReply = MagicMock()
mockReply1 = MagicMock()
mockGetDNForUsername = MagicMock()
mockGetVomsAttr = MagicMock()
mockGetProxy = MagicMock()
mockOperations = MagicMock()
mockTornadoClient = MagicMock()
mockDataManager = MagicMock()
mockAM = MagicMock()
mockNone = MagicMock()
mockNone.return_value = None

upDict = {
    "OK": True,
    "Value": {"User": "proxyUser", "Group": "proxyGroup"},
}


@pytest.fixture
def plaBase(mocker):
    mocker.patch("DIRAC.WorkloadManagementSystem.Agent.PilotLoggingAgent.AgentModule.__init__")
    mocker.patch(
        "DIRAC.WorkloadManagementSystem.Agent.JobCleaningAgent.AgentModule._AgentModule__moduleProperties",
        side_effect=lambda x, y=None: y,
        create=True,
    )
    mocker.patch("DIRAC.WorkloadManagementSystem.Agent.PilotLoggingAgent.AgentModule.am_getOption", return_value=mockAM)
    mocker.patch(
        "DIRAC.WorkloadManagementSystem.Agent.PilotLoggingAgent.getVOs",
        return_value={"OK": True, "Value": ["gridpp", "lz"]},
    )
    mocker.patch("DIRAC.WorkloadManagementSystem.Agent.PilotLoggingAgent.Operations.getValue", side_effect=mockReply)
    mocker.patch(
        "DIRAC.WorkloadManagementSystem.Agent.PilotLoggingAgent.Operations.getOptionsDict", side_effect=mockReply1
    )
    mocker.patch(
        "DIRAC.WorkloadManagementSystem.Agent.PilotLoggingAgent.getDNForUsername", side_effect=mockGetDNForUsername
    )
    mocker.patch("DIRAC.WorkloadManagementSystem.Agent.PilotLoggingAgent.getProxy", side_effect=mockGetProxy)
    pla = PilotLoggingAgent()
    pla.log = gLogger
    pla._AgentModule__configDefaults = mockAM
    return pla


@pytest.fixture
def pla(mocker, plaBase):
    mocker.patch(
        "DIRAC.WorkloadManagementSystem.Agent.PilotLoggingAgent.TornadoPilotLoggingClient",
        side_effect=mockTornadoClient,
    )
    mocker.patch("DIRAC.WorkloadManagementSystem.Agent.PilotLoggingAgent.Operations", side_effect=mockOperations)
    mocker.patch(
        "DIRAC.WorkloadManagementSystem.Agent.PilotLoggingAgent.DataManager",
        side_effect=mockDataManager,
    )
    return plaBase


@pytest.mark.parametrize(
    "remoteLogging, options, getDN, getVOMS, getProxy, resDict, expectedRes",
    [
        (
            [True, False],
            upDict,
            S_OK(["myDN"]),
            S_OK(),
            S_OK("proxyfilename"),
            {"gridpp": {"DN": ["myDN"], "group": "proxyGroup", "proxy": "proxyfilename"}},
            S_OK(),
        ),
        ([False, False], upDict, S_OK(["myDN"]), S_OK(), S_OK(), {}, S_OK()),
        ([True, False], upDict, S_ERROR("Could not obtain a DN"), S_OK(), S_OK(), {}, S_OK()),
        ([True, False], upDict, S_ERROR("Could not download proxy"), S_OK(), S_ERROR("Failure"), {}, S_OK()),
    ],
)
def test_initialize(plaBase, remoteLogging, options, getDN, getVOMS, getProxy, resDict, expectedRes):
    """
    After a successful initialisation the proxyDict should contain proxy filenames key by a VO name.
    test loops: gridpp enabled, lz disabled, proxy obtained, result proxyDict contains a proxy filename.
            both VOs disabled => proxyDict empty
            gridpp enabled, by getDNForUsername fails => proxyDict empty
            gridpp enabled, lz disabled, getProxy fails => proxyDict empty

    """
    mockReply.side_effect = remoteLogging  # Operations.getValue("/Pilot/RemoteLogging", False)
    mockReply1.return_value = options  # Operations.getOptionsDict("Shifter/DataManager")
    mockGetDNForUsername.return_value = getDN
    mockGetVomsAttr.return_value = getVOMS
    mockGetProxy.return_value = getProxy

    res = plaBase.initialize()

    assert plaBase.voList == plaModule.getVOs()["Value"]
    assert resDict == plaBase.proxyDict
    assert res == expectedRes


@pytest.mark.parametrize(
    "proxyDict, execVORes, expectedResult",
    [
        ({}, S_OK(), S_OK()),
        ({"gridpp": {"DN": ["myDN"], "group": "proxyGroup", "proxy": "proxyfilename"}}, S_OK(), S_OK()),
        (
            {"gridpp": {"DN": ["myDN"], "group": "proxyGroup", "proxy": "proxyfilename"}},
            S_ERROR("Execute for VO failed"),
            S_ERROR("Agent cycle for some VO finished with errors"),
        ),
    ],
)
def test_execute(plaBase, proxyDict, execVORes, expectedResult):
    """Testing a thin version of execute (executeForVO is mocked)"""

    plaBase.executeForVO = MagicMock()
    plaBase._isProxyExpired = MagicMock()
    plaBase._isProxyExpired.return_value = False
    plaBase.proxyDict = proxyDict
    plaBase.executeForVO.return_value = execVORes
    res = plaBase.execute()
    assert res["OK"] == expectedResult["OK"]


@pytest.mark.parametrize(
    "ppath, files, result",
    [
        ("pilot/log/path/", ["file1.log", "file2.log", "file3.log"], S_OK()),
        ("pilot/log/path/", [], S_OK()),
    ],
)
def test_executeForVO(pla, ppath, files, result):
    opsHelperValues = {"OK": True, "Value": {"UploadSE": "testUploadSE", "UploadPath": "/gridpp/uploadPath"}}
    # full local temporary path:
    filepath = os.path.join(tempfile.TemporaryDirectory().name, ppath)
    # this is what getMetadata returns:
    resDict = {"OK": True, "Value": {"LogPath": filepath}}
    mockTornadoClient.return_value.getMetadata.return_value = resDict
    mockDataManager.return_value.putAndRegister.return_value = result
    if files:
        os.makedirs(os.path.join(filepath, "gridpp"), exist_ok=True)
    for elem in files:
        open(os.path.join(filepath, "gridpp", elem), "w")
    mockOperations.return_value.getOptionsDict.return_value = opsHelperValues
    pla.opsHelper = mockOperations.return_value
    # success route
    res = pla.executeForVO(vo="gridpp")
    mockTornadoClient.assert_called_with(useCertificates=True)
    assert mockTornadoClient.return_value.getMetadata.called
    # only called with a non-empty file list:
    if files:
        assert mockDataManager.return_value.putAndRegister.called
    assert res == S_OK()


def test_executeForVOMetaFails(pla):
    opsHelperValues = {"OK": True, "Value": {"UploadSE": "testUploadSE", "UploadPath": "/gridpp/uploadPath"}}
    mockOperations.return_value.getOptionsDict.return_value = opsHelperValues
    pla.opsHelper = mockOperations.return_value
    # getMetadata call fails.
    mockTornadoClient.return_value.getMetadata.return_value = {"OK": False, "Message": "Failed, sorry.."}
    res = pla.executeForVO(vo="anything")
    assert res["OK"] is False


@pytest.mark.parametrize(
    "opsHelperValues, expectedRes",
    [
        ({"OK": True, "Value": {"UploadPath": "/gridpp/uploadPath"}}, S_ERROR("Upload SE not defined")),
        ({"OK": True, "Value": {"UploadSE": "testUploadSE"}}, S_ERROR("Upload path on SE testUploadSE not defined")),
        ({"OK": False}, S_ERROR(f"No pilot section for gridpp vo")),
    ],
)
def test_executeForVOBadConfig(pla, opsHelperValues, expectedRes):
    """Testing an incomplete configuration"""
    mockOperations.return_value.getOptionsDict.return_value = opsHelperValues
    pla.opsHelper = mockOperations.return_value
    res = pla.executeForVO(vo="gridpp")
    assert res["OK"] is False
    assert res["Message"] == expectedRes["Message"]
    mockTornadoClient.return_value.getMetadata.reset_mock()
    mockTornadoClient.return_value.getMetadata.assert_not_called()


@pytest.mark.parametrize(
    "filename, fileAge, ageLimit, expectedResult", [("survives.log", 10, 20, True), ("getsdeleted.log", 21, 20, False)]
)
def test_oldLogsCleaner(plaBase, filename, fileAge, ageLimit, expectedResult):
    """Testing old files removal"""
    plaBase.clearPilotsDelay = ageLimit
    filepath = tempfile.TemporaryDirectory().name
    os.makedirs(filepath, exist_ok=True)
    testfile = os.path.join(filepath, filename)
    fd = open(testfile, "w")
    fd.close()
    assert os.path.exists(testfile) is True
    # cannot patch os.stat globally because os.path.exists uses it !
    with patch("DIRAC.WorkloadManagementSystem.Agent.PilotLoggingAgent.os.stat") as mockOSStat:
        mockOSStat.return_value.st_mtime = time.time() - fileAge * 86400  # file older that fileAge in seconds
        plaBase.clearOldPilotLogs(filepath)
    assert os.path.exists(testfile) is expectedResult
