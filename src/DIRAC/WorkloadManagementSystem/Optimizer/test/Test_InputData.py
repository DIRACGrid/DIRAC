import pytest
from mock import MagicMock
from DIRAC.WorkloadManagementSystem.Client.JobState.CachedJobState import CachedJobState
from DIRAC.WorkloadManagementSystem.Client.JobState.JobManifest import JobManifest
from DIRAC.WorkloadManagementSystem.Optimizer.InputDataResolver import InputDataResolver

mockNone = MagicMock()
mockNone.return_value = None


@pytest.mark.parametrize(
    "manifestOptions, expected",
    [
        ({}, []),
        ({"InputSandbox": "SB:ProductionSandboxSE|/SandBox/l/lhcb_mc/4c7.bof"}, []),
        ({"InputSandbox": "LFN:/l/lhcb_mc/4c7.bof"}, ["/l/lhcb_mc/4c7.bof"]),
        (
            {"InputSandbox": ["SB:ProductionSandboxSE|/SandBox/l/lhcb_mc/4c7.bof", "LFN:/l/lhcb_mc/4c7.bof"]},
            ["/l/lhcb_mc/4c7.bof"],
        ),
        ({"InputSandbox": ["SB:ProductionSandboxSE|/SandBox/l/lhcb_mc/4c7.bof", "bif:/l/lhcb_mc/4c7.bof"]}, []),
        (
            {"InputSandbox": ["LFN:/l/lhcb_mc/4c8.bof", "LFN:/l/lhcb_mc/4c7.bof"]},
            ["/l/lhcb_mc/4c8.bof", "/l/lhcb_mc/4c7.bof"],
        ),
    ],
)
def test__getInputSandbox(mocker, manifestOptions, expected):
    # Arrange
    mocker.patch("DIRAC.WorkloadManagementSystem.Client.JobState.JobState.JobDB.__init__", side_effect=mockNone)
    mocker.patch("DIRAC.WorkloadManagementSystem.Client.JobState.JobState.JobLoggingDB.__init__", side_effect=mockNone)
    mocker.patch("DIRAC.WorkloadManagementSystem.Client.JobState.JobState.TaskQueueDB.__init__", side_effect=mockNone)

    js = CachedJobState(1)
    js.setAttribute("JobType", "User")

    manifest = JobManifest()
    for varName, varValue in manifestOptions.items():
        manifest.setOption(varName, varValue)

    js.setManifest(manifest)

    # Act
    res = InputDataResolver(js)._getInputSandbox()

    # Assert
    assert res["OK"] is True
    assert res["Value"] == expected
