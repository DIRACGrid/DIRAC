""" Test class for HTCondorResourceUsage utility
"""

import pytest

from DIRAC import S_OK
from DIRAC.Resources.Computing.BatchSystems.TimeLeft.HTCondorResourceUsage import HTCondorResourceUsage


HTCONDOR_OUT_0 = "86400 3600"
HTCONDOR_OUT_1 = "undefined 3600"
HTCONDOR_OUT_2 = ""


def test_getResourceUsage(mocker):
    runCommandMock = mocker.patch(
        "DIRAC.Resources.Computing.BatchSystems.TimeLeft.HTCondorResourceUsage.runCommand",
        side_effect=[S_OK(HTCONDOR_OUT_0), S_OK(HTCONDOR_OUT_1), S_OK(HTCONDOR_OUT_2)],
    )

    # First test: everything is fine
    htcondorResourceUsage = HTCondorResourceUsage("1234", {"Queue": "Test", "InfoPath": "/path/to/condor_ad"})
    res = htcondorResourceUsage.getResourceUsage()
    assert res["OK"], res["Message"]
    assert res["Value"]["WallClock"] == 3600
    assert res["Value"]["WallClockLimit"] == 86400

    # SKIP_LOCAL_CONFIG_FILE must be passed through the environment (runCommand runs without a shell,
    # so a "VAR=value cmd" prefix would not work), and it must not wipe the rest of the environment.
    _, kwargs = runCommandMock.call_args
    assert kwargs["env"]["SKIP_LOCAL_CONFIG_FILE"] == "true"
    assert "PATH" in kwargs["env"]

    # Second test: MaxRuntime is undefined
    htcondorResourceUsage = HTCondorResourceUsage("1234", {"Queue": "Test", "InfoPath": "/path/to/condor_ad"})
    res = htcondorResourceUsage.getResourceUsage()
    assert not res["OK"]
    assert res["Message"] == "Current batch system is not supported"

    # Third test: empty output
    htcondorResourceUsage = HTCondorResourceUsage("1234", {"Queue": "Test", "InfoPath": "/path/to/condor_ad"})
    res = htcondorResourceUsage.getResourceUsage()
    assert not res["OK"]
    assert res["Message"] == "Current batch system is not supported"
