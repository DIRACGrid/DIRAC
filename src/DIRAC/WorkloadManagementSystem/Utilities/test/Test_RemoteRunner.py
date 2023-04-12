""" Test class for Job Agent
"""

# imports
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
import pytest
import os
from diraccfg import CFG

# DIRAC Components
from DIRAC import gLogger, gConfig, S_OK
from DIRAC.ConfigurationSystem.Client.ConfigurationData import gConfigurationData
from DIRAC.Core.Security.X509Chain import X509Chain  # pylint: disable=import-error
from DIRAC.WorkloadManagementSystem.Utilities.RemoteRunner import RemoteRunner

gLogger.setLevel("DEBUG")


@pytest.mark.parametrize(
    "command, workingDirectory, expectedContent",
    [
        ("/path/to/script.sh", "/path/to", "./script.sh"),
        ("/path/to/script.sh", "/another/path/to", "/path/to/script.sh"),
        ("/path/to/script.sh arg1", "/path/to", "./script.sh arg1"),
        ("/path/to/script.sh /path/to/arg1", "/path/to", "./script.sh ./arg1"),
        ("/path/to/script.sh /anotherpath/to/arg1", "/path/to", "./script.sh /anotherpath/to/arg1"),
        ("/path/to/script.sh /another/path/to/arg1", "/path/to", "./script.sh /another/path/to/arg1"),
        ("./script.sh", ".", "./script.sh"),
        ("ls", "/path/to", "ls"),
        ("echo 'Hello World'", "/path/to", "echo 'Hello World'"),
        (
            "lb-prod-run prodConf_Gauss_12345_12345.json --verbose",
            ".",
            "lb-prod-run prodConf_Gauss_12345_12345.json --verbose",
        ),
    ],
)
def test__wrapCommand(command, workingDirectory, expectedContent):
    """Test RemoteRunner()._wrapCommand()"""
    executable = "workloadExec.sh"

    # Instantiate a RemoteRunner and wrap the command
    remoteRunner = RemoteRunner("Site1", "CE1", "queue1")
    remoteRunner._wrapCommand(command, workingDirectory, executable)

    # Test the results
    assert os.path.isfile(executable)
    with open(executable, "r") as f:
        content = f.read()
    os.remove(executable)
    assert content == expectedContent


@pytest.mark.parametrize(
    "payloadNumberOfProcessors, ceNumberOfProcessors, expectedResult, expectedNumberOfProcessors",
    [
        # CE has more processors than the payload requests
        (1, 1, True, 1),
        (2, 2, True, 2),
        (1, 2, True, 1),
        # CE has less processors than the payload requests
        (2, 1, False, "Not enough processors to execute the command"),
        # Specific case: we should not have 0
        (0, 1, False, "Inappropriate NumberOfProcessors value"),
        (1, 0, False, "Inappropriate NumberOfProcessors value"),
        (-4, 1, False, "Inappropriate NumberOfProcessors value"),
        (1, -4, False, "Inappropriate NumberOfProcessors value"),
        (0, 0, False, "Inappropriate NumberOfProcessors value"),
    ],
)
def test__setUpWorkloadCE(
    mocker, payloadNumberOfProcessors, ceNumberOfProcessors, expectedResult, expectedNumberOfProcessors
):
    """Test RemoteRunner()._setUpWorkloadCE()"""
    mocker.patch(
        "DIRAC.WorkloadManagementSystem.Utilities.RemoteRunner.getProxyInfo", return_value=S_OK({"chain": X509Chain()})
    )
    mocker.patch("DIRAC.Core.Security.X509Chain.X509Chain.getRemainingSecs", return_value=S_OK(1000))

    # Configure the CS with the number of available processors in the CE
    siteName = "DIRAC.Site1.site"
    ceName = "CE1"
    queueName = "queue1"

    config = {"Resources": {"Sites": {"DIRAC": {siteName: {"CEs": {ceName: {}}}}}}}
    ceConfig = config["Resources"]["Sites"]["DIRAC"][siteName]["CEs"][ceName]
    ceConfig["CEType"] = "HTCondorCE"
    ceConfig["Queues"] = {}
    ceConfig["Queues"][queueName] = {}
    ceConfig["Queues"][queueName]["NumberOfProcessors"] = ceNumberOfProcessors

    # Load the configuration
    gConfigurationData.localCFG = CFG()
    cfg = CFG()
    cfg.loadFromDict(config)
    gConfig.loadCFG(cfg)

    # Instantiate a RemoteRunner and set up the CE
    remoteRunner = RemoteRunner(siteName, ceName, queueName)
    result = remoteRunner._setUpWorkloadCE(payloadNumberOfProcessors)

    # Test the results
    assert result["OK"] == expectedResult
    if expectedResult:
        workloadCE = result["Value"]
        assert workloadCE.ceParameters["NumberOfProcessors"] == expectedNumberOfProcessors
    else:
        assert result["Message"] == expectedNumberOfProcessors
