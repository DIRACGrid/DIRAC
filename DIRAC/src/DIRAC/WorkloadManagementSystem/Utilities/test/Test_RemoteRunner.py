""" Test class for Job Agent
"""

# imports
import pytest
import os
from diraccfg import CFG

# DIRAC Components
from DIRAC import gLogger, gConfig, S_OK, S_ERROR
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
    remoteRunner._wrapCommand(command, workingDirectory)

    # Test the results
    assert os.path.isfile(remoteRunner.executable)
    with open(remoteRunner.executable) as f:
        content = f.read()
    os.remove(remoteRunner.executable)

    # This line is added at the end of the wrapper for any command
    expectedContent += f"\nmd5sum * > {remoteRunner.checkSumOutput}"
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


@pytest.mark.parametrize(
    "checkSumDict, expectedResult",
    [
        # Normal case
        ({"file1.txt": "826e8142e6baabe8af779f5f490cf5f5", "file2.txt": "1c1c96fd2cf8330db0bfa936ce82f3b9"}, S_OK()),
        # Files are corrupted
        (
            {"file1.txt": "c12f72e7b198fdbfe5f70c66dc6082c8", "file2.txt": "5ec149e38f09fb716b1e0f4cf23af679"},
            S_ERROR("./file1.txt is corrupted"),
        ),
        (
            {"file1.txt": "826e8142e6baabe8af779f5f490cf5f5", "file2.txt": "5ec149e38f09fb716b1e0f4cf23af679"},
            S_ERROR("./file2.txt is corrupted"),
        ),
        # Files do not exist
        (
            {
                "file3.txt": "826e8142e6baabe8af779f5f490cf5f5",
            },
            S_ERROR("./file3.txt was expected but not found"),
        ),
        # remoteRunner.checkSumOutput is empty
        ({}, S_OK()),
        # remoteRunner.checkSumOutput does not exist
        (None, S_ERROR("Cannot guarantee the integrity of the outputs")),
    ],
)
def test__checkOutputIntegrity(checkSumDict, expectedResult):
    """Test RemoteRunner()._checkOutputIntegrity()"""
    # Instantiate a RemoteRunner
    remoteRunner = RemoteRunner("Site1", "CE1", "queue1")

    # Create some files in workingDirectory
    workingDirectory = "."
    with open(os.path.join(workingDirectory, "file1.txt"), "w") as f:
        f.write("file1")
    with open(os.path.join(workingDirectory, "file2.txt"), "w") as f:
        f.write("file2")

    # Create remoteRunner.checkSumOutput
    if checkSumDict is not None:
        with open(os.path.join(workingDirectory, remoteRunner.checkSumOutput), "w") as f:
            for file, checkSum in checkSumDict.items():
                f.write(f"{checkSum}  {file}\n")

    # Check the integrity of the output
    result = remoteRunner._checkOutputIntegrity(".")

    # Test the results
    print(result)
    assert result["OK"] is expectedResult["OK"]
    if not expectedResult["OK"]:
        assert expectedResult["Message"] in result["Message"]

    # Delete files
    os.remove(os.path.join(workingDirectory, "file1.txt"))
    os.remove(os.path.join(workingDirectory, "file2.txt"))
    if os.path.exists(os.path.join(workingDirectory, remoteRunner.checkSumOutput)):
        os.remove(remoteRunner.checkSumOutput)
