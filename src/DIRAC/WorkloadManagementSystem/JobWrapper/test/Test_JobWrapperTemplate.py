""" JobWrapper test

Here we test the creation of a job wrapper and make sure it can be executed without crashing.
We don't test the actual execution of the wrapper or its payload.
"""
import json
import os
import re
import shutil
import sys

from diraccfg import CFG
import pytest

from DIRAC.WorkloadManagementSystem.Utilities.Utils import createJobWrapper
import subprocess


jobParams = {
    "JobID": "1",
    "JobType": "Merge",
    "JobGroup": "Group1",
    "JobName": "TestJob",
    "CPUTime": "1000000",
    "Executable": "dirac-jobexec",
    "Arguments": "helloWorld.xml -o LogLevel=DEBUG",
    "InputSandbox": ["helloWorld.xml", "exe-script.py"],
}
resourceParams = {
    "MaxTotalJobs": 4300,
    "MaxWaitingJobs": 200,
    "Site": "LCG.CERN.cern",
    "CEType": "AREX",
    "architecture": "x86_64",
    "OS": "linux_CentOS_7.9.2009",
    "SI00": 2775,
    "MaxRAM": 128534,
    "CPUTime": 3836159,
    "maxCPUTime": 5760,
    "Queue": "nordugrid-SLURM-grid",
    "GridCE": "ce1.cern.ch",
    "RequiredTag": [],
    "DIRACVersion": "9.0.0a20",
    "NumberOfProcessors": None,
}
optimizerParams = {
    "JobID": 1,
    "OptimizerChain": "JobPath,JobSanity,JobScheduling",
    "matchTime": 0.15743756294250488,
    "CEDict": {},
}
payloadParams = {
    "command": "dirac-jobexec helloworld.xml -o LogLevel=DEBUG",
    "env": {},
}


@pytest.fixture
def extraOptions():
    """Create a temporary cfg file"""
    extraOptions = "test.cfg"

    localCfg = CFG()
    localCfg.createNewSection("DIRAC")

    localCfg.createNewSection("/Systems")
    localCfg.createNewSection("/Systems/WorkloadManagement")
    localCfg.createNewSection("/Systems/WorkloadManagement/JobWrapper")

    localCfg.writeToFile(extraOptions)
    yield extraOptions
    os.remove(extraOptions)


@pytest.mark.slow
def test_createAndExecuteJobWrapperTemplate_success(extraOptions):
    """Test the creation of a classical job wrapper and its execution:
    There is an extra option cfg file to be passed to the job wrapper.
    """
    # Create job wrapper
    res = createJobWrapper(
        jobID=1,
        jobParams=jobParams,
        resourceParams=resourceParams,
        optimizerParams=optimizerParams,
        extraOptions=extraOptions,
    )
    assert res["OK"], res.get("Message")

    # Test job wrapper content
    jobWrapperPath = res["Value"]["JobWrapperPath"]
    assert jobWrapperPath
    assert os.path.exists(jobWrapperPath)

    with open(jobWrapperPath) as f:
        jobWrapperContent = f.read()

    assert "@SITEPYTHON@" not in jobWrapperContent
    assert f'os.path.realpath("{os.getcwd()}")' in jobWrapperContent

    # Test job wrapper configuration path
    jobWrapperConfigPath = res["Value"]["JobWrapperConfigPath"]
    assert jobWrapperConfigPath
    assert os.path.exists(jobWrapperConfigPath)

    with open(jobWrapperConfigPath) as f:
        jobWrapperConfigContent = json.load(f)

    assert jobWrapperConfigContent["Job"] == jobParams
    assert jobWrapperConfigContent["CE"] == resourceParams
    assert jobWrapperConfigContent["Optimizer"] == optimizerParams
    assert "Payload" not in jobWrapperConfigContent

    # Test job executable path
    jobExecutablePath = res["Value"]["JobExecutablePath"]
    assert jobExecutablePath
    assert os.path.exists(jobExecutablePath)

    with open(jobExecutablePath) as f:
        jobExecutableContent = f.read()

    assert os.path.realpath(sys.executable) in jobExecutableContent
    assert jobWrapperPath in jobExecutableContent
    assert extraOptions in jobExecutableContent
    assert "-o LogLevel=INFO" in jobExecutableContent
    assert "-o /DIRAC/Security/UseServerCertificate=no" in jobExecutableContent

    # Test job executable relocated path
    assert not res["Value"].get("JobExecutableRelocatedPath")

    # Execute wrapperFile in a subprocess
    os.chmod(jobExecutablePath, 0o755)
    result = subprocess.run(jobExecutablePath, shell=True, capture_output=True)

    assert result.returncode == 1, result.stderr
    assert b"Starting Job Wrapper Initialization for Job 1" in result.stdout, result.stdout
    assert b"Downloading InputSandbox for job 1: helloWorld.xml, exe-script.py" in result.stdout, result.stdout
    assert b"Job Wrapper is starting the pre processing phase for job" in result.stdout, result.stdout
    assert b"Job Wrapper is starting the processing phase for job" in result.stdout, result.stdout
    assert b"Final job status Failed" in result.stdout, result.stdout
    assert result.stderr == b"", result.stderr

    # This is the default wrapper path
    assert os.path.exists(os.path.join(os.getcwd(), "job/Wrapper"))
    shutil.rmtree(os.path.join(os.getcwd(), "job"))


@pytest.mark.slow
def test_createAndExecuteJobWrapperTemplate_missingExtraOptions():
    """Test the creation of a classical job wrapper and its execution:
    There is no extra options to be passed to the job wrapper.

    This might happen when the pilot.cfg does not contain any extra options.
    """
    # Create job wrapper
    res = createJobWrapper(jobID=1, jobParams=jobParams, resourceParams=resourceParams, optimizerParams=optimizerParams)
    assert res["OK"], res.get("Message")

    # Test job wrapper content
    jobWrapperPath = res["Value"]["JobWrapperPath"]
    assert jobWrapperPath
    assert os.path.exists(jobWrapperPath)

    with open(jobWrapperPath) as f:
        jobWrapperContent = f.read()

    assert "@SITEPYTHON@" not in jobWrapperContent
    assert f'os.path.realpath("{os.getcwd()}")' in jobWrapperContent

    # Test job wrapper configuration path
    jobWrapperConfigPath = res["Value"]["JobWrapperConfigPath"]
    assert jobWrapperConfigPath
    assert os.path.exists(jobWrapperConfigPath)

    with open(jobWrapperConfigPath) as f:
        jobWrapperConfigContent = json.load(f)

    assert jobWrapperConfigContent["Job"] == jobParams
    assert jobWrapperConfigContent["CE"] == resourceParams
    assert jobWrapperConfigContent["Optimizer"] == optimizerParams
    assert "Payload" not in jobWrapperConfigContent

    # Test job executable path
    jobExecutablePath = res["Value"]["JobExecutablePath"]
    assert jobExecutablePath
    assert os.path.exists(jobExecutablePath)

    with open(jobExecutablePath) as f:
        jobExecutableContent = f.read()

    assert os.path.realpath(sys.executable) in jobExecutableContent
    assert jobWrapperPath in jobExecutableContent
    assert "-o LogLevel=INFO" in jobExecutableContent
    assert "-o /DIRAC/Security/UseServerCertificate=no" in jobExecutableContent

    # Test job executable relocated path
    assert not res["Value"].get("JobExecutableRelocatedPath")

    # Execute wrapperFile in a subprocess
    os.chmod(jobExecutablePath, 0o755)
    result = subprocess.run(jobExecutablePath, shell=True, capture_output=True)
    assert result.returncode == 1, result.stderr
    assert result.stderr == b"", result.stderr

    # This is the default wrapper path
    assert os.path.exists(os.path.join(os.getcwd(), "job/Wrapper"))
    shutil.rmtree(os.path.join(os.getcwd(), "job"))


@pytest.mark.slow
def test_createAndExecuteRelocatedJobWrapperTemplate_success(extraOptions):
    """Test the creation of a relocated job wrapper and its execution:
    This is generally used when containers are involved (SingularityCE).
    """
    # Create a specific wrapper path
    wrapperPath = os.path.join(os.getcwd(), "DIRAC_containers/job")
    os.makedirs(wrapperPath, exist_ok=True)
    # Working directory within the container
    rootLocation = os.path.join(os.getcwd(), "tmp/Wrapper")
    os.makedirs(rootLocation, exist_ok=True)

    # Create relocated job wrapper
    res = createJobWrapper(
        jobID=1,
        jobParams=jobParams,
        resourceParams=resourceParams,
        optimizerParams=optimizerParams,
        # This is the interesting part
        pythonPath="python",
        wrapperPath=wrapperPath,
        rootLocation=rootLocation,
        extraOptions=extraOptions,
    )
    assert res["OK"], res.get("Message")

    # Test job wrapper content
    jobWrapperPath = res["Value"]["JobWrapperPath"]
    assert jobWrapperPath
    assert os.path.exists(jobWrapperPath)
    assert os.path.exists(os.path.join(wrapperPath, os.path.basename(jobWrapperPath)))
    assert not os.path.exists(os.path.join(rootLocation, os.path.basename(jobWrapperPath)))

    with open(jobWrapperPath) as f:
        jobWrapperContent = f.read()

    assert "@SITEPYTHON@" not in jobWrapperContent
    assert f'os.path.realpath("{rootLocation}")' in jobWrapperContent

    # Test job wrapper configuration path
    jobWrapperConfigPath = res["Value"]["JobWrapperConfigPath"]
    assert jobWrapperConfigPath
    assert os.path.exists(jobWrapperConfigPath)
    assert os.path.exists(os.path.join(wrapperPath, os.path.basename(jobWrapperConfigPath)))
    assert not os.path.exists(os.path.join(rootLocation, os.path.basename(jobWrapperConfigPath)))

    with open(jobWrapperConfigPath) as f:
        jobWrapperConfigContent = json.load(f)

    assert jobWrapperConfigContent["Job"] == jobParams
    assert jobWrapperConfigContent["CE"] == resourceParams
    assert jobWrapperConfigContent["Optimizer"] == optimizerParams
    assert "Payload" not in jobWrapperConfigContent

    # Test job executable path
    jobExecutablePath = res["Value"]["JobExecutablePath"]
    assert jobExecutablePath
    assert os.path.exists(jobExecutablePath)
    assert os.path.exists(os.path.join(wrapperPath, os.path.basename(jobExecutablePath)))
    assert not os.path.exists(os.path.join(rootLocation, os.path.basename(jobExecutablePath)))

    with open(jobExecutablePath) as f:
        jobExecutableContent = f.read()

    assert os.path.realpath(sys.executable) not in jobExecutableContent
    assert "python" in jobExecutableContent

    assert jobWrapperPath not in jobExecutableContent
    assert os.path.join(rootLocation) in jobExecutableContent
    assert extraOptions in jobExecutableContent
    assert "-o LogLevel=INFO" in jobExecutableContent
    assert "-o /DIRAC/Security/UseServerCertificate=no" in jobExecutableContent

    # Test job executable relocated path
    jobExecutableRelocatedPath = res["Value"]["JobExecutableRelocatedPath"]
    assert jobExecutableRelocatedPath
    assert jobExecutablePath != jobExecutableRelocatedPath
    assert os.path.basename(jobExecutablePath) == os.path.basename(jobExecutableRelocatedPath)
    assert not os.path.exists(jobExecutableRelocatedPath)

    # 1. Execute the executable file in a subprocess without relocating the files as a container bind mount would do
    # We expect it to fail because the job wrapper is not in the expected location
    os.chmod(jobExecutablePath, 0o755)
    result = subprocess.run(jobExecutablePath, shell=True, capture_output=True)

    assert result.returncode == 2, result.stderr
    assert result.stdout == b"", result.stdout
    assert (
        f"can't open file '{os.path.join(rootLocation, os.path.basename(jobWrapperPath))}'".encode() in result.stderr
    ), result.stderr

    # 2. Execute the relocated executable file in a subprocess without relocating the files as a container bind mount would do
    # We expect it to fail because the relocated executable should not exist
    os.chmod(jobExecutablePath, 0o755)
    result = subprocess.run(jobExecutableRelocatedPath, shell=True, capture_output=True)

    assert result.returncode == 127, result.stderr
    assert result.stdout == b"", result.stdout
    assert re.search(rf"{jobExecutableRelocatedPath}: (No such file or directory|not found)", result.stderr.decode())

    # 3. Now we relocate the files as a container bind mount would do and execute the relocated executable file in a subprocess
    # We expect it to work
    shutil.copy(jobWrapperPath, rootLocation)
    shutil.copy(jobWrapperConfigPath, rootLocation)
    shutil.copy(jobExecutablePath, rootLocation)
    os.chmod(jobExecutablePath, 0o755)

    result = subprocess.run(jobExecutableRelocatedPath, shell=True, capture_output=True)

    assert result.returncode == 1, result.stderr
    assert b"Starting Job Wrapper Initialization for Job 1" in result.stdout, result.stdout
    assert b"Downloading InputSandbox for job 1: helloWorld.xml, exe-script.py" in result.stdout, result.stdout
    assert b"Job Wrapper is starting the pre processing phase for job" in result.stdout, result.stdout
    assert b"Job Wrapper is starting the processing phase for job" in result.stdout, result.stdout
    assert b"Final job status Failed" in result.stdout, result.stdout
    assert result.stderr == b"", result.stderr

    shutil.rmtree(rootLocation)
    shutil.rmtree(wrapperPath)


@pytest.mark.slow
def test_createAndExecuteJobWrapperOfflineTemplate_success(extraOptions):
    """Test the creation of an offline job wrapper and its execution:
    This is generally used when pre/post processing operations are executed locally,
    while the workflow itself is executed on a remote computing resource (PushJobAgent).
    """
    # Working directory on the remote resource
    rootLocation = "."
    numberOfFiles = len(os.listdir(rootLocation))

    # Create relocated job wrapper
    res = createJobWrapper(
        jobID=1,
        jobParams=jobParams,
        resourceParams=resourceParams,
        optimizerParams=optimizerParams,
        # This is the interesting part
        defaultWrapperLocation="DIRAC/WorkloadManagementSystem/JobWrapper/JobWrapperOfflineTemplate.py",
        pythonPath="python",
        rootLocation=rootLocation,
        extraOptions=extraOptions,
    )
    assert res["OK"], res.get("Message")

    # Test job wrapper content
    jobWrapperPath = res["Value"].get("JobWrapperPath")
    assert jobWrapperPath
    assert os.path.exists(jobWrapperPath)
    assert not os.path.exists(os.path.join(rootLocation, os.path.basename(jobWrapperPath)))

    with open(jobWrapperPath) as f:
        jobWrapperContent = f.read()

    assert "@SITEPYTHON@" not in jobWrapperContent
    assert 'os.path.realpath(".")' in jobWrapperContent

    # Test job wrapper configuration path
    jobWrapperConfigPath = res["Value"].get("JobWrapperConfigPath")
    assert jobWrapperConfigPath
    assert os.path.exists(jobWrapperConfigPath)
    assert not os.path.exists(os.path.join(rootLocation, os.path.basename(jobWrapperConfigPath)))

    with open(jobWrapperConfigPath) as f:
        jobWrapperConfigContent = json.load(f)

    assert jobWrapperConfigContent["Job"] == jobParams
    assert jobWrapperConfigContent["CE"] == resourceParams
    assert jobWrapperConfigContent["Optimizer"] == optimizerParams
    assert "Payload" not in jobWrapperConfigContent

    # Test job executable path
    jobExecutablePath = res["Value"].get("JobExecutablePath")
    assert jobExecutablePath
    assert os.path.exists(jobExecutablePath)
    assert not os.path.exists(os.path.join(rootLocation, os.path.basename(jobExecutablePath)))

    with open(jobExecutablePath) as f:
        jobExecutableContent = f.read()

    assert os.path.realpath(sys.executable) not in jobExecutableContent
    assert "python" in jobExecutableContent

    assert jobWrapperPath not in jobExecutableContent
    assert os.path.join(rootLocation, os.path.basename(jobWrapperPath)) in jobExecutableContent
    assert extraOptions in jobExecutableContent
    assert "-o LogLevel=INFO" in jobExecutableContent
    assert "-o /DIRAC/Security/UseServerCertificate=no" in jobExecutableContent

    # Test job executable relocated path
    jobExecutableRelocatedPath = res["Value"].get("JobExecutableRelocatedPath")
    assert jobExecutableRelocatedPath
    assert jobExecutablePath != jobExecutableRelocatedPath
    assert os.path.basename(jobExecutablePath) == os.path.basename(jobExecutableRelocatedPath)
    assert not os.path.exists(jobExecutableRelocatedPath)

    # 1. Execute the executable file in a subprocess without relocating the files as if they were on the remote resource
    # We expect it to fail because the job wrapper is not in the expected location
    os.chmod(jobExecutablePath, 0o755)
    result = subprocess.run(jobExecutablePath, shell=True, capture_output=True)

    assert result.returncode == 2, result.stderr
    assert result.stdout == b"", result.stdout
    assert b"can't open file" in result.stderr, result.stderr

    # 2. Execute the relocated executable file in a subprocess without relocating the files as they would be on the remote resource
    # We expect it to fail because the relocated executable should not exist
    os.chmod(jobExecutablePath, 0o755)
    result = subprocess.run(jobExecutableRelocatedPath, shell=True, capture_output=True)

    assert result.returncode == 127, result.stderr
    assert result.stdout == b"", result.stdout
    assert re.search(rf"{jobExecutableRelocatedPath}: (No such file or directory|not found)", result.stderr.decode())

    # 3. Now we relocate the files as if they were on a remote resource and execute the relocated executable file in a subprocess
    # We expect it to fail because the payload parameters are not available
    shutil.copy(jobWrapperPath, rootLocation)
    shutil.copy(jobWrapperConfigPath, rootLocation)
    shutil.copy(jobExecutablePath, rootLocation)
    os.chmod(jobExecutablePath, 0o755)

    result = subprocess.run(jobExecutableRelocatedPath, shell=True, capture_output=True)

    assert result.returncode == 1, result.stderr
    assert b"JobID is not defined, running in current directory" not in result.stdout, result.stdout
    assert result.stderr == b"", result.stderr

    # 4. We recreate the job wrapper offline template with the payload params now
    # We did not specify where the results and checksum should be stored, so we expect it to fail
    res = createJobWrapper(
        jobID=1,
        jobParams=jobParams,
        resourceParams=resourceParams,
        optimizerParams=optimizerParams,
        # This is the interesting part
        defaultWrapperLocation="DIRAC/WorkloadManagementSystem/JobWrapper/JobWrapperOfflineTemplate.py",
        pythonPath="python",
        rootLocation=rootLocation,
        extraOptions=extraOptions,
        payloadParams=payloadParams,
    )
    assert res["OK"], res.get("Message")
    jobWrapperPath = res["Value"].get("JobWrapperPath")
    jobWrapperConfigPath = res["Value"].get("JobWrapperConfigPath")
    jobExecutablePath = res["Value"].get("JobExecutablePath")

    shutil.copy(jobWrapperPath, rootLocation)
    shutil.copy(jobWrapperConfigPath, rootLocation)
    shutil.copy(jobExecutablePath, rootLocation)
    os.chmod(jobExecutablePath, 0o755)

    result = subprocess.run(jobExecutableRelocatedPath, shell=True, capture_output=True)

    assert result.returncode == 1, result.stderr
    assert b"JobID is not defined, running in current directory" not in result.stdout, result.stdout
    assert result.stderr == b"", result.stderr

    # The root location should contain:
    # - the job wrapper
    # - the job wrapper configuration
    # - the job executable
    # - the job/Wrapper directory
    print(os.listdir(rootLocation))
    assert len(os.listdir(rootLocation)) == numberOfFiles + 4

    # 5. We recreate the job wrapper offline template with the payload params and the additional job params
    # It should work fine now
    jobParams["PayloadResults"] = "payloadResults.json"
    jobParams["Checksum"] = "checksum.json"

    res = createJobWrapper(
        jobID=1,
        jobParams=jobParams,
        resourceParams=resourceParams,
        optimizerParams=optimizerParams,
        # This is the interesting part
        defaultWrapperLocation="DIRAC/WorkloadManagementSystem/JobWrapper/JobWrapperOfflineTemplate.py",
        pythonPath="python",
        rootLocation=rootLocation,
        extraOptions=extraOptions,
        payloadParams=payloadParams,
    )
    assert res["OK"], res.get("Message")
    jobWrapperPath = res["Value"].get("JobWrapperPath")
    jobWrapperConfigPath = res["Value"].get("JobWrapperConfigPath")
    jobExecutablePath = res["Value"].get("JobExecutablePath")

    shutil.copy(jobWrapperPath, rootLocation)
    shutil.copy(jobWrapperConfigPath, rootLocation)
    shutil.copy(jobExecutablePath, rootLocation)
    os.chmod(jobExecutablePath, 0o755)

    result = subprocess.run(jobExecutableRelocatedPath, shell=True, capture_output=True)

    assert result.returncode == 0, result.stderr
    assert b"JobID is not defined, running in current directory" in result.stdout, result.stdout
    assert b"Job Wrapper is starting the processing phase for job" in result.stdout, result.stdout
    assert result.stderr == b"", result.stderr

    # The root location should contain:
    # - the job wrapper
    # - the job wrapper configuration
    # - the job executable
    # - the job/Wrapper directory
    assert len(os.listdir(rootLocation)) == numberOfFiles + 8
    assert os.path.exists(os.path.join(rootLocation, "payloadResults.json"))
    assert os.path.exists(os.path.join(rootLocation, "checksum.json"))

    with open(os.path.join(rootLocation, "payloadResults.json")) as f:
        payloadResults = json.load(f)

    assert payloadResults["OK"]
    assert "cpuTimeConsumed" in payloadResults["Value"]
    assert "payloadExecutorError" in payloadResults["Value"]
    assert "payloadOutput" in payloadResults["Value"]
    assert "payloadStatus" in payloadResults["Value"]

    with open(os.path.join(rootLocation, "checksum.json")) as f:
        checksums = json.load(f)

    assert jobParams["PayloadResults"] in checksums

    os.unlink(os.path.join(rootLocation, os.path.basename(jobWrapperPath)))
    os.unlink(os.path.join(rootLocation, os.path.basename(jobWrapperConfigPath)))
    os.unlink(os.path.join(rootLocation, os.path.basename(jobExecutablePath)))
    os.unlink(os.path.join(rootLocation, "payloadResults.json"))
    os.unlink(os.path.join(rootLocation, "checksum.json"))
    os.unlink(os.path.join(rootLocation, "std.err"))
    os.unlink(os.path.join(rootLocation, "job.info"))
    shutil.rmtree(os.path.join(os.getcwd(), "job"))
