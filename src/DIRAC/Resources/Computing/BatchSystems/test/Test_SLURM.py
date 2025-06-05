""" Test SLURM
"""
import os

import pytest

from DIRAC import gLogger
from DIRAC.Resources.Computing.BatchSystems.SLURM import SLURM

gLogger.setLevel("DEBUG")

executableContent = """
#!/bin/bash

echo "hello world from $HOME"
"""

expectedContent = r"""#!/bin/bash
cat > srunExec_1.sh << EOFEXEC

#!/bin/bash

echo "hello world from \$HOME"

EOFEXEC
chmod 755 srunExec_1.sh
srun -l -k srunExec_1.sh
"""

srunOutput = """
1: line1
1: line2
2: line1
1: line3
3: line1
2: line2
3: line2
2: line3
3: line3

"""


srunExpected1 = """
# Node 1

 line1
 line2
 line3
"""


srunExpected2 = """
# Node 3

 line1
 line2
 line3
"""


srunExpected3 = """
# Node 2

 line1
 line2
 line3
"""


srunExpected = [srunExpected1, srunExpected2, srunExpected3]


killedSrunOutput = """
1: line1
1: line2
2: line1
1: line3
2: line2
srun: Job step aborted: Waiting up to 32 seconds for job step to finish.
slurmstepd: error: *** JOB 9999 ON hpc CANCELLED DUE TO TIME LIMIT ***
1: slurmstepd: error: *** STEP 9999 ON hpc CANCELLED DUE TO TIME LIMIT ***

"""


killedSrunExpected1 = """
# Node 1

 line1
 line2
 line3
 slurmstepd: error: *** STEP 9999 ON hpc CANCELLED DUE TO TIME LIMIT ***
"""


killedSrunExpected2 = """
# Node 2

 line1
 line2
"""

killedSrunExpectedGI = """
# General Information

srun: Job step aborted: Waiting up to 32 seconds for job step to finish.
slurmstepd: error: *** JOB 9999 ON hpc CANCELLED DUE TO TIME LIMIT ***
"""


killedSrunExpected = [killedSrunExpected1, killedSrunExpected2, killedSrunExpectedGI]


normalOutput = """
line1
line2
line3
"""


normalExpected = """
line1
line2
line3
"""


@pytest.mark.parametrize(
    "expectedContent",
    [
        (expectedContent),
    ],
)
def test_generateWrapper(mocker, expectedContent):
    """Test generateWrapper()"""
    mocker.patch("DIRAC.Resources.Computing.BatchSystems.SLURM.random.randrange", return_value=1)
    slurm = SLURM()

    executableFile = "executableFile.sh"
    with open(executableFile, "w") as f:
        f.write(executableContent)

    slurm._generateSrunWrapper(executableFile)

    with open(executableFile) as f:
        res = f.read()

    # Make sure a wrapper file has been generated
    assert res == expectedContent

    os.remove(executableFile)


@pytest.mark.parametrize(
    "numberOfNodes, outputContent, expectedContent",
    [
        ("3-5", srunOutput, srunExpected),
        ("3-5", killedSrunOutput, killedSrunExpected),
        ("1", normalOutput, normalExpected),
    ],
)
def test_getJobOutputFiles(numberOfNodes, outputContent, expectedContent):
    """Test getJobOutputFiles()"""
    slurm = SLURM()

    # We remove the '\n' at the beginning/end of the file because there are not present in reality
    outputContent = outputContent.strip()
    # We only remove the '\n' at the beginning because processOutput adds a '\n' at the end
    expectedContent = [i.lstrip() for i in expectedContent]

    outputFile = "./1234.out"
    with open(outputFile, "w") as f:
        f.write(outputContent)

    errorFile = "./1234.err"
    with open(errorFile, "w") as f:
        f.write(outputContent)

    batchDict = {
        "JobIDList": ["1234"],
        "OutputDir": ".",
        "ErrorDir": ".",
        "NumberOfNodes": numberOfNodes,
    }
    result = slurm.getJobOutputFiles(**batchDict)
    assert result["Status"] == 0

    output = result["Jobs"]["1234"]["Output"]
    error = result["Jobs"]["1234"]["Error"]
    assert output == outputFile
    assert error == errorFile

    with open(outputFile) as f:
        wrapperContent = f.read()
    for srunLines in expectedContent:
        assert srunLines in wrapperContent

    os.remove(outputFile)
    os.remove(errorFile)


def test_submitJob_cmd_generation(mocker):
    """Test submitJob() command string generation for various kwargs"""
    slurm = SLURM()
    # Mock subprocess.Popen to capture the command
    popen_mock = mocker.patch("subprocess.Popen")
    process_mock = popen_mock.return_value
    process_mock.communicate.return_value = ("Submitted batch job 1234\n", "")
    process_mock.returncode = 0

    # Minimal kwargs
    kwargs = {
        "Executable": "/bin/echo",
        "OutputDir": "/tmp",
        "ErrorDir": "/tmp",
        "Queue": "testq",
        "SubmitOptions": "",
        "JobStamps": ["stamp1"],
        "NJobs": 1,
    }
    # Test default (WholeNode False)
    slurm.submitJob(**kwargs)
    cmd = popen_mock.call_args[0][0]
    assert "--cpus-per-task=1" in cmd
    assert "--exclusive" not in cmd

    # Test WholeNode True disables --cpus-per-task and adds --exclusive
    kwargs["WholeNode"] = True
    slurm.submitJob(**kwargs)
    cmd = popen_mock.call_args[0][0]
    assert "--exclusive" in cmd
    assert "--cpus-per-task" not in cmd

    # Test NumberOfProcessors
    kwargs["WholeNode"] = False
    kwargs["NumberOfProcessors"] = 8
    slurm.submitJob(**kwargs)
    cmd = popen_mock.call_args[0][0]
    assert "--cpus-per-task=8" in cmd

    # Test NumberOfGPUs
    kwargs["NumberOfGPUs"] = 2
    slurm.submitJob(**kwargs)
    cmd = popen_mock.call_args[0][0]
    assert "--gpus-per-task=2" in cmd
