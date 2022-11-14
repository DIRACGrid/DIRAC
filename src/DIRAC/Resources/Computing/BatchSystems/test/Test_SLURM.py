""" Test SLURM
"""
import pytest
import os
from six.moves import reload_module

from DIRAC import S_OK, gLogger
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
# On node 1

 line1
 line2
 line3
"""


srunExpected2 = """
# On node 3

 line1
 line2
 line3
"""


srunExpected3 = """
# On node 2

 line1
 line2
 line3
"""


srunExpected = [srunExpected1, srunExpected2, srunExpected3]


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
