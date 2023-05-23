# pylint: disable=protected-access,missing-docstring, invalid-name

from unittest.mock import patch
import pytest


from DIRAC import S_OK
from DIRAC.Core.Utilities.ClassAd.ClassAdLight import ClassAd
from DIRAC.Core.Utilities.JDL import loadJDLasJob
from DIRAC.Interfaces.API.Job import Job

# Most partial JDL possible
MOST_PARTIAL_JDL = """
[
    Executable = "/bin/ls";
]
"""

# Most complete non-parametric jdl possible to parse
# (we suppose that the StdOutput and StdError have already been parsed)
MOST_COMPLETE_JDL = """
[
    Arguments = "-ltr";
    BannedSites = "LCG.KEK.jp";
    CPUTime = 3600;
    Executable = "/bin/ls";
    ExecutionEnvironment =
        {
            MYVAR1=3,
            MYVAR2=test
        };
    GridCE = "CE.LAPP.fr";
    InputData = "LFN:/vo/production/DC04/v2/DST/00000742_00003493_10.dst";
    InputDataPolicy = "Download";
    InputSandbox = "LFN:/vo.formation.idgrilles.fr/user/v/vhamar/test.txt";
    JobGroup = "1234abcd";
    JobName = "LFNInputSandbox";
    LogLevel = "DEBUG";
    MaxNumberOfProcessors = 4;
    MinNumberOfProcessors = 2;
    NumberOfProcessors = 3;
    OutputData = "StdOut";
    OutputSE = "M3PEC-disk";
    OutputPath = "/myjobs/output";
    OutputSandbox =
        {
            "some-file.txt"
        };
    Platform = "x86_64-slc6-gcc62-opt";
    Priority = 10;
    Site = "LCG.LAPP.fr";
    Tags = MultiProcessor;
]
"""


@pytest.mark.parametrize(
    "jdl, expectedJDL",
    [
        (
            # Simple job
            MOST_PARTIAL_JDL,
            """
            [
                Arguments = "jobDescription.xml -o LogLevel=INFO";
                Executable = "dirac-jobexec";
                InputSandbox =
                    {
                        /bin/ls,
                        jobDescription.xml
                    };
                JobName = Name;
                JobType = User;
                LogLevel = INFO;
                OutputSandbox =
                    {
                        Script1_ls.log,
                        std.err,
                        std.out,
                        std.err,
                        std.out
                    };
                Priority = 1;
                StdError = std.err;
                StdOutput = std.out;
            ]
            """,
        ),
        (
            MOST_COMPLETE_JDL,
            """
            [
                Arguments = "jobDescription.xml -o LogLevel=DEBUG";
                BannedSites = LCG.KEK.jp;
                CPUTime = 3600;
                Executable = "dirac-jobexec";
                ExecutionEnvironment =
                    {
                        MYVAR1=3,
                        MYVAR2=test
                    };
                InputData = LFN:/vo/production/DC04/v2/DST/00000742_00003493_10.dst;
                InputDataPolicy = DIRAC.WorkloadManagementSystem.Client.DownloadInputData;
                InputSandbox =
                    {
                        LFN:/vo.formation.idgrilles.fr/user/v/vhamar/test.txt,
                        /bin/ls,
                        jobDescription.xml
                    };
                GridCE = CE.LAPP.fr;
                JobGroup = 1234abcd;
                JobName = LFNInputSandbox;
                JobType = User;
                LogLevel = DEBUG;
                MaxNumberOfProcessors = 3;
                NumberOfProcessors = 3;
                OutputData = StdOut;
                OutputSE = M3PEC-disk;
                OutputPath = myjobs/output;
                OutputSandbox =
                    {
                        some-file.txt,
                        Script1_ls.log,
                        std.err,
                        std.out
                    };
                Platform = x86_64-slc6-gcc62-opt;
                Priority = 10;
                Site = LCG.LAPP.fr;
                StdError = std.err;
                StdOutput = std.out;
                Tags = MultiProcessor;
            ]
            """,
        ),
    ],
)
def test_loadJDLasJob(jdl: str, expectedJDL: str):
    # Arrange
    with patch(
        "DIRAC.ConfigurationSystem.Client.Helpers.Operations.Operations.getValue",
        return_value="DIRAC.WorkloadManagementSystem.Client.DownloadInputData",
    ):
        with patch("DIRAC.Interfaces.API.Job.getCESiteMapping", return_value=S_OK({"CE.LAPP.fr": "LCG.LAPP.fr"})):
            with patch("DIRAC.Interfaces.API.Job.getDIRACPlatforms", return_value=S_OK("x86_64-slc6-gcc62-opt")):
                with patch("DIRAC.Core.Base.API.getSites", return_value=S_OK(["LCG.LAPP.fr"])):
                    job = Job()

                    # Act
                    res = loadJDLasJob(job, ClassAd(jdl))

    # Assert
    assert res["OK"], res["Message"]
    job = res["Value"]
    print(ClassAd(f"[{job._toJDL()}]").asJDL())
    assert ClassAd(f"[{job._toJDL()}]").asJDL() == ClassAd(expectedJDL).asJDL()
