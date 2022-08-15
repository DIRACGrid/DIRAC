""" This is a test of the parametric job generation tools
"""
# pylint: disable= missing-docstring

import pytest

from DIRAC.WorkloadManagementSystem.Utilities.ParametricJob import generateParametricJobs, getParameterVectorLength
from DIRAC.Core.Utilities.ClassAd.ClassAdLight import ClassAd

TEST_JDL_NO_PARAMETERS = """
[
    Executable = "my_executable";
    Arguments = "%s";
    JobName = "Test_%n";
]
"""

TEST_JDL_SIMPLE = """
[
    Executable = "my_executable";
    Arguments = "%s";
    JobName = "Test_%n";
    Parameters = { "a", "b", "c" }
]
"""

TEST_JDL_SIMPLE_BUNCH = """
[
    Executable = "my_executable";
    Arguments = "%s";
    JobName = "Test_%n";
    Parameters = 3;
    ParameterStart = 5;
]
"""

TEST_JDL_SIMPLE_PROGRESSION = """
[
    Executable = "my_executable";
    Arguments = "%s";
    JobName = "Test_%n";
    Parameters = 3;
    ParameterStart = 1;
    ParameterStep = 1;
    ParameterFactor = 2;
]
"""

TEST_JDL_MULTI = """
[
    Executable = "my_executable";
    Arguments = "%(A)s %(B)s";
    JobName = "Test_%n";
    Parameters = 3;
    ParameterStart.A = 1;
    ParameterStep.A = 1;
    ParameterFactor.A = 2;
    Parameters.B = { "a","b","c" };
]
"""

TEST_JDL_MULTI_BAD = """
[
    Executable = "my_executable";
    Arguments = "%(A)s %(B)s";
    JobName = "Test_%n";
    Parameters = 3;
    ParameterStart.A = 1;
    ParameterStep.A = 1;
    ParameterFactor.A = 2;
    Parameters.B = { "a","b","c","d" };
]
"""


@pytest.mark.parametrize(
    "jdl, expectedArguments",
    [
        (TEST_JDL_SIMPLE, ["a", "b", "c"]),
        (TEST_JDL_SIMPLE_BUNCH, ["5", "5", "5"]),
        (TEST_JDL_SIMPLE_PROGRESSION, ["1", "3", "7"]),
        (TEST_JDL_MULTI, ["1 a", "3 b", "7 c"]),
        (TEST_JDL_NO_PARAMETERS, []),
    ],
)
def test_getParameterVectorLength_successful(jdl: str, expectedArguments: list[str]):
    # Arrange
    jobDescription = ClassAd(jdl)

    # Act
    result = getParameterVectorLength(jobDescription)

    # Assert
    assert result["OK"], result["Message"]
    if expectedArguments:
        assert result["Value"] == len(expectedArguments)
    else:
        assert result["Value"] == None


@pytest.mark.parametrize("jdl", [TEST_JDL_MULTI_BAD])
def test_getParameterVectorLength_unsuccessful(jdl: str):
    # Arrange
    jobDescription = ClassAd(jdl)

    # Act
    result = getParameterVectorLength(jobDescription)

    # Assert
    assert not result["OK"], result["Value"]


@pytest.mark.parametrize(
    "jdl, expectedArguments",
    [
        (TEST_JDL_SIMPLE, ["a", "b", "c"]),
        (TEST_JDL_SIMPLE_BUNCH, ["5", "5", "5"]),
        (TEST_JDL_SIMPLE_PROGRESSION, ["1", "3", "7"]),
        (TEST_JDL_MULTI, ["1 a", "3 b", "7 c"]),
    ],
)
def test_generateParametricJobs(jdl: str, expectedArguments: list[str]):
    # Arrange
    parametricJobDescription = ClassAd(jdl)

    # Act
    result = generateParametricJobs(parametricJobDescription)

    # Assert
    assert result["OK"], result["Message"]
    assert result["Value"]
    jobDescList = result["Value"]
    assert len(jobDescList) == len(expectedArguments)

    for i in range(len(jobDescList)):
        jobDescription = ClassAd(jobDescList[i])
        assert jobDescription.getAttributeString("JobName") == f"Test_{i}"
        assert jobDescription.getAttributeString("Arguments") == expectedArguments[i]
