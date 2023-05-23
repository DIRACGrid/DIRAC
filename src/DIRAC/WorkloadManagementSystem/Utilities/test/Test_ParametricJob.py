""" This is a test of the parametric job generation tools
"""
# pylint: disable= missing-docstring, invalid-name

import pytest

from DIRAC.Core.Utilities.ClassAd.ClassAdLight import ClassAd
from DIRAC.WorkloadManagementSystem.Utilities.ParametricJob import (
    checkIfParametricJobIsCorrect,
    generateParametricJobs,
    putDefaultNameOnNamelessParameterSequence,
    transformParametricJobIntoParsableOne,
)

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

    for i, jobDescription in enumerate(jobDescList):
        jobDescription = ClassAd(jobDescription)
        assert jobDescription.getAttributeInt("ParameterNumber") == i
        assert jobDescription.getAttributeString("JobName") == f"Test_{i}"
        assert jobDescription.getAttributeString("Arguments") == expectedArguments[i]


@pytest.mark.parametrize(
    "jdl, expectedResult",
    [
        (TEST_JDL_NO_PARAMETERS, True),
        (TEST_JDL_SIMPLE, True),
        (TEST_JDL_SIMPLE_BUNCH, True),
        (TEST_JDL_SIMPLE_PROGRESSION, True),
        (TEST_JDL_MULTI, True),
        (
            """
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
            """,
            False,
        ),
    ],
)
def test_checkIfParametricJobIsCorrect(jdl: str, expectedResult: bool):
    # Arrange
    jobDescription = transformParametricJobIntoParsableOne(ClassAd(jdl))

    # Act
    res = checkIfParametricJobIsCorrect(jobDescription)

    # Assert
    if expectedResult:
        assert res["OK"], res["Message"]
    else:
        assert not res["OK"]


@pytest.mark.parametrize(
    "jdl, expected",
    [
        (TEST_JDL_NO_PARAMETERS, TEST_JDL_NO_PARAMETERS),
        (
            TEST_JDL_SIMPLE,
            """
            [
                Executable = "my_executable";
                Arguments = "%(A)s";
                JobName = "Test_%n";
                Parameters = 3;
                Parameters.A = { "a","b","c" };
            ]
            """,
        ),
        (
            TEST_JDL_SIMPLE_BUNCH,
            """
            [
                Executable = "my_executable";
                Arguments = "%(A)s";
                JobName = "Test_%n";
                Parameters = 3;
                ParameterStart.A = 5
            ]
            """,
        ),
        (
            TEST_JDL_SIMPLE_PROGRESSION,
            """
            [
                Executable = "my_executable";
                Arguments = "%(A)s";
                JobName = "Test_%n";
                Parameters = 3;
                ParameterStart.A = 1;
                ParameterStep.A = 1;
                ParameterFactor.A = 2;
            ]
            """,
        ),
        (TEST_JDL_MULTI, TEST_JDL_MULTI),
    ],
)
def test_putDefaultNameOnNamelessParameterSequence(jdl, expected):
    assert putDefaultNameOnNamelessParameterSequence(ClassAd(jdl)).asJDL() == ClassAd(expected).asJDL()


@pytest.mark.parametrize(
    "jdl, expected",
    [
        (TEST_JDL_NO_PARAMETERS, TEST_JDL_NO_PARAMETERS),
        (
            TEST_JDL_SIMPLE,
            """
            [
                Executable = "my_executable";
                Arguments = "%(A)s";
                JobName = "Test_%n";
                Parameters = 3;
                Parameters.A = {"a", "b", "c"};
            ]
            """,
        ),
        (
            TEST_JDL_SIMPLE_BUNCH,
            """
            [
                Executable = "my_executable";
                Arguments = "%(A)s";
                JobName = "Test_%n";
                Parameters = 3;
                Parameters.A = {5, 5, 5}
            ]
            """,
        ),
        (
            TEST_JDL_SIMPLE_PROGRESSION,
            """
            [
                Executable = "my_executable";
                Arguments = "%(A)s";
                JobName = "Test_%n";
                Parameters = 3;
                Parameters.A = {1, 3, 7};
            ]
            """,
        ),
        (
            TEST_JDL_MULTI,
            """
            [
                Executable = "my_executable";
                Arguments = "%(A)s %(B)s";
                JobName = "Test_%n";
                Parameters = 3;
                Parameters.A = {1, 3, 7};
                Parameters.B = {"a", "b", "c"};
            ]
            """,
        ),
    ],
)
def test_transformParametricJobIntoParsableOne(jdl, expected):
    assert transformParametricJobIntoParsableOne(ClassAd(jdl)).asJDL() == ClassAd(expected).asJDL()
