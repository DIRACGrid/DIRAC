import pytest

from DIRAC.Core.Utilities.ClassAd import ClassAd


@pytest.mark.parametrize(
    "jdl, expected",
    [
        ("", True),
        ("[]", True),
        ('[Executable="dirac-jobexec";]', False),
        ('[   Executable    =    "dirac-jobexec";   ]', False),
    ],
)
def test_analyse_jdl(jdl, expected):

    # Act
    jobDescription = ClassAd(jdl)

    # Assert
    assert jobDescription.isEmpty() is expected


def test_insertAttributeInt():

    # Arrange
    value = 1
    jobDescription = ClassAd("")

    # Act
    jobDescription.insertAttributeInt("Test", value)

    # Assert
    assert jobDescription.isEmpty() is False
    assert jobDescription.lookupAttribute("Test") is True
    assert jobDescription.getAttributeInt("Test") == value
    assert "".join(jobDescription.asJDL().split()) == f"[Test={value};]"


def test_insertAttributeString():

    # Arrange
    value = "foo"
    jobDescription = ClassAd("")

    # Act
    jobDescription.insertAttributeString("Test", value)

    # Assert
    assert jobDescription.isEmpty() is False
    assert jobDescription.lookupAttribute("Test") is True
    assert jobDescription.getAttributeString("Test") == value
    assert "".join(jobDescription.asJDL().split()) == f'[Test="{value}";]'


def test_insertAttributeVectorInt():

    # Arrange
    value = [1, 2, 3]
    jobDescription = ClassAd("")

    # Act
    jobDescription.insertAttributeVectorInt("Test", value)

    # Assert
    assert jobDescription.isEmpty() is False
    assert jobDescription.lookupAttribute("Test") is True
    assert jobDescription.getListFromExpression("Test") == [str(x) for x in value]
    assert "".join(jobDescription.asJDL().split()) == "[Test={1,2,3};]"


def test_insertAttributeVectorString():

    # Arrange
    value = ["1", "2", "3"]
    jobDescription = ClassAd("")

    # Act
    jobDescription.insertAttributeVectorString("Test", value)

    # Assert
    assert jobDescription.isEmpty() is False
    assert jobDescription.lookupAttribute("Test") is True
    assert jobDescription.getListFromExpression("Test") == [str(x) for x in value]
    assert "".join(jobDescription.asJDL().split()) == '[Test={"1","2","3"};]'
