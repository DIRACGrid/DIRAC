""" Unit tests for ClassAdLight module"""

# pylint: disable=protected-access, invalid-name

import pytest

from DIRACCommon.Core.Utilities.ClassAd.ClassAdLight import ClassAd


def test_classad_basic():
    """Test basic ClassAd functionality"""
    # Arrange
    jdl = '[Executable = "my_executable"; JobName = "Test"]'

    # Act
    classad = ClassAd(jdl)

    # Assert
    assert classad.isOK()
    assert classad.getAttributeString("Executable") == "my_executable"
    assert classad.getAttributeString("JobName") == "Test"


def test_classad_insert_attributes():
    """Test inserting attributes into ClassAd"""
    # Arrange
    jdl = "[]"
    classad = ClassAd(jdl)

    # Act
    classad.insertAttributeString("Executable", "test_exe")
    classad.insertAttributeInt("Priority", 5)
    classad.insertAttributeBool("Test", True)

    # Assert
    assert classad.getAttributeString("Executable") == "test_exe"
    assert classad.getAttributeInt("Priority") == 5
    assert classad.getAttributeBool("Test") == True


def test_classad_list_attributes():
    """Test list attributes in ClassAd"""
    # Arrange
    jdl = '[InputData = {"file1.txt", "file2.txt"}]'
    classad = ClassAd(jdl)

    # Act
    file_list = classad.getListFromExpression("InputData")

    # Assert
    assert len(file_list) == 2
    assert "file1.txt" in file_list
    assert "file2.txt" in file_list


def test_classad_as_jdl():
    """Test converting ClassAd back to JDL"""
    # Arrange
    original_jdl = '[Executable = "my_executable"; JobName = "Test"]'
    classad = ClassAd(original_jdl)

    # Act
    regenerated_jdl = classad.asJDL()

    # Assert
    assert "Executable" in regenerated_jdl
    assert "my_executable" in regenerated_jdl
    assert "JobName" in regenerated_jdl
    assert "Test" in regenerated_jdl
