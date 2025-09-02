""" Unit tests for JobDBUtils module"""

# pylint: disable=protected-access, invalid-name

import base64
import zlib

from DIRACCommon.WorkloadManagementSystem.DB.JobDBUtils import compressJDL, extractJDL, fixJDL


def test_compress_extract_jdl():
    """Test JDL compression and extraction"""
    # Arrange
    original_jdl = '[Executable = "my_executable"; JobName = "Test"]'

    # Act
    compressed = compressJDL(original_jdl)
    extracted = extractJDL(compressed)

    # Assert
    assert compressed != original_jdl
    assert extracted == original_jdl
    assert isinstance(compressed, str)


def test_extract_jdl_already_decompressed():
    """Test extracting JDL that's already decompressed"""
    # Arrange
    jdl = '[Executable = "my_executable"; JobName = "Test"]'

    # Act
    result = extractJDL(jdl)

    # Assert
    assert result == jdl


def test_extract_jdl_bytes():
    """Test extracting JDL from bytes"""
    # Arrange
    jdl_bytes = b'[Executable = "my_executable"; JobName = "Test"]'

    # Act
    result = extractJDL(jdl_bytes)

    # Assert
    assert result == jdl_bytes.decode()


def test_fix_jdl_missing_brackets():
    """Test fixing JDL missing brackets"""
    # Arrange
    jdl_no_brackets = 'Executable = "my_executable"; JobName = "Test"'

    # Act
    fixed = fixJDL(jdl_no_brackets)

    # Assert
    assert fixed.startswith("[")
    assert fixed.endswith("]")
    assert "Executable" in fixed


def test_fix_jdl_already_bracketed():
    """Test fixing JDL that already has brackets"""
    # Arrange
    jdl = '[Executable = "my_executable"; JobName = "Test"]'

    # Act
    fixed = fixJDL(jdl)

    # Assert
    assert fixed == jdl
