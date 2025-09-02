"""Tests for DErrno module"""

import pytest
from DIRACCommon.Core.Utilities import DErrno


def test_strerror():
    """Test strerror function"""
    # Test DIRAC specific errors
    assert DErrno.strerror(DErrno.ETYPE) == "Object Type Error"
    assert DErrno.strerror(DErrno.EIMPERR) == "Failed to import library"
    assert DErrno.strerror(DErrno.EOF) == "Cannot open file"

    # Test unknown error
    assert "Unknown error" in DErrno.strerror(999999)

    # Test zero error
    assert DErrno.strerror(0) == "Undefined error"

    # Test OS errors (should fall back to os.strerror)
    # Error code 2 is usually "No such file or directory" on Unix
    import errno

    assert DErrno.strerror(errno.ENOENT) == "No such file or directory"


def test_cmpError():
    """Test cmpError function"""
    # Test with integer
    assert DErrno.cmpError(DErrno.ETYPE, DErrno.ETYPE) is True
    assert DErrno.cmpError(DErrno.ETYPE, DErrno.EIMPERR) is False

    # Test with string (old style)
    assert DErrno.cmpError("Object Type Error", DErrno.ETYPE) is True
    assert DErrno.cmpError("Some error with Object Type Error in it", DErrno.ETYPE) is True
    assert DErrno.cmpError("Different error", DErrno.ETYPE) is False

    # Test with S_ERROR dictionary
    error_dict = {"OK": False, "Message": "Object Type Error", "Errno": DErrno.ETYPE}
    assert DErrno.cmpError(error_dict, DErrno.ETYPE) is True

    error_dict_no_errno = {"OK": False, "Message": "Object Type Error"}
    assert DErrno.cmpError(error_dict_no_errno, DErrno.ETYPE) is True

    error_dict_wrong = {"OK": False, "Message": "Different error", "Errno": DErrno.EIMPERR}
    assert DErrno.cmpError(error_dict_wrong, DErrno.ETYPE) is False

    # Test with invalid type
    with pytest.raises(TypeError):
        DErrno.cmpError([], DErrno.ETYPE)
