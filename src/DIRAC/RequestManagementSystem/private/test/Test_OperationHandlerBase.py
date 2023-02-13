""" tests for Graph OperationHandlerBase module
"""
import sys

import pytest

from DIRAC.RequestManagementSystem.private.OperationHandlerBase import DynamicProps


class TestClass(metaclass=DynamicProps):
    """
    .. class:: TestClass

    dummy class
    """

    pass


def test_DynamicProps():
    # # dummy instance
    testObj = TestClass()
    # # makeProperty in
    assert hasattr(testObj, "makeProperty")
    assert callable(getattr(testObj, "makeProperty"))
    # # .. and works  for rw properties
    testObj.makeProperty("rwTestProp", 10)  # pylint: disable=no-member
    assert hasattr(testObj, "rwTestProp")
    assert getattr(testObj, "rwTestProp") == 10
    testObj.rwTestProp += 1  # pylint: disable=no-member
    assert getattr(testObj, "rwTestProp") == 11
    # # .. and ro as well
    testObj.makeProperty("roTestProp", "I'm read only", True)  # pylint: disable=no-member
    assert hasattr(testObj, "roTestProp")
    assert getattr(testObj, "roTestProp") == "I'm read only"
    # # AttributeError for read only property setattr
    with pytest.raises(AttributeError) as exc_info:
        testObj.roTestProp = 11
    if sys.hexversion >= 0x03_0B_00_00:
        assert str(exc_info.value) == "property of 'TestClass' object has no setter"
    else:
        assert str(exc_info.value) == "can't set attribute"
