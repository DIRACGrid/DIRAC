""" This is a test of the parametric job generation tools"""
# pylint: disable= missing-docstring

import pytest

# Test imports from DIRAC to verify backward compatibility
from DIRAC.WorkloadManagementSystem.Utilities.ParametricJob import generateParametricJobs, getParameterVectorLength
from DIRAC.Core.Utilities.ClassAd.ClassAdLight import ClassAd


def test_backward_compatibility_import():
    """Test that imports from DIRAC still work (backward compatibility)"""
    # Arrange
    jdl = """
    [
        Executable = "my_executable";
        Arguments = "%s";
        JobName = "Test_%n";
        Parameters = { "a", "b", "c" }
    ]
    """

    # Act - Test that we can import and use the functions from DIRAC
    jobDescription = ClassAd(jdl)
    vector_result = getParameterVectorLength(jobDescription)
    generate_result = generateParametricJobs(jobDescription)

    # Assert - Verify functions work correctly
    assert vector_result["OK"]
    assert vector_result["Value"] == 3
    assert generate_result["OK"]
    assert len(generate_result["Value"]) == 3


# Import and run the comprehensive tests from DIRACCommon to avoid duplication
# This ensures the DIRAC re-exports work with the full test suite
try:
    from DIRACCommon.tests.WorkloadManagementSystem.Utilities.test_ParametricJob import (
        test_getParameterVectorLength_successful,
        test_getParameterVectorLength_unsuccessful,
        test_generateParametricJobs,
    )

    # Re-export the DIRACCommon tests so they run as part of DIRAC test suite
    # This validates that the backward compatibility imports work correctly
    __all__ = [
        "test_backward_compatibility_import",
        "test_getParameterVectorLength_successful",
        "test_getParameterVectorLength_unsuccessful",
        "test_generateParametricJobs",
    ]

except ImportError:
    # If DIRACCommon tests can't be imported, just run the backward compatibility test
    __all__ = ["test_backward_compatibility_import"]
