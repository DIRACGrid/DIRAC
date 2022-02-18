"""Test the API class."""
import copy

from DIRAC.Core.Base.API import API


def test_deepcopy():
    """Ensure we can copy the API classes."""
    instance_A = API()
    instance_B = copy.deepcopy(instance_A)
    assert instance_B.log
