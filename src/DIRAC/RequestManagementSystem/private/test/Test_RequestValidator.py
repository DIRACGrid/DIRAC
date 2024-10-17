""" :mod: RequestValidatorTests
    =======================

    .. module: RequestValidatorTests
    :synopsis: test cases for RequestValidator
    .. moduleauthor:: Krzysztof.Ciba@NOSPAMgmail.com

    test cases for RequestValidator
"""
import os
import pytest
from unittest.mock import MagicMock

from DIRAC.RequestManagementSystem.Client.Request import Request
from DIRAC.RequestManagementSystem.Client.Operation import Operation
from DIRAC.RequestManagementSystem.Client.File import File

# SUT
from DIRAC.RequestManagementSystem.private.RequestValidator import RequestValidator


@pytest.fixture
def setup():
    yield setup

    try:
        os.remove("1.txt")
        os.remove("InputData_*")
    except OSError:
        pass


def test_Validator(mocker, setup):
    """validator test"""

    request = Request()
    operation = Operation()
    file = File()

    # create validator
    validator = RequestValidator()
    assert isinstance(validator, RequestValidator)

    # RequestName not set
    ret = validator.validate(request)
    assert ret["OK"] is False
    request.RequestName = "test_request"

    # # no operations
    ret = validator.validate(request)
    assert ret["OK"] is False
    request.addOperation(operation)

    # # type not set
    ret = validator.validate(request)
    assert ret["OK"] is False
    operation.Type = "ReplicateAndRegister"

    # # files not present
    ret = validator.validate(request)
    assert ret["OK"] is False
    operation.addFile(file)

    # # targetSE not set
    ret = validator.validate(request)
    assert ret["OK"] is False
    operation.TargetSE = "CERN-USER"

    # # missing LFN
    ret = validator.validate(request)
    assert ret["OK"] is False
    file.LFN = "/a/b/c"

    # # no owner
    # force no owner because it takes the one of the current user
    request.Owner = ""
    ret = validator.validate(request)
    assert ret["OK"] is False
    request.Owner = "foo"

    # # no owner group
    # same, force it
    request.OwnerGroup = ""
    ret = validator.validate(request)
    assert ret["OK"] is False
    request.OwnerGroup = "dirac_user"

    # Checksum set, ChecksumType not set
    file.Checksum = "abcdef"
    ret = validator.validate(request)
    assert ret["OK"] is False

    # ChecksumType set, Checksum not set
    file.Checksum = ""
    file.ChecksumType = "adler32"

    ret = validator.validate(request)
    assert ret["OK"] is False

    # both set
    file.Checksum = "abcdef"
    file.ChecksumType = "adler32"
    ret = validator.validate(request)
    assert ret == {"OK": True, "Value": None}

    # both unset
    file.Checksum = ""
    file.ChecksumType = None
    ret = validator.validate(request)
    assert ret == {"OK": True, "Value": None}

    # all OK
    ret = validator.validate(request)
    assert ret == {"OK": True, "Value": None}


def test_Validator_backward_compatibility(mocker, setup):
    """validator test"""

    request = Request()
    operation = Operation()
    file = File()

    # create validator
    validator = RequestValidator()
    assert isinstance(validator, RequestValidator)

    request.RequestName = "test_request"
    request.OwnerGroup = "dirac_user"
    request.addOperation(operation)
    operation.Type = "ReplicateAndRegister"
    operation.TargetSE = "CERN-USER"
    operation.addFile(file)
    file.LFN = "/a/b/c"
    file.Checksum = "abcdef"
    file.ChecksumType = "adler32"

    # add an OwnerDN (old request)
    request.OwnerDN = "foo/bar/bz"

    ret = validator.validate(request)
    assert ret == {"OK": True, "Value": None}
