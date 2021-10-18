""" :mod: OperationTests
    ====================

    .. module: OperationTests
    :synopsis: Operation test cases
    .. moduleauthor:: Krzysztof.Ciba@NOSPAMgmail.com

    Operation test cases
"""

__RCSID__ = "$Id $"

import pytest

from DIRAC.RequestManagementSystem.Client.File import File
from DIRAC.RequestManagementSystem.Client.Operation import Operation


def test_ctor():
    """test constructors and (de)serialisation"""
    assert isinstance(Operation(), Operation), "empty ctor failed"

    # # using fromDict
    fromDict = {
        "Type": "replicateAndRegister",
        "TargetSE": "CERN-USER,PIC-USER",
        "SourceSE": None,
    }
    operation = Operation(fromDict)
    assert isinstance(operation, Operation), "fromDict ctor failed"
    for key, value in fromDict.items():
        assert getattr(operation, key) == value, "wrong attr value %s (%s) %s" % (key, getattr(operation, key), value)

    # # same with file
    operation = Operation(fromDict)
    operation.addFile(
        File(
            {
                "LFN": "/lhcb/user/c/cibak/testFile",
                "Checksum": "1234567",
                "ChecksumType": "ADLER32",
                "Size": 1024,
                "Status": "Waiting",
            }
        )
    )

    for key, value in fromDict.items():
        assert getattr(operation, key) == value, "wrong attr value %s (%s) %s" % (key, getattr(operation, key), value)

    toJSON = operation.toJSON()
    assert toJSON["OK"], "JSON serialization failed"


def test_valid_properties():
    operation = Operation()

    operation.Arguments = "foobar"
    assert operation.Arguments == b"foobar", "wrong Arguments"

    operation.SourceSE = "CERN-RAW"
    assert operation.SourceSE == "CERN-RAW", "wrong SourceSE"

    operation.TargetSE = "CERN-RAW"
    assert operation.TargetSE == "CERN-RAW", "wrong TargetSE"

    operation.Catalog = ""
    assert operation.Catalog == "", "wrong Catalog"

    operation.Catalog = "BookkeepingDB"
    assert operation.Catalog == "BookkeepingDB", "wrong Catalog"

    operation.Error = "error"
    assert operation.Error == "error", "wrong Error"

    toJSON = operation.toJSON()
    assert toJSON["OK"]


def test_StateMachine():
    """state machine"""
    op = Operation()
    assert op.Status == "Queued", "1. wrong status %s" % op.Status

    op.addFile(File({"Status": "Waiting"}))
    assert op.Status == "Queued", "2. wrong status %s" % op.Status

    op.addFile(File({"Status": "Scheduled"}))
    assert op.Status == "Scheduled", "3. wrong status %s" % op.Status

    op.addFile(File({"Status": "Done"}))
    assert op.Status == "Scheduled", "4. wrong status %s" % op.Status

    op.addFile(File({"Status": "Failed"}))
    assert op.Status == "Scheduled", "5. wrong status %s" % op.Status

    op[3].Status = "Scheduled"
    assert op.Status == "Scheduled", "6. wrong status %s" % op.Status

    op[0].Status = "Scheduled"
    assert op.Status == "Scheduled", "7. wrong status %s" % op.Status

    op[0].Status = "Waiting"
    assert op.Status == "Scheduled", "8. wrong status %s" % op.Status

    for f in op:
        f.Status = "Done"
    assert op.Status == "Done", "9. wrong status %s" % op.Status

    for f in op:
        f.Status = "Failed"
    assert op.Status == "Failed", "9. wrong status %s" % op.Status


def test_List():
    """getitem, setitem, delitem and dirty"""
    op = Operation()

    files = []
    for _ in range(5):
        f = File()
        files.append(f)
        op += f

    for i in range(len(op)):
        assert op[i] == files[i], "__getitem__ failed"

    for i in range(len(op)):
        op[i] = File({"LFN": "/%s" % i})
        assert op[i].LFN == "/%s" % i, "__setitem__ failed"

    del op[0]
    assert len(op) == 4, "__delitem__ failed"

    # opID set
    op.OperationID = 1
    del op[0]
