""" :mod: FileTest
    =======================

    .. module: FileTest
    :synopsis: test cases for Files
    .. moduleauthor:: Krzysztof.Ciba@NOSPAMgmail.com

    test cases for Files
"""

import pytest

from DIRAC.RequestManagementSystem.Client.Operation import Operation
from DIRAC.RequestManagementSystem.Client.File import File


def test_ctors():
    """File construction and (de)serialisation"""
    theFile = File()
    assert isinstance(theFile, File)

    fromDict = {
        "Size": 1,
        "LFN": "/test/lfn",
        "ChecksumType": "ADLER32",
        "Checksum": "123456",
        "Status": "Waiting",
    }
    try:
        theFile = File(fromDict)
    except AttributeError as error:
        print("AttributeError: %s" % str(error))

    assert isinstance(theFile, File)
    for key, value in fromDict.items():
        assert getattr(theFile, key) == value

    toJSON = theFile.toJSON()
    assert toJSON["OK"], "JSON serialization error"


def test_valid_properties():
    theFile = File()

    theFile.FileID = 1
    assert theFile.FileID == 1
    theFile.Status = "Done"
    assert theFile.Status == "Done"
    theFile.LFN = "/some/path/somewhere"
    assert theFile.LFN == "/some/path/somewhere"
    theFile.PFN = "/some/path/somewhere"
    assert theFile.PFN == "/some/path/somewhere"
    theFile.Attempt = 1
    assert theFile.Attempt == 1
    theFile.Size = 1
    assert theFile.Size == 1
    theFile.GUID = "2bbabe80-e2f1-11e1-9b23-0800200c9a66"
    assert theFile.GUID == "2bbabe80-e2f1-11e1-9b23-0800200c9a66"
    theFile.ChecksumType = "adler32"
    assert theFile.ChecksumType == "ADLER32"
    theFile.Checksum = "123456"
    assert theFile.Checksum == "123456"
