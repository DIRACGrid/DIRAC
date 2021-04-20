""" :mod: FileTest
    =======================

    .. module: FileTest
    :synopsis: test cases for Files
    .. moduleauthor:: Krzysztof.Ciba@NOSPAMgmail.com

    test cases for Files
"""

from __future__ import print_function
from __future__ import absolute_import
from __future__ import division
__RCSID__ = "$Id$"

import pytest

from DIRAC.RequestManagementSystem.Client.Operation import Operation
from DIRAC.RequestManagementSystem.Client.File import File


def test_ctors():
  """ File construction and (de)serialisation """
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


def test_invalid_properties():
  theFile = File()

  theFile.Checksum = None
  theFile.ChecksumType = None

  # FileID
  with pytest.raises(ValueError):
    theFile.FileID = "foo"

  # parent
  parent = Operation({"OperationID": 99999})
  parent += theFile

  theFile.FileID = 0

  with pytest.raises(AttributeError, match="can't set attribute"):
    theFile.OperationID = 111111

  # LFN
  with pytest.raises(TypeError, match="LFN has to be a string!"):
    theFile.LFN = 1
  with pytest.raises(ValueError, match="LFN should be an absolute path!"):
    theFile.LFN = "../some/path"

  # PFN
  with pytest.raises(TypeError, match="PFN has to be a string!"):
    theFile.PFN = 1
  with pytest.raises(ValueError, match="Wrongly formatted PFN!"):
    theFile.PFN = "snafu"

  # Size
  with pytest.raises(ValueError):
    theFile.Size = "snafu"
    theFile.Size = -1

  # GUID
  with pytest.raises(ValueError, match="'snafuu-uuu-uuu-uuu-uuu-u' is not a valid GUID!"):
    theFile.GUID = "snafuu-uuu-uuu-uuu-uuu-u"
  with pytest.raises(TypeError, match="GUID should be a string!"):
    theFile.GUID = 2233345

  # Attempt
  with pytest.raises(ValueError):
    theFile.Attempt = "snafu"
    theFile.Attempt = -1

  # Status
  with pytest.raises(ValueError, match="Unknown Status: None!"):
    theFile.Status = None

  # Error
  with pytest.raises(TypeError, match="Error has to be a string!"):
    theFile.Error = Exception("test")
