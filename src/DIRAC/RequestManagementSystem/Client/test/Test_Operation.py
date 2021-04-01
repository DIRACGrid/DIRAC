########################################################################
# File: OperationTests.py
# Author: Krzysztof.Ciba@NOSPAMgmail.com
# Date: 2012/08/14 14:30:20
########################################################################

""" :mod: OperationTests
    ====================

    .. module: OperationTests
    :synopsis: Operation test cases
    .. moduleauthor:: Krzysztof.Ciba@NOSPAMgmail.com

    Operation test cases
"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

__RCSID__ = "$Id $"

# #
# @file OperationTests.py
# @author Krzysztof.Ciba@NOSPAMgmail.com
# @date 2012/08/14 14:30:34
# @brief Definition of OperationTests class.

# # imports
import unittest
# # from DIRAC
from DIRAC.RequestManagementSystem.Client.File import File
# # SUT
from DIRAC.RequestManagementSystem.Client.Operation import Operation

########################################################################


class OperationTests(unittest.TestCase):
  """
  .. class:: OperationTests

  """

  def setUp(self):
    """ test set up """
    self.fromDict = {"Type": "replicateAndRegister",
                     "TargetSE": "CERN-USER,PIC-USER",
                     "SourceSE": None}
    self.subFile = File({"LFN": "/lhcb/user/c/cibak/testFile",
                         "Checksum": "1234567",
                         "ChecksumType": "ADLER32",
                         "Size": 1024,
                         "Status": "Waiting"})
    self.operation = None

  def tearDown(self):
    """ test case tear down """
    del self.fromDict
    del self.subFile

  def test01ctor(self):
    """ test constructors and (de)serialisation """
    # # empty ctor
    self.assertEqual(isinstance(Operation(), Operation), True, "empty ctor failed")

    # # using fromDict
    operation = Operation(self.fromDict)
    self.assertEqual(isinstance(operation, Operation), True, "fromDict ctor failed")
    for key, value in self.fromDict.items():
      self.assertEqual(getattr(operation, key), value, "wrong attr value %s (%s) %s" % (key,
                                                                                        getattr(operation, key),
                                                                                        value))

    # # same with file
    operation = Operation(self.fromDict)
    operation.addFile(self.subFile)

    for key, value in self.fromDict.items():
      self.assertEqual(getattr(operation, key), value, "wrong attr value %s (%s) %s" % (key,
                                                                                        getattr(operation, key),
                                                                                        value))

    toJSON = operation.toJSON()
    self.assertEqual(toJSON["OK"], True, "JSON serialization failed")

  def test02props(self):
    """ test properties """

    # # valid values
    operation = Operation()

    operation.Arguments = "foobar"
    self.assertEqual(operation.Arguments, b"foobar", "wrong Arguments")

    operation.SourceSE = "CERN-RAW"
    self.assertEqual(operation.SourceSE, "CERN-RAW", "wrong SourceSE")

    operation.TargetSE = "CERN-RAW"
    self.assertEqual(operation.TargetSE, "CERN-RAW", "wrong TargetSE")

    operation.Catalog = ""
    self.assertEqual(operation.Catalog, "", "wrong Catalog")

    operation.Catalog = "BookkeepingDB"
    self.assertEqual(operation.Catalog, "BookkeepingDB", "wrong Catalog")

    operation.Error = "error"
    self.assertEqual(operation.Error, "error", "wrong Error")

    # # wrong props
    try:
      operation.RequestID = "foo"
    except Exception as error:
      self.assertEqual(type(error), AttributeError, "wrong exc raised")
      self.assertEqual(str(error), "can't set attribute", "wrong exc reason")

    try:
      operation.OperationID = "foo"
    except Exception as error:
      self.assertEqual(type(error), ValueError, "wrong exc raised")

    # # timestamps
    try:
      operation.SubmitTime = "foo"
    except Exception as error:
      self.assertEqual(type(error), ValueError, "wrong exp raised")
      self.assertEqual(str(error), "time data 'foo' does not match format '%Y-%m-%d %H:%M:%S'", "wrong exc reason")

    try:
      operation.LastUpdate = "foo"
    except Exception as error:
      self.assertEqual(type(error), ValueError, "wrong exc raised")
      self.assertEqual(str(error), "time data 'foo' does not match format '%Y-%m-%d %H:%M:%S'", "wrong exc reason")

    # # Status
    operation = Operation()
    try:
      operation.Status = "foo"
    except Exception as error:
      self.assertEqual(type(error), ValueError, "wrong exc raised")
      self.assertEqual(str(error), "unknown Status 'foo'", "wrong exc reason")
    operation.addFile(File({"Status": "Waiting", "LFN": "/a"}))

  def test04StateMachine(self):
    """ state machine """
    op = Operation()
    self.assertEqual(op.Status, "Queued", "1. wrong status %s" % op.Status)

    op.addFile(File({"Status": "Waiting"}))
    self.assertEqual(op.Status, "Queued", "2. wrong status %s" % op.Status)

    op.addFile(File({"Status": "Scheduled"}))
    self.assertEqual(op.Status, "Scheduled", "3. wrong status %s" % op.Status)

    op.addFile(File({"Status": "Done"}))
    self.assertEqual(op.Status, "Scheduled", "4. wrong status %s" % op.Status)

    op.addFile(File({"Status": "Failed"}))
    self.assertEqual(op.Status, "Scheduled", "5. wrong status %s" % op.Status)

    op[3].Status = "Scheduled"
    self.assertEqual(op.Status, "Scheduled", "6. wrong status %s" % op.Status)

    op[0].Status = "Scheduled"
    self.assertEqual(op.Status, "Scheduled", "7. wrong status %s" % op.Status)

    op[0].Status = "Waiting"
    self.assertEqual(op.Status, "Scheduled", "8. wrong status %s" % op.Status)

    for f in op:
      f.Status = "Done"
    self.assertEqual(op.Status, "Done", "9. wrong status %s" % op.Status)

    for f in op:
      f.Status = "Failed"
    self.assertEqual(op.Status, "Failed", "9. wrong status %s" % op.Status)

  def test05List(self):
    """ getitem, setitem, delitem and dirty """
    op = Operation()

    files = []
    for i in range(5):
      f = File()
      files.append(f)
      op += f

    for i in range(len(op)):
      self.assertEqual(op[i], files[i], "__getitem__ failed")

    for i in range(len(op)):
      op[i] = File({"LFN": "/%s" % i})
      self.assertEqual(op[i].LFN, "/%s" % i, "__setitem__ failed")

    del op[0]
    self.assertEqual(len(op), 4, "__delitem__ failed")

    # # opID set
    op.OperationID = 1
    del op[0]


# # test execution
if __name__ == "__main__":
  testLoader = unittest.TestLoader()
  operationTests = testLoader.loadTestsFromTestCase(OperationTests)
  suite = unittest.TestSuite([operationTests])
  unittest.TextTestRunner(verbosity=3).run(suite)
