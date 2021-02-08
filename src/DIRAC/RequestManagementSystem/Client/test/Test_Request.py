########################################################################
# File: RequestTests.py
# Author: Krzysztof.Ciba@NOSPAMgmail.com
# Date: 2012/07/24 10:23:40
########################################################################

""" :mod: RequestTests
    =======================

    .. module: RequestTests
    :synopsis: test cases for Request class
    .. moduleauthor:: Krzysztof.Ciba@NOSPAMgmail.com

    test cases for Request class
"""

from __future__ import print_function
from __future__ import absolute_import
from __future__ import division
__RCSID__ = "$Id$"

# #
# @file RequestTests.py
# @author Krzysztof.Ciba@NOSPAMgmail.com
# @date 2012/07/24 10:23:52
# @brief Definition of RequestTests class.

# # imports
import six
import unittest
import datetime
# # from DIRAC
from DIRAC.RequestManagementSystem.Client.Operation import Operation
from DIRAC.RequestManagementSystem.Client.File import File
# # SUT
from DIRAC.RequestManagementSystem.Client.Request import Request
from DIRAC.RequestManagementSystem.Client.ReqClient import printRequest


def optimizeRequest(req, printOutput=None):
  from DIRAC import gLogger
  if printOutput:
    if isinstance(printOutput, six.string_types):
      gLogger.always('Request %s:' % printOutput)
    printRequest(req)
    gLogger.always('=========== Optimized ===============')
  res = req.optimize()
  if printOutput:
    printRequest(req)
    gLogger.always('')
  return res


def createRequest(reqType):
  r = Request()

  # Simple failover
  op1 = Operation()
  f = File()
  f.LFN = '/This/is/an/LFN'
  op1.addFile(f)
  op1.Type = 'ReplicateAndRegister'
  op1.SourceSE = 'CERN-FAILOVER'
  op1.TargetSE = 'CERN-BUFFER'
  r.addOperation(op1)

  # You cannot reuse the same File object,
  # since it is a different entry in the DB
  fr = File()
  fr.LFN = '/This/is/an/LFN'
  op2 = Operation()
  op2.addFile(fr)
  op2.Type = 'RemoveReplica'
  op2.TargetSE = 'CERN-FAILOVER'
  r.addOperation(op2)
  if reqType == 0:
    return r

  # two files for Failover
  f1 = File()
  f1.LFN = '/This/is/a/second/LFN'
  op3 = Operation()
  op3.addFile(f1)
  op3.Type = 'ReplicateAndRegister'
  op3.SourceSE = 'CERN-FAILOVER'
  op3.TargetSE = 'CERN-BUFFER'
  r.addOperation(op3)

  f1r = File()
  f1r.LFN = '/This/is/a/second/LFN'
  op3 = Operation()
  op3.addFile(f1r)
  op3.Type = 'RemoveReplica'
  op3.TargetSE = 'CERN-FAILOVER'
  r.addOperation(op3)
  if reqType == 1:
    return r

  op = Operation()
  op.Type = 'ForwardDiset'
  if reqType == 2:
    r.addOperation(op)
    return r

  r.insertBefore(op, r[0])
  if reqType == 3:
    return r

  op4 = Operation()
  op4.Type = 'ForwardDiset'
  r.addOperation(op4)
  if reqType == 4:
    return r

  # 2 different FAILOVER SEs: removal not optimized
  r[1].SourceSE = 'RAL-FAILOVER'
  r[2].SourceSE = 'RAL-FAILOVER'
  if reqType == 5:
    return r

  # 2 different destinations, same FAILOVER: replication not optimized
  r[3].SourceSE = 'RAL-FAILOVER'
  r[4].SourceSE = 'RAL-FAILOVER'
  r[3].TargetSE = 'RAL-BUFFER'
  if reqType == 6:
    return r

  print('This should not happen, reqType =', reqType)

########################################################################


class RequestTests(unittest.TestCase):
  """
  .. class:: RequestTests

  """

  def setUp(self):
    """ set up """
    self.fromDict = {"RequestName": "test", "JobID": 12345}

  def tearDown(self):
    """ tear down """
    del self.fromDict

  def test_01CtorSerilization(self):
    """ c'tor and serialization """
    # # empty c'tor
    req = Request()
    self.assertEqual(isinstance(req, Request), True)
    self.assertEqual(req.JobID, 0)
    self.assertEqual(req.Status, "Waiting")

    req = Request(self.fromDict)
    self.assertEqual(isinstance(req, Request), True)
    self.assertEqual(req.RequestName, "test")
    self.assertEqual(req.JobID, 12345)
    self.assertEqual(req.Status, "Waiting")

    toJSON = req.toJSON()
    self.assertEqual(toJSON["OK"], True, "JSON serialization failed")

    fromJSON = toJSON["Value"]
    req = Request(fromJSON)

  def test_02Props(self):
    """ props """
    # # valid values
    req = Request()

    req.RequestID = 1
    self.assertEqual(req.RequestID, 1)

    req.RequestName = "test"
    self.assertEqual(req.RequestName, "test")

    req.JobID = 1
    self.assertEqual(req.JobID, 1)

    req.CreationTime = "1970-01-01 00:00:00"
    self.assertEqual(req.CreationTime, datetime.datetime(1970, 1, 1, 0, 0, 0))
    req.CreationTime = datetime.datetime(1970, 1, 1, 0, 0, 0)
    self.assertEqual(req.CreationTime, datetime.datetime(1970, 1, 1, 0, 0, 0))

    req.SubmitTime = "1970-01-01 00:00:00"
    self.assertEqual(req.SubmitTime, datetime.datetime(1970, 1, 1, 0, 0, 0))
    req.SubmitTime = datetime.datetime(1970, 1, 1, 0, 0, 0)
    self.assertEqual(req.SubmitTime, datetime.datetime(1970, 1, 1, 0, 0, 0))

    req.LastUpdate = "1970-01-01 00:00:00"
    self.assertEqual(req.LastUpdate, datetime.datetime(1970, 1, 1, 0, 0, 0))
    req.LastUpdate = datetime.datetime(1970, 1, 1, 0, 0, 0)
    self.assertEqual(req.LastUpdate, datetime.datetime(1970, 1, 1, 0, 0, 0))

    req.Error = ""

  def test_04Operations(self):
    """ operations arithmetic and state machine """
    req = Request()
    self.assertEqual(len(req), 0)

    transfer = Operation()
    transfer.Type = "ReplicateAndRegister"
    transfer.addFile(File({"LFN": "/a/b/c", "Status": "Waiting"}))

    getWaiting = req.getWaiting()
    self.assertEqual(getWaiting["OK"], True)
    self.assertEqual(getWaiting["Value"], None)

    req.addOperation(transfer)
    self.assertEqual(len(req), 1)
    self.assertEqual(transfer.Order, req.Order)
    self.assertEqual(transfer.Status, "Waiting")

    getWaiting = req.getWaiting()
    self.assertEqual(getWaiting["OK"], True)
    self.assertEqual(getWaiting["Value"], transfer)

    removal = Operation({"Type": "RemoveFile"})
    removal.addFile(File({"LFN": "/a/b/c", "Status": "Waiting"}))

    req.insertBefore(removal, transfer)

    getWaiting = req.getWaiting()
    self.assertEqual(getWaiting["OK"], True)
    self.assertEqual(getWaiting["Value"], removal)

    self.assertEqual(len(req), 2)
    self.assertEqual([op.Status for op in req], ["Waiting", "Queued"])
    self.assertEqual(req.subStatusList(), ["Waiting", "Queued"])

    self.assertEqual(removal.Order, 0)
    self.assertEqual(removal.Order, req.Order)

    self.assertEqual(transfer.Order, 1)

    self.assertEqual(removal.Status, "Waiting")
    self.assertEqual(transfer.Status, "Queued")

    for subFile in removal:
      subFile.Status = "Done"
    removal.Status = "Done"

    self.assertEqual(removal.Status, "Done")

    self.assertEqual(transfer.Status, "Waiting")
    self.assertEqual(transfer.Order, req.Order)

    # # len, looping
    self.assertEqual(len(req), 2)
    self.assertEqual([op.Status for op in req], ["Done", "Waiting"])
    self.assertEqual(req.subStatusList(), ["Done", "Waiting"])

    digest = req.toJSON()
    self.assertEqual(digest["OK"], True)

    getWaiting = req.getWaiting()
    self.assertEqual(getWaiting["OK"], True)
    self.assertEqual(getWaiting["Value"], transfer)

  def test_05FTS(self):
    """ FTS state machine """

    req = Request()
    req.RequestName = "FTSTest"

    ftsTransfer = Operation()
    ftsTransfer.Type = "ReplicateAndRegister"
    ftsTransfer.TargetSE = "CERN-USER"

    ftsFile = File()
    ftsFile.LFN = "/a/b/c"
    ftsFile.Checksum = "123456"
    ftsFile.ChecksumType = "Adler32"

    ftsTransfer.addFile(ftsFile)
    req.addOperation(ftsTransfer)

    self.assertEqual(req.Status, "Waiting", "1. wrong request status: %s" % req.Status)
    self.assertEqual(ftsTransfer.Status, "Waiting", "1. wrong ftsStatus status: %s" % ftsTransfer.Status)

    # # scheduled
    ftsFile.Status = "Scheduled"

    self.assertEqual(ftsTransfer.Status, "Scheduled", "2. wrong status for ftsTransfer: %s" % ftsTransfer.Status)
    self.assertEqual(req.Status, "Scheduled", "2. wrong status for request: %s" % req.Status)

    # # add new operation before FTS
    insertBefore = Operation()
    insertBefore.Type = "RegisterReplica"
    insertBefore.TargetSE = "CERN-USER"
    insertFile = File()
    insertFile.LFN = "/a/b/c"
    insertFile.PFN = "http://foo/bar"
    insertBefore.addFile(insertFile)
    req.insertBefore(insertBefore, ftsTransfer)

    self.assertEqual(insertBefore.Status, "Waiting", "3. wrong status for insertBefore: %s" % insertBefore.Status)
    self.assertEqual(ftsTransfer.Status, "Scheduled", "3. wrong status for ftsStatus: %s" % ftsTransfer.Status)
    self.assertEqual(req.Status, "Waiting", "3. wrong status for request: %s" % req.Status)

    # # prev done
    insertFile.Status = "Done"

    self.assertEqual(insertBefore.Status, "Done", "4. wrong status for insertBefore: %s" % insertBefore.Status)
    self.assertEqual(ftsTransfer.Status, "Scheduled", "4. wrong status for ftsStatus: %s" % ftsTransfer.Status)
    self.assertEqual(req.Status, "Scheduled", "4. wrong status for request: %s" % req.Status)

    # # reschedule
    ftsFile.Status = "Waiting"

    self.assertEqual(insertBefore.Status, "Done", "5. wrong status for insertBefore: %s" % insertBefore.Status)
    self.assertEqual(ftsTransfer.Status, "Waiting", "5. wrong status for ftsStatus: %s" % ftsTransfer.Status)
    self.assertEqual(req.Status, "Waiting", "5. wrong status for request: %s" % req.Status)

    # # fts done
    ftsFile.Status = "Done"

    self.assertEqual(insertBefore.Status, "Done", "5. wrong status for insertBefore: %s" % insertBefore.Status)
    self.assertEqual(ftsTransfer.Status, "Done", "5. wrong status for ftsStatus: %s" % ftsTransfer.Status)
    self.assertEqual(req.Status, "Done", "5. wrong status for request: %s" % req.Status)

  def test_06StateMachine(self):
    """ state machine tests """
    r = Request({"RequestName": "SMT"})
    self.assertEqual(r.Status, "Waiting", "1. wrong status %s" % r.Status)

    r.addOperation(Operation({"Status": "Queued"}))
    self.assertEqual(r.Status, "Waiting", "2. wrong status %s" % r.Status)

    r.addOperation(Operation({"Status": "Queued"}))
    self.assertEqual(r.Status, "Waiting", "3. wrong status %s" % r.Status)

    r[0].Status = "Done"
    self.assertEqual(r.Status, "Waiting", "4. wrong status %s" % r.Status)

    r[1].Status = "Done"
    self.assertEqual(r.Status, "Done", "5. wrong status %s" % r.Status)

    r[0].Status = "Failed"
    self.assertEqual(r.Status, "Failed", "6. wrong status %s" % r.Status)

    r[0].Status = "Queued"
    self.assertEqual(r.Status, "Waiting", "7. wrong status %s" % r.Status)

    r.insertBefore(Operation({"Status": "Queued"}), r[0])
    self.assertEqual(r.Status, "Waiting", "8. wrong status %s" % r.Status)

    r.insertBefore(Operation({"Status": "Queued"}), r[0])
    self.assertEqual(r.Status, "Waiting", "9. wrong status %s" % r.Status)

    r.insertBefore(Operation({"Status": "Scheduled"}), r[0])
    self.assertEqual(r.Status, "Scheduled", "10. wrong status %s" % r.Status)

    r.insertBefore(Operation({"Status": "Queued"}), r[0])
    self.assertEqual(r.Status, "Waiting", "11. wrong status %s" % r.Status)

    r[0].Status = "Failed"
    self.assertEqual(r.Status, "Failed", "12. wrong status %s" % r.Status)

    r[0].Status = "Done"
    self.assertEqual(r.Status, "Scheduled", "13. wrong status %s" % r.Status)

    r[1].Status = "Failed"
    self.assertEqual(r.Status, "Failed", "14. wrong status %s" % r.Status)

    r[1].Status = "Done"
    self.assertEqual(r.Status, "Waiting", "15. wrong status %s" % r.Status)

    r[2].Status = "Scheduled"
    self.assertEqual(r.Status, "Scheduled", "16. wrong status %s" % r.Status)

    r[2].Status = "Queued"
    self.assertEqual(r.Status, "Waiting", "17. wrong status %s" % r.Status)

    r[2].Status = "Scheduled"
    self.assertEqual(r.Status, "Scheduled", "18. wrong status %s" % r.Status)

    r = Request()
    for i in range(5):
      r.addOperation(Operation({"Status": "Queued"}))

    r[0].Status = "Done"
    self.assertEqual(r.Status, "Waiting", "19. wrong status %s" % r.Status)

    r[1].Status = "Done"
    self.assertEqual(r.Status, "Waiting", "20. wrong status %s" % r.Status)

    r[2].Status = "Scheduled"
    self.assertEqual(r.Status, "Scheduled", "21. wrong status %s" % r.Status)

    r[2].Status = "Done"
    self.assertEqual(r.Status, "Waiting", "22. wrong status %s" % r.Status)

  def test_07List(self):
    """ setitem, delitem, getitem and dirty """

    r = Request()

    ops = [Operation() for i in range(5)]

    for op in ops:
      r.addOperation(op)

    for i, op in enumerate(ops):
      self.assertEqual(op, r[i], "__getitem__ failed")

    op = Operation()
    r[0] = op
    self.assertEqual(op, r[0], "__setitem__ failed")

    del r[0]
    self.assertEqual(len(r), 4, "__delitem__ failed")

  def test_08Optimize(self):
    title = {
        0: 'Simple Failover',
        1: 'Double Failover',
        2: 'Double Failover + ForwardDiset',
        3: 'ForwardDiset + Double Failover',
        4: 'ForwardDiset + Double Failover + ForwardDiset',
        5: 'ForwardDiset + Double Failover (# Failover SE) + ForwardDiset',
        6: 'ForwardDiset + Double Failover (# Destination SE) + ForwardDiset'
    }
    debug = False
    if debug:
      print('')
    for reqType in title:

      r = createRequest(reqType)
      res = optimizeRequest(r, printOutput=title[reqType] if (debug == reqType and debug is not False) else False)
      self.assertEqual(res['OK'], True)
      self.assertEqual(res['Value'], True)
      if reqType in (0, 1):
        self.assertEqual(len(r), 2, 'Wrong number of operations: %d' % len(r))
        self.assertEqual(r[0].Type, 'ReplicateAndRegister')
        self.assertEqual(r[1].Type, 'RemoveReplica')
      if reqType == 1:
        self.assertEqual(len(r[0]), 2, 'Wrong number of files: %d' % len(r[0]))
        self.assertEqual(len(r[1]), 2, 'Wrong number of files: %d' % len(r[1]))
      elif reqType == 2:
        self.assertEqual(len(r), 3, 'Wrong number of operations: %d' % len(r))
        self.assertEqual(r[0].Type, 'ReplicateAndRegister')
        self.assertEqual(r[1].Type, 'RemoveReplica')
        self.assertEqual(r[2].Type, 'ForwardDiset')
        self.assertEqual(len(r[0]), 2, 'Wrong number of files: %d' % len(r[0]))
        self.assertEqual(len(r[1]), 2, 'Wrong number of files: %d' % len(r[1]))
      elif reqType == 3:
        self.assertEqual(len(r), 3, 'Wrong number of operations: %d' % len(r))
        self.assertEqual(r[1].Type, 'ReplicateAndRegister')
        self.assertEqual(r[2].Type, 'RemoveReplica')
        self.assertEqual(r[0].Type, 'ForwardDiset')
        self.assertEqual(len(r[1]), 2, 'Wrong number of files: %d' % len(r[0]))
        self.assertEqual(len(r[2]), 2, 'Wrong number of files: %d' % len(r[1]))
      elif reqType == 4:
        self.assertEqual(len(r), 4, 'Wrong number of operations: %d' % len(r))
        self.assertEqual(r[1].Type, 'ReplicateAndRegister')
        self.assertEqual(r[2].Type, 'RemoveReplica')
        self.assertEqual(r[0].Type, 'ForwardDiset')
        self.assertEqual(r[3].Type, 'ForwardDiset')
        self.assertEqual(len(r[1]), 2, 'Wrong number of files: %d' % len(r[0]))
        self.assertEqual(len(r[2]), 2, 'Wrong number of files: %d' % len(r[1]))
      elif reqType == 5:
        self.assertEqual(len(r), 5, 'Wrong number of operations: %d' % len(r))
        self.assertEqual(r[1].Type, 'ReplicateAndRegister')
        self.assertEqual(r[2].Type, 'RemoveReplica')
        self.assertEqual(r[3].Type, 'RemoveReplica')
        self.assertEqual(r[0].Type, 'ForwardDiset')
        self.assertEqual(r[4].Type, 'ForwardDiset')
        self.assertEqual(len(r[1]), 2, 'Wrong number of files: %d' % len(r[0]))
        self.assertEqual(len(r[2]), 1, 'Wrong number of files: %d' % len(r[1]))
        self.assertEqual(len(r[3]), 1, 'Wrong number of files: %d' % len(r[1]))
      elif reqType == 6:
        self.assertEqual(len(r), 5, 'Wrong number of operations: %d' % len(r))
        self.assertEqual(r[1].Type, 'ReplicateAndRegister')
        self.assertEqual(r[2].Type, 'ReplicateAndRegister')
        self.assertEqual(r[3].Type, 'RemoveReplica')
        self.assertEqual(r[0].Type, 'ForwardDiset')
        self.assertEqual(r[4].Type, 'ForwardDiset')
        self.assertEqual(len(r[1]), 1, 'Wrong number of files: %d' % len(r[0]))
        self.assertEqual(len(r[2]), 1, 'Wrong number of files: %d' % len(r[1]))
        self.assertEqual(len(r[3]), 2, 'Wrong number of files: %d' % len(r[1]))

# # test execution
if __name__ == "__main__":

  suite = unittest.defaultTestLoader.loadTestsFromTestCase(RequestTests)
  testResult = unittest.TextTestRunner(verbosity=2).run(suite)
