""" This is a test of the ReqDB

    It supposes that the DB is present
"""

# pylint: disable=invalid-name,wrong-import-position

from __future__ import print_function
from __future__ import absolute_import
from __future__ import division

import unittest
import sys
import time

from DIRAC.Core.Base.Script import parseCommandLine
parseCommandLine()


from DIRAC import gLogger

from DIRAC.RequestManagementSystem.Client.Request import Request
from DIRAC.RequestManagementSystem.Client.Operation import Operation
from DIRAC.RequestManagementSystem.Client.File import File

from DIRAC.RequestManagementSystem.DB.RequestDB import RequestDB


class ReqDBTestCase(unittest.TestCase):
  """
  .. class:: ReqClientTestCase

  """

  def setUp(self):
    """ test case set up """

    gLogger.setLevel('INFO')

    self.stressRequests = 1000
    self.bulkRequest = 1000


class ReqDB(ReqDBTestCase):

  def test_db(self):

    # # empty DB at that stage
    ret = RequestDB().getDBSummary()
    self.assertEqual(ret,
                     {'OK': True,
                      'Value': {'Operation': {}, 'Request': {}, 'File': {}}})


class ReqDBMix(ReqDBTestCase):

  def test01Stress(self):
    """ stress test """

    db = RequestDB()

    reqIDs = []
    for i in range(self.stressRequests):
      request = Request({"RequestName": "test-%d" % i})
      op = Operation({"Type": "RemoveReplica", "TargetSE": "CERN-USER"})
      op += File({"LFN": "/lhcb/user/c/cibak/foo"})
      request += op
      put = db.putRequest(request)
      self.assertEqual(put["OK"], True, put['Message'] if 'Message' in put else 'OK')
      reqIDs.append(put['Value'])

    startTime = time.time()

    for reqID in reqIDs:
      get = db.getRequest(reqID)
      if "Message" in get:
        print(get["Message"])
      self.assertEqual(get["OK"], True, get['Message'] if 'Message' in get else 'OK')

    endTime = time.time()

    print("getRequest duration %s " % (endTime - startTime))

    for reqID in reqIDs:
      delete = db.deleteRequest(reqID)
      self.assertEqual(delete["OK"], True, delete['Message'] if 'Message' in delete else 'OK')

  def test01StressBulk(self):
    """ stress test bulk """

    db = RequestDB()

    reqIDs = []
    for i in range(self.stressRequests):
      request = Request({"RequestName": "test-%d" % i})
      op = Operation({"Type": "RemoveReplica", "TargetSE": "CERN-USER"})
      op += File({"LFN": "/lhcb/user/c/cibak/foo"})
      request += op
      put = db.putRequest(request)
      self.assertEqual(put["OK"], True)
      reqIDs.append(put['Value'])

    loops = self.stressRequests // self.bulkRequest + \
        (1 if (self.stressRequests % self.bulkRequest) else 0)
    totalSuccessful = 0

    time.sleep(1)
    startTime = time.time()

    for i in range(loops):
      get = db.getBulkRequests(self.bulkRequest, True)
      if "Message" in get:
        print(get["Message"])
      self.assertEqual(get["OK"], True, "get failed")

      totalSuccessful += len(get["Value"])

    endTime = time.time()

    print("getRequests duration %s " % (endTime - startTime))

    self.assertEqual(
        totalSuccessful,
        self.stressRequests,
        "Did not retrieve all the requests: %s instead of %s" %
        (totalSuccessful,
         self.stressRequests))

    for reqID in reqIDs:
      delete = db.deleteRequest(reqID)
      self.assertEqual(delete["OK"], True, delete['Message'] if 'Message' in delete else 'OK')

  def test02Scheduled(self):
    """ scheduled request r/w """

    db = RequestDB()

    req = Request({"RequestName": "FTSTest"})
    op = Operation({"Type": "ReplicateAndRegister", "TargetSE": "CERN-USER"})
    op += File({"LFN": "/a/b/c", "Status": "Scheduled",
                "Checksum": "123456", "ChecksumType": "ADLER32"})
    req += op

    put = db.putRequest(req)
    self.assertEqual(put["OK"], True, put['Message'] if 'Message' in put else 'OK')
    reqID = put['Value']

    peek = db.peekRequest(reqID)
    self.assertEqual(peek["OK"], True, peek['Message'] if 'Message' in peek else 'OK')

    peek = peek["Value"]
    for op in peek:
      opId = op.OperationID

    getFTS = db.getScheduledRequest(opId)
    self.assertEqual(getFTS["OK"], True, "getScheduled failed")
    self.assertEqual(getFTS["Value"].RequestName, "FTSTest", "wrong request selected")

    delete = db.deleteRequest(reqID)
    self.assertEqual(delete["OK"], True, delete['Message'] if 'Message' in delete else 'OK')

  def test03Dirty(self):
    """ dirty records """
    db = RequestDB()

    r = Request()
    r.RequestName = "dirty"

    op1 = Operation({"Type": "ReplicateAndRegister", "TargetSE": "CERN-USER"})
    op1 += File({"LFN": "/a/b/c/1", "Status": "Scheduled",
                 "Checksum": "123456", "ChecksumType": "ADLER32"})

    op2 = Operation({"Type": "ReplicateAndRegister", "TargetSE": "CERN-USER"})
    op2 += File({"LFN": "/a/b/c/2", "Status": "Scheduled",
                 "Checksum": "123456", "ChecksumType": "ADLER32"})

    op3 = Operation({"Type": "ReplicateAndRegister", "TargetSE": "CERN-USER"})
    op3 += File({"LFN": "/a/b/c/3", "Status": "Scheduled",
                 "Checksum": "123456", "ChecksumType": "ADLER32"})

    r += op1
    r += op2
    r += op3

    put = db.putRequest(r)
    self.assertEqual(put["OK"], True, "1. putRequest failed: %s" % put.get("Message", ""))
    reqID = put['Value']

    r = db.getRequest(reqID)
    self.assertEqual(r["OK"], True, "1. getRequest failed: %s" % r.get("Message", ""))
    r = r["Value"]

    del r[0]
    self.assertEqual(len(r), 2, "1. len wrong")

    put = db.putRequest(r)
    self.assertEqual(put["OK"], True, "2. putRequest failed: %s" % put.get("Message", ""))
    reqID = put['Value']

    r = db.getRequest(reqID)
    self.assertEqual(r["OK"], True, "2. getRequest failed: %s" % r.get("Message", ""))

    r = r["Value"]
    self.assertEqual(len(r), 2, "2. len wrong")

    op4 = Operation({"Type": "ReplicateAndRegister", "TargetSE": "CERN-USER"})
    op4 += File({"LFN": "/a/b/c/4", "Status": "Scheduled",
                 "Checksum": "123456", "ChecksumType": "ADLER32"})

    r[0] = op4
    put = db.putRequest(r)
    self.assertEqual(put["OK"], True, "3. putRequest failed: %s" % put.get("Message", ""))
    reqID = put['Value']

    r = db.getRequest(reqID)
    self.assertEqual(r["OK"], True, "3. getRequest failed: %s" % r.get("Message", ""))
    r = r["Value"]

    self.assertEqual(len(r), 2, "3. len wrong")

    delete = db.deleteRequest(reqID)
    self.assertEqual(delete["OK"], True, delete['Message'] if 'Message' in delete else 'OK')


if __name__ == '__main__':
  suite = unittest.defaultTestLoader.loadTestsFromTestCase(ReqDBTestCase)
  suite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(ReqDB))
  suite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(ReqDBMix))
  testResult = unittest.TextTestRunner(verbosity=2).run(suite)
  sys.exit(not testResult.wasSuccessful())
