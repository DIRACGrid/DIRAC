""" This is a test of the chain
    ReqClient -> ReqManagerHandler -> ReqDB

    It supposes that the DB is present, and that the service is running
"""

# pylint: disable=invalid-name,wrong-import-position

from DIRAC.Core.Base.Script import parseCommandLine
parseCommandLine()

import unittest
import sys

from DIRAC import gLogger

from DIRAC.Core.Security.ProxyInfo import getProxyInfo
from DIRAC.Core.Security.Properties import FULL_DELEGATION, LIMITED_DELEGATION

from DIRAC.RequestManagementSystem.Client.Request import Request
from DIRAC.RequestManagementSystem.Client.Operation import Operation
from DIRAC.RequestManagementSystem.Client.File import File
from DIRAC.RequestManagementSystem.Client.ReqClient import ReqClient


from DIRAC.RequestManagementSystem.DB.RequestDB import RequestDB

import time


class ReqClientTestCase(unittest.TestCase):
  """
  .. class:: ReqClientTestCase

  """

  def setUp(self):
    """ test case set up """

    gLogger.setLevel('INFO')

    self.file = File()
    self.file.LFN = "/lhcb/user/c/cibak/testFile"
    self.file.Checksum = "123456"
    self.file.ChecksumType = "ADLER32"

    self.file2 = File()
    self.file2.LFN = "/lhcb/user/f/fstagni/testFile"
    self.file2.Checksum = "654321"
    self.file2.ChecksumType = "ADLER32"

    self.operation = Operation()
    self.operation.Type = "ReplicateAndRegister"
    self.operation.TargetSE = "CERN-USER"
    self.operation.addFile(self.file)
    self.operation.addFile(self.file2)

    proxyInfo = getProxyInfo()['Value']
    self.request = Request()
    self.request.RequestName = "RequestManagerHandlerTests"
    self.request.OwnerDN = proxyInfo['identity']
    self.request.OwnerGroup = proxyInfo['group']
    self.request.JobID = 123
    self.request.addOperation(self.operation)

    # # JSON representation of a whole request
    self.jsonStr = self.request.toJSON()['Value']
    # # request client
    self.requestClient = ReqClient()

    self.stressRequests = 1000
    self.bulkRequest = 1000

  def tearDown(self):
    """ clean up """
    del self.request
    del self.operation
    del self.file
    del self.jsonStr


class ReqDB(ReqClientTestCase):

  def test_db(self):

    # # empty DB at that stage
    ret = RequestDB().getDBSummary()
    self.assertEqual(ret,
                     {'OK': True,
                      'Value': {'Operation': {}, 'Request': {}, 'File': {}}})


class ReqClientMix(ReqClientTestCase):

  def test01fullChain(self):
    put = self.requestClient.putRequest(self.request)
    self.assertTrue(put['OK'], put)

    self.assertEqual(type(put['Value']), long)
    reqID = put['Value']

    # # summary
    ret = RequestDB().getDBSummary()
    self.assertEqual(ret,
                     {'OK': True,
                      'Value': {'Operation': {'ReplicateAndRegister': {'Waiting': 1}},
                                'Request': {'Waiting': 1},
                                'File': {'Waiting': 2}}})

    get = self.requestClient.getRequest(reqID)
    self.assertTrue(get['OK'])
    self.assertEqual(isinstance(get['Value'], Request), True)
    # # summary - the request became "Assigned"
    res = RequestDB().getDBSummary()
    self.assertEqual(res,
                     {'OK': True,
                      'Value': {'Operation': {'ReplicateAndRegister': {'Waiting': 1}},
                                'Request': {'Assigned': 1},
                                'File': {'Waiting': 2}}})

    res = self.requestClient.getRequestInfo(reqID)
    self.assertEqual(res['OK'], True, res['Message'] if 'Message' in res else 'OK')

    res = self.requestClient.getRequestFileStatus(reqID, self.file.LFN)
    self.assertEqual(res['OK'], True, res['Message'] if 'Message' in res else 'OK')

    res = self.requestClient.getRequestFileStatus(reqID, [self.file.LFN])
    self.assertEqual(res['OK'], True, res['Message'] if 'Message' in res else 'OK')

    res = self.requestClient.getDigest(reqID)
    self.assertEqual(res['OK'], True, res['Message'] if 'Message' in res else 'OK')

    res = self.requestClient.readRequestsForJobs([123])
    self.assertEqual(res['OK'], True, res['Message'] if 'Message' in res else 'OK')
    self.assertTrue(isinstance(res['Value']['Successful'][123], Request))

    proxyInfo = getProxyInfo()['Value']
    # Adding new request
    request2 = Request()
    request2.RequestName = "RequestManagerHandlerTests-2"
    self.request.OwnerDN = proxyInfo['identity']
    self.request.OwnerGroup = proxyInfo['group']
    request2.JobID = 456
    request2.addOperation(self.operation)

    # # update
    res = self.requestClient.putRequest(request2)
    self.assertEqual(res['OK'], True, res['Message'] if 'Message' in res else 'OK')
    reqID2 = res['Value']

    # # get summary again
    ret = RequestDB().getDBSummary()
    self.assertEqual(ret,
                     {'OK': True,
                      'Value': {'Operation': {'ReplicateAndRegister': {'Waiting': 2}},
                                'Request': {'Waiting': 1, 'Assigned': 1},
                                'File': {'Waiting': 4}}})

    delete = self.requestClient.deleteRequest(reqID)
    self.assertEqual(delete['OK'], True, delete['Message'] if 'Message' in delete else 'OK')
    delete = self.requestClient.deleteRequest(reqID2)
    self.assertEqual(delete['OK'], True, delete['Message'] if 'Message' in delete else 'OK')

    # # should be empty now
    ret = RequestDB().getDBSummary()
    self.assertEqual(ret,
                     {'OK': True,
                      'Value': {'Operation': {}, 'Request': {}, 'File': {}}})

  def test04Stress(self):
    """ stress test """

    db = RequestDB()

    reqIDs = []
    for i in xrange(self.stressRequests):
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
        print get["Message"]
      self.assertEqual(get["OK"], True, get['Message'] if 'Message' in get else 'OK')

    endTime = time.time()

    print "getRequest duration %s " % (endTime - startTime)

    for reqID in reqIDs:
      delete = db.deleteRequest(reqID)
      self.assertEqual(delete["OK"], True, delete['Message'] if 'Message' in delete else 'OK')

  def test04StressBulk(self):
    """ stress test bulk """

    db = RequestDB()

    reqIDs = []
    for i in xrange(self.stressRequests):
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

    for i in xrange(loops):
      get = db.getBulkRequests(self.bulkRequest, True)
      if "Message" in get:
        print get["Message"]
      self.assertEqual(get["OK"], True, "get failed")

      totalSuccessful += len(get["Value"])

    endTime = time.time()

    print "getRequests duration %s " % (endTime - startTime)

    self.assertEqual(
        totalSuccessful,
        self.stressRequests,
        "Did not retrieve all the requests: %s instead of %s" %
        (totalSuccessful,
         self.stressRequests))

    for reqID in reqIDs:
      delete = db.deleteRequest(reqID)
      self.assertEqual(delete["OK"], True, delete['Message'] if 'Message' in delete else 'OK')

  def test05Scheduled(self):
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

  def test06Dirty(self):
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

  def test07Authorization(self):
    """ Test whether request sets on behalf of others are rejected, unless done with Delegation properties
        This test is kind of stupid though, since we do the same thing than the server... not a real test !
    """

    request = Request({"RequestName": "unauthorized"})
    request.OwnerDN = 'NotMe'
    request.OwnerDN = 'AnotherGroup'
    op = Operation({"Type": "RemoveReplica", "TargetSE": "CERN-USER"})
    op += File({"LFN": "/lhcb/user/c/cibak/foo"})
    request += op
    res = self.requestClient.putRequest(request)
    credProperties = getProxyInfo()['Value']['groupProperties']

    # If the proxy with which we test has delegation, it should work
    if FULL_DELEGATION in credProperties or LIMITED_DELEGATION in credProperties:
      self.assertTrue(res['OK'], res)
      self.requestClient.deleteRequest(res['Value'])
    # otherwise no
    else:
      self.assertFalse(res['OK'], res)


if __name__ == '__main__':
  suite = unittest.defaultTestLoader.loadTestsFromTestCase(ReqClientTestCase)
  suite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(ReqDB))
  suite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(ReqClientMix))
  testResult = unittest.TextTestRunner(verbosity=2).run(suite)
  sys.exit(not testResult.wasSuccessful())
