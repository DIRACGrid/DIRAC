""" This is a test of the chain
    ReqClient -> ReqManagerHandler -> ReqDB

    It supposes that the DB is present, and that the service is running
"""

# pylint: disable=invalid-name,wrong-import-position

from __future__ import print_function
from __future__ import absolute_import
from __future__ import division

import copy
import unittest
import sys
import six

from DIRAC.Core.Base.Script import parseCommandLine
parseCommandLine()


from DIRAC import gLogger

from DIRAC.Core.Security.ProxyInfo import getProxyInfo
from DIRAC.Core.Security.Properties import FULL_DELEGATION, LIMITED_DELEGATION

from DIRAC.RequestManagementSystem.Client.Request import Request
from DIRAC.RequestManagementSystem.Client.Operation import Operation
from DIRAC.RequestManagementSystem.Client.File import File
from DIRAC.RequestManagementSystem.Client.ReqClient import ReqClient


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

  def tearDown(self):
    """ clean up """
    del self.request
    del self.operation
    del self.file
    del self.jsonStr


class ReqClientMix(ReqClientTestCase):
  def _checkSummary(self, initial, changes):
    """Check if getDBSummary has be updated as expected

    :param initial: Return value from ``self.requestClient.getDBSummary()``
                    before the test was started.
    :param changes: The expected changes to the database summary. Tuple
                    consisting of ``(parentKey, key, state, delta)``.
    """
    expected = copy.deepcopy(initial)
    for parent, key, state, delta in changes:
      if parent is None:
        d = expected
      else:
        d = expected.setdefault(parent, {})
      d.setdefault(key, {})
      d[key].setdefault(state, 0)
      d[key][state] += delta

    res = self.requestClient.getDBSummary()
    self.assertTrue(res['OK'])
    self.assertEqual(res["Value"], expected)

  def test01fullChain(self):
    ret = self.requestClient.getDBSummary()
    self.assertTrue(ret['OK'])
    initialSummary = ret["Value"]

    put = self.requestClient.putRequest(self.request)
    self.assertTrue(put['OK'], put)

    self.assertTrue(isinstance(put['Value'], six.integer_types))
    reqID = put['Value']

    # summary
    ret = self.requestClient.getDBSummary()
    self.assertTrue(ret['OK'])
    self._checkSummary(initialSummary, [
        ("Operation", "ReplicateAndRegister", "Waiting", 1),
        (None, "Request", "Waiting", 1),
        (None, "File", "Waiting", 2),
    ])

    get = self.requestClient.getRequest(reqID)
    self.assertTrue(get['OK'])
    self.assertEqual(isinstance(get['Value'], Request), True)
    # # summary - the request became "Assigned"
    self._checkSummary(initialSummary, [
        ("Operation", "ReplicateAndRegister", "Waiting", 1),
        (None, "Request", "Assigned", 1),
        (None, "File", "Waiting", 2),
    ])

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
    ret = self.requestClient.getDBSummary()
    self.assertTrue(ret['OK'])
    self._checkSummary(initialSummary, [
        ("Operation", "ReplicateAndRegister", "Waiting", 2),
        (None, "Request", "Waiting", 1),
        (None, "Request", "Assigned", 1),
        (None, "File", "Waiting", 4),
    ])

    delete = self.requestClient.deleteRequest(reqID)
    self.assertEqual(delete['OK'], True, delete['Message'] if 'Message' in delete else 'OK')
    delete = self.requestClient.deleteRequest(reqID2)
    self.assertEqual(delete['OK'], True, delete['Message'] if 'Message' in delete else 'OK')

    # # should be empty now
    ret = self.requestClient.getDBSummary()
    self.assertTrue(ret['OK'])
    self.assertEqual(ret['Value'], initialSummary)

  def test02Authorization(self):
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
  suite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(ReqClientMix))
  testResult = unittest.TextTestRunner(verbosity=2).run(suite)
  sys.exit(not testResult.wasSuccessful())
