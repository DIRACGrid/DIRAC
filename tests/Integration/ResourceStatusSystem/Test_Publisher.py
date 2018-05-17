""" This is a test of the PublisherHandler

    It supposes that the RSS DBs are present, and that the service is running
"""

# pylint: disable=invalid-name,wrong-import-position

import unittest

from DIRAC.Core.Base.Script import parseCommandLine
parseCommandLine()

from DIRAC.Core.DISET.RPCClient import RPCClient
from DIRAC import gLogger


class TestPublisherTestCase(unittest.TestCase):

  def setUp(self):
    self.publisher = RPCClient("ResourceStatus/Publisher")
    gLogger.setLevel('DEBUG')

  def tearDown(self):
    pass


class PublisherGet(TestPublisherTestCase):

  def test_get(self):
    res = self.publisher.getSites()
    self.assertTrue(res['OK'])

    res = self.publisher.getSitesResources(None)
    self.assertTrue(res['OK'])

    res = self.publisher.getElementStatuses('Site', None, None, None, None, None)
    self.assertTrue(res['OK'])

    res = self.publisher.getElementHistory('Site', None, None, None)
    self.assertTrue(res['OK'])

    res = self.publisher.getElementPolicies('Site', None, None)
    self.assertTrue(res['OK'])

    res = self.publisher.getNodeStatuses()
    self.assertTrue(res['OK'])

    res = self.publisher.getTree('', '')
    self.assertTrue(res['OK'])


if __name__ == '__main__':
  suite = unittest.defaultTestLoader.loadTestsFromTestCase(TestPublisherTestCase)
  suite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(PublisherGet))
  testResult = unittest.TextTestRunner(verbosity=2).run(suite)
