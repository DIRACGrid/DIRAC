""" This is a test of the chain
    SiteStatus ->  ResourceStatusClient -> ResourceStatusDB
    It supposes that the DB is present, and that the service is running
"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

# pylint: disable=invalid-name,wrong-import-position

from datetime import datetime
import unittest
import sys

from DIRAC.Core.Base.Script import parseCommandLine
parseCommandLine()

from DIRAC.ResourceStatusSystem.Client.SiteStatus import SiteStatus
from DIRAC.ResourceStatusSystem.Client.ResourceStatusClient import ResourceStatusClient


Datetime = datetime.now()

testSite = 'test1234.test.test'


class TestClientSiteStatusTestCase(unittest.TestCase):

  def setUp(self):
    self.rsClient = ResourceStatusClient()
    self.stClient = SiteStatus()
    self.stClient.rssFlag = True

  def tearDown(self):
    pass


class ClientChain(TestClientSiteStatusTestCase):

  def test_addAndRemove(self):

    # make sure that the test sites are not presented in the db
    self.rsClient.deleteStatusElement('Site', 'Status', testSite)
    self.rsClient.deleteStatusElement('Site', 'Status', 'testActive1.test.test')
    self.rsClient.deleteStatusElement('Site', 'Status', 'testActive.test.test')
    self.rsClient.deleteStatusElement('Site', 'Status', 'testBanned.test.test')

    # add test site
    res = self.rsClient.insertStatusElement('Site', 'Status', testSite, 'all',
                                            'Active', 'Site', 'Synchronized', Datetime,
                                            Datetime, 'tokenOwner', Datetime)
    self.assertTrue(res['OK'])
    self.stClient.rssCache.refreshCache()

    # TEST getSites
    # ...............................................................................

    result = self.stClient.getSites()
    self.assertTrue(result['OK'])

    self.assertTrue(testSite in result['Value'])

    # TEST getSiteStatuses
    # ...............................................................................

    result = self.stClient.getSiteStatuses([testSite])
    self.assertTrue(result['OK'])

    self.assertEqual(result['Value'][testSite], "Active")

    # TEST getUsableSites
    # ...............................................................................

    result = self.stClient.getUsableSites([testSite])
    self.assertTrue(result['OK'])

    self.assertEqual(result['Value'][0], testSite)

    # finally delete the test site
    res = self.rsClient.deleteStatusElement('Site', 'Status', testSite)
    self.assertTrue(res['OK'])

    # ...............................................................................
    # adding some more test sites and more complex tests
    # ...............................................................................

    res = self.rsClient.insertStatusElement('Site', 'Status', 'testActive.test.test', 'all',
                                            'Active', 'Site', 'Synchronized', Datetime,
                                            Datetime, 'tokenOwner', Datetime)
    self.assertTrue(res['OK'])

    res = self.rsClient.insertStatusElement('Site', 'Status', 'testActive1.test.test', 'all',
                                            'Active', 'Site', 'Synchronized', Datetime,
                                            Datetime, 'tokenOwner', Datetime)
    self.assertTrue(res['OK'])

    res = self.rsClient.insertStatusElement('Site', 'Status', 'testBanned.test.test', 'all',
                                            'Banned', 'Site', 'Synchronized', Datetime,
                                            Datetime, 'tokenOwner', Datetime)
    self.assertTrue(res['OK'])
    self.stClient.rssCache.refreshCache()

    # TEST getSites
    # ...............................................................................

    result = self.stClient.getSites()
    self.assertTrue(result['OK'])

    self.assertTrue('testActive1.test.test' in result['Value'])
    self.assertTrue('testActive.test.test' in result['Value'])
    self.assertFalse('testBanned.test.test' in result['Value'])

    # TEST getSites
    # ...............................................................................

    result = self.stClient.getSites('All')
    self.assertTrue(result['OK'])

    self.assertTrue('testActive1.test.test' in result['Value'])
    self.assertTrue('testActive.test.test' in result['Value'])
    self.assertTrue('testBanned.test.test' in result['Value'])

    # TEST getUsableSites
    # ...............................................................................

    result = self.stClient.getUsableSites()
    self.assertTrue(result['OK'])

    self.assertTrue('testActive1.test.test' in result['Value'])
    self.assertTrue('testActive.test.test' in result['Value'])

    # setting a status
    result = self.stClient.setSiteStatus('testBanned.test.test', 'Probing')
    self.assertTrue(result['OK'])
    self.stClient.rssCache.refreshCache()

    result = self.stClient.getSites('Probing')
    self.assertTrue(result['OK'])
    self.assertTrue('testBanned.test.test' in result['Value'])
    self.assertFalse('testActive.test.test' in result['Value'])


if __name__ == '__main__':
  suite = unittest.defaultTestLoader.loadTestsFromTestCase(TestClientSiteStatusTestCase)
  suite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(ClientChain))
  testResult = unittest.TextTestRunner(verbosity=2).run(suite)
  sys.exit(not testResult.wasSuccessful())
