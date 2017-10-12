""" This is a test of the chain
    SiteStatus ->  ResourceStatusClient -> ResourceStatusDB
    It supposes that the DB is present, and that the service is running
"""

#pylint: disable=invalid-name,wrong-import-position,missing-docstring

from datetime import datetime
import unittest

from DIRAC.Core.Base.Script import parseCommandLine
parseCommandLine()

from DIRAC.ResourceStatusSystem.Client.SiteStatus           import SiteStatus
from DIRAC.ResourceStatusSystem.Client.ResourceStatusClient import ResourceStatusClient


Datetime = datetime.now()

testSite = 'test1234.test.test'

class TestClientSiteStatusTestCase( unittest.TestCase ):

  def setUp( self ):
    self.rsClient = ResourceStatusClient()
    self.stClient = SiteStatus()
    self.stClient.rssFlag = True

  def tearDown( self ):
    pass

class ClientChain( TestClientSiteStatusTestCase ):

  def test_addAndRemove(self):

    # make sure that the test site is not presented in the db
    self.rsClient.deleteStatusElement('Site', 'Status', testSite)

    # add test site
    res = self.rsClient.insertStatusElement('Site', 'Status', testSite, 'all',
                                            'Active', 'Site', 'Synchronized', Datetime,
                                            Datetime, 'tokenOwner', Datetime)
    self.assertTrue(res['OK'])

    # TEST getSites
    # ...............................................................................

    result = self.stClient.getSites()
    self.assertTrue(result['OK'])

    self.assertTrue( testSite in result['Value'] )

    # TEST getSiteStatuses
    # ...............................................................................

    result = self.stClient.getSiteStatuses( [ testSite ] )
    self.assertTrue(result['OK'])

    self.assertEqual( result['Value'][testSite], "Active")

    # TEST getUsableSites
    # ...............................................................................

    result = self.stClient.getUsableSites( [testSite] )
    self.assertTrue(result['OK'])

    self.assertEqual(result['Value'][0], testSite)

    # TEST isUsableSite
    # ...............................................................................

    result = self.stClient.isUsableSite(testSite)
    self.assertTrue(result['OK'])
    self.assertTrue(result['Value'])

    # finally delete the test site
    res = self.rsClient.deleteStatusElement('Site', 'Status', testSite)
    self.assertTrue(res['OK'])


if __name__ == '__main__':
  suite = unittest.defaultTestLoader.loadTestsFromTestCase( TestClientSiteStatusTestCase )
  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( ClientChain ) )
  testResult = unittest.TextTestRunner( verbosity = 2 ).run( suite )
