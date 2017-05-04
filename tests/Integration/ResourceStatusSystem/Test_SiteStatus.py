""" This is a test of the chain
    SiteStatus ->  ResourceStatusClient -> ResourceStatusDB
    It supposes that the DB is present, and that the service is running

    this is pytest!
"""

#pylint: disable=invalid-name,wrong-import-position,missing-docstring

from datetime import datetime

from DIRAC.Core.Base.Script import parseCommandLine
parseCommandLine()

from DIRAC.ResourceStatusSystem.Client.SiteStatus           import SiteStatus
from DIRAC.ResourceStatusSystem.Client.ResourceStatusClient import ResourceStatusClient

StClient = SiteStatus()
StClient.rssFlag = True

rsClient = ResourceStatusClient()
Datetime = datetime.now()

testSite = 'test1234.test.test'


def test_addAndRemove():

  # make sure that the test site is not presented in the db
  rsClient.deleteStatusElement('Site', 'Status', testSite)

  # add test site
  res = rsClient.insertStatusElement('Site', 'Status', testSite, 'all',
                                     'Active', 'Site', 'Synchronized', Datetime,
                                     Datetime, 'tokenOwner', Datetime)
  assert res['OK'] is True

  # TEST getSites
  # ...............................................................................

  result = StClient.getSites()
  assert result['OK'] is True

  assert testSite in result['Value']

  # TEST getSiteStatuses
  # ...............................................................................

  result = StClient.getSiteStatuses( [ testSite ] )
  assert result['OK'] is True

  assert result['Value'][testSite] == "Active"

  # TEST getUsableSites
  # ...............................................................................

  result = StClient.getUsableSites( [testSite] )
  assert result['OK'] is True

  assert result['Value'][0] == testSite

  # TEST isUsableSite
  # ...............................................................................

  result = StClient.isUsableSite(testSite)
  assert result['OK'] is True
  assert result['Value'] is True

  # finally delete the test site
  res = rsClient.deleteStatusElement('Site', 'Status', testSite)
  assert res['OK'] is True
