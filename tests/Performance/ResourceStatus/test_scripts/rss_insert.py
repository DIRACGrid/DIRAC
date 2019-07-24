"""
Performance test created using multi-mechnize to analyze
performance of ResourceStatusService.

rss_select runs concurrently wirh rss_insert in this test

Requires that the ResourceStatusHandler and ResourceStatusDB are up and running
"""

import time
import random
import datetime

from DIRAC.ResourceStatusSystem.Client.ResourceStatusClient import ResourceStatusClient
from DIRAC.Core.Base.Script import parseCommandLine
parseCommandLine()


class Transaction(object):

  def __init__(self):
    self.custom_timers = {}
    self.rssClient = ResourceStatusClient()

    # generate a random SiteName from "TestSite1", to "TestSite<numSites>"
    numSites = 10
    self.siteName = 'TestSite' + str(random.randint(1, numSites))

  def run(self):

    currentDateTime = datetime.datetime.utcnow()

    start_time = time.time()
    # insert record into RSSDB
    res = self.rssClient.insertStatusElement('Site', 'Status', self.siteName, 'statusType',
                                             'Active', 'elementType', 'reason', currentDateTime,
                                             currentDateTime, 'tokenOwner', currentDateTime)
    end_time = time.time()

    assert res['OK'] is True
    latency = end_time - start_time
    self.custom_timers['rss_insert'] = latency


if __name__ == '__main__':
  trans = Transaction()
  trans.run()
