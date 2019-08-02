"""
Performance test created using multi-mechnize to analyze
performance of ResourceStatusService.

rss_select runs concurrently wirh rss_insert in this test

Requires that the ResourceStatusHandler and ResourceStatusDB are up and running
"""

import time
import random

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

    start_time = time.time()
    # select all SiteStatusElements with name as self.siteName
    res = self.rssClient.selectStatusElement('Site', 'Status', self.siteName)
    end_time = time.time()

    assert res['OK'] is True
    latency = end_time - start_time
    self.custom_timers['rss_select'] = latency


if __name__ == '__main__':
  trans = Transaction()
  trans.run()
