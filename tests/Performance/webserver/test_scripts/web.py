#!/usr/bin/env python
"""
It is used to test the tornado web framework
"""

import time
import httplib


class Transaction(object):

  def __init__(self):
    self.custom_timers = {}
    self.conn = httplib.HTTPConnection("lhcb-cert-dirac.cern.ch")

  def run(self):
    # print len(datasets)
    start_time = time.time()
    self.conn.request("GET", "/DIRAC/s:LHCb-Certification/g:lhcb_prmgr/ExampleApp/getSelectionData")
    r1 = self.conn.getresponse()
    if r1.status != 200:
      print r1.status, r1.reason
    _ = r1.read()
    end_time = time.time()
    self.custom_timers['Tornado_ResponseTime'] = end_time - start_time


if __name__ == '__main__':
  trans = Transaction()
  trans.run()
  print trans.custom_timers
