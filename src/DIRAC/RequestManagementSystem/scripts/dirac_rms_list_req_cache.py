#!/bin/env python
"""
List the number of requests in the caches of all the ReqProxyies

Usage:
  dirac-rms-list-req-cache [options]
"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

__RCSID__ = "$Id$"

import DIRAC
from DIRAC.Core.Base import Script
from DIRAC.Core.Utilities.DIRACScript import DIRACScript


@DIRACScript()
def main():
  Script.registerSwitch('', 'Full', '   Print full list of requests')
  from DIRAC.Core.Base.Script import parseCommandLine
  parseCommandLine()
  from DIRAC.RequestManagementSystem.Client.ReqClient import ReqClient

  fullPrint = False

  for switch in Script.getUnprocessedSwitches():
    if switch[0] == 'Full':
      fullPrint = True

  reqClient = ReqClient()

  for server, rpcClient in reqClient.requestProxies().items():
    DIRAC.gLogger.always("Checking request cache at %s" % server)
    reqCache = rpcClient.listCacheDir()
    if not reqCache['OK']:
      DIRAC.gLogger.error("Cannot list request cache", reqCache)
      continue
    reqCache = reqCache['Value']

    if fullPrint:
      DIRAC.gLogger.always("List of requests", reqCache)
    else:
      DIRAC.gLogger.always("Number of requests in the cache", len(reqCache))

  DIRAC.exit(0)


if __name__ == "__main__":
  main()
