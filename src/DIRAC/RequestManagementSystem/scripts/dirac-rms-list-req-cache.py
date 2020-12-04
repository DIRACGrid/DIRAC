#!/bin/env python
""" List the number of requests in the caches of all the ReqProxyies """
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
__RCSID__ = "$Id$"
import DIRAC

from DIRAC.Core.Base import Script
from DIRAC.Core.Base.Script import parseCommandLine

Script.registerSwitch('', 'Full', '   Print full list of requests')
Script.setUsageMessage('\n'.join([__doc__, 'Usage:', ' %s [option|cfgfile]' % Script.scriptName]))
parseCommandLine()
from DIRAC.RequestManagementSystem.Client.ReqClient import ReqClient


if __name__ == "__main__":

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
