#!/usr/bin/env python
"""
Get a proxy from the proxy manager

Usage:
  dirac-proxy-download [options] ... DN

Arguments:
  DN: User DN
"""
from __future__ import print_function
from __future__ import absolute_import
from __future__ import division
import os

from DIRAC.Core.Base import Script
Script.registerSwitch('R:', 'role=', "set the User DN.")
Script.parseCommandLine()

for unprocSw in Script.getUnprocessedSwitches():
  if unprocSw[0] in ("R", "role"):
    role = unprocSw[1]

args = Script.getPositionalArgs()
dn = ' '.join(args)

uid = os.getuid()
from DIRAC.FrameworkSystem.Client.ProxyManagerClient import gProxyManager

print("Getting proxy for User DN: %s, User role %s" % (dn, role))

res = gProxyManager.downloadProxyToFile(dn, role,
                                        limited=False, requiredTimeLeft=1200,
                                        cacheTime=43200, filePath='/tmp/x509up_u%s' % uid, proxyToConnect=False,
                                        token=False)

if not res['OK']:
  print("Error downloading proxy", res['Message'])
  exit(1)
