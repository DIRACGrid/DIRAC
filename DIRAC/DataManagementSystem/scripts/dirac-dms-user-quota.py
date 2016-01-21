#!/usr/bin/env python
########################################################################
# $HeadURL$
########################################################################
__RCSID__   = "$Id$"

from DIRAC.Core.Base import Script

Script.setUsageMessage("""
Get the currently defined user data volume quotas

Usage:
   %s [options]
""" % Script.scriptName)

Script.parseCommandLine( ignoreErrors = False )

import DIRAC
from DIRAC import gLogger, gConfig
from DIRAC.Core.Security.ProxyInfo import getProxyInfo

res = getProxyInfo( False, False )
if not res['OK']:
  gLogger.error( "Failed to get client proxy information.", res['Message'] )
  DIRAC.exit( 2 )
proxyInfo = res['Value']
username = proxyInfo['username']

try:
  quota = gConfig.getValue('/Registry/DefaultStorageQuota', 0. )
  quota = gConfig.getValue('/Registry/Users/%s/Quota' % username, quota )
  gLogger.notice('Current quota found to be %.1f GB' % quota)
  DIRAC.exit(0)
except Exception,x:
  gLogger.exception("Failed to convert retrieved quota",'',x)
  DIRAC.exit(-1)
