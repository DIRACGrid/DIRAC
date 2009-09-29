#!/usr/bin/env python
########################################################################
# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/DataManagementSystem/scripts/dirac-dms-user-quota.py,v 1.1 2009/09/29 13:29:12 acsmith Exp $
########################################################################
__RCSID__   = "$Id: dirac-dms-user-quota.py,v 1.1 2009/09/29 13:29:12 acsmith Exp $"
__VERSION__ = "$Revision: 1.1 $"
import DIRAC
from DIRAC.Core.Base import Script
Script.parseCommandLine( ignoreErrors = False )
from DIRAC import gLogger, gConfig
from DIRAC.Core.Security.Misc import getProxyInfo

res = getProxyInfo(False,False)
if not res['OK']:
  gLogger.error("Failed to get client proxy information.",res['Message'])
  DIRAC.exit(2)
proxyInfo = res['Value']
username = proxyInfo['username']

try:
  quota = gConfig.getValue('/Security/Users/%s/Quota' % username,0)
  if not quota:
    quota = gConfig.getValue('/Security/DefaultStorageQuota')
  quota = float(quota)
  gLogger.info('Current quota found to be %.1f MB' % quota)
  DIRAC.exit(0)
except Exception,x:
  gLogger.exception("Failed to convert retrieved quota",'',x)
  DIRAC.exit(-1)
