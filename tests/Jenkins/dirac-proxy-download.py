#!/usr/bin/env python
""" Get a proxy from the proxy manager
"""

from DIRAC.Core.Base import Script

Script.setUsageMessage( '\n'.join( [ __doc__.split( '\n' )[1],
                                     'Usage:',
                                     '  %s [option|cfgFile] UserName Role' % Script.scriptName,
                                     'Arguments:',
                                     '  UserName: User DN'] ) )

Script.registerSwitch( 'R:', 'role=', "set the User DN." )

Script.parseCommandLine()

for unprocSw in Script.getUnprocessedSwitches():
  if unprocSw[0] in ( "R", "role" ):
    role = unprocSw[1]

args = Script.getPositionalArgs()
dn = ' '.join( args )

import os
uid = os.getuid()
from DIRAC.FrameworkSystem.Client.ProxyManagerClient        import gProxyManager

res = gProxyManager.downloadProxyToFile( dn, role,
                                         limited = False, requiredTimeLeft = 1200,
                                         cacheTime = 43200, filePath = '/tmp/x509up_u%s' % uid, proxyToConnect = False,
                                         token = False )

if not res['OK']:
  print "Error downloading proxy", res['Message']
  exit( 1 )
