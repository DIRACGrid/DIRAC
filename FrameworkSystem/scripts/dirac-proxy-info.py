#!/usr/bin/env python
########################################################################
# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/FrameworkSystem/scripts/dirac-proxy-info.py,v 1.2 2008/07/09 10:18:50 rgracian Exp $
# File :   dirac-proxy-init.py
# Author : Adrian Casajus
########################################################################
__RCSID__   = "$Id: dirac-proxy-info.py,v 1.2 2008/07/09 10:18:50 rgracian Exp $"
__VERSION__ = "$Revision: 1.2 $"

import sys
import os.path
from DIRACEnvironment import DIRAC
from DIRAC.Core.Base import Script


class Params:

  proxyLoc = False
  checkExists = False
  showUsername = False

  def showVersion( self, arg ):
    print "Version:"
    print " ", __RCSID__
    print " ", __VERSION__
    sys.exit(0)
    return DIRAC.S_OK()

  def setProxyLocation( self, arg ):
    self.proxyLoc = arg
    return DIRAC.S_OK()

  def checkExists( self, arg ):
    self.checkExists = True
    return DIRAC.S_OK()
  
  def enableShowUsername( self, arg ):
    self.showUsername = True
    return DIRAC.S_OK()

params = Params()

Script.registerSwitch( "f:", "file=", "File to use as user key", params.setProxyLocation )
Script.registerSwitch( "i", "version", "Print version", params.showVersion )
Script.registerSwitch( "u", "username", "Show username if possible", params.enableShowUsername )


Script.disableCS()
Script.parseCommandLine()

if params.showUsername:
  retVal = Script.enableCS()
  if not retVal[ 'OK' ]:
    print "Cannot contact CS to get user list"

from DIRAC.Core.Security.Misc import getProxyInfo

result = getProxyInfo( params.proxyLoc, params.showUsername )
if not result[ 'OK' ]:
  print "Error: %s" % result[ 'Message' ]
print result[ 'Value' ],
sys.exit(0)