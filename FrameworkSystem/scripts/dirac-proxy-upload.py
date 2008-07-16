#!/usr/bin/env python
########################################################################
# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/FrameworkSystem/scripts/dirac-proxy-upload.py,v 1.1 2008/07/16 11:35:41 acasajus Exp $
# File :   dirac-proxy-init.py
# Author : Adrian Casajus
########################################################################
__RCSID__   = "$Id: dirac-proxy-upload.py,v 1.1 2008/07/16 11:35:41 acasajus Exp $"
__VERSION__ = "$Revision: 1.1 $"

import sys
import getpass
from DIRACEnvironment import DIRAC
from DIRAC.Core.Base import Script

class Params:

  proxyLoc = False

  def setProxyLocation( self, arg ):
    self.proxyLoc = arg
    return DIRAC.S_OK()

  def showVersion( self, arg ):
    print "Version:"
    print " ", __RCSID__
    print " ", __VERSION__
    sys.exit(0)
    return DIRAC.S_OK()

  def debugMsg( self, msg ):
    if self.debug:
      print msg

params = Params()

Script.registerSwitch( "f:", "file=", "File to use as proxy", params.setProxyLocation )
Script.registerSwitch( "i", "version", "Print version", params.showVersion )

Script.addDefaultOptionValue( "LogLevel", "always" )
Script.parseCommandLine()

from DIRAC.FrameworkSystem.Client.ProxyManagerClient import gProxyManager
from DIRAC.Core.Security import Locations

if not params.proxyLoc:
  params.proxyLoc = Locations.getProxyLocation()

if not params.proxyLoc:
  print "Can't find any valid proxy"
  sys.exit(1)
print "Uploading proxy file %s" % params.proxyLoc

retVal = gProxyManager.uploadProxy( params.proxyLoc )
if not retVal[ 'OK' ]:
  print "Can't upload proxy:"
  print " ", retVal[ 'Message' ]
  sys.exit(1)
print "Proxy uploaded"
sys.exit(0)


