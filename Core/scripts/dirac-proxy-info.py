#!/usr/bin/env python
########################################################################
# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/Core/scripts/Attic/dirac-proxy-info.py,v 1.4 2008/06/06 12:32:03 acasajus Exp $
# File :   dirac-proxy-init.py
# Author : Adrian Casajus
########################################################################
__RCSID__   = "$Id: dirac-proxy-info.py,v 1.4 2008/06/06 12:32:03 acasajus Exp $"
__VERSION__ = "$Revision: 1.4 $"

import sys
import os.path
from DIRACEnvironment import DIRAC
from DIRAC.Core.Base import Script


class Params:

  debug = False
  proxyLoc = False
  checkExists = False

  def setDebug( self, arg ):
    print "Enabling debug output"
    self.debug = True
    return DIRAC.S_OK()

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

params = Params()

Script.registerSwitch( "d", "debug", "Enable debug output", params.setDebug )
Script.registerSwitch( "f:", "file=", "File to use as user key", params.setProxyLocation )
Script.registerSwitch( "i", "version", "Print version", params.showVersion )


Script.disableCS()
Script.parseCommandLine()

from DIRAC.Core import Security

proxyLoc = params.proxyLoc
if not proxyLoc:
  proxyLoc = Security.Locations.getProxyLocation()

if params.debug:
  print "Proxy file: %s" % proxyLoc
if not proxyLoc:
  print "Can't find any valid proxy"
  sys.exit(1)

chain = Security.X509Chain()
retVal = chain.loadChainFromFile( proxyLoc )
if not retVal[ 'OK' ]:
  print "Can't load %s: %s" % ( proxyLoc, retVal[ 'Message' ] )
  sys.exit(1)
retVal = chain.loadKeyFromFile( proxyLoc )
print retVal
if not retVal[ 'OK' ]:
  print "Can't load %s: %s" % ( proxyLoc, retVal[ 'Message' ] )
  sys.exit(1)

lastCert = chain.getCertInChain()['Value']
mainCert = chain.getCertInChain(-1)['Value']
print "subject     : %s" % lastCert.getSubjectDN()[ 'Value' ]
print "issuer      : %s" % lastCert.getIssuerDN()[ 'Value' ]
print "identity    : %s" % mainCert.getSubjectDN()[ 'Value' ]
print "path        : %s" % proxyLoc
group = chain.getDIRACGroup()[ 'Value' ]
if not group:
  group = '<not set>'
print "DIRAC Group : %s" % group
secs = chain.getRemainingSecs()[ 'Value' ]
hours = int( secs /  3600 )
secs -= hours * 3600
mins = int( secs / 60 )
secs -= mins * 60
print "time left   : %02d:%02d:%02d" % ( hours, mins, secs )
sys.exit(0)
