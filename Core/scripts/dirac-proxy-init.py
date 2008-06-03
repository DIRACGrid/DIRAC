#!/usr/bin/env python
########################################################################
# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/Core/scripts/Attic/dirac-proxy-init.py,v 1.1 2008/06/03 10:27:52 acasajus Exp $
# File :   dirac-proxy-init.py
# Author : Adrian Casajus
########################################################################
__RCSID__   = "$Id: dirac-proxy-init.py,v 1.1 2008/06/03 10:27:52 acasajus Exp $"
__VERSION__ = "$Revision: 1.1 $"

import sys
from DIRACEnvironment import DIRAC
from DIRAC.Core.Base import Script

class Params:

  proxyLifeTime = 86400
  diracGroup = False
  proxyStrength = 1024
  limitedProxy = False
  debug = False
  certLoc = False
  keyLoc = False
  proxyLoc = False

  def setProxyLifeTime( self, arg ):
    try:
      fields = [ f.strip() for f in arg.split(":") ]
      self.proxyLifeTime = fields[0] * 3600 + fields[1] * 60
    except:
      print "Can't parse %s time! Is it a HH:MM?" % arg
      return DIRAC.S_ERROR( "Can't parse time argument" )
    return DIRAC.S_OK()

  def setDIRACGroup( self, arg ):
    self.diracGroup = arg
    return DIRAC.S_OK()

  def setProxyStrength( self, arg ):
    try:
      self.proxyStrength = int( arg )
    except:
      print "Can't parse %s bits! Is it a number?" % arg
      return DIRAC.S_ERROR( "Can't parse strength argument" )
    return DIRAC.S_OK()

  def setProxyLimited( self, arg ):
    self.limitedProxy = True
    return DIRAC.S_OK()

  def setDebug( self, arg ):
    print "Enabling debug output"
    self.debug = True
    return DIRAC.S_OK()

  def setCertLocation( self, arg ):
    self.certLoc = arg
    return DIRAC.S_OK()

  def setKeyLocation( self, arg ):
    self.keyLoc = arg
    return DIRAC.S_OK()

  def setProxyLocation( self, arg ):
    self.proxyLoc = arg
    return DIRAC.S_OK()

params = Params()

Script.registerSwitch( "v:", "valid=", "Valid HH:MM for the proxy. By default is 24 hours", params.setProxyLifeTime )
Script.registerSwitch( "g:", "group=", "DIRAC Group to embed in the proxy", params.setDIRACGroup )
Script.registerSwitch( "b:", "strength=", "Set the proxy strength in bytes", params.setProxyStrength )
Script.registerSwitch( "l", "limited", "Generate a limited proxy", params.setProxyLimited )
Script.registerSwitch( "d", "debug", "Enable debug output", params.setDebug )
Script.registerSwitch( "c:", "cert=", "File to use as user certificate", params.setCertLocation )
Script.registerSwitch( "k:", "key=", "File to use as user key", params.setKeyLocation )
Script.registerSwitch( "u:", "out=", "File to write as proxy", params.setProxyLocation )

Script.disableCS()
Script.parseCommandLine()

from DIRAC.Core import Security

certLoc = params.certLoc
keyLoc = params.keyLoc
if not certLoc or not keyLoc:
  cakLoc = Security.getCertificateAndKeyLocation()
  if not cakLoc:
    print "Can't find user certificate and key"
    sys.exit(1)
  if not certLoc:
    certLoc = cakLoc[0]
  if not keyLoc:
    keyLoc = cakLoc[1]

proxyLoc = params.proxyLoc
if not proxyLoc:
  proxyLoc = Security.getDefaultProxyLocation()

if params.debug:
  print "User cert is %s" % certLoc
  print "User key  is %s" % keyLoc
  print "Proxy will be written to %s" % proxyLoc
  if params.diracGroup:
    print "DIRAC Group will be set to %s" % params.diracGroup
  else:
    print "No DIRAC Group will be set"
  print "Proxy strength will be %s" % params.proxyStrength
  if params.limitedProxy:
    print "Proxy will be limited"

chain = Security.X509Chain()
#Load user cert and key
retVal = chain.loadChainFromFile( certLoc )
if not retVal[ 'OK' ]:
  print "Can't load %s" % certLoc
  sys.exit(1)
retVal = chain.loadKeyFromFile( keyLoc )
if not retVal[ 'OK' ]:
  print "Can't load %s" % keyLoc
  sys.exit(1)

retVal = chain.generateProxyToFile( proxyLoc,
                                    params.proxyLifeTime,
                                    params.diracGroup,
                                    params.proxyStrength,
                                    params.limitedProxy )

if not retVal[ 'OK' ]:
  print "Couldn't generate proxy: %s" % retVal[ 'Message' ]
  sys.exit(1)
sys.exit(0)

