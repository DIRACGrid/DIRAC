#!/usr/bin/env python
########################################################################
# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/Core/scripts/Attic/dirac-proxy-init.py,v 1.6 2008/06/10 12:38:09 acasajus Exp $
# File :   dirac-proxy-init.py
# Author : Adrian Casajus
########################################################################
__RCSID__   = "$Id: dirac-proxy-init.py,v 1.6 2008/06/10 12:38:09 acasajus Exp $"
__VERSION__ = "$Revision: 1.6 $"

import sys
import getpass
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
  checkWithCS = True

  def setProxyLifeTime( self, arg ):
    try:
      fields = [ f.strip() for f in arg.split(":") ]
      self.proxyLifeTime = int( fields[0] ) * 3600 + int( fields[1] ) * 60
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

  def setDisableCSCheck( self, arg ):
    self.checkWithCS = False
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

Script.registerSwitch( "v:", "valid=", "Valid HH:MM for the proxy. By default is 24 hours", params.setProxyLifeTime )
Script.registerSwitch( "g:", "group=", "DIRAC Group to embed in the proxy", params.setDIRACGroup )
Script.registerSwitch( "b:", "strength=", "Set the proxy strength in bytes", params.setProxyStrength )
Script.registerSwitch( "l", "limited", "Generate a limited proxy", params.setProxyLimited )
Script.registerSwitch( "d", "debug", "Enable debug output", params.setDebug )
Script.registerSwitch( "c:", "cert=", "File to use as user certificate", params.setCertLocation )
Script.registerSwitch( "k:", "key=", "File to use as user key", params.setKeyLocation )
Script.registerSwitch( "u:", "out=", "File to write as proxy", params.setProxyLocation )
Script.registerSwitch( "x", "nocs", "Disable CS check", params.setDisableCSCheck )
Script.registerSwitch( "i", "version", "Print version", params.showVersion )
Script.addDefaultOptionValue( "LogLevel", "always" )

Script.disableCS()
Script.parseCommandLine()

userPasswd = getpass.getpass( "Enter Certificate password:" )

from DIRAC.Core import Security

certLoc = params.certLoc
keyLoc = params.keyLoc
if not certLoc or not keyLoc:
  cakLoc = Security.Locations.getCertificateAndKeyLocation()
  if not cakLoc:
    print "Can't find user certificate and key"
    sys.exit(1)
  if not certLoc:
    certLoc = cakLoc[0]
  if not keyLoc:
    keyLoc = cakLoc[1]

proxyLoc = params.proxyLoc
if not proxyLoc:
  proxyLoc = Security.Locations.getDefaultProxyLocation()

if params.debug:
  h = int( params.proxyLifeTime / 3600 )
  m = int( params.proxyLifeTime / 60 )- h * 60
  print "Proxy lifetime will be %02d:%02d" % ( h, m )
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
retVal = chain.loadKeyFromFile( keyLoc, password = userPasswd )
if not retVal[ 'OK' ]:
  print "Can't load %s" % keyLoc
  sys.exit(1)

if params.checkWithCS:
  retVal = chain.generateProxyToFile( proxyLoc,
                                      params.proxyLifeTime,
                                      strength =params.proxyStrength,
                                      limited = params.limitedProxy )

  params.debugMsg( "Contacting CS..." )

  retVal = Script.enableCS()
  if not retVal[ 'OK' ]:
    print "Can't contact DIRAC CS: %s" % retVal[ 'Message' ]
    print "Aborting..."
    sys.exit(1)
  if not params.diracGroup:
    params.diracGroup = Security.CS.getDefaultUserGroup()
  userDN = chain.getCertInChain( -1 )['Value'].getSubjectDN()['Value']
  params.debugMsg( "Checking DN %s" % userDN )
  retVal = Security.CS.getUsernameForDN( userDN )
  if not retVal[ 'OK' ]:
    print "DN %s is not registered" % userDN
    sys.exit(1)
  username = retVal[ 'Value' ]
  params.debugMsg( "Username is %s" % username )
  retVal = Security.CS.getGroupsForUser( username )
  if not retVal[ 'OK' ]:
    print "User %s has no groups defined" % username
    sys.exit(1)
  groups = retVal[ 'Value' ]
  if params.diracGroup not in groups:
    print "Requested group %s is not valid for user %s" % ( params.diracGroup, username )
    sys.exit(1)
  params.debugMsg( "Creating proxy for %s@%s (%s)" % ( username, params.diracGroup, userDN ) )

retVal = chain.generateProxyToFile( proxyLoc,
                                    params.proxyLifeTime,
                                    params.diracGroup,
                                    strength =params.proxyStrength,
                                    limited = params.limitedProxy )

if not retVal[ 'OK' ]:
  print "Couldn't generate proxy: %s" % retVal[ 'Message' ]
  sys.exit(1)
sys.exit(0)

