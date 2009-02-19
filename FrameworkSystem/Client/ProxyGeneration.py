########################################################################
# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/FrameworkSystem/Client/ProxyGeneration.py,v 1.2 2009/02/19 19:18:55 acasajus Exp $
# File :   dirac-proxy-init.py
# Author : Adrian Casajus
########################################################################
__RCSID__   = "$Id: ProxyGeneration.py,v 1.2 2009/02/19 19:18:55 acasajus Exp $"
__VERSION__ = "$Revision: 1.2 $"

import sys
import getpass
from DIRACEnvironment import DIRAC
from DIRAC.Core.Base import Script
from DIRAC.Core.Utilities.NTP import getClockDeviation


class CLIParams:

  proxyLifeTime = 86400
  diracGroup = False
  proxyStrength = 1024
  limitedProxy = False
  strict = False
  debug = False
  certLoc = False
  keyLoc = False
  proxyLoc = False
  checkWithCS = True
  stdinPasswd = False
  userPasswd = ""
  checkClock = True

  def setProxyLifeTime( self, arg ):
    try:
      fields = [ f.strip() for f in arg.split(":") ]
      self.proxyLifeTime = int( fields[0] ) * 3600 + int( fields[1] ) * 60
    except:
      print "Can't parse %s time! Is it a HH:MM?" % arg
      return DIRAC.S_ERROR( "Can't parse time argument" )
    return DIRAC.S_OK()

  def setProxyRemainingSecs( self, arg ):
    self.proxyLifeTime = int( arg )
    return DIRAC.S_OK()

  def getProxyLifeTime( self ):
    hours = self.proxyLifeTime / 3600
    mins = self.proxyLifeTime / 60 - hours * 60
    return "%s:%s" % ( hours, mins )

  def getProxyRemainingSecs( self ):
    return self.proxyLifeTime

  def setDIRACGroup( self, arg ):
    self.diracGroup = arg
    return DIRAC.S_OK()

  def getDIRACGroup( self ):
    return self.diracGroup

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

  def setStdinPasswd( self, arg ):
    self.stdinPasswd = True
    return DIRAC.S_OK()

  def setStrict( self, arg ):
    self.strict = True
    return DIRAC.S_OK()

  def showVersion( self, arg ):
    print "Version:"
    print " ", __RCSID__
    print " ", __VERSION__
    sys.exit(0)
    return DIRAC.S_OK()

  def disableClockCheck( self, arg ):
    self.checkClock = False
    return DIRAC.S_OK()

  def debugMsg( self, msg ):
    if self.debug:
      print msg

  def registerCLISwitches( self ):
    Script.registerSwitch( "v:", "valid=", "Valid HH:MM for the proxy. By default is 24 hours", self.setProxyLifeTime )
    Script.registerSwitch( "g:", "group=", "DIRAC Group to embed in the proxy", self.setDIRACGroup )
    Script.registerSwitch( "b:", "strength=", "Set the proxy strength in bytes", self.setProxyStrength )
    Script.registerSwitch( "l", "limited", "Generate a limited proxy", self.setProxyLimited )
    Script.registerSwitch( "t", "strict", "Fail on each error. Treat warnings as errors.", self.setStrict )
    Script.registerSwitch( "d", "debug", "Enable debug output", self.setDebug )
    Script.registerSwitch( "c:", "cert=", "File to use as user certificate", self.setCertLocation )
    Script.registerSwitch( "k:", "key=", "File to use as user key", self.setKeyLocation )
    Script.registerSwitch( "u:", "out=", "File to write as proxy", self.setProxyLocation )
    Script.registerSwitch( "x", "nocs", "Disable CS check", self.setDisableCSCheck )
    Script.registerSwitch( "p", "pwstdin", "Get passwd from stdin", self.setStdinPasswd )
    Script.registerSwitch( "i", "version", "Print version", self.showVersion )
    Script.registerSwitch( "j", "noclockcheck", "Disable checking if time is ok", self.disableClockCheck )
    Script.addDefaultOptionValue( "LogLevel", "always" )

from DIRAC import S_OK, S_ERROR
from DIRAC.Core.Security.X509Chain import X509Chain
from DIRAC.Core.Security import Locations, CS


def generateProxy( params ):

  if params.checkClock:
    result = getClockDeviation()
    if not result[ 'OK' ]:
      print "Error: %s" % result[ 'Message' ]
    else:
      deviation = result[ 'Value' ]
      if deviation > 600:
        print "Error: Your host clock seems to be off by more than TEN MINUTES! Thats really bad."
        print "We're cowardly refusing to generate a proxy. Please fix your system time"
        DIRAC.exit(1)
      elif deviation > 180:
        print "Error: Your host clock seems to be off by more than THREE minutes! Thats bad."
        print "Warn : We'll generate the proxy but please fix your system time"
      elif deviation > 60:
        print "Error: Your host clock seems to be off by more than a minute! Thats not good."
        print "Warn : We'll generate the proxy but please fix your system time"

  certLoc = params.certLoc
  keyLoc = params.keyLoc
  if not certLoc or not keyLoc:
    cakLoc = Locations.getCertificateAndKeyLocation()
    if not cakLoc:
      return S_ERROR( "Can't find user certificate and key" )
    if not certLoc:
      certLoc = cakLoc[0]
    if not keyLoc:
      keyLoc = cakLoc[1]

  testChain = X509Chain()
  retVal = testChain.loadKeyFromFile(keyLoc, password = params.userPasswd )
  if not retVal[ 'OK' ]:
    passwdPrompt = "Enter Certificate password:"
    if params.stdinPasswd:
      userPasswd = sys.stdin.readline().strip("\n")
    else:
      userPasswd = getpass.getpass( passwdPrompt )
    params.userPasswd = userPasswd

  proxyLoc = params.proxyLoc
  if not proxyLoc:
    proxyLoc = Locations.getDefaultProxyLocation()

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

  chain = X509Chain()
  #Load user cert and key
  retVal = chain.loadChainFromFile( certLoc )
  if not retVal[ 'OK' ]:
    return S_ERROR( "Can't load %s" % certLoc )
  retVal = chain.loadKeyFromFile( keyLoc, password = params.userPasswd )
  if not retVal[ 'OK' ]:
    return S_ERROR( "Can't load %s" % keyLoc )

  if params.checkWithCS and params.diracGroup:
    retVal = chain.generateProxyToFile( proxyLoc,
                                        params.proxyLifeTime,
                                        strength =params.proxyStrength,
                                        limited = params.limitedProxy )

    params.debugMsg( "Contacting CS..." )

    retVal = Script.enableCS()
    if not retVal[ 'OK' ]:
      return S_ERROR( "Can't contact DIRAC CS: %s" % retVal[ 'Message' ] )
    if not params.diracGroup:
      params.diracGroup = CS.getDefaultUserGroup()
    userDN = chain.getCertInChain( -1 )['Value'].getSubjectDN()['Value']
    params.debugMsg( "Checking DN %s" % userDN )
    retVal = CS.getUsernameForDN( userDN )
    if not retVal[ 'OK' ]:
      return S_ERROR( "DN %s is not registered" % userDN )
    username = retVal[ 'Value' ]
    params.debugMsg( "Username is %s" % username )
    retVal = CS.getGroupsForUser( username )
    if not retVal[ 'OK' ]:
      return S_ERROR( "User %s has no groups defined" % username )
    groups = retVal[ 'Value' ]
    if params.diracGroup not in groups:
      return S_ERROR( "Requested group %s is not valid for user %s" % ( params.diracGroup, username ) )
    params.debugMsg( "Creating proxy for %s@%s (%s)" % ( username, params.diracGroup, userDN ) )

  retVal = chain.generateProxyToFile( proxyLoc,
                                      params.proxyLifeTime,
                                      params.diracGroup,
                                      strength =params.proxyStrength,
                                      limited = params.limitedProxy )

  if not retVal[ 'OK' ]:
    return S_ERROR( "Couldn't generate proxy: %s" % retVal[ 'Message' ] )
  return S_OK( proxyLoc )

