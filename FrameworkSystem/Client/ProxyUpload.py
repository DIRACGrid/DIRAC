########################################################################
# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/FrameworkSystem/Client/ProxyUpload.py,v 1.1 2009/01/12 15:44:14 acasajus Exp $
# File :   dirac-proxy-init.py
# Author : Adrian Casajus
###########################################################from DIRAC.Core.Base import Script#############
__RCSID__   = "$Id: ProxyUpload.py,v 1.1 2009/01/12 15:44:14 acasajus Exp $"
__VERSION__ = "$Revision: 1.1 $"

import sys
import getpass
from DIRACEnvironment import DIRAC
from DIRAC.Core.Base import Script

class CLIParams:

  proxyLifeTime = 0
  diracGroup = False
  debug = False
  certLoc = False
  keyLoc = False
  proxyLoc = False
  onTheFly = False
  stdinPasswd = False
  userPasswd = ""

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

  def setOnTheFly( self, arg ):
    self.onTheFly = True
    return DIRAC.S_OK()

  def setStdinPasswd( self, arg ):
    self.stdinPasswd = True
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

  def registerCLISwitches( self ):
    Script.registerSwitch( "v:", "valid=", "Valid HH:MM for the proxy. By default is 24 hours", self.setProxyLifeTime )
    Script.registerSwitch( "g:", "group=", "DIRAC Group to embed in the proxy", self.setDIRACGroup )
    Script.registerSwitch( "d", "debug", "Enable debug output", self.setDebug )
    Script.registerSwitch( "c:", "cert=", "File to use as user certificate", self.setCertLocation )
    Script.registerSwitch( "k:", "key=", "File to use as user key", self.setKeyLocation )
    Script.registerSwitch( "p:", "proxy=", "File to use as proxy", self.setProxyLocation )
    Script.registerSwitch( "f", "onthefly", "Generate a proxy on the fly", self.setOnTheFly )
    Script.registerSwitch( "p", "pwstdin", "Get passwd from stdin", self.setStdinPasswd )
    Script.registerSwitch( "i", "version", "Print version", self.showVersion )
    Script.addDefaultOptionValue( "LogLevel", "always" )

from DIRAC import S_OK, S_ERROR
from DIRAC.Core.Security.X509Chain import X509Chain
from DIRAC.Core.Security import Locations, CS
from DIRAC.FrameworkSystem.Client.ProxyManagerClient import gProxyManager

def uploadProxy( params ):
  params.debugMsg( "Loading user proxy" )
  proxyLoc = params.proxyLoc
  if not proxyLoc:
    proxyLoc = Locations.getDefaultProxyLocation()
  if not proxyLoc:
    return S_ERROR( "Can't find any proxy" )

  proxyChain = X509Chain()
  retVal = proxyChain.loadProxyFromFile( proxyLoc )
  if not retVal[ 'OK' ]:
    return S_ERROR( "Can't load proxy file %s: %s" % ( params.proxyLoc, retVal[ 'Message' ] ) )

  if params.onTheFly:
    params.debugMsg( "Uploading proxy on-the-fly" )
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

    params.debugMsg( "Cert file %s" % certLoc )
    params.debugMsg( "Key file  %s" % keyLoc )

    testChain = X509Chain()
    retVal = testChain.loadKeyFromFile( keyLoc, password = params.userPasswd )
    if not retVal[ 'OK' ]:
      passwdPrompt = "Enter Certificate password:"
      if params.stdinPasswd:
        userPasswd = sys.stdin.readline().strip("\n")
      else:
        userPasswd = getpass.getpass( passwdPrompt )
      params.userPasswd = userPasswd

    params.debugMsg( "Loading cert and key" )
    chain = X509Chain()
    #Load user cert and key
    retVal = chain.loadChainFromFile( certLoc )
    if not retVal[ 'OK' ]:
      return S_ERROR( "Can't load %s" % certLoc )
    retVal = chain.loadKeyFromFile( keyLoc, password = params.userPasswd )
    if not retVal[ 'OK' ]:
      return S_ERROR( "Can't load %s" % keyLoc )
    params.debugMsg( "User credentials loaded" )

    diracGroup = params.diracGroup
    if not diracGroup:
      diracGroup = CS.getDefaultUserGroup()
    restrictLifeTime = params.proxyLifeTime

  else:
    chain = proxyChain
    diracGroup = False
    restrictLifeTime = 0

  params.debugMsg( " Uploading..." )
  return gProxyManager.uploadProxy( chain, diracGroup, restrictLifeTime = restrictLifeTime )
