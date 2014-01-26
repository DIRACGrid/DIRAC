########################################################################
# $HeadURL$
# File :    dirac-proxy-init.py
# Author :  Adrian Casajus
###########################################################from DIRAC.Core.Base import Script#############
__RCSID__ = "$Id$"

import sys
import getpass
import DIRAC
from DIRAC.Core.Base import Script

class CLIParams:

  proxyLifeTime = 2592000
  diracGroup = False
  certLoc = False
  keyLoc = False
  proxyLoc = False
  onTheFly = False
  stdinPasswd = False
  userPasswd = ""

  def __str__( self ):
    data = []
    for k in ( 'proxyLifeTime', 'diracGroup', 'certLoc', 'keyLoc', 'proxyLoc',
               'onTheFly', 'stdinPasswd', 'userPasswd' ):
      if k == 'userPasswd':
        data.append( "userPasswd = *****" )
      else:
        data.append( "%s=%s" % ( k, getattr( self, k ) ) )
    msg = "<UploadCLIParams %s>" % " ".join( data )
    return msg

  def setProxyLifeTime( self, arg ):
    try:
      fields = [ f.strip() for f in arg.split( ":" ) ]
      self.proxyLifeTime = int( fields[0] ) * 3600 + int( fields[1] ) * 60
    except ValueError:
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
    sys.exit( 0 )
    return DIRAC.S_OK()

  def registerCLISwitches( self ):
    Script.registerSwitch( "v:", "valid=", "Valid HH:MM for the proxy. By default is one month", self.setProxyLifeTime )
    Script.registerSwitch( "g:", "group=", "DIRAC Group to embed in the proxy", self.setDIRACGroup )
    Script.registerSwitch( "C:", "Cert=", "File to use as user certificate", self.setCertLocation )
    Script.registerSwitch( "K:", "Key=", "File to use as user key", self.setKeyLocation )
    Script.registerSwitch( "P:", "Proxy=", "File to use as proxy", self.setProxyLocation )
    Script.registerSwitch( "f", "onthefly", "Generate a proxy on the fly", self.setOnTheFly )
    Script.registerSwitch( "p", "pwstdin", "Get passwd from stdin", self.setStdinPasswd )
    Script.registerSwitch( "i", "version", "Print version", self.showVersion )
    Script.addDefaultOptionValue( "LogLevel", "always" )

from DIRAC import S_ERROR
from DIRAC.Core.Security.X509Chain import X509Chain
from DIRAC.Core.Security import Locations
from DIRAC.FrameworkSystem.Client.ProxyManagerClient import gProxyManager

def uploadProxy( params ):
  DIRAC.gLogger.info( "Loading user proxy" )
  proxyLoc = params.proxyLoc
  if not proxyLoc:
    proxyLoc = Locations.getDefaultProxyLocation()
  if not proxyLoc:
    return S_ERROR( "Can't find any proxy" )

  if params.onTheFly:
    DIRAC.gLogger.info( "Uploading proxy on-the-fly" )
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

    DIRAC.gLogger.info( "Cert file %s" % certLoc )
    DIRAC.gLogger.info( "Key file  %s" % keyLoc )

    testChain = X509Chain()
    retVal = testChain.loadKeyFromFile( keyLoc, password = params.userPasswd )
    if not retVal[ 'OK' ]:
      passwdPrompt = "Enter Certificate password:"
      if params.stdinPasswd:
        userPasswd = sys.stdin.readline().strip( "\n" )
      else:
        userPasswd = getpass.getpass( passwdPrompt )
      params.userPasswd = userPasswd

    DIRAC.gLogger.info( "Loading cert and key" )
    chain = X509Chain()
    #Load user cert and key
    retVal = chain.loadChainFromFile( certLoc )
    if not retVal[ 'OK' ]:
      return S_ERROR( "Can't load %s" % certLoc )
    retVal = chain.loadKeyFromFile( keyLoc, password = params.userPasswd )
    if not retVal[ 'OK' ]:
      return S_ERROR( "Can't load %s" % keyLoc )
    DIRAC.gLogger.info( "User credentials loaded" )

    diracGroup = params.diracGroup
    if not diracGroup:
      result = chain.getCredentials()
      if not result['OK']:
        return result
      if 'group' not in result['Value']:
        return S_ERROR( 'Can not get Group from existing credentials' )
      diracGroup = result['Value']['group']
    restrictLifeTime = params.proxyLifeTime

  else:
    proxyChain = X509Chain()
    retVal = proxyChain.loadProxyFromFile( proxyLoc )
    if not retVal[ 'OK' ]:
      return S_ERROR( "Can't load proxy file %s: %s" % ( params.proxyLoc, retVal[ 'Message' ] ) )

    chain = proxyChain
    diracGroup = False
    restrictLifeTime = 0

  DIRAC.gLogger.info( " Uploading..." )
  return gProxyManager.uploadProxy( chain, diracGroup, restrictLifeTime = restrictLifeTime )
