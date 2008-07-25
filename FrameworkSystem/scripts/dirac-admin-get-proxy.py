#!/usr/bin/env python
########################################################################
# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/FrameworkSystem/scripts/dirac-admin-get-proxy.py,v 1.3 2008/07/25 13:12:03 acasajus Exp $
# File :   dirac-admin-get-proxy
# Author : Stuart Paterson
########################################################################
__RCSID__   = "$Id: dirac-admin-get-proxy.py,v 1.3 2008/07/25 13:12:03 acasajus Exp $"
__VERSION__ = "$Revision: 1.3 $"
import os
from DIRACEnvironment import DIRAC
from DIRAC.Core.Base import Script
from DIRAC.FrameworkSystem.Client.ProxyManagerClient import gProxyManager
from DIRAC.Core.Security import CS

class Params:

  limited = False
  proxyPath = False
  proxyLifeTime = 86400
  enableVOMS = False
  vomsAttr = False

  def setLimited( self, args ):
    self.limited = True
    return DIRAC.S_OK()

  def setProxyLocation( self, args ):
    self.proxyPath = args
    return DIRAC.S_OK()

  def setProxyLifeTime( self, arg ):
    try:
      fields = [ f.strip() for f in arg.split(":") ]
      self.proxyLifeTime = int( fields[0] ) * 3600 + int( fields[1] ) * 60
    except:
      print "Can't parse %s time! Is it a HH:MM?" % arg
      return DIRAC.S_ERROR( "Can't parse time argument" )
    return DIRAC.S_OK()

  def automaticVOMS( self, arg ):
    self.enableVOMS = True
    return DIRAC.S_OK()

  def setVOMSAttr( self, arg ):
    self.enableVOMS = True
    self.vomsAttr = arg
    return DIRAC.S_OK()

  def registerCLISwitches( self ):
    Script.registerSwitch( "v:", "valid=", "Valid HH:MM for the proxy. By default is 24 hours", self.setProxyLifeTime )
    Script.registerSwitch( "l", "limited", "Get a limited proxy", self.setLimited )
    Script.registerSwitch( "u:", "out=", "File to write as proxy", self.setProxyLocation )
    Script.registerSwitch( "a", "voms", "Get proxy with VOMS extension mapped to the DIRAC group", self.automaticVOMS )
    Script.registerSwitch( "m:", "vomsAttr=", "VOMS attribute to require", self.setVOMSAttr )

params = Params()
params.registerCLISwitches()

Script.parseCommandLine( ignoreErrors = True )
args = Script.getPositionalArgs()

def usage():
  print 'Usage: %s <DN>/<username> <DIRAC group>' %( Script.scriptName )
  print " <username> will fail if there's more than one DN registered for that username"
  DIRAC.exit(2)

if len(args) != 2:
  usage()

userGroup = str(args[1])
userDN = str(args[0])
userName = False
if userDN.find( "/" ) != 0:
  userName = userDN
  retVal = CS.getDNForUsername( userName )
  if not retVal[ 'OK' ]:
    print "Cannot discover DN for username %s\n\t%s" % ( userName, retVal[ 'Message' ] )
    DIRAC.exit(2)
  DNList = retVal[ 'Value' ]
  if len( DNList ) > 1:
    print "Username %s has more than one DN registered" % userName
    for dn in DNList:
      print " %s" % dn
    print "Which dn do you want to download?"
    DIRAC.exit(2)
  userDN = DNList[0]

if not params.proxyPath:
  if not userName:
    result = CS.getUsernameForDN( userDN )
    if not result[ 'OK' ]:
      print "DN '%s' is not registered in DIRAC" % userDN
      DIRAC.exit(2)
    userName = result[ 'Value' ]
  params.proxyPath = "%s/proxy.%s.%s" % ( os.getcwd(), userName, userGroup )

if params.enableVOMS:
  result = gProxyManager.downloadVOMSProxy( userDN, userGroup, limited = params.limited,
                                            requiredTimeLeft = params.proxyLifeTime,
                                            requiredVOMSAttribute = params.vomsAttr )
else:
  result = gProxyManager.downloadProxy( userDN, userGroup, limited = params.limited,
                                        requiredTimeLeft = params.proxyLifeTime )
if not result['OK']:
  print 'Proxy file cannot be retrieved: %s' % result['Message']
  DIRAC.exit(2)
chain = result[ 'Value' ]
result = chain.dumpAllToFile( params.proxyPath )
if not result['OK']:
  print 'Proxy file cannot be written to %s: %s' % ( params.proxyPath, result['Message'] )
  DIRAC.exit(2)
print "Proxy downloaded to %s" % params.proxyPath
DIRAC.exit(0)