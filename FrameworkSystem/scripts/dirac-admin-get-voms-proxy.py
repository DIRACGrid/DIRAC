#!/usr/bin/env python
########################################################################
# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/FrameworkSystem/scripts/Attic/dirac-admin-get-voms-proxy.py,v 1.1 2008/07/23 13:37:10 acasajus Exp $
# File :   dirac-admin-get-proxy
# Author : Stuart Paterson
########################################################################
__RCSID__   = "$Id: dirac-admin-get-voms-proxy.py,v 1.1 2008/07/23 13:37:10 acasajus Exp $"
__VERSION__ = "$Revision: 1.1 $"
import os
from DIRACEnvironment import DIRAC
from DIRAC.Core.Base import Script
from DIRAC.FrameworkSystem.Client.ProxyManagerClient import gProxyManager
from DIRAC.Core.Security import CS

class Params:

  limited = False
  proxyPath = False
  proxyLifeTime = 86400
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

  def setVOMSAttr( self, arg ):
    self.vomsAttr = arg
    return DIRAC.S_OK()

  def registerCLISwitches( self ):
    Script.registerSwitch( "v:", "valid=", "Valid HH:MM for the proxy. By default is 24 hours", self.setProxyLifeTime )
    Script.registerSwitch( "l", "limited", "Get a limited proxy", self.setLimited )
    Script.registerSwitch( "u:", "out=", "File to write as proxy", self.setProxyLocation )
    Script.registerSwitch( "a:", "voms=", "VOMS attribute to require", self.setVOMSAttr )

params = Params()
params.registerCLISwitches()

Script.parseCommandLine( ignoreErrors = True )
args = Script.getPositionalArgs()

def usage():
  print 'Usage: %s <DN> <DIRAC group>' %(Script.scriptName)
  DIRAC.exit(2)

if len(args) != 2:
  usage()

try:
  userDN = str(args[0])
  userGroup = str(args[1])
except Exception,x:
  print 'Expected strings for DN and proxy group', args[0], args[1]
  DIRAC.exit(2)

if not params.proxyPath:
  result = CS.getUsernameForDN( userDN )
  if not result[ 'OK' ]:
    print "DN '%s' is not registered in DIRAC" % userDN
    DIRAC.exit(2)
  userName = result[ 'Value' ]
  params.proxyPath = "%s/proxy.voms.%s.%s" % ( os.getcwd(), userName, userGroup )


result = gProxyManager.downloadVOMSProxy( userDN, userGroup, limited = params.limited,
                                          requiredTimeLeft = params.proxyLifeTime,
                                          requiredVOMSAttribute = params.vomsAttr)
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