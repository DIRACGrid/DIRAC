#!/usr/bin/env python
########################################################################
# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/FrameworkSystem/scripts/dirac-admin-users-with-proxy.py,v 1.1 2008/07/23 18:54:53 acasajus Exp $
# File :   dirac-admin-get-proxy
# Author : Stuart Paterson
########################################################################
__RCSID__   = "$Id: dirac-admin-users-with-proxy.py,v 1.1 2008/07/23 18:54:53 acasajus Exp $"
__VERSION__ = "$Revision: 1.1 $"
import os
from DIRACEnvironment import DIRAC
from DIRAC.Core.Base import Script
from DIRAC.FrameworkSystem.Client.ProxyManagerClient import gProxyManager
from DIRAC.Core.Security import CS
from DIRAC.Core.Utilities import Time

class Params:

  limited = False
  proxyPath = False
  proxyLifeTime = 3600

  def setProxyLifeTime( self, arg ):
    try:
      fields = [ f.strip() for f in arg.split(":") ]
      self.proxyLifeTime = int( fields[0] ) * 3600 + int( fields[1] ) * 60
    except:
      print "Can't parse %s time! Is it a HH:MM?" % arg
      return DIRAC.S_ERROR( "Can't parse time argument" )
    return DIRAC.S_OK()

  def registerCLISwitches( self ):
    Script.registerSwitch( "v:", "valid=", "Required HH:MM for the users", self.setProxyLifeTime )

params = Params()
params.registerCLISwitches()

Script.parseCommandLine( ignoreErrors = True )
args = Script.getPositionalArgs()
result = gProxyManager.getDBContents()
if not result[ 'OK' ]:
  print "Can't retrieve list of users: %s" % result[ 'Message' ]
  DIRAC.exit(1)

keys = result[ 'Value' ][ 'ParameterNames' ]
records = result[ 'Value' ][ 'Records' ]
dataDict = {}
now = Time.dateTime()
for record in records:
  expirationDate =  record[ 2 ]
  dt = expirationDate - now
  secsLeft = dt.days * 86400 + dt.seconds
  if secsLeft > params.proxyLifeTime:
    userDN = record[ 0 ]
    userGroup = record[ 1 ]
    persistent = record[ 3 ]
    retVal = CS.getUsernameForDN( userDN )
    if retVal[ 'OK' ]:
      userName = retVal[ 'Value' ]
      if not userName in dataDict:
        dataDict[ userName ] = []
      dataDict[ userName ].append( ( userDN, userGroup, expirationDate, persistent ) )

for userName in dataDict:
  print userName
  for data in dataDict[ userName ]:
    print " %s %s" % ( data[1].ljust(10), data[0] )
    print "  %s" %  data[2]
    print "  persistent %s" %  data[3]

DIRAC.exit(0)