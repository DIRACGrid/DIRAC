#!/usr/bin/env python
########################################################################
# $HeadURL: svn+ssh://svn.cern.ch/reps/dirac/DIRAC/trunk/DIRAC/FrameworkSystem/scripts/dirac-proxy-upload.py $
# File :    dirac-proxy-init.py
# Author :  Adrian Casajus
###########################################################from DIRAC.Core.Base import Script#############
__RCSID__ = "$Id: dirac-proxy-upload.py 18161 2009-11-11 12:07:09Z acasajus $"

import sys
import DIRAC
from DIRAC.Core.Base import Script
from DIRAC.FrameworkSystem.Client.ProxyManagerClient import ProxyManagerClient
from DIRAC.Core.Security import CS, Properties
from DIRAC.Core.Security.ProxyInfo import *

userName = False

def setUser( arg ):
  global userName
  userName = arg
  return DIRAC.S_OK()

Script.registerSwitch( "u:", "user=", "User to query (by default oneself)", setUser )

Script.parseCommandLine()

result = getProxyInfo()
if not result[ 'OK' ]:
  print "Do you have a valid proxy?"
  print result[ 'Message' ]
  sys.exit( 1 )
proxyProps = result[ 'Value' ]

if not userName:
  userName = proxyProps[ 'username' ]

if userName in CS.getAllUsers():
  if Properties.PROXY_MANAGEMENT not in proxyProps[ 'groupProperties' ]:
    if userName != proxyProps[ 'username' ] and userName != proxyProps[ 'issuer' ]:
      print "You can only query info about yourself!"
      sys.exit( 1 )
  result = CS.getDNForUsername( userName )
  if not result[ 'OK' ]:
    print "Oops %s" % result[ 'Message' ]
  dnList = result[ 'Value' ]
  if not dnList:
    print "User %s has no DN defined!" % userName
    sys.exit( 1 )
  userDNs = dnList
else:
  userDNs = [ userName ]


print "Checking for DNs %s" % " | ".join( userDNs )
pmc = ProxyManagerClient()
result = pmc.getDBContents( { 'UserDN' : userDNs } )
if not result[ 'OK' ]:
  print "Could not retrieve the proxy list: %s" % result[ 'Message' ]
  sys.exit( 1 )

data = result[ 'Value' ]
colLengths = []
for pN in data[ 'ParameterNames' ]:
  colLengths.append( len( pN ) )
for row in data[ 'Records' ] :
  for i in range( len( row ) ):
    colLengths[ i ] = max( colLengths[i], len( str( row[i] ) ) )

lines = [""]
for i in range( len( data[ 'ParameterNames' ] ) ):
  pN = data[ 'ParameterNames' ][i]
  lines[0] += "| %s " % pN.ljust( colLengths[i] )
lines[0] += "|"
tL = len( lines[0] )
lines.insert( 0, "-"*tL )
lines.append( "-"*tL )
for row in data[ 'Records' ] :
  nL = ""
  for i in range( len( row ) ):
    nL += "| %s " % str( row[i] ).ljust( colLengths[i] )
  nL += "|"
  lines.append( nL )
  lines.append( "-"*tL )

print "\n".join( lines )


