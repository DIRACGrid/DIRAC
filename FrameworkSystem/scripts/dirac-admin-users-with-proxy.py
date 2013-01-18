#!/usr/bin/env python
########################################################################
# $HeadURL$
# File :    dirac-admin-get-proxy
# Author :  Stuart Paterson
########################################################################
__RCSID__ = "$Id$"
import os
import DIRAC
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
      fields = [ f.strip() for f in arg.split( ":" ) ]
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
  DIRAC.exit( 1 )

keys = result[ 'Value' ][ 'ParameterNames' ]
records = result[ 'Value' ][ 'Records' ]
dataDict = {}
now = Time.dateTime()
for record in records:
  expirationDate = record[ 3 ] 
  dt = expirationDate - now 
  secsLeft = dt.days * 86400 + dt.seconds
  if secsLeft > params.proxyLifeTime:
    userName = record[ 0 ] 
    userDN = record[ 1 ] 
    userGroup = record[ 2 ] 
    persistent = record[ 4 ] 
    if not userName in dataDict:
      dataDict[ userName ] = []
    dataDict[ userName ].append( ( userDN, userGroup, expirationDate, persistent ) ) 



for userName in dataDict:
  print "* %s" % userName
  for iP in range( len( dataDict[ userName ] ) ):
    data = dataDict[ userName ][ iP ]
    print " DN         : %s" % data[0]
    print " group      : %s" % data[1]
    print " not after  : %s" % Time.toString( data[2] )
    print " persistent : %s" % data[3]
    if iP < len( dataDict[ userName ] ) - 1:
      print " -"



DIRAC.exit( 0 )
