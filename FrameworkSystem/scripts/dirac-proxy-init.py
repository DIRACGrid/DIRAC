#!/usr/bin/env python
########################################################################
# $HeadURL$
# File :    dirac-proxy-init.py
# Author :  Adrian Casajus
########################################################################
__RCSID__ = "$Id$"

import sys
import DIRAC
from DIRAC import gLogger
from DIRAC.Core.Base import Script
#from DIRAC.FrameworkSystem.Client.ProxyGeneration import CLIParams, generateProxy
from DIRAC.FrameworkSystem.Client import ProxyGeneration, ProxyUpload
from DIRAC.Core.Security import X509Chain

if __name__ == "__main__":
  pxParams = ProxyGeneration.CLIParams()
  pxParams.registerCLISwitches()

  Script.disableCS()
  Script.parseCommandLine( ignoreErrors = True )

  gLogger.notice( "Generating proxy..." )
  result = ProxyGeneration.generateProxy( pxParams )
  if not result[ 'OK' ]:
    gLogger.error( result[ 'Message' ] )
    sys.exit( 1 )
  proxyLocation = result[ 'Value' ]
  if pxParams.uploadProxy:
    proxyChain = X509Chain.X509Chain()
    result = proxyChain.loadChainFromFile( proxyLocation )
    if not result[ 'OK' ]:
      gLogger.error( "Could not load the proxy: %s" % result[ 'Message' ] )
      sys.exit( 1 )
    result = proxyChain.getIssuerCert()
    if not result[ 'OK' ]:
      gLogger.error( "Could not load the proxy: %s" % result[ 'Message' ] )
      sys.exit( 1 )
    userCert = result[ 'Value' ]
    secsLeft = userCert.getRemainingSecs()[ 'Value' ] - 300

    gLogger.notice( "Uploading proxy to ProxyManager..." )
    upParams = ProxyUpload.CLIParams()
    upParams.onTheFly = True
    upParams.proxyLifeTime = secsLeft
    for k in ( 'diracGroup', 'certLoc', 'keyLoc', 'userPasswd' ):
      setattr( upParams, k , getattr( pxParams, k ) )

    result = ProxyUpload.uploadProxy( upParams )
    if not result[ 'OK' ]:
      gLogger.error( result[ 'Message' ] )
      sys.exit( 1 )
    gLogger.notice( "Proxy uploaded" )

  sys.exit( 0 )
