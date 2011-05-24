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

if __name__ == "__main__":
  pxParams = ProxyGeneration.CLIParams()
  pxParams.registerCLISwitches()

  Script.disableCS()
  Script.parseCommandLine( ignoreErrors = True )

  gLogger.notice( "Generating proxy..." )
  retVal = ProxyGeneration.generateProxy( pxParams )
  if not retVal[ 'OK' ]:
    gLogger.error( retVal[ 'Message' ] )
    sys.exit( 1 )
  if pxParams.uploadProxy:
    gLogger.notice( "Uploading proxy to ProxyManager..." )
    upParams = ProxyUpload.CLIParams()
    upParams.onTheFly = True
    for k in ( 'diracGroup', 'certLoc', 'keyLoc', 'userPasswd' ):
      setattr( upParams, k , getattr( pxParams, k ) )

    result = ProxyUpload.uploadProxy( upParams )
    if not retVal[ 'OK' ]:
      gLogger.error( retVal[ 'Message' ] )
      sys.exit( 1 )
    gLogger.notice( "Proxy uploaded" )

  sys.exit( 0 )
