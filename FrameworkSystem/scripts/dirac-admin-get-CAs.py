#!/usr/bin/env python
########################################################################
# $HeadURL$
# File :    dirac-admin-get-CAs
# Author :  Ricardo Graciani
########################################################################
__RCSID__ = "$Id$"
import os

from DIRAC.Core.Base import Script
from DIRAC.FrameworkSystem.Client.BundleDeliveryClient import BundleDeliveryClient

Script.addDefaultOptionValue( '/DIRAC/Security/SkipCAChecks', 'yes' )
Script.parseCommandLine( ignoreErrors = True )

from DIRAC import gLogger, exit as DIRACExit

bdc = BundleDeliveryClient()

result = bdc.syncCAs()
if not result[ 'OK' ]:
  gLogger.error( "Error while updating CAs", result[ 'Message' ] )
  DIRACExit( 1 )
elif result[ 'Value' ]:
  gLogger.info( "CAs got updated" )
else:
  gLogger.info( "CAs are already synchronized" )

result = bdc.syncCRLs()
if not result[ 'OK' ]:
  gLogger.error( "Error while updating CRLs", result[ 'Message' ] )
  DIRACExit( 1 )
elif result[ 'Value' ]:
  gLogger.info( "CRLs got updated" )
else:
  gLogger.info( "CRLs are already synchronized" )

DIRACExit( 0 )
