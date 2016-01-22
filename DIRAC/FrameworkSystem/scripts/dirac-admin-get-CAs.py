#!/usr/bin/env python
########################################################################
# $HeadURL$
# File :    dirac-admin-get-CAs
# Author :  Ricardo Graciani
########################################################################
__RCSID__ = "$Id$"
import os
import DIRAC
from DIRAC.Core.Base import Script
from DIRAC.FrameworkSystem.Client.BundleDeliveryClient import BundleDeliveryClient

Script.addDefaultOptionValue( '/DIRAC/Security/SkipCAChecks', 'yes' )
Script.parseCommandLine( ignoreErrors = True )

bdc = BundleDeliveryClient()

result = bdc.syncCAs()
if not result[ 'OK' ]:
  DIRAC.gLogger.error( "Error while updating CAs", result[ 'Message' ] )
  DIRAC.exit( 1 )
elif result[ 'Value' ]:
  DIRAC.gLogger.info( "CAs got updated" )
else:
  DIRAC.gLogger.info( "CAs are already synchronized" )

result = bdc.syncCRLs()
if not result[ 'OK' ]:
  DIRAC.gLogger.error( "Error while updating CRLs", result[ 'Message' ] )
  DIRAC.exit( 1 )
elif result[ 'Value' ]:
  DIRAC.gLogger.info( "CRLs got updated" )
else:
  DIRAC.gLogger.info( "CRLs are already synchronized" )

DIRAC.exit( 0 )
