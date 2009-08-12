#!/usr/bin/env python
########################################################################
# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/FrameworkSystem/scripts/dirac-admin-get-CAs.py,v 1.1 2009/08/12 17:39:51 rgracian Exp $
# File :   dirac-admin-get-CAs
# Author : Ricardo Graciani
########################################################################
__RCSID__   = "$Id: dirac-admin-get-CAs.py,v 1.1 2009/08/12 17:39:51 rgracian Exp $"
__VERSION__ = "$Revision: 1.1 $"
import os
from DIRACEnvironment import DIRAC
from DIRAC.Core.Base import Script
from DIRAC.FrameworkSystem.Client.BundleDeliveryClient import BundleDeliveryClient

Script.addDefaultOptionValue( '/DIRAC/Security/SkipCAChecks', 'yes' )
Script.parseCommandLine( ignoreErrors = True )

bdc = BundleDeliveryClient()

result = bdc.syncCAs()
if not result[ 'OK' ]:
  DIRAC.gLogger.error( "Error while updating CAs", result[ 'Message' ] )
  DIRAC.exit(1)
elif result[ 'Value' ]:
  DIRAC.gLogger.info( "CAs got updated" )
else:
  DIRAC.gLogger.info( "CAs are already synchronized" )

result = bdc.syncCRLs()
if not result[ 'OK' ]:
  DIRAC.gLogger.error( "Error while updating CRLs", result[ 'Message' ] )
  DIRAC.exit(1)
elif result[ 'Value' ]:
  DIRAC.gLogger.info( "CRLs got updated" )
else:
  DIRAC.gLogger.info( "CRLs are already synchronized" )

DIRAC.exit(0)