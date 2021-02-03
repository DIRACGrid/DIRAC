#!/usr/bin/env python
########################################################################
# File :    dirac-admin-get-CAs
# Author :  Ricardo Graciani
########################################################################
"""
Refresh the local copy of the CA certificates and revocation lists.

Connects to the BundleDelivery service to obtain the tar balls. Needed when proxies appear to be
invalid.

Usage:

  dirac-admin-get-CAs (<options>|<cfgFile>)*

Example:

  $ dirac-admin-get-CAs
"""

import DIRAC
from DIRAC.Core.Base import Script
from DIRAC.FrameworkSystem.Client.BundleDeliveryClient import BundleDeliveryClient

__RCSID__ = "$Id$"
Script.addDefaultOptionValue('/DIRAC/Security/SkipCAChecks', 'yes')
Script.setUsageMessage(__doc__)

Script.parseCommandLine(ignoreErrors=True)

bdc = BundleDeliveryClient()

result = bdc.syncCAs()
if not result['OK']:
  DIRAC.gLogger.error("Error while updating CAs", result['Message'])
  DIRAC.exit(1)
elif result['Value']:
  DIRAC.gLogger.notice("CAs got updated")
else:
  DIRAC.gLogger.notice("CAs are already synchronized")

result = bdc.syncCRLs()
if not result['OK']:
  DIRAC.gLogger.error("Error while updating CRLs", result['Message'])
  DIRAC.exit(1)
elif result['Value']:
  DIRAC.gLogger.notice("CRLs got updated")
else:
  DIRAC.gLogger.notice("CRLs are already synchronized")

DIRAC.exit(0)
