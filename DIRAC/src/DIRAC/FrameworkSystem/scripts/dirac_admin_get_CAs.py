#!/usr/bin/env python
"""
Refresh the local copy of the CA certificates and revocation lists.
Connects to the BundleDelivery service to obtain the tar balls.

Example:
  $ dirac-admin-get-CAs
"""
import DIRAC
from DIRAC.Core.Base.Script import Script
from DIRAC.FrameworkSystem.Client.BundleDeliveryClient import BundleDeliveryClient


@Script()
def main():
    Script.addDefaultOptionValue("/DIRAC/Security/SkipCAChecks", "yes")
    Script.parseCommandLine(ignoreErrors=True)

    bdc = BundleDeliveryClient()

    result = bdc.syncCAs()
    if not result["OK"]:
        DIRAC.gLogger.error("Error while updating CAs", result["Message"])
        DIRAC.exit(1)
    DIRAC.gLogger.notice("CAs got updated")

    result = bdc.syncCRLs()
    if not result["OK"]:
        DIRAC.gLogger.error("Error while updating CRLs", result["Message"])
        DIRAC.exit(1)
    DIRAC.gLogger.notice("CRLs got updated")

    DIRAC.exit(0)


if __name__ == "__main__":
    main()
