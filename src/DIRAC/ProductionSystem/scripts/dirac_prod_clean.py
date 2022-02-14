#!/usr/bin/env python
"""
Clean a given production
"""
import DIRAC
from DIRAC.Core.Base.Script import Script


@Script()
def main():
    # Registering arguments will automatically add their description to the help menu
    Script.registerArgument("prodID: Production ID")
    _, args = Script.parseCommandLine()

    from DIRAC.ProductionSystem.Client.ProductionClient import ProductionClient

    # get arguments
    prodID = args[0]

    res = ProductionClient().setProductionStatus(prodID, "Cleaned")
    if not res["OK"]:
        DIRAC.gLogger.error(res["Message"])
        DIRAC.exit(1)

    DIRAC.gLogger.notice("Production %s successully cleaned" % prodID)
    DIRAC.exit(0)


if __name__ == "__main__":
    main()
