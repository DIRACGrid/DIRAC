#!/usr/bin/env python
"""
Complete a given production

Example:
  $ dirac-prod-complete 312
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

    res = ProductionClient().setProductionStatus(prodID, "Completed")
    if not res["OK"]:
        DIRAC.gLogger.error(res["Message"])
        DIRAC.exit(1)

    DIRAC.gLogger.notice(f"Production {prodID} successully completed")
    DIRAC.exit(0)


if __name__ == "__main__":
    main()
