#!/usr/bin/env python
"""
Get the description of a given production

Example:
  $ dirac-prod-get-description 381
"""
import DIRAC
from DIRAC.Core.Base.Script import Script


@Script()
def main():
    # Registering arguments will automatically add their description to the help menu
    Script.registerArgument("prodID: Production ID")
    _, args = Script.parseCommandLine()

    from DIRAC.ProductionSystem.Client.ProductionClient import ProductionClient

    prodClient = ProductionClient()

    # get arguments
    prodID = args[0]
    res = prodClient.getProduction(prodID)

    if not res["OK"]:
        DIRAC.gLogger.error(res["Message"])
        DIRAC.exit(-1)

    prod = res["Value"]

    print(f"Description for production {prodID}:\n")
    print(prod["Description"])

    DIRAC.exit(0)


if __name__ == "__main__":
    main()
