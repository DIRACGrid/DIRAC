#!/usr/bin/env python

"""
Delete a given production
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

    prodClient = ProductionClient()

    res = prodClient.deleteProduction(prodID)
    if res["OK"]:
        DIRAC.gLogger.notice(f"Production {prodID} successully deleted")
    else:
        DIRAC.gLogger.error(res["Message"])
        DIRAC.exit(-1)

    DIRAC.exit(0)


if __name__ == "__main__":
    main()
