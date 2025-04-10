#!/usr/bin/env python
"""
Get informations for a given production

Example:
  $ dirac-prod-get 381
"""
import DIRAC
from DIRAC.Core.Base.Script import Script


@Script()
def main():
    # Registering arguments will automatically add their description to the help menu
    Script.registerArgument("prodID: Production ID")
    _, args = Script.parseCommandLine()

    from DIRAC.Core.Utilities.PrettyPrint import printTable
    from DIRAC.ProductionSystem.Client.ProductionClient import ProductionClient

    prodClient = ProductionClient()

    # get arguments
    prodID = args[0]
    res = prodClient.getProduction(prodID)

    fields = ["ProductionName", "Status", "ProductionID", "CreationDate", "LastUpdate", "Author", "AuthorGroup"]
    records = []

    if res["OK"]:
        prodList = res["Value"]
        if not isinstance(res["Value"], list):
            prodList = [res["Value"]]
        for prod in prodList:
            records.append(
                [
                    str(prod["ProductionName"]),
                    str(prod["Status"]),
                    str(prod["ProductionID"]),
                    str(prod["CreationDate"]),
                    str(prod["LastUpdate"]),
                    str(prod["Author"]),
                    str(prod["AuthorGroup"]),
                ]
            )
    else:
        DIRAC.gLogger.error(res["Message"])
        DIRAC.exit(-1)

    printTable(fields, records)

    DIRAC.exit(0)


if __name__ == "__main__":
    main()
