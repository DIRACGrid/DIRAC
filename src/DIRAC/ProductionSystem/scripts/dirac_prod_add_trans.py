#!/usr/bin/env python
"""
Add an existing transformation to an existing production.
Transformations already belonging to another production cannot be added.
"""
import DIRAC
from DIRAC.Core.Base.Script import Script


@Script()
def main():
    # Registering arguments will automatically add their description to the help menu
    Script.registerArgument("prodID:         Production ID")
    Script.registerArgument("transID:        Transformation ID")
    Script.registerArgument("parentTransID:  Parent Transformation ID", default="", mandatory=False)
    _, args = Script.parseCommandLine()

    from DIRAC.ProductionSystem.Client.ProductionClient import ProductionClient
    from DIRAC.TransformationSystem.Client.TransformationClient import TransformationClient

    prodClient = ProductionClient()
    transClient = TransformationClient()

    # get arguments
    prodID, transID, parentTransID = Script.getPositionalArgs(group=True)
    if len(args) > 3:
        Script.showHelp(exitCode=1)

    res = transClient.getTransformation(transID)
    if not res["OK"]:
        DIRAC.gLogger.error(f"Failed to get transformation {transID}: {res['Message']}")
        DIRAC.exit(-1)

    transID = res["Value"]["TransformationID"]

    if parentTransID:
        res = transClient.getTransformation(parentTransID)
        if not res["OK"]:
            DIRAC.gLogger.error(f"Failed to get transformation {parentTransID}: {res['Message']}")
            DIRAC.exit(-1)
        parentTransID = res["Value"]["TransformationID"]

    res = prodClient.getProduction(prodID)
    if not res["OK"]:
        DIRAC.gLogger.error(f"Failed to get production {prodID}: {res['Message']}")
        DIRAC.exit(-1)

    prodID = res["Value"]["ProductionID"]
    res = prodClient.addTransformationsToProduction(prodID, transID, parentTransID)
    if not res["OK"]:
        DIRAC.gLogger.error(res["Message"])
        DIRAC.exit(-1)

    if parentTransID:
        msg = "Transformation {} successfully added to production {} with parent transformation {}".format(
            transID,
            prodID,
            parentTransID,
        )
    else:
        msg = f"Transformation {transID} successfully added to production {prodID} with no parent transformation"

    DIRAC.gLogger.notice(msg)

    DIRAC.exit(0)


if __name__ == "__main__":
    main()
