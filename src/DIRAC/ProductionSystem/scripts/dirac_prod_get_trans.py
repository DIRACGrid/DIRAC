#!/usr/bin/env python
"""
Get the transformations belonging to a given production

Example:
  $ dirac-prod-get-trans 381
"""
import DIRAC
from DIRAC.Core.Utilities.PrettyPrint import printTable
from DIRAC.Core.Base.Script import Script


@Script()
def main():
    # Registering arguments will automatically add their description to the help menu
    Script.registerArgument("prodID: Production ID")
    _, args = Script.parseCommandLine()

    from DIRAC.MonitoringSystem.Client.WebAppClient import WebAppClient
    from DIRAC.ProductionSystem.Client.ProductionClient import ProductionClient

    # get arguments
    prodID = args[0]

    prodClient = ProductionClient()

    res = prodClient.getProductionTransformations(prodID)
    transIDs = []

    if res["OK"]:
        transList = res["Value"]
        if not transList:
            DIRAC.gLogger.notice(f"No transformation associated with production {prodID}")
            DIRAC.exit(-1)
        for trans in transList:
            transIDs.append(trans["TransformationID"])
    else:
        DIRAC.gLogger.error(res["Message"])
        DIRAC.exit(-1)

    fields = [
        "TransformationName",
        "Status",
        "F_Proc.",
        "F_Proc.(%)",
        "TransformationID",
        "ProductionID",
        "Prod_LastUpdate",
        "Prod_InsertedTime",
    ]

    records = []

    paramShowNames = [
        "TransformationID",
        "TransformationName",
        "Type",
        "Status",
        "Files_Total",
        "Files_PercentProcessed",
        "Files_Processed",
        "Files_Unused",
        "Jobs_TotalCreated",
        "Jobs_Waiting",
        "Jobs_Running",
        "Jobs_Done",
        "Jobs_Failed",
        "Jobs_Stalled",
    ]
    resList = []

    res = WebAppClient().getTransformationSummaryWeb({"TransformationID": transIDs}, [], 0, len(transIDs))

    if not res["OK"]:
        DIRAC.gLogger.error(res["Message"])
        DIRAC.exit(-1)

    if res["Value"]["TotalRecords"] > 0:
        paramNames = res["Value"]["ParameterNames"]
        for paramValues in res["Value"]["Records"]:
            paramShowValues = map(lambda pname: paramValues[paramNames.index(pname)], paramShowNames)
            showDict = dict(zip(paramShowNames, paramShowValues))
            resList.append(showDict)

    for res in resList:
        files_Processed = res["Files_Processed"]
        files_PercentProcessed = res["Files_PercentProcessed"]
        status = res["Status"]
        type = res["Type"]
        transName = res["TransformationName"]
        transID = res["TransformationID"]
        records.append(
            [
                transName,
                status,
                str(files_Processed),
                str(files_PercentProcessed),
                str(transID),
                str(prodID),
                str(trans["LastUpdate"]),
                str(trans["InsertedTime"]),
            ]
        )

    printTable(fields, records)

    DIRAC.exit(0)


if __name__ == "__main__":
    main()
