#!/usr/bin/env python
"""
Select/Add/Delete a new DownTime entry for a given Site or Service.
"""
import datetime

from DIRAC import exit as DIRACExit
from DIRAC import gLogger
from DIRAC.Core.Base.Script import Script
from DIRAC.Core.Utilities import TimeUtilities
from DIRAC.Core.Utilities.PrettyPrint import printTable
from DIRAC.ResourceStatusSystem.Utilities import Utils


def registerSwitches():
    """
    Registers all switches that can be used while calling the script from the
    command line interface.
    """

    switches = (
        ("downtimeID=", "ID of the downtime"),
        ("element=", "Element (Site, Service) affected by the downtime"),
        ("name=", "Name of the element"),
        ("startDate=", "Starting date of the downtime"),
        ("endDate=", "Ending date of the downtime"),
        ("severity=", "Severity of the downtime (Warning, Outage)"),
        ("description=", "Description of the downtime"),
        ("link=", "URL of the downtime announcement"),
        ("ongoing", 'To force "select" to return the ongoing downtimes'),
    )

    for switch in switches:
        Script.registerSwitch("", switch[0], switch[1])


def parseSwitches():
    """
    Parses the arguments passed by the user
    """

    Script.parseCommandLine(ignoreErrors=True)
    args = Script.getPositionalArgs()
    if not args:
        error("Missing mandatory 'query' argument")
    elif not args[0].lower() in ("select", "add", "delete"):
        error("Missing mandatory argument")
    else:
        query = args[0].lower()

    switches = dict(Script.getUnprocessedSwitches())

    # Default values
    switches.setdefault("downtimeID", None)
    switches.setdefault("element", None)
    switches.setdefault("name", None)
    switches.setdefault("startDate", None)
    switches.setdefault("endDate", None)
    switches.setdefault("severity", None)
    switches.setdefault("description", None)
    switches.setdefault("link", None)

    if query in ("add", "delete") and switches["downtimeID"] is None:
        error("'downtimeID' switch is mandatory for '%s' but found missing" % query)

    if query in ("add", "delete") and "ongoing" in switches:
        error("'ongoing' switch can be used only with 'select'")

    gLogger.debug("The switches used are:")
    map(gLogger.debug, switches.items())

    return (args, switches)


# UTILS: for filtering 'select' output


def filterDate(selectOutput, start, end):
    """
    Selects all the downtimes that meet the constraints of 'start' and 'end' dates
    """

    downtimes = selectOutput
    downtimesFiltered = []

    if start is not None:
        try:
            start = TimeUtilities.fromString(start)
        except Exception:
            error("datetime formt is incorrect, pls try [%Y-%m-%d[ %H:%M:%S]]")
        start = TimeUtilities.toEpoch(start)

    if end is not None:
        try:
            end = TimeUtilities.fromString(end)
        except Exception:
            error("datetime formt is incorrect, pls try [%Y-%m-%d[ %H:%M:%S]]")
        end = TimeUtilities.toEpoch(end)

    if start is not None and end is not None:
        for dt in downtimes:
            dtStart = TimeUtilities.toEpoch(dt["startDate"])
            dtEnd = TimeUtilities.toEpoch(dt["endDate"])
            if (dtStart >= start) and (dtEnd <= end):
                downtimesFiltered.append(dt)

    elif start is not None and end is None:
        for dt in downtimes:
            dtStart = TimeUtilities.toEpoch(dt["startDate"])
            if dtStart >= start:
                downtimesFiltered.append(dt)

    elif start is None and end is not None:
        for dt in downtimes:
            dtEnd = TimeUtilities.toEpoch(dt["endDate"])
            if dtEnd <= end:
                downtimesFiltered.append(dt)

    else:
        downtimesFiltered = downtimes

    return downtimesFiltered


def filterOngoing(selectOutput):
    """
    Selects all the ongoing downtimes
    """

    downtimes = selectOutput
    downtimesFiltered = []
    currentDate = TimeUtilities.toEpoch(datetime.datetime.utcnow())

    for dt in downtimes:
        dtStart = TimeUtilities.toEpoch(dt["startDate"])
        dtEnd = TimeUtilities.toEpoch(dt["endDate"])
        if (dtStart <= currentDate) and (dtEnd >= currentDate):
            downtimesFiltered.append(dt)

    return downtimesFiltered


def filterDescription(selectOutput, description):
    """
    Selects all the downtimes that match 'description'
    """

    downtimes = selectOutput
    downtimesFiltered = []
    if description is not None:
        for dt in downtimes:
            if description in dt["description"]:
                downtimesFiltered.append(dt)
    else:
        downtimesFiltered = downtimes

    return downtimesFiltered


# Utils: for formatting query output and notifications


def error(msg):
    """
    Format error messages
    """

    gLogger.error("\nERROR:")
    gLogger.error("\t" + msg)
    gLogger.error("\tPlease, check documentation below")
    Script.showHelp(exitCode=1)


def confirm(query, matches):
    """
    Format confirmation messages
    """

    gLogger.notice(f"\nNOTICE: '{query}' request successfully executed ( matches' number: {matches} )! \n")


def tabularPrint(table):

    columns_names = list(table[0])
    records = []
    for row in table:
        record = []
        for k, v in row.items():
            if isinstance(v, datetime.datetime):
                record.append(TimeUtilities.toString(v))
            elif v is None:
                record.append("")
            else:
                record.append(v)
        records.append(record)

    output = printTable(columns_names, records, numbering=False, columnSeparator=" | ", printOut=False)

    gLogger.notice(output)


def select(switchDict):
    """
    Given the switches, request a query 'select' on the ResourceManagementDB
    that gets from DowntimeCache all rows that match the parameters given.
    """

    rmsClient = ResourceManagementClient()

    meta = {
        "columns": [
            "DowntimeID",
            "Element",
            "Name",
            "StartDate",
            "EndDate",
            "Severity",
            "Description",
            "Link",
            "DateEffective",
        ]
    }

    result = {"output": None, "OK": None, "Message": None, "match": None}
    output = rmsClient.selectDowntimeCache(
        downtimeID=switchDict["downtimeID"],
        element=switchDict["element"],
        name=switchDict["name"],
        severity=switchDict["severity"],
        meta=meta,
    )

    if not output["OK"]:
        return output
    result["output"] = [dict(zip(output["Columns"], dt)) for dt in output["Value"]]
    if "ongoing" in switchDict:
        result["output"] = filterOngoing(result["output"])
    else:
        result["output"] = filterDate(result["output"], switchDict["startDate"], switchDict["endDate"])
    result["output"] = filterDescription(result["output"], switchDict["description"])
    result["match"] = len(result["output"])
    result["OK"] = True
    result["message"] = output["Message"] if "Message" in output else None

    return result


def add(switchDict):
    """
    Given the switches, request a query 'addOrModify' on the ResourceManagementDB
    that inserts or updates-if-duplicated from DowntimeCache.
    """

    rmsClient = ResourceManagementClient()

    result = {"output": None, "OK": None, "Message": None, "match": None}
    output = rmsClient.addOrModifyDowntimeCache(
        downtimeID=switchDict["downtimeID"],
        element=switchDict["element"],
        name=switchDict["name"],
        startDate=switchDict["startDate"],
        endDate=switchDict["endDate"],
        severity=switchDict["severity"],
        description=switchDict["description"],
        link=switchDict["link"],
    )

    if not output["OK"]:
        return output

    if output["Value"]:
        result["match"] = int(output["Value"])
    result["OK"] = True
    result["message"] = output["Message"] if "Message" in output else None

    return result


def delete(switchDict):
    """
    Given the switches, request a query 'delete' on the ResourceManagementDB
    that deletes from DowntimeCache all rows that match the parameters given.
    """

    rmsClient = ResourceManagementClient()

    result = {"output": None, "OK": None, "Message": None, "match": None}
    output = rmsClient.deleteDowntimeCache(
        downtimeID=switchDict["downtimeID"],
        element=switchDict["element"],
        name=switchDict["name"],
        startDate=switchDict["startDate"],
        endDate=switchDict["endDate"],
        severity=switchDict["severity"],
        description=switchDict["description"],
        link=switchDict["link"],
    )
    if not output["OK"]:
        return output

    if output["Value"]:
        result["match"] = int(output["Value"])
    result["OK"] = True
    result["Message"] = output["Message"] if "Message" in output else None

    return result


def run(args, switchDict):
    """
    Main function of the script
    """

    query = args[0]

    # it exectues the query request: e.g. if it's a 'select' it executes 'select()'
    # the same if it is add, delete
    result = eval(query + "( switchDict )")

    if result["OK"]:
        if query == "select" and result["match"] > 0:
            tabularPrint(result["output"])
        confirm(query, result["match"])
    else:
        error(result["Message"])


@Script()
def main():
    global ResourceManagementClient

    # Script initialization
    registerSwitches()
    args, switchDict = parseSwitches()

    ResourceManagementClient = getattr(
        Utils.voimport("DIRAC.ResourceStatusSystem.Client.ResourceManagementClient"),
        "ResourceManagementClient",
    )

    # Run script
    run(args, switchDict)

    # Bye
    DIRACExit(0)


if __name__ == "__main__":
    main()
