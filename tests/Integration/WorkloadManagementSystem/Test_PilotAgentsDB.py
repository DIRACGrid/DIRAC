""" This tests only need the PilotAgentsDB, and connects directly to it

    Suggestion: for local testing, run this with::
        python -m pytest -c ../pytest.ini  -vv tests/Integration/WorkloadManagementSystem/Test_PilotAgentsDB.py
"""
# pylint: disable=wrong-import-position

import csv
from datetime import datetime, timedelta
from unittest.mock import patch

import DIRAC

DIRAC.initialize(require_auth=False, host_credentials=True)  # Initialize configuration

from DIRAC import gLogger
from DIRAC.WorkloadManagementSystem.DB.PilotAgentsDB import PilotAgentsDB, PivotedPilotSummaryTable

gLogger.setLevel("DEBUG")

paDB = PilotAgentsDB()


def preparePilots(stateCount, testSite, testCE, testGroup):
    """
    Set up a bunch of pilots in different states.

    :param list stateCount: number of pilots per state. States are:'Submitted', 'Done', 'Failed',
    'Aborted', 'Running', 'Waiting', 'Scheduled', 'Ready'
    :param str testSite: Site name
    :param str testCE: CE name
    :param str testGroup: group name
    :return list pilot reference list:
    """
    pilotRef = []
    nPilots = sum(stateCount)

    for i in range(nPilots):
        pilotRef.append("pilotRef_" + str(i))

    res = paDB.addPilotReferences(
        pilotRef,
        testGroup,
    )
    assert res["OK"] is True, res["Message"]

    index = 0
    for j, num in enumerate(stateCount):
        for i in range(num):
            pNum = i + index
            res = paDB.setPilotStatus(
                "pilotRef_" + str(pNum),
                PivotedPilotSummaryTable.pstates[j],
                destination=testCE,
                statusReason="Test States",
                gridSite=testSite,
                queue=None,
                benchmark=None,
                currentJob=num,
                updateTime=None,
                conn=False,
            )
            assert res["OK"] is True, res["Message"]

        index += num
    return pilotRef


def cleanUpPilots(pilotRef):
    """
    Delete all pilots pointed to by pilotRef

    :param  lipilotRef:
    :return:
    """

    for elem in pilotRef:
        res = paDB.deletePilot(elem)
        assert res["OK"] is True, res["Message"]


def test_basic():
    """usual insert/verify"""
    res = paDB.addPilotReferences(["pilotRef"], "VO")
    assert res["OK"] is True

    res = paDB.deletePilot("pilotRef")


@patch("DIRAC.WorkloadManagementSystem.DB.PilotAgentsDB.getVOForGroup")
def test_getGroupedPilotSummary(mocked_fcn):
    """
    Test 'pivoted' pilot summary method.

    :return: None
    """
    stateCount = [10, 50, 7, 3, 12, 8, 6, 4]
    testVO = "VO"
    testCE = "TestCE"
    testSite = "TestSite"

    mocked_fcn.return_value = "ownerGroupVO"

    pilotRef = preparePilots(stateCount, testSite, testCE, testVO)
    columnList = ["GridSite", "DestinationSite", "VO"]
    res = paDB.getGroupedPilotSummary(columnList)

    cleanUpPilots(pilotRef)
    expectedParameterList = [
        "Site",
        "CE",
        "VO",
        "Submitted",
        "Done",
        "Failed",
        "Aborted",
        "Running",
        "Waiting",
        "Scheduled",
        "Ready",
        "Aborted_Hour",
        "Total",
        "PilotsPerJob",
        "PilotJobEff",
        "Status",
    ]

    assert res["OK"] is True, res["Message"]
    values = res["Value"]
    assert "ParameterNames" in values, "ParameterNames key missing in result"
    assert values["ParameterNames"] == expectedParameterList, "Expected and obtained ParameterNames differ"

    assert "Records" in values, "Records key missing in result"
    # in the setup with one Site/CE/OwnerGroup there will be at least one record:
    assert len(values["Records"]) >= 1
    record = values["Records"][0]
    assert len(record) == len(expectedParameterList)

    # all pilots have the same timestamp, so Aborted_Hour count is the same as Aborted:
    assert record[expectedParameterList.index("Aborted")] == record[expectedParameterList.index("Aborted_Hour")]
    # Total
    total = record[expectedParameterList.index("Total")]
    # pilot efficiency
    delta = 0.01
    accuracy = (
        record[expectedParameterList.index("PilotJobEff")]
        - 100.0 * (total - record[expectedParameterList.index("Aborted")]) / total
    )
    assert accuracy <= delta, " Pilot eff accuracy %d should be < %d " % (accuracy, delta)
    # there aren't any jobs, so:
    assert record[expectedParameterList.index("Status")] == "Idle"


def test_PivotedPilotSummaryTable():
    """
    Test the 'pivoted' query only. Check whether the number of pilots in different states returned by
    the query is correct.

    :return: None
    """

    # PivotedPilotSummaryTable pstates gives pilot possible states (table.pstates)
    # pstates = ['Submitted', 'Done', 'Failed', 'Aborted', 'Running', 'Waiting', 'Scheduled', 'Ready']

    stateCount = [10, 50, 7, 3, 12, 8, 6, 4]
    testVO = "vo"
    testCE = "jenkins.cern.ch"
    testSite = "DIRAC.Jenkins.ch"

    pilotRef = preparePilots(stateCount, testSite, testCE, testVO)

    table = PivotedPilotSummaryTable(["GridSite", "DestinationSite", "VO"])

    sqlQuery = table.buildSQL()
    res = paDB._query(sqlQuery)
    assert res["OK"] is True, res["Message"]

    columns = table.getColumnList()
    # first 3 columns are: Site, CE and a group (VO mapping comes later, not in the SQL above)
    assert "Site" in columns
    assert columns.index("Site") == 0
    assert "CE" in columns
    assert columns.index("CE") == 1
    assert "VO" in columns
    assert columns.index("VO") == 2

    # pilot numbers by states:
    assert "Total" in columns

    cleanUpPilots(pilotRef)


# Parse date strings into datetime objects
def process_data(data):
    converted_data = []

    for row in data:
        # date fields
        date_indices = [10, 11]  # Positions of date fields
        for i in date_indices:
            if not row[i]:
                row[i] = None
            else:
                try:
                    row[i] = datetime.strptime(row[i], "%Y-%m-%d %H:%M:%S")
                except ValueError:
                    # Handle invalid dates
                    row[i] = None
        # Convert other fields to appropriate types
        int_indices = [0, 1]  # Positions of integer fields
        for i in int_indices:
            if not row[i]:
                row[i] = 0
            else:
                try:
                    row[i] = int(row[i])
                except ValueError:
                    # Handle invalid integers
                    row[i] = 0
        float_indices = [9]  # Positions of float fields
        for i in float_indices:
            if not row[i]:
                row[i] = 0
            else:
                try:
                    row[i] = float(row[i])
                except ValueError:
                    # Handle invalid float
                    row[i] = 0
        converted_data.append(tuple(row))
    return converted_data


def test_summarySnapshot():
    # first delete all pilots
    sql = "DELETE FROM PilotAgents"
    res = paDB._update(sql)
    assert res["OK"], res["Message"]
    sql = "DELETE FROM PilotsHistorySummary"
    res = paDB._update(sql)
    assert res["OK"], res["Message"]

    # insert some predefined pilots to test the summary snapshot
    with open("pilots.csv", newline="", encoding="utf-8") as csvfile:
        csvreader = csv.reader(csvfile)
        data = list(csvreader)
        processed_data = process_data(data)
        placeholders = ",".join(["%s"] * len(processed_data[0]))
        sql = f"INSERT INTO PilotAgents (InitialJobID, CurrentJobID, PilotJobReference, PilotStamp, DestinationSite, Queue, GridSite, VO, GridType, BenchMark, SubmissionTime, LastUpdateTime, Status, StatusReason, AccountingSent) VALUES ({placeholders})"
        res = paDB._updatemany(sql, processed_data)
        assert res["OK"], res["Message"]
    sql = "SELECT * FROM PilotsHistorySummary ORDER BY GridSite, DestinationSite, Status, VO;"
    result = PilotAgentsDB()._query(sql)
    assert result["OK"], result["Message"]
    values = result["Value"][1]
    assert len(values) == 5, "Expected 5 record in the summary"
    # Check it corresponds to the basic "GROUP BY" query
    sql = "SELECT GridSite, DestinationSite, Status, VO, COUNT(*) FROM PilotAgents GROUP BY GridSite, DestinationSite, Status, VO ORDER BY GridSite, DestinationSite, Status, VO;"
    result_grouped = PilotAgentsDB()._query(sql)
    assert result_grouped["OK"], result_grouped["Message"]
    sql = "SELECT * FROM PilotsHistorySummary ORDER BY GridSite, DestinationSite, Status, VO;"
    result_summary = PilotAgentsDB()._query(sql)
    assert result_summary["OK"], result_summary["Message"]
    assert result_grouped["Value"] == result_summary["Value"], "Summary and grouped query results differ"

    # deleting now
    with open("pilots.csv", newline="", encoding="utf-8") as csvfile:
        csvreader = csv.reader(csvfile)
        data = list(csvreader)
        processed_data = process_data(data)
        pilotStamps = [row[3] for row in processed_data]
        pilotStampsStr = ",".join("'" + p + "'" for p in pilotStamps)
        sql = f"DELETE FROM PilotAgents WHERE PilotStamp IN (%s)" % pilotStampsStr
        res = paDB._update(sql)
        assert res["OK"], res["Message"]
    # Check it corresponds to the basic "GROUP BY" query
    sql = "SELECT GridSite, DestinationSite, Status, VO, COUNT(*) FROM PilotAgents GROUP BY GridSite, DestinationSite, Status, VO ORDER BY GridSite, DestinationSite, Status, VO;"
    result_grouped = PilotAgentsDB()._query(sql)
    assert result_grouped["OK"], result_grouped["Message"]
    sql = "select * FROM PilotsHistorySummary ORDER BY GridSite, DestinationSite, Status, VO;"
    result_summary = PilotAgentsDB()._query(sql)
    assert result_summary["OK"], result_summary["Message"]
    assert result_grouped["Value"] == result_summary["Value"], "Summary and grouped query results differ"
