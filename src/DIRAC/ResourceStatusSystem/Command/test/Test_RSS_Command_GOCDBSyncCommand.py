# """
# Test_RSS_Command_GOCDBSyncCommand
# """

from datetime import datetime, timedelta

from unittest import mock

from DIRAC import gLogger, S_OK
from DIRAC.ResourceStatusSystem.Command.GOCDBSyncCommand import GOCDBSyncCommand

mock_GOCDBClient = mock.MagicMock()
mock_RMClient = mock.MagicMock()
mock_RMClient.addOrModifyDowntimeCache.return_value = S_OK()


"""
Setup
"""
gLogger.setLevel("DEBUG")


def test_instantiate():
    """tests that we can instantiate one object of the tested class"""

    command = GOCDBSyncCommand()
    assert command.__class__.__name__ == "GOCDBSyncCommand"


def test_init():
    """tests that the init method does what it should do"""

    command = GOCDBSyncCommand()
    assert command.args == {"onlyCache": False}
    assert command.apis == {}

    command = GOCDBSyncCommand(clients={"GOCDBClient": mock_GOCDBClient})
    assert command.args == {"onlyCache": False}
    assert command.apis == {"GOCDBClient": mock_GOCDBClient}


def test_doNew():
    """tests the doNew method"""

    now = datetime.utcnow()
    resFromDB = {
        "OK": True,
        "Value": (
            (
                now - timedelta(hours=2),
                "dummy.host1.dummy",
                "https://a1.domain",
                now + timedelta(hours=3),
                "dummy.host.dummy",
                now - timedelta(hours=2),
                "maintenance",
                "OUTAGE",
                now,
                "Resource",
                "APEL",
            ),
            (
                now - timedelta(hours=2),
                "dummy.host2.dummy",
                "https://a2.domain",
                now + timedelta(hours=3),
                "dummy.host2.dummy",
                now - timedelta(hours=2),
                "maintenance",
                "OUTAGE",
                now,
                "Resource",
                "CREAM",
            ),
        ),
        "Columns": [
            "StartDate",
            "DowntimeID",
            "Link",
            "EndDate",
            "Name",
            "DateEffective",
            "Description",
            "Severity",
            "LastCheckTime",
            "Element",
            "GOCDBServiceType",
        ],
    }

    resFromGOCDBclient = {
        "OK": True,
        "Value": """<?xml version="1.0" encoding="UTF-8"?>
                    <results>
                      <DOWNTIME ID="dummy.host1.dummy" PRIMARY_KEY="dummy.host1.dummy" CLASSIFICATION="SCHEDULED">
                        <PRIMARY_KEY>dummy.host1.dummy</PRIMARY_KEY>
                        <HOSTNAME>dummy.host1.dummy</HOSTNAME>
                        <SERVICE_TYPE>gLExec</SERVICE_TYPE>
                        <ENDPOINT>dummy.host1.dummy</ENDPOINT>
                        <HOSTED_BY>dummy.host1.dummy</HOSTED_BY>
                        <GOCDB_PORTAL_URL>https://a1.domain</GOCDB_PORTAL_URL>
                        <AFFECTED_ENDPOINTS/>
                        <SEVERITY>OUTAGE</SEVERITY>
                        <DESCRIPTION>Network connectivity problems</DESCRIPTION>
                        <INSERT_DATE>1473460659</INSERT_DATE>
                        <START_DATE>1473547200</START_DATE>
                        <END_DATE>1473677747</END_DATE>
                        <FORMATED_START_DATE>2016-09-10 22:40</FORMATED_START_DATE>
                        <FORMATED_END_DATE>2016-09-12 10:55</FORMATED_END_DATE>
                      </DOWNTIME>
                    </results>""",
    }

    mock_RMClient.selectDowntimeCache.return_value = resFromDB
    mock_GOCDBClient.getHostnameDowntime.return_value = resFromGOCDBclient
    command = GOCDBSyncCommand({"ResourceManagementClient": mock_RMClient, "GOCDBClient": mock_GOCDBClient})

    res = command.doNew()
    assert res["OK"] is False
