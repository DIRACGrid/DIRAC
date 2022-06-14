""" This is a test of the chain
    ResourceStatus -> ResourceStatusHandler -> ResourceStatusDB
    It supposes that the DB is present, and that the service is running
"""

# pylint: disable=wrong-import-position, missing-docstring
import time
import datetime

import pytest

import DIRAC

DIRAC.initialize()  # Initialize configuration

from DIRAC import gLogger
from DIRAC.ResourceStatusSystem.Client.ResourceStatusClient import ResourceStatusClient

gLogger.setLevel("DEBUG")

Datetime = datetime.datetime.utcnow() - datetime.timedelta(hours=1)


@pytest.fixture(name="rssClient")
def fixtureResourceStatusClient():
    yield ResourceStatusClient()


def test_addAndRemove(rssClient: ResourceStatusClient):

    # clean up
    rssClient.deleteStatusElement("Site", "Status", "TestSite1234")
    rssClient.deleteStatusElement("Site", "History", "TestSite1234")
    rssClient.deleteStatusElement("Site", "Log", "TestSite1234")
    rssClient.deleteStatusElement("Resource", "Status", "TestName1234")
    rssClient.deleteStatusElement("Resource", "History", "TestName1234")
    rssClient.deleteStatusElement("Resource", "Log", "TestName1234")

    # TEST insertStatusElement
    # ...............................................................................

    # add an element
    res = rssClient.insertStatusElement(
        "Resource",
        "Status",
        "TestName1234",
        "statusType",
        "Active",
        "elementType",
        "reason",
        Datetime,
        Datetime,
        "tokenOwner",
        Datetime,
    )
    # check if the insert query was executed properly
    assert res["OK"] is True, res["Message"]

    # select the previously entered element
    res = rssClient.selectStatusElement("Resource", "Status", "TestName1234")
    # check if the select query was executed properly
    assert res["OK"] is True, res["Message"]
    assert res["Value"][0][0] == "TestName1234"
    assert res["Value"][0][1] == "statusType"
    assert res["Value"][0][2] == "Active"
    assert res["Value"][0][9] == "all"  # vo value at the end of the list

    # try to select the previously entered element from the Log table (it should NOT be there)
    res = rssClient.selectStatusElement("Resource", "Log", "TestName1234")
    # check if the select query was executed properly
    assert res["OK"] is True, res["Message"]
    assert res["Value"] == []

    # try to select the previously entered element from the Log table,
    # with a reduced list of columns
    # (it should NOT be there)
    res = rssClient.selectStatusElement("Resource", "Log", "TestName1234", meta={"columns": ["name"]})
    # check if the select query was executed properly
    assert res["OK"] is True, res["Message"]
    assert res["Value"] == []

    # TEST insertStatusElement (now site)
    # ...............................................................................

    print("add an element (status: Active)")
    res = rssClient.insertStatusElement(
        "Site",
        "Status",
        "TestSite1234",
        "statusType",
        "Active",
        "elementType",
        "reason",
        Datetime,
        Datetime,
        "tokenOwner",
        Datetime,
    )
    # check if the insert query was executed properly
    assert res["OK"] is True, res["Message"]

    # select the previously entered element
    res = rssClient.selectStatusElement("Site", "Status", "TestSite1234")
    # check if the select query was executed properly
    assert res["OK"] is True, res["Message"]
    assert res["Value"][0][0] == "TestSite1234"
    assert res["Value"][0][1] == "statusType"
    assert res["Value"][0][2] == "Active"
    print("inserted lastCheckTime and DateEffective: %s, %s" % (res["Value"][0][7], res["Value"][0][4]))

    # try to select the previously entered element from the Log table (it should NOT be there)
    res = rssClient.selectStatusElement("Site", "Log", "TestSite1234")
    # check if the select query was executed properly
    assert res["OK"] is True, res["Message"]
    assert res["Value"] == []

    # try to select the previously entered element from the Log table,
    # with a reduced list of columns
    # (it should NOT be there)
    res = rssClient.selectStatusElement("Site", "Log", "TestName1234", meta={"columns": ["name"]})
    # check if the select query was executed properly
    assert res["OK"] is True, res["Message"]
    assert res["Value"] == []

    # TEST addOrModifyStatusElement (this time for modifying)
    # ...............................................................................

    print("modify the previously entered element (making it Banned)")
    res = rssClient.addOrModifyStatusElement(
        "Resource", "Status", "TestName1234", "statusType", "Banned", "elementType", "reason"
    )
    # check if the addOrModify query was executed properly
    assert res["OK"] is True, res["Message"]

    # select the previously modified element
    res = rssClient.selectStatusElement("Resource", "Status", "TestName1234")
    # check if the select query was executed properly
    assert res["OK"] is True, res["Message"]
    assert res["Value"][0][0] == "TestName1234"
    assert res["Value"][0][1] == "statusType"
    assert res["Value"][0][2] == "Banned"
    print("inserted lastCheckTime and DateEffective: %s, %s" % (res["Value"][0][7], res["Value"][0][4]))

    # try to select the previously entered element from the Log table (now it should be there)
    res = rssClient.selectStatusElement("Resource", "Log", "TestName1234")
    # check if the select query was executed properly
    assert res["OK"] is True, res["Message"]
    assert res["Value"][0][1] == "TestName1234"
    assert res["Value"][0][2] == "statusType"
    assert res["Value"][0][3] == "Banned"

    # try to select the previously entered element from the Log table
    # with a reduced list of columns
    # (now it should be there)
    res = rssClient.selectStatusElement("Resource", "Log", "TestName1234", meta={"columns": ["name"]})
    # check if the select query was executed properly
    assert res["OK"] is True, res["Message"]
    assert res["Value"][0][0] == "TestName1234"

    # try to select the previously entered element from the Log table
    # with a reduced list of columns
    # (now it should be there)
    res = rssClient.selectStatusElement("Resource", "Log", "TestName1234", meta={"columns": ["statustype", "status"]})
    # check if the select query was executed properly
    assert res["OK"] is True, res["Message"]
    assert res["Value"][0][0] == "statusType"
    assert res["Value"][0][1] == "Banned"

    # TEST modifyStatusElement
    # ...............................................................................

    print("modify again the previously entered element, putting it back to active")
    res = rssClient.modifyStatusElement(
        "Resource", "Status", "TestName1234", "statusType", "Active", "elementType", "reason"
    )
    # check if the modify query was executed properly
    assert res["OK"] is True, res["Message"]

    # select the previously modified element
    res = rssClient.selectStatusElement("Resource", "Status", "TestName1234")
    # check if the select query was executed properly
    assert res["OK"] is True, res["Message"]
    assert res["Value"][0][0] == "TestName1234"
    assert res["Value"][0][1] == "statusType"
    assert res["Value"][0][2] == "Active"
    print("inserted lastCheckTime and DateEffective: %s, %s" % (res["Value"][0][7], res["Value"][0][4]))

    # try to select the previously entered element from the Log table (now it should be there)
    res = rssClient.selectStatusElement("Resource", "Log", "TestName1234")
    # check if the select query was executed properly
    assert res["OK"] is True, res["Message"]
    assert res["Value"][0][1] == "TestName1234"
    assert res["Value"][0][2] == "statusType"
    assert res["Value"][0][3] == "Banned"
    assert res["Value"][1][3] == "Active"  # this is the last one

    print("modifing once more the previously entered element")
    res = rssClient.modifyStatusElement(
        "Resource", "Status", "TestName1234", "statusType", "Probing", "elementType", "reason"
    )
    # check if the modify query was executed properly
    assert res["OK"] is True, res["Message"]

    # select the previously modified element
    res = rssClient.selectStatusElement("Resource", "Status", "TestName1234")
    # check if the select query was executed properly
    assert res["OK"] is True, res["Message"]
    assert res["Value"][0][0] == "TestName1234"
    assert res["Value"][0][1] == "statusType"
    assert res["Value"][0][2] == "Probing"
    print("inserted lastCheckTime and DateEffective: %s, %s" % (res["Value"][0][7], res["Value"][0][4]))

    # try to select the previously entered element from the Log table (now it should be there)
    res = rssClient.selectStatusElement("Resource", "Log", "TestName1234")
    # check if the select query was executed properly
    assert res["OK"] is True, res["Message"]
    assert res["Value"][0][1] == "TestName1234"
    assert res["Value"][0][2] == "statusType"
    assert res["Value"][0][3] == "Banned"
    assert res["Value"][1][3] == "Active"
    assert res["Value"][2][3] == "Probing"  # this is the last one

    # try to select the previously entered element from the Log table (now it should be there)
    # with a reduced list of columns
    res = rssClient.selectStatusElement("Resource", "Log", "TestName1234", meta={"columns": ["status", "StatusType"]})
    # check if the select query was executed properly
    assert res["OK"] is True, res["Message"]
    assert res["Value"][0][0] == "Banned"
    assert res["Value"][0][1] == "statusType"
    assert res["Value"][1][0] == "Active"
    assert res["Value"][2][0] == "Probing"  # this is the last one

    time.sleep(3)  # just for seeing a difference between lastCheckTime and DateEffective
    print("modifing once more the previously entered element, but this time we only modify the reason")
    res = rssClient.modifyStatusElement(
        "Resource", "Status", "TestName1234", "statusType", "Probing", "elementType", "a new reason"
    )
    # check if the modify query was executed properly
    assert res["OK"] is True, res["Message"]

    # select the previously modified element
    res = rssClient.selectStatusElement("Resource", "Status", "TestName1234")
    # check if the select query was executed properly
    assert res["OK"] is True, res["Message"]
    assert res["Value"][0][0] == "TestName1234"
    assert res["Value"][0][1] == "statusType"
    assert res["Value"][0][2] == "Probing"
    assert res["Value"][0][3] == "a new reason"
    print("inserted lastCheckTime and DateEffective: %s, %s" % (res["Value"][0][7], res["Value"][0][4]))
    assert res["Value"][0][7] != res["Value"][0][4]

    # try to select the previously entered element from the Log table (now it should be there)
    res = rssClient.selectStatusElement("Resource", "Log", "TestName1234")
    # check if the select query was executed properly
    assert res["OK"] is True, res["Message"]
    assert res["Value"][0][1] == "TestName1234"
    assert res["Value"][0][2] == "statusType"
    assert res["Value"][0][3] == "Banned"
    assert res["Value"][1][3] == "Active"
    assert res["Value"][2][3] == "Probing"
    assert res["Value"][3][3] == "Probing"  # this is the last one

    # try to select the previously entered element from the Log table (now it should be there)
    # Using also Meta
    res = rssClient.selectStatusElement(
        "Resource",
        "Log",
        "TestName1234",
        meta={"columns": ["Status", "StatusType"], "newer": ["DateEffective", Datetime]},
    )
    # check if the select query was executed properly
    assert res["OK"] is True, res["Message"]
    assert res["Value"][0][0] == "Banned"
    assert res["Value"][0][1] == "statusType"
    assert res["Value"][1][0] == "Active"
    assert res["Value"][1][1] == "statusType"
    assert res["Value"][2][0] == "Probing"
    assert res["Value"][2][1] == "statusType"
    assert res["Value"][3][0] == "Probing"  # this is the last one
    assert res["Value"][3][1] == "statusType"

    # try to select the previously entered element from the Log table (now it should be there)
    # Using also Meta
    res = rssClient.selectStatusElement(
        "Resource",
        "Log",
        "TestName1234",
        meta={"columns": ["Status", "StatusType"], "older": ["DateEffective", Datetime]},
    )
    # check if the select query was executed properly
    assert res["OK"] is True, res["Message"]
    assert res["Value"] == []

    # try to select the previously entered element from the Log table (now it should be there)
    # Using Meta with order
    res = rssClient.selectStatusElement(
        "Resource",
        "Log",
        "TestName1234",
        meta={
            "columns": ["Status", "StatusType"],
            "newer": ["DateEffective", Datetime],
            "order": ["status", "DESC"],
        },
    )
    # check if the select query was executed properly
    assert res["OK"] is True, res["Message"]
    assert res["Value"][0][0] == "Probing"
    assert res["Value"][0][1] == "statusType"
    assert res["Value"][1][0] == "Probing"
    assert res["Value"][1][1] == "statusType"
    assert res["Value"][2][0] == "Banned"
    assert res["Value"][2][1] == "statusType"
    assert res["Value"][3][0] == "Active"  # this is the last one (in this order)
    assert res["Value"][3][1] == "statusType"

    # try to select the previously entered element from the Log table (now it should be there)
    # Using Meta with limit
    res = rssClient.selectStatusElement(
        "Resource",
        "Log",
        "TestName1234",
        meta={
            "columns": ["Status", "StatusType"],
            "newer": ["DateEffective", Datetime],
            "order": "status",
            "limit": 1,
        },
    )
    # check if the select query was executed properly
    assert res["OK"] is True, res["Message"]
    assert res["Value"][0][0] == "Active"
    assert res["Value"][0][1] == "statusType"
    assert len(res["Value"]) == 1

    # TEST deleteStatusElement
    # ...............................................................................

    # delete the element
    res = rssClient.deleteStatusElement("Resource", "Status", "TestName1234")
    # check if the delete query was executed properly
    assert res["OK"] is True, res["Message"]

    res = rssClient.deleteStatusElement("Site", "Status", "TestSite1234")
    # check if the delete query was executed properly
    assert res["OK"] is True, res["Message"]

    res = rssClient.deleteStatusElement("Site", "Log", "TestSite1234")
    # check if the delete query was executed properly
    assert res["OK"] is True, res["Message"]

    # try to select the previously deleted element
    res = rssClient.selectStatusElement("Resource", "Status", "TestName1234")
    # check if the select query was executed properly
    assert res["OK"] is True, res["Message"]
    # check if the returned value is empty
    assert not res["Value"]


def test_addIfNotThereStatusElement(rssClient: ResourceStatusClient):

    # Clean up
    rssClient.deleteStatusElement("Resource", "Status", "TestName123456789")
    rssClient.deleteStatusElement("Resource", "History", "TestName123456789")
    rssClient.deleteStatusElement("Resource", "Log", "TestName123456789")

    # add the element
    res = rssClient.addIfNotThereStatusElement(
        "Resource",
        "Status",
        "TestName123456789",
        "statusType",
        "Active",
        "elementType",
        "reason",
        Datetime,
        Datetime,
        "tokenOwner",
        Datetime,
    )
    # check if the addIfNotThereStatus query was executed properly
    assert res["OK"] is True, res["Message"]

    res = rssClient.selectStatusElement("Resource", "Status", "TestName123456789")
    # check if the select query was executed properly
    assert res["OK"] is True, res["Message"]
    # check if the name that we got is equal to the previously added 'TestName123456789'
    assert res["Value"][0][0] == "TestName123456789"
    assert res["Value"][0][1] == "statusType"
    assert res["Value"][0][2] == "Active"

    # try to re-add the same element but with different value
    res = rssClient.addIfNotThereStatusElement(
        "Resource",
        "Status",
        "TestName123456789",
        "statusType",
        "Banned",
        "elementType",
        "another reason",
        Datetime,
        Datetime,
        "tokenOwner",
        Datetime,
    )
    # check if the addIfNotThereStatus query was executed properly
    assert res["OK"] is True, res["Message"]
    res = rssClient.selectStatusElement("Resource", "Status", "TestName123456789")
    # check if the select query was executed properly
    assert res["OK"] is True, res["Message"]
    # check if the name that we got is equal to the previously added 'TestName123456789'
    assert res["Value"][0][0] == "TestName123456789"
    assert res["Value"][0][1] == "statusType"
    assert res["Value"][0][2] == "Active"  # NOT Banned

    # delete it
    res = rssClient.deleteStatusElement("Resource", "Status", "TestName123456789")
    # check if the delete query was executed properly
    assert res["OK"] is True, res["Message"]

    # try to select the previously deleted element
    res = rssClient.selectStatusElement("Resource", "Status", "TestName123456789")
    # check if the select query was executed properly
    assert res["OK"] is True, res["Message"]
    # check if the returned value is empty
    assert not res["Value"]
