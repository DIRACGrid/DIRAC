""" Test for AccountingDB
"""
import pytest

import DIRAC

DIRAC.initialize()  # Initialize configuration

from DIRAC import gLogger
from DIRAC.AccountingSystem.DB.AccountingDB import AccountingDB


gLogger.setLevel("DEBUG")

acDB = AccountingDB()

startTime = 1262200000
middleTime = 1262300000
endTime = 1262400000
keyValues_1 = [
    "User_1",
    "UserGroup_1",
    "Site_1",
    "GridCE_1",
    "GridMiddleware_1",
    "GridResourceBroker_1",
    "GridStatus_1",
]
nonKeyValue_1 = [123]

keyValues_2 = [
    "User_2",
    "UserGroup_2",
    "Site_2",
    "GridCE_2",
    "GridMiddleware_2",
    "GridResourceBroker_2",
    "GridStatus_2",
]
nonKeyValue_2 = [456]


@pytest.fixture
def inout():
    res = acDB.insertRecordDirectly("Pilot", startTime, middleTime, keyValues_1 + nonKeyValue_1)
    assert res["OK"], res["Message"]

    res = acDB.insertRecordDirectly("Pilot", middleTime, endTime, keyValues_2 + nonKeyValue_2)
    assert res["OK"], res["Message"]

    yield inout

    res = acDB.deleteRecord(
        "Pilot",
        startTime,
        middleTime,
        keyValues_1 + nonKeyValue_1,
    )
    assert res["OK"], res["Message"]

    res = acDB.deleteRecord(
        "Pilot",
        middleTime,
        endTime,
        keyValues_2 + nonKeyValue_2,
    )
    assert res["OK"], res["Message"]


# Real tests from here


def test_mix():
    res = acDB.getRegisteredTypes()
    assert res["OK"], res["Message"]


def test_retrieveRawRecords(inout):
    # retrieve RAW records
    res = acDB.retrieveRawRecords("Pilot", startTime, endTime, {}, "")
    assert res["OK"], res["Message"]
    assert len(res["Value"]) == 2
    assert res["Value"] == (
        tuple([startTime, middleTime] + keyValues_1 + nonKeyValue_1),
        tuple([middleTime, endTime] + keyValues_2 + nonKeyValue_2),
    )


def test_retrieveBucketedData():
    # retrieve bucketed data
    res = acDB.retrieveBucketedData(
        "Pilot",
        startTime,
        endTime,
        selectFields=["%s, SUM(%s)", ["Site", "Jobs"]],
        condDict={},
        groupFields=["%s", ["Site"]],
        orderFields=["%s", ["Site"]],
    )
    assert res["OK"], res["Message"]
    assert len(res["Value"]) == 2

    res = acDB.retrieveBucketedData(
        "Pilot",
        startTime,
        endTime,
        selectFields=["%s, %s, SUM(%s)", ["Site", "GridCE", "Jobs"]],
        condDict={},
        groupFields=["%s, %s", ["Site", "GridCE"]],
        orderFields=["%s", ["Site"]],
    )
    assert res["OK"], res["Message"]
    assert len(res["Value"]) == 2

    res = acDB.retrieveBucketedData(
        "Pilot",
        startTime,
        endTime,
        selectFields=["SUM(%s)", ["Jobs"]],
        condDict={},
        groupFields=[],
        orderFields=[],
    )
    assert res["OK"], res["Message"]
    assert len(res["Value"]) == 1
