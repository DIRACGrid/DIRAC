# pylint: disable=invalid-name, missing-docstring

import datetime

import DIRAC

DIRAC.initialize()  # Initialize configuration

# sut
from DIRAC.MonitoringSystem.Client.WebAppClient import WebAppClient


def test_WebAppClient():
    res = WebAppClient().getSiteSummaryWeb({}, [], 0, 100)
    assert res["OK"], res["Message"]
    assert res["Value"]["TotalRecords"] in [0, 1, 2, 34]

    res = WebAppClient().getSiteSummarySelectors()
    assert res["OK"], res["Message"]

    res = WebAppClient().getApplicationStates()
    assert res["OK"], res["Message"]

    res = WebAppClient().getJobTypes()
    assert res["OK"], res["Message"]

    res = WebAppClient().getOwners()
    assert res["OK"], res["Message"]

    res = WebAppClient().getOwnerGroup()
    assert res["OK"], res["Message"]

    res = WebAppClient().getJobGroups()
    assert res["OK"], res["Message"]
    resJG_empty = res["Value"]

    res = WebAppClient().getJobGroups(None, datetime.datetime.utcnow())
    assert res["OK"], res["Message"]
    resJG_olderThanNow = res["Value"]
    assert resJG_empty == resJG_olderThanNow

    res = WebAppClient().getJobGroups(None, datetime.datetime.utcnow() - datetime.timedelta(days=365))
    assert res["OK"], res["Message"]
    resJG_olderThanOneYear = res["Value"]
    assert set(resJG_olderThanOneYear).issubset(set(resJG_olderThanNow))

    res = WebAppClient().getSites()
    assert res["OK"], res["Message"]

    res = WebAppClient().getStates()
    assert res["OK"], res["Message"]

    res = WebAppClient().getMinorStates()
    assert res["OK"], res["Message"]
