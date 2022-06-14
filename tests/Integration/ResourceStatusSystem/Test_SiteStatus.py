""" This is a test of the chain
    SiteStatus ->  ResourceStatusClient -> ResourceStatusDB
    It supposes that the DB is present, and that the service is running
"""
# pylint: disable=wrong-import-position, missing-docstring

from datetime import datetime

import pytest

import DIRAC

DIRAC.initialize()  # Initialize configuration

from DIRAC.ResourceStatusSystem.Client.SiteStatus import SiteStatus
from DIRAC.ResourceStatusSystem.Client.ResourceStatusClient import ResourceStatusClient


Datetime = datetime.now()

testSite = "test1234.test.test"


@pytest.fixture(name="stClient")
def fixtureSiteStatus():
    siteStatus = SiteStatus()
    siteStatus.rssFlag = True
    yield siteStatus


def test_addAndRemove_simpleCase(stClient):

    # make sure that the test sites are not presented in the db
    rsClient = ResourceStatusClient()
    rsClient.deleteStatusElement("Site", "Status", testSite)
    rsClient.deleteStatusElement("Site", "Status", "testActive1.test.test")
    rsClient.deleteStatusElement("Site", "Status", "testActive.test.test")
    rsClient.deleteStatusElement("Site", "Status", "testBanned.test.test")

    # add test site
    result = rsClient.insertStatusElement(
        "Site",
        "Status",
        testSite,
        "all",
        "Active",
        "Site",
        "Synchronized",
        Datetime,
        Datetime,
        "tokenOwner",
        Datetime,
    )
    assert result["OK"] is True, result["Message"]
    stClient.rssCache.refreshCache()

    # TEST getSites
    # ...............................................................................

    result = stClient.getSites()
    assert result["OK"] is True, result["Message"]
    assert testSite in result["Value"]

    # TEST getSiteStatuses
    # ...............................................................................

    result = stClient.getSiteStatuses([testSite])
    assert result["OK"] is True, result["Message"]
    assert result["Value"][testSite] == "Active"

    # TEST getUsableSites
    # ...............................................................................

    result = stClient.getUsableSites([testSite])
    assert result["OK"] is True, result["Message"]
    assert testSite in result["Value"]

    # finally delete the test site
    result = rsClient.deleteStatusElement("Site", "Status", testSite)
    assert result["OK"] is True, result["Message"]


def test_addAndRemove_complicatedTest(stClient):
    rsClient = ResourceStatusClient()
    result = rsClient.insertStatusElement(
        "Site",
        "Status",
        "testActive.test.test",
        "all",
        "Active",
        "Site",
        "Synchronized",
        Datetime,
        Datetime,
        "tokenOwner",
        Datetime,
    )
    assert result["OK"] is True, result["Message"]

    result = rsClient.insertStatusElement(
        "Site",
        "Status",
        "testActive1.test.test",
        "all",
        "Active",
        "Site",
        "Synchronized",
        Datetime,
        Datetime,
        "tokenOwner",
        Datetime,
    )
    assert result["OK"] is True, result["Message"]

    result = rsClient.insertStatusElement(
        "Site",
        "Status",
        "testBanned.test.test",
        "all",
        "Banned",
        "Site",
        "Synchronized",
        Datetime,
        Datetime,
        "tokenOwner",
        Datetime,
    )
    assert result["OK"] is True, result["Message"]
    stClient.rssCache.refreshCache()

    # TEST getSites
    # ...............................................................................

    result = stClient.getSites()
    assert result["OK"] is True, result["Message"]

    assert "testActive1.test.test" in result["Value"]
    assert "testActive.test.test" in result["Value"]
    assert "testBanned.test.test" not in result["Value"]

    # TEST getSites
    # ...............................................................................

    result = stClient.getSites("All")
    assert result["OK"] is True, result["Message"]

    assert "testActive1.test.test" in result["Value"]
    assert "testActive.test.test" in result["Value"]
    assert "testBanned.test.test" in result["Value"]

    # TEST getUsableSites
    # ...............................................................................

    result = stClient.getUsableSites()
    assert result["OK"] is True, result["Message"]

    assert "testActive1.test.test" in result["Value"]
    assert "testActive.test.test" in result["Value"]

    # setting a status
    result = stClient.setSiteStatus("testBanned.test.test", "Probing")
    assert result["OK"] is True, result["Message"]
    stClient.rssCache.refreshCache()

    result = stClient.getSites("Probing")
    assert result["OK"] is True, result["Message"]
    assert "testBanned.test.test" in result["Value"]
    assert "testActive.test.test" not in result["Value"]
