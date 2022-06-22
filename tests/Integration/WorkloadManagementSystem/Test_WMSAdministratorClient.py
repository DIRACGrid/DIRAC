# pylint: disable=invalid-name, missing-docstring

import DIRAC

DIRAC.initialize()  # Initialize configuration

# sut
from DIRAC.WorkloadManagementSystem.Client.WMSAdministratorClient import WMSAdministratorClient


def test_WMSAdministratorClient():

    wmsAdministrator = WMSAdministratorClient()

    sitesList = ["My.Site.org", "Your.Site.org"]
    res = wmsAdministrator.setSiteMask(sitesList)
    assert res["OK"], res["Message"]

    res = wmsAdministrator.getSiteMask()
    assert res["OK"], res["Message"]
    assert sorted(res["Value"]) == sorted(sitesList)

    res = wmsAdministrator.banSite("My.Site.org", "This is a comment")
    assert res["OK"], res["Message"]

    res = wmsAdministrator.getSiteMask()
    assert res["OK"], res["Message"]
    assert res["Value"] == ["Your.Site.org"]

    res = wmsAdministrator.allowSite("My.Site.org", "This is a comment")
    assert res["OK"], res["Message"]

    res = wmsAdministrator.getSiteMask()
    assert res["OK"], res["Message"]
    assert res["Value"] == sorted(sitesList)

    res = wmsAdministrator.getSiteMaskLogging(sitesList)
    assert res["OK"], res["Message"]
    assert res["Value"]["My.Site.org"][0][3] == "No comment"

    res = wmsAdministrator.getSiteMaskSummary()
    assert res["OK"], res["Message"]
    assert res["Value"]["My.Site.org"] == "Active"

    res = wmsAdministrator.getSiteSummaryWeb({}, [], 0, 100)
    assert res["OK"], res["Message"]
    assert res["Value"]["TotalRecords"] in [0, 1, 2, 34]

    res = wmsAdministrator.getSiteSummarySelectors()
    assert res["OK"], res["Message"]

    res = wmsAdministrator.clearMask()
    assert res["OK"], res["Message"]
    res = wmsAdministrator.getSiteMask()
    assert res["OK"], res["Message"]
    assert res["Value"] == []
