# pylint: disable=invalid-name, missing-docstring

import DIRAC

DIRAC.initialize()  # Initialize configuration

# sut
from DIRAC.MonitoringSystem.Client.WebAppClient import WebAppClient


def test_WMSAdministratorClient():
    res = WebAppClient().getSiteSummaryWeb({}, [], 0, 100)
    assert res["OK"], res["Message"]
    assert res["Value"]["TotalRecords"] in [0, 1, 2, 34]

    res = WebAppClient().getSiteSummarySelectors()
    assert res["OK"], res["Message"]
