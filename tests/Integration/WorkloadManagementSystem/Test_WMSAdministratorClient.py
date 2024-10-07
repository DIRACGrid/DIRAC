# pylint: disable=invalid-name, missing-docstring

import DIRAC

DIRAC.initialize()  # Initialize configuration

# sut
from DIRAC.WorkloadManagementSystem.Client.WMSAdministratorClient import WMSAdministratorClient


def test_WMSAdministratorClient():
    wmsAdministrator = WMSAdministratorClient()

    res = wmsAdministrator.getSiteSummaryWeb({}, [], 0, 100)
    assert res["OK"], res["Message"]
    assert res["Value"]["TotalRecords"] in [0, 1, 2, 34]

    res = wmsAdministrator.getSiteSummarySelectors()
    assert res["OK"], res["Message"]
