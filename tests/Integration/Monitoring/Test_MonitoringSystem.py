"""
It is used to test client->db-> service.
  It requires the Monitoring service to be running and installed (so discoverable in the .cfg),
  and this monitoring service should be connecting to an ElasticSeach instance
"""
# pylint: disable=invalid-name,wrong-import-position

import time
import json
from datetime import datetime

import pytest

from DIRAC.tests.Utilities.utils import find_all

import DIRAC

DIRAC.initialize()  # Initialize configuration

from DIRAC import gLogger
from DIRAC.MonitoringSystem.Client.MonitoringClient import MonitoringClient
from DIRAC.Core.Utilities.JEncode import strToFloatDict


#############################################

gLogger.setLevel("DEBUG")

client = MonitoringClient()


# fixture for preparation + teardown
@pytest.fixture
def putAndDelete():
    # Find the test data
    fj = find_all("WMSHistory_testData.json", "../", "tests/Integration/Monitoring")[0]
    with open(fj) as fp:
        data = json.load(fp)

    # put
    res = client.addRecords("wmshistory_index", "WMSHistory", data)
    assert res["OK"]
    assert res["Value"] == len(data)
    time.sleep(5)

    yield putAndDelete

    # from here on is teardown

    # delete the index
    today = datetime.today().strftime("%Y-%m-%d")
    result = "%s-%s" % ("wmshistory_index", today)
    client.deleteIndex(result)


#############################################
#  actual tests
#############################################


def test_listReports(putAndDelete):

    result = client.listReports("WMSHistory")
    assert result["OK"], result["Message"]
    assert result["Value"] == ["AverageNumberOfJobs", "NumberOfJobs", "NumberOfReschedules"]


def test_listUniqueKeyValues(putAndDelete):

    result = client.listUniqueKeyValues("WMSHistory")
    assert result["OK"], result["Message"]
    assert "Status" in result["Value"]
    assert "JobSplitType" in result["Value"]
    assert "MinorStatus" in result["Value"]
    assert "Site" in result["Value"]
    assert "ApplicationStatus" in result["Value"]
    assert "User" in result["Value"]
    assert "JobGroup" in result["Value"]
    assert "UserGroup" in result["Value"]
    assert result["Value"] == {
        "Status": [],
        "JobSplitType": [],
        "MinorStatus": [],
        "Site": [],
        "ApplicationStatus": [],
        "User": [],
        "JobGroup": [],
        "UserGroup": [],
    }


def test_generateDelayedPlot(putAndDelete):

    params = (
        "WMSHistory",
        "NumberOfJobs",
        datetime(2016, 3, 16, 12, 30, 0, 0),
        datetime(2016, 3, 17, 19, 29, 0, 0),
        {"grouping": ["Site"]},
        "Site",
        {},
    )
    result = client.generateDelayedPlot(*params)
    assert result["OK"], result["Message"]
    # self.assertEqual(
    #     result['Value'],
    #     {
    #     plot = 'Z:eNpljcEKwjAQRH8piWLbvQkeRLAeKnhOm7Us2CTsbsH69UYUFIQZZvawb4LUMKQYdjRoKH3kNGeK403W0JEiolSAMZ\
    #     xpwodXcsZukFZItipukFyxeSmiNIB3Zb_lUQL-wD4ssQYYc2Jt_VQuB-089cin6yH1Ur5FPev_\
    #     UgnrSjXfpRp0yfjGGLgcuz2JJl7wCYg6Slo='
    #         'plot': plot,
    #         'thumbnail': False})

    # tempFile = tempfile.TemporaryFile()
    # transferClient = TransferClient('Monitoring/Monitoring')

    # result = transferClient.receiveFile(tempFile, result['Value']['plot'])
    # assert result['OK'], result['Message']


def test_getReport(putAndDelete):

    params = (
        "WMSHistory",
        "NumberOfJobs",
        datetime(2016, 3, 16, 12, 30, 0, 0),
        datetime(2016, 3, 17, 19, 29, 0, 0),
        {"grouping": ["Site"]},
        "Site",
        {},
    )
    result = client.getReport(*params)
    assert result["OK"], result["Message"]
    result["Value"]["data"] = {site: strToFloatDict(value) for site, value in result["Value"]["data"].items()}
    assert result["Value"] == {
        "data": {
            "Multiple": {1458198000000: 227.0},
            "LCG.RRCKI.ru": {1458225000000: 3.0},
            "LCG.IHEP.su": {1458217800000: 18.0},
            "LCG.CNAF.it": {
                1458144000000: None,
                1458172800000: None,
                1458194400000: None,
                1458145800000: None,
                1458189000000: None,
                1458147600000: None,
                1458178200000: None,
                1458183600000: None,
                1458212400000: None,
                1458149400000: None,
                1458207000000: None,
                1458151200000: None,
                1458169200000: None,
                1458201600000: None,
                1458153000000: None,
                1458196200000: None,
                1458154800000: None,
                1458174600000: None,
                1458190800000: None,
                1458156600000: None,
                1458185400000: None,
                1458214200000: None,
                1458158400000: None,
                1458180000000: None,
                1458216000000: None,
                1458208800000: None,
                1458160200000: None,
                1458203400000: None,
                1458162000000: None,
                1458142200000: None,
                1458198000000: None,
                1458163800000: None,
                1458192600000: None,
                1458165600000: None,
                1458176400000: None,
                1458187200000: None,
                1458167400000: None,
                1458210600000: None,
                1458140400000: 4.0,
                1458181800000: None,
                1458205200000: None,
                1458171000000: None,
                1458217800000: 22.0,
                1458199800000: None,
            },
            "LCG.NIKHEF.nl": {1458217800000: 27.0},
            "LCG.Bari.it": {1458221400000: 34.0},
            "Group.RAL.uk": {1458140400000: 34.0},
            "LCG.DESYZN.de": {1458225000000: 43.0},
            "LCG.RAL.uk": {
                1458144000000: None,
                1458158400000: None,
                1458194400000: None,
                1458145800000: None,
                1458223200000: None,
                1458189000000: None,
                1458221400000: None,
                1458225000000: 5.0,
                1458147600000: None,
                1458135000000: None,
                1458183600000: None,
                1458212400000: None,
                1458149400000: None,
                1458178200000: None,
                1458207000000: None,
                1458151200000: None,
                1458169200000: None,
                1458172800000: None,
                1458219600000: None,
                1458201600000: None,
                1458153000000: None,
                1458196200000: None,
                1458154800000: None,
                1458160200000: None,
                1458190800000: None,
                1458156600000: None,
                1458185400000: None,
                1458214200000: None,
                1458129600000: 2.0,
                1458165600000: None,
                1458180000000: None,
                1458216000000: None,
                1458208800000: None,
                1458131400000: None,
                1458174600000: None,
                1458203400000: None,
                1458162000000: None,
                1458171000000: None,
                1458198000000: None,
                1458163800000: None,
                1458192600000: None,
                1458136800000: None,
                1458133200000: None,
                1458187200000: None,
                1458167400000: None,
                1458181800000: None,
                1458210600000: None,
                1458140400000: None,
                1458138600000: None,
                1458176400000: None,
                1458205200000: None,
                1458142200000: None,
                1458217800000: None,
                1458199800000: None,
            },
            "LCG.PIC.es": {1458129600000: 1.0},
            "LCG.GRIDKA.de": {1458129600000: 2.0},
            "LCG.Bristol.uk": {1458221400000: 9.0},
            "LCG.CERN.ch": {1458140400000: 120.0},
            "LCG.Bologna.it": {1458221400000: 1.0},
        },
        "granularity": 1800,
    }


def test_getLastDayData(putAndDelete):
    params = {"Status": "Running", "Site": "LCG.NIKHEF.nl"}
    result = client.getLastDayData("WMSHistory", params)
    assert result["OK"], result["Message"]
    assert len(result["Value"]) == 2
    assert sorted(result["Value"][0]) == sorted(
        [
            "Status",
            "Jobs",
            "JobSplitType",
            "timestamp",
            "MinorStatus",
            "Site",
            "Reschedules",
            "ApplicationStatus",
            "User",
            "JobGroup",
            "UserGroup",
        ]
    )
