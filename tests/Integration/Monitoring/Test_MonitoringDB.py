""" Test for MonitoringDB
"""

import time
import json
import pytest

from DIRAC.tests.Utilities.utils import find_all

import DIRAC

DIRAC.initialize()  # Initialize configuration

from DIRAC import gLogger
from DIRAC.MonitoringSystem.DB.MonitoringDB import MonitoringDB


#############################################

gLogger.setLevel("DEBUG")

# Aggs result

# For bucketed data
aggResult = {
    "Running": {1458216000000: 22.333333333333332, 1458219600000: 44.0, 1458223200000: 43.0},
    "Waiting": {
        1458129600000: 5.0,
        1458133200000: None,
        1458136800000: None,
        1458140400000: 158.0,
        1458144000000: None,
        1458147600000: None,
        1458151200000: None,
        1458154800000: None,
        1458158400000: None,
        1458162000000: None,
        1458165600000: None,
        1458169200000: None,
        1458172800000: None,
        1458176400000: None,
        1458180000000: None,
        1458183600000: None,
        1458187200000: None,
        1458190800000: None,
        1458194400000: None,
        1458198000000: 227.0,
        1458201600000: None,
        1458205200000: None,
        1458208800000: None,
        1458212400000: None,
        1458216000000: None,
        1458219600000: None,
        1458223200000: 8.0,
    },
}

aggResultStatusRunning = {"Running": {1458216000000: 22.333333333333332, 1458219600000: 44.0, 1458223200000: 43.0}}

aggResultStatusRunningAndSite = {"Running": {1458223200000: 43.0}}


# for aggregated data
aggResult_aggregated = {
    "Running": {1458216000000: 6.090909090909091, 1458219600000: 7.333333333333333, 1458223200000: 10.75},
    "Waiting": {
        1458129600000: 1.25,
        1458140400000: 31.6,
        1458198000000: 75.66666666666667,
        1458223200000: 1.1428571428571428,
    },
}

aggResultStatusRunning_aggregated = {
    "Running": {1458216000000: 6.090909090909091, 1458219600000: 7.333333333333333, 1458223200000: 10.75}
}

aggResultStatusRunningAndSite_aggregated = {"Running": {1458223200000: 10.75}}


# create the MonitoringDB object and document type
monitoringDB = MonitoringDB()


# fixture for preparation + teardown
@pytest.fixture
def putAndDelete():
    # Find the test data
    fj = find_all("WMSHistory_testData.json", "../", "tests/Integration/Monitoring")[0]
    with open(fj) as fp:
        data = json.load(fp)

    # hack to work on a test index, which is just the same as WMSHistory
    mapping = {
        "properties": {
            "Status": {"type": "keyword"},
            "Site": {"type": "keyword"},
            "JobSplitType": {"type": "keyword"},
            "ApplicationStatus": {"type": "keyword"},
            "MinorStatus": {"type": "keyword"},
            "User": {"type": "keyword"},
            "JobGroup": {"type": "keyword"},
            "UserGroup": {"type": "keyword"},
        }
    }
    monitoringDB.documentTypes.setdefault(
        "test", {"indexName": "test", "mapping": mapping, "monitoringFields": ["Jobs", "Reschedules"], "period": "day"}
    )

    # put
    res = monitoringDB.put(data, "test")
    assert res["OK"]
    #  Add a time delay to allow updating the modified index before querying it.
    time.sleep(4)

    yield putAndDelete

    # from here on is teardown

    # delete the index
    monitoringDB.deleteIndex("test-*")


#############################################
#  actual tests
#############################################


def test_deleteWMSIndex():
    result = monitoringDB.getIndexName("WMSHistory")
    assert result["OK"], result["Message"]

    today = time.strftime("%Y-%m-%d")
    indexName = "{}-{}".format(result["Value"], today)
    res = monitoringDB.deleteIndex(indexName)
    assert res["OK"]


def test_putAndGetWMSHistory(putAndDelete):
    res = monitoringDB.getDataForAGivenPeriod("test", {}, initialDate="16/03/2016 03:46", endDate="20/03/2016 00:00")
    assert res["OK"]
    assert len(res["Value"]) == 40


@pytest.mark.parametrize(
    "selectField_input, condDict_input, expected, expected_result",
    [
        ("", {}, False, None),
        ("Jobs", {}, True, aggResult),
        ("Jobs", {"": ""}, True, aggResult),
        ("Jobs", {"Status": [""]}, True, aggResult),
        ("Jobs", {"Status": ["Running"]}, True, aggResultStatusRunning),
        ("Jobs", {"Status": ["Running", ""]}, True, aggResultStatusRunning),
        ("Jobs", {"Status": ["Running", "Waiting"]}, True, aggResult),
        ("Jobs", {"Status": ["Done"]}, True, {}),
        ("Jobs", {"Status": ["Running"], "Site": ["LCG.DESYZN.de"]}, True, aggResultStatusRunningAndSite),
    ],
)
def test_retrieveBucketedData(selectField_input, condDict_input, expected, expected_result, putAndDelete):
    res = monitoringDB.retrieveBucketedData(
        typeName="test",
        startTime=1458100000000,
        endTime=1458500000000,
        interval="1h",
        selectField=selectField_input,
        condDict=condDict_input,
        grouping="Status",
    )
    assert res["OK"] is expected
    if res["OK"]:
        print(res["Value"])
        assert res["Value"] == expected_result


@pytest.mark.parametrize(
    "selectField_input, condDict_input, expected, expected_result",
    [
        ("", {}, False, None),
        ("Jobs", {}, True, aggResult_aggregated),
        ("Jobs", {"": ""}, True, aggResult_aggregated),
        ("Jobs", {"Status": [""]}, True, aggResult_aggregated),
        ("Jobs", {"Status": ["Running"]}, True, aggResultStatusRunning_aggregated),
        ("Jobs", {"Status": ["Running", ""]}, True, aggResultStatusRunning_aggregated),
        ("Jobs", {"Status": ["Running", "Waiting"]}, True, aggResult_aggregated),
        ("Jobs", {"Status": ["Done"]}, True, {}),
        ("Jobs", {"Status": ["Running"], "Site": ["LCG.DESYZN.de"]}, True, aggResultStatusRunningAndSite_aggregated),
    ],
)
def test_retrieveAggregatedData(selectField_input, condDict_input, expected, expected_result, putAndDelete):
    res = monitoringDB.retrieveAggregatedData(
        typeName="test",
        startTime=1458100000000,
        endTime=1458500000000,
        interval="1h",
        selectField=selectField_input,
        condDict=condDict_input,
        grouping="Status",
    )
    assert res["OK"] is expected
    if res["OK"]:
        print(res["Value"])
        assert res["Value"] == expected_result
