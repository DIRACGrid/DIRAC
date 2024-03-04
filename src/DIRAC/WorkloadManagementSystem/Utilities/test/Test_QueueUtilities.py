""" Test class for QueueUtilities
"""
import copy
from unittest.mock import MagicMock

import pytest
from DIRAC import S_OK
from DIRAC.WorkloadManagementSystem.Utilities.QueueUtilities import *

siteDict1 = {
    "Site1": {
        "CE1": {"Queues": {"Queue1": {}, "Queue2": {}}, "CEType": "Type1"},
        "CE2": {"Queues": {"Queue1": {}}, "CEType": "Type2"},
    },
    "Site2": {"CE3": {"Queues": {"Queue1": {"NumberOfProcessors": 3}}, "CEType": "Type2"}},
}


expectedQueueDict1 = {
    "CE1_Queue1": {
        "CEName": "CE1",
        "CEType": "Type1",
        "QueueName": "Queue1",
        "Site": "Site1",
        "ParametersDict": {
            "GridCE": "CE1",
            "Queue": "Queue1",
            "RequiredTag": [],
            "Setup": None,
            "Site": "Site1",
            "Tag": [],
            "WorkingDirectory": "Queue1",
        },
    },
    "CE1_Queue2": {
        "CEName": "CE1",
        "CEType": "Type1",
        "QueueName": "Queue2",
        "Site": "Site1",
        "ParametersDict": {
            "GridCE": "CE1",
            "Queue": "Queue2",
            "RequiredTag": [],
            "Setup": None,
            "Site": "Site1",
            "Tag": [],
            "WorkingDirectory": "Queue2",
        },
    },
    "CE2_Queue1": {
        "CEName": "CE2",
        "CEType": "Type2",
        "QueueName": "Queue1",
        "Site": "Site1",
        "ParametersDict": {
            "GridCE": "CE2",
            "Queue": "Queue1",
            "RequiredTag": [],
            "Setup": None,
            "Site": "Site1",
            "Tag": [],
            "WorkingDirectory": "Queue1",
        },
    },
    "CE3_Queue1": {
        "CEName": "CE3",
        "CEType": "Type2",
        "QueueName": "Queue1",
        "Site": "Site2",
        "ParametersDict": {
            "GridCE": "CE3",
            "Queue": "Queue1",
            "RequiredTag": [],
            "Setup": None,
            "Site": "Site1",
            "Tag": ["MultiProcessor"],
            "NumberOfProcessors": 3,
            "WorkingDirectory": "Queue1",
        },
    },
}


@pytest.mark.parametrize(
    "queueDict, dictExpected",
    [
        ({}, {}),
        ({"notUsefulParam": ""}, {"notUsefulParam": ""}),
        ({"maxCPUTime": 45000}, {"maxCPUTime": 45000}),
        ({"SI00": 54}, {"SI00": 54}),
        ({"maxCPUTime": 45000, "SI00": 54}, {"maxCPUTime": 45000, "SI00": 54, "CPUTime": 583200}),
    ],
)
def test_computeQueueCPULimit(queueDict, dictExpected):
    """Test the computeCPULimit function"""
    computeQueueCPULimit(queueDict)
    assert queueDict == dictExpected


@pytest.mark.parametrize(
    "ceDict, queueDict, dictExpected",
    [
        ({}, {}, {"Tag": [], "RequiredTag": []}),
        ({"notUsefulParam": ""}, {"notUsefulParam": ""}, {"Tag": [], "RequiredTag": []}),
        ({"Tag": "Test"}, {}, {"Tag": ["Test"], "RequiredTag": []}),
        ({}, {"Tag": "Test"}, {"Tag": ["Test"], "RequiredTag": []}),
        ({"Tag": "Test"}, {"Tag": "Test"}, {"Tag": ["Test"], "RequiredTag": []}),
        ({"Tag": "Test1"}, {"Tag": "Test2"}, {"Tag": ["Test1", "Test2"], "RequiredTag": []}),
        ({"Tag": ["Test1", "Test2"]}, {"Tag": ["Test2", "Test2"]}, {"Tag": ["Test1", "Test2"], "RequiredTag": []}),
        ({"RequiredTag": "Test"}, {}, {"Tag": [], "RequiredTag": ["Test"]}),
        ({}, {"RequiredTag": "Test"}, {"Tag": [], "RequiredTag": ["Test"]}),
        ({"RequiredTag": "Test"}, {"RequiredTag": "Test"}, {"Tag": [], "RequiredTag": ["Test"]}),
        ({"RequiredTag": "Test1"}, {"RequiredTag": "Test2"}, {"Tag": [], "RequiredTag": ["Test1", "Test2"]}),
        (
            {"Tag": ["Test1", "Test2"], "RequiredTag": ["Test2"]},
            {"Tag": ["Test1", "Test3"], "RequiredTag": ["Test2", "Test3"]},
            {"Tag": ["Test1", "Test2", "Test3"], "RequiredTag": ["Test2", "Test3"]},
        ),
    ],
)
def test_resolveTags(ceDict, queueDict, dictExpected):
    """Test the resolveTags function"""
    resolveTags(ceDict, queueDict)
    assert queueDict["Tag"].sort() == dictExpected["Tag"].sort()
    assert queueDict["RequiredTag"].sort() == dictExpected["RequiredTag"].sort()


@pytest.mark.parametrize(
    "ceDict, queueDict, dictExpected",
    [
        ({}, {}, {}),
        ({"Platform": "x86_64"}, {}, {"Platform": "x86_64"}),
        ({"Platform": "notexist"}, {}, {"Platform": "notexist"}),
        ({}, {"Platform": "x86_64"}, {"Platform": "x86_64"}),
        ({}, {"Platform": "notexist"}, {"Platform": "notexist"}),
        ({"Platform": "x86_64", "OS": "centos"}, {}, {"Platform": "x86_64"}),
        ({"Platform": "notexist", "OS": "centos"}, {}, {"Platform": "notexist"}),
        ({}, {"Platform": "x86_64", "OS": "centos"}, {"Platform": "x86_64", "OS": "centos"}),
        ({}, {"Platform": "notexist", "OS": "centos"}, {"Platform": "notexist", "OS": "centos"}),
    ],
)
def test_setPlatform(ceDict, queueDict, dictExpected):
    """Test the setPlatform function"""
    setPlatform(ceDict, queueDict)
    assert queueDict == dictExpected


@pytest.mark.parametrize(
    "queueDict, queuesExpected",
    [
        (siteDict1, expectedQueueDict1),
    ],
)
def test_getQueuesResolved(mocker, queueDict, queuesExpected):
    """Test the getQueuesResolvedEnhanced function"""
    queueCECache = {}
    queueDictLocal = copy.deepcopy(queueDict)

    ce = MagicMock()
    ce.isValid = MagicMock(return_value=S_OK())
    ceFactoryModule = "DIRAC.Resources.Computing.ComputingElementFactory"
    mocker.patch(f"{ceFactoryModule}.ComputingElementFactory.getCE", return_value=S_OK(ce))
    queueDictResolved = getQueuesResolved(queueDictLocal, queueCECache)

    assert queueDictResolved["OK"]
    for qName, qDictResolved in queueDictResolved["Value"].items():
        assert sorted(qDictResolved) == sorted(queuesExpected[qName])
