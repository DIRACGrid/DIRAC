""" Test class for plugins
"""
# pylint: disable=protected-access, missing-docstring

import pytest
from unittest.mock import MagicMock

from DIRAC.DataManagementSystem.Client.test.mock_DM import dm_mock
from DIRAC.Resources.Catalog.test.mock_FC import fc_mock

from DIRAC import gLogger, S_OK

from DIRAC.TransformationSystem.Agent.TransformationPlugin import TransformationPlugin

paramsBase = {
    "AgentType": "Automatic",
    "DerivedProduction": "0",
    "FileMask": "",
    "GroupSize": 1,
    "InheritedFrom": 0,
    "JobType": "MCSimulation",
    "MaxNumberOfTasks": 0,
    "OutputDirectories": "['/lhcb/MC/20', '/lhcb/debug/20']",
    "OutputLFNs": "{'LogTargetPath': ['/lhcb/9.tar'], 'LogFilePath': ['/lhcb/9']}",
    "Priority": "0",
    "SizeGroup": "1",
    "Status": "Active",
    "TransformationID": 1080,
    "Type": "MCSimulation",
    "outputDataFileMask": "GAUSSHIST;ALLSTREAMS.DST",
}

data = {
    "/this/is/at.1": ["SE1"],
    "/this/is/at.2": ["SE2"],
    "/this/is/als/at.2": ["SE2"],
    "/this/is/at.12": ["SE1", "SE2"],
    "/this/is/also/at.12": ["SE1", "SE2"],
    "/this/is/at_123": ["SE1", "SE2", "SE3"],
    "/this/is/at_23": ["SE2", "SE3"],
    "/this/is/at_4": ["SE4"],
}


@pytest.fixture
def setup(mocker):
    tpName = "DIRAC.TransformationSystem.Agent.TransformationPlugin"
    tuName = "DIRAC.TransformationSystem.Client.Utilities"
    mocker.patch(tpName + ".TransformationClient", new=MagicMock("tClient"))
    mocker.patch(tpName + ".TransformationPlugin._getSiteForSE", return_value=S_OK([]))
    mocker.patch(tuName + ".DataManager", new=dm_mock)
    mocker.patch(tuName + ".FileCatalog", new=fc_mock)
    mocker.patch(tuName + ".StorageElement", new=MagicMock("SEMock"))
    gLogger.setLevel("DEBUG")


def test__Standard_G10(setup):
    """Test StandardPlugin: no input data, active."""
    params = dict(paramsBase)
    params["GroupSize"] = 10
    pluginStandard = TransformationPlugin("Standard")
    pluginStandard.setParameters(params)
    res = pluginStandard.run()
    assert res["OK"]
    # no data
    assert res["Value"] == []


def test__Standard_Data_G10(setup):
    """Test StandardPlugin: input data, active."""
    params = dict(paramsBase)
    params["GroupSize"] = 10
    pluginStandard = TransformationPlugin("Standard")
    pluginStandard.setParameters(params)
    pluginStandard.setInputData(data)
    res = pluginStandard.run()
    assert res["OK"]
    # data less than 10
    assert res["Value"] == []


def test__Standard_Flush_G10(setup):
    """Test StandardPlugin: input data, flush."""
    pluginStandard = TransformationPlugin("Standard")
    params = dict(paramsBase)
    params["GroupSize"] = 10
    params["Status"] = "Flush"
    pluginStandard.setParameters(params)
    pluginStandard.setInputData(data)
    res = pluginStandard.run()
    sortedData = [
        ("SE1", {"/this/is/at.1"}),
        ("SE1,SE2", {"/this/is/also/at.12", "/this/is/at.12"}),
        ("SE1,SE2,SE3", {"/this/is/at_123"}),
        ("SE2", {"/this/is/als/at.2", "/this/is/at.2"}),
        ("SE2,SE3", {"/this/is/at_23"}),
        ("SE4", {"/this/is/at_4"}),
    ]
    assert res["OK"]
    assert [(a, set(b)) for a, b in res["Value"]] == sortedData


def test__Standard_G1(setup):
    """Test StandardPlugin: not input data, active."""
    pluginStandard = TransformationPlugin("Standard")
    pluginStandard.setParameters(paramsBase)
    res = pluginStandard.run()
    assert res["OK"]
    assert res["Value"] == []


def test__Standard_Data_G1(setup):
    """Test StandardPlugin: input data, active."""
    pluginStandard = TransformationPlugin("Standard")
    pluginStandard.setParameters(paramsBase)
    pluginStandard.setInputData(data)
    res = pluginStandard.run()
    assert res["OK"]
    sortedData = sorted((",".join(SEs), [lfn]) for lfn, SEs in data.items())
    assert res["Value"], sortedData


def test__Standard_Flush_G1(setup):
    """Test StandardPlugin: input data, flush."""
    pluginStandard = TransformationPlugin("Standard")
    params = dict(paramsBase)
    params["Status"] = "Flush"
    pluginStandard.setParameters(params)
    pluginStandard.setInputData(data)
    res = pluginStandard.run()
    sortedData = sorted((",".join(SEs), [lfn]) for lfn, SEs in data.items())
    assert res["OK"]
    assert sorted(res["Value"]) == sorted(sortedData)


def test__Broadcast_Active_G1(setup):
    """Test BroadcastPlugin: input data, Active"""
    thePlugin = TransformationPlugin("Broadcast")
    params = dict(paramsBase)
    params["Status"] = "Active"
    params["SourceSE"] = "SE1"
    params["TargetSE"] = "SE2"
    thePlugin.setParameters(params)
    thePlugin.setInputData(data)
    res = thePlugin.run()
    # sort returned values by first lfn in LFNs
    sortedReturn = [(SE, lfns) for SE, lfns in sorted(res["Value"], key=lambda t: t[1][0])]
    # sort data by lfn
    expected = [("SE2", [lfn]) for lfn, SEs in sorted(data.items(), key=lambda t: t[0]) if "SE1" in SEs]
    assert res["OK"]
    assert sortedReturn == expected


def test__Broadcast_Active_G10(setup):
    """Test BroadcastPlugin: input data, Active"""
    thePlugin = TransformationPlugin("Broadcast")
    params = dict(paramsBase)
    params["Status"] = "Active"
    params["SourceSE"] = "['SE1']"
    params["TargetSE"] = ["SE2"]
    params["GroupSize"] = 10
    thePlugin.setParameters(params)
    thePlugin.setInputData(data)
    res = thePlugin.run()
    assert res["OK"]
    assert res["Value"] == []


def test__Broadcast_Active_G1_NoSource(setup):
    """Test BroadcastPlugin: input data, Active, noSource"""
    thePlugin = TransformationPlugin("Broadcast")
    params = dict(paramsBase)
    params["Status"] = "Active"
    params["TargetSE"] = ["SE2"]
    params["GroupSize"] = 1
    thePlugin.setParameters(params)
    thePlugin.setInputData(data)
    res = thePlugin.run()
    # sort returned values by first lfn in LFNs
    sortedReturn = [(SE, lfns) for SE, lfns in sorted(res["Value"], key=lambda t: t[1][0])]
    # sort data by lfn
    expected = [("SE2", [lfn]) for lfn in sorted(data.keys())]
    assert res["OK"]
    assert sortedReturn == expected
